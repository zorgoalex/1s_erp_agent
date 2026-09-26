using System.Diagnostics;
using System.Net;
using System.Text;
using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Application.Configuration;
using ErpOnecAgent.Domain.Etl;
using ErpOnecAgent.Infrastructure.ErpApi;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Xunit;

namespace ErpOnecAgent.UnitTests;

/// <summary>
/// ETL transfer channel: a batch upload (up to 100 MB) or a run completion may legitimately take
/// longer than the ordinary pipeline's 10 s attempt / 30 s total caps. A cut-off after the request
/// was sent is an UNKNOWN outcome that quarantines the batch and blocks the run, so these calls
/// travel a dedicated single-attempt channel bounded by Erp:TransferTimeoutSeconds. Every test
/// drives the real production registration; only the transports are stubbed.
/// </summary>
public sealed class ErpTransferResilienceTests
{
    [Fact]
    public async Task A_slow_batch_upload_is_not_cut_off_by_the_ordinary_attempt_timeout()
    {
        var transfer = new ProbeHandler(async (request, ct) =>
        {
            await Task.Delay(TimeSpan.FromSeconds(12), ct);
            return Ack(request);
        });
        await using var provider = NewServices(transfer);
        var client = provider.GetRequiredService<IErpClient>();

        var stopwatch = Stopwatch.StartNew();
        var ack = await client.UploadBatchAsync(Batch(), new MemoryStream([1, 2, 3]), CancellationToken.None);
        stopwatch.Stop();

        Assert.Equal("acknowledged", ack.Status);
        Assert.True(stopwatch.Elapsed >= TimeSpan.FromSeconds(11), $"returned too early: {stopwatch.Elapsed}");
        Assert.Equal(1, transfer.Requests);
    }

    [Fact]
    public async Task A_slow_run_completion_is_not_cut_off()
    {
        var transfer = new ProbeHandler(async (request, ct) =>
        {
            await Task.Delay(TimeSpan.FromSeconds(12), ct);
            return new HttpResponseMessage(HttpStatusCode.NoContent);
        });
        await using var provider = NewServices(transfer);
        var client = provider.GetRequiredService<IErpClient>();

        await client.CompleteEtlRunRawAsync(Guid.NewGuid(), "{\"runId\":\"x\"}", CancellationToken.None);

        Assert.Equal(1, transfer.Requests);
    }

    [Fact]
    public async Task An_upload_answered_503_is_sent_exactly_once()
    {
        var transfer = new ProbeHandler((_, _) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)));
        await using var provider = NewServices(transfer);
        var client = provider.GetRequiredService<IErpClient>();

        await Assert.ThrowsAsync<HttpRequestException>(() => client.UploadBatchAsync(Batch(), new MemoryStream([1]), CancellationToken.None));

        Assert.Equal(1, transfer.Requests);
    }

    [Fact]
    public async Task Uploads_and_completions_travel_the_transfer_channel_and_other_calls_do_not()
    {
        var transfer = new ProbeHandler((request, _) => Task.FromResult(request.RequestUri!.AbsolutePath.EndsWith("/complete", StringComparison.Ordinal)
            ? new HttpResponseMessage(HttpStatusCode.NoContent) : Ack(request)));
        var ordinary = new ProbeHandler((_, _) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.NoContent)));
        await using var provider = NewServices(transfer, ordinary);
        var client = provider.GetRequiredService<IErpClient>();

        await client.UploadBatchAsync(Batch(), new MemoryStream([1]), CancellationToken.None);
        await client.CompleteEtlRunRawAsync(Guid.NewGuid(), "{}", CancellationToken.None);
        await client.AcknowledgeResultAsync(Guid.NewGuid(), "{}", CancellationToken.None);

        Assert.Equal(2, transfer.Requests);
        Assert.Equal(1, ordinary.Requests);
        Assert.Equal("agent-a", transfer.LastHeaders["X-Agent-Id"]);
    }

    [Fact]
    public async Task A_transfer_that_exceeds_its_budget_fails_instead_of_hanging()
    {
        var transfer = new ProbeHandler(async (request, ct) =>
        {
            await Task.Delay(TimeSpan.FromMinutes(5), ct);
            return Ack(request);
        });
        await using var provider = NewServices(transfer, transferTimeoutSeconds: 2);
        var client = provider.GetRequiredService<IErpClient>();

        var stopwatch = Stopwatch.StartNew();
        await Assert.ThrowsAsync<Polly.Timeout.TimeoutRejectedException>(() => client.UploadBatchAsync(Batch(), new MemoryStream([1]), CancellationToken.None));

        // The 2 s transfer attempt cap fired — not the 10 s ordinary cap, not the HttpClient timeout.
        Assert.True(stopwatch.Elapsed < TimeSpan.FromSeconds(8), $"did not honour the transfer budget: {stopwatch.Elapsed}");
        Assert.Equal(1, transfer.Requests);
    }

    [Fact]
    public async Task An_ack_body_that_stalls_after_its_headers_is_bounded()
    {
        var transfer = new ProbeHandler((_, _) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK) { Content = new StreamContent(new StalledStream()) }));
        await using var provider = NewServices(transfer, transferTimeoutSeconds: 2);
        var client = provider.GetRequiredService<IErpClient>();

        var stopwatch = Stopwatch.StartNew();
        await Assert.ThrowsAnyAsync<Exception>(() => client.UploadBatchAsync(Batch(), new MemoryStream([1]), CancellationToken.None));

        // Bounded by the transfer budget (2 s attempt + 15 s HttpClient margin), never a hang.
        Assert.True(stopwatch.Elapsed < TimeSpan.FromSeconds(25), $"a stalled ACK body hung the upload: {stopwatch.Elapsed}");
    }

    [Fact]
    public void The_transfer_budget_is_ordered_attempt_then_httpclient()
    {
        var options = new ErpOptions { TransferTimeoutSeconds = 300 };

        Assert.Equal(TimeSpan.FromSeconds(300), ErpClientRegistration.TransferAttemptTimeout(options));
        Assert.True(ErpClientRegistration.TransferHttpClientTimeout(options) > ErpClientRegistration.TransferAttemptTimeout(options));
    }

    private static ServiceProvider NewServices(HttpMessageHandler transferProbe, HttpMessageHandler? ordinaryProbe = null, int transferTimeoutSeconds = 300)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<IOptions<ErpOptions>>(Options.Create(new ErpOptions
        {
            BaseUrl = "https://erp.test",
            ApiVersion = "v1",
            LongPollSeconds = 25,
            RequestTimeoutSeconds = 120,
            TransferTimeoutSeconds = transferTimeoutSeconds,
            RequireClientCertificate = false
        }));
        services.AddSingleton<IOptions<AgentOptions>>(Options.Create(new AgentOptions { AgentId = "agent-a", SiteId = "site-s" }));
        services.AddErpApi();
        // Later primary-handler registrations win: stub every channel's transport. Before the
        // fix there is no transfer channel, so uploads reach the ordinary stub (which delegates
        // to the transfer probe to keep the scenario identical) through the 10 s pipeline.
        services.AddHttpClient(ErpClientRegistration.ErpHttpClientName)
            .ConfigurePrimaryHttpMessageHandler(_ => ordinaryProbe ?? transferProbe);
        services.AddHttpClient(ErpClientRegistration.TransferHttpClientName)
            .ConfigurePrimaryHttpMessageHandler(_ => transferProbe);
        services.AddHttpClient(ErpClientRegistration.LongPollHttpClientName)
            .ConfigurePrimaryHttpMessageHandler(_ => new ProbeHandler((_, _) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK))));
        return services.BuildServiceProvider();
    }

    private static EtlBatch Batch() =>
        new(Guid.NewGuid(), Guid.NewGuid(), "clients", 1, "unused", EtlBatchStatus.Uploading, 3, null, null, "hash", 3, 3, 1, DateTimeOffset.UtcNow);

    private static HttpResponseMessage Ack(HttpRequestMessage request) => new(HttpStatusCode.OK)
    {
        Content = new StringContent(
            $$"""{"batchId":"{{request.Headers.GetValues("X-Batch-Id").Single()}}","status":"acknowledged","rowsAccepted":3,"checksumValid":true,"acknowledgedAtUtc":"2026-09-26T00:00:00+00:00"}""",
            Encoding.UTF8, "application/json")
    };

    private sealed class StalledStream : Stream
    {
        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => 0; set => throw new NotSupportedException(); }
        public override void Flush() { }
        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            await Task.Delay(Timeout.Infinite, cancellationToken);
            return 0;
        }
        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
            ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }

    private sealed class ProbeHandler(Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> respond) : HttpMessageHandler
    {
        private int _requests;
        public int Requests => Volatile.Read(ref _requests);
        public Dictionary<string, string> LastHeaders { get; } = new(StringComparer.OrdinalIgnoreCase);

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            Interlocked.Increment(ref _requests);
            foreach (var header in request.Headers) LastHeaders[header.Key] = string.Join(",", header.Value);
            return await respond(request, ct).ConfigureAwait(false);
        }
    }
}
