using System.Net;
using System.Text;
using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Application.Configuration;
using ErpOnecAgent.Contracts.Erp;
using ErpOnecAgent.Domain.Etl;
using ErpOnecAgent.Infrastructure.ErpApi;
using Microsoft.Extensions.Options;
using Xunit;

namespace ErpOnecAgent.UnitTests;

/// <summary>
/// H1: completion replay sends the stored complete_payload_json byte-for-byte with a stable
/// dedup identity on every retry.
/// </summary>
public sealed class ErpClientCompletionReplayTests
{
    private static readonly Guid RunId = Guid.Parse("33333333-3333-3333-3333-333333333333");

    [Fact]
    public async Task Raw_completion_sends_the_exact_stored_bytes_and_dedup_identity_on_every_retry()
    {
        // Deliberately non-canonical text: key order, spacing, an escaped unicode letter and
        // a numeric lexeme that a serializer would normalize.
        const string stored = "{ \"runId\":\"33333333-3333-3333-3333-333333333333\",  \"rows\": 1.0, \"name\":\"\\u041a\", \"a\":[3,1] }";
        var captured = new List<Captured>();
        var client = NewClient(captured, HttpStatusCode.OK);

        await client.CompleteEtlRunRawAsync(RunId, stored, CancellationToken.None);
        await client.CompleteEtlRunRawAsync(RunId, stored, CancellationToken.None);

        Assert.Equal(2, captured.Count);
        var expected = new UTF8Encoding(false).GetBytes(stored);
        Assert.Equal(expected, captured[0].Body);
        Assert.Equal(expected, captured[1].Body);
        Assert.Equal("POST", captured[0].Method);
        Assert.Equal("/api/etl/runs/33333333-3333-3333-3333-333333333333/complete", captured[0].Path);
        Assert.Equal(RunId.ToString("D"), captured[0].IdempotencyKey);
        Assert.Equal(captured[0].IdempotencyKey, captured[1].IdempotencyKey);
        Assert.Equal("application/json; charset=utf-8", captured[0].ContentType);
        Assert.Equal(captured[0].ContentType, captured[1].ContentType);
    }

    [Fact]
    public async Task Raw_completion_surfaces_a_non_success_status_as_an_exception()
    {
        var client = NewClient([], HttpStatusCode.ServiceUnavailable);

        var ex = await Assert.ThrowsAsync<HttpRequestException>(() => client.CompleteEtlRunRawAsync(RunId, "{}", CancellationToken.None));

        Assert.Equal(HttpStatusCode.ServiceUnavailable, ex.StatusCode);
    }

    [Fact]
    public async Task Raw_completion_refuses_invalid_utf16_instead_of_substituting()
    {
        var captured = new List<Captured>();
        var client = NewClient(captured, HttpStatusCode.OK);
        var loneSurrogate = "{\"a\":\"" + (char)0xD800 + "\"}";

        await Assert.ThrowsAsync<EncoderFallbackException>(() => client.CompleteEtlRunRawAsync(RunId, loneSurrogate, CancellationToken.None));

        Assert.Empty(captured);
    }

    [Fact]
    public async Task Error_messages_never_contain_the_erp_response_body()
    {
        var http = new HttpClient(new BodyHandler(HttpStatusCode.BadRequest, "secret-token=abc; customer=Ivanov")) { BaseAddress = new Uri("https://erp.test/api/") };
        var client = new ErpClient(http, Options.Create(new AgentOptions { AgentId = "agent-1", SiteId = "site-1" }));

        var ex = await Assert.ThrowsAsync<HttpRequestException>(() => client.CompleteEtlRunRawAsync(RunId, "{}", CancellationToken.None));

        Assert.DoesNotContain("secret-token", ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("Ivanov", ex.Message, StringComparison.Ordinal);
        Assert.Contains("400", ex.Message, StringComparison.Ordinal);
    }

    private sealed class BodyHandler(HttpStatusCode status, string body) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken) =>
            Task.FromResult(new HttpResponseMessage(status) { Content = new StringContent(body) });
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public async Task Raw_completion_refuses_an_empty_payload_without_sending(string? payload)
    {
        var captured = new List<Captured>();
        var client = NewClient(captured, HttpStatusCode.OK);

        await Assert.ThrowsAnyAsync<ArgumentException>(() => client.CompleteEtlRunRawAsync(RunId, payload!, CancellationToken.None));

        Assert.Empty(captured);
    }

    [Fact]
    public async Task Clients_without_replay_support_fail_closed()
    {
        IErpClient fake = new NoReplayClient();

        await Assert.ThrowsAsync<NotSupportedException>(() => fake.CompleteEtlRunRawAsync(RunId, "{}", CancellationToken.None));
    }

    private static ErpClient NewClient(List<Captured> captured, HttpStatusCode status)
    {
        var http = new HttpClient(new CapturingHandler(captured, status)) { BaseAddress = new Uri("https://erp.test/api/") };
        return new ErpClient(http, Options.Create(new AgentOptions { AgentId = "agent-1", SiteId = "site-1" }));
    }

    private sealed record Captured(string Method, string Path, string? IdempotencyKey, string? ContentType, byte[] Body);

    private sealed class CapturingHandler(List<Captured> captured, HttpStatusCode status) : HttpMessageHandler
    {
        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var body = request.Content is null ? [] : await request.Content.ReadAsByteArrayAsync(cancellationToken);
            captured.Add(new Captured(request.Method.Method, request.RequestUri!.AbsolutePath,
                request.Headers.TryGetValues("Idempotency-Key", out var key) ? key.Single() : null,
                request.Content?.Headers.ContentType?.ToString(), body));
            return new HttpResponseMessage(status) { Content = new StringContent(string.Empty) };
        }
    }

    private sealed class NoReplayClient : IErpClient
    {
        public Task<SessionStartResponse> StartSessionAsync(SessionStartRequest request, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task<LeaseResponse> LeaseCommandAsync(LeaseRequest request, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task AcknowledgeReceivedAsync(Guid commandId, CommandReceivedRequest request, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task AcknowledgeResultAsync(Guid commandId, string resultJson, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task<BatchAcknowledgement> UploadBatchAsync(EtlBatch batch, Stream content, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task CompleteEtlRunAsync(Guid runId, object summary, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task SendHeartbeatAsync(HeartbeatRequest request, CancellationToken cancellationToken) => throw new NotImplementedException();
        public Task<RemoteConfigurationResponse?> GetConfigurationAsync(long currentVersion, CancellationToken cancellationToken) => throw new NotImplementedException();
    }
}
