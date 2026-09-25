using System.Net;
using System.Text;
using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Application.Configuration;
using ErpOnecAgent.Domain.Etl;
using ErpOnecAgent.Infrastructure.OneC;
using Microsoft.Extensions.Options;
using Xunit;

namespace ErpOnecAgent.UnitTests;

/// <summary>A10c: bounded retry of one OData page on transient failures; the read is idempotent.</summary>
public sealed class OnecODataRetryTests
{
    private static readonly EtlEntityDefinition Entity =
        new("clients", "Catalog_Clients", "Ref_Key", "UpdatedAt", "DeletionMark", ["Description"], "incremental", 500, 10, ODataVersion: 4);
    private static readonly EtlCursor Upper = new(DateTimeOffset.Parse("2026-09-26T00:00:00Z", System.Globalization.CultureInfo.InvariantCulture), null);

    [Theory]
    [InlineData(HttpStatusCode.ServiceUnavailable)]
    [InlineData(HttpStatusCode.BadGateway)]
    [InlineData(HttpStatusCode.GatewayTimeout)]
    [InlineData(HttpStatusCode.InternalServerError)]
    [InlineData(HttpStatusCode.TooManyRequests)]
    [InlineData(HttpStatusCode.RequestTimeout)]
    public async Task A_transient_status_is_retried_and_the_page_is_read_once(HttpStatusCode status)
    {
        var handler = new ScriptedHandler(Status(status), Ok(Page(3)));

        var rows = await ReadAllAsync(Client(handler));

        Assert.Equal(3, rows.Count);
        Assert.Equal(2, handler.Calls);
    }

    [Fact]
    public async Task A_network_failure_is_retried()
    {
        var handler = new ScriptedHandler(Throw(new HttpRequestException("connection reset")), Throw(new HttpRequestException("again")), Ok(Page(2)));

        Assert.Equal(2, (await ReadAllAsync(Client(handler))).Count);
        Assert.Equal(3, handler.Calls);
    }

    [Fact]
    public async Task An_http_timeout_is_retried()
    {
        var handler = new ScriptedHandler(Throw(new TaskCanceledException("timeout")), Ok(Page(1)));

        Assert.Single(await ReadAllAsync(Client(handler)));
        Assert.Equal(2, handler.Calls);
    }

    [Theory]
    [InlineData(HttpStatusCode.NotFound)]
    [InlineData(HttpStatusCode.Unauthorized)]
    [InlineData(HttpStatusCode.Forbidden)]
    [InlineData(HttpStatusCode.BadRequest)]
    public async Task A_permanent_status_is_not_retried(HttpStatusCode status)
    {
        var handler = new ScriptedHandler(Status(status), Ok(Page(1)));

        await Assert.ThrowsAsync<HttpRequestException>(() => ReadAllAsync(Client(handler)));
        Assert.Equal(1, handler.Calls);
    }

    [Fact]
    public async Task Retries_are_bounded()
    {
        var handler = new ScriptedHandler(Status(HttpStatusCode.ServiceUnavailable), Status(HttpStatusCode.ServiceUnavailable),
            Status(HttpStatusCode.ServiceUnavailable), Status(HttpStatusCode.ServiceUnavailable), Ok(Page(1)));

        await Assert.ThrowsAsync<HttpRequestException>(() => ReadAllAsync(Client(handler, retries: 3)));
        Assert.Equal(4, handler.Calls);
    }

    [Fact]
    public async Task Zero_retries_disables_the_retry()
    {
        var handler = new ScriptedHandler(Status(HttpStatusCode.ServiceUnavailable), Ok(Page(1)));

        await Assert.ThrowsAsync<HttpRequestException>(() => ReadAllAsync(Client(handler, retries: 0)));
        Assert.Equal(1, handler.Calls);
    }

    [Fact]
    public async Task A_limit_violation_is_not_retried()
    {
        var handler = new ScriptedHandler(Ok(Page(1, 200 * 1024)), Ok(Page(1)));

        await Assert.ThrowsAsync<InvalidDataException>(() => ReadAllAsync(Client(handler)));
        Assert.Equal(1, handler.Calls);
    }

    [Fact]
    public async Task Malformed_json_is_not_retried()
    {
        var handler = new ScriptedHandler(Ok("{\"value\":[{"), Ok(Page(1)));

        await Assert.ThrowsAnyAsync<System.Text.Json.JsonException>(() => ReadAllAsync(Client(handler)));
        Assert.Equal(1, handler.Calls);
    }

    [Fact]
    public async Task Caller_cancellation_is_not_retried()
    {
        using var cancellation = new CancellationTokenSource();
        var handler = new ScriptedHandler(_ => { cancellation.Cancel(); throw new TaskCanceledException("cancelled", null, cancellation.Token); }, Ok(Page(1)));

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => ReadAllAsync(Client(handler), cancellation.Token));
        Assert.Equal(1, handler.Calls);
    }

    [Fact]
    public async Task A_second_page_failure_retries_only_that_page()
    {
        var first = "{\"value\":[" + Row() + "],\"@odata.nextLink\":\"Catalog_Clients?$skiptoken=2\"}";
        var handler = new ScriptedHandler(Ok(first), Status(HttpStatusCode.ServiceUnavailable), Ok(Page(2)));

        var rows = await ReadAllAsync(Client(handler));

        Assert.Equal(3, rows.Count);
        Assert.Equal(3, handler.Calls);
        Assert.Equal(handler.Requests[1], handler.Requests[2]);
    }

    [Fact]
    public async Task A_repeating_next_link_is_refused()
    {
        var looping = "{\"value\":[" + Row() + "],\"@odata.nextLink\":\"Catalog_Clients?$skiptoken=loop\"}";
        var handler = new ScriptedHandler(Ok(looping), Ok(looping), Ok(looping), Ok(looping));
        var rows = new List<System.Text.Json.JsonElement>();

        await Assert.ThrowsAsync<InvalidDataException>(() => ReadIntoAsync(Client(handler), Entity, rows));

        Assert.Equal(2, handler.Calls);
        Assert.Equal(2, rows.Count);
    }

    [Fact]
    public async Task A_next_link_back_to_the_first_page_is_refused_before_it_is_read_again()
    {
        var handler = new ScriptedHandler(request => Ok("{\"value\":[" + Row() + "],\"@odata.nextLink\":\"" + request.RequestUri!.AbsoluteUri + "\"}")(request));
        var rows = new List<System.Text.Json.JsonElement>();

        await Assert.ThrowsAsync<InvalidDataException>(() => ReadIntoAsync(Client(handler), Entity, rows));

        Assert.Equal(1, handler.Calls);
        Assert.Single(rows);
    }

    [Fact]
    public async Task A_body_interrupted_mid_stream_is_retried_without_duplicating_rows()
    {
        var page = Page(4);
        var handler = new ScriptedHandler(_ => Streamed(new FailingStream(Encoding.UTF8.GetBytes(page), failAfter: page.Length / 2)), Ok(page));

        var rows = await ReadAllAsync(Client(handler));

        Assert.Equal(4, rows.Count);
        Assert.Equal(2, handler.Calls);
    }

    [Fact]
    public async Task A_stalled_body_times_out_and_is_retried()
    {
        var handler = new ScriptedHandler(_ => Streamed(new StalledStream()), Ok(Page(2)));

        var rows = await ReadAllAsync(Client(handler, timeout: TimeSpan.FromMilliseconds(300)));

        Assert.Equal(2, rows.Count);
        Assert.Equal(2, handler.Calls);
    }

    [Fact]
    public async Task A_skip_page_failure_retries_the_same_skip()
    {
        var small = Entity with { PageSize = 2 };
        var handler = new ScriptedHandler(Ok(Page(2)), Status(HttpStatusCode.ServiceUnavailable), Ok(Page(1)));
        var rows = new List<System.Text.Json.JsonElement>();

        await ReadIntoAsync(Client(handler), small, rows);

        Assert.Equal(3, rows.Count);
        Assert.Equal(3, handler.Calls);
        Assert.Contains("%24skip=2", handler.Requests[1].Replace("$skip", "%24skip", StringComparison.Ordinal), StringComparison.Ordinal);
        Assert.Equal(handler.Requests[1], handler.Requests[2]);
    }

    [Fact]
    public void Tls_failures_are_not_transient()
    {
        Assert.False(OnecODataClient.IsTransient(new HttpRequestException(HttpRequestError.SecureConnectionError, "tls"), CancellationToken.None));
        Assert.True(OnecODataClient.IsTransient(new HttpRequestException(HttpRequestError.ConnectionError, "reset"), CancellationToken.None));
    }

    private static async Task<List<System.Text.Json.JsonElement>> ReadAllAsync(OnecODataClient client, CancellationToken cancellationToken = default)
    {
        var rows = new List<System.Text.Json.JsonElement>();
        await ReadIntoAsync(client, Entity, rows, cancellationToken);
        return rows;
    }

    private static async Task ReadIntoAsync(OnecODataClient client, EtlEntityDefinition entity, List<System.Text.Json.JsonElement> rows, CancellationToken cancellationToken = default)
    {
        await foreach (var row in client.ReadEntityAsync(entity, null, Upper, full: true, cancellationToken)) rows.Add(row);
    }

    private static HttpResponseMessage Streamed(Stream body)
    {
        var content = new StreamContent(body);
        content.Headers.ContentType = new("application/json");
        return new HttpResponseMessage(HttpStatusCode.OK) { Content = content };
    }

    private sealed class FailingStream(byte[] bytes, int failAfter) : MemoryStream(bytes)
    {
        public override int Read(byte[] buffer, int offset, int count) => Fail(base.Read(buffer, offset, Math.Min(count, Remaining())));
        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) =>
            ValueTask.FromResult(Fail(base.Read(buffer.Span[..Math.Min(buffer.Length, Remaining())])));
        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
            Task.FromResult(Read(buffer, offset, count));
        private int Remaining() => Math.Max(1, failAfter - (int)Position);
        private int Fail(int read) => Position > failAfter ? throw new IOException("connection reset mid-body") : read;
    }

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

    private static string Row(int descriptionLength = 10) =>
        $$"""{"Ref_Key":"{{Guid.NewGuid()}}","UpdatedAt":"2026-09-25T00:00:00Z","DeletionMark":false,"Description":"{{new string('d', descriptionLength)}}"}""";

    private static string Page(int rows, int descriptionLength = 10) =>
        "{\"value\":[" + string.Join(',', Enumerable.Range(0, rows).Select(_ => Row(descriptionLength))) + "]}";

    private static Func<HttpRequestMessage, HttpResponseMessage> Ok(string body) => _ =>
        new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(body, Encoding.UTF8, "application/json") };

    private static Func<HttpRequestMessage, HttpResponseMessage> Status(HttpStatusCode status) => _ =>
        new HttpResponseMessage(status) { Content = new StringContent("{}", Encoding.UTF8, "application/json") };

    private static Func<HttpRequestMessage, HttpResponseMessage> Throw(Exception ex) => _ => throw ex;

    private static OnecODataClient Client(ScriptedHandler handler, int retries = 3, TimeSpan? timeout = null)
    {
        var http = new HttpClient(handler) { BaseAddress = new Uri("http://127.0.0.1:5099/odata/"), Timeout = timeout ?? TimeSpan.FromSeconds(30) };
        return new OnecODataClient(http, new OnecAuthentication(new StaticSecretStore(), Options.Create(new OnecOptions { CredentialSecretName = "test" })),
            Options.Create(new EtlOptions { MaxODataRowBytes = 64 * 1024, ODataPageRetries = retries, ODataRetryBaseDelayMilliseconds = 1 }));
    }

    private sealed class ScriptedHandler(params Func<HttpRequestMessage, HttpResponseMessage>[] steps) : HttpMessageHandler
    {
        public int Calls { get; private set; }
        public List<string> Requests { get; } = [];

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            Requests.Add(request.RequestUri!.ToString());
            var step = steps[Math.Min(Calls, steps.Length - 1)];
            Calls++;
            return Task.FromResult(step(request));
        }
    }

    private sealed class StaticSecretStore : ISecretStore
    {
        public Task SaveAsync(string name, string secret, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task<string?> ReadAsync(string name, CancellationToken cancellationToken) => Task.FromResult<string?>("user:password");
    }
}
