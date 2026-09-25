using System.Net;
using System.Text;
using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Application.Configuration;
using ErpOnecAgent.Application.Etl;
using ErpOnecAgent.Infrastructure.OneC;
using Microsoft.Extensions.Options;
using Xunit;

namespace ErpOnecAgent.UnitTests;

/// <summary>
/// S1 root-review cases: the extension's real response bodies, outages that must not look
/// like contract violations, credential failures, a stalled body, and the publication tie
/// between the OData root and the HTTP-service root.
/// </summary>
public sealed class OnecIdentityClientReviewTests
{
    private const string BaseAddress = "https://onec.test/unf/hs/erp-integration/v1/";

    [Theory]
    [InlineData("uninitialized")]
    [InlineData("blocked")]
    public async Task Real_extension_not_ready_body_is_not_ready(string state)
    {
        // The extension answers not-ready with the full identity shape and nulls.
        var body = $$"""{"protocolVersion":1,"state":"{{state}}","scope":"whole-infobase","databaseId":null,"exportEpoch":null,"environment":null}""";
        var result = await Client(Respond((HttpStatusCode)503, body)).GetIdentityAsync(CancellationToken.None);

        Assert.Equal(OnecIdentityFetchKind.NotReady, result.Kind);
        Assert.Equal(state, result.State);
    }

    [Fact]
    public async Task Real_extension_storage_failure_is_an_outage_not_a_contract_violation()
    {
        const string body = """{"status":"error","error":{"code":"STORAGE_UNAVAILABLE","message":"Storage is unavailable.","retryable":true,"details":{}}}""";
        var result = await Client(Respond((HttpStatusCode)503, body)).GetIdentityAsync(CancellationToken.None);

        Assert.Equal(OnecIdentityFetchKind.Unavailable, result.Kind);
        Assert.Equal(503, result.HttpStatus);
    }

    [Fact]
    public async Task Proxy_html_503_is_an_outage()
    {
        var result = await Client((_, _) => Task.FromResult(new HttpResponseMessage((HttpStatusCode)503)
        {
            Content = new StringContent("<html><body>Service Unavailable</body></html>", Encoding.UTF8, "text/html")
        })).GetIdentityAsync(CancellationToken.None);

        Assert.Equal(OnecIdentityFetchKind.Unavailable, result.Kind);
    }

    [Fact]
    public async Task Non_json_200_is_still_malformed()
    {
        var result = await Client((_, _) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new StringContent("<html/>", Encoding.UTF8, "text/html")
        })).GetIdentityAsync(CancellationToken.None);

        Assert.Equal(OnecIdentityFetchKind.Malformed, result.Kind);
    }

    [Fact]
    public async Task Uppercase_uuids_from_the_server_are_accepted_as_the_same_identity()
    {
        const string body = """{"protocolVersion":1,"state":"ready","scope":"whole-infobase","databaseId":"7A19EEBD-37A8-479D-99E7-15CFA3EFA7B1","exportEpoch":"F29BF20F-EA9E-4092-8EE5-F552AB4F2A21","environment":"test"}""";
        var result = await Client(Respond(HttpStatusCode.OK, body)).GetIdentityAsync(CancellationToken.None);

        Assert.Equal(OnecIdentityFetchKind.Ready, result.Kind);
        Assert.Equal(Guid.Parse("7a19eebd-37a8-479d-99e7-15cfa3efa7b1"), result.Identity!.DatabaseId);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("no-colon-secret")]
    [InlineData("{\"username\":\"u\"}")]
    public async Task Credential_failures_are_classified_and_send_nothing(string? secret)
    {
        var sent = 0;
        var http = new HttpClient(new DelegateHandler((_, _) => { sent++; return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)); })) { BaseAddress = new Uri(BaseAddress) };
        var client = new OnecIdentityClient(http, new OnecAuthentication(new FixedSecretStore(secret), Options.Create(new OnecOptions { CredentialSecretName = "onec" })));

        var result = await client.GetIdentityAsync(CancellationToken.None);

        Assert.Equal(OnecIdentityFetchKind.Unavailable, result.Kind);
        Assert.Equal(0, sent);
    }

    [Fact]
    public async Task A_stalled_body_is_bounded_by_the_client_timeout()
    {
        var http = new HttpClient(new DelegateHandler((_, _) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK) { Content = new StreamContent(new StallingStream()) })))
        {
            BaseAddress = new Uri(BaseAddress),
            Timeout = TimeSpan.FromMilliseconds(300)
        };
        var client = new OnecIdentityClient(http, new OnecAuthentication(new FixedSecretStore("u:p"), Options.Create(new OnecOptions { CredentialSecretName = "onec" })));

        var started = DateTimeOffset.UtcNow;
        var result = await client.GetIdentityAsync(CancellationToken.None);

        Assert.Equal(OnecIdentityFetchKind.Unavailable, result.Kind);
        Assert.True(DateTimeOffset.UtcNow - started < TimeSpan.FromSeconds(10));
    }

    [Fact]
    public async Task Caller_cancellation_during_a_stalled_body_propagates()
    {
        var http = new HttpClient(new DelegateHandler((_, _) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK) { Content = new StreamContent(new StallingStream()) })))
        {
            BaseAddress = new Uri(BaseAddress),
            Timeout = TimeSpan.FromMinutes(5)
        };
        var client = new OnecIdentityClient(http, new OnecAuthentication(new FixedSecretStore("u:p"), Options.Create(new OnecOptions { CredentialSecretName = "onec" })));
        using var caller = new CancellationTokenSource(TimeSpan.FromMilliseconds(200));

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => client.GetIdentityAsync(caller.Token));
    }

    [Theory]
    [InlineData("https://host/unf/odata/standard.odata", "https://host/unf/hs/erp-integration/v1", true)]
    [InlineData("https://HOST:443/unf/odata/standard.odata/", "https://host/unf/hs/erp-integration/v1/", true)]
    [InlineData("https://host/unf/odata/standard.odata", "https://host/other/hs/erp-integration/v1", false)]
    [InlineData("https://host/unf/odata/standard.odata", "https://other/unf/hs/erp-integration/v1", false)]
    [InlineData("https://host/unf/odata/standard.odata", "http://host/unf/hs/erp-integration/v1", false)]
    [InlineData("https://host:8443/unf/odata/standard.odata", "https://host/unf/hs/erp-integration/v1", false)]
    [InlineData("https://host/custom-odata-root", "https://host/custom-hs-root", true)]
    [InlineData("not a url", "https://host/unf/hs/erp-integration/v1", false)]
    public void Same_publication_ties_the_odata_root_to_the_identity_service(string odata, string command, bool expected) =>
        Assert.Equal(expected, SourceEndpoint.SamePublication(odata, command));

    [Fact]
    public async Task Guard_resolves_a_fresh_client_on_every_check()
    {
        var created = 0;
        var guard = new SourceIdentityGuard(() => { created++; return new ReadyClient(); }, Options.Create(new OnecOptions
        {
            ODataBaseUrl = "https://host/unf/odata/standard.odata",
            SourceBinding = new OnecSourceBindingOptions
            {
                DatabaseId = "7a19eebd-37a8-479d-99e7-15cfa3efa7b1",
                ExportEpoch = "f29bf20f-ea9e-4092-8ee5-f552ab4f2a21",
                Environment = "test",
                ODataEndpoint = "https://host/unf/odata/standard.odata"
            }
        }));

        Assert.True((await guard.CheckAsync(CancellationToken.None)).IsMatch);
        Assert.True((await guard.CheckAsync(CancellationToken.None)).IsMatch);
        Assert.Equal(2, created);
    }

    private static OnecIdentityClient Client(Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> respond)
    {
        var http = new HttpClient(new DelegateHandler(respond)) { BaseAddress = new Uri(BaseAddress) };
        return new OnecIdentityClient(http, new OnecAuthentication(new FixedSecretStore("u:p"), Options.Create(new OnecOptions { CredentialSecretName = "onec" })));
    }

    private static Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> Respond(HttpStatusCode status, string body) =>
        (_, _) => Task.FromResult(new HttpResponseMessage(status) { Content = new StringContent(body, Encoding.UTF8, "application/json") });

    private sealed class DelegateHandler(Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> respond) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken) => respond(request, cancellationToken);
    }

    private sealed class FixedSecretStore(string? secret) : ISecretStore
    {
        public Task SaveAsync(string name, string value, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task<string?> ReadAsync(string name, CancellationToken cancellationToken) => Task.FromResult(secret);
    }

    private sealed class ReadyClient : IOnecIdentityClient
    {
        public Task<OnecIdentityFetchResult> GetIdentityAsync(CancellationToken cancellationToken) =>
            Task.FromResult(OnecIdentityFetchResult.Ready(new OnecSourceIdentity(1, "ready", "whole-infobase",
                Guid.Parse("7a19eebd-37a8-479d-99e7-15cfa3efa7b1"), Guid.Parse("f29bf20f-ea9e-4092-8ee5-f552ab4f2a21"), "test")));
    }

    /// <summary>A body that never produces data until cancelled.</summary>
    private sealed class StallingStream : Stream
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
            await Task.Delay(Timeout.Infinite, cancellationToken).ConfigureAwait(false);
            return 0;
        }
        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) => ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }
}
