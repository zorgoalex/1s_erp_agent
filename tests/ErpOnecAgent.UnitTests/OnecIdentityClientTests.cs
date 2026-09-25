using System.Net;
using System.Text;
using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Application.Configuration;
using ErpOnecAgent.Application.Etl;
using ErpOnecAgent.Infrastructure.OneC;
using Microsoft.Extensions.Options;
using Xunit;

namespace ErpOnecAgent.UnitTests;

public sealed class OnecIdentityClientTests
{
    private static readonly Guid DatabaseId = Guid.Parse("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
    private static readonly Guid ExportEpoch = Guid.Parse("b1eebc99-9c0b-4ef8-bb6d-6bb9bd380a22");
    private const string BaseAddress = "http://127.0.0.1:5099/erp-integration/v1/";

    private static readonly string ReadyBody =
        $$"""{"protocolVersion":1,"state":"ready","scope":"whole-infobase","databaseId":"{{DatabaseId:D}}","exportEpoch":"{{ExportEpoch:D}}","environment":"test"}""";

    // C1: a 200 ready body is Ready with exact fields; unknown properties are tolerated.
    [Fact]
    public async Task C1_ready_200_body_parses_exact_fields_and_tolerates_unknown_properties()
    {
        var body =
            $$$"""{"protocolVersion":1,"state":"ready","scope":"whole-infobase","databaseId":"{{{DatabaseId:D}}}","exportEpoch":"{{{ExportEpoch:D}}}","environment":"test","unknown":"x","nested":{"a":[1,2]}}""";
        var client = CreateClient(new StubHandler((_, _) => Task.FromResult(Json(HttpStatusCode.OK, body))));

        var result = await client.GetIdentityAsync(CancellationToken.None);

        Assert.Equal(OnecIdentityFetchKind.Ready, result.Kind);
        Assert.Equal(200, result.HttpStatus);
        Assert.Equal("ready", result.State);
        Assert.Equal(new OnecSourceIdentity(1, "ready", "whole-infobase", DatabaseId, ExportEpoch, "test"), result.Identity);
    }

    // C2: 503 with a recognized not-ready state is NotReady; any other 503 is a retryable outage (Unavailable) — root review S1.
    [Theory]
    [InlineData("uninitialized")]
    [InlineData("blocked")]
    public async Task C2_503_with_recognized_state_is_not_ready(string state)
    {
        var client = CreateClient(new StubHandler((_, _) => Task.FromResult(Json((HttpStatusCode)503, $$"""{"state":"{{state}}"}"""))));

        var result = await client.GetIdentityAsync(CancellationToken.None);

        Assert.Equal(OnecIdentityFetchKind.NotReady, result.Kind);
        Assert.Equal(503, result.HttpStatus);
        Assert.Equal(state, result.State);
        Assert.Null(result.Identity);
    }

    [Theory]
    [InlineData("""{"state":"ready"}""")]
    [InlineData("""{"state":"migrating"}""")]
    [InlineData("""{}""")]
    [InlineData("""{"state":503}""")]
    public async Task C2_503_without_recognized_state_is_an_outage(string body)
    {
        var client = CreateClient(new StubHandler((_, _) => Task.FromResult(Json((HttpStatusCode)503, body))));

        var result = await client.GetIdentityAsync(CancellationToken.None);

        Assert.Equal(OnecIdentityFetchKind.Unavailable, result.Kind);
        Assert.Equal(503, result.HttpStatus);
    }

    // C3: a 200 body reporting a not-ready state is NotReady.
    [Fact]
    public async Task C3_200_in_uninitialized_state_is_not_ready()
    {
        var client = CreateClient(new StubHandler((_, _) => Task.FromResult(Json(HttpStatusCode.OK, """{"protocolVersion":1,"state":"uninitialized"}"""))));

        var result = await client.GetIdentityAsync(CancellationToken.None);

        Assert.Equal(OnecIdentityFetchKind.NotReady, result.Kind);
        Assert.Equal(200, result.HttpStatus);
        Assert.Equal("uninitialized", result.State);
    }

    // C4: bodies that violate the identity contract are Malformed (parsed directly).
    [Theory]
    [InlineData("{")] // invalid JSON
    [InlineData("[1,2]")] // array
    [InlineData("\"ready\"")] // string
    [InlineData("42")] // number
    [InlineData("null")] // JSON null
    public void C4_invalid_json_or_non_object_body_is_malformed(string body)
    {
        var result = OnecIdentityClient.Parse(200, Encoding.UTF8.GetBytes(body));

        Assert.Equal(OnecIdentityFetchKind.Malformed, result.Kind);
        Assert.Equal(200, result.HttpStatus);
        Assert.Null(result.Identity);
    }

    [Theory]
    // protocolVersion 2, missing, or a string
    [InlineData("""{"protocolVersion":2,"state":"ready","scope":"whole-infobase","databaseId":"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11","exportEpoch":"b1eebc99-9c0b-4ef8-bb6d-6bb9bd380a22","environment":"test"}""")]
    [InlineData("""{"state":"ready","scope":"whole-infobase","databaseId":"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11","exportEpoch":"b1eebc99-9c0b-4ef8-bb6d-6bb9bd380a22","environment":"test"}""")]
    [InlineData("""{"protocolVersion":"1","state":"ready","scope":"whole-infobase","databaseId":"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11","exportEpoch":"b1eebc99-9c0b-4ef8-bb6d-6bb9bd380a22","environment":"test"}""")]
    // empty-string GUID, zero GUID, non-D GUID
    [InlineData("""{"protocolVersion":1,"state":"ready","scope":"whole-infobase","databaseId":"","exportEpoch":"b1eebc99-9c0b-4ef8-bb6d-6bb9bd380a22","environment":"test"}""")]
    [InlineData("""{"protocolVersion":1,"state":"ready","scope":"whole-infobase","databaseId":"00000000-0000-0000-0000-000000000000","exportEpoch":"b1eebc99-9c0b-4ef8-bb6d-6bb9bd380a22","environment":"test"}""")]
    [InlineData("""{"protocolVersion":1,"state":"ready","scope":"whole-infobase","databaseId":"a0eebc999c0b4ef8bb6d6bb9bd380a11","exportEpoch":"b1eebc99-9c0b-4ef8-bb6d-6bb9bd380a22","environment":"test"}""")]
    [InlineData("""{"protocolVersion":1,"state":"ready","scope":"whole-infobase","databaseId":"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11","exportEpoch":"b1eebc999c0b4ef8bb6d6bb9bd380a22","environment":"test"}""")]
    // environment outside the set or missing; scope missing
    [InlineData("""{"protocolVersion":1,"state":"ready","scope":"whole-infobase","databaseId":"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11","exportEpoch":"b1eebc99-9c0b-4ef8-bb6d-6bb9bd380a22","environment":"staging"}""")]
    [InlineData("""{"protocolVersion":1,"state":"ready","scope":"whole-infobase","databaseId":"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11","exportEpoch":"b1eebc99-9c0b-4ef8-bb6d-6bb9bd380a22","environment":"Test"}""")]
    [InlineData("""{"protocolVersion":1,"state":"ready","scope":"whole-infobase","databaseId":"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11","exportEpoch":"b1eebc99-9c0b-4ef8-bb6d-6bb9bd380a22"}""")]
    [InlineData("""{"protocolVersion":1,"state":"ready","databaseId":"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11","exportEpoch":"b1eebc99-9c0b-4ef8-bb6d-6bb9bd380a22","environment":"test"}""")]
    // state missing or unknown on a 200 body
    [InlineData("""{"protocolVersion":1,"scope":"whole-infobase","databaseId":"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11","exportEpoch":"b1eebc99-9c0b-4ef8-bb6d-6bb9bd380a22","environment":"test"}""")]
    [InlineData("""{"protocolVersion":1,"state":"migrating","scope":"whole-infobase","databaseId":"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11","exportEpoch":"b1eebc99-9c0b-4ef8-bb6d-6bb9bd380a22","environment":"test"}""")]
    // duplicate property names, case-variant names, only a case-variant name
    [InlineData("""{"protocolVersion":1,"protocolVersion":1,"state":"ready","scope":"whole-infobase","databaseId":"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11","exportEpoch":"b1eebc99-9c0b-4ef8-bb6d-6bb9bd380a22","environment":"test"}""")]
    [InlineData("""{"protocolVersion":1,"state":"ready","scope":"whole-infobase","DatabaseId":"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11","databaseId":"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11","exportEpoch":"b1eebc99-9c0b-4ef8-bb6d-6bb9bd380a22","environment":"test"}""")]
    [InlineData("""{"protocolVersion":1,"state":"ready","scope":"whole-infobase","DatabaseId":"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11","exportEpoch":"b1eebc99-9c0b-4ef8-bb6d-6bb9bd380a22","environment":"test"}""")]
    public void C4_contract_violating_body_is_malformed(string body)
    {
        var result = OnecIdentityClient.Parse(200, Encoding.UTF8.GetBytes(body));

        Assert.Equal(OnecIdentityFetchKind.Malformed, result.Kind);
        Assert.Null(result.Identity);
    }

    // C4: a body over 16 KiB is Malformed — declared length and streamed length alike.
    [Fact]
    public async Task C4_content_length_over_the_limit_is_malformed()
    {
        var oversized = new string('x', OnecIdentityClient.MaxBodyBytes + 1);
        var client = CreateClient(new StubHandler((_, _) => Task.FromResult(Json(HttpStatusCode.OK, oversized))));

        var result = await client.GetIdentityAsync(CancellationToken.None);

        Assert.Equal(OnecIdentityFetchKind.Malformed, result.Kind);
        Assert.Equal(200, result.HttpStatus);
    }

    [Fact]
    public async Task C4_streamed_body_without_content_length_over_the_limit_is_malformed()
    {
        var oversized = new byte[OnecIdentityClient.MaxBodyBytes + 1];
        var client = CreateClient(new StubHandler((_, _) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new UnknownLengthContent(oversized)
        })));

        var result = await client.GetIdentityAsync(CancellationToken.None);

        Assert.Equal(OnecIdentityFetchKind.Malformed, result.Kind);
        Assert.Equal(200, result.HttpStatus);
    }

    // C5: transport failures and unexpected statuses are Unavailable.
    [Theory]
    [InlineData(401)]
    [InlineData(403)]
    [InlineData(404)]
    [InlineData(500)]
    public async Task C5_unexpected_http_status_is_unavailable(int status)
    {
        var client = CreateClient(new StubHandler((_, _) => Task.FromResult(Json((HttpStatusCode)status, """{"state":"ready"}"""))));

        var result = await client.GetIdentityAsync(CancellationToken.None);

        Assert.Equal(OnecIdentityFetchKind.Unavailable, result.Kind);
        Assert.Equal(status, result.HttpStatus);
    }

    [Fact]
    public async Task C5_http_request_exception_is_unavailable()
    {
        var client = CreateClient(new StubHandler((_, _) => throw new HttpRequestException("connection refused")));

        var result = await client.GetIdentityAsync(CancellationToken.None);

        Assert.Equal(OnecIdentityFetchKind.Unavailable, result.Kind);
        Assert.Null(result.HttpStatus);
        Assert.Contains("HttpRequestException", result.Detail, StringComparison.Ordinal);
    }

    [Fact]
    public async Task C5_timeout_without_caller_cancellation_is_unavailable()
    {
        var client = CreateClient(new StubHandler((_, _) => throw new TaskCanceledException("request timed out")));

        var result = await client.GetIdentityAsync(CancellationToken.None);

        Assert.Equal(OnecIdentityFetchKind.Unavailable, result.Kind);
        Assert.Null(result.HttpStatus);
        Assert.Contains("TaskCanceledException", result.Detail, StringComparison.Ordinal);
    }

    // C6: caller cancellation propagates; it is never classified Unavailable.
    [Fact]
    public async Task C6_caller_cancellation_propagates_as_operation_canceled()
    {
        var client = CreateClient(new StubHandler((_, _) => throw new TaskCanceledException("cancelled")));
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => client.GetIdentityAsync(cts.Token));
    }

    // C7: the request is GET {base}identity with a Basic auth header and no body.
    [Fact]
    public async Task C7_request_is_get_identity_with_basic_auth_and_no_body()
    {
        var handler = new StubHandler((_, _) => Task.FromResult(Json(HttpStatusCode.OK, ReadyBody)));
        var client = CreateClient(handler);

        await client.GetIdentityAsync(CancellationToken.None);

        var request = Assert.Single(handler.Requests);
        Assert.Equal(HttpMethod.Get, request.Method);
        Assert.Equal(new Uri(BaseAddress + "identity"), request.RequestUri);
        Assert.Equal("Basic", request.Headers.Authorization?.Scheme);
        Assert.Equal(Convert.ToBase64String(Encoding.UTF8.GetBytes("user:password")), request.Headers.Authorization?.Parameter);
        Assert.False(handler.LastRequestHadContent);
    }

    private static OnecIdentityClient CreateClient(HttpMessageHandler handler)
    {
        var http = new HttpClient(handler) { BaseAddress = new Uri(BaseAddress) };
        return new OnecIdentityClient(http, new OnecAuthentication(new StaticSecretStore(), Options.Create(new OnecOptions { CredentialSecretName = "test" })));
    }

    private static HttpResponseMessage Json(HttpStatusCode status, string body) =>
        new(status) { Content = new StringContent(body, Encoding.UTF8, "application/json") };

    private sealed class StaticSecretStore : ISecretStore
    {
        public Task SaveAsync(string name, string secret, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task<string?> ReadAsync(string name, CancellationToken cancellationToken) => Task.FromResult<string?>("user:password");
    }

    private sealed class StubHandler(Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> respond) : HttpMessageHandler
    {
        public List<HttpRequestMessage> Requests { get; } = [];
        public bool LastRequestHadContent { get; private set; }

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            Requests.Add(request);
            LastRequestHadContent = request.Content is not null;
            return respond(request, cancellationToken);
        }
    }

    private sealed class UnknownLengthContent(byte[] bytes) : HttpContent
    {
        protected override Task SerializeToStreamAsync(Stream stream, TransportContext? context) =>
            stream.WriteAsync(bytes, 0, bytes.Length);

        protected override Task SerializeToStreamAsync(Stream stream, TransportContext? context, CancellationToken cancellationToken) =>
            stream.WriteAsync(bytes, cancellationToken).AsTask();

        protected override bool TryComputeLength(out long length)
        {
            length = -1;
            return false;
        }
    }
}
