using System.Net;
using System.Text;
using System.Text.Json;
using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Application.Configuration;
using ErpOnecAgent.Domain.Commands;
using ErpOnecAgent.Domain.Common;
using ErpOnecAgent.Infrastructure.OneC;
using Microsoft.Extensions.Options;
using Xunit;

namespace ErpOnecAgent.UnitTests;

public sealed class OnecCommandClientTests
{
    [Fact]
    public async Task Http_200_business_status_is_not_misclassified_as_success()
    {
        var id = Guid.NewGuid();
        var handler = new StaticHandler($$"""{"commandId":"{{id:D}}","status":"business_failed","error":{"code":"VALIDATION","message":"Rejected","retryable":false},"warnings":[],"resultVersion":1}""");
        var http = new HttpClient(handler) { BaseAddress = new Uri("http://127.0.0.1:5099/erp-integration/v1/") };
        var client = new OnecCommandClient(http, new OnecAuthentication(new StaticSecretStore(), Options.Create(new OnecOptions { CredentialSecretName = "test" })));
        using var payload = JsonDocument.Parse("{}");
        var created = DateTimeOffset.Parse("2026-08-30T10:00:00Z", System.Globalization.CultureInfo.InvariantCulture);
        var command = new CommandEnvelope(id, "create_customer_order", 1, 100, "order:1", null, created, null, null, null, PayloadHasher.Compute(payload.RootElement), payload.RootElement.Clone());

        var result = await client.ExecuteAsync(command, CancellationToken.None);

        Assert.Equal(OnecExecutionKind.BusinessError, result.Kind);
        Assert.Equal("VALIDATION", result.ErrorCode);
        Assert.Contains(created.ToString("yyyy-MM-dd", System.Globalization.CultureInfo.InvariantCulture), handler.RequestBody, StringComparison.Ordinal);
    }

    [Fact]
    public async Task The_command_body_is_sent_with_a_content_length_not_chunked()
    {
        var id = Guid.NewGuid();
        var handler = new StaticHandler($$"""{"commandId":"{{id:D}}","status":"succeeded","document":{"type":"integration_probe","ref":"t","marker":"m"},"warnings":[],"resultVersion":1}""");
        var http = new HttpClient(handler) { BaseAddress = new Uri("http://127.0.0.1:5099/erp-integration/v1/") };
        var client = new OnecCommandClient(http, new OnecAuthentication(new StaticSecretStore(), Options.Create(new OnecOptions { CredentialSecretName = "test" })));
        using var payload = JsonDocument.Parse("{\"marker\":\"m\"}");
        var command = new CommandEnvelope(id, "integration_probe", 1, 100, null, null, DateTimeOffset.UtcNow, null, null, null, PayloadHasher.Compute(payload.RootElement), payload.RootElement.Clone());

        await client.ExecuteAsync(command, CancellationToken.None);

        // The 1C extension's checks were all made with Content-Length; a chunked body is untested there.
        Assert.Equal(Encoding.UTF8.GetByteCount(handler.RequestBody), handler.RequestContentLength);
        Assert.NotEqual(true, handler.RequestChunked);
        Assert.Equal("application/json", handler.RequestMediaType);
        Assert.Equal("utf-8", handler.RequestCharset);
    }

    public static TheoryData<string> WorstCasePayloads()
    {
        // Canonical exactly 61440 bytes of escaped Cyrillic and HTML-sensitive text.
        var unit = "ж<&'+`";                     // 6 chars -> 6 x \uXXXX = 36 canonical bytes
        var prefix = "{\"s\":\"";
        var repeats = (61_440 - prefix.Length - 2) / 36;
        var text = string.Concat(Enumerable.Repeat(unit, repeats));
        var pad = new string('a', 61_440 - prefix.Length - 2 - repeats * 36);
        return new TheoryData<string>
        {
            prefix + text + pad + "\"}",
            string.Concat(Enumerable.Repeat("[", 31)) + "0" + string.Concat(Enumerable.Repeat("]", 31)),
            "[" + string.Join(',', Enumerable.Repeat("0", 4088)) + "]"
        };
    }

    [Theory]
    [MemberData(nameof(WorstCasePayloads))]
    public async Task A_payload_at_the_agreed_limits_produces_a_body_the_1c_parser_accepts(string payloadJson)
    {
        using var payload = JsonDocument.Parse(payloadJson);
        var command = new CommandEnvelope(Guid.NewGuid(), "create_customer_order", 1, 100, null, null, DateTimeOffset.UtcNow, null, null, null, PayloadHasher.Compute(payload.RootElement), payload.RootElement.Clone());
        Assert.True(ErpOnecAgent.Application.Commands.CommandValidator.Validate(command, ["create_customer_order"], 61_440).IsValid);
        var handler = new StaticHandler($$"""{"commandId":"{{command.CommandId:D}}","status":"succeeded","document":{},"warnings":[],"resultVersion":1}""");
        var http = new HttpClient(handler) { BaseAddress = new Uri("http://127.0.0.1:5099/erp-integration/v1/") };
        var client = new OnecCommandClient(http, new OnecAuthentication(new StaticSecretStore(), Options.Create(new OnecOptions { CredentialSecretName = "test" })));

        await client.ExecuteAsync(command, CancellationToken.None);

        // The 1C extension's BSL parser: body ≤ 65536 bytes, value depth ≤ 32 (root = 0),
        // values ≤ 4096 (scalars included, property names excluded), ≤ 128 pairs per object.
        Assert.True(Encoding.UTF8.GetByteCount(handler.RequestBody) <= 65_536, $"body is {Encoding.UTF8.GetByteCount(handler.RequestBody)} bytes");
        using var body = JsonDocument.Parse(handler.RequestBody);
        var nodes = 0;
        var maxDepth = 0;
        Measure(body.RootElement, 0, ref nodes, ref maxDepth);
        Assert.True(maxDepth <= 32, $"body depth {maxDepth}");
        Assert.True(nodes <= 4096, $"body nodes {nodes}");
    }

    private static void Measure(JsonElement value, int depth, ref int nodes, ref int maxDepth)
    {
        nodes++;
        maxDepth = Math.Max(maxDepth, depth);
        if (value.ValueKind == JsonValueKind.Object) foreach (var property in value.EnumerateObject()) Measure(property.Value, depth + 1, ref nodes, ref maxDepth);
        else if (value.ValueKind == JsonValueKind.Array) foreach (var item in value.EnumerateArray()) Measure(item, depth + 1, ref nodes, ref maxDepth);
    }

    private sealed class StaticSecretStore : ISecretStore
    {
        public Task SaveAsync(string name, string secret, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task<string?> ReadAsync(string name, CancellationToken cancellationToken) => Task.FromResult<string?>("user:password");
    }

    private sealed class StaticHandler(string body) : HttpMessageHandler
    {
        public string RequestBody { get; private set; } = string.Empty;
        public long? RequestContentLength { get; private set; }
        public bool? RequestChunked { get; private set; }
        public string? RequestMediaType { get; private set; }
        public string? RequestCharset { get; private set; }
        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            // Captured before the body is read: this is what the transport would put on the wire.
            RequestContentLength = request.Content!.Headers.ContentLength;
            RequestChunked = request.Headers.TransferEncodingChunked;
            RequestMediaType = request.Content.Headers.ContentType?.MediaType;
            RequestCharset = request.Content.Headers.ContentType?.CharSet;
            RequestBody = await request.Content!.ReadAsStringAsync(cancellationToken);
            return new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(body, Encoding.UTF8, "application/json") };
        }
    }
}
