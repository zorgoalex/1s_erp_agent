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

    private sealed class StaticSecretStore : ISecretStore
    {
        public Task SaveAsync(string name, string secret, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task<string?> ReadAsync(string name, CancellationToken cancellationToken) => Task.FromResult<string?>("user:password");
    }

    private sealed class StaticHandler(string body) : HttpMessageHandler
    {
        public string RequestBody { get; private set; } = string.Empty;
        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            RequestBody = await request.Content!.ReadAsStringAsync(cancellationToken);
            return new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(body, Encoding.UTF8, "application/json") };
        }
    }
}
