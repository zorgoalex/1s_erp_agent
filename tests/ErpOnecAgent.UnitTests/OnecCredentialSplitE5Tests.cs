using System.Net;
using System.Text;
using System.Text.Json;
using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Application.Configuration;
using ErpOnecAgent.Infrastructure.OneC;
using Microsoft.Extensions.Options;
using Xunit;

namespace ErpOnecAgent.UnitTests;

/// <summary>E5: commands run under their own 1C credential; OData/health/identity keep the read-only one.</summary>
public sealed class OnecCredentialSplitE5Tests
{
    private static readonly Dictionary<string, string> Secrets = new(StringComparer.Ordinal)
    {
        ["onec-main"] = "{\"username\":\"odata-reader\",\"password\":\"r\"}",
        ["onec-command"] = "{\"username\":\"command-writer\",\"password\":\"w\"}"
    };

    [Fact]
    public async Task Command_calls_use_the_command_credential_and_reads_use_the_read_credential()
    {
        var authentication = new OnecAuthentication(new MapSecretStore(Secrets), Options.Create(new OnecOptions { CredentialSecretName = "onec-main", CommandCredentialSecretName = "onec-command" }));
        using var read = new HttpRequestMessage(HttpMethod.Get, "https://onec.test/odata/x");
        using var command = new HttpRequestMessage(HttpMethod.Post, "https://onec.test/hs/x");

        await authentication.ApplyAsync(read, CancellationToken.None);
        await authentication.ApplyCommandAsync(command, CancellationToken.None);

        Assert.Equal("odata-reader", User(read));
        Assert.Equal("command-writer", User(command));
    }

    [Fact]
    public async Task Without_a_command_secret_commands_fall_back_to_the_single_credential()
    {
        var authentication = new OnecAuthentication(new MapSecretStore(Secrets), Options.Create(new OnecOptions { CredentialSecretName = "onec-main" }));
        using var command = new HttpRequestMessage(HttpMethod.Post, "https://onec.test/hs/x");

        await authentication.ApplyCommandAsync(command, CancellationToken.None);

        Assert.Equal("odata-reader", User(command));
        Assert.Equal("onec-main", new OnecOptions { CommandCredentialSecretName = " " }.EffectiveCommandCredentialSecretName);
    }

    [Fact]
    public async Task A_missing_command_secret_fails_the_call_naming_the_secret()
    {
        var authentication = new OnecAuthentication(new MapSecretStore(Secrets), Options.Create(new OnecOptions { CredentialSecretName = "onec-main", CommandCredentialSecretName = "onec-absent" }));
        using var command = new HttpRequestMessage(HttpMethod.Post, "https://onec.test/hs/x");

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => authentication.ApplyCommandAsync(command, CancellationToken.None));

        Assert.Contains("onec-absent", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task The_command_client_sends_execute_and_status_under_the_command_credential()
    {
        var handler = new CapturingHandler();
        using var http = new HttpClient(handler) { BaseAddress = new Uri("https://onec.test/hs/erp-integration/v1/") };
        var client = new OnecCommandClient(http, new OnecAuthentication(new MapSecretStore(Secrets), Options.Create(new OnecOptions { CredentialSecretName = "onec-main", CommandCredentialSecretName = "onec-command" })));
        using var document = JsonDocument.Parse("{\"amount\":1}");
        var payload = document.RootElement.Clone();
        var envelope = new ErpOnecAgent.Domain.Commands.CommandEnvelope(Guid.NewGuid(), "synthetic", 1, 100, null, null, DateTimeOffset.UtcNow, null, null, null, ErpOnecAgent.Domain.Common.PayloadHasher.Compute(payload), payload);

        await client.ExecuteAsync(envelope, CancellationToken.None);
        await client.GetStatusAsync(envelope.CommandId, CancellationToken.None);

        Assert.Equal(2, handler.Users.Count);
        Assert.All(handler.Users, static user => Assert.Equal("command-writer", user));
    }

    private static string User(HttpRequestMessage request) =>
        Encoding.UTF8.GetString(Convert.FromBase64String(request.Headers.Authorization!.Parameter!)).Split(':')[0];

    private sealed class MapSecretStore(Dictionary<string, string> secrets) : ISecretStore
    {
        public Task<string?> ReadAsync(string name, CancellationToken cancellationToken) => Task.FromResult(secrets.TryGetValue(name, out var value) ? value : null);
        public Task SaveAsync(string name, string value, CancellationToken cancellationToken) => throw new NotSupportedException();
    }

    private sealed class CapturingHandler : HttpMessageHandler
    {
        public List<string> Users { get; } = [];

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            Users.Add(User(request));
            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.ServiceUnavailable) { Content = new StringContent("{}", Encoding.UTF8, "application/json") });
        }
    }
}
