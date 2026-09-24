using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Application.Configuration;
using ErpOnecAgent.Contracts.Erp;
using ErpOnecAgent.Domain.Etl;
using Microsoft.Extensions.Options;

namespace ErpOnecAgent.Infrastructure.ErpApi;

public sealed class ErpClient : IErpClient
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);
    private readonly HttpClient _httpClient;
    private readonly AgentOptions _agent;

    /// <summary>DI constructor for the ordinary ERP channel (single-client scenarios).</summary>
    public ErpClient(HttpClient httpClient, IOptions<AgentOptions> agentOptions)
        : this(httpClient, agentOptions.Value, null, null) { }

    /// <summary>
    /// Creates the client with an optional dedicated long-poll channel (A06). When
    /// <paramref name="longPollSendAsync"/> is null, <c>commands/lease</c> falls back to the
    /// primary channel. The production registration supplies a dedicated long-poll channel so
    /// the lease uses its own timeout-safe pipeline while other calls keep the ordinary one.
    /// </summary>
    internal ErpClient(
        HttpClient httpClient,
        AgentOptions agentOptions,
        Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>>? sendAsync,
        Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>>? longPollSendAsync)
    {
        _httpClient = httpClient;
        _agent = agentOptions;
        _sendAsync = sendAsync ?? SendViaHttpClientAsync;
        _longPollSendAsync = longPollSendAsync ?? _sendAsync;
    }

    private readonly Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> _sendAsync;
    private readonly Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> _longPollSendAsync;

    public Task<SessionStartResponse> StartSessionAsync(SessionStartRequest request, CancellationToken cancellationToken) =>
        SendJsonAsync<SessionStartResponse>(HttpMethod.Post, "session/start", request, cancellationToken);

    public Task<LeaseResponse> LeaseCommandAsync(LeaseRequest request, CancellationToken cancellationToken) =>
        SendJsonAsync<LeaseResponse>(HttpMethod.Post, "commands/lease", request, cancellationToken, useLongPoll: true);

    public async Task AcknowledgeReceivedAsync(Guid commandId, CommandReceivedRequest request, CancellationToken cancellationToken) =>
        await SendNoContentAsync(HttpMethod.Post, $"commands/{commandId:D}/received", JsonContent.Create(request, options: JsonOptions), cancellationToken).ConfigureAwait(false);

    public async Task AcknowledgeResultAsync(Guid commandId, string resultJson, CancellationToken cancellationToken) =>
        await SendNoContentAsync(HttpMethod.Put, $"commands/{commandId:D}/result", new StringContent(resultJson, Encoding.UTF8, "application/json"), cancellationToken).ConfigureAwait(false);

    public async Task<BatchAcknowledgement> UploadBatchAsync(EtlBatch batch, Stream content, CancellationToken cancellationToken)
    {
        using var request = CreateRequest(HttpMethod.Post, "etl/batches");
        request.Headers.TryAddWithoutValidation("Idempotency-Key", batch.BatchId.ToString("D"));
        request.Headers.TryAddWithoutValidation("X-Content-SHA256", batch.Sha256);
        request.Headers.TryAddWithoutValidation("X-Batch-Id", batch.BatchId.ToString("D"));
        request.Headers.TryAddWithoutValidation("X-Run-Id", batch.RunId.ToString("D"));
        request.Headers.TryAddWithoutValidation("X-Entity", batch.EntityName);
        request.Headers.TryAddWithoutValidation("X-Schema-Version", batch.SchemaVersion.ToString(System.Globalization.CultureInfo.InvariantCulture));
        request.Headers.TryAddWithoutValidation("X-Row-Count", batch.RowCount.ToString(System.Globalization.CultureInfo.InvariantCulture));
        request.Content = new StreamContent(content);
        request.Content.Headers.ContentType = new("application/x-ndjson");
        request.Content.Headers.ContentEncoding.Add("gzip");
        using var response = await _sendAsync(request, cancellationToken).ConfigureAwait(false);
        await EnsureSuccessAsync(response, cancellationToken).ConfigureAwait(false);
        return await response.Content.ReadFromJsonAsync<BatchAcknowledgement>(JsonOptions, cancellationToken).ConfigureAwait(false)
            ?? throw new InvalidDataException("ERP returned an empty ETL acknowledgement.");
    }

    public async Task CompleteEtlRunAsync(Guid runId, object summary, CancellationToken cancellationToken) =>
        await SendNoContentAsync(HttpMethod.Post, $"etl/runs/{runId:D}/complete", JsonContent.Create(summary, options: JsonOptions), cancellationToken).ConfigureAwait(false);

    public async Task SendHeartbeatAsync(HeartbeatRequest request, CancellationToken cancellationToken) =>
        await SendNoContentAsync(HttpMethod.Post, "heartbeat", JsonContent.Create(request, options: JsonOptions), cancellationToken).ConfigureAwait(false);

    public async Task<RemoteConfigurationResponse?> GetConfigurationAsync(long currentVersion, CancellationToken cancellationToken)
    {
        using var request = CreateRequest(HttpMethod.Get, "configuration?currentVersion=" + currentVersion.ToString(System.Globalization.CultureInfo.InvariantCulture));
        using var response = await _sendAsync(request, cancellationToken).ConfigureAwait(false);
        if (response.StatusCode == System.Net.HttpStatusCode.NotModified) return null;
        await EnsureSuccessAsync(response, cancellationToken).ConfigureAwait(false);
        return await response.Content.ReadFromJsonAsync<RemoteConfigurationResponse>(JsonOptions, cancellationToken).ConfigureAwait(false) ?? throw new InvalidDataException("ERP returned an empty remote configuration.");
    }

    private async Task<T> SendJsonAsync<T>(HttpMethod method, string path, object body, CancellationToken cancellationToken, bool useLongPoll = false)
    {
        using var request = CreateRequest(method, path); request.Content = JsonContent.Create(body, options: JsonOptions);
        var send = useLongPoll ? _longPollSendAsync : _sendAsync;
        using var response = await send(request, cancellationToken).ConfigureAwait(false);
        await EnsureSuccessAsync(response, cancellationToken).ConfigureAwait(false);
        return await response.Content.ReadFromJsonAsync<T>(JsonOptions, cancellationToken).ConfigureAwait(false) ?? throw new InvalidDataException("ERP returned an empty JSON response.");
    }

    private async Task SendNoContentAsync(HttpMethod method, string path, HttpContent content, CancellationToken cancellationToken)
    {
        using var request = CreateRequest(method, path); request.Content = content;
        using var response = await _sendAsync(request, cancellationToken).ConfigureAwait(false);
        await EnsureSuccessAsync(response, cancellationToken).ConfigureAwait(false);
    }

    private Task<HttpResponseMessage> SendViaHttpClientAsync(HttpRequestMessage request, CancellationToken cancellationToken) =>
        _httpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken);

    private HttpRequestMessage CreateRequest(HttpMethod method, string path)
    {
        var request = new HttpRequestMessage(method, path);
        request.Headers.TryAddWithoutValidation("X-Agent-Id", _agent.AgentId);
        request.Headers.TryAddWithoutValidation("X-Agent-Version", ThisAssembly.Version);
        request.Headers.TryAddWithoutValidation("X-Site-Id", _agent.SiteId);
        request.Headers.TryAddWithoutValidation("X-Request-Id", Guid.NewGuid().ToString("D"));
        request.Headers.TryAddWithoutValidation("X-Correlation-Id", Guid.NewGuid().ToString("D"));
        return request;
    }

    private static async Task EnsureSuccessAsync(HttpResponseMessage response, CancellationToken cancellationToken)
    {
        if (response.IsSuccessStatusCode) return;
        var body = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
        if (body.Length > 2048) body = body[..2048];
        throw new HttpRequestException($"ERP API returned {(int)response.StatusCode}: {body}", null, response.StatusCode);
    }
}

internal static class ThisAssembly
{
    public static string Version { get; } = typeof(ThisAssembly).Assembly.GetName().Version?.ToString(3) ?? "1.0.0";
}
