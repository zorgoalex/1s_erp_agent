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
        Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>>? longPollSendAsync,
        Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>>? transferSendAsync = null)
    {
        _httpClient = httpClient;
        _agent = agentOptions;
        _sendAsync = sendAsync ?? SendViaHttpClientAsync;
        _longPollSendAsync = longPollSendAsync ?? _sendAsync;
        _transferSendAsync = transferSendAsync ?? _sendAsync;
    }

    private readonly Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> _sendAsync;
    private readonly Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> _longPollSendAsync;
    // Batch upload and run completion: the single-attempt ETL transfer channel.
    private readonly Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> _transferSendAsync;

    public Task<SessionStartResponse> StartSessionAsync(SessionStartRequest request, CancellationToken cancellationToken) =>
        SendJsonAsync<SessionStartResponse>(HttpMethod.Post, "session/start", request, cancellationToken);

    public Task<LeaseResponse> LeaseCommandAsync(LeaseRequest request, CancellationToken cancellationToken) =>
        SendJsonAsync<LeaseResponse>(HttpMethod.Post, "commands/lease", request, cancellationToken, useLongPoll: true);

    public async Task AcknowledgeReceivedAsync(Guid commandId, CommandReceivedRequest request, CancellationToken cancellationToken) =>
        await SendNoContentAsync(HttpMethod.Post, $"commands/{commandId:D}/received", JsonContent.Create(request, options: JsonOptions), cancellationToken).ConfigureAwait(false);

    public async Task AcknowledgeResultAsync(Guid commandId, string resultJson, CancellationToken cancellationToken) =>
        await SendNoContentAsync(HttpMethod.Put, $"commands/{commandId:D}/result", new StringContent(resultJson, Encoding.UTF8, "application/json"), cancellationToken).ConfigureAwait(false);

    public async Task<BatchAcknowledgement> UploadBatchAsync(EtlBatch batch, Stream content, CancellationToken cancellationToken) =>
        (await UploadBatchWithEvidenceAsync(batch, content, cancellationToken).ConfigureAwait(false)).Ack;

    public Task<BatchUploadResponse> UploadBatchWithEvidenceAsync(EtlBatch batch, Stream content, CancellationToken cancellationToken) =>
        UploadBatchWithEvidenceAsync(batch, new EtlBatchSourceLabels(null, null), content, cancellationToken);

    public async Task<BatchUploadResponse> UploadBatchWithEvidenceAsync(EtlBatch batch, EtlBatchSourceLabels labels, Stream content, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(labels);
        using var request = CreateRequest(HttpMethod.Post, "etl/batches");
        request.Headers.TryAddWithoutValidation("Idempotency-Key", batch.BatchId.ToString("D"));
        request.Headers.TryAddWithoutValidation("X-Content-SHA256", batch.Sha256);
        request.Headers.TryAddWithoutValidation("X-Batch-Id", batch.BatchId.ToString("D"));
        request.Headers.TryAddWithoutValidation("X-Run-Id", batch.RunId.ToString("D"));
        request.Headers.TryAddWithoutValidation("X-Entity", batch.EntityName);
        request.Headers.TryAddWithoutValidation("X-Schema-Version", batch.SchemaVersion.ToString(System.Globalization.CultureInfo.InvariantCulture));
        request.Headers.TryAddWithoutValidation("X-Row-Count", batch.RowCount.ToString(System.Globalization.CultureInfo.InvariantCulture));
        // E3: labels are validated where they are stored (canonical namespace, printable-ASCII
        // token), so they are header-safe; runs created before a label existed send none.
        if (labels.SourceNamespace is not null) request.Headers.TryAddWithoutValidation("X-Source-Namespace", labels.SourceNamespace);
        if (labels.SourceGeneration is not null) request.Headers.TryAddWithoutValidation("X-Source-Generation", labels.SourceGeneration);
        request.Content = new StreamContent(content);
        request.Content.Headers.ContentType = new("application/x-ndjson");
        request.Content.Headers.ContentEncoding.Add("gzip");
        using var response = await _transferSendAsync(request, cancellationToken).ConfigureAwait(false);
        await EnsureSuccessAsync(response, cancellationToken).ConfigureAwait(false);
        var body = await response.Content.ReadAsByteArrayAsync(cancellationToken).ConfigureAwait(false);
        var ack = JsonSerializer.Deserialize<BatchAcknowledgement>(body, JsonOptions)
            ?? throw new InvalidDataException("ERP returned an empty ETL acknowledgement.");
        return new BatchUploadResponse(ack, body, (int)response.StatusCode);
    }

    public async Task CompleteEtlRunAsync(Guid runId, object summary, CancellationToken cancellationToken) =>
        await CompleteEtlRunRawAsync(runId, JsonSerializer.Serialize(summary, JsonOptions), cancellationToken).ConfigureAwait(false);

    /// <inheritdoc cref="IErpClient.CompleteEtlRunRawAsync"/>
    public async Task CompleteEtlRunRawAsync(Guid runId, string completePayloadJson, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(completePayloadJson);
        using var request = CreateRequest(HttpMethod.Post, $"etl/runs/{runId:D}/complete");
        request.Headers.TryAddWithoutValidation("Idempotency-Key", runId.ToString("D"));
        // ByteArrayContent, not StringContent: the exact stored bytes, UTF-8 without BOM,
        // with an explicit media type — nothing is re-encoded or re-serialized.
        // throwOnInvalidBytes: a lone surrogate fails closed instead of silently becoming U+FFFD.
        request.Content = new ByteArrayContent(new UTF8Encoding(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true).GetBytes(completePayloadJson));
        request.Content.Headers.ContentType = new("application/json") { CharSet = "utf-8" };
        using var response = await _transferSendAsync(request, cancellationToken).ConfigureAwait(false);
        await EnsureSuccessAsync(response, cancellationToken).ConfigureAwait(false);
    }

    public async Task SendHeartbeatAsync(HeartbeatRequest request, CancellationToken cancellationToken) =>
        await SendNoContentAsync(HttpMethod.Post, "heartbeat", JsonContent.Create(request, options: JsonOptions), cancellationToken).ConfigureAwait(false);

    public async Task<EtlBatchRemoteStatus> GetBatchStatusAsync(Guid batchId, CancellationToken cancellationToken)
    {
        using var request = CreateRequest(HttpMethod.Get, $"etl/batches/{batchId:D}");
        using var response = await _sendAsync(request, cancellationToken).ConfigureAwait(false);
        if (response.StatusCode == System.Net.HttpStatusCode.NotFound) return new EtlBatchRemoteStatus(false, null);
        await EnsureSuccessAsync(response, cancellationToken).ConfigureAwait(false);
        var ack = await response.Content.ReadFromJsonAsync<BatchAcknowledgement>(JsonOptions, cancellationToken).ConfigureAwait(false)
            ?? throw new InvalidDataException("ERP returned an empty batch acknowledgement.");
        if (ack.BatchId != batchId) throw new InvalidDataException("ERP returned the acknowledgement of a different batch.");
        return new EtlBatchRemoteStatus(true, ack);
    }

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

    // Stage 6 redaction: the ERP response body is never copied into the exception message,
    // which is logged and may be persisted (e.g. as a completion retry error); only the status
    // and the agreed machine-readable code are reported.
    /// <summary>The prefix of an error body read to find <c>ApiError.code</c>; the rest is never read.</summary>
    internal const int MaxErrorBodyBytes = 4096;

    /// <summary>Bound on reading an error body: after ResponseHeadersRead no HTTP timeout applies.</summary>
    internal static readonly TimeSpan ErrorBodyReadTimeout = TimeSpan.FromSeconds(5);

    private static async Task EnsureSuccessAsync(HttpResponseMessage response, CancellationToken cancellationToken)
    {
        if (response.IsSuccessStatusCode) return;
        // Only the agreed machine-readable code is extracted (bounded read, strict shape); the
        // body is never logged or kept — it may carry ERP data.
        var code = await ReadApiErrorCodeAsync(response, cancellationToken).ConfigureAwait(false);
        var retryAfter = response.Headers.RetryAfter switch
        {
            { Delta: { } delta } => delta,
            { Date: { } date } => date - DateTimeOffset.UtcNow,
            _ => (TimeSpan?)null
        };
        throw new ErpApiException(response.StatusCode, code, retryAfter is { } value && value > TimeSpan.Zero ? value : null, RequestIdOf(response));
    }

    // ERP's echo first, else the id this agent sent; bounded to a plain token so a log line cannot be forged.
    private static string? RequestIdOf(HttpResponseMessage response)
    {
        var value = response.Headers.TryGetValues("X-Request-Id", out var echoed) ? echoed.FirstOrDefault()
            : response.RequestMessage?.Headers.TryGetValues("X-Request-Id", out var sent) == true ? sent.FirstOrDefault() : null;
        return value is { Length: > 0 and <= 128 } && value.All(static c => char.IsAsciiLetterOrDigit(c) || c is '-' or '_' or '.' or ':') ? value : null;
    }

    private static async Task<string?> ReadApiErrorCodeAsync(HttpResponseMessage response, CancellationToken cancellationToken)
    {
        using var bounded = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        bounded.CancelAfter(ErrorBodyReadTimeout);
        try
        {
            await using var stream = await response.Content.ReadAsStreamAsync(bounded.Token).ConfigureAwait(false);
            var buffer = new byte[MaxErrorBodyBytes];
            var read = 0;
            while (read < buffer.Length)
            {
                var n = await stream.ReadAsync(buffer.AsMemory(read), bounded.Token).ConfigureAwait(false);
                if (n == 0) break;
                read += n;
            }
            return ExtractCode(buffer.AsSpan(0, read), isFinalBlock: read < buffer.Length);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            return null; // a stalled error body: no code, the status still decides
        }
        catch (Exception ex) when (ex is IOException or HttpRequestException or ArgumentException)
        {
            return null;
        }
    }

    /// <summary>
    /// Streams the body prefix and returns the first top-level string property <c>code</c> when
    /// it is well formed ([A-Z0-9_]{1,64}). A prefix cut inside a later property is fine: the code
    /// is usually first, and a truncated tail is simply the end of what is looked at.
    /// </summary>
    internal static string? ExtractCode(ReadOnlySpan<byte> prefix, bool isFinalBlock)
    {
        var reader = new Utf8JsonReader(prefix, isFinalBlock, default);
        try
        {
            if (!reader.Read() || reader.TokenType != JsonTokenType.StartObject) return null;
            while (reader.Read())
            {
                if (reader.TokenType == JsonTokenType.EndObject) return null;
                if (reader.TokenType != JsonTokenType.PropertyName) return null;
                var isCode = reader.ValueTextEquals("code"u8);
                if (!reader.Read()) return null;
                if (isCode)
                {
                    if (reader.TokenType != JsonTokenType.String) return null;
                    var code = reader.GetString();
                    return code is { Length: >= 1 and <= 64 } && code.All(static c => c is >= 'A' and <= 'Z' or >= '0' and <= '9' or '_') ? code : null;
                }
                if (reader.TokenType is JsonTokenType.StartObject or JsonTokenType.StartArray && !reader.TrySkip()) return null;
            }
            return null;
        }
        catch (JsonException)
        {
            return null;
        }
    }
}

internal static class ThisAssembly
{
    public static string Version { get; } = typeof(ThisAssembly).Assembly.GetName().Version?.ToString(3) ?? "1.0.0";
}
