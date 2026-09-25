using System.Text.Json;
using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Application.Etl;

namespace ErpOnecAgent.Infrastructure.OneC;

// S1: GET {CommandApiBaseUrl}/identity of the ERPIntegration.Core extension (0.3.0+).
// Contract (repo_1c_extension docs/identity-server-report-2026-09-25.md):
//   200 → {protocolVersion:1, state:"ready", scope:"whole-infobase", databaseId, exportEpoch,
//          environment:"test"|"production"}
//   503 → the same shape with state "uninitialized"|"blocked" and null UUIDs/environment.
//   503 → {"status":"error","error":{"code":"STORAGE_UNAVAILABLE",...}} when the register
//          itself cannot be read — a retryable outage, as is any 503 without a recognized
//          identity state (e.g. a proxy error page).
// A 200 body is parsed strictly and case-sensitively: an ambiguous or incomplete body is
// Malformed and never trusted. The whole call — headers AND body — is bounded by the
// client timeout. Every failure is classified; the call throws only on caller cancellation.
public sealed class OnecIdentityClient(HttpClient httpClient, OnecAuthentication authentication) : IOnecIdentityClient
{
    internal const int MaxBodyBytes = 16 * 1024;
    internal static readonly TimeSpan DefaultCallTimeout = TimeSpan.FromSeconds(30);

    public async Task<OnecIdentityFetchResult> GetIdentityAsync(CancellationToken cancellationToken)
    {
        using var request = new HttpRequestMessage(HttpMethod.Get, "identity");
        try
        {
            await authentication.ApplyAsync(request, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
        {
            // Missing or unreadable credentials: the source cannot be verified; the run does not start.
            return OnecIdentityFetchResult.Unavailable(null, $"1C credentials unavailable: {ex.GetType().Name}.");
        }

        // ResponseHeadersRead disables HttpClient.Timeout once headers arrive, so the body read
        // gets its own deadline through a linked token.
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        deadline.CancelAfter(httpClient.Timeout > TimeSpan.Zero && httpClient.Timeout != Timeout.InfiniteTimeSpan ? httpClient.Timeout : DefaultCallTimeout);
        try
        {
            using var response = await httpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, deadline.Token).ConfigureAwait(false);
            var status = (int)response.StatusCode;
            if (status is not (200 or 503))
                return OnecIdentityFetchResult.Unavailable(status, $"Unexpected identity HTTP status {status}.");

            byte[] body;
            try
            {
                body = await ReadBoundedAsync(response, deadline.Token).ConfigureAwait(false);
            }
            catch (InvalidDataException)
            {
                return status == 200
                    ? OnecIdentityFetchResult.Malformed(status, "Identity body exceeds the size limit.")
                    : OnecIdentityFetchResult.Unavailable(status, "Oversized 503 body.");
            }
            return Parse(status, body);
        }
        catch (Exception ex) when ((ex is HttpRequestException or OperationCanceledException or IOException) && !cancellationToken.IsCancellationRequested)
        {
            return OnecIdentityFetchResult.Unavailable(null, $"Identity request failed: {ex.GetType().Name}.");
        }
    }

    internal static OnecIdentityFetchResult Parse(int status, byte[] body)
    {
        JsonDocument document;
        try
        {
            document = JsonDocument.Parse(body);
        }
        catch (JsonException)
        {
            return status == 503
                ? OnecIdentityFetchResult.Unavailable(status, "503 without an identity body.")
                : OnecIdentityFetchResult.Malformed(status, "Identity body is not valid JSON.");
        }

        using (document)
        {
            var root = document.RootElement;
            if (status == 503)
            {
                // Only an unambiguous identity body with a recognized state is NotReady; any
                // other 503 (storage failure, proxy page) is an outage.
                var state503 = root.ValueKind == JsonValueKind.Object && !HasDuplicateOrCaseVariantNames(root) ? ReadString(root, "state") : null;
                return state503 is "uninitialized" or "blocked"
                    ? OnecIdentityFetchResult.NotReady(status, state503)
                    : OnecIdentityFetchResult.Unavailable(status, "503 without a recognized identity state.");
            }

            if (root.ValueKind != JsonValueKind.Object) return OnecIdentityFetchResult.Malformed(status, "Identity body is not an object.");
            if (HasDuplicateOrCaseVariantNames(root)) return OnecIdentityFetchResult.Malformed(status, "Identity body has duplicate or case-variant property names.");
            var state = ReadString(root, "state");
            if (!root.TryGetProperty("protocolVersion", out var version) || version.ValueKind != JsonValueKind.Number || !version.TryGetInt32(out var protocolVersion))
                return OnecIdentityFetchResult.Malformed(status, "protocolVersion is missing or not an integer.");
            if (protocolVersion != 1) return OnecIdentityFetchResult.Malformed(status, $"Unsupported identity protocolVersion {protocolVersion}.");
            if (state != "ready") return state is "uninitialized" or "blocked"
                ? OnecIdentityFetchResult.NotReady(status, state)
                : OnecIdentityFetchResult.Malformed(status, "200 identity body is not in state 'ready'.");
            var scope = ReadString(root, "scope");
            var environment = ReadString(root, "environment");
            if (scope is null || environment is not ("test" or "production"))
                return OnecIdentityFetchResult.Malformed(status, "scope or environment is missing or invalid.");
            if (!TryReadGuid(root, "databaseId", out var databaseId) || !TryReadGuid(root, "exportEpoch", out var exportEpoch))
                return OnecIdentityFetchResult.Malformed(status, "databaseId or exportEpoch is not a non-empty UUID.");
            return OnecIdentityFetchResult.Ready(new OnecSourceIdentity(protocolVersion, state, scope, databaseId, exportEpoch, environment));
        }
    }

    private static async Task<byte[]> ReadBoundedAsync(HttpResponseMessage response, CancellationToken cancellationToken)
    {
        if (response.Content.Headers.ContentLength is > MaxBodyBytes) throw new InvalidDataException("Identity body too large.");
        await using var stream = await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
        using var buffer = new MemoryStream();
        var chunk = new byte[4096];
        int read;
        while ((read = await stream.ReadAsync(chunk, cancellationToken).ConfigureAwait(false)) > 0)
        {
            if (buffer.Length + read > MaxBodyBytes) throw new InvalidDataException("Identity body too large.");
            buffer.Write(chunk, 0, read);
        }
        return buffer.ToArray();
    }

    private static bool HasDuplicateOrCaseVariantNames(JsonElement root)
    {
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var property in root.EnumerateObject())
        {
            if (!seen.Add(property.Name)) return true;
        }
        return false;
    }

    private static string? ReadString(JsonElement root, string name) =>
        root.TryGetProperty(name, out var value) && value.ValueKind == JsonValueKind.String ? value.GetString() : null;

    private static bool TryReadGuid(JsonElement root, string name, out Guid value)
    {
        value = Guid.Empty;
        var text = ReadString(root, name);
        return text is not null && Guid.TryParseExact(text, "D", out value) && value != Guid.Empty;
    }
}
