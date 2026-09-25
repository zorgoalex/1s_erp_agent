using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.RegularExpressions;
using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Application.Configuration;
using ErpOnecAgent.Application.Etl;
using ErpOnecAgent.Domain.Etl;
using Microsoft.Extensions.Options;

namespace ErpOnecAgent.Infrastructure.OneC;

// A10: every page body is read through a byte-bounded stream (after decompression) and every
// record's JSON text is bounded, so a huge or runaway response fails the read with
// InvalidDataException instead of exhausting memory. The run then fails closed.
// A10c: a page that fails transiently (network, timeout, 408/429/5xx) is fetched again, a
// bounded number of times with doubling delay. The read is idempotent and the page is parsed
// completely before any of its rows is yielded, so a retry never duplicates or skips rows.
// Limit violations, malformed JSON, TLS failures, other HTTP statuses and caller cancellation
// are not retried. Each attempt (headers AND body) is bounded by the client timeout, so a
// stalled body is a retryable timeout instead of a hang. This is the ONLY retry layer of the
// OData client (no resilience handler is registered for it). A nextLink that repeats is
// refused instead of looping forever.
public sealed partial class OnecODataClient(HttpClient httpClient, OnecAuthentication authentication, IOptions<EtlOptions>? etlOptions = null) : IOnecODataClient
{
    private readonly long _maxPageBytes = etlOptions?.Value.MaxODataPageBytes ?? new EtlOptions().MaxODataPageBytes;
    private readonly int _maxRowBytes = etlOptions?.Value.MaxODataRowBytes ?? new EtlOptions().MaxODataRowBytes;
    private readonly int _pageRetries = etlOptions?.Value.ODataPageRetries ?? new EtlOptions().ODataPageRetries;
    private readonly int _retryBaseDelayMilliseconds = etlOptions?.Value.ODataRetryBaseDelayMilliseconds ?? new EtlOptions().ODataRetryBaseDelayMilliseconds;

    [GeneratedRegex("^[\\p{L}\\p{N}_.$-]+$", RegexOptions.CultureInvariant)]
    private static partial Regex IdentifierPattern();

    public async IAsyncEnumerable<JsonElement> ReadEntityAsync(EtlEntityDefinition entity, EtlCursor? committedCursor, EtlCursor upperBound, bool full, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        Validate(entity);
        var skip = 0;
        var continuationMode = false;
        var visitedContinuations = new HashSet<string>(StringComparer.Ordinal);
        Uri? next = BuildInitialUri(entity, committedCursor, upperBound, full, skip);
        visitedContinuations.Add(Absolute(next).AbsoluteUri);
        while (next is not null)
        {
            var currentUri = Absolute(next);
            using var document = await FetchPageAsync(next, entity, cancellationToken).ConfigureAwait(false);
            var values = FindResultsArray(document.RootElement);
            foreach (var value in values.EnumerateArray())
            {
                if (System.Runtime.InteropServices.JsonMarshal.GetRawUtf8Value(value).Length > _maxRowBytes)
                    throw new InvalidDataException($"An OData record of '{entity.EntityCode}' exceeds the {_maxRowBytes}-byte row limit.");
                yield return value.Clone();
            }
            var continuation = ResolveContinuation(document.RootElement, currentUri);
            if (continuation is not null)
            {
                if (!visitedContinuations.Add(continuation.AbsoluteUri))
                    throw new InvalidDataException($"OData nextLink of '{entity.EntityCode}' repeats ({continuation}); refusing to loop.");
                continuationMode = true;
                next = continuation;
                continue;
            }
            if (continuationMode)
            {
                next = null;
                continue;
            }
            if (values.GetArrayLength() >= entity.PageSize)
            {
                skip += values.GetArrayLength();
                next = BuildInitialUri(entity, committedCursor, upperBound, full, skip);
            }
            else
            {
                next = null;
            }
        }
    }

    private async Task<JsonDocument> FetchPageAsync(Uri uri, EtlEntityDefinition entity, CancellationToken cancellationToken)
    {
        for (var attempt = 0; ; attempt++)
        {
            try
            {
                return await FetchPageOnceAsync(uri, entity, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (attempt < _pageRetries && IsTransient(ex, cancellationToken))
            {
                var delay = TimeSpan.FromMilliseconds(Math.Min(60_000d, _retryBaseDelayMilliseconds * Math.Pow(2, attempt)));
                await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private Uri Absolute(Uri uri) => uri.IsAbsoluteUri
        ? uri
        : new Uri(httpClient.BaseAddress ?? throw new InvalidOperationException("OData BaseAddress is missing."), uri);

    private async Task<JsonDocument> FetchPageOnceAsync(Uri uri, EtlEntityDefinition entity, CancellationToken callerToken)
    {
        // HttpClient.Timeout ends at the response headers (ResponseHeadersRead); the linked
        // source bounds the body read too. Its expiry surfaces as an OperationCanceledException
        // while the caller's token is not cancelled, which IsTransient treats as a timeout.
        using var attempt = CancellationTokenSource.CreateLinkedTokenSource(callerToken);
        if (httpClient.Timeout != Timeout.InfiniteTimeSpan) attempt.CancelAfter(httpClient.Timeout);
        var cancellationToken = attempt.Token;
        using var request = new HttpRequestMessage(HttpMethod.Get, uri);
        request.Headers.TryAddWithoutValidation("Accept", "application/json");
        request.Headers.TryAddWithoutValidation("OData-Version", entity.ODataVersion.ToString(CultureInfo.InvariantCulture));
        await authentication.ApplyAsync(request, cancellationToken).ConfigureAwait(false);
        using var response = await httpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken).ConfigureAwait(false);
        response.EnsureSuccessStatusCode();
        if (response.Content.Headers.ContentLength is { } declared && declared > _maxPageBytes)
            throw new InvalidDataException($"OData page declares {declared} bytes, over the {_maxPageBytes}-byte limit.");
        await using var raw = await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
        await using var stream = new BoundedReadStream(raw, _maxPageBytes);
        return await JsonDocument.ParseAsync(stream, cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    /// <summary>A10c: failures worth another attempt of the same idempotent page read.</summary>
    internal static bool IsTransient(Exception ex, CancellationToken cancellationToken) => ex switch
    {
        OperationCanceledException => !cancellationToken.IsCancellationRequested, // HttpClient timeout, not the caller
        HttpRequestException { StatusCode: null, HttpRequestError: HttpRequestError.SecureConnectionError or HttpRequestError.ConfigurationLimitExceeded } => false,
        HttpRequestException { StatusCode: null } => true,                          // connection-level failure
        HttpRequestException { StatusCode: { } status } => (int)status is 408 or 429 or 500 or 502 or 503 or 504,
        InvalidDataException => false,                                              // A10 limits
        IOException => true,                                                        // body read interrupted
        _ => false
    };

    public async Task<bool> CheckAsync(CancellationToken cancellationToken)
    {
        try
        {
            using var request = new HttpRequestMessage(HttpMethod.Get, "$metadata");
            await authentication.ApplyAsync(request, cancellationToken).ConfigureAwait(false);
            using var response = await httpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken).ConfigureAwait(false);
            return response.IsSuccessStatusCode;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { throw; }
        catch (HttpRequestException) { return false; }
    }

    private static Uri BuildInitialUri(EtlEntityDefinition entity, EtlCursor? committed, EtlCursor upperBound, bool full, int skip)
    {
        var keyFields = entity.EffectiveKeyFields();
        var select = new HashSet<string>(entity.Select, StringComparer.Ordinal);
        foreach (var keyField in keyFields) select.Add(keyField);
        if (entity.UpdatedAtField is not null) select.Add(entity.UpdatedAtField);
        if (entity.DeletedField is not null) select.Add(entity.DeletedField);
        var query = new List<string>
        {
            "$select=" + Uri.EscapeDataString(string.Join(',', select.Order(StringComparer.Ordinal))),
            "$top=" + entity.PageSize.ToString(CultureInfo.InvariantCulture),
            "$skip=" + skip.ToString(CultureInfo.InvariantCulture)
        };
        IReadOnlyList<string> orderFields = entity.UpdatedAtField is null
            ? keyFields
            : [entity.UpdatedAtField, .. keyFields.Where(key => !string.Equals(key, entity.UpdatedAtField, StringComparison.Ordinal))];
        var order = string.Join(',', orderFields);
        query.Add("$orderby=" + Uri.EscapeDataString(order));
        if (entity.UpdatedAtField is not null)
        {
            var filters = new List<string>();
            if (!full && EtlCursorPolicy.QueryFrom(committed, entity.OverlapMinutes).UpdatedAtUtc is { } updated)
            {
                filters.Add($"{entity.UpdatedAtField} ge {FormatDate(entity.ODataVersion, entity.UpdatedAtEdmType, updated)}");
            }
            if (upperBound.UpdatedAtUtc is { } upper) filters.Add($"{entity.UpdatedAtField} le {FormatDate(entity.ODataVersion, entity.UpdatedAtEdmType, upper)}");
            if (filters.Count > 0) query.Add("$filter=" + Uri.EscapeDataString(string.Join(" and ", filters)));
        }
        return new Uri(entity.ODataPath + "?" + string.Join('&', query), UriKind.Relative);
    }

    private static string FormatDate(int odataVersion, string edmType, DateTimeOffset value)
    {
        var iso = value.UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss.fffffff'Z'", CultureInfo.InvariantCulture);
        if (odataVersion > 3) return iso;
        if (string.Equals(edmType, "Edm.DateTime", StringComparison.OrdinalIgnoreCase))
        {
            var dateTime = value.UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss.fffffff", CultureInfo.InvariantCulture);
            return $"datetime'{dateTime}'";
        }
        return $"datetimeoffset'{iso}'";
    }

    private static JsonElement FindResultsArray(JsonElement root)
    {
        if (root.ValueKind == JsonValueKind.Array) return root;
        if (root.TryGetProperty("value", out var value) && value.ValueKind == JsonValueKind.Array) return value;
        if (root.TryGetProperty("d", out var d))
        {
            if (d.ValueKind == JsonValueKind.Array) return d;
            if (d.ValueKind == JsonValueKind.Object && d.TryGetProperty("results", out var results) && results.ValueKind == JsonValueKind.Array) return results;
        }
        throw new InvalidDataException("Unsupported 1C OData response: records array was not found.");
    }

    private Uri? ResolveContinuation(JsonElement root, Uri currentRequestUri)
    {
        string? value = null;
        foreach (var propertyName in new[] { "@odata.nextLink", "odata.nextLink", "__next" })
            if (root.TryGetProperty(propertyName, out var link) && link.ValueKind == JsonValueKind.String) { value = link.GetString(); break; }
        if (value is null && root.TryGetProperty("d", out var d) && d.ValueKind == JsonValueKind.Object && d.TryGetProperty("__next", out var nested) && nested.ValueKind == JsonValueKind.String) value = nested.GetString();
        if (string.IsNullOrWhiteSpace(value)) return null;
        return ValidateContinuationLink(value, currentRequestUri);
    }

    private Uri ValidateContinuationLink(string value, Uri currentRequestUri)
    {
        var baseAddress = httpClient.BaseAddress ?? throw new InvalidOperationException("OData BaseAddress is missing.");
        if (!Uri.TryCreate(currentRequestUri, value, out var resolved))
            throw new InvalidDataException("OData continuation link is not a valid URI.");
        if (!string.Equals(resolved.Scheme, baseAddress.Scheme, StringComparison.OrdinalIgnoreCase) || !string.Equals(resolved.Authority, baseAddress.Authority, StringComparison.OrdinalIgnoreCase))
            throw new InvalidDataException("OData continuation link points outside configured 1C endpoint.");
        var basePath = baseAddress.AbsolutePath.EndsWith('/') ? baseAddress.AbsolutePath : baseAddress.AbsolutePath + "/";
        var resolvedPath = resolved.AbsolutePath.EndsWith('/') ? resolved.AbsolutePath : resolved.AbsolutePath + "/";
        if (!resolvedPath.StartsWith(basePath, StringComparison.Ordinal))
            throw new InvalidDataException("OData continuation link points outside configured OData service boundary.");
        return resolved;
    }

    private static void Validate(EtlEntityDefinition entity)
    {
        var keyFields = entity.EffectiveKeyFields();
        var identifiers = entity.Select.Append(entity.ODataPath).Concat(keyFields);
        if (entity.UpdatedAtField is not null) identifiers = identifiers.Append(entity.UpdatedAtField);
        if (entity.DeletedField is not null) identifiers = identifiers.Append(entity.DeletedField);
        if (identifiers.Any(value => !IdentifierPattern().IsMatch(value))) throw new InvalidDataException($"Unsafe OData identifier in entity '{entity.EntityCode}'.");
        if (keyFields.Any(string.IsNullOrWhiteSpace) || keyFields.Distinct(StringComparer.Ordinal).Count() != keyFields.Count || !keyFields.Contains(entity.KeyField, StringComparer.Ordinal)) throw new InvalidDataException($"Invalid OData key fields in entity '{entity.EntityCode}'.");
        if (entity.PageSize is < 1 or > 10_000) throw new InvalidDataException($"Invalid page size for entity '{entity.EntityCode}'.");
        if (entity.ODataVersion is < 3 or > 4) throw new InvalidDataException($"Unsupported OData version for entity '{entity.EntityCode}'.");
        if (entity.UpdatedAtField is not null && entity.UpdatedAtEdmType is not ("Edm.DateTime" or "Edm.DateTimeOffset")) throw new InvalidDataException($"Unsupported updated-at EDM type for entity '{entity.EntityCode}'.");
    }
}

/// <summary>A10: a read-only stream wrapper that throws once more than <c>limit</c> bytes are read.</summary>
internal sealed class BoundedReadStream(Stream inner, long limit) : Stream
{
    private long _read;

    public override bool CanRead => true;
    public override bool CanSeek => false;
    public override bool CanWrite => false;
    public override long Length => throw new NotSupportedException();
    public override long Position { get => _read; set => throw new NotSupportedException(); }
    public override void Flush() { }
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();
    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

    public override int Read(byte[] buffer, int offset, int count) => Count(inner.Read(buffer, offset, count));

    public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) =>
        Count(await inner.ReadAsync(buffer, cancellationToken).ConfigureAwait(false));

    public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
        ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

    private int Count(int read)
    {
        _read += read;
        if (_read > limit) throw new InvalidDataException($"OData page exceeds the {limit}-byte limit.");
        return read;
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing) inner.Dispose();
        base.Dispose(disposing);
    }
}
