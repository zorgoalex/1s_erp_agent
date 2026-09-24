using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.RegularExpressions;
using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Application.Etl;
using ErpOnecAgent.Domain.Etl;

namespace ErpOnecAgent.Infrastructure.OneC;

public sealed partial class OnecODataClient(HttpClient httpClient, OnecAuthentication authentication) : IOnecODataClient
{
    [GeneratedRegex("^[\\p{L}\\p{N}_.$-]+$", RegexOptions.CultureInvariant)]
    private static partial Regex IdentifierPattern();

    public async IAsyncEnumerable<JsonElement> ReadEntityAsync(EtlEntityDefinition entity, EtlCursor? committedCursor, EtlCursor upperBound, bool full, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        Validate(entity);
        var skip = 0;
        var continuationMode = false;
        Uri? next = BuildInitialUri(entity, committedCursor, upperBound, full, skip);
        while (next is not null)
        {
            var currentUri = next.IsAbsoluteUri
                ? next
                : new Uri(httpClient.BaseAddress ?? throw new InvalidOperationException("OData BaseAddress is missing."), next);
            using var request = new HttpRequestMessage(HttpMethod.Get, next);
            request.Headers.TryAddWithoutValidation("Accept", "application/json");
            request.Headers.TryAddWithoutValidation("OData-Version", entity.ODataVersion.ToString(CultureInfo.InvariantCulture));
            await authentication.ApplyAsync(request, cancellationToken).ConfigureAwait(false);
            using var response = await httpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken).ConfigureAwait(false);
            response.EnsureSuccessStatusCode();
            await using var stream = await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
            using var document = await JsonDocument.ParseAsync(stream, cancellationToken: cancellationToken).ConfigureAwait(false);
            var values = FindResultsArray(document.RootElement);
            foreach (var value in values.EnumerateArray()) yield return value.Clone();
            var continuation = ResolveContinuation(document.RootElement, currentUri);
            if (continuation is not null)
            {
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
