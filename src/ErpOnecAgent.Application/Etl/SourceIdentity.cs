using ErpOnecAgent.Application.Configuration;

namespace ErpOnecAgent.Application.Etl;

// S1: source identity of the 1C infobase (extension GET /v1/identity) and the stable,
// non-secret ETL source namespace derived from the configured binding
// (spec_1c-agent/reviews/source-identity-vs-source-namespace-2026-09-25.md §4).
// Identity is an administrative guarantee, not cryptographic proof: a byte clone with the
// same UUIDs is not detected. The namespace includes exportEpoch, so a backup restore or
// epoch rotation deliberately changes the cursor domain (DOMAIN_CHANGED) and requires a
// new baseline.

/// <summary>The identity reported by a ready source.</summary>
public sealed record OnecSourceIdentity(int ProtocolVersion, string State, string Scope, Guid DatabaseId, Guid ExportEpoch, string Environment);

public enum OnecIdentityFetchKind
{
    /// <summary>200 with a well-formed ready identity.</summary>
    Ready,
    /// <summary>The source answered but is not ready (503 uninitialized/blocked).</summary>
    NotReady,
    /// <summary>Transport failure, timeout, or an unexpected HTTP status.</summary>
    Unavailable,
    /// <summary>A body that violates the identity contract (never trusted).</summary>
    Malformed
}

public sealed record OnecIdentityFetchResult(OnecIdentityFetchKind Kind, OnecSourceIdentity? Identity, int? HttpStatus, string? State, string? Detail)
{
    public static OnecIdentityFetchResult Ready(OnecSourceIdentity identity) => new(OnecIdentityFetchKind.Ready, identity, 200, identity.State, null);
    public static OnecIdentityFetchResult NotReady(int httpStatus, string? state) => new(OnecIdentityFetchKind.NotReady, null, httpStatus, state, null);
    public static OnecIdentityFetchResult Unavailable(int? httpStatus, string detail) => new(OnecIdentityFetchKind.Unavailable, null, httpStatus, null, detail);
    public static OnecIdentityFetchResult Malformed(int? httpStatus, string detail) => new(OnecIdentityFetchKind.Malformed, null, httpStatus, null, detail);
}

public enum SourceIdentityVerdict
{
    Match,
    BindingMissing,
    BindingInvalid,
    EndpointMismatch,
    Unavailable,
    NotReady,
    Malformed,
    ScopeUnsupported,
    DatabaseMismatch,
    EpochMismatch,
    EnvironmentMismatch
}

/// <summary>Outcome of verifying the live source against the configured binding. <see cref="SourceNamespace"/> is set only for <see cref="SourceIdentityVerdict.Match"/>.</summary>
public sealed record SourceIdentityCheck(SourceIdentityVerdict Verdict, string? SourceNamespace, string Detail)
{
    public bool IsMatch => Verdict == SourceIdentityVerdict.Match;
}

/// <summary>A validated binding: parsed UUIDs, normalized environment and endpoint.</summary>
public sealed record OnecSourceBinding(Guid DatabaseId, Guid ExportEpoch, string Environment, string ODataEndpoint)
{
    public const string NamespacePrefix = "1c-identity:v1:";

    /// <summary>The stable non-secret ETL source namespace: <c>1c-identity:v1:{databaseId}:{exportEpoch}:{environment}</c>.</summary>
    public string SourceNamespace => $"{NamespacePrefix}{DatabaseId:D}:{ExportEpoch:D}:{Environment}";

    /// <summary>Parses configured options; returns null with a reason when the binding is absent or invalid.</summary>
    public static OnecSourceBinding? TryCreate(OnecSourceBindingOptions? options, out string reason)
    {
        if (options is null) { reason = "No OneC:SourceBinding is configured."; return null; }
        if (!Guid.TryParseExact(options.DatabaseId, "D", out var databaseId) || databaseId == Guid.Empty)
        { reason = "SourceBinding.DatabaseId must be a non-empty UUID in D format."; return null; }
        if (!Guid.TryParseExact(options.ExportEpoch, "D", out var exportEpoch) || exportEpoch == Guid.Empty)
        { reason = "SourceBinding.ExportEpoch must be a non-empty UUID in D format."; return null; }
        if (options.Environment is not ("test" or "production"))
        { reason = "SourceBinding.Environment must be 'test' or 'production'."; return null; }
        var endpoint = SourceEndpoint.Normalize(options.ODataEndpoint);
        if (endpoint is null)
        { reason = "SourceBinding.ODataEndpoint must be an absolute http(s) URL without credentials, query or fragment."; return null; }
        reason = string.Empty;
        return new OnecSourceBinding(databaseId, exportEpoch, options.Environment, endpoint);
    }
}

/// <summary>Normalization of an OData service root for binding comparison.</summary>
public static class SourceEndpoint
{
    /// <summary>
    /// scheme and host lower-cased, default port elided, path kept case-sensitive with a
    /// single trailing slash removed. Credentials, query and fragment are rejected (null).
    /// </summary>
    public static string? Normalize(string? url)
    {
        if (string.IsNullOrWhiteSpace(url) || !Uri.TryCreate(url.Trim(), UriKind.Absolute, out var uri)) return null;
        if (uri.Scheme is not ("http" or "https")) return null;
        if (!string.IsNullOrEmpty(uri.UserInfo) || !string.IsNullOrEmpty(uri.Query) || !string.IsNullOrEmpty(uri.Fragment)) return null;
        var port = uri.IsDefaultPort ? string.Empty : $":{uri.Port}";
        var path = uri.AbsolutePath.Length > 1 && uri.AbsolutePath.EndsWith('/') ? uri.AbsolutePath[..^1] : uri.AbsolutePath;
        if (path == "/") path = string.Empty;
        return $"{uri.Scheme}://{uri.Host.ToLowerInvariant()}{port}{path}";
    }

    /// <summary>
    /// True when the OData root and the extension HTTP-service root belong to the same 1C
    /// publication: identical scheme/host/port and, where the standard markers are present
    /// ("/odata/" and "/hs/"), the identical publication prefix before them. The identity is
    /// read through the HTTP service while data is read through OData — both must be one base.
    /// </summary>
    public static bool SamePublication(string? odataBaseUrl, string? commandApiBaseUrl)
    {
        var odata = Normalize(odataBaseUrl);
        var command = Normalize(commandApiBaseUrl);
        if (odata is null || command is null) return false;
        var odataUri = new Uri(odata + "/");
        var commandUri = new Uri(command + "/");
        if (!string.Equals(odataUri.GetLeftPart(UriPartial.Authority), commandUri.GetLeftPart(UriPartial.Authority), StringComparison.Ordinal)) return false;
        var odataMarker = odataUri.AbsolutePath.IndexOf("/odata/", StringComparison.OrdinalIgnoreCase);
        var commandMarker = commandUri.AbsolutePath.IndexOf("/hs/", StringComparison.OrdinalIgnoreCase);
        if (odataMarker < 0 || commandMarker < 0) return true;
        return string.Equals(odataUri.AbsolutePath[..odataMarker], commandUri.AbsolutePath[..commandMarker], StringComparison.Ordinal);
    }
}

/// <summary>Pure verification of a fetched identity against the binding and the configured OData root.</summary>
public static class SourceIdentityVerifier
{
    public static SourceIdentityCheck Verify(OnecSourceBindingOptions? bindingOptions, string odataBaseUrl, OnecIdentityFetchResult fetched)
    {
        ArgumentNullException.ThrowIfNull(fetched);
        if (bindingOptions is null) return new(SourceIdentityVerdict.BindingMissing, null, "No OneC:SourceBinding is configured.");
        var binding = OnecSourceBinding.TryCreate(bindingOptions, out var reason);
        if (binding is null) return new(SourceIdentityVerdict.BindingInvalid, null, reason);
        // The identity is read over the extension HTTP service while data is read over OData:
        // the configured OData root must be the one the binding was verified for.
        if (!string.Equals(SourceEndpoint.Normalize(odataBaseUrl), binding.ODataEndpoint, StringComparison.Ordinal))
            return new(SourceIdentityVerdict.EndpointMismatch, null, "OneC:ODataBaseUrl does not equal the bound SourceBinding.ODataEndpoint.");

        switch (fetched.Kind)
        {
            case OnecIdentityFetchKind.Unavailable:
                return new(SourceIdentityVerdict.Unavailable, null, fetched.Detail ?? "Identity endpoint unavailable.");
            case OnecIdentityFetchKind.NotReady:
                return new(SourceIdentityVerdict.NotReady, null, $"Source identity state '{fetched.State ?? "unknown"}' is not ready.");
            case OnecIdentityFetchKind.Malformed:
                return new(SourceIdentityVerdict.Malformed, null, fetched.Detail ?? "Identity response violates the contract.");
        }

        var identity = fetched.Identity!;
        if (identity.Scope != "whole-infobase") return new(SourceIdentityVerdict.ScopeUnsupported, null, $"Scope '{identity.Scope}' is not supported.");
        if (identity.DatabaseId != binding.DatabaseId) return new(SourceIdentityVerdict.DatabaseMismatch, null, "The source databaseId differs from the binding.");
        if (identity.ExportEpoch != binding.ExportEpoch) return new(SourceIdentityVerdict.EpochMismatch, null, "The source exportEpoch differs from the binding; a new baseline is required.");
        if (identity.Environment != binding.Environment) return new(SourceIdentityVerdict.EnvironmentMismatch, null, "The source environment differs from the binding.");
        return new(SourceIdentityVerdict.Match, binding.SourceNamespace, "Source identity matches the binding.");
    }
}
