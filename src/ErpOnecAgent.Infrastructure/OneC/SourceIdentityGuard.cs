using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Application.Configuration;
using ErpOnecAgent.Application.Etl;
using Microsoft.Extensions.Options;

namespace ErpOnecAgent.Infrastructure.OneC;

/// <summary>
/// S1: the ETL gate that turns the configured binding plus a live identity read into a
/// source namespace. Callers check it before claiming a run (a mismatch or unavailable
/// source means the run does not start) and again before sealing extraction (a change
/// means the run is blocked and no watermark is committed). A Match yields the stable
/// namespace for <c>EtlEntityExtractionRequest.SourceNamespace</c>. The identity client is
/// resolved per call, so its typed HttpClient comes from the factory with rotated handlers.
/// </summary>
public sealed class SourceIdentityGuard
{
    private readonly Func<IOnecIdentityClient> _clientFactory;
    private readonly IOptions<OnecOptions> _options;

    public SourceIdentityGuard(Func<IOnecIdentityClient> clientFactory, IOptions<OnecOptions> options)
    {
        _clientFactory = clientFactory;
        _options = options;
    }

    public SourceIdentityGuard(IOnecIdentityClient client, IOptions<OnecOptions> options)
        : this(() => client, options) { }

    public async Task<SourceIdentityCheck> CheckAsync(CancellationToken cancellationToken)
    {
        var value = _options.Value;
        // A missing or invalid binding is decided without a network call.
        if (value.SourceBinding is null) return new(SourceIdentityVerdict.BindingMissing, null, "No OneC:SourceBinding is configured.");
        if (OnecSourceBinding.TryCreate(value.SourceBinding, out var reason) is null) return new(SourceIdentityVerdict.BindingInvalid, null, reason);
        var fetched = await _clientFactory().GetIdentityAsync(cancellationToken).ConfigureAwait(false);
        return SourceIdentityVerifier.Verify(value.SourceBinding, value.ODataBaseUrl, fetched);
    }

    /// <summary>
    /// The before/after rule for one extraction: both checks must match AND name the same
    /// namespace; otherwise the run must not commit watermarks.
    /// </summary>
    public static bool SameSource(SourceIdentityCheck before, SourceIdentityCheck after) =>
        before.IsMatch && after.IsMatch && string.Equals(before.SourceNamespace, after.SourceNamespace, StringComparison.Ordinal);
}
