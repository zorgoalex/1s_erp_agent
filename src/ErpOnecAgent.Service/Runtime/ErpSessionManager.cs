using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Application.Configuration;
using ErpOnecAgent.Contracts.Erp;
using ErpOnecAgent.Infrastructure.Persistence.Sqlite;
using Microsoft.Extensions.Options;

namespace ErpOnecAgent.Service.Runtime;

public sealed class ErpSessionManager(IErpClient erp, IOptions<AgentOptions> agentOptions, AgentRuntimeState state, ILogger<ErpSessionManager>? logger = null) : IDisposable
{
    private static readonly string[] Capabilities = ["commands.long-poll.v1", "commands.result.v1", "etl.ndjson-gzip.v1", "onec.odata.v1"];
    private readonly SemaphoreSlim _lock = new(1, 1);
    private Guid? _sessionId;

    public async Task<Guid> GetSessionAsync(CancellationToken cancellationToken)
    {
        if (_sessionId is { } existing) return existing;
        await _lock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_sessionId is { } current) return current;
            var options = agentOptions.Value;
            var sentAt = DateTimeOffset.UtcNow;
            var roundTrip = System.Diagnostics.Stopwatch.StartNew();
            var response = await erp.StartSessionAsync(new(options.AgentId, options.SiteId, ThisAssembly.Version, SqliteMigrator.CurrentSchemaVersion, Capabilities, sentAt), cancellationToken).ConfigureAwait(false);
            roundTrip.Stop();
            // A07b B6: an explicit version rejection (accepted:false or a parsed minimum above the
            // running binary) latches CompatibilityRejected in the runtime state; the throw keeps
            // callers' existing retry semantics while the latch gives the rest of the agent the
            // incompatible_version visibility TZ §30.2 requires. Network/protocol failures neither
            // invent nor clear the latch.
            if (!response.Accepted)
            {
                state.LatchCompatibilityRejection();
                throw new InvalidOperationException($"ERP rejected agent version {ThisAssembly.Version}; minimum is {response.MinimumAgentVersion}.");
            }
            // minimumAgentVersion is contract-required (openapi: required string, non-nullable DTO).
            // An absent/malformed value can never establish compatibility — fail closed as a protocol
            // error: it neither latches a rejection nor clears an existing one, and no session is cached.
            if (!Version.TryParse(response.MinimumAgentVersion, out var minimum))
                throw new InvalidDataException($"ERP handshake returned an invalid minimumAgentVersion '{response.MinimumAgentVersion}'.");
            if (!Version.TryParse(ThisAssembly.Version, out var currentVersion))
                throw new InvalidOperationException($"Local agent version '{ThisAssembly.Version}' cannot be compared against the ERP minimum.");
            if (currentVersion < minimum)
            {
                state.LatchCompatibilityRejection();
                throw new InvalidOperationException($"Agent {currentVersion} is older than required {minimum}.");
            }
            if (response.SessionId == Guid.Empty) throw new InvalidDataException("ERP handshake response is missing a session id.");
            // One accepted compatible response publishes compatibility + maintenance coherently;
            // it clears ONLY the compatibility latch, never the local pause or remote mode inputs.
            state.ApplyCompatibleHandshake(response.MaintenanceMode);
            // A07 time: ERP-defined deadlines (command expiry) are judged against the ERP clock
            // estimate; a large difference is reported, since it also skews every stored UTC date.
            if (response.ServerTimeUtc != default)
            {
                if (!state.RecordErpClock(response.ServerTimeUtc, sentAt, roundTrip.Elapsed))
                    logger?.LogWarning("ERP_CLOCK_SAMPLE_IGNORED RoundTripMs={RoundTrip} ServerTimeUtc={ServerTime} — too slow or implausible; the previous estimate is kept", roundTrip.Elapsed.TotalMilliseconds, response.ServerTimeUtc);
                else if (state.ClockDriftExceeds(TimeSpan.FromSeconds(Math.Max(1, options.MaxClockDriftSeconds))) && state.ErpClockOffset is { } offset)
                    logger?.LogWarning("CLOCK_DRIFT OffsetSeconds={Offset} UncertaintyMs={Uncertainty} — local clock differs from ERP; check Windows time synchronization", Math.Round(offset.TotalSeconds, 1), state.ErpClockUncertainty.TotalMilliseconds);
            }
            state.LastErpSuccessAtUtc = DateTimeOffset.UtcNow;
            _sessionId = response.SessionId;
            return response.SessionId;
        }
        finally { _lock.Release(); }
    }

    public void Invalidate() => _sessionId = null;
    public void Dispose() => _lock.Dispose();
}

internal static class ThisAssembly
{
    public static string Version { get; } = typeof(ThisAssembly).Assembly.GetName().Version?.ToString(3) ?? "1.0.0";
}
