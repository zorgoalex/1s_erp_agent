using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Application.Configuration;
using ErpOnecAgent.Contracts.Erp;
using ErpOnecAgent.Domain.Agent;
using ErpOnecAgent.Domain.Etl;
using ErpOnecAgent.Infrastructure.Security;
using ErpOnecAgent.Service.Runtime;
using Microsoft.Extensions.Options;

namespace ErpOnecAgent.Service.Workers;

public sealed class HeartbeatWorker(
    IErpClient erp,
    IAgentStore store,
    ErpSessionManager sessions,
    AgentRuntimeState state,
    AgentMetricsCollector metricsCollector,
    IOptions<AgentOptions> agentOptions,
    IOptions<ErpOptions> erpOptions,
    IOptions<StorageOptions> storageOptions,
    ILogger<HeartbeatWorker> logger,
    DynamicConfigurationState? configuration = null,
    IOptions<EtlOptions>? etlOptions = null) : BackgroundService
{
    private static readonly TimeSpan HealthSummaryInterval = TimeSpan.FromMinutes(15);
    private int? _lastCertificateWarning;
    private string? _lastHealthState;
    private long _lastHealthSummary;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            if (!state.IsReady)
            {
                await Task.Delay(250, stoppingToken).ConfigureAwait(false);
                continue;
            }
            try
            {
                // A07b B6 (TZ §30.2): the heartbeat request carries agentId and no sessionId, so it
                // must never wait on the handshake — an explicit compatibility rejection or a hung
                // GetSessionAsync would otherwise suppress the only signal ERP has to observe the
                // incompatible agent. Compatibility state itself arrives via the session/config path.
                var queues = await store.GetQueueMetricsAsync(stoppingToken).ConfigureAwait(false);
                var metrics = await metricsCollector.CaptureAsync(stoppingToken).ConfigureAwait(false);
                DateTimeOffset? certificateExpiry = null;
                if (erpOptions.Value.RequireClientCertificate)
                {
                    // Expiry is read before the validity-checked load, which throws for an
                    // expired certificate and would otherwise hide CERTIFICATE_EXPIRED.
                    if (CertificateLoader.TryReadNotAfter(erpOptions.Value.ClientCertificateThumbprint) is { } notAfter) WarnCertificateExpiry(notAfter);
                    using var certificate = CertificateLoader.LoadClientCertificate(erpOptions.Value.ClientCertificateThumbprint);
                    certificateExpiry = certificate.NotAfter.ToUniversalTime();
                }
                // ETL lag: three schedule intervals without a successful run (once one succeeded
                // in this process) means the pipeline is stuck.
                // Not while ETL is deliberately paused (PauseEtl or the local pause), and not when
                // nothing is scheduled (a manual-only setup never runs on its own).
                TimeSpan? etlLagLimit = etlOptions?.Value.Enabled == true && configuration is not null && state.Snapshot.CanExtract
                    && configuration.Entities.Any(static entity => entity.Enabled && entity.RunsOnSchedule())
                    ? TimeSpan.FromMinutes(3 * Math.Max(1, configuration.IntervalMinutes))
                    : null;
                var (healthState, healthReason) = EvaluateHealth(state, metrics, storageOptions.Value, TimeSpan.FromSeconds(Math.Max(1, agentOptions.Value.MaxClockDriftSeconds)), queues, etlLagLimit);
                LogHealthSummary(healthState, healthReason, queues, metrics);
                var request = new HeartbeatRequest(agentOptions.Value.AgentId, ThisAssembly.Version, healthState,
                    (long)state.Uptime.TotalSeconds,
                    new(state.OnecODataAvailable, state.OnecCommandApiAvailable, state.LastOnecSuccessAtUtc, state.LastOnecError),
                    new(queues.CommandsPending, queues.ResultsPending, queues.EtlBatchesPending, queues.DeadLetters),
                    new(state.LastEtlSuccessAtUtc, state.CurrentEtlRunId),
                    new(metrics.DiskFreeBytes, metrics.WorkingSetBytes, metrics.CpuPercent, metrics.SqliteSizeBytes, metrics.SpoolSizeBytes),
                    new(certificateExpiry));
                await erp.SendHeartbeatAsync(request, stoppingToken).ConfigureAwait(false);
                state.LastErpSuccessAtUtc = DateTimeOffset.UtcNow;
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { return; }
            catch (Exception ex) { sessions.Invalidate(); logger.LogWarning(ex, "Heartbeat failed"); }
            await Task.Delay(TimeSpan.FromSeconds(Math.Max(10, agentOptions.Value.HeartbeatIntervalSeconds)), stoppingToken).ConfigureAwait(false);
        }
    }

    // Stage 6: the heartbeat contract has no room for queue ages or unresolved runs, so the
    // full picture is logged locally: on every health-state change and every 15 minutes.
    private void LogHealthSummary(string healthState, string healthReason, QueueMetrics queues, AgentMetrics metrics)
    {
        var changed = !string.Equals(healthState + "/" + healthReason, _lastHealthState, StringComparison.Ordinal);
        if (!changed && _lastHealthSummary != 0 && System.Diagnostics.Stopwatch.GetElapsedTime(_lastHealthSummary) < HealthSummaryInterval) return;
        _lastHealthState = healthState + "/" + healthReason;
        _lastHealthSummary = System.Diagnostics.Stopwatch.GetTimestamp();
        var now = DateTimeOffset.UtcNow;
        var snapshot = state.Snapshot;
        logger.Log(string.Equals(healthState, "healthy", StringComparison.Ordinal) ? LogLevel.Information : LogLevel.Warning,
            "AGENT_HEALTH State={State} Reason={Reason} Mode={Mode} LocalEtlPaused={LocalEtlPaused} CommandsPending={CommandsPending} OldestCommandAgeSec={OldestCommandAge} ResultsPending={ResultsPending} OldestResultAgeSec={OldestResultAge} EtlBatchesPending={EtlBatches} EtlRunsUnresolved={EtlRunsUnresolved} DeadLetters={DeadLetters} LastEtlSuccessUtc={LastEtlSuccess} DiskFreeBytes={DiskFree} ClockOffsetSec={ClockOffset}",
            healthState, healthReason, snapshot.EffectiveMode, snapshot.LocalEtlPaused, queues.CommandsPending, AgeSeconds(queues.OldestPendingCommandAtUtc, now), queues.ResultsPending, AgeSeconds(queues.OldestPendingResultAtUtc, now),
            queues.EtlBatchesPending, queues.EtlRunsUnresolved, queues.DeadLetters, state.LastEtlSuccessAtUtc, metrics.DiskFreeBytes, state.ErpClockOffset is { } offset ? Math.Round(offset.TotalSeconds, 1) : null);
    }

    private static long? AgeSeconds(DateTimeOffset? since, DateTimeOffset now) => since is { } value ? (long)Math.Max(0, (now - value).TotalSeconds) : null;

    // One log entry per crossed threshold per process (30/14 warning, 7/3/1 error, expired
    // critical); the heartbeat itself always carries the expiry date for ERP-side monitoring.
    private void WarnCertificateExpiry(DateTimeOffset expiry)
    {
        var crossed = Application.Security.CertificateExpiryPolicy.CrossedThreshold(expiry, DateTimeOffset.UtcNow);
        if (crossed is null)
        {
            _lastCertificateWarning = null; // a renewed certificate starts its own warning sequence
            return;
        }
        if (crossed == _lastCertificateWarning) return;
        _lastCertificateWarning = crossed;
        if (crossed == 0) logger.LogCritical("CERTIFICATE_EXPIRED NotAfter={NotAfter}", expiry);
        else if (crossed <= 7) logger.LogError("CERTIFICATE_EXPIRING DaysThreshold={Days} NotAfter={NotAfter}", crossed, expiry);
        else logger.LogWarning("CERTIFICATE_EXPIRING DaysThreshold={Days} NotAfter={NotAfter}", crossed, expiry);
    }

    internal static string GetHealthState(AgentRuntimeState state, AgentMetrics metrics, StorageOptions storage, TimeSpan maxClockDrift, QueueMetrics? queues = null, TimeSpan? etlLagLimit = null) =>
        EvaluateHealth(state, metrics, storage, maxClockDrift, queues, etlLagLimit).State;

    /// <summary>The heartbeat health state plus a reason code for the local AGENT_HEALTH log.</summary>
    internal static (string State, string Reason) EvaluateHealth(AgentRuntimeState state, AgentMetrics metrics, StorageOptions storage, TimeSpan maxClockDrift, QueueMetrics? queues = null, TimeSpan? etlLagLimit = null)
    {
        var snapshot = state.Snapshot;
        // incompatible_version outranks maintenance/storage/degraded: while an explicit rejection
        // is latched it is the only signal that explains why admission is off and why the operator
        // must act (TZ §30.2); the other states become observable again after a compatible handshake.
        if (snapshot.CompatibilityRejected) return ("incompatible_version", "COMPATIBILITY_REJECTED");
        if (snapshot.EffectiveMode is Domain.Agent.AgentMode.Maintenance or Domain.Agent.AgentMode.Disabled) return ("maintenance", "MODE_" + snapshot.EffectiveMode.ToString().ToUpperInvariant());
        if (metrics.SqliteSizeBytes >= storage.MaxSqliteBytes) return ("storage_critical", "SQLITE_LIMIT");
        if (metrics.SpoolSizeBytes >= storage.MaxSpoolBytes) return ("storage_critical", "SPOOL_LIMIT");
        if (metrics.DiskFreeBytes <= storage.MinimumReservedBytesForCommands) return ("storage_critical", "DISK_RESERVE");
        if (!state.OnecCommandApiAvailable && !state.OnecODataAvailable) return ("offline_onec", "ONEC_UNAVAILABLE");
        if (!state.OnecCommandApiAvailable) return ("degraded", "ONEC_COMMAND_API_UNAVAILABLE");
        if (!state.OnecODataAvailable) return ("degraded", "ONEC_ODATA_UNAVAILABLE");
        // A07 time: a clock far from ERP skews expiry decisions and every stored UTC date.
        if (state.ClockDriftExceeds(maxClockDrift)) return ("degraded", "CLOCK_DRIFT");
        // Stage 6: runs waiting for operator resolution hold entities and schedule keys.
        if (queues is { EtlRunsUnresolved: > 0 }) return ("degraded", "ETL_RUNS_UNRESOLVED");
        if (etlLagLimit is { } lagLimit && state.LastEtlSuccessAtUtc is { } lastEtl && DateTimeOffset.UtcNow - lastEtl > lagLimit) return ("degraded", "ETL_LAG");
        return ("healthy", "OK");
    }
}
