using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Application.Configuration;
using ErpOnecAgent.Contracts.Erp;
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
    ILogger<HeartbeatWorker> logger) : BackgroundService
{
    private int? _lastCertificateWarning;

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
                var healthState = GetHealthState(state, metrics, storageOptions.Value);
                var request = new HeartbeatRequest(agentOptions.Value.AgentId, ThisAssembly.Version, healthState,
                    (long)(DateTimeOffset.UtcNow - state.StartedAtUtc).TotalSeconds,
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

    private static string GetHealthState(AgentRuntimeState state, AgentMetrics metrics, StorageOptions storage)
    {
        var snapshot = state.Snapshot;
        // incompatible_version outranks maintenance/storage/degraded: while an explicit rejection
        // is latched it is the only signal that explains why admission is off and why the operator
        // must act (TZ §30.2); the other states become observable again after a compatible handshake.
        if (snapshot.CompatibilityRejected) return "incompatible_version";
        if (snapshot.EffectiveMode is Domain.Agent.AgentMode.Maintenance or Domain.Agent.AgentMode.Disabled) return "maintenance";
        if (metrics.SqliteSizeBytes >= storage.MaxSqliteBytes || metrics.SpoolSizeBytes >= storage.MaxSpoolBytes || metrics.DiskFreeBytes <= storage.MinimumReservedBytesForCommands) return "storage_critical";
        if (!state.OnecCommandApiAvailable && !state.OnecODataAvailable) return "offline_onec";
        if (!state.OnecCommandApiAvailable || !state.OnecODataAvailable) return "degraded";
        return "healthy";
    }
}
