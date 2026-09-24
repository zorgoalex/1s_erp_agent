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
                await sessions.GetSessionAsync(stoppingToken).ConfigureAwait(false);
                var queues = await store.GetQueueMetricsAsync(stoppingToken).ConfigureAwait(false);
                var metrics = await metricsCollector.CaptureAsync(stoppingToken).ConfigureAwait(false);
                DateTimeOffset? certificateExpiry = null;
                if (erpOptions.Value.RequireClientCertificate) { using var certificate = CertificateLoader.LoadClientCertificate(erpOptions.Value.ClientCertificateThumbprint); certificateExpiry = certificate.NotAfter.ToUniversalTime(); }
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

    private static string GetHealthState(AgentRuntimeState state, AgentMetrics metrics, StorageOptions storage)
    {
        if (state.EffectiveMode is Domain.Agent.AgentMode.Maintenance or Domain.Agent.AgentMode.Disabled) return "maintenance";
        if (metrics.SqliteSizeBytes >= storage.MaxSqliteBytes || metrics.SpoolSizeBytes >= storage.MaxSpoolBytes || metrics.DiskFreeBytes <= storage.MinimumReservedBytesForCommands) return "storage_critical";
        if (!state.OnecCommandApiAvailable && !state.OnecODataAvailable) return "offline_onec";
        if (!state.OnecCommandApiAvailable || !state.OnecODataAvailable) return "degraded";
        return "healthy";
    }
}
