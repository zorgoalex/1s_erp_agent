using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Domain.Etl;
using ErpOnecAgent.Service.Runtime;

namespace ErpOnecAgent.Service.Workers.Etl;

/// <summary>
/// Retention and revocation of sensitive entities (frozen definition <c>deleteBatchAfterAck</c>;
/// agent-bridge to-erp/0031, to-onec/0030). Each sweep:
/// <list type="number">
/// <item>revocation — only under an ERP configuration (version &gt; 0; the local appsettings list
/// is never authoritative for it): every unfinished run whose frozen entities include a sensitive
/// entity missing from the active configuration is blocked ENTITY_REVOKED, and then EVERY
/// remaining file of that entity is deleted, whatever its batch status;</item>
/// <item>catch-up — the file of an acknowledged sensitive batch that the upload worker could not
/// delete right after the ACK (crash, sharing violation) is deleted.</item>
/// </list>
/// Batch rows (metadata: id, row count, hashes) always stay; the log carries counts only.
/// </summary>
public sealed class EtlRetentionWorker(
    IAgentStore store,
    ISpoolStore spool,
    DynamicConfigurationState configuration,
    AgentRuntimeState state,
    ILogger<EtlRetentionWorker> logger) : BackgroundService
{
    internal static TimeSpan Interval = TimeSpan.FromMinutes(1);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            if (state.IsReady)
            {
                try
                {
                    await SweepAsync(stoppingToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { return; }
                catch (Exception ex) { logger.LogError(ex, "ETL_RETENTION_SWEEP_FAILED"); }
            }
            await Task.Delay(state.IsReady ? Interval : TimeSpan.FromMilliseconds(250), stoppingToken).ConfigureAwait(false);
        }
    }

    internal async Task<EtlRetentionSweep> SweepAsync(CancellationToken cancellationToken)
    {
        var snapshot = configuration.Snapshot;
        HashSet<string>? active = null;
        var blockedRuns = 0;
        if (snapshot.Version > 0)
        {
            active = snapshot.Entities.Select(static entity => entity.EntityCode).ToHashSet(StringComparer.Ordinal);
            // Runs first: once blocked, nothing can register, claim or send a batch of the entity,
            // so the files deleted below cannot be needed any more.
            var blocked = await store.BlockRunsWithRevokedEntitiesAsync(active, cancellationToken).ConfigureAwait(false);
            blockedRuns = blocked.Select(static run => run.RunId).Distinct().Count();
            foreach (var run in blocked)
                logger.LogWarning("ETL_RUN_BLOCKED_ENTITY_REVOKED RunId={RunId} Entity={Entity}", run.RunId, run.EntityName);
        }

        var files = await store.GetSensitiveBatchFilesAsync(cancellationToken).ConfigureAwait(false);
        var revokedEntities = active is null ? [] : files.Select(static file => file.EntityName).Where(entity => !active.Contains(entity)).ToHashSet(StringComparer.Ordinal);
        var deleted = 0;
        var failed = 0;
        foreach (var file in files)
        {
            // A live batch of a still-configured entity keeps its file: it has not reached ERP yet.
            if (!revokedEntities.Contains(file.EntityName) && !string.Equals(file.Status, "acknowledged", StringComparison.Ordinal)) continue;
            if (!File.Exists(file.FilePath)) continue;
            try
            {
                await spool.DeleteAcknowledgedAsync(ToBatch(file), cancellationToken).ConfigureAwait(false);
                deleted++;
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
            {
                failed++;
                logger.LogWarning(ex, "ETL_BATCH_FILE_DELETE_FAILED BatchId={BatchId} — retried by the next sweep", file.BatchId);
            }
        }
        if (deleted > 0 || failed > 0)
            logger.LogInformation("ETL_SENSITIVE_FILES_DELETED Deleted={Deleted} Failed={Failed} RevokedEntities={RevokedEntities}", deleted, failed, revokedEntities.Count);
        return new EtlRetentionSweep(blockedRuns, deleted, failed);
    }

    // The spool only needs the path (and checks it stays inside the spool root).
    private static EtlBatch ToBatch(EtlSensitiveBatchFile file) =>
        new(file.BatchId, file.RunId, file.EntityName, 1, file.FilePath, EtlBatchStatus.Acknowledged, 0, null, null, string.Empty, 0, 0, 0, DateTimeOffset.UtcNow);
}

internal sealed record EtlRetentionSweep(int BlockedRuns, int DeletedFiles, int FailedDeletes);
