using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Application.Configuration;
using ErpOnecAgent.Infrastructure.Persistence.Sqlite;
using ErpOnecAgent.Service.Runtime;
using Microsoft.Extensions.Options;

namespace ErpOnecAgent.Service.Workers;

public sealed partial class MaintenanceWorker(IAgentStore store, ISpoolStore spool, AgentRuntimeState state, IOptions<AgentOptions> agentOptions, IOptions<StorageOptions> storageOptions, ILogger<MaintenanceWorker> logger) : BackgroundService
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
                await RunCycleAsync(DateTimeOffset.UtcNow, stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { return; }
            catch (Exception ex) { logger.LogError(ex, "Maintenance cycle failed"); }
            await Task.Delay(TimeSpan.FromHours(Math.Max(1, storageOptions.Value.MaintenanceIntervalHours)), stoppingToken).ConfigureAwait(false);
        }
    }

    /// <summary>One maintenance cycle: verified backup, then (only for a healthy database) retention cleanup and checkpoint.</summary>
    internal async Task RunCycleAsync(DateTimeOffset now, CancellationToken cancellationToken)
    {
        var storage = storageOptions.Value;
        // A corrupt live database is not backed up and not cleaned up (cleanup deletes rows);
        // the operator restores from the last verified backup (docs/operations.md).
        if (await RunBackupAsync(now, cancellationToken).ConfigureAwait(false) != BackupOutcome.LiveDatabaseCorrupt)
        {
            foreach (var batch in await store.GetAcknowledgedBatchesAsync(now.AddDays(-storage.AcknowledgedBatchRetentionDays), cancellationToken).ConfigureAwait(false))
            {
                await spool.DeleteAcknowledgedAsync(batch, cancellationToken).ConfigureAwait(false);
                await store.MarkBatchDeletedAsync(batch.BatchId, cancellationToken).ConfigureAwait(false);
            }
            await store.CleanupAsync(now.AddDays(-storage.CompletedCommandRetentionDays), now.AddDays(-storage.AcknowledgedBatchRetentionDays), cancellationToken).ConfigureAwait(false);
            await store.RunMaintenanceAsync(cancellationToken).ConfigureAwait(false);
        }
        var databasePath = Path.Combine(agentOptions.Value.DataDirectory, "data", "agent.db");
        var sqliteBytes = GetSize(databasePath) + GetSize(databasePath + "-wal") + GetSize(databasePath + "-shm");
        if (sqliteBytes >= storage.MaxSqliteBytes) logger.LogWarning("STORAGE_WARNING SQLite size {SqliteBytes} reached configured limit {MaxSqliteBytes}", sqliteBytes, storage.MaxSqliteBytes);
    }

    internal enum BackupOutcome { Created, LiveDatabaseCorrupt, BackupInvalid }

    /// <summary>
    /// Stage 6 backup: only a database that passes the integrity check is backed up; the copy
    /// is written as <c>.tmp</c>, verified independently, and only then named
    /// <c>agent-yyyyMMdd-HHmmss.db</c>. Rotation (newest by name) therefore only ever sees
    /// verified backups, and a bad copy can never displace a good one.
    /// </summary>
    internal async Task<BackupOutcome> RunBackupAsync(DateTimeOffset now, CancellationToken cancellationToken)
    {
        var storage = storageOptions.Value;
        var backups = Path.Combine(agentOptions.Value.DataDirectory, "data", "backups"); Directory.CreateDirectory(backups);
        foreach (var stale in Directory.EnumerateFiles(backups, "agent-*.db.tmp*")) TryDelete(stale); // our own interrupted copies and their sidecars

        var integrity = await store.IntegrityCheckAsync(cancellationToken).ConfigureAwait(false);
        if (!string.Equals(integrity, "ok", StringComparison.OrdinalIgnoreCase))
        {
            logger.LogCritical("SQLITE_INTEGRITY_FAILED Result={Result} — no backup taken, cleanup skipped; existing backups are kept", integrity);
            return BackupOutcome.LiveDatabaseCorrupt;
        }

        var final = Path.Combine(backups, $"agent-{now:yyyyMMdd-HHmmss}.db");
        var temporary = final + ".tmp";
        await store.BackupAsync(temporary, cancellationToken).ConfigureAwait(false);
        var verification = await SqliteBackupVerifier.VerifyAsync(temporary, cancellationToken).ConfigureAwait(false);
        if (!string.Equals(verification, "ok", StringComparison.OrdinalIgnoreCase))
        {
            File.Delete(temporary);
            logger.LogCritical("BACKUP_VERIFICATION_FAILED Result={Result} — the new backup was discarded; existing backups are kept", verification);
            return BackupOutcome.BackupInvalid;
        }
        File.Move(temporary, final, overwrite: true);

        // Only timestamped names take part in rotation; an operator's own agent-*.db is left alone.
        foreach (var old in Directory.EnumerateFiles(backups, "agent-*.db")
                     .Where(static path => BackupName().IsMatch(Path.GetFileName(path)))
                     .OrderByDescending(static path => Path.GetFileName(path), StringComparer.Ordinal)
                     .Skip(Math.Max(1, storage.BackupRetentionCount)))
            TryDelete(old);
        logger.LogInformation("BACKUP_CREATED Path={Path}", final);
        return BackupOutcome.Created;
    }

    [System.Text.RegularExpressions.GeneratedRegex(@"^agent-\d{8}-\d{6}\.db$", System.Text.RegularExpressions.RegexOptions.CultureInvariant)]
    private static partial System.Text.RegularExpressions.Regex BackupName();

    // A file held open (antivirus, an operator copying a backup) must not abort the cycle.
    private void TryDelete(string path)
    {
        try
        {
            File.Delete(path);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            logger.LogWarning(ex, "BACKUP_ROTATION_FAILED Path={Path} — will retry next cycle", path);
        }
    }

    private static long GetSize(string path) => File.Exists(path) ? new FileInfo(path).Length : 0;
}
