using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Application.Configuration;
using ErpOnecAgent.Service.Runtime;
using Microsoft.Extensions.Options;

namespace ErpOnecAgent.Service.Workers;

public sealed class MaintenanceWorker(IAgentStore store, ISpoolStore spool, AgentRuntimeState state, IOptions<AgentOptions> agentOptions, IOptions<StorageOptions> storageOptions, ILogger<MaintenanceWorker> logger) : BackgroundService
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
                var storage = storageOptions.Value; var now = DateTimeOffset.UtcNow;
                var backups = Path.Combine(agentOptions.Value.DataDirectory, "data", "backups"); Directory.CreateDirectory(backups);
                await store.BackupAsync(Path.Combine(backups, $"agent-{now:yyyyMMdd-HHmmss}.db"), stoppingToken).ConfigureAwait(false);
                var files = new DirectoryInfo(backups).EnumerateFiles("agent-*.db").OrderByDescending(static file => file.CreationTimeUtc).Skip(storage.BackupRetentionCount).ToArray();
                foreach (var file in files) file.Delete();
                var integrity = await store.IntegrityCheckAsync(stoppingToken).ConfigureAwait(false);
                if (!string.Equals(integrity, "ok", StringComparison.OrdinalIgnoreCase)) logger.LogCritical("SQLITE_INTEGRITY_FAILED Result={Result}", integrity);
                foreach (var batch in await store.GetAcknowledgedBatchesAsync(now.AddDays(-storage.AcknowledgedBatchRetentionDays), stoppingToken).ConfigureAwait(false))
                {
                    await spool.DeleteAcknowledgedAsync(batch, stoppingToken).ConfigureAwait(false);
                    await store.MarkBatchDeletedAsync(batch.BatchId, stoppingToken).ConfigureAwait(false);
                }
                await store.CleanupAsync(now.AddDays(-storage.CompletedCommandRetentionDays), now.AddDays(-storage.AcknowledgedBatchRetentionDays), stoppingToken).ConfigureAwait(false);
                await store.RunMaintenanceAsync(stoppingToken).ConfigureAwait(false);
                var databasePath = Path.Combine(agentOptions.Value.DataDirectory, "data", "agent.db");
                var sqliteBytes = GetSize(databasePath) + GetSize(databasePath + "-wal") + GetSize(databasePath + "-shm");
                if (sqliteBytes >= storage.MaxSqliteBytes) logger.LogWarning("STORAGE_WARNING SQLite size {SqliteBytes} reached configured limit {MaxSqliteBytes}", sqliteBytes, storage.MaxSqliteBytes);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { return; }
            catch (Exception ex) { logger.LogError(ex, "Maintenance cycle failed"); }
            await Task.Delay(TimeSpan.FromHours(Math.Max(1, storageOptions.Value.MaintenanceIntervalHours)), stoppingToken).ConfigureAwait(false);
        }
    }

    private static long GetSize(string path) => File.Exists(path) ? new FileInfo(path).Length : 0;
}
