using ErpOnecAgent.Application.Abstractions;
using Microsoft.Data.Sqlite;

namespace ErpOnecAgent.Infrastructure.Persistence.Sqlite;

// C1 cutover support: startup fence for legacy runs and spool reconciliation inputs.
public sealed partial class SqliteAgentStore
{
    /// <inheritdoc cref="IAgentStore.BlockLegacyEtlRunsAsync"/>
    public async Task<int> BlockLegacyEtlRunsAsync(CancellationToken cancellationToken)
    {
        await using var connection = await factory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        var now = UtcNow();
        const string legacyRuns = """
            status IN ('running','uploading','completing')
            AND NOT EXISTS (SELECT 1 FROM etl_run_ownership_bindings b WHERE b.run_id = etl_runs.run_id)
            """;
        await ExecuteAsync(connection, transaction, $"""
            UPDATE etl_batches SET status='dead_letter', quarantine_code='RUN_BLOCKED', last_error='LEGACY_UNRESOLVED', row_version=row_version+1
            WHERE status IN ('creating','ready','retry_waiting')
              AND run_id IN (SELECT run_id FROM etl_runs WHERE {legacyRuns});
            """, cancellationToken).ConfigureAwait(false);
        var blocked = await ExecuteAsync(connection, transaction, $"""
            UPDATE etl_runs SET status='blocked', finalize_conflict_code='LEGACY_UNRESOLVED',
                finalize_conflict_message='Run written by the legacy ETL pipeline before cutover; it cannot complete on the durable path — resolve (R1), reset the domain (D1) and run a new baseline.',
                finished_at_utc=COALESCE(finished_at_utc,$now), updated_at_utc=$now, row_version=row_version+1
            WHERE {legacyRuns};
            """, cancellationToken, ("$now", now)).ConfigureAwait(false);
        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        return blocked;
    }

    /// <inheritdoc cref="IAgentStore.BlockRunsWithMissingSpoolFilesAsync"/>
    public async Task<int> BlockRunsWithMissingSpoolFilesAsync(Func<string, bool> fileExists, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(fileExists);
        await using var connection = await factory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        var pending = new List<(string BatchId, string RunId, string FilePath)>();
        await using (var select = connection.CreateCommand())
        {
            select.Transaction = transaction;
            select.CommandText = "SELECT batch_id, run_id, file_path FROM etl_batches WHERE status IN ('ready','retry_waiting');";
            await using var reader = await select.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false)) pending.Add((reader.GetString(0), reader.GetString(1), reader.GetString(2)));
        }
        var now = UtcNow();
        var missing = 0;
        foreach (var batch in pending.Where(batch => !fileExists(batch.FilePath)))
        {
            missing++;
            // quarantine_code is CHECK-constrained by migration 008; the missing-file cause is
            // recorded in last_error and in the run's finalize_conflict_code.
            await ExecuteAsync(connection, transaction, """
                UPDATE etl_batches SET status='dead_letter', quarantine_code='RUN_BLOCKED', last_error='SPOOL_FILE_MISSING', row_version=row_version+1
                WHERE batch_id=$batch AND status IN ('ready','retry_waiting');
                """, cancellationToken, ("$batch", batch.BatchId)).ConfigureAwait(false);
            await ExecuteAsync(connection, transaction, """
                UPDATE etl_batches SET status='dead_letter', quarantine_code='RUN_BLOCKED', last_error='RUN_BLOCKED', row_version=row_version+1
                WHERE run_id=$run AND status IN ('creating','ready','retry_waiting');
                """, cancellationToken, ("$run", batch.RunId)).ConfigureAwait(false);
            await ExecuteAsync(connection, transaction, """
                UPDATE etl_runs SET status='blocked', finalize_conflict_code='SPOOL_FILE_MISSING',
                    finalize_conflict_message='A registered batch file is missing from the spool; the batch was never sent. Resolve the run (R1) and re-extract.',
                    last_error='SPOOL_FILE_MISSING',
                    finished_at_utc=COALESCE(finished_at_utc,$now), updated_at_utc=$now, row_version=row_version+1
                WHERE run_id=$run AND status IN ('running','uploading','completing');
                """, cancellationToken, ("$run", batch.RunId), ("$now", now)).ConfigureAwait(false);
            await ExecuteAsync(connection, transaction, """
                UPDATE etl_jobs SET status='blocked', updated_at_utc=$now, row_version=row_version+1
                WHERE run_id=$run AND status NOT IN ('finished','cancelled','blocked');
                """, cancellationToken, ("$run", batch.RunId), ("$now", now)).ConfigureAwait(false);
        }
        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        return missing;
    }

    /// <inheritdoc cref="IAgentStore.GetReferencedBatchFilePathsAsync"/>
    public async Task<IReadOnlySet<string>> GetReferencedBatchFilePathsAsync(CancellationToken cancellationToken)
    {
        var paths = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        await using var connection = await factory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT file_path FROM etl_batches WHERE status <> 'deleted';";
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false)) paths.Add(reader.GetString(0));
        return paths;
    }
}
