using System.Text.Json;
using ErpOnecAgent.Domain.Agent;
using ErpOnecAgent.Domain.Etl;
using Microsoft.Data.Sqlite;

namespace ErpOnecAgent.Infrastructure.Persistence.Sqlite;

public sealed partial class SqliteAgentStore
{
    public async Task CreateEtlRunAsync(EtlRun run, CancellationToken cancellationToken)
    {
        await using var connection = await factory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = "INSERT INTO etl_runs(run_id,mode,requested_entities_json,status,started_at_utc) VALUES($id,$mode,$entities,'running',$now);";
        Add(command, "$id", run.RunId.ToString("D")); Add(command, "$mode", run.Mode); Add(command, "$entities", JsonSerializer.Serialize(run.Entities, JsonOptions)); Add(command, "$now", UtcNow());
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task RegisterBatchAsync(EtlBatch batch, CancellationToken cancellationToken)
    {
        await using var connection = await factory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        await ExecuteAsync(connection, transaction, """
            INSERT INTO etl_batches(batch_id,run_id,entity_name,schema_version,file_path,status,row_count,watermark_from_json,watermark_to_json,sha256,compressed_size,uncompressed_size,attempt_count,created_at_utc,next_attempt_at_utc)
            VALUES($id,$run,$entity,$schema,$path,'ready',$rows,$from,$to,$hash,$compressed,$uncompressed,0,$created,$created);
            """, cancellationToken,
            ("$id", batch.BatchId.ToString("D")), ("$run", batch.RunId.ToString("D")), ("$entity", batch.EntityName), ("$schema", batch.SchemaVersion), ("$path", batch.FilePath), ("$rows", batch.RowCount),
            ("$from", SerializeCursor(batch.WatermarkFrom)), ("$to", SerializeCursor(batch.WatermarkTo)), ("$hash", batch.Sha256), ("$compressed", batch.CompressedSize), ("$uncompressed", batch.UncompressedSize), ("$created", batch.CreatedAtUtc.ToUniversalTime().ToString("O"))).ConfigureAwait(false);
        await ExecuteAsync(connection, transaction, "UPDATE etl_runs SET batches_created=batches_created+1,rows_read=rows_read+$rows WHERE run_id=$run;", cancellationToken, ("$rows", batch.RowCount), ("$run", batch.RunId.ToString("D"))).ConfigureAwait(false);
        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task MarkEtlRunExtractedAsync(Guid runId, CancellationToken cancellationToken)
    {
        await using var connection = await factory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = "UPDATE etl_runs SET status='uploading' WHERE run_id=$id AND status='running';";
        Add(command, "$id", runId.ToString("D"));
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<IReadOnlyList<EtlBatch>> GetPendingBatchesAsync(int limit, DateTimeOffset nowUtc, CancellationToken cancellationToken)
    {
        var batches = new List<EtlBatch>();
        await using var connection = await factory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "SELECT batch_id,run_id,entity_name,schema_version,file_path,status,row_count,watermark_from_json,watermark_to_json,sha256,compressed_size,uncompressed_size,attempt_count,created_at_utc FROM etl_batches WHERE status IN ('ready','retry_waiting') AND (next_attempt_at_utc IS NULL OR next_attempt_at_utc <= $now) ORDER BY created_at_utc LIMIT $limit;";
        Add(command, "$now", nowUtc.ToUniversalTime().ToString("O")); Add(command, "$limit", limit);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            batches.Add(new EtlBatch(Guid.Parse(reader.GetString(0)), Guid.Parse(reader.GetString(1)), reader.GetString(2), reader.GetInt32(3), reader.GetString(4), ParseBatchStatus(reader.GetString(5)), reader.GetInt32(6), DeserializeCursor(NullableString(reader, 7)), DeserializeCursor(NullableString(reader, 8)), reader.GetString(9), reader.GetInt64(10), reader.GetInt64(11), reader.GetInt32(12), ParseDate(reader.GetString(13))));
        }
        await reader.DisposeAsync().ConfigureAwait(false);
        foreach (var batch in batches)
            await ExecuteAsync(connection, transaction, "UPDATE etl_batches SET status='uploading' WHERE batch_id=$id AND status IN ('ready','retry_waiting');", cancellationToken, ("$id", batch.BatchId.ToString("D"))).ConfigureAwait(false);
        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        return batches;
    }

    public async Task MarkBatchRetryAsync(Guid batchId, string errorMessage, DateTimeOffset retryAtUtc, CancellationToken cancellationToken)
    {
        await using var connection = await factory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = "UPDATE etl_batches SET status='retry_waiting',attempt_count=attempt_count+1,next_attempt_at_utc=$retry,last_error=$error WHERE batch_id=$id AND status='uploading';";
        Add(command, "$retry", retryAtUtc.ToUniversalTime().ToString("O")); Add(command, "$error", errorMessage); Add(command, "$id", batchId.ToString("D"));
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task AcknowledgeBatchAsync(Guid batchId, DateTimeOffset acknowledgedAtUtc, CancellationToken cancellationToken)
    {
        await using var connection = await factory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        var at = acknowledgedAtUtc.ToUniversalTime().ToString("O");
        await using var acknowledge = connection.CreateCommand();
        acknowledge.Transaction = transaction;
        acknowledge.CommandText = "UPDATE etl_batches SET status='acknowledged',acknowledged_at_utc=$at WHERE batch_id=$id AND status='uploading' RETURNING run_id;";
        Add(acknowledge, "$at", at); Add(acknowledge, "$id", batchId.ToString("D"));
        var runId = await acknowledge.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) as string;
        if (runId is null) throw new InvalidOperationException($"ETL batch {batchId} is not in uploading state.");
        await ExecuteAsync(connection, transaction, "UPDATE etl_runs SET batches_acknowledged=batches_acknowledged+1 WHERE run_id=$runId;", cancellationToken, ("$runId", runId)).ConfigureAwait(false);
        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<IReadOnlyList<EtlBatch>> GetAcknowledgedBatchesAsync(DateTimeOffset beforeUtc, CancellationToken cancellationToken)
    {
        var batches = new List<EtlBatch>();
        await using var connection = await factory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT batch_id,run_id,entity_name,schema_version,file_path,status,row_count,watermark_from_json,watermark_to_json,sha256,compressed_size,uncompressed_size,attempt_count,created_at_utc FROM etl_batches WHERE status='acknowledged' AND acknowledged_at_utc < $before;";
        Add(command, "$before", beforeUtc.ToUniversalTime().ToString("O"));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false)) batches.Add(new EtlBatch(Guid.Parse(reader.GetString(0)), Guid.Parse(reader.GetString(1)), reader.GetString(2), reader.GetInt32(3), reader.GetString(4), ParseBatchStatus(reader.GetString(5)), reader.GetInt32(6), DeserializeCursor(NullableString(reader, 7)), DeserializeCursor(NullableString(reader, 8)), reader.GetString(9), reader.GetInt64(10), reader.GetInt64(11), reader.GetInt32(12), ParseDate(reader.GetString(13))));
        return batches;
    }

    public async Task MarkBatchDeletedAsync(Guid batchId, CancellationToken cancellationToken)
    {
        await using var connection = await factory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = "UPDATE etl_batches SET status='deleted' WHERE batch_id=$id AND status='acknowledged';";
        Add(command, "$id", batchId.ToString("D")); await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<IReadOnlyList<ErpOnecAgent.Application.Abstractions.EtlRunCompletion>> GetRunsReadyToCompleteAsync(CancellationToken cancellationToken)
    {
        var completions = new List<ErpOnecAgent.Application.Abstractions.EtlRunCompletion>();
        var readyRuns = new List<(Guid RunId, long RowsRead, int BatchCount)>();
        await using var connection = await factory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using (var runs = connection.CreateCommand())
        {
            runs.CommandText = "SELECT run_id,rows_read,batches_created FROM etl_runs r WHERE status='uploading' AND NOT EXISTS (SELECT 1 FROM etl_batches b WHERE b.run_id=r.run_id AND b.status <> 'acknowledged');";
            await using var reader = await runs.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false)) readyRuns.Add((Guid.Parse(reader.GetString(0)), reader.GetInt64(1), reader.GetInt32(2)));
        }
        foreach (var ready in readyRuns)
        {
            var watermarks = new Dictionary<string, EtlCursor>(StringComparer.Ordinal);
            await using var watermarkCommand = connection.CreateCommand();
            watermarkCommand.CommandText = "SELECT entity_name,watermark_to_json FROM etl_batches WHERE run_id=$id AND status='acknowledged' ORDER BY created_at_utc;";
            Add(watermarkCommand, "$id", ready.RunId.ToString("D"));
            await using var watermarkReader = await watermarkCommand.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await watermarkReader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                var cursor = DeserializeCursor(NullableString(watermarkReader, 1));
                if (cursor is not null) watermarks[watermarkReader.GetString(0)] = cursor;
            }
            completions.Add(new(ready.RunId, watermarks, ready.RowsRead, ready.BatchCount));
        }
        return completions;
    }

    public async Task<EtlCursor?> GetCommittedWatermarkAsync(string entityName, CancellationToken cancellationToken)
    {
        await using var connection = await factory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT committed_cursor_json FROM watermarks WHERE entity_name=$entity;";
        Add(command, "$entity", entityName);
        return DeserializeCursor(await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) as string);
    }

    public async Task CommitWatermarkAsync(string entityName, EtlCursor cursor, Guid runId, CancellationToken cancellationToken)
    {
        await using var connection = await factory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = "INSERT INTO watermarks(entity_name,committed_cursor_json,last_run_id,updated_at_utc) VALUES($entity,$cursor,$run,$now) ON CONFLICT(entity_name) DO UPDATE SET committed_cursor_json=excluded.committed_cursor_json,extracting_cursor_json=NULL,last_run_id=excluded.last_run_id,updated_at_utc=excluded.updated_at_utc;";
        Add(command, "$entity", entityName); Add(command, "$cursor", SerializeCursor(cursor)); Add(command, "$run", runId.ToString("D")); Add(command, "$now", UtcNow());
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task CompleteEtlRunAsync(Guid runId, EtlRunStatus status, string? errorMessage, CancellationToken cancellationToken)
    {
        await using var connection = await factory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = "UPDATE etl_runs SET status=$status,finished_at_utc=$now,last_error=$error,error_count=CASE WHEN $error IS NULL THEN error_count ELSE error_count+1 END WHERE run_id=$id;";
        Add(command, "$status", ToDb(status)); Add(command, "$now", UtcNow()); Add(command, "$error", errorMessage); Add(command, "$id", runId.ToString("D"));
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<QueueMetrics> GetQueueMetricsAsync(CancellationToken cancellationToken)
    {
        await using var connection = await factory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        // Dead-letter accounting (A09): a command is counted from its local dead-letter outcome
        // (result_status='dead_letter', including the result_pending delivery state and rejected
        // admissions) and stays counted after the result is acknowledged - one count per command,
        // never double counted and never zero while a dead-letter result exists. ETL batch dead
        // letters are included in the total; CommandsDeadLetter is the command-only breakdown.
        command.CommandText = """
            SELECT
              (SELECT COUNT(*) FROM commands_inbox WHERE status IN ('queued','retry_waiting','unknown_result','executing')),
              (SELECT COUNT(*) FROM results_outbox WHERE status <> 'acknowledged'),
              (SELECT COUNT(*) FROM etl_batches WHERE status IN ('ready','uploading','retry_waiting')),
              (SELECT COUNT(*) FROM commands_inbox WHERE result_status='dead_letter') + (SELECT COUNT(*) FROM etl_batches WHERE status='dead_letter'),
              (SELECT COUNT(*) FROM commands_inbox WHERE result_status='dead_letter');
            """;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
        return new QueueMetrics(reader.GetInt64(0), reader.GetInt64(1), reader.GetInt64(2), reader.GetInt64(3), reader.GetInt64(4));
    }
}
