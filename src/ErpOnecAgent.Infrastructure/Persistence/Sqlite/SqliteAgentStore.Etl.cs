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
        // Only batches of a fully succeeded run are retention-eligible. Acknowledged batches of
        // an unfinished run (extraction still running, or run-completion not yet accepted by ERP)
        // and of any other terminal outcome (failed/partial/cancelled) keep their spool file as
        // watermark and audit evidence until a separate manual policy resolves them.
        command.CommandText = "SELECT batch_id,run_id,entity_name,schema_version,file_path,status,row_count,watermark_from_json,watermark_to_json,sha256,compressed_size,uncompressed_size,attempt_count,created_at_utc FROM etl_batches WHERE status='acknowledged' AND acknowledged_at_utc < $before AND EXISTS (SELECT 1 FROM etl_runs r WHERE r.run_id=etl_batches.run_id AND r.status='succeeded');";
        Add(command, "$before", beforeUtc.ToUniversalTime().ToString("O"));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false)) batches.Add(new EtlBatch(Guid.Parse(reader.GetString(0)), Guid.Parse(reader.GetString(1)), reader.GetString(2), reader.GetInt32(3), reader.GetString(4), ParseBatchStatus(reader.GetString(5)), reader.GetInt32(6), DeserializeCursor(NullableString(reader, 7)), DeserializeCursor(NullableString(reader, 8)), reader.GetString(9), reader.GetInt64(10), reader.GetInt64(11), reader.GetInt32(12), ParseDate(reader.GetString(13))));
        return batches;
    }

    public async Task MarkBatchDeletedAsync(Guid batchId, CancellationToken cancellationToken)
    {
        await using var connection = await factory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        // Same parent guard independently of the selection path: a batch row can never enter the
        // purgeable 'deleted' state while its run has not succeeded.
        command.CommandText = "UPDATE etl_batches SET status='deleted' WHERE batch_id=$id AND status='acknowledged' AND EXISTS (SELECT 1 FROM etl_runs r WHERE r.run_id=etl_batches.run_id AND r.status='succeeded');";
        Add(command, "$id", batchId.ToString("D")); await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<IReadOnlyList<ErpOnecAgent.Application.Abstractions.EtlRunCompletion>> GetRunsReadyToCompleteAsync(CancellationToken cancellationToken)
    {
        var completions = new List<ErpOnecAgent.Application.Abstractions.EtlRunCompletion>();
        var candidates = new List<(Guid RunId, long RowsRead, int BatchesCreated, long BatchesAcknowledged, string? EntitiesJson)>();
        await using var connection = await factory.OpenAsync(cancellationToken).ConfigureAwait(false);
        // Readiness screening and watermark collection share ONE read transaction so the run
        // manifest, the batch rows and the collected cursors are a single consistent snapshot.
        await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        await using (var runs = connection.CreateCommand())
        {
            runs.Transaction = transaction;
            runs.CommandText = "SELECT run_id,rows_read,batches_created,batches_acknowledged,requested_entities_json FROM etl_runs WHERE status='uploading';";
            await using var reader = await runs.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false)) candidates.Add((Guid.Parse(reader.GetString(0)), reader.GetInt64(1), reader.GetInt32(2), reader.GetInt64(3), NullableString(reader, 4)));
        }
        foreach (var candidate in candidates)
        {
            // Fail closed: an unprovable run is left 'uploading' for a later manual policy and is
            // never completed. The manifest must be an explicit non-empty set of unique entity
            // names, the created/acknowledged counters must match the batch rows exactly, every
            // batch must be acknowledged, and the covered entities must equal the requested set.
            var expected = ParseRequestedEntities(candidate.EntitiesJson);
            if (expected is null || candidate.BatchesCreated <= 0 || candidate.BatchesAcknowledged != candidate.BatchesCreated) continue;
            var watermarks = new Dictionary<string, EtlCursor>(StringComparer.Ordinal);
            var actual = new HashSet<string>(StringComparer.Ordinal);
            var batchRows = 0L;
            var blocked = false;
            await using (var batches = connection.CreateCommand())
            {
                batches.Transaction = transaction;
                // Provisional final-cursor selection keeps the pre-existing created_at order with
                // last writer per entity. created_at ties are not guaranteed safe; the durable
                // final-cursor ordering design is deferred to A05b and not invented here.
                batches.CommandText = "SELECT entity_name,status,watermark_to_json FROM etl_batches WHERE run_id=$id ORDER BY created_at_utc;";
                Add(batches, "$id", candidate.RunId.ToString("D"));
                await using var reader = await batches.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    batchRows++;
                    if (!string.Equals(reader.GetString(1), "acknowledged", StringComparison.Ordinal)) { blocked = true; break; }
                    EtlCursor? cursor;
                    try { cursor = DeserializeCursor(NullableString(reader, 2)); }
                    catch (JsonException) { blocked = true; break; }
                    if (cursor is null) { blocked = true; break; }
                    var entity = reader.GetString(0);
                    actual.Add(entity);
                    watermarks[entity] = cursor;
                }
            }
            if (blocked || batchRows != candidate.BatchesCreated || !actual.SetEquals(expected)) continue;
            completions.Add(new(candidate.RunId, watermarks, candidate.RowsRead, candidate.BatchesCreated));
        }
        return completions;
    }

    private static HashSet<string>? ParseRequestedEntities(string? entitiesJson)
    {
        if (string.IsNullOrWhiteSpace(entitiesJson)) return null;
        try
        {
            using var document = JsonDocument.Parse(entitiesJson);
            if (document.RootElement.ValueKind != JsonValueKind.Array) return null;
            var entities = new HashSet<string>(StringComparer.Ordinal);
            foreach (var element in document.RootElement.EnumerateArray())
            {
                if (element.ValueKind != JsonValueKind.String) return null;
                var entity = element.GetString();
                if (string.IsNullOrWhiteSpace(entity) || !entities.Add(entity)) return null;
            }
            return entities.Count > 0 ? entities : null;
        }
        catch (JsonException)
        {
            return null;
        }
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
        // Retention eligibility must stay monotonic: a run that already succeeded is never
        // reopened, and a run may enter 'succeeded' only from a non-terminal state so a resolved
        // (failed/cancelled/partial) run cannot silently become retention-eligible. Other
        // terminal-state transitions are still unguarded; the full run state machine is deferred.
        command.CommandText = "UPDATE etl_runs SET status=$status,finished_at_utc=$now,last_error=$error,error_count=CASE WHEN $error IS NULL THEN error_count ELSE error_count+1 END WHERE run_id=$id AND status <> 'succeeded' AND ($status <> 'succeeded' OR status IN ('pending','running','paused','uploading','completing'));";
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
              (SELECT COUNT(*) FROM commands_inbox WHERE result_status='dead_letter'),
              (SELECT MIN(received_at_utc) FROM commands_inbox WHERE status IN ('queued','retry_waiting','unknown_result','executing')),
              (SELECT MIN(created_at_utc) FROM results_outbox WHERE status IN ('pending','sending','retry_waiting')),
              (SELECT COUNT(*) FROM etl_runs r WHERE r.status IN ('blocked','failed')
                 AND NOT EXISTS (SELECT 1 FROM etl_run_resolutions x WHERE x.run_id = r.run_id)),
              (SELECT COUNT(*) FROM (
                 SELECT e.status, e.failure_code,
                        ROW_NUMBER() OVER (PARTITION BY e.entity_name ORDER BY r.finished_at_utc DESC, r.run_id DESC) AS latest
                 FROM etl_run_entities e JOIN etl_runs r ON r.run_id = e.run_id
                 WHERE r.status = 'succeeded')
               WHERE latest = 1 AND status = 'failed' AND failure_code IS NOT NULL);
            """;
        // Stage 6: the ages of the oldest pending command/result (timestamps are stored as UTC
        // round-trip strings, so MIN orders them chronologically) and the runs waiting for R1.
        // The result age covers rows awaiting delivery only, read through ix_results_pending.
        // Partial runs: an entity is "failing" while its latest finalized run skipped it — a
        // partial run is 'succeeded', so without this a permanently failing entity is silent.
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
        return new QueueMetrics(reader.GetInt64(0), reader.GetInt64(1), reader.GetInt64(2), reader.GetInt64(3), reader.GetInt64(4),
            reader.IsDBNull(5) ? null : ParseUtc(reader.GetString(5)),
            reader.IsDBNull(6) ? null : ParseUtc(reader.GetString(6)),
            reader.GetInt64(7),
            reader.GetInt64(8));
    }

    private static DateTimeOffset ParseUtc(string value) =>
        DateTimeOffset.Parse(value, System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.RoundtripKind).ToUniversalTime();
}
