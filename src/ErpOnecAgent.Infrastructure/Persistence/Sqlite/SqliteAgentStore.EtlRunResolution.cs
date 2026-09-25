using ErpOnecAgent.Application.Abstractions;
using Microsoft.Data.Sqlite;

namespace ErpOnecAgent.Infrastructure.Persistence.Sqlite;

// R1 DARK storage slice: attested manual resolution of failed/blocked ETL runs
// (etl-ownership-upload-design.md §8, migration 010). The store proves only what durable
// state can prove — no admitted send attempt, no in-flight batch, no live extraction,
// completion or dispatch fence — and records the operator's attestations of worker
// quiescence and remote verification. One commit writes the record and resolved_at_utc,
// fences the remaining pre-acknowledgement batches and releases the run's epoch-bound
// ownership. It never commits watermarks and never finishes the job.
public sealed partial class SqliteAgentStore
{
    /// <inheritdoc cref="IAgentStore.ResolveEtlRunAsync"/>
    public async Task<EtlRunResolutionOutcome> ResolveEtlRunAsync(EtlRunResolutionRequest request, DateTimeOffset nowUtc, CancellationToken cancellationToken)
    {
        ValidateResolutionRequest(request);
        var now = nowUtc.ToUniversalTime().ToString("O");
        var runText = request.RunId.ToString("D");

        await using var connection = await factory.OpenAsync(cancellationToken).ConfigureAwait(false);
        // BEGIN IMMEDIATE: concurrent resolutions of one run serialize; the loser sees the
        // committed record.
        await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);

        var existing = await ReadResolutionRecordAsync(connection, transaction, request.RunId, cancellationToken).ConfigureAwait(false);
        if (existing is not null)
        {
            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
            var same = existing.OperatorId == request.OperatorId && existing.Decision == request.Decision
                && existing.RemoteVerification == request.RemoteVerification;
            return new EtlRunResolutionOutcome.AlreadyResolved(existing, same);
        }

        string? status = null;
        string? resolvedAt = null;
        string? extractionClaim = null;
        string? completionClaim = null;
        string? conflictCode = null;
        await using (var read = connection.CreateCommand())
        {
            read.Transaction = transaction;
            read.CommandText = "SELECT status,resolved_at_utc,extraction_claim_id,completion_claim_id,finalize_conflict_code FROM etl_runs WHERE run_id=$run;";
            Add(read, "$run", runText);
            await using var reader = await read.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                status = reader.GetString(0);
                resolvedAt = NullableString(reader, 1);
                extractionClaim = NullableString(reader, 2);
                completionClaim = NullableString(reader, 3);
                conflictCode = NullableString(reader, 4);
            }
        }

        var refusal = status is null ? EtlRunResolutionRefusal.RunNotFound
            : status is not ("failed" or "blocked") || resolvedAt is not null ? EtlRunResolutionRefusal.RunNotResolvable
            : (EtlRunResolutionRefusal?)null;
        refusal ??= await ClassifyResolutionBlockerAsync(connection, transaction, runText, extractionClaim, completionClaim, cancellationToken).ConfigureAwait(false);
        if (refusal is not null)
        {
            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
            return new EtlRunResolutionOutcome.Refused(refusal.Value);
        }

        // Remaining pre-acknowledgement batches can never be dispatched after resolution.
        // 'uploading' is excluded by the precondition above.
        var fenced = await ExecuteAsync(connection, transaction,
            "UPDATE etl_batches SET status='dead_letter', quarantine_code='RUN_BLOCKED', last_error='RUN_RESOLVED', row_version=row_version+1 WHERE run_id=$run AND status IN ('creating','ready','retry_waiting');",
            cancellationToken, ("$run", runText)).ConfigureAwait(false);

        // Epoch-bound release: only active rows this run owns at the bound epoch — a row
        // re-acquired by another run (epoch moved) or owned by someone else never moves.
        var released = await ExecuteAsync(connection, transaction, """
            UPDATE etl_entity_ownership AS o SET released_at_utc=$now, release_reason='manual_release', updated_at_utc=$now, row_version=row_version+1
            WHERE o.released_at_utc IS NULL AND o.owner_run_id=$run
              AND EXISTS (SELECT 1 FROM etl_run_ownership_bindings b
                          WHERE b.run_id=$run AND b.entity_name=o.entity_name AND b.expected_epoch=o.ownership_epoch);
            """, cancellationToken, ("$now", now), ("$run", runText)).ConfigureAwait(false);

        var runResolved = await ExecuteAsync(connection, transaction,
            "UPDATE etl_runs SET resolved_at_utc=$now, updated_at_utc=$now, row_version=row_version+1 WHERE run_id=$run AND status=$status AND resolved_at_utc IS NULL;",
            cancellationToken, ("$now", now), ("$run", runText), ("$status", status)).ConfigureAwait(false);
        if (runResolved != 1) throw new InvalidOperationException($"ETL run {request.RunId:D} changed mid-resolution.");

        var record = new EtlRunResolutionRecord(request.RunId, Guid.NewGuid(), ParseDate(now), request.OperatorId, request.Decision,
            request.RemoteVerification, status!, conflictCode, released, fenced);
        var inserted = await ExecuteAsync(connection, transaction, """
            INSERT INTO etl_run_resolutions(run_id,resolution_id,resolved_at_utc,operator_id,decision,remote_verification,workers_quiesced,prior_status,prior_conflict_code,ownership_released,batches_fenced)
            VALUES($run,$id,$now,$operator,$decision,$verification,1,$status,$conflict,$released,$fenced);
            """, cancellationToken,
            ("$run", runText), ("$id", record.ResolutionId.ToString("D")), ("$now", now), ("$operator", request.OperatorId),
            ("$decision", DecisionText(request.Decision)), ("$verification", request.RemoteVerification), ("$status", status), ("$conflict", conflictCode),
            ("$released", released), ("$fenced", fenced)).ConfigureAwait(false);
        if (inserted != 1) throw new InvalidOperationException($"Resolution record for ETL run {request.RunId:D} was not written.");

        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        return new EtlRunResolutionOutcome.Resolved(record);
    }

    // ---------- R1 internals ----------

    private static async Task<EtlRunResolutionRefusal?> ClassifyResolutionBlockerAsync(SqliteConnection connection, SqliteTransaction transaction, string runText, string? extractionClaim, string? completionClaim, CancellationToken cancellationToken)
    {
        // An admitted attempt's sender may still act; its outcome is unknown.
        if (await ScalarLongAsync(connection, transaction,
                "SELECT COUNT(*) FROM etl_batch_send_attempts a JOIN etl_batches b ON b.batch_id=a.batch_id WHERE b.run_id=$run AND a.outcome='admitted';",
                cancellationToken, ("$run", runText)).ConfigureAwait(false) != 0)
            return EtlRunResolutionRefusal.AdmittedSendAttempt;
        // An 'uploading' batch (ledger-less legacy included) is in-flight evidence.
        if (await ScalarLongAsync(connection, transaction,
                "SELECT COUNT(*) FROM etl_batches WHERE run_id=$run AND status='uploading';",
                cancellationToken, ("$run", runText)).ConfigureAwait(false) != 0)
            return EtlRunResolutionRefusal.BatchInFlight;
        if (extractionClaim is not null) return EtlRunResolutionRefusal.LiveExtractionClaim;
        if (completionClaim is not null) return EtlRunResolutionRefusal.LiveCompletionClaim;
        if (await ScalarLongAsync(connection, transaction,
                "SELECT COUNT(*) FROM etl_jobs WHERE run_id=$run AND status IN ('pending','deferred','running');",
                cancellationToken, ("$run", runText)).ConfigureAwait(false) != 0)
            return EtlRunResolutionRefusal.LiveJobDispatch;
        return null;
    }

    private static async Task<EtlRunResolutionRecord?> ReadResolutionRecordAsync(SqliteConnection connection, SqliteTransaction transaction, Guid runId, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "SELECT resolution_id,resolved_at_utc,operator_id,decision,remote_verification,prior_status,prior_conflict_code,ownership_released,batches_fenced FROM etl_run_resolutions WHERE run_id=$run;";
        Add(command, "$run", runId.ToString("D"));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        return await reader.ReadAsync(cancellationToken).ConfigureAwait(false)
            ? new EtlRunResolutionRecord(runId, Guid.Parse(reader.GetString(0)), ParseDate(reader.GetString(1)), reader.GetString(2),
                ParseDecision(reader.GetString(3)), reader.GetString(4), reader.GetString(5), NullableString(reader, 6), (int)reader.GetInt64(7), (int)reader.GetInt64(8))
            : null;
    }

    private static string DecisionText(EtlRunResolutionDecision decision) => decision switch
    {
        EtlRunResolutionDecision.Abandon => "abandon",
        EtlRunResolutionDecision.Retry => "retry",
        EtlRunResolutionDecision.Rebaseline => "rebaseline",
        _ => throw new ArgumentOutOfRangeException(nameof(decision))
    };

    private static EtlRunResolutionDecision ParseDecision(string value) => value switch
    {
        "abandon" => EtlRunResolutionDecision.Abandon,
        "retry" => EtlRunResolutionDecision.Retry,
        "rebaseline" => EtlRunResolutionDecision.Rebaseline,
        _ => throw new InvalidOperationException($"Unknown stored resolution decision '{value}'.")
    };

    private static void ValidateResolutionRequest(EtlRunResolutionRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (string.IsNullOrWhiteSpace(request.OperatorId))
            throw new ArgumentException("An operator identity is required.", nameof(request));
        if (string.IsNullOrWhiteSpace(request.RemoteVerification))
            throw new ArgumentException("The remote verification performed at ERP must be stated.", nameof(request));
        if (!request.WorkersQuiesced)
            throw new ArgumentException("Resolution requires the attestation that dispatchers and upload workers were stopped and drained.", nameof(request));
        if (!Enum.IsDefined(request.Decision))
            throw new ArgumentOutOfRangeException(nameof(request), "Unknown resolution decision.");
    }
}
