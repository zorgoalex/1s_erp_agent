using ErpOnecAgent.Application.Abstractions;
using Microsoft.Data.Sqlite;

namespace ErpOnecAgent.Infrastructure.Persistence.Sqlite;

// O2 DARK storage slice: the durable admitted-attempt send ledger
// (etl_batch_send_attempts) with owner-fenced claim/ACK/outcome and fail-closed
// unknown-outcome handling (etl-ownership-upload-design.md §5 + root disposition
// item 5; orchestrator decisions D1-D3). Nothing here is called by workers,
// RecoverAsync or the ERP client; the legacy upload path (GetPendingBatchesAsync /
// MarkBatchRetryAsync / AcknowledgeBatchAsync) remains a §9 bypass to be fenced
// atomically at cutover.
//
// Invariants: 'admitted' is the only pre-network state — admission is never proof
// a byte left the process; an attempt is single-use and never re-armed; only
// 'precheck_failed' (the trusted never-invoked attestation) permits a bounded new
// attempt; uncertain outcomes quarantine the batch and block the run eagerly with
// ownership retained; the first recorded ACK observation is never overwritten.
public sealed partial class SqliteAgentStore
{
    private const string QuarantineUploadOutcomeUnknown = "UPLOAD_OUTCOME_UNKNOWN";
    private const string QuarantineAckInvalid = "ACK_INVALID";
    private const string QuarantineUploadAttemptsExhausted = "UPLOAD_ATTEMPTS_EXHAUSTED";
    private const string QuarantineRunBlocked = "RUN_BLOCKED";
    private const string SendLedgerLost = "SEND_LEDGER_LOST";
    private const string SendLedgerConflict = "SEND_LEDGER_CONFLICT";
    private const string PrecheckAttestationContradicted = "PRECHECK_ATTESTATION_CONTRADICTED";

    // Re-admission is decided by the ledger, never by the batch status alone: a batch
    // is admissible only while every prior attempt is 'precheck_failed' (the trusted
    // never-invoked attestation). An admitted, uncertain, rejected or acknowledged
    // attempt makes the batch non-admissible forever, even if a legacy writer
    // (RecoverAsync uploading->ready, MarkBatchRetryAsync) made it due again.
    private const string OnlyPrecheckFailedAttempts = """
        NOT EXISTS (SELECT 1 FROM etl_batch_send_attempts x
                    WHERE x.batch_id = b.batch_id AND x.outcome <> 'precheck_failed')
        """;

    // Admission gate on the parent run (alias b on etl_batches): the run must be
    // extraction-active ('running'|'uploading') AND satisfy the SAME complete
    // three-way ownership/binding equality predicate the extraction and finalize
    // paths use — a per-entity shortcut is expressly forbidden (root disposition 5).
    private const string SendRunOwnershipGate = $"""
        EXISTS (SELECT 1 FROM etl_runs r WHERE r.run_id = b.run_id
                AND r.status IN ('running','uploading')
                AND {RunOwnershipSetPredicate})
        """;

    // The batch's own entity must carry an epoch-bound active ownership row for the
    // run — a stray batch of an unowned entity can never be admitted.
    private const string SendEntityOwnershipArm = """
        EXISTS (SELECT 1 FROM etl_entity_ownership eo
                JOIN etl_run_ownership_bindings eb
                  ON eb.run_id = eo.owner_run_id AND eb.entity_name = eo.entity_name
                WHERE eo.entity_name = b.entity_name AND eo.owner_run_id = b.run_id
                  AND eo.released_at_utc IS NULL AND eb.expected_epoch > 0
                  AND eo.ownership_epoch = eb.expected_epoch)
        """;

    /// <inheritdoc cref="IAgentStore.GetDueBatchUploadsAsync"/>
    public async Task<IReadOnlyList<EtlDueBatchUpload>> GetDueBatchUploadsAsync(int limit, DateTimeOffset nowUtc, CancellationToken cancellationToken)
    {
        var batches = new List<EtlDueBatchUpload>();
        await using var connection = await factory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        // 'retry_waiting' exists on this path ONLY for proven-unsent precheck retries
        // (legacy writers may produce it too — the §9 bypass stays until cutover).
        // Run-active + entity-owned + no-live-admission are admission predicates that
        // the claim transaction re-verifies together with the full ownership set.
        command.CommandText = $"""
            SELECT b.batch_id, b.run_id, b.entity_name, b.schema_version, b.file_path, b.row_count, b.sha256,
                   (SELECT COUNT(*) FROM etl_batch_send_attempts a0 WHERE a0.batch_id=b.batch_id)
            FROM etl_batches b
            WHERE (b.status='ready'
                   OR (b.status='retry_waiting' AND (b.next_attempt_at_utc IS NULL OR b.next_attempt_at_utc <= $now)))
              AND EXISTS (SELECT 1 FROM etl_runs r WHERE r.run_id=b.run_id
                          AND r.status IN ('running','uploading'))
              AND EXISTS (SELECT 1 FROM etl_entity_ownership o WHERE o.entity_name=b.entity_name
                          AND o.owner_run_id=b.run_id AND o.released_at_utc IS NULL)
              AND {OnlyPrecheckFailedAttempts}
            ORDER BY b.created_at_utc, b.batch_id LIMIT $limit;
            """;
        Add(command, "$now", nowUtc.ToUniversalTime().ToString("O"));
        Add(command, "$limit", limit);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            batches.Add(new EtlDueBatchUpload(
                Guid.Parse(reader.GetString(0)), Guid.Parse(reader.GetString(1)), reader.GetString(2),
                reader.GetInt32(3), reader.GetString(4), reader.GetInt32(5), reader.GetString(6),
                (int)reader.GetInt64(7)));
        }
        return batches;
    }

    /// <inheritdoc cref="IAgentStore.TryClaimBatchUploadAsync"/>
    public async Task<EtlBatchUploadClaimOutcome> TryClaimBatchUploadAsync(Guid batchId, string ownerId, DateTimeOffset nowUtc, int maxAttempts, CancellationToken cancellationToken)
    {
        ValidateOwner(ownerId);
        ArgumentOutOfRangeException.ThrowIfLessThan(maxAttempts, 1);

        await using var connection = await factory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        var now = nowUtc.ToUniversalTime().ToString("O");
        var attemptId = Guid.NewGuid();

        // The savepoint wraps flip + ledger insert so the two failure modes stay
        // distinct (root disposition 4): a swallowed ledger write is a controlled
        // zero-row outcome — the flip rolls back to the savepoint and one commit
        // writes the quarantine block; a thrown SQL error unwinds the WHOLE
        // transaction and leaves the batch claimable for a convergent retry.
        await ExecuteAsync(connection, transaction, "SAVEPOINT etl_send_claim;", cancellationToken).ConfigureAwait(false);

        // Write-first guarded flip: serializes concurrent claimants on the batch row
        // — exactly one wins. Every admission predicate holds atomically: due status,
        // run extraction-active, the FULL bound-epoch ownership set, the batch entity
        // arm, no live 'admitted' attempt, the admitted-count bound below the durable
        // limit, and identical-value enforcement of that bound (D1).
        var claimed = await ExecuteAsync(connection, transaction, $"""
            UPDATE etl_batches AS b SET status='uploading', send_attempt_id=$attempt,
                upload_max_attempts=COALESCE(b.upload_max_attempts,$max), next_attempt_at_utc=NULL, row_version=b.row_version+1
            WHERE b.batch_id=$batch
              AND (b.status='ready' OR (b.status='retry_waiting' AND (b.next_attempt_at_utc IS NULL OR b.next_attempt_at_utc <= $now)))
              AND {SendRunOwnershipGate}
              AND {SendEntityOwnershipArm}
              AND {OnlyPrecheckFailedAttempts}
              AND (SELECT COUNT(*) FROM etl_batch_send_attempts a2 WHERE a2.batch_id=b.batch_id) < COALESCE(b.upload_max_attempts,$max)
              AND (b.upload_max_attempts IS NULL OR b.upload_max_attempts=$max);
            """, cancellationToken,
            ("$attempt", attemptId.ToString("D")), ("$max", maxAttempts), ("$now", now), ("$batch", batchId.ToString("D"))).ConfigureAwait(false);

        if (claimed == 0)
        {
            await ExecuteAsync(connection, transaction, "RELEASE SAVEPOINT etl_send_claim;", cancellationToken).ConfigureAwait(false);
            // A due batch whose ledger already holds a non-precheck attempt was made
            // due again by a writer that bypassed the ledger: its remote outcome is
            // unknown, so it is quarantined (never re-sent) and its run blocked.
            var contradictedRun = await ClassifyLedgerContradictionAsync(connection, transaction, batchId, now, cancellationToken).ConfigureAwait(false);
            if (contradictedRun is not null)
            {
                const string conflictMessage = "The batch is due again although the send ledger holds an admitted or terminal attempt; the remote outcome is unknown, the batch is quarantined and the run is blocked.";
                await CommitUploadBlockAsync(connection, transaction, contradictedRun.Value, batchId, SendLedgerConflict, QuarantineUploadOutcomeUnknown, conflictMessage, now, cancellationToken).ConfigureAwait(false);
                await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
                return new EtlBatchUploadClaimOutcome.Blocked(SendLedgerConflict, conflictMessage);
            }
            // Only a still-due batch whose persisted bound the presented limit MATCHES
            // and whose admitted count already reached it — with every other admission
            // predicate still holding — commits the exhaustion quarantine. Every other
            // refusal (bound mismatch, live attempt, wrong status, inactive run, broken
            // ownership) is strictly read-only.
            var exhaustedRun = await ClassifyUploadBoundExhaustionAsync(connection, transaction, batchId, maxAttempts, now, cancellationToken).ConfigureAwait(false);
            if (exhaustedRun is null)
            {
                await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
                return new EtlBatchUploadClaimOutcome.NotClaimed();
            }
            const string exhaustedMessage = "Admitted batch send attempts reached the durable bound; the batch is quarantined and the run is blocked, evidence preserved for manual resolution.";
            await CommitUploadBlockAsync(connection, transaction, exhaustedRun.Value, batchId, QuarantineUploadAttemptsExhausted, QuarantineUploadAttemptsExhausted, exhaustedMessage, now, cancellationToken).ConfigureAwait(false);
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            return new EtlBatchUploadClaimOutcome.Blocked(QuarantineUploadAttemptsExhausted, exhaustedMessage);
        }

        // The 'admitted' ledger row is written in the same commit as the flip — the
        // durable record that a send was ADMITTED under this identity (never that the
        // network was reached). attempt_no is the per-batch monotonic admission
        // sequence, computed in-transaction.
        var attemptNo = await ScalarLongAsync(connection, transaction, """
            INSERT INTO etl_batch_send_attempts(attempt_id,batch_id,attempt_no,owner_id,admitted_at_utc,outcome)
            SELECT $attempt,$batch,(SELECT COALESCE(MAX(a.attempt_no),0)+1 FROM etl_batch_send_attempts a WHERE a.batch_id=$batch),$owner,$now,'admitted'
            RETURNING attempt_no;
            """, cancellationToken,
            ("$attempt", attemptId.ToString("D")), ("$batch", batchId.ToString("D")), ("$owner", ownerId), ("$now", now)).ConfigureAwait(false);
        if (attemptNo < 1)
        {
            // Controlled zero-row outcome (e.g. RAISE(IGNORE) swallowed the ledger
            // write): restore the batch to its pre-claim state via the savepoint, then
            // commit the fail-closed block — a batch that cannot be ledgered can never
            // be sent. Distinct from a thrown error, which rolls back everything.
            await ExecuteAsync(connection, transaction, "ROLLBACK TO SAVEPOINT etl_send_claim; RELEASE SAVEPOINT etl_send_claim;", cancellationToken).ConfigureAwait(false);
            const string lostMessage = "The send-attempt ledger write changed no rows; the batch was restored to its pre-claim state and the run is blocked — corrupt durable evidence is never dispatched.";
            var lostRunId = await ReadBatchRunIdAsync(connection, transaction, batchId, cancellationToken).ConfigureAwait(false);
            if (lostRunId is null)
            {
                await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
                return new EtlBatchUploadClaimOutcome.NotClaimed();
            }
            await CommitUploadBlockAsync(connection, transaction, lostRunId.Value, batchId, SendLedgerLost, SendLedgerLost, lostMessage, now, cancellationToken).ConfigureAwait(false);
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            return new EtlBatchUploadClaimOutcome.Blocked(SendLedgerLost, lostMessage);
        }

        await ExecuteAsync(connection, transaction, "RELEASE SAVEPOINT etl_send_claim;", cancellationToken).ConfigureAwait(false);
        var claim = await ReadSendClaimAsync(connection, transaction, batchId, attemptId, ownerId, cancellationToken).ConfigureAwait(false);
        if (claim is null) throw new InvalidOperationException($"ETL batch {batchId:D} lost its admitted attempt {attemptId:D} mid-transaction.");
        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        return new EtlBatchUploadClaimOutcome.Claimed(claim);
    }

    /// <inheritdoc cref="IAgentStore.AcknowledgeClaimedBatchAsync"/>
    public async Task<EtlBatchAckOutcome> AcknowledgeClaimedBatchAsync(Guid batchId, Guid attemptId, EtlBatchAckEvidence acknowledgement, string? ackPayloadHash, int? httpStatus, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(acknowledgement);
        // The hash is the identity of the observed ACK body: without it an exact replay
        // cannot be told apart from a conflicting observation.
        ArgumentException.ThrowIfNullOrWhiteSpace(ackPayloadHash);

        await using var connection = await factory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        var now = UtcNow();
        var batchText = batchId.ToString("D");
        var attemptText = attemptId.ToString("D");

        // The ACK binds to the durable (batch_id, attempt_id) ledger row — never to a
        // bare batch id or to the 'uploading' pointer alone. A foreign attempt
        // identity writes nothing.
        var attempt = await ReadAttemptRowAsync(connection, transaction, batchId, attemptId, cancellationToken).ConfigureAwait(false);
        if (attempt is null)
        {
            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
            return new EtlBatchAckOutcome.ClaimLost();
        }

        var (ackRunId, ackRowCount) = await ReadBatchRunRowsAsync(connection, transaction, batchId, cancellationToken).ConfigureAwait(false)
            ?? throw new InvalidOperationException($"ETL batch {batchId:D} vanished mid-transaction.");
        var ackValid = IsValidAcknowledgement(acknowledgement, batchId, ackRowCount);

        if (string.Equals(attempt.Outcome, "precheck_failed", StringComparison.Ordinal))
        {
            // ERP acknowledged an attempt the worker attested as never sent: the
            // attestation is false, so the batch may already be applied remotely. The
            // first observation is recorded; the batch is quarantined and the run
            // blocked — it is never re-sent under the broken attestation.
            if (attempt.AckObservedAtUtc is not null)
            {
                await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
                return ReplayOutcome(attempt, ackPayloadHash);
            }
            await RecordFirstObservationAsync(connection, transaction, attemptText, batchText, ackPayloadHash, ackValid, httpStatus, now, cancellationToken).ConfigureAwait(false);
            // The run is blocked whenever it is still active — even if a later attempt
            // already acknowledged the batch: the batch then reached ERP twice under
            // unproven dedup and the run must not finalize. An acknowledged batch stays
            // as evidence; a still-due batch is quarantined.
            string contradictedMessage;
            if (await IsRunActiveAsync(connection, transaction, ackRunId, cancellationToken).ConfigureAwait(false))
            {
                contradictedMessage = "ERP acknowledged a send attempt that the worker attested as never invoked; the batch may have been applied more than once — the run is blocked for manual resolution.";
                await CommitUploadBlockAsync(connection, transaction, ackRunId, batchId, PrecheckAttestationContradicted, QuarantineUploadOutcomeUnknown, contradictedMessage, now, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                contradictedMessage = "ERP acknowledged a send attempt that the worker attested as never invoked; the run is already terminal, the evidence is recorded and any still-pending batch is quarantined.";
                await QuarantineStrayBatchAsync(connection, transaction, ackRunId, batchId, PrecheckAttestationContradicted, contradictedMessage, now, cancellationToken).ConfigureAwait(false);
            }
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            return new EtlBatchAckOutcome.AttestationContradicted(PrecheckAttestationContradicted, contradictedMessage);
        }

        var live = false;
        if (string.Equals(attempt.Outcome, "admitted", StringComparison.Ordinal))
        {
            // Live-fence probe: batch still 'uploading' under this exact attempt AND the
            // run is extraction-active AND the FULL bound-epoch ownership set holds AND
            // the batch entity arm holds. The guarded row_version bump also serializes
            // this transaction against a concurrent outcome writer on the same batch.
            live = await ExecuteAsync(connection, transaction, $"""
                UPDATE etl_batches AS b SET row_version=b.row_version+1
                WHERE b.batch_id=$batch AND b.status='uploading' AND b.send_attempt_id=$attempt
                  AND {SendRunOwnershipGate}
                  AND {SendEntityOwnershipArm};
                """, cancellationToken, ("$batch", batchText), ("$attempt", attemptText)).ConfigureAwait(false) == 1;
        }

        if (live)
        {
            var runId = ackRunId;
            var rowCount = ackRowCount;

            if (ackValid)
            {
                // Fenced apply — expected-one writes; a lost guard mid-transaction
                // aborts the whole transaction (the ACK stays applicable on retry)
                // rather than committing a half-applied acknowledgement.
                var acked = await ExecuteAsync(connection, transaction,
                    "UPDATE etl_batches SET status='acknowledged', acknowledged_at_utc=$ackAt, row_version=row_version+1 WHERE batch_id=$batch AND status='uploading' AND send_attempt_id=$attempt;",
                    cancellationToken, ("$batch", batchText), ("$attempt", attemptText),
                    ("$ackAt", (acknowledgement.AcknowledgedAtUtc ?? DateTimeOffset.UtcNow).ToUniversalTime().ToString("O"))).ConfigureAwait(false);
                if (acked != 1) throw new InvalidOperationException($"ETL batch {batchId:D} lost its uploading fence mid-transaction.");
                var applied = await ExecuteAsync(connection, transaction,
                    "UPDATE etl_batch_send_attempts SET outcome='acknowledged', finished_at_utc=$now, http_status=$http, ack_payload_hash=$hash, ack_observed_at_utc=$now, ack_valid=1 WHERE attempt_id=$attempt AND batch_id=$batch AND outcome='admitted';",
                    cancellationToken, ("$attempt", attemptText), ("$batch", batchText), ("$now", now), ("$http", httpStatus), ("$hash", ackPayloadHash)).ConfigureAwait(false);
                if (applied != 1) throw new InvalidOperationException($"ETL send attempt {attemptId:D} left 'admitted' mid-transaction.");
                var counter = await ExecuteAsync(connection, transaction,
                    "UPDATE etl_runs SET batches_acknowledged=batches_acknowledged+1, updated_at_utc=$now, row_version=row_version+1 WHERE run_id=$run;",
                    cancellationToken, ("$run", runId.ToString("D")), ("$now", now)).ConfigureAwait(false);
                if (counter != 1) throw new InvalidOperationException($"ETL run {runId:D} vanished mid-transaction.");
                await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
                return new EtlBatchAckOutcome.Acknowledged();
            }

            // Invalid ACK under a live fence: the attempt is terminally rejected with
            // the observation evidence recorded, and the run blocks eagerly in the
            // same commit — a malformed ACK is evidence, never an application.
            var invalidMessage = $"ERP batch acknowledgement failed validation ({DescribeAckMismatch(acknowledgement, batchId, rowCount)}); evidence recorded, the batch is quarantined and the run blocked.";
            var rejected = await ExecuteAsync(connection, transaction,
                "UPDATE etl_batch_send_attempts SET outcome='rejected_ack', finished_at_utc=$now, http_status=$http, ack_payload_hash=$hash, ack_observed_at_utc=$now, ack_valid=0, last_error=$error WHERE attempt_id=$attempt AND batch_id=$batch AND outcome='admitted';",
                cancellationToken, ("$attempt", attemptText), ("$batch", batchText), ("$now", now), ("$http", httpStatus), ("$hash", ackPayloadHash), ("$error", $"{QuarantineAckInvalid}: {invalidMessage}")).ConfigureAwait(false);
            if (rejected != 1) throw new InvalidOperationException($"ETL send attempt {attemptId:D} left 'admitted' mid-transaction.");
            await CommitUploadBlockAsync(connection, transaction, runId, batchId, QuarantineAckInvalid, QuarantineAckInvalid, invalidMessage, now, cancellationToken).ConfigureAwait(false);
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            return new EtlBatchAckOutcome.Rejected(QuarantineAckInvalid, invalidMessage);
        }

        // Matched-late evidence: the (batch_id, attempt_id) row exists but its live
        // fence is gone (terminal attempt, fenced batch, or inactive run). The ACK is
        // real evidence belonging to that attempt — the same field validation runs and
        // its result (ack_valid) is recorded with the observation on the attempt row
        // ONLY: batch, run, job and ownership are untouched — a late ACK never
        // resurrects and never releases. The FIRST recorded observation wins: an exact
        // replay is preserved and a conflicting later observation is reported
        // explicitly, never overwritten.
        if (attempt.AckObservedAtUtc is not null)
        {
            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
            return ReplayOutcome(attempt, ackPayloadHash);
        }
        await RecordFirstObservationAsync(connection, transaction, attemptText, batchText, ackPayloadHash, ackValid, httpStatus, now, cancellationToken).ConfigureAwait(false);
        // If a legacy writer made the batch due again behind the ledger, the late ACK
        // is acted on like a late failure: the batch is quarantined (never re-sent,
        // never applied) and an active run blocked. An already-fenced batch is left.
        await QuarantineStrayBatchAsync(connection, transaction, ackRunId, batchId, QuarantineUploadOutcomeUnknown,
            "An ACK arrived for an attempt whose batch was made due again outside the send ledger; the batch is quarantined and the run is blocked.", now, cancellationToken).ConfigureAwait(false);
        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        return new EtlBatchAckOutcome.LateEvidenceRecorded();
    }

    /// <inheritdoc cref="IAgentStore.FailClaimedBatchSendAsync"/>
    public async Task<EtlBatchSendFailureOutcome> FailClaimedBatchSendAsync(Guid batchId, Guid attemptId, string errorMessage, int? httpStatus, CancellationToken cancellationToken)
    {
        var diagnostic = string.IsNullOrWhiteSpace(errorMessage) ? "uncertain send outcome" : errorMessage;

        await using var connection = await factory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        var now = UtcNow();
        var batchText = batchId.ToString("D");
        var attemptText = attemptId.ToString("D");

        var attempt = await ReadAttemptRowAsync(connection, transaction, batchId, attemptId, cancellationToken).ConfigureAwait(false);
        // A terminal attempt is a no-op — including a late failure report for an
        // already-acknowledged attempt (the attempt is terminal evidence; the run-level
        // failure path remains available separately for other evidence).
        if (attempt is null || !string.Equals(attempt.Outcome, "admitted", StringComparison.Ordinal))
        {
            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
            return new EtlBatchSendFailureOutcome.ClaimLost();
        }

        // Fail-closed quarantine proceeds even when ownership evidence is already
        // corrupt: the live fence here is the batch's uploading pointer under the exact
        // attempt identity plus an extraction-active run — never the ownership state,
        // which is itself grounds to block.
        var live = await ExecuteAsync(connection, transaction, """
            UPDATE etl_batches AS b SET row_version=b.row_version+1
            WHERE b.batch_id=$batch AND b.status='uploading' AND b.send_attempt_id=$attempt
              AND EXISTS (SELECT 1 FROM etl_runs r WHERE r.run_id=b.run_id AND r.status IN ('running','uploading'));
            """, cancellationToken, ("$batch", batchText), ("$attempt", attemptText)).ConfigureAwait(false);

        if (live == 1)
        {
            // Uncertain outcome after admission: attempt 'unknown', batch quarantined,
            // run + job blocked, siblings fenced — ownership and bindings RETAINED.
            // There is no resend: absence of a response is never proof of no remote
            // effect.
            var marked = await ExecuteAsync(connection, transaction,
                "UPDATE etl_batch_send_attempts SET outcome='unknown', finished_at_utc=$now, http_status=$http, last_error=$error WHERE attempt_id=$attempt AND batch_id=$batch AND outcome='admitted';",
                cancellationToken, ("$attempt", attemptText), ("$batch", batchText), ("$now", now), ("$http", httpStatus), ("$error", $"{QuarantineUploadOutcomeUnknown}: {diagnostic}")).ConfigureAwait(false);
            if (marked != 1) throw new InvalidOperationException($"ETL send attempt {attemptId:D} left 'admitted' mid-transaction.");
            var runId = await ReadBatchRunIdAsync(connection, transaction, batchId, cancellationToken).ConfigureAwait(false)
                ?? throw new InvalidOperationException($"ETL batch {batchId:D} vanished mid-transaction.");
            await CommitUploadBlockAsync(connection, transaction, runId, batchId, QuarantineUploadOutcomeUnknown, QuarantineUploadOutcomeUnknown, diagnostic, now, cancellationToken).ConfigureAwait(false);
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            return new EtlBatchSendFailureOutcome.Blocked(QuarantineUploadOutcomeUnknown, diagnostic);
        }

        // Late outcome on a dead fence: the still-'admitted' attempt records 'unknown'
        // as evidence. When the run is already blocked and the batch fenced, nothing
        // else moves. When a legacy writer made the batch due again (RecoverAsync
        // uploading->ready, MarkBatchRetryAsync), the batch is quarantined and its run
        // blocked in the same commit — an uncertain send is never left re-sendable.
        var late = await ExecuteAsync(connection, transaction,
            "UPDATE etl_batch_send_attempts SET outcome='unknown', finished_at_utc=$now, http_status=$http, last_error=$error WHERE attempt_id=$attempt AND batch_id=$batch AND outcome='admitted';",
            cancellationToken, ("$attempt", attemptText), ("$batch", batchText), ("$now", now), ("$http", httpStatus), ("$error", $"{QuarantineUploadOutcomeUnknown}: {diagnostic}")).ConfigureAwait(false);
        if (late != 1) throw new InvalidOperationException($"ETL send attempt {attemptId:D} left 'admitted' mid-transaction.");
        var lateRunId = await ReadBatchRunIdAsync(connection, transaction, batchId, cancellationToken).ConfigureAwait(false)
            ?? throw new InvalidOperationException($"ETL batch {batchId:D} vanished mid-transaction.");
        var quarantined = await QuarantineStrayBatchAsync(connection, transaction, lateRunId, batchId, QuarantineUploadOutcomeUnknown, diagnostic, now, cancellationToken).ConfigureAwait(false);
        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        return quarantined
            ? new EtlBatchSendFailureOutcome.Blocked(QuarantineUploadOutcomeUnknown, diagnostic)
            : new EtlBatchSendFailureOutcome.LateOutcomeRecorded();
    }

    /// <inheritdoc cref="IAgentStore.RetryClaimedBatchSendAsync"/>
    public async Task<EtlBatchSendRetryOutcome> RetryClaimedBatchSendAsync(Guid batchId, Guid attemptId, string errorMessage, DateTimeOffset nextAttemptAtUtc, CancellationToken cancellationToken)
    {
        var diagnostic = string.IsNullOrWhiteSpace(errorMessage) ? "precheck failed before the network call was invoked" : errorMessage;
        var next = nextAttemptAtUtc.ToUniversalTime().ToString("O");

        await using var connection = await factory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        var now = UtcNow();
        var batchText = batchId.ToString("D");
        var attemptText = attemptId.ToString("D");

        var attempt = await ReadAttemptRowAsync(connection, transaction, batchId, attemptId, cancellationToken).ConfigureAwait(false);
        if (attempt is null || !string.Equals(attempt.Outcome, "admitted", StringComparison.Ordinal))
        {
            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
            return new EtlBatchSendRetryOutcome.ClaimLost();
        }

        // Live fence: uploading batch under the exact attempt identity + run active +
        // the FULL bound-epoch ownership set + the batch entity arm. A broken set can
        // never schedule a re-admission.
        var live = await ExecuteAsync(connection, transaction, $"""
            UPDATE etl_batches AS b SET row_version=b.row_version+1
            WHERE b.batch_id=$batch AND b.status='uploading' AND b.send_attempt_id=$attempt
              AND {SendRunOwnershipGate}
              AND {SendEntityOwnershipArm};
            """, cancellationToken, ("$batch", batchText), ("$attempt", attemptText)).ConfigureAwait(false);

        if (live == 0)
        {
            // Dead fence: the trusted precheck attestation is still real evidence for
            // this attempt (the send was never invoked) — record it on the attempt row
            // only; batch/run/ownership untouched.
            var late = await ExecuteAsync(connection, transaction,
                "UPDATE etl_batch_send_attempts SET outcome='precheck_failed', finished_at_utc=$now, last_error=$error WHERE attempt_id=$attempt AND batch_id=$batch AND outcome='admitted';",
                cancellationToken, ("$attempt", attemptText), ("$batch", batchText), ("$now", now), ("$error", diagnostic)).ConfigureAwait(false);
            if (late != 1) throw new InvalidOperationException($"ETL send attempt {attemptId:D} left 'admitted' mid-transaction.");
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            return new EtlBatchSendRetryOutcome.LateOutcomeRecorded();
        }

        var row = await ReadBatchBoundAsync(connection, transaction, batchId, cancellationToken).ConfigureAwait(false)
            ?? throw new InvalidOperationException($"ETL batch {batchId:D} vanished mid-transaction.");
        // The durable bound persisted at first claim is authoritative; the bound
        // counts ADMITTED attempts (every ledger row started 'admitted'), including
        // this one. A batch row that lost its bound is corrupt — fail closed.
        var bound = row.UploadMaxAttempts ?? 0;
        var marked = await ExecuteAsync(connection, transaction,
            "UPDATE etl_batch_send_attempts SET outcome='precheck_failed', finished_at_utc=$now, last_error=$error WHERE attempt_id=$attempt AND batch_id=$batch AND outcome='admitted';",
            cancellationToken, ("$attempt", attemptText), ("$batch", batchText), ("$now", now), ("$error", diagnostic)).ConfigureAwait(false);
        if (marked != 1) throw new InvalidOperationException($"ETL send attempt {attemptId:D} left 'admitted' mid-transaction.");

        if (row.AttemptCount >= bound)
        {
            const string exhaustedMessage = "Admitted batch send attempts reached the durable bound; the batch is quarantined and the run is blocked, evidence preserved for manual resolution.";
            await CommitUploadBlockAsync(connection, transaction, row.RunId, batchId, QuarantineUploadAttemptsExhausted, QuarantineUploadAttemptsExhausted, exhaustedMessage, now, cancellationToken).ConfigureAwait(false);
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            return new EtlBatchSendRetryOutcome.Blocked(QuarantineUploadAttemptsExhausted, exhaustedMessage);
        }

        var scheduled = await ExecuteAsync(connection, transaction,
            "UPDATE etl_batches SET status='retry_waiting', next_attempt_at_utc=$next, last_error=$error, row_version=row_version+1 WHERE batch_id=$batch AND status='uploading';",
            cancellationToken, ("$batch", batchText), ("$next", next), ("$error", diagnostic)).ConfigureAwait(false);
        if (scheduled != 1) throw new InvalidOperationException($"ETL batch {batchId:D} lost its uploading fence mid-transaction.");
        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        return new EtlBatchSendRetryOutcome.Scheduled();
    }

    // ---------- O2 shared internals ----------

    private sealed record AttemptRow(string Outcome, string? AckObservedAtUtc, string? AckPayloadHash);

    private sealed record BatchBoundRow(Guid RunId, long? UploadMaxAttempts, long AttemptCount);

    private static async Task<AttemptRow?> ReadAttemptRowAsync(SqliteConnection connection, SqliteTransaction transaction, Guid batchId, Guid attemptId, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "SELECT outcome, ack_observed_at_utc, ack_payload_hash FROM etl_batch_send_attempts WHERE batch_id=$batch AND attempt_id=$attempt;";
        Add(command, "$batch", batchId.ToString("D")); Add(command, "$attempt", attemptId.ToString("D"));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        return await reader.ReadAsync(cancellationToken).ConfigureAwait(false)
            ? new AttemptRow(reader.GetString(0), NullableString(reader, 1), NullableString(reader, 2))
            : null;
    }

    private static async Task<Guid?> ReadBatchRunIdAsync(SqliteConnection connection, SqliteTransaction transaction, Guid batchId, CancellationToken cancellationToken) =>
        ParseGuid(await ScalarStringAsync(connection, transaction, "SELECT run_id FROM etl_batches WHERE batch_id=$batch;", cancellationToken, ("$batch", batchId.ToString("D"))).ConfigureAwait(false));

    private static async Task<(Guid RunId, long RowCount)?> ReadBatchRunRowsAsync(SqliteConnection connection, SqliteTransaction transaction, Guid batchId, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "SELECT run_id,row_count FROM etl_batches WHERE batch_id=$batch;";
        Add(command, "$batch", batchId.ToString("D"));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        return await reader.ReadAsync(cancellationToken).ConfigureAwait(false)
            ? (Guid.Parse(reader.GetString(0)), reader.GetInt64(1))
            : null;
    }

    private static async Task<BatchBoundRow?> ReadBatchBoundAsync(SqliteConnection connection, SqliteTransaction transaction, Guid batchId, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "SELECT run_id,upload_max_attempts,(SELECT COUNT(*) FROM etl_batch_send_attempts a WHERE a.batch_id=etl_batches.batch_id) FROM etl_batches WHERE batch_id=$batch;";
        Add(command, "$batch", batchId.ToString("D"));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        return await reader.ReadAsync(cancellationToken).ConfigureAwait(false)
            ? new BatchBoundRow(Guid.Parse(reader.GetString(0)), NullableLong(reader, 1), reader.GetInt64(2))
            : null;
    }

    private static async Task<EtlBatchSendClaim?> ReadSendClaimAsync(SqliteConnection connection, SqliteTransaction transaction, Guid batchId, Guid attemptId, string ownerId, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "SELECT b.run_id, a.attempt_no, b.upload_max_attempts FROM etl_batches b JOIN etl_batch_send_attempts a ON a.batch_id=b.batch_id AND a.attempt_id=$attempt WHERE b.batch_id=$batch;";
        Add(command, "$batch", batchId.ToString("D")); Add(command, "$attempt", attemptId.ToString("D"));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        return await reader.ReadAsync(cancellationToken).ConfigureAwait(false)
            ? new EtlBatchSendClaim(batchId, Guid.Parse(reader.GetString(0)), attemptId, (int)reader.GetInt64(1), ownerId, (int)(reader.GetInt64(2)))
            : null;
    }

    // Classification after a lost claim flip: only a still-due batch whose persisted
    // bound the presented limit MATCHES (identical-value enforcement), whose admitted
    // count already reached it, with NO live admitted attempt, an extraction-active
    // run and an intact ownership set earns the exhaustion quarantine — every other
    // refusal is strictly read-only.
    private static async Task<Guid?> ClassifyUploadBoundExhaustionAsync(SqliteConnection connection, SqliteTransaction transaction, Guid batchId, int maxAttempts, string now, CancellationToken cancellationToken)
    {
        Guid? runId = null;
        string? entityName = null;
        string? batchStatus = null;
        string? nextAttemptAt = null;
        long? bound = null;
        long attempts = 0;
        long admitted = 0;
        string? runStatus = null;
        await using (var read = connection.CreateCommand())
        {
            read.Transaction = transaction;
            read.CommandText = """
                SELECT b.run_id, b.entity_name, b.status, b.next_attempt_at_utc, b.upload_max_attempts,
                       (SELECT COUNT(*) FROM etl_batch_send_attempts a WHERE a.batch_id=b.batch_id),
                       (SELECT COUNT(*) FROM etl_batch_send_attempts a WHERE a.batch_id=b.batch_id AND a.outcome='admitted'),
                       (SELECT r.status FROM etl_runs r WHERE r.run_id=b.run_id)
                FROM etl_batches b WHERE b.batch_id=$batch;
                """;
            Add(read, "$batch", batchId.ToString("D"));
            await using var reader = await read.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false)) return null;
            runId = ParseGuid(reader.GetString(0));
            entityName = reader.GetString(1);
            batchStatus = reader.GetString(2);
            nextAttemptAt = NullableString(reader, 3);
            bound = NullableLong(reader, 4);
            attempts = reader.GetInt64(5);
            admitted = reader.GetInt64(6);
            runStatus = NullableString(reader, 7);
        }
        if (runId is null || entityName is null) return null;
        var due = string.Equals(batchStatus, "ready", StringComparison.Ordinal)
            || (string.Equals(batchStatus, "retry_waiting", StringComparison.Ordinal)
                && (nextAttemptAt is null || ParseDate(nextAttemptAt) <= ParseDate(now)));
        if (!due || bound is null || bound != maxAttempts || attempts < bound || admitted != 0
            || runStatus is not ("running" or "uploading"))
        {
            return null;
        }
        if (!await RunOwnershipSetHoldsAsync(connection, transaction, runId.Value, cancellationToken).ConfigureAwait(false)) return null;
        if (!await EntityOwnershipHoldsAsync(connection, transaction, runId.Value, entityName, cancellationToken).ConfigureAwait(false)) return null;
        return runId;
    }

    // The fail-closed eager block for an upload outcome: the run and its job block in
    // the same commit, the trigger batch carries the cause quarantine code and every
    // other still-pending sibling batch is fenced RUN_BLOCKED — a blocked run never
    // leaves dispatchable rows. Ownership rows and bindings are RETAINED: a blocked
    // run proves nothing about remote quiescence.
    private static async Task CommitUploadBlockAsync(SqliteConnection connection, SqliteTransaction transaction, Guid runId, Guid batchId, string runConflictCode, string? batchQuarantineCode, string message, string now, CancellationToken cancellationToken)
    {
        await ExecuteAsync(connection, transaction, """
            UPDATE etl_runs SET status='blocked', finished_at_utc=$now, last_error=$message,
                finalize_conflict_code=$code, finalize_conflict_message=$message,
                completion_claim_id=NULL, completion_claim_owner_id=NULL, completion_claim_acquired_at_utc=NULL,
                extraction_claim_id=NULL, extraction_claim_owner_id=NULL, extraction_claim_acquired_at_utc=NULL,
                updated_at_utc=$now, row_version=row_version+1
            WHERE run_id=$run;
            """, cancellationToken,
            ("$now", now), ("$message", message), ("$code", runConflictCode), ("$run", runId.ToString("D"))).ConfigureAwait(false);
        await ExecuteAsync(connection, transaction,
            "UPDATE etl_jobs SET status='blocked', updated_at_utc=$now, row_version=row_version+1 WHERE run_id=$run AND status NOT IN ('finished','cancelled','blocked');",
            cancellationToken, ("$now", now), ("$run", runId.ToString("D"))).ConfigureAwait(false);
        // Still-extracting entities of a 'running' run fail with it (same rule as
        // TerminateRunAsync/recovery) — a blocked run never leaves live work rows.
        await ExecuteAsync(connection, transaction,
            "UPDATE etl_run_entities SET status='failed', last_error=$code, updated_at_utc=$now, row_version=row_version+1 WHERE run_id=$run AND status='extracting';",
            cancellationToken, ("$code", runConflictCode), ("$now", now), ("$run", runId.ToString("D"))).ConfigureAwait(false);
        await ExecuteAsync(connection, transaction, """
            UPDATE etl_batches SET status='dead_letter',
                quarantine_code=CASE WHEN batch_id=$batch THEN $qcode ELSE 'RUN_BLOCKED' END,
                last_error=CASE WHEN batch_id=$batch THEN $message ELSE 'RUN_BLOCKED' END,
                row_version=row_version+1
            WHERE run_id=$run AND status IN ('creating','ready','retry_waiting','uploading');
            """, cancellationToken,
            ("$batch", batchId.ToString("D")), ("$qcode", batchQuarantineCode), ("$message", message), ("$run", runId.ToString("D"))).ConfigureAwait(false);
    }

    // The wire ACK is valid only when every field matches the claimed batch:
    // status 'acknowledged', the exact batch identity, a positive checksum
    // verification and the durable row count — anything else is rejected evidence.
    private static bool IsValidAcknowledgement(EtlBatchAckEvidence ack, Guid batchId, long rowCount) =>
        string.Equals(ack.Status, "acknowledged", StringComparison.Ordinal)
        && ack.BatchId == batchId
        && ack.ChecksumValid == true
        && ack.RowsAccepted == rowCount;

    private static string DescribeAckMismatch(EtlBatchAckEvidence ack, Guid batchId, long rowCount)
    {
        var parts = new List<string>();
        if (!string.Equals(ack.Status, "acknowledged", StringComparison.Ordinal)) parts.Add($"status='{ack.Status ?? "<null>"}'");
        if (ack.BatchId != batchId) parts.Add($"batchId={ack.BatchId?.ToString("D") ?? "<null>"}");
        if (ack.ChecksumValid != true) parts.Add($"checksumValid={ack.ChecksumValid?.ToString() ?? "<null>"}");
        if (ack.RowsAccepted != rowCount) parts.Add($"rowsAccepted={ack.RowsAccepted?.ToString(System.Globalization.CultureInfo.InvariantCulture) ?? "<null>"}");
        return string.Join(", ", parts);
    }

    // Records the FIRST ACK observation on the attempt row only (hash, time, validity).
    private static async Task RecordFirstObservationAsync(SqliteConnection connection, SqliteTransaction transaction, string attemptText, string batchText, string ackPayloadHash, bool ackValid, int? httpStatus, string now, CancellationToken cancellationToken)
    {
        var wrote = await ExecuteAsync(connection, transaction,
            "UPDATE etl_batch_send_attempts SET ack_observed_at_utc=$now, ack_payload_hash=$hash, ack_valid=$valid, http_status=COALESCE(http_status,$http) WHERE attempt_id=$attempt AND batch_id=$batch AND ack_observed_at_utc IS NULL;",
            cancellationToken, ("$attempt", attemptText), ("$batch", batchText), ("$now", now), ("$hash", ackPayloadHash), ("$valid", ackValid ? 1 : 0), ("$http", httpStatus)).ConfigureAwait(false);
        if (wrote != 1) throw new InvalidOperationException($"ETL send attempt {attemptText} changed mid-transaction.");
    }

    private static EtlBatchAckOutcome ReplayOutcome(AttemptRow attempt, string ackPayloadHash) =>
        string.Equals(attempt.AckPayloadHash, ackPayloadHash, StringComparison.Ordinal)
            ? new EtlBatchAckOutcome.AlreadyObserved()
            : new EtlBatchAckOutcome.ObservationConflict();

    // A batch left due (or in flight) after its attempt became uncertain is quarantined
    // UPLOAD_OUTCOME_UNKNOWN. An active run (anything that could still dispatch or
    // finalize) is blocked with its job and siblings via the eager block, ownership
    // retained. A terminal run keeps its own status and failure evidence; only the
    // batch is fenced. Returns false when the batch was already terminal.
    private static async Task<bool> QuarantineStrayBatchAsync(SqliteConnection connection, SqliteTransaction transaction, Guid runId, Guid batchId, string runConflictCode, string message, string now, CancellationToken cancellationToken)
    {
        var batchStatus = await ScalarStringAsync(connection, transaction, "SELECT status FROM etl_batches WHERE batch_id=$batch;", cancellationToken, ("$batch", batchId.ToString("D"))).ConfigureAwait(false);
        if (batchStatus is not ("creating" or "ready" or "retry_waiting" or "uploading")) return false;
        if (await IsRunActiveAsync(connection, transaction, runId, cancellationToken).ConfigureAwait(false))
        {
            await CommitUploadBlockAsync(connection, transaction, runId, batchId, runConflictCode, QuarantineUploadOutcomeUnknown, message, now, cancellationToken).ConfigureAwait(false);
            return true;
        }
        var fenced = await ExecuteAsync(connection, transaction,
            "UPDATE etl_batches SET status='dead_letter', quarantine_code=$qcode, last_error=$message, row_version=row_version+1 WHERE batch_id=$batch AND status IN ('creating','ready','retry_waiting','uploading');",
            cancellationToken, ("$qcode", QuarantineUploadOutcomeUnknown), ("$message", message), ("$batch", batchId.ToString("D"))).ConfigureAwait(false);
        if (fenced != 1) throw new InvalidOperationException($"ETL batch {batchId:D} changed mid-transaction.");
        return true;
    }

    // Active = anything that could still dispatch, upload or finalize. Terminal runs
    // (succeeded/failed/blocked/cancelled) keep their own status and evidence.
    private static async Task<bool> IsRunActiveAsync(SqliteConnection connection, SqliteTransaction transaction, Guid runId, CancellationToken cancellationToken) =>
        await ScalarStringAsync(connection, transaction, "SELECT status FROM etl_runs WHERE run_id=$run;", cancellationToken, ("$run", runId.ToString("D"))).ConfigureAwait(false)
            is "pending" or "running" or "paused" or "uploading" or "completing";

    // Classification after a lost claim flip: a due batch of an extraction-active run
    // whose ledger holds any non-precheck attempt was made due again behind the
    // ledger's back. Ownership is deliberately not required — a broken ownership set
    // is itself grounds to block, never to leave the batch re-sendable.
    private static async Task<Guid?> ClassifyLedgerContradictionAsync(SqliteConnection connection, SqliteTransaction transaction, Guid batchId, string now, CancellationToken cancellationToken)
    {
        var runText = await ScalarStringAsync(connection, transaction, """
            SELECT b.run_id FROM etl_batches b
            WHERE b.batch_id=$batch
              AND (b.status='ready' OR (b.status='retry_waiting' AND (b.next_attempt_at_utc IS NULL OR b.next_attempt_at_utc <= $now)))
              AND EXISTS (SELECT 1 FROM etl_runs r WHERE r.run_id=b.run_id AND r.status IN ('running','uploading'))
              AND EXISTS (SELECT 1 FROM etl_batch_send_attempts x WHERE x.batch_id=b.batch_id AND x.outcome <> 'precheck_failed');
            """, cancellationToken, ("$batch", batchId.ToString("D")), ("$now", now)).ConfigureAwait(false);
        return ParseGuid(runText);
    }
}
