namespace ErpOnecAgent.Application.Abstractions;

// O2 DARK storage contracts: the durable per-batch send-attempt ledger with
// owner-fenced claim/ACK/outcome APIs and fail-closed unknown-outcome handling
// (etl-ownership-upload-design.md §5 + root disposition item 5, orchestrator
// decisions D1-D3). Nothing here is wired into workers, the ERP client, or
// production dispatch; O1+O2 remain dark until the §9 bypasses are fenced
// atomically at cutover.
//
// An 'admitted' attempt proves a send was ADMITTED under the minted attempt
// fence — never that bytes left the process. Only 'precheck_failed' (the trusted
// worker attestation that the network call was never invoked) may re-arm a
// bounded new attempt; uncertain outcomes (timeout/transport/non-2xx/crash after
// admission) quarantine the batch and block the run with ownership retained.

/// <summary>
/// One batch surfaced by <c>GetDueBatchUploadsAsync</c>: the durable batch identity
/// plus <see cref="PriorAttempts"/> — the number of admitted attempts already in the
/// ledger — so the caller can price the caller-side precheck_failed backoff policy
/// (D2: <c>CommandPolicy.BackoffDelay(attempt, base: 5s, max: 5min)</c> with jitter;
/// the store never invents a schedule and always takes the next-attempt timestamp
/// from the caller).
/// </summary>
public sealed record EtlDueBatchUpload(
    Guid BatchId,
    Guid RunId,
    string EntityName,
    int SchemaVersion,
    string FilePath,
    int RowCount,
    string Sha256,
    int PriorAttempts);

/// <summary>
/// One won send claim. <see cref="AttemptId"/> is the fresh GUID minted inside the
/// claim commit — the exact send-attempt fence identity (never the caller's
/// <see cref="OwnerId"/>, which is diagnostics only). <see cref="AttemptNo"/> is the
/// per-batch monotonic admission sequence. <see cref="UploadMaxAttempts"/> is the
/// durable bound persisted at first claim.
/// </summary>
public sealed record EtlBatchSendClaim(
    Guid BatchId,
    Guid RunId,
    Guid AttemptId,
    int AttemptNo,
    string OwnerId,
    int UploadMaxAttempts);

/// <summary>
/// Typed outcome of <c>TryClaimBatchUploadAsync</c>. <c>Claimed</c> commits the batch
/// flip, the send-attempt fence and the 'admitted' ledger row in ONE transaction.
/// <c>NotClaimed</c> is strictly read-only (lost race, not due, wrong status, live
/// admitted attempt, run not active, ownership set mismatch, or a presented attempt
/// bound that differs from the persisted one). <c>Blocked</c> commits a durable
/// quarantine — bound exhaustion (UPLOAD_ATTEMPTS_EXHAUSTED) or a controlled
/// ledger-write loss (SEND_LEDGER_LOST) — with the batch dead-lettered and the run
/// and job blocked in the same transaction.
/// </summary>
public abstract record EtlBatchUploadClaimOutcome
{
    private EtlBatchUploadClaimOutcome() { }
    public sealed record Claimed(EtlBatchSendClaim Claim) : EtlBatchUploadClaimOutcome;
    public sealed record NotClaimed : EtlBatchUploadClaimOutcome;
    public sealed record Blocked(string Code, string Message) : EtlBatchUploadClaimOutcome;
}

/// <summary>
/// The caller's best-effort parsed ACK evidence for one admitted attempt — every
/// field nullable so malformed wire bodies are still reportable. The store validates
/// <c>Status='acknowledged'</c>, <c>BatchId</c> equal to the claimed batch,
/// <c>ChecksumValid=true</c> and <c>RowsAccepted</c> equal to the durable row_count;
/// a violation under a live fence quarantines batch and run (ACK_INVALID).
/// </summary>
public sealed record EtlBatchAckEvidence(
    string? Status,
    Guid? BatchId,
    long? RowsAccepted,
    bool? ChecksumValid,
    DateTimeOffset? AcknowledgedAtUtc);

/// <summary>
/// Typed outcome of <c>AcknowledgeClaimedBatchAsync</c>. <c>Acknowledged</c> is the
/// fenced apply (batch + attempt + run counter in one commit). A ledger row whose
/// live fence is gone still attaches evidence: <c>LateEvidenceRecorded</c> writes the
/// first ACK observation on that attempt row only; <c>AlreadyObserved</c> is an exact
/// replay of the recorded observation (preserved, no writes); <c>ObservationConflict</c>
/// is a conflicting later observation (explicit, never an overwrite).
/// <c>ClaimLost</c> means no ledger row exists for (batch, attempt) — zero writes.
/// <c>Rejected</c> is an invalid ACK under a live fence: rejected_ack + dead_letter
/// ACK_INVALID + eager run block in one commit. <c>AttestationContradicted</c> is an
/// ACK for an attempt recorded as precheck_failed (attested never sent): the first
/// observation is recorded, the batch is quarantined UPLOAD_OUTCOME_UNKNOWN and an
/// active run is blocked PRECHECK_ATTESTATION_CONTRADICTED. Every observation stores
/// the store's validation of the body in <c>ack_valid</c>; the payload hash is
/// required (it identifies the observation for replay/conflict detection).
/// </summary>
public abstract record EtlBatchAckOutcome
{
    private EtlBatchAckOutcome() { }
    public sealed record Acknowledged : EtlBatchAckOutcome;
    public sealed record AttestationContradicted(string Code, string Message) : EtlBatchAckOutcome;
    public sealed record LateEvidenceRecorded : EtlBatchAckOutcome;
    public sealed record AlreadyObserved : EtlBatchAckOutcome;
    public sealed record ObservationConflict : EtlBatchAckOutcome;
    public sealed record ClaimLost : EtlBatchAckOutcome;
    public sealed record Rejected(string Code, string Message) : EtlBatchAckOutcome;
}

/// <summary>
/// Typed outcome of <c>FailClaimedBatchSendAsync</c> — an uncertain outcome after
/// admission (timeout, transport exception, non-2xx, crash). <c>Blocked</c> commits
/// the fail-closed quarantine: attempt 'unknown', batch dead_letter
/// UPLOAD_OUTCOME_UNKNOWN, run + job blocked, ownership retained — never a resend.
/// <c>LateOutcomeRecorded</c> writes 'unknown' on a still-admitted attempt whose
/// fence is already dead (e.g. run already blocked) — evidence only. If a legacy
/// writer made that batch due again, the dead-fence path also quarantines it and
/// blocks an active run, returning <c>Blocked</c>.
/// <c>ClaimLost</c> means no admitted attempt row — including a late failure report
/// for an already-acknowledged attempt, which is a zero-write no-op.
/// </summary>
public abstract record EtlBatchSendFailureOutcome
{
    private EtlBatchSendFailureOutcome() { }
    public sealed record Blocked(string Code, string Message) : EtlBatchSendFailureOutcome;
    public sealed record LateOutcomeRecorded : EtlBatchSendFailureOutcome;
    public sealed record ClaimLost : EtlBatchSendFailureOutcome;
}

/// <summary>
/// Typed outcome of <c>RetryClaimedBatchSendAsync</c> — admissible ONLY for the
/// trusted precheck_failed attestation (the network call was never invoked).
/// <c>Scheduled</c> commits attempt 'precheck_failed' + batch 'retry_waiting' with
/// the caller-supplied next-attempt timestamp. <c>Blocked</c> commits bound
/// exhaustion (UPLOAD_ATTEMPTS_EXHAUSTED) + eager run block. <c>LateOutcomeRecorded</c>
/// writes the precheck attestation on a still-admitted attempt whose fence is dead.
/// <c>ClaimLost</c> is a stale/foreign/terminal attempt — zero writes.
/// </summary>
public abstract record EtlBatchSendRetryOutcome
{
    private EtlBatchSendRetryOutcome() { }
    public sealed record Scheduled : EtlBatchSendRetryOutcome;
    public sealed record Blocked(string Code, string Message) : EtlBatchSendRetryOutcome;
    public sealed record LateOutcomeRecorded : EtlBatchSendRetryOutcome;
    public sealed record ClaimLost : EtlBatchSendRetryOutcome;
}
