using ErpOnecAgent.Application.Commands;
using ErpOnecAgent.Domain.Agent;
using ErpOnecAgent.Domain.Commands;
using ErpOnecAgent.Domain.Etl;

namespace ErpOnecAgent.Application.Abstractions;

public sealed record PendingResult(Guid CommandId, string PayloadJson, int AttemptCount);

/// <summary>
/// Identity of one durable command attempt (one row in <c>command_attempts</c>).
/// <see cref="AttemptId"/> is the exact row identity; <see cref="AttemptNo"/> is the per-command
/// monotonic audit sequence (<c>COALESCE(MAX(attempt_no),0)+1</c>), independent of the
/// post/lookup budget counters. <see cref="OperationalAttemptCount"/> preserves the historical
/// meaning of <c>MarkExecutingAsync</c>: the command's operational attempt count used for logging.
/// </summary>
public sealed record CommandAttemptId(Guid AttemptId, int AttemptNo, int OperationalAttemptCount);
public sealed record EtlRunCompletion(Guid RunId, IReadOnlyDictionary<string, EtlCursor> Watermarks, long RowsRead, int BatchCount);
public sealed record ConfigSnapshot(long Version, string ConfigurationJson, string Hash);
public sealed record CommandPayloadConflictEvent(
    Guid EventId,
    Guid CommandId,
    string Code,
    string Severity,
    string OriginalDeclaredHashFingerprint,
    string IncomingDeclaredHashFingerprint,
    string OriginalCommandStatus,
    DateTimeOffset FirstSeenAtUtc,
    DateTimeOffset LastSeenAtUtc,
    long OccurrenceCount);

public interface IAgentStore
{
    Task InitializeAsync(CancellationToken cancellationToken);
    Task RecoverAsync(CancellationToken cancellationToken);
    Task<string> IntegrityCheckAsync(CancellationToken cancellationToken);
    Task BackupAsync(string destinationPath, CancellationToken cancellationToken);
    Task RunMaintenanceAsync(CancellationToken cancellationToken);

    Task<StoreCommandOutcome> StoreCommandAsync(CommandEnvelope command, DateTimeOffset receivedAtUtc, CancellationToken cancellationToken);
    Task<StoreCommandOutcome> AdmitCommandAsync(CommandEnvelope command, DateTimeOffset receivedAtUtc, ValidationResult validation, CancellationToken cancellationToken);
    Task<IReadOnlyList<CommandPayloadConflictEvent>> GetCommandPayloadConflictEventsAsync(int limit, CancellationToken cancellationToken);
    Task<IReadOnlyList<StoredCommand>> GetReadyCommandsAsync(int limit, DateTimeOffset nowUtc, CancellationToken cancellationToken);
    /// <summary>
    /// Sent-work-only variant of <see cref="GetReadyCommandsAsync"/> (A07b resolution/admission
    /// split): identical due/schedule, same-ordering-key head-of-line, ordering and LIMIT semantics,
    /// plus the durable send-evidence predicate
    /// (<c>status='unknown_result' OR attempt_count&gt;0 OR post_attempt_count&gt;0 OR first_sent_at_utc IS NOT NULL</c>)
    /// applied in SQL BEFORE <c>LIMIT</c> so blocked fresh rows can never starve resolution work.
    /// The predicate is the same evidence set the execution pass uses for live-row routing.
    /// </summary>
    Task<IReadOnlyList<StoredCommand>> GetDueSentCommandsAsync(int limit, DateTimeOffset nowUtc, CancellationToken cancellationToken);
    Task<int> MarkExecutingAsync(Guid commandId, CancellationToken cancellationToken);
    Task<CommandAttemptId?> ClaimPostAttemptAsync(Guid commandId, CancellationToken cancellationToken);
    Task<CommandAttemptId?> ClaimPostAttemptAsync(Guid commandId, string claimOwner, CancellationToken cancellationToken);
    /// <summary>
    /// Atomically takes the durable per-pass execution claim for one command before any 1C call
    /// (status lookup or fresh POST) of that pass. The guard checks due schedule (not_before/next_attempt),
    /// current state and same-ordering-key head-of-line in the same transaction and returns the live
    /// row state on the claim. Exactly one claimant wins per command generation: a second claimant
    /// (including one holding a stale ready snapshot) gets <c>null</c> and must not call 1C. Claims of
    /// a dead process are cleared by <c>RecoverAsync</c> at startup; the <c>staleBeforeUtc</c> boundary
    /// (<see cref="CommandQueueOrder.IsClaimStale"/>) stays available for explicit store-level takeover.
    /// </summary>
    Task<ExecutionClaim?> TryAcquireCommandExecutionClaimAsync(Guid commandId, string ownerId, DateTimeOffset acquiredAtUtc, DateTimeOffset staleBeforeUtc, CancellationToken cancellationToken);
    /// <summary>
    /// Releases ONLY the claim owned by <paramref name="ownerId"/> at the end of a pass (success,
    /// scheduling transition or cancellation), so a wrong token has no effect and a newer claim
    /// generation is never released or overwritten. Callers pass an independent cancellation token so
    /// cleanup runs even when the pass itself was cancelled.
    /// </summary>
    Task ReleaseCommandExecutionClaimAsync(Guid commandId, string ownerId, CancellationToken cancellationToken);
    Task<CommandAttemptId?> ClaimLookupAttemptAsync(Guid commandId, string errorCode, string message, DateTimeOffset nextAttemptAtUtc, CancellationToken cancellationToken);
    Task<CommandAttemptId?> ClaimLookupAttemptAsync(Guid commandId, string claimOwner, string errorCode, string message, DateTimeOffset nextAttemptAtUtc, CancellationToken cancellationToken);
    Task CompleteAttemptAsync(Guid attemptId, CommandStatus outcome, string? errorCode, string? errorMessage, int? httpStatus, CancellationToken cancellationToken);
    // Attempt-aware scheduling variants return whether the guarded row transition was applied.
    Task<bool> MarkUnknownResultAsync(Guid commandId, Guid attemptId, string errorCode, string message, DateTimeOffset? nextAttemptAtUtc, CancellationToken cancellationToken);
    Task<bool> MarkUnknownResultAsync(Guid commandId, Guid attemptId, string claimOwner, string errorCode, string message, DateTimeOffset? nextAttemptAtUtc, CancellationToken cancellationToken);
    // Scheduling-only variants are used when no attempt identity is available. Legacy variants are
    // deliberately unclaimed-only; owner-aware variants require the exact persisted claim owner.
    Task<bool> MarkUnknownResultAsync(Guid commandId, string errorCode, string message, DateTimeOffset? nextAttemptAtUtc, CancellationToken cancellationToken);
    Task<bool> MarkUnknownResultAsync(Guid commandId, string claimOwner, string errorCode, string message, DateTimeOffset? nextAttemptAtUtc, CancellationToken cancellationToken);
    Task<bool> ScheduleRetryAsync(Guid commandId, string errorCode, string message, DateTimeOffset retryAtUtc, CancellationToken cancellationToken);
    Task<bool> ScheduleRetryAsync(Guid commandId, string claimOwner, string errorCode, string message, DateTimeOffset retryAtUtc, CancellationToken cancellationToken);
    Task<bool> CompleteLocallyAsync(Guid commandId, CommandStatus localStatus, string resultJson, string? externalRef, string? externalNumber, Guid? resolvedAttemptId, CancellationToken cancellationToken);
    Task<bool> CompleteLocallyAsync(Guid commandId, string claimOwner, CommandStatus localStatus, string resultJson, string? externalRef, string? externalNumber, Guid? resolvedAttemptId, CancellationToken cancellationToken);
    Task<IReadOnlyList<PendingResult>> GetPendingResultsAsync(int limit, DateTimeOffset nowUtc, CancellationToken cancellationToken);
    Task<bool> MarkResultRetryAsync(Guid commandId, string errorMessage, DateTimeOffset retryAtUtc, CancellationToken cancellationToken);
    Task<bool> AcknowledgeResultAsync(Guid commandId, DateTimeOffset acknowledgedAtUtc, CancellationToken cancellationToken);

    /// <summary>
    /// Atomically accepts one durable manual ETL job under the CURRENT persisted execution claim of
    /// its command. In ONE SQLite transaction the store guards, on the current saved row, that the
    /// command is still in a never-sent state (<c>queued</c>/<c>retry_waiting</c>, matching
    /// <paramref name="claimOwner"/>, no attempt/post/first-send evidence, no existing job, not past
    /// <c>expires_at_utc</c>), that the saved <c>command_type</c> canonically maps to the requested
    /// mode (<c>start_full_sync → bootstrap_full</c>, <c>reload_entity → entity_reload</c>; any
    /// other type refuses), and that the resolved selection corresponds to the saved payload
    /// (<c>reload_entity</c>: exactly one resolved entity matching <c>payload.entity</c>;
    /// <c>start_full_sync</c>: an explicit non-empty <c>payload.entities</c> list must equal the
    /// resolved selection — an omitted/empty list means the caller-resolved enabled set). Only then
    /// does it insert the pending run, the durable job (stable job/run ids, frozen entity
    /// definitions + config version, the canonical saved payload hash, the exact acceptance
    /// result), resolve the command to <c>result_pending</c> with the
    /// <c>succeeded</c>/<c>accepted:true/runId/mode</c> result, and create the results_outbox row.
    /// A repeated commandId replays the original acceptance identity (<c>AlreadyAccepted</c>) —
    /// never a second job and never a rewrite of terminal/delivery history. Every
    /// <c>NotApplied</c> outcome (foreign/stale/missing owner, wrong type/mode/selection, expired,
    /// non-active state, send evidence, or a payload conflicting with an existing job) is strictly
    /// read-only: nothing is written, including conflict evidence.
    /// </summary>
    Task<EtlJobAcceptanceOutcome> AcceptEtlJobAndCompleteCommandAsync(Guid commandId, string claimOwner, EtlJobAcceptanceRequest request, CancellationToken cancellationToken);

    // --- O1 dark storage APIs: durable per-entity ownership + the run extraction
    // claim fence (isolated new path; NOT wired into workers, recovery, or the ERP
    // client — see EtlOwnership.cs header). O1 alone is not a safe production state.

    /// <summary>
    /// One-transaction fair claim of a durable manual ETL job. The guarded probe write
    /// serializes claimants on the job row; inside the transaction the store verifies the
    /// frozen job identity (run pending, mode/configuration_version agree, frozen
    /// entities_json manifest equals the run's requested_entities_json — violations block
    /// job+run JOB_MANIFEST_INCONSISTENT), quarantines provably-never-started elders with
    /// invalid manifests (unproven elders defer the claimant elder_manifest_invalid),
    /// enforces the elder-overlap reservation (queued_overlap), then under a SAVEPOINT
    /// acquires ownership of EVERY manifest entity and writes the immutable epoch
    /// bindings — any conflict rolls the partial acquisition back and defers busy_entity.
    /// On success the run becomes 'running' and a fresh GUID extraction claim is minted —
    /// the fence every extraction mutation requires. claim_attempt_count counts committed
    /// claims only. Ownership is retained through failure/restart and released only
    /// inside the successful finalize commit.
    /// </summary>
    Task<EtlJobClaimOutcome> TryClaimEtlJobAsync(Guid jobId, string ownerId, DateTimeOffset deferUntilUtc, DateTimeOffset nowUtc, CancellationToken cancellationToken);

    /// <summary>
    /// Enumerates dispatchable manual ETL jobs: pending (or deferred-and-due) jobs whose
    /// pending run has no elder-overlap reservation and whose manifest entities are all
    /// acquirable — both predicates applied in SQL BEFORE LIMIT, ranked by run
    /// (created_at_utc, run_id), so a busy/deferred head never hides disjoint work and the
    /// enumerator can never bypass the in-transaction claim authority. Quarantine
    /// diagnostics for corrupt pending runs are surfaced even when no candidate is
    /// eligible.
    /// </summary>
    Task<EtlJobDispatchPage> GetDispatchableEtlJobsAsync(int limit, DateTimeOffset nowUtc, CancellationToken cancellationToken);

    /// <summary>
    /// O3: idempotent scheduled tick. Inserts one pending jobless run carrying
    /// <c>schedule_key</c>, the ordered entity-code manifest, the frozen full definitions
    /// (<c>resolved_entities_json</c>) and the configuration version — unless an active or
    /// unresolved run already holds the key (pending/running/uploading/completing, or
    /// failed/blocked without <c>resolved_at_utc</c>), in which case it returns that run
    /// with zero writes. A failed or blocked scheduled run therefore never mints a
    /// successor. Invalid requests (blank key, unsupported mode, empty/duplicate/invalid
    /// definitions, negative configuration version) throw <see cref="ArgumentException"/>.
    /// </summary>
    Task<EtlScheduledRunEnsureOutcome> EnsureScheduledEtlRunAsync(EtlScheduledRunRequest request, DateTimeOffset nowUtc, CancellationToken cancellationToken);

    /// <summary>
    /// O3: claims a pending scheduled run through the SAME transaction shape as
    /// <see cref="TryClaimEtlJobAsync"/> — elder-manifest quarantine, elder-overlap
    /// reservation by run (created_at_utc, run_id) shared with manual jobs, all-entity
    /// ownership acquisition with immutable bindings under a savepoint, and the fresh
    /// extraction claim — with <c>owner_job_id</c> NULL. The frozen identity
    /// (<c>resolved_entities_json</c> codes equal to the manifest) is verified in the
    /// transaction; corrupt identity never dispatches.
    /// </summary>
    Task<EtlScheduledRunClaimOutcome> TryClaimScheduledRunAsync(Guid runId, string ownerId, DateTimeOffset nowUtc, CancellationToken cancellationToken);

    /// <summary>
    /// O3: enumerates pending scheduled runs that the claim would admit — consistent frozen
    /// identity, no elder-overlap reservation, all manifest entities acquirable — applied in
    /// SQL BEFORE LIMIT, ranked by run (created_at_utc, run_id). Read-only.
    /// </summary>
    Task<IReadOnlyList<EtlDueScheduledRun>> GetDueScheduledRunsAsync(int limit, DateTimeOffset nowUtc, CancellationToken cancellationToken);

    /// <summary>
    /// R1 (§8): attested manual resolution of a failed or blocked ETL run. Durable
    /// preconditions are verified in the transaction: the run is failed/blocked, no send
    /// attempt of its batches is 'admitted', no batch is 'uploading', no extraction or
    /// completion claim is live, and its job is not pending/deferred/running. A legacy
    /// ledger-less send whose batch a block already dead-lettered is not durably visible —
    /// until cutover it rests on the workers-quiesced attestation. The request
    /// must attest that workers were stopped and drained and state the remote verification
    /// (invalid requests throw <see cref="ArgumentException"/>). One commit writes the
    /// immutable resolution record and resolved_at_utc, fences remaining pre-acknowledgement
    /// batches and releases the run's epoch-bound ownership ('manual_release'); watermarks
    /// and the job's blocked status are untouched. A resolved run releases its schedule key.
    /// </summary>
    Task<EtlRunResolutionOutcome> ResolveEtlRunAsync(EtlRunResolutionRequest request, DateTimeOffset nowUtc, CancellationToken cancellationToken);

    /// <summary>
    /// D1: attested watermark domain reset — the explicit exit from DOMAIN_CHANGED and
    /// DOMAIN_UNKNOWN. Under a generation CAS and only while no active run owns the entity,
    /// ONE transaction archives the row verbatim (watermark_domain_resets) and removes it;
    /// the next extraction must then be a full baseline (an incremental read is refused
    /// with BaselineRequired). Refusals write nothing; invalid requests throw.
    /// </summary>
    Task<EtlWatermarkDomainResetOutcome> ResetEtlWatermarkDomainAsync(EtlWatermarkDomainResetRequest request, DateTimeOffset nowUtc, CancellationToken cancellationToken);

    Task<IReadOnlyList<EtlBatch>> GetAcknowledgedBatchesAsync(DateTimeOffset beforeUtc, CancellationToken cancellationToken);
    Task MarkBatchDeletedAsync(Guid batchId, CancellationToken cancellationToken);
    Task<EtlCursor?> GetCommittedWatermarkAsync(string entityName, CancellationToken cancellationToken);

    // --- A05b F1 dark storage APIs (isolated new path; NOT wired into workers,
    // recovery, or the ERP client — see EtlFinalize.cs header). Existing v5 writers
    // above still bypass generation/seal and must be retired/fenced at cutover (F2).

    /// <summary>
    /// Atomically captures the entity's expected watermark base BEFORE extraction in one
    /// transaction — under the run's live extraction claim fence (the fresh GUID minted by
    /// the committed job claim; stale/foreign/missing claims reject with zero writes) and
    /// the exact ownership gate (the run's valid manifest must equal its immutable epoch
    /// bindings AND its active ownership rows — a released/re-acquired row fails closed
    /// even for the same owner). Then: row presence, generation, raw committed cursor
    /// text, and stored domain fingerprint — classifying the domain fail-closed
    /// (absent|same proceed; changed|unknown — and an incremental read without a base,
    /// BaselineRequired — write a 'failed' entity row with its failure code and reject, so a
    /// partial run can skip the entity; a
    /// missing source namespace or a definition conflicting with the run's frozen etl_job
    /// definition rejects with zero writes). A run without an etl_jobs row is rejected
    /// outright — O1 has no frozen identity source for jobless runs (scheduled identity
    /// is O3). The returned base is the exact committed cursor the query must use —
    /// recorded base and query base are identical by construction.
    /// </summary>
    Task<EtlEntityBeginOutcome> BeginEtlEntityExtractionAsync(Guid runId, Guid extractionClaimId, EtlEntityExtractionRequest request, CancellationToken cancellationToken);

    /// <summary>
    /// Guarded batch registration for the new path only — under the run's live extraction
    /// claim fence and exact ownership gate: inserts the batch and bumps the entity and
    /// run counters in one transaction, but only while the run is 'running', unsealed,
    /// claim-matched and exactly owned, and the entity row is still 'extracting'. A late
    /// batch after done/seal/completing, a stale claim, or a broken ownership set is
    /// rejected and moves no counters. The legacy RegisterBatchAsync is unchanged and
    /// still unguarded.
    /// </summary>
    Task<EtlBatchRegistrationOutcome> RegisterGuardedEtlBatchAsync(EtlBatch batch, Guid extractionClaimId, CancellationToken cancellationToken);

    /// <summary>
    /// Durably completes one extracting entity — under the run's live extraction claim
    /// fence and exact ownership gate — with an explicit typed final cursor (valid JSON
    /// object, at least one non-NULL component — never order-derived) and the expected
    /// batch count, which must equal the durable per-entity batch counter. Entity
    /// metadata and counts freeze after this.
    /// </summary>
    Task<EtlEntityCompletionOutcome> CompleteEtlEntityExtractionAsync(Guid runId, Guid extractionClaimId, string entityName, string finalWatermarkJson, int expectedBatchCount, CancellationToken cancellationToken);

    /// <summary>
    /// Partial runs: marks an 'extracting' entity of the claimed, unsealed run as failed at the
    /// source, under the same write-first fence as completion. Its already-registered batches
    /// stay (they may be uploading) and are frozen as its expected count; its watermark is
    /// never committed, so the next run retries it. Zero writes on any fence miss.
    /// </summary>
    Task<EtlEntityFailureOutcome> FailEtlEntityExtractionAsync(Guid runId, Guid extractionClaimId, string entityName, string failureCode, string failureMessage, CancellationToken cancellationToken);

    /// <summary>
    /// Seals extraction output in one transaction — under the run's live extraction claim
    /// fence and exact ownership gate: the validated requested manifest must equal the
    /// entity-row set exactly, every entity must be 'done' with a valid non-empty final — or
    /// skipped ('failed' with a failure code, no final; not all of them),
    /// and per-entity/run counters must match the actual batch rows. On success the run
    /// becomes 'uploading' with frozen seal counts and the extraction claim is cleared —
    /// the extraction fence ends at seal; afterwards every new-path mutation API is
    /// fenced.
    /// </summary>
    Task<EtlRunSealOutcome> SealEtlRunExtractionAsync(Guid runId, Guid extractionClaimId, CancellationToken cancellationToken);

    /// <summary>
    /// fail_run only: guarded 'running' + live-extraction-claim → 'failed'; a stale or
    /// foreign claim can never terminate a newer execution. Termination may preserve or
    /// block a run whose ownership evidence is corrupted — it never releases ownership.
    /// Still-extracting entities fail; pending batches (creating/ready/retry_waiting/
    /// uploading) are fenced to dead_letter; the job becomes 'blocked', never 'finished'.
    /// A SQL status flip can never cancel an HTTP upload already in flight or recall
    /// remote effects.
    /// </summary>
    Task<EtlRunTerminationOutcome> FailEtlRunAsync(Guid runId, Guid extractionClaimId, string errorMessage, CancellationToken cancellationToken);

    /// <summary>Same shape as FailEtlRunAsync to 'blocked' with a conflict code (domain/seal/claim violations, legacy quarantine), under the live extraction claim.</summary>
    Task<EtlRunTerminationOutcome> BlockEtlRunAsync(Guid runId, Guid extractionClaimId, string code, string message, CancellationToken cancellationToken);

    /// <summary>Runs due for a completion attempt: sealed 'uploading' runs, or 'completing' runs with a released claim whose retry is due.</summary>
    Task<IReadOnlyList<EtlRunCompletionCandidate>> GetDueRunCompletionsAsync(int limit, DateTimeOffset nowUtc, CancellationToken cancellationToken);

    /// <summary>
    /// One-transaction claim: verifies the full readiness invariant in-tx (sealed, exact
    /// manifest/entity set, all entities done with valid finals, exact acknowledged batch
    /// counts, no corrupt state — and the exact three-way ownership equality: valid
    /// manifest == immutable epoch bindings == active ownership rows at the bound
    /// positive epochs, both directions, or the run commits 'blocked'
    /// OWNERSHIP_SET_MISMATCH), then mints a NEW GUID claim identity, bumps the bounded
    /// attempt counter, and writes the immutable complete payload on first claim only.
    /// A legacy or corrupted 'uploading' run commits 'blocked' with evidence instead of
    /// being claimed; a transient not-yet-acknowledged run rolls back untouched.
    /// <paramref name="maxAttempts"/> is the completion-attempt budget enforced at claim
    /// admission: the first successful claim persists it on the run row, every later
    /// claim must present the identical limit, and once the stored attempt count reaches
    /// the bound no further send is admissible — a released due claim instead commits the
    /// run 'blocked' COMPLETION_ATTEMPTS_EXHAUSTED with payload and evidence preserved.
    /// </summary>
    Task<EtlRunClaimOutcome> TryClaimRunCompletionAsync(Guid runId, string ownerId, DateTimeOffset nextAttemptAtUtc, int maxAttempts, CancellationToken cancellationToken);

    /// <summary>
    /// Atomic finalize under the exact claim identity: re-verifies the seal AND the exact
    /// three-way ownership equality (mismatch commits blocked OWNERSHIP_SET_MISMATCH),
    /// then applies every entity's presence+generation+cursor+domain CAS inside one
    /// savepoint together with the epoch-bound ownership release — the release must change
    /// exactly sealed_entity_count rows or every watermark write AND the partial release
    /// roll back to the savepoint and one commit writes blocked run +
    /// OWNERSHIP_RELEASE_MISMATCH + blocked job. All watermarks + succeeded run +
    /// finished job + exactly-manifest ownership release commit together; a stale claim
    /// loses silently with zero writes; a SQL/fault exception rolls the whole transaction
    /// back preserving the completing claim for a fenced retry.
    /// </summary>
    Task<EtlRunFinalizeOutcome> FinalizeEtlRunAsync(Guid runId, Guid claimId, CancellationToken cancellationToken);

    /// <summary>
    /// Fenced retry mark under the exact claim identity: releases the claim (so the next
    /// claim mints a fresh identity), preserves the stored payload verbatim, and schedules
    /// the bounded next attempt; reaching the attempt bound commits 'blocked'
    /// COMPLETION_ATTEMPTS_EXHAUSTED plus a blocked job.
    /// </summary>
    Task<EtlRunCompletionRetryOutcome> MarkRunCompletionRetryAsync(Guid runId, Guid claimId, string errorMessage, DateTimeOffset nextAttemptAtUtc, int maxAttempts, CancellationToken cancellationToken);

    /// <summary>
    /// Explicit dead-process recovery — startup-only, exclusive-host precondition, NOT
    /// used by RecoverAsync and never a live/time-based takeover: releases every
    /// 'completing' claim (payload and evidence preserved), orphans every still-
    /// 'admitted' send attempt (O2 — a dead owner can never produce a real outcome),
    /// quarantines every 'uploading' batch UPLOAD_OUTCOME_UNKNOWN (a new-path
    /// 'uploading' batch is NEVER reset to 'ready'), blocks their runs, then blocks
    /// 'running' runs INTERRUPTED_NO_CHECKPOINT and clears their dead extraction
    /// claims, marks their extracting entities failed, fences their pending batches to
    /// dead_letter, and blocks their jobs — ownership rows and bindings are RETAINED
    /// in all cases (they are the evidence of exactly what the interrupted run owned).
    /// 'pending' runs untouched.
    /// </summary>
    Task<EtlRecoveryResult> RecoverInterruptedEtlRunsAsync(CancellationToken cancellationToken);

    /// <summary>
    /// C1 upgrade fence (startup-only, exclusive host): every running/uploading/completing run
    /// without ownership bindings was written by the legacy pipeline and can never reach the
    /// new path's readiness. It is blocked LEGACY_UNRESOLVED with its remaining
    /// pre-acknowledgement batches fenced (RUN_BLOCKED), evidence preserved for R1 resolution.
    /// Returns the number of runs blocked.
    /// </summary>
    Task<int> BlockLegacyEtlRunsAsync(CancellationToken cancellationToken);

    /// <summary>C1 spool reconciliation: file paths referenced by every non-deleted batch row.</summary>
    Task<IReadOnlySet<string>> GetReferencedBatchFilePathsAsync(CancellationToken cancellationToken);

    /// <summary>
    /// C1 spool reconciliation (startup-only, exclusive host): every not-yet-sent batch
    /// (ready/retry_waiting) whose spool file no longer exists is dead-lettered RUN_BLOCKED with
    /// last_error SPOOL_FILE_MISSING, the rest of its run's pending batches are fenced RUN_BLOCKED, and
    /// the run is blocked SPOOL_FILE_MISSING (ownership retained, evidence preserved for R1).
    /// Returns the number of batches found missing.
    /// </summary>
    Task<int> BlockRunsWithMissingSpoolFilesAsync(Func<string, bool> fileExists, CancellationToken cancellationToken);

    // --- O2 dark storage APIs: the durable admitted-attempt send ledger with
    // owner-fenced claim/ACK/outcome and fail-closed unknown-outcome handling
    // (isolated new path; NOT wired into workers, recovery wiring, or the ERP
    // client — see EtlSendAttempts.cs header). Admission and ACK application require
    // the FULL run ownership/binding three-way equality at positive epoch — the same
    // O1 predicate — never a per-entity shortcut. An 'admitted' attempt is proof of
    // admission only; uncertain outcomes quarantine and block, never replay.

    /// <summary>
    /// Batches admissible for upload right now: 'ready' (or due 'retry_waiting' from a
    /// ledger-proven precheck failure) batches whose run is 'running'/'uploading' and
    /// whose entity carries active ownership for that run, with no live 'admitted'
    /// attempt — eligibility evaluated in SQL BEFORE LIMIT, ordered by
    /// (created_at_utc, batch_id). The claim transaction re-verifies every predicate
    /// plus the full three-way ownership set; enumeration can never bypass it.
    /// </summary>
    Task<IReadOnlyList<EtlDueBatchUpload>> GetDueBatchUploadsAsync(int limit, DateTimeOffset nowUtc, CancellationToken cancellationToken);

    /// <summary>
    /// One-transaction send claim: a guarded write-first flip of the batch to
    /// 'uploading' + the fresh send_attempt_id fence + the 'admitted' ledger row +
    /// the persisted upload-attempt bound — all in ONE commit. The guarded flip holds
    /// only while the batch is 'ready'/due-'retry_waiting', the run is
    /// 'running'/'uploading', the FULL run ownership/binding set equality holds at
    /// positive epoch, the batch entity is epoch-bound owned, no live 'admitted'
    /// attempt exists, and the admitted-attempt count is below the durable bound.
    /// <paramref name="maxAttempts"/> is the caller's policy bound (D1): persisted at
    /// first claim, identical-value enforced on every later claim — a mismatch is
    /// refused with zero writes. Bound exhaustion commits dead_letter
    /// UPLOAD_ATTEMPTS_EXHAUSTED + eager run block; a swallowed ledger write (controlled
    /// zero-row outcome) rolls the flip back to the savepoint then commits a block;
    /// a thrown SQL error rolls back the whole transaction leaving the batch claimable.
    /// </summary>
    Task<EtlBatchUploadClaimOutcome> TryClaimBatchUploadAsync(Guid batchId, string ownerId, DateTimeOffset nowUtc, int maxAttempts, CancellationToken cancellationToken);

    /// <summary>
    /// Applies an ERP batch ACK bound to the durable admitted attempt — never to a
    /// bare batch id. Fenced apply requires batch 'uploading' + send_attempt_id match +
    /// attempt 'admitted' + run active + the full bound-epoch ownership set; a valid
    /// ACK then acknowledges batch + attempt and bumps the run counter in one commit.
    /// An invalid ACK under a live fence commits rejected_ack + dead_letter ACK_INVALID
    /// + eager run block. When the live fence is gone but the (batch_id, attempt_id)
    /// ledger row exists, the ACK is recorded as observation evidence on that attempt
    /// row only (first observation wins; exact replay is preserved; a conflicting later
    /// observation is explicit and never overwrites); if that batch was made due again
    /// outside the ledger it is quarantined and an active run blocked. An ACK for a
    /// precheck_failed attempt is AttestationContradicted: evidence recorded, an active
    /// run blocked even when a later attempt acknowledged the batch. Every observation
    /// records the store's validation of the body (ack_valid). <paramref name="ackPayloadHash"/>
    /// is required (non-empty) — it identifies the observation for replay/conflict
    /// detection. A foreign attempt id is ClaimLost with zero writes.
    /// </summary>
    Task<EtlBatchAckOutcome> AcknowledgeClaimedBatchAsync(Guid batchId, Guid attemptId, EtlBatchAckEvidence acknowledgement, string? ackPayloadHash, int? httpStatus, CancellationToken cancellationToken);

    /// <summary>
    /// Fail-closed uncertain outcome after admission (timeout, transport exception,
    /// non-2xx, process death): under the live fence commits attempt 'unknown' + batch
    /// dead_letter UPLOAD_OUTCOME_UNKNOWN + run blocked + job blocked in one
    /// transaction — ownership and bindings retained, the batch is never re-claimable,
    /// and no sibling batch of the blocked run is admissible afterwards (eager block).
    /// A late report for a still-'admitted' attempt whose fence is already dead writes
    /// 'unknown' on the attempt only; a report for a terminal (e.g. already-acknowledged)
    /// attempt or a foreign attempt id is ClaimLost — zero writes.
    /// </summary>
    Task<EtlBatchSendFailureOutcome> FailClaimedBatchSendAsync(Guid batchId, Guid attemptId, string errorMessage, int? httpStatus, CancellationToken cancellationToken);

    /// <summary>
    /// Records the trusted precheck_failed attestation — the worker positively never
    /// invoked the network call — under the live fence: attempt 'precheck_failed' +
    /// batch 'retry_waiting' + the caller-supplied <paramref name="nextAttemptAtUtc"/>
    /// (D2: the store invents no schedule; caller policy is BackoffDelay(attempt,
    /// 5s, 5min) + jitter). The durable bound persisted at first claim is
    /// authoritative; exhaustion commits dead_letter UPLOAD_ATTEMPTS_EXHAUSTED + eager
    /// run block. Any post-invocation failure uses
    /// the fail path instead; a stale/foreign attempt id is ClaimLost, zero writes.
    /// </summary>
    Task<EtlBatchSendRetryOutcome> RetryClaimedBatchSendAsync(Guid batchId, Guid attemptId, string errorMessage, DateTimeOffset nextAttemptAtUtc, CancellationToken cancellationToken);

    Task<QueueMetrics> GetQueueMetricsAsync(CancellationToken cancellationToken);
    Task SetStateAsync(string key, string valueJson, CancellationToken cancellationToken);
    Task<string?> GetStateAsync(string key, CancellationToken cancellationToken);
    Task SaveConfigSnapshotAsync(long version, string configurationJson, string hash, string status, CancellationToken cancellationToken);
    Task<ConfigSnapshot> AcceptAndActivateConfigSnapshotAsync(long version, string configurationJson, string hash, CancellationToken cancellationToken);
    Task ActivateConfigSnapshotAsync(long version, CancellationToken cancellationToken);
    Task<ConfigSnapshot?> GetActiveConfigSnapshotAsync(CancellationToken cancellationToken);
    Task CleanupAsync(DateTimeOffset completedBeforeUtc, DateTimeOffset batchesBeforeUtc, CancellationToken cancellationToken);
}

/// <summary>
/// C1: the legacy (pre-O1) ETL writers — each one bypasses ownership, the extraction claim,
/// the seal, the send ledger or the atomic finalize (design §9). They are fenced OUT of
/// <see cref="IAgentStore"/>, so production code cannot reach them. They remain only for
/// historical tests and upgrade fixtures that reproduce legacy rows.
/// </summary>
public interface ILegacyEtlStore
{
    Task CreateEtlRunAsync(EtlRun run, CancellationToken cancellationToken);
    Task RegisterBatchAsync(EtlBatch batch, CancellationToken cancellationToken);
    Task MarkEtlRunExtractedAsync(Guid runId, CancellationToken cancellationToken);
    Task<IReadOnlyList<EtlBatch>> GetPendingBatchesAsync(int limit, DateTimeOffset nowUtc, CancellationToken cancellationToken);
    Task MarkBatchRetryAsync(Guid batchId, string errorMessage, DateTimeOffset retryAtUtc, CancellationToken cancellationToken);
    Task AcknowledgeBatchAsync(Guid batchId, DateTimeOffset acknowledgedAtUtc, CancellationToken cancellationToken);
    Task<IReadOnlyList<EtlRunCompletion>> GetRunsReadyToCompleteAsync(CancellationToken cancellationToken);
    Task CommitWatermarkAsync(string entityName, EtlCursor cursor, Guid runId, CancellationToken cancellationToken);
    Task CompleteEtlRunAsync(Guid runId, EtlRunStatus status, string? errorMessage, CancellationToken cancellationToken);
}

public interface ISpoolStore
{
    Task<EtlBatch> WriteBatchAsync(Guid runId, EtlEntityDefinition entity, IReadOnlyList<System.Text.Json.JsonElement> rows, EtlCursor? watermarkFrom, EtlCursor? watermarkTo, CancellationToken cancellationToken);
    Task<Stream> OpenReadAsync(EtlBatch batch, CancellationToken cancellationToken);
    Task<long> GetSizeAsync(CancellationToken cancellationToken);
    Task QuarantineTemporaryFilesAsync(CancellationToken cancellationToken);
    Task DeleteAcknowledgedAsync(EtlBatch batch, CancellationToken cancellationToken);

    /// <summary>C1: moves every ready spool file not in <paramref name="referencedPaths"/> to quarantine (a crash between file rename and batch registration). Returns the moved count.</summary>
    Task<int> QuarantineUnreferencedReadyFilesAsync(IReadOnlySet<string> referencedPaths, CancellationToken cancellationToken) => Task.FromResult(0);
}

public interface ISecretStore
{
    Task SaveAsync(string name, string secret, CancellationToken cancellationToken);
    Task<string?> ReadAsync(string name, CancellationToken cancellationToken);
}
