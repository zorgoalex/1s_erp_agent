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

    Task CreateEtlRunAsync(EtlRun run, CancellationToken cancellationToken);
    Task RegisterBatchAsync(EtlBatch batch, CancellationToken cancellationToken);
    Task MarkEtlRunExtractedAsync(Guid runId, CancellationToken cancellationToken);
    Task<IReadOnlyList<EtlBatch>> GetPendingBatchesAsync(int limit, DateTimeOffset nowUtc, CancellationToken cancellationToken);
    Task MarkBatchRetryAsync(Guid batchId, string errorMessage, DateTimeOffset retryAtUtc, CancellationToken cancellationToken);
    Task AcknowledgeBatchAsync(Guid batchId, DateTimeOffset acknowledgedAtUtc, CancellationToken cancellationToken);
    Task<IReadOnlyList<EtlBatch>> GetAcknowledgedBatchesAsync(DateTimeOffset beforeUtc, CancellationToken cancellationToken);
    Task MarkBatchDeletedAsync(Guid batchId, CancellationToken cancellationToken);
    Task<IReadOnlyList<EtlRunCompletion>> GetRunsReadyToCompleteAsync(CancellationToken cancellationToken);
    Task<EtlCursor?> GetCommittedWatermarkAsync(string entityName, CancellationToken cancellationToken);
    Task CommitWatermarkAsync(string entityName, EtlCursor cursor, Guid runId, CancellationToken cancellationToken);
    Task CompleteEtlRunAsync(Guid runId, EtlRunStatus status, string? errorMessage, CancellationToken cancellationToken);

    Task<QueueMetrics> GetQueueMetricsAsync(CancellationToken cancellationToken);
    Task SetStateAsync(string key, string valueJson, CancellationToken cancellationToken);
    Task<string?> GetStateAsync(string key, CancellationToken cancellationToken);
    Task SaveConfigSnapshotAsync(long version, string configurationJson, string hash, string status, CancellationToken cancellationToken);
    Task<ConfigSnapshot> AcceptAndActivateConfigSnapshotAsync(long version, string configurationJson, string hash, CancellationToken cancellationToken);
    Task ActivateConfigSnapshotAsync(long version, CancellationToken cancellationToken);
    Task<ConfigSnapshot?> GetActiveConfigSnapshotAsync(CancellationToken cancellationToken);
    Task CleanupAsync(DateTimeOffset completedBeforeUtc, DateTimeOffset batchesBeforeUtc, CancellationToken cancellationToken);
}

public interface ISpoolStore
{
    Task<EtlBatch> WriteBatchAsync(Guid runId, EtlEntityDefinition entity, IReadOnlyList<System.Text.Json.JsonElement> rows, EtlCursor? watermarkFrom, EtlCursor? watermarkTo, CancellationToken cancellationToken);
    Task<Stream> OpenReadAsync(EtlBatch batch, CancellationToken cancellationToken);
    Task<long> GetSizeAsync(CancellationToken cancellationToken);
    Task QuarantineTemporaryFilesAsync(CancellationToken cancellationToken);
    Task DeleteAcknowledgedAsync(EtlBatch batch, CancellationToken cancellationToken);
}

public interface ISecretStore
{
    Task SaveAsync(string name, string secret, CancellationToken cancellationToken);
    Task<string?> ReadAsync(string name, CancellationToken cancellationToken);
}
