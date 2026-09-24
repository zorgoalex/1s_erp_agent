using System.Text.Json;

namespace ErpOnecAgent.Domain.Commands;

public enum CommandStatus
{
    Received,
    Queued,
    Executing,
    UnknownResult,
    RetryWaiting,
    SucceededLocal,
    BusinessFailedLocal,
    Expired,
    Cancelled,
    DeadLetter,
    ResultPending,
    Completed
}

public sealed record RequestedBy(string? UserId, string? DisplayName);

public sealed record CommandEnvelope(
    Guid CommandId,
    string CommandType,
    int PayloadVersion,
    int Priority,
    string? OrderingKey,
    Guid? CorrelationId,
    DateTimeOffset CreatedAtUtc,
    DateTimeOffset? NotBeforeUtc,
    DateTimeOffset? ExpiresAtUtc,
    RequestedBy? RequestedBy,
    string PayloadHash,
    JsonElement Payload);

/// <summary>
/// Identity of one durable execution-claim generation (<c>exec_claim_owner_id</c>). A claim is taken
/// atomically before any 1C call of a pass (status lookup or fresh POST) and identifies exactly that
/// pass: a stale ready snapshot from any other pass holds a different owner id and cannot acquire or
/// release that claim while the persisted live claim is held. An explicit store-level stale takeover
/// remains available only through the documented boundary. Owner-aware scheduling transitions
/// also require this exact id; terminal completion is state-guarded but remains outside this owner
/// fence. <c>StaleBeforeUtc</c> is the
/// exclusive cutoff for a claim left over by a dead process that may be taken over (production
/// disables time takeover and relies on the startup <c>RecoverAsync</c>; the parameter keeps the
/// store-level boundary explicit and testable via <see cref="CommandQueueOrder.IsClaimStale"/>). The
/// <c>Current*</c> values are the row state read back in the SAME transaction as the claim, so a pass
/// never executes against a stale snapshot: a snapshot that still looks fresh is re-routed to
/// resolve-before-retry when the live row shows a prior send (version-mismatch-style refresh instead
/// of blind trust in the snapshot).
/// </summary>
public sealed record ExecutionClaim(
    string OwnerId,
    DateTimeOffset AcquiredAtUtc,
    DateTimeOffset StaleBeforeUtc,
    CommandStatus CurrentStatus = CommandStatus.Queued,
    int CurrentAttemptCount = 0,
    int CurrentLookupAttemptCount = 0,
    int CurrentPostAttemptCount = 0,
    DateTimeOffset? CurrentFirstSentAtUtc = null);

public sealed record StoredCommand(
    CommandEnvelope Envelope,
    CommandStatus Status,
    DateTimeOffset ReceivedAtUtc,
    int AttemptCount,
    DateTimeOffset? NextAttemptAtUtc,
    string? LastErrorCode,
    string? LastErrorMessage,
    int LookupAttemptCount = 0,
    int PostAttemptCount = 0,
    DateTimeOffset? FirstSentAtUtc = null,
    long QueueSequence = 0);

/// <summary>
/// Required queue semantics of <c>commands_inbox</c>: a stable total order used both to select ready
/// work and to compute per-<c>ordering_key</c> head-of-line, and the durable claim-ownership rules
/// for fresh execution. Both the ready query and the atomic claims use exactly these definitions, so
/// equal <c>received_at_utc</c> can never produce two heads of one ordering key and two claimants can
/// never both execute the same command.
/// </summary>
public static class CommandQueueOrder
{
    /// <summary>Total order of the queue: priority (desc), then receive time, then the durable admission tie-break.</summary>
    public static int Compare(StoredCommand left, StoredCommand right)
    {
        var byPriority = right.Envelope.Priority.CompareTo(left.Envelope.Priority);
        if (byPriority != 0) return byPriority;
        var byReceivedAt = left.ReceivedAtUtc.CompareTo(right.ReceivedAtUtc);
        return byReceivedAt != 0 ? byReceivedAt : left.QueueSequence.CompareTo(right.QueueSequence);
    }

    /// <summary>Stable sort used by schedulers and tests: the same total order every time.</summary>
    public static IReadOnlyList<StoredCommand> Sort(IEnumerable<StoredCommand> commands) =>
        commands.OrderBy(static command => command, Comparer<StoredCommand>.Create(Compare)).ToArray();

    /// <summary>
    /// Head-of-line of an ordering key under the total order: a row is released only when no earlier
    /// predecessor of the same key is still awaiting a 1C outcome or an ERP result ACK. A locally
    /// completed predecessor in <c>result_pending</c> still blocks its successor (the ERP action is
    /// not yet acknowledged); it releases the successor only once it leaves the queue via
    /// <c>completed</c> (ERP ACK) or a non-deliverable terminal (<c>cancelled</c>/<c>expired</c>/
    /// <c>dead_letter</c>) — preserved baseline ordering-release semantics.
    /// </summary>
    public static bool IsEarlier(StoredCommand predecessor, StoredCommand command) =>
        predecessor.ReceivedAtUtc < command.ReceivedAtUtc
        || (predecessor.ReceivedAtUtc == command.ReceivedAtUtc && predecessor.QueueSequence < command.QueueSequence);

    /// <summary>
    /// Exclusive boundary after which an uncompleted fresh-execution claim is considered stale and may
    /// be taken over: the claim is stale only once the boundary instant has <b>passed</b>, never before.
    /// </summary>
    public static bool IsClaimStale(DateTimeOffset acquiredAtUtc, DateTimeOffset staleBeforeUtc) =>
        acquiredAtUtc > staleBeforeUtc;
}

public enum StoreCommandOutcome
{
    Stored,
    Duplicate,
    PayloadConflict,
    Rejected
}

public sealed record CommandResult(
    Guid CommandId,
    string Status,
    DateTimeOffset CompletedAtUtc,
    JsonElement? Document,
    JsonElement? Error,
    IReadOnlyList<string> Warnings,
    int ResultVersion = 1);

