namespace ErpOnecAgent.Application.Abstractions;

// R1 DARK storage contract: attested manual resolution of failed/blocked ETL runs
// (etl-ownership-upload-design.md §8, migration 010). The store verifies the durable
// preconditions; the exclusive-maintenance procedure (workers stopped AND drained) and
// the remote verification at ERP are operator attestations it records but cannot prove.

/// <summary>What may happen after resolution.</summary>
public enum EtlRunResolutionDecision
{
    /// <summary>Nothing is retried; the work is abandoned.</summary>
    Abandon,
    /// <summary>A new command/job or schedule tick may redo the work from the committed watermarks.</summary>
    Retry,
    /// <summary>The next run for these entities must be a new full baseline.</summary>
    Rebaseline
}

/// <summary>
/// One resolution request. <see cref="WorkersQuiesced"/> must be true: it attests that
/// dispatchers and upload workers were stopped and drained — blocking new admissions is
/// not quiescence. <see cref="RemoteVerification"/> states what was verified at ERP.
/// </summary>
public sealed record EtlRunResolutionRequest(
    Guid RunId,
    string OperatorId,
    EtlRunResolutionDecision Decision,
    string RemoteVerification,
    bool WorkersQuiesced);

/// <summary>The immutable resolution record written in the resolution commit.</summary>
public sealed record EtlRunResolutionRecord(
    Guid RunId,
    Guid ResolutionId,
    DateTimeOffset ResolvedAtUtc,
    string OperatorId,
    EtlRunResolutionDecision Decision,
    string RemoteVerification,
    string PriorStatus,
    string? PriorConflictCode,
    int OwnershipReleased,
    int BatchesFenced);

/// <summary>Why a resolution was refused. Every refusal writes nothing.</summary>
public enum EtlRunResolutionRefusal
{
    /// <summary>No such run.</summary>
    RunNotFound,
    /// <summary>The run is not failed or blocked (active, succeeded or cancelled runs are never resolved).</summary>
    RunNotResolvable,
    /// <summary>A send attempt of the run is still 'admitted' — its outcome is unknown and a sender may still act.</summary>
    AdmittedSendAttempt,
    /// <summary>A batch of the run is still 'uploading'. A legacy ledger-less send whose batch was already dead-lettered by a block is NOT visible here — until cutover it is covered only by the workers-quiesced attestation.</summary>
    BatchInFlight,
    /// <summary>The run still holds an extraction claim fence.</summary>
    LiveExtractionClaim,
    /// <summary>The run still holds a completion claim fence.</summary>
    LiveCompletionClaim,
    /// <summary>The run's job is still pending, deferred or running.</summary>
    LiveJobDispatch
}

/// <summary>
/// Typed outcome of <c>ResolveEtlRunAsync</c>. <c>Resolved</c> committed, in ONE
/// transaction: the resolution record, <c>etl_runs.resolved_at_utc</c>, remaining
/// pre-acknowledgement batches fenced (dead_letter, quarantine_code RUN_BLOCKED,
/// last_error RUN_RESOLVED), and the run's epoch-bound active ownership released
/// ('manual_release'). The job stays blocked (never finished); watermarks are untouched.
/// <c>AlreadyResolved</c> returns the existing record with zero writes; <c>SameRequest</c>
/// tells whether the repeated request carried the same operator, decision and verification.
/// <c>Refused</c> writes nothing.
/// </summary>
public abstract record EtlRunResolutionOutcome
{
    private EtlRunResolutionOutcome() { }
    public sealed record Resolved(EtlRunResolutionRecord Record) : EtlRunResolutionOutcome;
    public sealed record AlreadyResolved(EtlRunResolutionRecord Record, bool SameRequest) : EtlRunResolutionOutcome;
    public sealed record Refused(EtlRunResolutionRefusal Reason) : EtlRunResolutionOutcome;
}
