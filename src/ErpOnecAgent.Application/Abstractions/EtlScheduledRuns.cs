using ErpOnecAgent.Domain.Etl;

namespace ErpOnecAgent.Application.Abstractions;

// O3 DARK storage contracts for scheduled ETL runs (migration 009): frozen identity
// and dedup for jobless runs, and scheduled participation in the SAME claim,
// ownership and overlap-priority machinery as manual jobs (O1). Nothing here is
// wired into OnecEtlWorker, recovery or the ERP client; every §9 bypass stays open
// until cutover.

/// <summary>
/// One scheduled tick. <see cref="Entities"/> are the full effective definitions frozen
/// into the run (<c>resolved_entities_json</c>) together with
/// <see cref="ConfigurationVersion"/>; the ordered entity codes become the run manifest.
/// <see cref="Mode"/> must be a supported watermark extraction mode.
/// </summary>
public sealed record EtlScheduledRunRequest(
    string ScheduleKey,
    string Mode,
    IReadOnlyList<EtlEntityDefinition> Entities,
    long ConfigurationVersion);

/// <summary>
/// Typed outcome of <c>EnsureScheduledEtlRunAsync</c>. <c>Created</c> inserted one
/// pending scheduled run. <c>ActiveExisting</c> means an active or unresolved run
/// (pending/running/uploading/completing, or failed/blocked with no resolution) already
/// holds the schedule key: zero writes, and its frozen identity is not replaced.
/// </summary>
public abstract record EtlScheduledRunEnsureOutcome
{
    private EtlScheduledRunEnsureOutcome() { }
    public sealed record Created(Guid RunId) : EtlScheduledRunEnsureOutcome;
    public sealed record ActiveExisting(Guid RunId, string Status) : EtlScheduledRunEnsureOutcome;
}

/// <summary>
/// Identity of one committed scheduled-run claim. <see cref="ExtractionClaimId"/> is the
/// fresh GUID minted in the claim transaction — the extraction fence, exactly as for a
/// job claim. <see cref="ResolvedEntitiesJson"/> is the frozen definition set (verbatim
/// storage text) that Begin compares every caller definition against.
/// </summary>
public sealed record EtlScheduledRunClaim(
    Guid RunId,
    Guid ExtractionClaimId,
    string ScheduleKey,
    string Mode,
    string ResolvedEntitiesJson,
    long ConfigurationVersion);

/// <summary>
/// Typed outcome of <c>TryClaimScheduledRunAsync</c>. <c>Claimed</c> commits the run
/// transition, all manifest ownership rows and epoch bindings in ONE transaction.
/// <c>Deferred</c> leaves the scheduled run pending (it keeps its overlap reservation);
/// a scheduled run has no job row, so the deferral itself writes nothing — only an
/// elder quarantine performed in the same pass may commit. <c>NotClaimable</c> is
/// read-only (missing, not pending, or not a scheduled run). <c>Blocked</c> means the
/// run's frozen identity is corrupt: a provably never-started run is blocked; with
/// unproven prior effects it keeps its pending admission hold and nothing is written.
/// </summary>
public abstract record EtlScheduledRunClaimOutcome
{
    private EtlScheduledRunClaimOutcome() { }
    public sealed record Claimed(EtlScheduledRunClaim Claim) : EtlScheduledRunClaimOutcome;
    public sealed record NotClaimable : EtlScheduledRunClaimOutcome;
    public sealed record Deferred(EtlJobDeferralReason Reason) : EtlScheduledRunClaimOutcome;
    public sealed record Blocked(string Code, string Message) : EtlScheduledRunClaimOutcome;
}

/// <summary>One pending scheduled run surfaced by the eligibility enumeration.</summary>
public sealed record EtlDueScheduledRun(Guid RunId, string ScheduleKey, string Mode, long ConfigurationVersion);
