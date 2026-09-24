namespace ErpOnecAgent.Application.Abstractions;

// O1 DARK storage contracts for durable ETL ownership: per-entity lifetime
// ownership with immutable epoch bindings, the run-level extraction claim fence,
// and transactional fair job claim/deferral. Nothing here is wired into workers,
// recovery, or the ERP client; O1 alone is not a safe production state (the send
// ledger is O2, scheduled runs are O3, and every §9 bypass stays open until
// cutover).

/// <summary>
/// Identity of one committed ETL job claim. <see cref="ExtractionClaimId"/> is the fresh
/// GUID minted inside the claim transaction — the authoritative execution fence for the
/// extraction phase, required by every extraction mutation API. <see cref="EntitiesJson"/>
/// is the job's frozen resolved-definition manifest (verbatim storage bytes).
/// </summary>
public sealed record EtlJobClaim(Guid JobId, Guid RunId, Guid ExtractionClaimId, string Mode, string EntitiesJson, long ConfigurationVersion);

/// <summary>Why a claim pass deferred the job instead of dispatching it.</summary>
public enum EtlJobDeferralReason
{
    /// <summary>At least one manifest entity is owned by another active run; ownership/binding writes were rolled back.</summary>
    BusyEntity,
    /// <summary>An older pending run overlaps the manifest; the elder reserves its overlap before any newer claim.</summary>
    QueuedOverlap,
    /// <summary>An older pending run holds an invalid manifest with unproven effects; admission stops pending explicit resolution.</summary>
    ElderManifestInvalid
}

/// <summary>
/// Typed outcome of <c>IAgentStore.TryClaimEtlJobAsync</c>. <c>Claimed</c> means the job,
/// the run transition, all manifest ownership rows and all epoch bindings committed in ONE
/// transaction. <c>Deferred</c> means a guarded deferral committed with zero ownership
/// writes (a partial acquisition is always rolled back to the claim savepoint first).
/// <c>NotClaimable</c> is strictly read-only. <c>Blocked</c> means corrupt durable
/// evidence was quarantined and never dispatched. With unproven prior effects the job
/// blocks while its pending run retains the admission hold.
/// </summary>
public abstract record EtlJobClaimOutcome
{
    private EtlJobClaimOutcome() { }
    public sealed record Claimed(EtlJobClaim Claim) : EtlJobClaimOutcome;
    public sealed record NotClaimable : EtlJobClaimOutcome;
    public sealed record Deferred(EtlJobDeferralReason Reason) : EtlJobClaimOutcome;
    public sealed record Blocked(string Code, string Message) : EtlJobClaimOutcome;
}

/// <summary>One dispatchable durable ETL job surfaced by the eligibility enumeration.</summary>
public sealed record EtlDispatchableJob(Guid JobId, Guid RunId, string Mode, long ConfigurationVersion, string EntitiesJson);

/// <summary>
/// Diagnostic for a non-terminal pending run whose stored manifest fails typed validation —
/// surfaced by enumeration even when no eligible candidate exists; the claim transaction
/// decides quarantine vs. admission hold.
/// </summary>
public sealed record EtlPendingRunQuarantine(Guid RunId, Guid? JobId, string Code, string Message);

/// <summary>
/// One page of dispatchable manual ETL jobs ranked by run (created_at_utc, run_id) —
/// eligibility (no elder overlap reservation, all manifest entities acquirable) is applied
/// in SQL BEFORE LIMIT, so a busy/deferred head can never hide disjoint eligible work —
/// plus the quarantine diagnostics for corrupt pending runs.
/// </summary>
public sealed record EtlJobDispatchPage(IReadOnlyList<EtlDispatchableJob> Jobs, IReadOnlyList<EtlPendingRunQuarantine> Quarantined);
