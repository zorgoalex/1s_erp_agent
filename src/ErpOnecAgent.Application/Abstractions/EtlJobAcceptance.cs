using ErpOnecAgent.Domain.Etl;

namespace ErpOnecAgent.Application.Abstractions;

/// <summary>
/// Frozen acceptance input for one durable manual ETL job. <see cref="Mode"/> is bounded to the
/// authorized manual modes (<c>bootstrap_full</c>, <c>entity_reload</c>); reconcile modes are not
/// accepted and can never masquerade as another mode. <see cref="Entities"/> is the fully resolved,
/// non-empty set of effective <see cref="EtlEntityDefinition"/>s serialized verbatim into the job,
/// and <see cref="ConfigurationVersion"/> is the configuration version they were resolved against.
/// </summary>
public sealed record EtlJobAcceptanceRequest(string Mode, IReadOnlyList<EtlEntityDefinition> Entities, long ConfigurationVersion);

/// <summary>Stable identity of one accepted durable ETL job: the job/run pair fixed at acceptance and the exact stored acceptance result.</summary>
public sealed record EtlAcceptedJob(Guid JobId, Guid RunId, string Mode, string AcceptanceResultJson);

/// <summary>
/// Why an acceptance call did not create a job. Every <c>NotApplied</c> outcome is strictly
/// read-only: the command, its claim, any existing job/run, results, outbox rows, and conflict
/// history are never mutated — a changed payload is refused without recording anything.
/// </summary>
public enum EtlJobAcceptanceRejection
{
    /// <summary>No saved inbox row exists for the command id.</summary>
    CommandNotFound,
    /// <summary>The persisted execution claim is held by a different owner (foreign, stale, or already released).</summary>
    ClaimNotOwned,
    /// <summary>The live row left the never-sent acceptance state (executing/unknown/terminal status or persisted send evidence).</summary>
    CommandNotAcceptable,
    /// <summary>A durable job exists for the command id but the saved inbox payload hash conflicts with the job's frozen original hash.</summary>
    PayloadConflict,
    /// <summary>The saved command type does not canonically map to the requested mode, or the resolved entity selection does not correspond to the saved payload.</summary>
    TypeModeMismatch,
    /// <summary>The saved never-sent command is already past its expiry; the normal executor expiry path owns it.</summary>
    Expired
}

/// <summary>
/// Typed outcome of <c>IAgentStore.AcceptEtlJobAndCompleteCommandAsync</c>. <c>Applied</c> means the
/// job + pending run + command result + outbox committed in one transaction; <c>AlreadyAccepted</c>
/// replays the original acceptance identity and exact result for a repeated commandId; <c>NotApplied</c>
/// means nothing was mutated.
/// </summary>
public abstract record EtlJobAcceptanceOutcome
{
    private EtlJobAcceptanceOutcome() { }

    public sealed record Applied(EtlAcceptedJob Job) : EtlJobAcceptanceOutcome;
    public sealed record AlreadyAccepted(EtlAcceptedJob Job) : EtlJobAcceptanceOutcome;
    public sealed record NotApplied(EtlJobAcceptanceRejection Reason) : EtlJobAcceptanceOutcome;
}
