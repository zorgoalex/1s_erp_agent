using System.Text.Json;
using ErpOnecAgent.Domain.Common;

namespace ErpOnecAgent.Application.Abstractions;

// DARK storage contracts for the A05b F1 slice (durable entity bases/finals, sealed
// extraction, atomic claim+finalize). Nothing here is wired into workers, recovery,
// or the ERP client; the guarantees hold only for the isolated new path — existing
// v5 writers (CommitWatermarkAsync, unguarded RegisterBatchAsync, CompleteEtlRunAsync,
// MarkEtlRunExtractedAsync) still bypass generation/seal and must be retired/fenced
// atomically at cutover (F2) before any production use.

/// <summary>
/// Conservative domain identity of a cursor: the project-hash over the canonical tuple
/// { source namespace, entity, entire frozen entity definition, query mode }. The
/// <paramref name="sourceNamespace"/> must be a stable, explicitly configured,
/// NON-SECRET identity of the 1C source the cursor was read from — credentials are
/// never hashed and never accepted here. Any definition change that could plausibly
/// alter cursor semantics produces a different fingerprint; equal serialized cursors
/// under different fingerprints are different domains.
/// </summary>
public static class EtlDomainFingerprint
{
    public static string Compute(string sourceNamespace, string entityName, string entityDefinitionJson, string queryMode)
    {
        using var definition = JsonDocument.Parse(entityDefinitionJson);
        var tuple = JsonSerializer.SerializeToElement(
            new { definition = definition.RootElement, entity = entityName, queryMode, source = sourceNamespace });
        return PayloadHasher.Compute(tuple);
    }
}

/// <summary>
/// Frozen capture input for one entity extraction. <see cref="EntityDefinitionJson"/> is
/// the entire effective definition used for this read (stored verbatim and fingerprinted
/// canonically); <see cref="SourceNamespace"/> is the required explicit non-secret source
/// identity; <see cref="QueryMode"/> freezes the read shape (e.g.
/// bootstrap_full|entity_reload|incremental) into the domain fingerprint;
/// <see cref="SnapshotUpperBoundJson"/> is the fixed bounded-read window end (a stored
/// cursor shape, not a snapshot guarantee).
/// </summary>
public sealed record EtlEntityExtractionRequest(
    string EntityName,
    string EntityDefinitionJson,
    string SourceNamespace,
    string QueryMode,
    string? SnapshotUpperBoundJson);

/// <summary>
/// Durable expected base captured before extraction: whether the watermarks row existed,
/// its raw committed cursor text (the exact base the query must use), its generation and
/// domain fingerprint at capture, and the classified domain status (absent|same).
/// </summary>
public sealed record EtlEntityExtractionBase(
    string EntityName,
    bool BaseRowPresent,
    string? CommittedCursorJson,
    long? ExpectedBaseGeneration,
    string? ExpectedBaseDomainFingerprint,
    string DomainFingerprint,
    string DomainStatus);

public enum EtlEntityBeginRejection
{
    /// <summary>The run is missing, not running, already sealed, or the entity already has a row.</summary>
    RunNotAcceptingEntities,
    /// <summary>No explicit non-secret source namespace was supplied; no domain fingerprint can be computed.</summary>
    SourceNamespaceMissing,
    /// <summary>A present watermark row carries a different non-NULL domain fingerprint.</summary>
    DomainChanged,
    /// <summary>A present watermark row carries no domain fingerprint; unknown domains are never adopted.</summary>
    DomainUnknown,
    /// <summary>The run is associated with an etl_job and the supplied definition does not equal the job's frozen definition for this entity.</summary>
    JobDefinitionMismatch,
    /// <summary>The requested query mode differs from the run's saved mode, or the saved mode is not a supported watermark extraction mode.</summary>
    RunModeMismatch,
    /// <summary>The run's saved requested_entities_json is not a valid non-empty unique entity manifest.</summary>
    RunManifestInvalid,
    /// <summary>The entity is not a member of the run's saved requested entity set.</summary>
    EntityNotInManifest,
    /// <summary>The supplied definition is not a valid typed entity definition or its EntityCode differs from the requested entity.</summary>
    EntityDefinitionInvalid,
    /// <summary>The run is associated with an etl_job whose mode or configuration version no longer equals the run's durable identity.</summary>
    JobInconsistent
}

public abstract record EtlEntityBeginOutcome
{
    private EtlEntityBeginOutcome() { }
    public sealed record Begun(EtlEntityExtractionBase Base) : EtlEntityBeginOutcome;
    public sealed record Rejected(EtlEntityBeginRejection Reason) : EtlEntityBeginOutcome;
}

public enum EtlBatchRegistrationRejection
{
    /// <summary>The run is missing, not running, or already sealed/terminal.</summary>
    RunNotAcceptingBatches,
    /// <summary>No entity row in 'extracting' status matches the batch entity.</summary>
    EntityNotExtracting
}

public abstract record EtlBatchRegistrationOutcome
{
    private EtlBatchRegistrationOutcome() { }
    public sealed record Registered : EtlBatchRegistrationOutcome;
    public sealed record Rejected(EtlBatchRegistrationRejection Reason) : EtlBatchRegistrationOutcome;
}

public enum EtlEntityCompletionRejection
{
    /// <summary>The run is missing, not running, or already sealed.</summary>
    RunNotAcceptingEntities,
    /// <summary>No entity row in 'extracting' status exists for the entity.</summary>
    EntityNotExtracting,
    /// <summary>The declared expected batch count does not equal the durable per-entity batch counter.</summary>
    BatchCountMismatch
}

public abstract record EtlEntityCompletionOutcome
{
    private EtlEntityCompletionOutcome() { }
    public sealed record Completed : EtlEntityCompletionOutcome;
    public sealed record Rejected(EtlEntityCompletionRejection Reason) : EtlEntityCompletionOutcome;
}

public enum EtlRunSealRejection
{
    /// <summary>The run is missing, not running, or already sealed.</summary>
    RunNotRunningOrAlreadySealed,
    /// <summary>requested_entities_json is not a non-empty array of unique non-empty strings.</summary>
    ManifestInvalid,
    /// <summary>The entity-row set does not equal the validated requested set (missing or extra).</summary>
    EntitySetMismatch,
    /// <summary>At least one entity row is not 'done'.</summary>
    EntityNotDone,
    /// <summary>A stored final watermark is malformed or has no non-NULL component.</summary>
    FinalWatermarkInvalid,
    /// <summary>expected_batch_count is below 1 or does not equal the actual batch rows; run/entity counters inconsistent.</summary>
    ExpectedBatchCountMismatch
}

public abstract record EtlRunSealOutcome
{
    private EtlRunSealOutcome() { }
    public sealed record Sealed(int EntityCount, int ExpectedBatchCount) : EtlRunSealOutcome;
    public sealed record Rejected(EtlRunSealRejection Reason) : EtlRunSealOutcome;
}

public enum EtlRunTerminationRejection
{
    /// <summary>The run is missing or no longer 'running' (sealed, completing, or terminal).</summary>
    RunNotRunning
}

public abstract record EtlRunTerminationOutcome
{
    private EtlRunTerminationOutcome() { }
    public sealed record Applied : EtlRunTerminationOutcome;
    public sealed record Rejected(EtlRunTerminationRejection Reason) : EtlRunTerminationOutcome;
}

/// <summary>A run the completion path should evaluate: a sealed 'uploading' run or a 'completing' run whose claim was released and whose retry is due.</summary>
public sealed record EtlRunCompletionCandidate(Guid RunId, bool IsReclaim);

/// <summary>
/// One won completion claim. <see cref="ClaimId"/> is a fresh unpredictable GUID minted on
/// every successful claim — the exact fence identity. <see cref="CompletePayloadJson"/> is
/// the immutable raw body written once at first claim and replayed byte-identical on reclaim.
/// </summary>
public sealed record EtlRunCompletionClaim(Guid RunId, Guid ClaimId, string OwnerId, string CompletePayloadJson, int Attempt);

public abstract record EtlRunClaimOutcome
{
    private EtlRunClaimOutcome() { }
    /// <summary>The claim was won; the caller may send the stored payload then finalize under ClaimId.</summary>
    public sealed record Claimed(EtlRunCompletionClaim Claim) : EtlRunClaimOutcome;
    /// <summary>Not claimable now: already claimed, not due, wrong state, or uploads still in flight. Zero writes.</summary>
    public sealed record NotClaimed : EtlRunClaimOutcome;
    /// <summary>The run was durably blocked with evidence (legacy/unprovable or violated seal); the block committed.</summary>
    public sealed record Blocked(string Code, string Message) : EtlRunClaimOutcome;
}

public abstract record EtlRunFinalizeOutcome
{
    private EtlRunFinalizeOutcome() { }
    /// <summary>All watermark CAS writes + succeeded run + finished job committed in one transaction.</summary>
    public sealed record Finalized : EtlRunFinalizeOutcome;
    /// <summary>A CAS/guard mismatch rolled back every watermark change; one commit wrote blocked run + conflict + blocked job.</summary>
    public sealed record Blocked(string Code, string Message) : EtlRunFinalizeOutcome;
    /// <summary>The claim identity is stale or superseded; zero writes.</summary>
    public sealed record ClaimLost : EtlRunFinalizeOutcome;
}

public abstract record EtlRunCompletionRetryOutcome
{
    private EtlRunCompletionRetryOutcome() { }
    /// <summary>The claim was released and the next attempt was scheduled; the stored payload is preserved.</summary>
    public sealed record Scheduled : EtlRunCompletionRetryOutcome;
    /// <summary>The bounded attempt budget was reached; the run and its job committed 'blocked'.</summary>
    public sealed record Blocked : EtlRunCompletionRetryOutcome;
    /// <summary>The claim identity is stale or superseded; zero writes.</summary>
    public sealed record ClaimLost : EtlRunCompletionRetryOutcome;
}

/// <summary>
/// Counts applied by the explicit startup-only recovery API (dead-process claim release,
/// interrupted-run blocking, batch fencing, job blocking). Exclusive-host precondition:
/// never run while another live agent could hold a claim.
/// </summary>
public sealed record EtlRecoveryResult(int ClaimsReleased, int RunsBlocked, int BatchesFenced, int EntitiesFailed, int JobsBlocked);
