namespace ErpOnecAgent.Application.Abstractions;

// D1 DARK storage contract: attested reset of an entity's watermark domain (migration 011).

/// <summary>
/// Reset request. <see cref="ExpectedGeneration"/> is the watermarks generation the
/// operator inspected; a moved generation refuses the reset (CAS).
/// </summary>
public sealed record EtlWatermarkDomainResetRequest(string EntityName, long ExpectedGeneration, string OperatorId, string Reason);

public sealed record EtlWatermarkDomainReset(Guid ResetId, string EntityName, DateTimeOffset ResetAtUtc, long PriorGeneration, string? PriorCursorJson, string? PriorDomainFingerprint);

public enum EtlWatermarkDomainResetRefusal
{
    /// <summary>The entity has no watermarks row (nothing to reset; the next baseline establishes the domain).</summary>
    WatermarkMissing,
    /// <summary>The row's generation differs from the expected one.</summary>
    GenerationMismatch,
    /// <summary>The entity is owned by an active run (ownership not released).</summary>
    EntityOwned
}

/// <summary>
/// <c>Reset</c> archived the row into watermark_domain_resets and removed it in ONE
/// transaction; the next extraction must be a full baseline. <c>Refused</c> writes nothing.
/// </summary>
public abstract record EtlWatermarkDomainResetOutcome
{
    private EtlWatermarkDomainResetOutcome() { }
    public sealed record Reset(EtlWatermarkDomainReset Record) : EtlWatermarkDomainResetOutcome;
    public sealed record Refused(EtlWatermarkDomainResetRefusal Reason) : EtlWatermarkDomainResetOutcome;
}
