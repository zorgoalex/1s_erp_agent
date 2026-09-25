using System.Text.Json;
using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Application.Configuration;
using ErpOnecAgent.Application.Etl;
using ErpOnecAgent.Domain.Etl;
using ErpOnecAgent.Infrastructure.OneC;
using ErpOnecAgent.Service.Runtime;
using Microsoft.Extensions.Options;

namespace ErpOnecAgent.Service.Workers.Etl;

/// <summary>
/// C1: the durable extraction pipeline. Each pass:
/// <list type="number">
/// <item>schedules the incremental run (O3);</item>
/// <item>claims the next manual job (O1) or scheduled run under the shared overlap priority;</item>
/// <item>extracts every frozen entity under the extraction claim: Begin with the source
/// namespace from the identity binding (S1), OData pages, spool batches under the disk
/// reserve (A08), guarded batch registration, and entity completion;</item>
/// <item>re-verifies the source identity and seals the run.</item>
/// </list>
/// Upload and completion are separate workers. Nothing here sends to ERP.
/// </summary>
public sealed class EtlExtractionWorker(
    IAgentStore store,
    ISpoolStore spool,
    IOnecODataClient odata,
    SourceIdentityGuard identity,
    IDiskSpaceProbe diskProbe,
    AgentRuntimeState state,
    DynamicConfigurationState configuration,
    IOptions<EtlOptions> etlOptions,
    IOptions<StorageOptions> storageOptions,
    IOptions<AgentOptions> agentOptions,
    ILogger<EtlExtractionWorker> logger) : BackgroundService
{
    internal const string ScheduleKey = "incremental";
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);
    private static readonly TimeSpan IdleDelay = TimeSpan.FromSeconds(2);
    internal const int AfterCheckRetries = 3;
    internal static TimeSpan AfterCheckRetryDelay = TimeSpan.FromSeconds(2);
    private readonly string _owner = $"{agentOptions.Value.AgentId}:extract:{Environment.ProcessId}";
    private DateTimeOffset _nextScheduleTick = DateTimeOffset.MinValue;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _nextScheduleTick = etlOptions.Value.RunOnStartup ? DateTimeOffset.UtcNow : DateTimeOffset.UtcNow.AddMinutes(configuration.IntervalMinutes);
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                if (!await RunOnceAsync(stoppingToken).ConfigureAwait(false))
                    await Task.Delay(IdleDelay, stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { break; }
            catch (Exception ex)
            {
                logger.LogError(ex, "ETL_EXTRACTION_PASS_FAILED");
                await Task.Delay(IdleDelay, stoppingToken).ConfigureAwait(false);
            }
        }
    }

    /// <summary>One pass; returns true when a run was claimed (the caller loops without delay).</summary>
    internal async Task<bool> RunOnceAsync(CancellationToken cancellationToken)
    {
        if (!etlOptions.Value.Enabled || !state.Snapshot.CanExtract) return false;
        if (!DiskAllowsExtraction())
        {
            logger.LogWarning("DISK_WARNING ETL extraction deferred: free disk space would not keep the command reserve");
            return false;
        }
        // The spool quota is checked before a run is claimed: a full spool (upload backlog)
        // defers new work instead of starting a run that would block on its first batch.
        var spoolBytes = await spool.GetSizeAsync(cancellationToken).ConfigureAwait(false);
        if (spoolBytes >= storageOptions.Value.MaxSpoolBytes)
        {
            logger.LogWarning("DISK_WARNING ETL extraction deferred: spool limit reached ({SpoolBytes} of {MaxSpoolBytes} bytes)", spoolBytes, storageOptions.Value.MaxSpoolBytes);
            return false;
        }
        var source = await identity.CheckAsync(cancellationToken).ConfigureAwait(false);
        if (!source.IsMatch)
        {
            logger.LogWarning("ETL_SOURCE_IDENTITY_NOT_MATCHED Verdict={Verdict} Detail={Detail}", source.Verdict, source.Detail);
            return false;
        }

        await EnsureScheduledRunAsync(cancellationToken).ConfigureAwait(false);
        var now = DateTimeOffset.UtcNow;

        var page = await store.GetDispatchableEtlJobsAsync(1, now, cancellationToken).ConfigureAwait(false);
        foreach (var quarantine in page.Quarantined)
            logger.LogWarning("ETL_PENDING_RUN_QUARANTINE RunId={RunId} JobId={JobId} Code={Code}", quarantine.RunId, quarantine.JobId, quarantine.Code);
        foreach (var job in page.Jobs)
        {
            var outcome = await store.TryClaimEtlJobAsync(job.JobId, _owner, now.AddSeconds(30), now, cancellationToken).ConfigureAwait(false);
            if (outcome is EtlJobClaimOutcome.Claimed claimed)
            {
                await ExtractRunAsync(claimed.Claim.RunId, claimed.Claim.ExtractionClaimId, claimed.Claim.Mode, claimed.Claim.EntitiesJson, source, cancellationToken).ConfigureAwait(false);
                return true;
            }
        }

        foreach (var due in await store.GetDueScheduledRunsAsync(1, now, cancellationToken).ConfigureAwait(false))
        {
            var outcome = await store.TryClaimScheduledRunAsync(due.RunId, _owner, now, cancellationToken).ConfigureAwait(false);
            if (outcome is EtlScheduledRunClaimOutcome.Claimed claimed)
            {
                await ExtractRunAsync(claimed.Claim.RunId, claimed.Claim.ExtractionClaimId, claimed.Claim.Mode, claimed.Claim.ResolvedEntitiesJson, source, cancellationToken).ConfigureAwait(false);
                return true;
            }
        }
        return false;
    }

    private async Task EnsureScheduledRunAsync(CancellationToken cancellationToken)
    {
        if (DateTimeOffset.UtcNow < _nextScheduleTick) return;
        _nextScheduleTick = DateTimeOffset.UtcNow.AddMinutes(configuration.IntervalMinutes);
        // D1 policy: an incremental read never establishes a domain, so the scheduled manifest
        // contains only entities whose baseline exists. An entity without one would make the
        // whole run BaselineRequired; it is reported instead until an explicit
        // start_full_sync/reload_entity establishes it.
        var candidates = configuration.Entities.Where(static entity => entity.Enabled && entity.RunsOnSchedule()).ToArray();
        var entities = new List<EtlEntityDefinition>();
        foreach (var entity in candidates)
        {
            if (await store.GetCommittedWatermarkAsync(entity.EntityCode, cancellationToken).ConfigureAwait(false) is not null) entities.Add(entity);
            else logger.LogWarning("ETL_BASELINE_REQUIRED Entity={Entity} — excluded from the schedule until a start_full_sync or reload_entity baseline completes", entity.EntityCode);
        }
        if (entities.Count == 0) return;
        var outcome = await store.EnsureScheduledEtlRunAsync(new EtlScheduledRunRequest(ScheduleKey, "incremental", entities, configuration.Version), DateTimeOffset.UtcNow, cancellationToken).ConfigureAwait(false);
        if (outcome is EtlScheduledRunEnsureOutcome.ActiveExisting existing && existing.Status is "failed" or "blocked")
            logger.LogWarning("ETL_SCHEDULE_HELD_BY_UNRESOLVED_RUN RunId={RunId} Status={Status} — manual resolution required", existing.RunId, existing.Status);
    }

    private async Task ExtractRunAsync(Guid runId, Guid claimId, string mode, string frozenEntitiesJson, SourceIdentityCheck before, CancellationToken cancellationToken)
    {
        state.CurrentEtlRunId = runId;
        logger.LogInformation("ETL_RUN_STARTED RunId={RunId} Mode={Mode}", runId, mode);
        try
        {
            using var frozen = JsonDocument.Parse(frozenEntitiesJson);
            foreach (var element in frozen.RootElement.EnumerateArray())
            {
                // B3: the mode is re-checked at every entity boundary; a pause holds the claim
                // in-process and continues on resume (a restart blocks the run INTERRUPTED).
                while (!state.Snapshot.CanExtract)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    await Task.Delay(IdleDelay, cancellationToken).ConfigureAwait(false);
                }
                var definitionJson = element.GetRawText();
                var entity = element.Deserialize<EtlEntityDefinition>(JsonOptions) ?? throw new InvalidDataException("Frozen entity definition is null.");
                if (!await ExtractEntityAsync(runId, claimId, mode, entity, definitionJson, before.SourceNamespace!, cancellationToken).ConfigureAwait(false)) return;
            }

            // The after-check tolerates a short outage (bounded retries): a transient
            // Unavailable must not block a run whose source never changed. Any other verdict,
            // or a persisting outage, blocks the run without committing watermarks.
            var after = await identity.CheckAsync(cancellationToken).ConfigureAwait(false);
            for (var retry = 1; retry <= AfterCheckRetries && after.Verdict == SourceIdentityVerdict.Unavailable; retry++)
            {
                await Task.Delay(AfterCheckRetryDelay, cancellationToken).ConfigureAwait(false);
                after = await identity.CheckAsync(cancellationToken).ConfigureAwait(false);
            }
            if (!SourceIdentityGuard.SameSource(before, after))
            {
                await BlockAsync(runId, claimId, "SOURCE_IDENTITY_CHANGED", $"Source identity after extraction: {after.Verdict}.", cancellationToken).ConfigureAwait(false);
                return;
            }
            var sealedOutcome = await store.SealEtlRunExtractionAsync(runId, claimId, cancellationToken).ConfigureAwait(false);
            if (sealedOutcome is EtlRunSealOutcome.Sealed) logger.LogInformation("ETL_RUN_SEALED RunId={RunId}", runId);
            else await BlockAsync(runId, claimId, "SEAL_REFUSED", $"Sealing the extraction was refused: {sealedOutcome}.", CancellationToken.None).ConfigureAwait(false);
        }
        catch (EtlDiskReserveException ex)
        {
            await BlockAsync(runId, claimId, "DISK_RESERVE", ex.Message, CancellationToken.None).ConfigureAwait(false);
        }
        catch (EtlSpoolLimitException ex)
        {
            await BlockAsync(runId, claimId, "SPOOL_LIMIT", ex.Message, CancellationToken.None).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Shutdown: the run keeps its claim; startup recovery blocks it INTERRUPTED.
            throw;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "ETL_RUN_FAILED RunId={RunId}", runId);
            await store.FailEtlRunAsync(runId, claimId, ex.GetType().Name + ": " + ex.Message, CancellationToken.None).ConfigureAwait(false);
        }
        finally { state.CurrentEtlRunId = null; }
    }

    /// <summary>Returns false when the run was blocked/failed and extraction must stop.</summary>
    private async Task<bool> ExtractEntityAsync(Guid runId, Guid claimId, string mode, EtlEntityDefinition entity, string definitionJson, string sourceNamespace, CancellationToken cancellationToken)
    {
        var options = etlOptions.Value;
        var upper = new EtlCursor(DateTimeOffset.UtcNow.AddSeconds(-Math.Max(0, options.SafetyLagSeconds)), null);
        var begin = await store.BeginEtlEntityExtractionAsync(runId, claimId,
            new EtlEntityExtractionRequest(entity.EntityCode, definitionJson, sourceNamespace, mode, JsonSerializer.Serialize(upper, JsonOptions)), cancellationToken).ConfigureAwait(false);
        if (begin is EtlEntityBeginOutcome.Rejected rejected)
        {
            await BlockAsync(runId, claimId, "BEGIN_" + ToCode(rejected.Reason.ToString()), $"Entity '{entity.EntityCode}' extraction was refused: {rejected.Reason}.", cancellationToken).ConfigureAwait(false);
            return false;
        }
        var captured = ((EtlEntityBeginOutcome.Begun)begin).Base;
        var committed = captured.CommittedCursorJson is null ? null : JsonSerializer.Deserialize<EtlCursor>(captured.CommittedCursorJson, JsonOptions);
        var full = mode is not "incremental";

        var rows = new List<JsonElement>();
        long approximateBytes = 0;
        var from = committed;
        EtlCursor? last = null;
        var batches = 0;
        await foreach (var row in odata.ReadEntityAsync(entity, committed, upper, full, cancellationToken).ConfigureAwait(false))
        {
            rows.Add(row); approximateBytes += row.GetRawText().Length + 128;
            last = CursorFrom(row, entity);
            if (approximateBytes >= options.TargetBatchUncompressedBytes)
            {
                if (!await FlushAsync(runId, claimId, entity, rows, from, last, cancellationToken).ConfigureAwait(false)) return false;
                batches++; from = last; rows = []; approximateBytes = 0;
            }
        }
        // Every entity ends with at least one batch — an empty window is an explicit
        // zero-row batch whose ACK proves ERP saw the (empty) range.
        if (rows.Count > 0 || batches == 0)
        {
            if (!await FlushAsync(runId, claimId, entity, rows, from, last ?? upper, cancellationToken).ConfigureAwait(false)) return false;
            batches++;
        }
        var final = last ?? upper;
        var completed = await store.CompleteEtlEntityExtractionAsync(runId, claimId, entity.EntityCode, JsonSerializer.Serialize(final, JsonOptions), batches, cancellationToken).ConfigureAwait(false);
        if (completed is not EtlEntityCompletionOutcome.Completed)
        {
            // A refusal leaves the run without a way forward; it is blocked (fenced by the
            // claim, so a lost claim makes the block a no-op) instead of lingering claimed.
            await BlockAsync(runId, claimId, "ENTITY_COMPLETION_REFUSED", $"Completing entity '{entity.EntityCode}' was refused: {completed}.", CancellationToken.None).ConfigureAwait(false);
            return false;
        }
        return true;
    }

    private async Task<bool> FlushAsync(Guid runId, Guid claimId, EtlEntityDefinition entity, List<JsonElement> rows, EtlCursor? from, EtlCursor? to, CancellationToken cancellationToken)
    {
        var batch = await spool.WriteBatchAsync(runId, entity, rows, from, to, cancellationToken).ConfigureAwait(false);
        var registered = await store.RegisterGuardedEtlBatchAsync(batch, claimId, cancellationToken).ConfigureAwait(false);
        if (registered is EtlBatchRegistrationOutcome.Registered)
        {
            logger.LogInformation("ETL_BATCH_CREATED RunId={RunId} BatchId={BatchId} Entity={Entity} Rows={Rows}", runId, batch.BatchId, entity.EntityCode, rows.Count);
            return true;
        }
        // The file exists but no batch row does: it is an orphan that startup spool
        // reconciliation quarantines; it is never uploaded.
        logger.LogWarning("ETL_BATCH_REGISTRATION_REFUSED RunId={RunId} BatchId={BatchId} Outcome={Outcome}", runId, batch.BatchId, registered);
        await BlockAsync(runId, claimId, "BATCH_REGISTRATION_REFUSED", $"Registering batch {batch.BatchId:D} of '{entity.EntityCode}' was refused: {registered}.", CancellationToken.None).ConfigureAwait(false);
        return false;
    }

    private async Task BlockAsync(Guid runId, Guid claimId, string code, string message, CancellationToken cancellationToken)
    {
        var outcome = await store.BlockEtlRunAsync(runId, claimId, code, message, cancellationToken).ConfigureAwait(false);
        logger.LogWarning("ETL_RUN_BLOCKED RunId={RunId} Code={Code} Outcome={Outcome} Message={Message}", runId, code, outcome, message);
    }

    private bool DiskAllowsExtraction()
    {
        var storage = storageOptions.Value;
        long free;
        try
        {
            free = diskProbe.GetAvailableFreeBytes(agentOptions.Value.DataDirectory);
        }
        catch (Exception ex) when (ex is IOException or ArgumentException or UnauthorizedAccessException or InvalidOperationException)
        {
            // Unknown free space fails closed: no extraction this pass.
            logger.LogWarning(ex, "DISK_WARNING free disk space could not be determined");
            return false;
        }
        return EtlDiskAdmission.Allows(free, storage.MinimumReservedBytesForCommands, Math.Min(storage.MaxBatchCompressedBytes, EtlDiskAdmission.DefaultBatchHeadroomBytes));
    }

    private static string ToCode(string pascal) =>
        string.Concat(pascal.Select((c, i) => i > 0 && char.IsUpper(c) ? "_" + c : c.ToString())).ToUpperInvariant();

    internal static EtlCursor CursorFrom(JsonElement row, EtlEntityDefinition entity)
    {
        var id = entity.SourceIdFrom(row);
        DateTimeOffset? updated = null;
        if (entity.UpdatedAtField is not null && row.TryGetProperty(entity.UpdatedAtField, out var value)
            && DateTimeOffset.TryParse(value.ToString(), System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.AssumeUniversal, out var parsed))
            updated = parsed.ToUniversalTime();
        return new(updated, id);
    }
}
