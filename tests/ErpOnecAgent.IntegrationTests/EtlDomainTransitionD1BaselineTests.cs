using System.Globalization;
using System.Text.Json;
using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Domain.Etl;
using ErpOnecAgent.Infrastructure.Persistence.Sqlite;
using Microsoft.Data.Sqlite;
using Xunit;

namespace ErpOnecAgent.IntegrationTests;

/// <summary>
/// D1 behavioural transition probes restricted to the PRE-D1 API surface (no reset API,
/// no BaselineRequired member reference) so the file compiles on the pre-D1 baseline.
/// On the baseline both tests are RED: the fingerprint hashed the read mode, so a
/// committed bootstrap_full watermark is a different domain from a scheduled
/// incremental read (DomainChanged), and an incremental Begin with no watermark row
/// Begun on 'absent' instead of being refused (BaselineRequired). On the D1 build they
/// are GREEN: the watermark-cursor/v1 class is shared across the supported modes and a
/// baseline-less incremental read is refused with zero writes. All against a real
/// migrated SQLite database.
/// </summary>
public sealed class EtlDomainTransitionD1BaselineTests : IAsyncLifetime
{
    private const string SourceNamespace = "onec-infobase-a";
    private const string QueryMode = "bootstrap_full";
    private const string ScheduledMode = "incremental";
    private const string RemoteVerification = "ERP staging reconciled: no rows landed for the unresolved work";
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    private static readonly EtlCursor FinalClients = new(DateTimeOffset.Parse("2026-09-19T09:59:00.0000000+00:00", CultureInfo.InvariantCulture), "A10");
    private static readonly EtlCursor FinalClients2 = new(DateTimeOffset.Parse("2026-09-20T09:59:00.0000000+00:00", CultureInfo.InvariantCulture), "A11");

    private readonly SqliteTestDatabase _database = new();
    private readonly Dictionary<Guid, Guid> _extractionClaims = new();
    private SqliteConnectionFactory _factory = null!;
    private SqliteAgentStore _store = null!;

    private Guid ClaimOf(Guid runId) => _extractionClaims[runId];

    public async Task InitializeAsync()
    {
        _factory = _database.CreateFactory(Path.Combine("data", "agent.db"));
        _store = new(_factory, new SqliteMigrator(_factory));
        await _store.InitializeAsync(CancellationToken.None);
    }

    public async Task DisposeAsync()
    {
        await _database.DisposeAsync();
    }

    // ---------- T1: a committed full baseline continues as a scheduled incremental ----------

    [Fact]
    public async Task Full_baseline_then_scheduled_incremental_continues_the_same_domain_to_generation_2()
    {
        // 1. A manual bootstrap_full job run extracts, seals, uploads (O2 claim + ACK)
        //    and finalizes — the watermark row is created with the domain fingerprint.
        var baselineRun = await NewRunAsync("clients");
        var begun = Assert.IsType<EtlEntityBeginOutcome.Begun>(
            await _store.BeginEtlEntityExtractionAsync(baselineRun, ClaimOf(baselineRun), Request("clients"), CancellationToken.None));
        Assert.Equal("absent", begun.Base.DomainStatus);
        var batch = MakeBatch(baselineRun, "clients", 5, FinalClients);
        Assert.IsType<EtlBatchRegistrationOutcome.Registered>(
            await _store.RegisterGuardedEtlBatchAsync(batch, ClaimOf(baselineRun), CancellationToken.None));
        Assert.IsType<EtlEntityCompletionOutcome.Completed>(
            await _store.CompleteEtlEntityExtractionAsync(baselineRun, ClaimOf(baselineRun), "clients", CursorJson(FinalClients), 1, CancellationToken.None));
        Assert.IsType<EtlRunSealOutcome.Sealed>(
            await _store.SealEtlRunExtractionAsync(baselineRun, ClaimOf(baselineRun), CancellationToken.None));
        var upload = Assert.IsType<EtlBatchUploadClaimOutcome.Claimed>(
            await _store.TryClaimBatchUploadAsync(batch.BatchId, "uploader-1", DateTimeOffset.UtcNow, 5, CancellationToken.None));
        Assert.IsType<EtlBatchAckOutcome.Acknowledged>(
            await _store.AcknowledgeClaimedBatchAsync(batch.BatchId, upload.Claim.AttemptId,
                new EtlBatchAckEvidence("acknowledged", batch.BatchId, 5, true, DateTimeOffset.UtcNow), "ack-hash", 200, CancellationToken.None));
        var completion = Assert.IsType<EtlRunClaimOutcome.Claimed>(
            await _store.TryClaimRunCompletionAsync(baselineRun, "finalizer", DateTimeOffset.UtcNow, 8, CancellationToken.None));
        Assert.IsType<EtlRunFinalizeOutcome.Finalized>(
            await _store.FinalizeEtlRunAsync(baselineRun, completion.Claim.ClaimId, CancellationToken.None));
        Assert.Equal("succeeded", await RunStatusAsync(baselineRun));
        Assert.Equal(1, await ScalarAsync("SELECT generation FROM watermarks WHERE entity_name='clients'"));
        var baselineFingerprint = await ScalarStringAsync("SELECT domain_fingerprint FROM watermarks WHERE entity_name='clients'");
        Assert.Equal(
            EtlDomainFingerprint.Compute(SourceNamespace, "clients", DefinitionJson("clients"), QueryMode),
            baselineFingerprint);

        // 2. A scheduled incremental run for the same entity and the same frozen
        //    definition continues the committed domain: Begin 'same' over the committed
        //    cursor, and the finalize UPDATE path lands generation 2.
        var ensured = Assert.IsType<EtlScheduledRunEnsureOutcome.Created>(
            await _store.EnsureScheduledEtlRunAsync(EnsureRequest("nightly", ["clients"]), DateTimeOffset.UtcNow, CancellationToken.None));
        var incrementalRun = ensured.RunId;
        var incrementalClaim = Assert.IsType<EtlScheduledRunClaimOutcome.Claimed>(
            await _store.TryClaimScheduledRunAsync(incrementalRun, "scheduler-1", DateTimeOffset.UtcNow, CancellationToken.None)).Claim.ExtractionClaimId;

        var continued = Assert.IsType<EtlEntityBeginOutcome.Begun>(
            await _store.BeginEtlEntityExtractionAsync(incrementalRun, incrementalClaim, IncrementalRequest("clients"), CancellationToken.None));
        Assert.Equal("same", continued.Base.DomainStatus);
        Assert.Equal(CursorJson(FinalClients), continued.Base.CommittedCursorJson);
        Assert.Equal(1, continued.Base.ExpectedBaseGeneration);
        Assert.Equal(baselineFingerprint, continued.Base.ExpectedBaseDomainFingerprint);

        var incrementalBatch = MakeBatch(incrementalRun, "clients", 3, FinalClients2);
        Assert.IsType<EtlBatchRegistrationOutcome.Registered>(
            await _store.RegisterGuardedEtlBatchAsync(incrementalBatch, incrementalClaim, CancellationToken.None));
        Assert.IsType<EtlEntityCompletionOutcome.Completed>(
            await _store.CompleteEtlEntityExtractionAsync(incrementalRun, incrementalClaim, "clients", CursorJson(FinalClients2), 1, CancellationToken.None));
        Assert.IsType<EtlRunSealOutcome.Sealed>(
            await _store.SealEtlRunExtractionAsync(incrementalRun, incrementalClaim, CancellationToken.None));
        var incrementalUpload = Assert.IsType<EtlBatchUploadClaimOutcome.Claimed>(
            await _store.TryClaimBatchUploadAsync(incrementalBatch.BatchId, "uploader-1", DateTimeOffset.UtcNow, 5, CancellationToken.None));
        Assert.IsType<EtlBatchAckOutcome.Acknowledged>(
            await _store.AcknowledgeClaimedBatchAsync(incrementalBatch.BatchId, incrementalUpload.Claim.AttemptId,
                new EtlBatchAckEvidence("acknowledged", incrementalBatch.BatchId, 3, true, DateTimeOffset.UtcNow), "ack-hash-2", 200, CancellationToken.None));
        var incrementalCompletion = Assert.IsType<EtlRunClaimOutcome.Claimed>(
            await _store.TryClaimRunCompletionAsync(incrementalRun, "finalizer", DateTimeOffset.UtcNow, 8, CancellationToken.None));
        Assert.IsType<EtlRunFinalizeOutcome.Finalized>(
            await _store.FinalizeEtlRunAsync(incrementalRun, incrementalCompletion.Claim.ClaimId, CancellationToken.None));

        // The UPDATE CAS path: the same watermark row advanced to generation 2 in the
        // SAME domain — the stored fingerprint is byte-identical to the baseline's.
        Assert.Equal("succeeded", await RunStatusAsync(incrementalRun));
        Assert.Equal(1, await ScalarAsync("SELECT COUNT(*) FROM watermarks WHERE entity_name='clients'"));
        Assert.Equal(2, await ScalarAsync("SELECT generation FROM watermarks WHERE entity_name='clients'"));
        Assert.Equal(baselineFingerprint, await ScalarStringAsync("SELECT domain_fingerprint FROM watermarks WHERE entity_name='clients'"));
        Assert.Equal(CursorJson(FinalClients2), await ScalarStringAsync("SELECT committed_cursor_json FROM watermarks WHERE entity_name='clients'"));
        Assert.Equal(incrementalRun.ToString("D"), await ScalarStringAsync("SELECT last_run_id FROM watermarks WHERE entity_name='clients'"));
    }

    // ---------- B1: an incremental read never establishes a domain ----------

    [Fact]
    public async Task Incremental_begin_without_a_watermark_row_is_refused_and_records_a_failed_entity_only()
    {
        var (runId, claim) = await ClaimedScheduledRunAsync("nightly", ["clients"]);
        var rowVersionBefore = await ScalarAsync($"SELECT row_version FROM etl_runs WHERE run_id='{runId:D}'");

        var outcome = await _store.BeginEtlEntityExtractionAsync(runId, claim, IncrementalRequest("clients"), CancellationToken.None);

        // BaselineRequired — a pre-D1 build BEGAN the extraction on 'absent' instead.
        var rejected = Assert.IsType<EtlEntityBeginOutcome.Rejected>(outcome);
        Assert.Equal("BaselineRequired", rejected.Reason.ToString());
        // Partial runs: the only writes are the fence bump and a failed, skippable entity row
        // (BASELINE_REQUIRED, no expected batches); the run keeps running with its live claim,
        // and no watermark was minted.
        Assert.Equal(1, await ScalarAsync($"SELECT COUNT(*) FROM etl_run_entities WHERE run_id='{runId:D}' AND status='failed' AND failure_code='BASELINE_REQUIRED' AND expected_batch_count=0 AND final_watermark_json IS NULL"));
        Assert.Equal("running", await RunStatusAsync(runId));
        Assert.Equal(rowVersionBefore + 1, await ScalarAsync($"SELECT row_version FROM etl_runs WHERE run_id='{runId:D}'"));
        Assert.Equal(claim.ToString("D"), await ScalarStringAsync($"SELECT extraction_claim_id FROM etl_runs WHERE run_id='{runId:D}'"));
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM watermarks"));

        // Release the refused run's ownership so a manual baseline can own the entity.
        Assert.IsType<EtlRunTerminationOutcome.Applied>(
            await _store.FailEtlRunAsync(runId, claim, "baseline required", CancellationToken.None));
        Assert.IsType<EtlRunResolutionOutcome.Resolved>(
            await _store.ResolveEtlRunAsync(Resolution(runId), DateTimeOffset.UtcNow, CancellationToken.None));

        // The same entity under bootstrap_full begins 'absent' — the baseline path.
        var baselineRun = await NewRunAsync("clients");
        var begun = Assert.IsType<EtlEntityBeginOutcome.Begun>(
            await _store.BeginEtlEntityExtractionAsync(baselineRun, ClaimOf(baselineRun), Request("clients"), CancellationToken.None));
        Assert.False(begun.Base.BaseRowPresent);
        Assert.Equal("absent", begun.Base.DomainStatus);
    }

    // ---------- helpers ----------

    private static string Now() => DateTimeOffset.UtcNow.ToString("O");
    private static string CursorJson(EtlCursor cursor) => JsonSerializer.Serialize(cursor, JsonOptions);
    private static string DefinitionJson(string entity) =>
        JsonSerializer.Serialize(MakeEntities().Single(e => e.EntityCode == entity), JsonOptions);
    private static string DefinitionsJson(string[] entities) =>
        JsonSerializer.Serialize(entities.Select(e => MakeEntities().Single(x => x.EntityCode == e)).ToArray(), JsonOptions);
    private static string ManifestJson(string[] entities) => JsonSerializer.Serialize(entities, JsonOptions);
    private static EtlEntityDefinition[] MakeEntities() =>
    [
        new("clients", "Catalog_Clients", "Ref_Key", "UpdatedAt", "DeletionMark", ["Ref_Key", "UpdatedAt", "DeletionMark"], "incremental", 500, 10),
        new("orders", "Document_Orders", "Ref_Key", "Date", "Posted", ["Ref_Key", "Date", "Posted"], "incremental", 500, 10)
    ];
    private static EtlEntityExtractionRequest Request(string entity, string? sourceNamespace = null) =>
        new(entity, DefinitionJson(entity), sourceNamespace ?? SourceNamespace, QueryMode,
            JsonSerializer.Serialize(new EtlCursor(DateTimeOffset.UtcNow, null), JsonOptions));
    private static EtlEntityExtractionRequest IncrementalRequest(string entity) =>
        new(entity, DefinitionJson(entity), SourceNamespace, ScheduledMode,
            JsonSerializer.Serialize(new EtlCursor(DateTimeOffset.UtcNow, null), JsonOptions));
    private static EtlScheduledRunRequest EnsureRequest(string scheduleKey, string[] entities) =>
        new(scheduleKey, ScheduledMode, entities.Select(e => MakeEntities().Single(x => x.EntityCode == e)).ToArray(), 7);
    private static EtlBatch MakeBatch(Guid runId, string entity, int rowCount, EtlCursor? watermarkTo = null) =>
        new(Guid.NewGuid(), runId, entity, 1, $"spool/{runId:N}-{entity}-{Guid.NewGuid():N}.gz", EtlBatchStatus.Ready, rowCount,
            null, watermarkTo ?? FinalClients, "hash-" + Guid.NewGuid().ToString("N"), 1000, 4000, 0, DateTimeOffset.UtcNow);
    private static EtlRunResolutionRequest Resolution(Guid runId) =>
        new(runId, "operator-1", EtlRunResolutionDecision.Rebaseline, RemoteVerification, true);

    private async Task<(Guid RunId, Guid JobId)> NewPendingJobRunAsync(string mode, string[] entities)
    {
        var runId = Guid.NewGuid();
        var jobId = Guid.NewGuid();
        var created = Now();
        await ExecuteSqlAsync(
            "INSERT INTO etl_runs(run_id,mode,requested_entities_json,status,configuration_version,created_at_utc,updated_at_utc,row_version) VALUES($run,$mode,$manifest,'pending',7,$now,$now,1);",
            ("$run", runId.ToString("D")), ("$mode", mode), ("$manifest", ManifestJson(entities)), ("$now", created));
        await ExecuteSqlAsync(
            "INSERT INTO etl_jobs(job_id,command_id,run_id,mode,entities_json,configuration_version,status,command_payload_hash,acceptance_result_json,created_at_utc,updated_at_utc,row_version) VALUES($job,$cmd,$run,$mode,$defs,7,'pending','hash','{}',$now,$now,1);",
            ("$job", jobId.ToString("D")), ("$cmd", Guid.NewGuid().ToString("D")), ("$run", runId.ToString("D")), ("$mode", mode), ("$defs", DefinitionsJson(entities)), ("$now", created));
        return (runId, jobId);
    }

    private async Task<Guid> NewRunAsync(params string[] entities)
    {
        var (runId, jobId) = await NewPendingJobRunAsync(QueryMode, entities);
        var claimed = Assert.IsType<EtlJobClaimOutcome.Claimed>(
            await _store.TryClaimEtlJobAsync(jobId, "test-dispatcher", DateTimeOffset.UtcNow.AddMinutes(5), DateTimeOffset.UtcNow, CancellationToken.None));
        _extractionClaims[runId] = claimed.Claim.ExtractionClaimId;
        return runId;
    }

    private async Task<(Guid RunId, Guid ExtractionClaimId)> ClaimedScheduledRunAsync(string scheduleKey, string[] entities)
    {
        var ensured = Assert.IsType<EtlScheduledRunEnsureOutcome.Created>(
            await _store.EnsureScheduledEtlRunAsync(EnsureRequest(scheduleKey, entities), DateTimeOffset.UtcNow, CancellationToken.None));
        var claimed = Assert.IsType<EtlScheduledRunClaimOutcome.Claimed>(
            await _store.TryClaimScheduledRunAsync(ensured.RunId, "scheduler-1", DateTimeOffset.UtcNow, CancellationToken.None));
        return (ensured.RunId, claimed.Claim.ExtractionClaimId);
    }

    private async Task<string?> RunStatusAsync(Guid runId) => await ScalarStringAsync($"SELECT status FROM etl_runs WHERE run_id='{runId:D}'");

    private async Task<long> ScalarAsync(string sql)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        return Convert.ToInt64(await command.ExecuteScalarAsync(CancellationToken.None), CultureInfo.InvariantCulture);
    }

    private async Task<string?> ScalarStringAsync(string sql)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        var value = await command.ExecuteScalarAsync(CancellationToken.None);
        return value is null or DBNull ? null : Convert.ToString(value, CultureInfo.InvariantCulture);
    }

    private async Task ExecuteSqlAsync(string sql, params (string Name, object? Value)[] parameters)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        foreach (var (name, value) in parameters) command.Parameters.AddWithValue(name, value ?? DBNull.Value);
        await command.ExecuteNonQueryAsync(CancellationToken.None);
    }
}
