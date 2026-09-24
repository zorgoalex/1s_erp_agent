using System.Globalization;
using System.Text.Json;
using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Domain.Common;
using ErpOnecAgent.Domain.Etl;
using ErpOnecAgent.Infrastructure.Persistence.Sqlite;
using Microsoft.Data.Sqlite;
using Xunit;

namespace ErpOnecAgent.IntegrationTests;

/// <summary>
/// O1 ownership fence regressions (schema v7). These cases began as the recorded
/// runtime-RED evidence against the unchanged v6 implementation — every unowned or
/// unclaimed mutation below was previously ADMITTED. They are now adapted to the
/// claim-bearing signatures and must pass: a jobless/unclaimed run, a stale or foreign
/// extraction claim GUID, and a broken ownership set all reject with zero writes.
/// </summary>
public sealed class EtlOwnershipRedTests : IAsyncLifetime
{
    private const string SourceNamespace = "onec-infobase-a";
    private const string QueryMode = "bootstrap_full";
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);
    private static readonly EtlCursor FinalClients = new(DateTimeOffset.Parse("2026-09-19T09:59:00.0000000+00:00", CultureInfo.InvariantCulture), "A10");

    private readonly SqliteTestDatabase _database = new();
    private readonly Dictionary<Guid, Guid> _extractionClaims = new();
    private SqliteConnectionFactory _factory = null!;
    private SqliteAgentStore _store = null!;

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

    [Fact]
    public async Task Unowned_running_run_cannot_begin_extraction()
    {
        var runId = await NewLegacyRunningRunAsync("clients");

        var outcome = await _store.BeginEtlEntityExtractionAsync(runId, Guid.NewGuid(), Request("clients"), CancellationToken.None);

        Assert.Equal(EtlEntityBeginRejection.ExtractionClaimLost, Assert.IsType<EtlEntityBeginOutcome.Rejected>(outcome).Reason);
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM etl_run_entities"));
    }

    [Fact]
    public async Task Unowned_running_run_cannot_register_a_batch()
    {
        // Meaningful pre-O1-admitted state: running run + a live 'extracting' entity row
        // (the exact shape the unguarded register accepted). Now the missing claim
        // rejects with zero writes.
        var runId = await NewLegacyRunningRunAsync("clients");
        await SeedLegacyEntityAsync(runId, "clients", status: "extracting", batchesCreated: 1, rowsRead: 5);

        var outcome = await _store.RegisterGuardedEtlBatchAsync(MakeBatch(runId, "clients", 5), Guid.NewGuid(), CancellationToken.None);

        Assert.Equal(EtlBatchRegistrationRejection.ExtractionClaimLost, Assert.IsType<EtlBatchRegistrationOutcome.Rejected>(outcome).Reason);
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM etl_batches"));
        Assert.Equal(1, await ScalarAsync($"SELECT COUNT(*) FROM etl_run_entities WHERE run_id='{runId:D}' AND batches_created=1"));
    }

    [Fact]
    public async Task Unowned_running_run_cannot_complete_an_entity()
    {
        var runId = await NewLegacyRunningRunAsync("clients");
        await SeedLegacyEntityAsync(runId, "clients", status: "extracting", batchesCreated: 1, rowsRead: 5);

        var outcome = await _store.CompleteEtlEntityExtractionAsync(runId, Guid.NewGuid(), "clients", CursorJson(FinalClients), 1, CancellationToken.None);

        // The entity IS 'extracting' — the rejection is the absent claim fence, not a
        // missing entity row (this state was admitted pre-O1).
        Assert.Equal(EtlEntityCompletionRejection.ExtractionClaimLost, Assert.IsType<EtlEntityCompletionOutcome.Rejected>(outcome).Reason);
        Assert.Equal("extracting", await ScalarStringAsync($"SELECT status FROM etl_run_entities WHERE run_id='{runId:D}'"));
    }

    [Fact]
    public async Task Unowned_running_run_cannot_seal()
    {
        // Seal-ready legacy shape: 'done' entity with a consistent batch/counter set —
        // the exact state the unguarded seal accepted pre-O1.
        var runId = await NewLegacyRunningRunAsync("clients");
        await SeedLegacyEntityAsync(runId, "clients", status: "done", batchesCreated: 1, rowsRead: 5,
            finalWatermarkJson: CursorJson(FinalClients), expectedBatchCount: 1);
        await ExecuteSqlAsync(
            "INSERT INTO etl_batches(batch_id,run_id,entity_name,schema_version,file_path,status,row_count,sha256,compressed_size,uncompressed_size,created_at_utc) VALUES($b,$run,'clients',1,'spool/legacy.gz','ready',5,'h',10,10,$now);",
            ("$b", Guid.NewGuid().ToString("D")), ("$run", runId.ToString("D")), ("$now", Now()));
        await ExecuteSqlAsync("UPDATE etl_runs SET rows_read=5, batches_created=1 WHERE run_id=$run;", ("$run", runId.ToString("D")));

        var outcome = await _store.SealEtlRunExtractionAsync(runId, Guid.NewGuid(), CancellationToken.None);

        Assert.Equal(EtlRunSealRejection.ExtractionClaimLost, Assert.IsType<EtlRunSealOutcome.Rejected>(outcome).Reason);
        Assert.Equal("running", await ScalarStringAsync("SELECT status FROM etl_runs"));
    }

    [Fact]
    public async Task Unclaimed_running_run_cannot_fail_or_block()
    {
        var runId = await NewLegacyRunningRunAsync("clients");

        var failed = await _store.FailEtlRunAsync(runId, Guid.NewGuid(), "boom", CancellationToken.None);
        var blocked = await _store.BlockEtlRunAsync(runId, Guid.NewGuid(), "X", "x", CancellationToken.None);

        // O1: public termination requires the current extraction claim identity.
        Assert.Equal(EtlRunTerminationRejection.ExtractionClaimLost, Assert.IsType<EtlRunTerminationOutcome.Rejected>(failed).Reason);
        Assert.Equal(EtlRunTerminationRejection.ExtractionClaimLost, Assert.IsType<EtlRunTerminationOutcome.Rejected>(blocked).Reason);
        Assert.Equal("running", await ScalarStringAsync("SELECT status FROM etl_runs"));
    }

    [Fact]
    public async Task Unowned_sealed_run_cannot_claim_completion()
    {
        // A fully sealed+acknowledged run whose ownership/bindings were removed behind
        // the API: the completion claim must re-verify manifest==bindings==ownership and
        // block, never mint a sendable payload under broken ownership.
        var runId = await NewClaimedRunAsync("clients");
        var claim = ClaimOf(runId);
        await BeginExtractCompleteAsync(runId, claim);
        Assert.IsType<EtlRunSealOutcome.Sealed>(await _store.SealEtlRunExtractionAsync(runId, claim, CancellationToken.None));
        await AcknowledgeAllBatchesAsync(runId);
        await ExecuteSqlAsync("DELETE FROM etl_run_ownership_bindings WHERE run_id=$run;", ("$run", runId.ToString("D")));
        await ExecuteSqlAsync("DELETE FROM etl_entity_ownership WHERE owner_run_id=$run;", ("$run", runId.ToString("D")));

        var outcome = await _store.TryClaimRunCompletionAsync(runId, "owner-1", DateTimeOffset.UtcNow, 8, CancellationToken.None);

        var blocked = Assert.IsType<EtlRunClaimOutcome.Blocked>(outcome);
        Assert.Equal("OWNERSHIP_SET_MISMATCH", blocked.Code);
        Assert.Equal("blocked", await ScalarStringAsync($"SELECT status FROM etl_runs WHERE run_id='{runId:D}'"));
    }

    [Fact]
    public async Task Unowned_run_cannot_finalize_watermarks()
    {
        var runId = await NewClaimedRunAsync("clients");
        var claim = ClaimOf(runId);
        await BeginExtractCompleteAsync(runId, claim);
        Assert.IsType<EtlRunSealOutcome.Sealed>(await _store.SealEtlRunExtractionAsync(runId, claim, CancellationToken.None));
        await AcknowledgeAllBatchesAsync(runId);
        var completion = Assert.IsType<EtlRunClaimOutcome.Claimed>(await _store.TryClaimRunCompletionAsync(runId, "owner-1", DateTimeOffset.UtcNow, 8, CancellationToken.None));
        // Ownership broken after the completion claim: finalize must re-verify and block.
        await ExecuteSqlAsync("DELETE FROM etl_run_ownership_bindings WHERE run_id=$run;", ("$run", runId.ToString("D")));

        var outcome = await _store.FinalizeEtlRunAsync(runId, completion.Claim.ClaimId, CancellationToken.None);

        var blocked = Assert.IsType<EtlRunFinalizeOutcome.Blocked>(outcome);
        Assert.Equal("OWNERSHIP_SET_MISMATCH", blocked.Code);
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM watermarks"));
        Assert.Equal("blocked", await ScalarStringAsync($"SELECT status FROM etl_runs WHERE run_id='{runId:D}'"));
    }

    [Fact]
    public async Task Accepted_job_run_without_a_committed_claim_cannot_extract()
    {
        // An accepted durable job whose pending run was flipped to 'running' behind the
        // API (no claim, no ownership): extraction capture must still refuse.
        var (runId, _) = await AcceptJobRunAsync("entity_reload");
        await ExecuteSqlAsync("UPDATE etl_runs SET status='running', started_at_utc=$now WHERE run_id=$run;", ("$now", Now()), ("$run", runId.ToString("D")));
        var entities = await JobEntitiesAsync(runId);

        var outcome = await _store.BeginEtlEntityExtractionAsync(runId, Guid.NewGuid(),
            new EtlEntityExtractionRequest("clients", entities, SourceNamespace, "entity_reload", null), CancellationToken.None);

        Assert.Equal(EtlEntityBeginRejection.ExtractionClaimLost, Assert.IsType<EtlEntityBeginOutcome.Rejected>(outcome).Reason);
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM etl_run_entities"));
    }

    [Fact]
    public async Task Blocked_run_leaves_no_dispatchable_batches()
    {
        // A corrupted-evidence block must fence the run's pending batches to dead_letter
        // so the blocked run leaves no claimable rows behind.
        var runId = await NewClaimedRunAsync("clients");
        var claim = ClaimOf(runId);
        await BeginExtractCompleteAsync(runId, claim);
        Assert.IsType<EtlRunSealOutcome.Sealed>(await _store.SealEtlRunExtractionAsync(runId, claim, CancellationToken.None));
        await AcknowledgeAllBatchesAsync(runId);
        Assert.IsType<EtlRunClaimOutcome.Claimed>(await _store.TryClaimRunCompletionAsync(runId, "owner-1", DateTimeOffset.UtcNow, 8, CancellationToken.None));
        await _store.RecoverInterruptedEtlRunsAsync(CancellationToken.None);
        // Regression behind the API: the acknowledged batch is un-acknowledged.
        await ExecuteSqlAsync("UPDATE etl_batches SET status='ready',acknowledged_at_utc=NULL WHERE run_id=$run;", ("$run", runId.ToString("D")));

        var outcome = await _store.TryClaimRunCompletionAsync(runId, "owner-2", DateTimeOffset.UtcNow, 8, CancellationToken.None);

        Assert.IsType<EtlRunClaimOutcome.Blocked>(outcome);
        Assert.Equal(1, await ScalarAsync($"SELECT COUNT(*) FROM etl_batches WHERE run_id='{runId:D}' AND status='dead_letter'"));
    }

    // ---------- helpers ----------

    private Guid ClaimOf(Guid runId) => _extractionClaims[runId];
    private static string Now() => DateTimeOffset.UtcNow.ToString("O");
    private static string CursorJson(EtlCursor cursor) => JsonSerializer.Serialize(cursor, JsonOptions);
    private static string DefinitionJson(string entity) =>
        JsonSerializer.Serialize(MakeEntities().Single(e => e.EntityCode == entity), JsonOptions);
    private static EtlEntityDefinition[] MakeEntities() =>
    [
        new("clients", "Catalog_Clients", "Ref_Key", "UpdatedAt", "DeletionMark", ["Ref_Key", "UpdatedAt", "DeletionMark"], "incremental", 500, 10),
        new("orders", "Document_Orders", "Ref_Key", "Date", "Posted", ["Ref_Key", "Date", "Posted"], "incremental", 500, 10)
    ];
    private static EtlEntityExtractionRequest Request(string entity) =>
        new(entity, DefinitionJson(entity), SourceNamespace, QueryMode,
            JsonSerializer.Serialize(new EtlCursor(DateTimeOffset.UtcNow, null), JsonOptions));
    private static EtlBatch MakeBatch(Guid runId, string entity, int rowCount) =>
        new(Guid.NewGuid(), runId, entity, 1, $"spool/{runId:N}-{entity}-{Guid.NewGuid():N}.gz", EtlBatchStatus.Ready, rowCount,
            null, FinalClients, "hash-" + Guid.NewGuid().ToString("N"), 1000, 4000, 0, DateTimeOffset.UtcNow);

    // A legacy-shape jobless 'running' run — the exact state the pre-O1 APIs admitted.
    private async Task<Guid> NewLegacyRunningRunAsync(params string[] entities)
    {
        var runId = Guid.NewGuid();
        await _store.CreateEtlRunAsync(new EtlRun(runId, "incremental", entities, EtlRunStatus.Running), CancellationToken.None);
        return runId;
    }

    // Direct SQL seed of the durable entity evidence pre-O1 Begin/Register wrote — the
    // tests above prove the same persisted shape is now rejected without a live claim.
    private async Task SeedLegacyEntityAsync(Guid runId, string entity, string status, long batchesCreated, long rowsRead,
        string? finalWatermarkJson = null, long? expectedBatchCount = null)
    {
        await ExecuteSqlAsync(
            """
            INSERT INTO etl_run_entities(run_id,entity_name,entity_definition_json,domain_fingerprint,status,
                base_row_present,domain_status,rows_read,batches_created,final_watermark_json,expected_batch_count,
                created_at_utc,updated_at_utc,row_version)
            VALUES($run,$entity,$def,'legacy-fp',$status,0,'absent',$rows,$batches,$final,$expected,$now,$now,1);
            """,
            ("$run", runId.ToString("D")), ("$entity", entity), ("$def", DefinitionJson(entity)),
            ("$status", status), ("$rows", rowsRead), ("$batches", batchesCreated),
            ("$final", finalWatermarkJson), ("$expected", expectedBatchCount), ("$now", Now()));
    }

    // A pending run + pending durable job claimed through the real O1 transaction.
    private async Task<Guid> NewClaimedRunAsync(params string[] entities)
    {
        var runId = Guid.NewGuid();
        var jobId = Guid.NewGuid();
        var manifest = JsonSerializer.Serialize(entities, JsonOptions);
        var definitions = JsonSerializer.Serialize(entities.Select(e => MakeEntities().Single(x => x.EntityCode == e)).ToArray(), JsonOptions);
        await ExecuteSqlAsync(
            "INSERT INTO etl_runs(run_id,mode,requested_entities_json,status,configuration_version,created_at_utc,updated_at_utc,row_version) VALUES($run,$mode,$manifest,'pending',7,$now,$now,1);",
            ("$run", runId.ToString("D")), ("$mode", QueryMode), ("$manifest", manifest), ("$now", Now()));
        await ExecuteSqlAsync(
            "INSERT INTO etl_jobs(job_id,command_id,run_id,mode,entities_json,configuration_version,status,command_payload_hash,acceptance_result_json,created_at_utc,updated_at_utc,row_version) VALUES($job,$cmd,$run,$mode,$defs,7,'pending','hash','{}',$now,$now,1);",
            ("$job", jobId.ToString("D")), ("$cmd", Guid.NewGuid().ToString("D")), ("$run", runId.ToString("D")), ("$mode", QueryMode), ("$defs", definitions), ("$now", Now()));
        var claimed = Assert.IsType<EtlJobClaimOutcome.Claimed>(
            await _store.TryClaimEtlJobAsync(jobId, "test-dispatcher", DateTimeOffset.UtcNow.AddMinutes(5), DateTimeOffset.UtcNow, CancellationToken.None));
        _extractionClaims[runId] = claimed.Claim.ExtractionClaimId;
        return runId;
    }

    private async Task BeginExtractCompleteAsync(Guid runId, Guid claim)
    {
        Assert.IsType<EtlEntityBeginOutcome.Begun>(await _store.BeginEtlEntityExtractionAsync(runId, claim, Request("clients"), CancellationToken.None));
        Assert.IsType<EtlBatchRegistrationOutcome.Registered>(await _store.RegisterGuardedEtlBatchAsync(MakeBatch(runId, "clients", 5), claim, CancellationToken.None));
        Assert.IsType<EtlEntityCompletionOutcome.Completed>(await _store.CompleteEtlEntityExtractionAsync(runId, claim, "clients", CursorJson(FinalClients), 1, CancellationToken.None));
    }

    private async Task<(Guid RunId, EtlEntityDefinition[] Entities)> AcceptJobRunAsync(string mode)
    {
        var entities = mode == "entity_reload" ? [MakeEntities()[0]] : MakeEntities();
        var payloadJson = mode == "entity_reload" ? "{\"entity\":\"clients\"}" : "{}";
        var commandType = mode == "entity_reload" ? "reload_entity" : "start_full_sync";
        using var document = JsonDocument.Parse(payloadJson);
        var payload = document.RootElement.Clone();
        var command = new ErpOnecAgent.Domain.Commands.CommandEnvelope(Guid.NewGuid(), commandType, 1, 100, null, null, DateTimeOffset.UtcNow, null, null, null, PayloadHasher.Compute(payload), payload);
        await _store.StoreCommandAsync(command, DateTimeOffset.UtcNow, CancellationToken.None);
        var owner = "job-owner-" + Guid.NewGuid().ToString("N");
        var claim = await _store.TryAcquireCommandExecutionClaimAsync(command.CommandId, owner, DateTimeOffset.UtcNow, DateTimeOffset.MinValue, CancellationToken.None);
        Assert.NotNull(claim);
        var outcome = await _store.AcceptEtlJobAndCompleteCommandAsync(command.CommandId, owner, new EtlJobAcceptanceRequest(mode, entities, 7), CancellationToken.None);
        var applied = Assert.IsType<EtlJobAcceptanceOutcome.Applied>(outcome);
        return (applied.Job.RunId, entities);
    }

    private async Task<string> JobEntitiesAsync(Guid runId)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT entities_json FROM etl_jobs WHERE run_id=$run;";
        command.Parameters.AddWithValue("$run", runId.ToString("D"));
        await using var reader = await command.ExecuteReaderAsync(CancellationToken.None);
        Assert.True(await reader.ReadAsync(CancellationToken.None));
        using var doc = JsonDocument.Parse(reader.GetString(0));
        return doc.RootElement[0].GetRawText();
    }

    private async Task AcknowledgeAllBatchesAsync(Guid runId)
    {
        var batchIds = new List<string>();
        await using (var connection = await _factory.OpenAsync(CancellationToken.None))
        await using (var command = connection.CreateCommand())
        {
            command.CommandText = "SELECT batch_id FROM etl_batches WHERE run_id=$run AND status='ready';";
            command.Parameters.AddWithValue("$run", runId.ToString("D"));
            await using var reader = await command.ExecuteReaderAsync(CancellationToken.None);
            while (await reader.ReadAsync(CancellationToken.None)) batchIds.Add(reader.GetString(0));
        }
        foreach (var batchId in batchIds)
        {
            await ExecuteSqlAsync("UPDATE etl_batches SET status='uploading' WHERE batch_id=$id;", ("$id", batchId));
            await _store.AcknowledgeBatchAsync(Guid.Parse(batchId), DateTimeOffset.UtcNow, CancellationToken.None);
        }
    }

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
        return await command.ExecuteScalarAsync(CancellationToken.None) as string;
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
