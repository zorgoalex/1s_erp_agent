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
/// A05b F1 dark storage (schema v6): durable per-entity expected bases/finals captured
/// atomically before extraction, guarded batch registration, frozen seal manifests,
/// single-transaction presence+generation+cursor+domain CAS finalize (all watermarks +
/// succeeded run + finished job, or all rolled back + blocked conflict), fenced
/// claim/retry identities with an immutable stored payload, fail-closed legacy
/// quarantine, and explicit startup-only recovery. All cases run against a real migrated
/// temporary SQLite database and drive ONLY the new APIs; no worker, ERP client, or
/// RecoverAsync wiring exists yet (F1 is dark by design).
/// </summary>
public sealed class EtlFinalizeStorageTests : IAsyncLifetime
{
    private const string SourceNamespace = "onec-infobase-a";
    private const string OtherNamespace = "onec-infobase-b";
    private const string QueryMode = "incremental";
    private static readonly TimeSpan GateTimeout = TimeSpan.FromSeconds(15);
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    private static readonly EtlCursor CursorX = new(DateTimeOffset.Parse("2026-09-11T05:59:00.0000000+00:00", CultureInfo.InvariantCulture), "A7");
    private static readonly EtlCursor CursorY = new(DateTimeOffset.Parse("2026-09-12T05:59:00.0000000+00:00", CultureInfo.InvariantCulture), "B9");
    private static readonly EtlCursor FinalClients = new(DateTimeOffset.Parse("2026-09-19T09:59:00.0000000+00:00", CultureInfo.InvariantCulture), "A10");
    private static readonly EtlCursor FinalOrders = new(DateTimeOffset.Parse("2026-09-19T09:58:00.0000000+00:00", CultureInfo.InvariantCulture), "O8");

    private readonly SqliteTestDatabase _database = new();
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

    // ---------- capture ----------

    [Fact]
    public async Task Begin_on_absent_watermark_captures_absent_base_and_extracting_row()
    {
        var runId = await NewRunAsync("clients");

        var outcome = await _store.BeginEtlEntityExtractionAsync(runId, Request("clients"), CancellationToken.None);

        var begun = Assert.IsType<EtlEntityBeginOutcome.Begun>(outcome);
        Assert.False(begun.Base.BaseRowPresent);
        Assert.Null(begun.Base.CommittedCursorJson);
        Assert.Null(begun.Base.ExpectedBaseGeneration);
        Assert.Equal("absent", begun.Base.DomainStatus);
        Assert.Equal(Fingerprint("clients"), begun.Base.DomainFingerprint);

        var entity = await EntityRowAsync(runId, "clients");
        Assert.NotNull(entity);
        Assert.Equal("extracting", entity.Status);
        Assert.Equal(0, entity.BaseRowPresent);
        Assert.Null(entity.ExpectedBaseGeneration);
        Assert.Null(entity.ExpectedBaseCursorJson);
        Assert.Null(entity.ExpectedBaseDomainFingerprint);
        Assert.Equal("absent", entity.DomainStatus);
        Assert.Equal(Fingerprint("clients"), entity.DomainFingerprint);
        Assert.Equal(DefinitionJson("clients"), entity.EntityDefinitionJson);
    }

    [Fact]
    public async Task Begin_on_present_same_domain_captures_raw_base_atomically()
    {
        var runId = await NewRunAsync("clients");
        var cursorJson = CursorJson(CursorX);
        await SeedWatermarkAsync("clients", cursorJson, generation: 7, fingerprint: Fingerprint("clients"));

        var outcome = await _store.BeginEtlEntityExtractionAsync(runId, Request("clients"), CancellationToken.None);

        var begun = Assert.IsType<EtlEntityBeginOutcome.Begun>(outcome);
        Assert.True(begun.Base.BaseRowPresent);
        Assert.Equal(cursorJson, begun.Base.CommittedCursorJson);
        Assert.Equal(7, begun.Base.ExpectedBaseGeneration);
        Assert.Equal(Fingerprint("clients"), begun.Base.ExpectedBaseDomainFingerprint);
        Assert.Equal("same", begun.Base.DomainStatus);

        var entity = await EntityRowAsync(runId, "clients");
        Assert.Equal("same", entity!.DomainStatus);
        Assert.Equal(cursorJson, entity.ExpectedBaseCursorJson);
        Assert.Equal(7, entity.ExpectedBaseGeneration);
        Assert.Equal(cursorJson, entity.WatermarkFromJson);
    }

    [Fact]
    public async Task Begin_on_present_null_cursor_with_null_fingerprint_blocks_domain_unknown()
    {
        var runId = await NewRunAsync("clients");
        await SeedWatermarkAsync("clients", null, generation: 3, fingerprint: null);

        var outcome = await _store.BeginEtlEntityExtractionAsync(runId, Request("clients"), CancellationToken.None);

        Assert.Equal(EtlEntityBeginRejection.DomainUnknown, Assert.IsType<EtlEntityBeginOutcome.Rejected>(outcome).Reason);
        var entity = await EntityRowAsync(runId, "clients");
        Assert.NotNull(entity);
        Assert.Equal("failed", entity.Status);
        Assert.Equal("unknown", entity.DomainStatus);
        Assert.Equal("DOMAIN_UNKNOWN", entity.LastError);
        Assert.Equal(1, entity.BaseRowPresent);
        Assert.Equal(3, entity.ExpectedBaseGeneration);
        Assert.Equal("running", await RunStatusAsync(runId));
        // The ambiguous row is preserved, never adopted or reset.
        var watermark = await WatermarkAsync("clients");
        Assert.Equal(3, watermark.Generation);
        Assert.Null(watermark.Fingerprint);
    }

    [Fact]
    public async Task Begin_with_same_cursor_bytes_under_different_domain_fingerprint_blocks_domain_changed()
    {
        var runId = await NewRunAsync("clients");
        // Same committed cursor text, but the stored fingerprint belongs to another source.
        await SeedWatermarkAsync("clients", CursorJson(CursorX), generation: 2, fingerprint: Fingerprint("clients", OtherNamespace));

        var outcome = await _store.BeginEtlEntityExtractionAsync(runId, Request("clients"), CancellationToken.None);

        Assert.Equal(EtlEntityBeginRejection.DomainChanged, Assert.IsType<EtlEntityBeginOutcome.Rejected>(outcome).Reason);
        var entity = await EntityRowAsync(runId, "clients");
        Assert.Equal("failed", entity!.Status);
        Assert.Equal("changed", entity.DomainStatus);
        Assert.Equal("DOMAIN_CHANGED", entity.LastError);
        Assert.Equal(CursorJson(CursorX), (await WatermarkAsync("clients")).CursorJson);
    }

    [Fact]
    public async Task Begin_without_source_namespace_rejects_without_writes()
    {
        var runId = await NewRunAsync("clients");

        var outcome = await _store.BeginEtlEntityExtractionAsync(runId, Request("clients", sourceNamespace: "  "), CancellationToken.None);

        Assert.Equal(EtlEntityBeginRejection.SourceNamespaceMissing, Assert.IsType<EtlEntityBeginOutcome.Rejected>(outcome).Reason);
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM etl_run_entities"));
    }

    [Theory]
    [InlineData("not-json")]
    [InlineData("[1]")]
    [InlineData("\"x\"")]
    [InlineData("{\"a\":1,\"a\":2}")]
    [InlineData("{\"a\":1,\"A\":2}")]
    public async Task Begin_with_malformed_or_ambiguous_definition_rejects(string definitionJson)
    {
        var runId = await NewRunAsync("clients");

        await Assert.ThrowsAnyAsync<ArgumentException>(() =>
            _store.BeginEtlEntityExtractionAsync(runId, Request("clients", definitionJson: definitionJson), CancellationToken.None));

        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM etl_run_entities"));
    }

    [Fact]
    public async Task Begin_on_duplicate_entity_or_non_running_run_rejects()
    {
        var runId = await NewRunAsync("clients");
        Assert.IsType<EtlEntityBeginOutcome.Begun>(await _store.BeginEtlEntityExtractionAsync(runId, Request("clients"), CancellationToken.None));

        var duplicate = await _store.BeginEtlEntityExtractionAsync(runId, Request("clients"), CancellationToken.None);
        Assert.Equal(EtlEntityBeginRejection.RunNotAcceptingEntities, Assert.IsType<EtlEntityBeginOutcome.Rejected>(duplicate).Reason);

        var missing = await _store.BeginEtlEntityExtractionAsync(Guid.NewGuid(), Request("clients"), CancellationToken.None);
        Assert.Equal(EtlEntityBeginRejection.RunNotAcceptingEntities, Assert.IsType<EtlEntityBeginOutcome.Rejected>(missing).Reason);
        Assert.Equal(1, await ScalarAsync("SELECT COUNT(*) FROM etl_run_entities"));
    }

    [Fact]
    public async Task Begin_on_job_associated_run_enforces_frozen_definition()
    {
        var (runId, entities) = await AcceptJobRunAsync("entity_reload");
        await ExecuteSqlAsync("UPDATE etl_runs SET status='running', started_at_utc=$now WHERE run_id=$run;", ("$now", Now()), ("$run", runId.ToString("D")));

        var matching = JsonSerializer.Serialize(entities[0], JsonOptions);
        var begun = await _store.BeginEtlEntityExtractionAsync(runId, Request("clients", definitionJson: matching, queryMode: "entity_reload"), CancellationToken.None);
        Assert.IsType<EtlEntityBeginOutcome.Begun>(begun);

        var other = new EtlEntityDefinition("clients", "Catalog_Other", "Ref_Key", "UpdatedAt", "DeletionMark", ["Ref_Key"], "incremental", 500, 10);
        var mismatch = await _store.BeginEtlEntityExtractionAsync(runId, Request("clients", definitionJson: JsonSerializer.Serialize(other, JsonOptions), queryMode: "entity_reload"), CancellationToken.None);
        // Second begin on the same entity is rejected by the entity-existence guard anyway;
        // use a fresh job run for the mismatch case below.
        Assert.Equal(EtlEntityBeginRejection.RunNotAcceptingEntities, Assert.IsType<EtlEntityBeginOutcome.Rejected>(mismatch).Reason);

        var (runId2, _) = await AcceptJobRunAsync("entity_reload");
        await ExecuteSqlAsync("UPDATE etl_runs SET status='running', started_at_utc=$now WHERE run_id=$run;", ("$now", Now()), ("$run", runId2.ToString("D")));
        var mismatched = await _store.BeginEtlEntityExtractionAsync(runId2, Request("clients", definitionJson: JsonSerializer.Serialize(other, JsonOptions), queryMode: "entity_reload"), CancellationToken.None);
        Assert.Equal(EtlEntityBeginRejection.JobDefinitionMismatch, Assert.IsType<EtlEntityBeginOutcome.Rejected>(mismatched).Reason);
        Assert.Equal(0, await ScalarAsync($"SELECT COUNT(*) FROM etl_run_entities WHERE run_id='{runId2:D}'"));

        // An entity outside the frozen job selection can never be appended to its run —
        // the requested-set membership guard fires before the job definition comparison.
        var foreign = await _store.BeginEtlEntityExtractionAsync(runId2, Request("orders", definitionJson: DefinitionJson("orders"), queryMode: "entity_reload"), CancellationToken.None);
        Assert.Equal(EtlEntityBeginRejection.EntityNotInManifest, Assert.IsType<EtlEntityBeginOutcome.Rejected>(foreign).Reason);
    }

    // ---------- guarded batch registration ----------

    [Fact]
    public async Task Guarded_register_commits_batch_and_entity_run_counters_atomically()
    {
        var runId = await NewRunAsync("clients");
        Assert.IsType<EtlEntityBeginOutcome.Begun>(await _store.BeginEtlEntityExtractionAsync(runId, Request("clients"), CancellationToken.None));

        var outcome = await _store.RegisterGuardedEtlBatchAsync(MakeBatch(runId, "clients", 25), CancellationToken.None);

        Assert.IsType<EtlBatchRegistrationOutcome.Registered>(outcome);
        var entity = await EntityRowAsync(runId, "clients");
        Assert.Equal(1, entity!.BatchesCreated);
        Assert.Equal(25, entity.RowsRead);
        Assert.Equal(1, await ScalarAsync($"SELECT batches_created FROM etl_runs WHERE run_id='{runId:D}'"));
        Assert.Equal(25, await ScalarAsync($"SELECT rows_read FROM etl_runs WHERE run_id='{runId:D}'"));
        Assert.Equal(1, await ScalarAsync($"SELECT COUNT(*) FROM etl_batches WHERE run_id='{runId:D}' AND status='ready'"));
    }

    [Fact]
    public async Task Guarded_register_after_done_seal_and_completing_is_rejected_without_writes()
    {
        var runId = await NewRunAsync("clients");
        await BeginExtractCompleteAsync(runId, "clients", CursorJson(FinalClients), batchRows: 5);

        var afterDone = await _store.RegisterGuardedEtlBatchAsync(MakeBatch(runId, "clients", 3), CancellationToken.None);
        Assert.Equal(EtlBatchRegistrationRejection.EntityNotExtracting, Assert.IsType<EtlBatchRegistrationOutcome.Rejected>(afterDone).Reason);

        var entity = await EntityRowAsync(runId, "clients");
        Assert.Equal(1, entity!.BatchesCreated);
        Assert.Equal(5, entity.RowsRead);
        Assert.Equal(1, await ScalarAsync($"SELECT COUNT(*) FROM etl_batches WHERE run_id='{runId:D}'"));

        // After seal registration is fenced by the run state.
        Assert.IsType<EtlRunSealOutcome.Sealed>(await _store.SealEtlRunExtractionAsync(runId, CancellationToken.None));
        var afterSeal = await _store.RegisterGuardedEtlBatchAsync(MakeBatch(runId, "clients", 3), CancellationToken.None);
        Assert.Equal(EtlBatchRegistrationRejection.RunNotAcceptingBatches, Assert.IsType<EtlBatchRegistrationOutcome.Rejected>(afterSeal).Reason);
        Assert.Equal(1, await ScalarAsync($"SELECT COUNT(*) FROM etl_batches WHERE run_id='{runId:D}'"));

        // After claim ('completing') registration is equally fenced.
        await AcknowledgeAllBatchesAsync(runId);
        var claim = Assert.IsType<EtlRunClaimOutcome.Claimed>(await _store.TryClaimRunCompletionAsync(runId, "owner-1", DateTimeOffset.UtcNow.AddMinutes(1), 8, CancellationToken.None));
        var completing = await _store.RegisterGuardedEtlBatchAsync(MakeBatch(runId, "clients", 3), CancellationToken.None);
        Assert.Equal(EtlBatchRegistrationRejection.RunNotAcceptingBatches, Assert.IsType<EtlBatchRegistrationOutcome.Rejected>(completing).Reason);
        Assert.Equal(1, await ScalarAsync($"SELECT COUNT(*) FROM etl_batches WHERE run_id='{runId:D}'"));
        Assert.NotNull(claim.Claim.CompletePayloadJson);
    }

    [Fact]
    public async Task Guarded_register_after_done_before_seal_is_rejected()
    {
        var runId = await NewRunAsync("clients");
        Assert.IsType<EtlEntityBeginOutcome.Begun>(await _store.BeginEtlEntityExtractionAsync(runId, Request("clients"), CancellationToken.None));
        Assert.IsType<EtlBatchRegistrationOutcome.Registered>(await _store.RegisterGuardedEtlBatchAsync(MakeBatch(runId, "clients", 5), CancellationToken.None));
        Assert.IsType<EtlEntityCompletionOutcome.Completed>(await _store.CompleteEtlEntityExtractionAsync(runId, "clients", CursorJson(FinalClients), 1, CancellationToken.None));

        var late = await _store.RegisterGuardedEtlBatchAsync(MakeBatch(runId, "clients", 3), CancellationToken.None);

        Assert.Equal(EtlBatchRegistrationRejection.EntityNotExtracting, Assert.IsType<EtlBatchRegistrationOutcome.Rejected>(late).Reason);
        Assert.Equal(1, await ScalarAsync($"SELECT COUNT(*) FROM etl_batches WHERE run_id='{runId:D}'"));
    }

    [Fact]
    public async Task Guarded_register_for_unknown_entity_or_negative_rows_rejects()
    {
        var runId = await NewRunAsync("clients");
        Assert.IsType<EtlEntityBeginOutcome.Begun>(await _store.BeginEtlEntityExtractionAsync(runId, Request("clients"), CancellationToken.None));

        var unknown = await _store.RegisterGuardedEtlBatchAsync(MakeBatch(runId, "orders", 1), CancellationToken.None);
        Assert.Equal(EtlBatchRegistrationRejection.EntityNotExtracting, Assert.IsType<EtlBatchRegistrationOutcome.Rejected>(unknown).Reason);
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => _store.RegisterGuardedEtlBatchAsync(MakeBatch(runId, "clients", -1), CancellationToken.None));
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM etl_batches"));
    }

    // ---------- entity completion ----------

    [Fact]
    public async Task Complete_freezes_final_and_expected_count()
    {
        var runId = await NewRunAsync("clients");
        Assert.IsType<EtlEntityBeginOutcome.Begun>(await _store.BeginEtlEntityExtractionAsync(runId, Request("clients"), CancellationToken.None));
        Assert.IsType<EtlBatchRegistrationOutcome.Registered>(await _store.RegisterGuardedEtlBatchAsync(MakeBatch(runId, "clients", 5), CancellationToken.None));

        var outcome = await _store.CompleteEtlEntityExtractionAsync(runId, "clients", CursorJson(FinalClients), 1, CancellationToken.None);

        Assert.IsType<EtlEntityCompletionOutcome.Completed>(outcome);
        var entity = await EntityRowAsync(runId, "clients");
        Assert.Equal("done", entity!.Status);
        Assert.Equal(CursorJson(FinalClients), entity.FinalWatermarkJson);
        Assert.Equal(1, entity.ExpectedBatchCount);
    }

    [Theory]
    [InlineData("{}")]
    [InlineData("{\"updatedAtUtc\":null,\"sourceId\":null}")]
    [InlineData("[1]")]
    [InlineData("\"x\"")]
    [InlineData("not-json")]
    [InlineData("{\"updatedAtUtc\":\"not-a-date\"}")]
    [InlineData("{\"updatedAtUtc\":null,\"sourceId\":\"S\",\"extra\":1}")]
    [InlineData("{\"updatedAtUtc\":\"2026-09-19T09:59:00.0000000+00:00\",\"updatedAtUtc\":\"2026-09-19T10:00:00.0000000+00:00\"}")]
    public async Task Complete_with_invalid_final_rejects_before_write(string finalJson)
    {
        var runId = await NewRunAsync("clients");
        Assert.IsType<EtlEntityBeginOutcome.Begun>(await _store.BeginEtlEntityExtractionAsync(runId, Request("clients"), CancellationToken.None));
        Assert.IsType<EtlBatchRegistrationOutcome.Registered>(await _store.RegisterGuardedEtlBatchAsync(MakeBatch(runId, "clients", 5), CancellationToken.None));

        await Assert.ThrowsAsync<ArgumentException>(() => _store.CompleteEtlEntityExtractionAsync(runId, "clients", finalJson, 1, CancellationToken.None));

        var entity = await EntityRowAsync(runId, "clients");
        Assert.Equal("extracting", entity!.Status);
        Assert.Null(entity.FinalWatermarkJson);
    }

    [Theory]
    // Both legal cursor component shapes complete: (timestamp,NULL) and (NULL,sourceId).
    [InlineData("{\"updatedAtUtc\":\"2026-09-19T09:59:00.0000000+00:00\",\"sourceId\":null}")]
    [InlineData("{\"updatedAtUtc\":null,\"sourceId\":\"S1\"}")]
    [InlineData("{\"sourceId\":\"S2\"}")]
    public async Task Complete_with_valid_component_shapes_succeeds(string finalJson)
    {
        var runId = await NewRunAsync("clients");
        Assert.IsType<EtlEntityBeginOutcome.Begun>(await _store.BeginEtlEntityExtractionAsync(runId, Request("clients"), CancellationToken.None));
        Assert.IsType<EtlBatchRegistrationOutcome.Registered>(await _store.RegisterGuardedEtlBatchAsync(MakeBatch(runId, "clients", 5), CancellationToken.None));

        var outcome = await _store.CompleteEtlEntityExtractionAsync(runId, "clients", finalJson, 1, CancellationToken.None);

        Assert.IsType<EtlEntityCompletionOutcome.Completed>(outcome);
        Assert.Equal(finalJson, (await EntityRowAsync(runId, "clients"))!.FinalWatermarkJson);
    }

    [Fact]
    public async Task Complete_with_wrong_count_or_state_rejects()
    {
        var runId = await NewRunAsync("clients");
        Assert.IsType<EtlEntityBeginOutcome.Begun>(await _store.BeginEtlEntityExtractionAsync(runId, Request("clients"), CancellationToken.None));
        Assert.IsType<EtlBatchRegistrationOutcome.Registered>(await _store.RegisterGuardedEtlBatchAsync(MakeBatch(runId, "clients", 5), CancellationToken.None));

        var mismatch = await _store.CompleteEtlEntityExtractionAsync(runId, "clients", CursorJson(FinalClients), 2, CancellationToken.None);
        Assert.Equal(EtlEntityCompletionRejection.BatchCountMismatch, Assert.IsType<EtlEntityCompletionOutcome.Rejected>(mismatch).Reason);
        var unknown = await _store.CompleteEtlEntityExtractionAsync(runId, "orders", CursorJson(FinalOrders), 1, CancellationToken.None);
        Assert.Equal(EtlEntityCompletionRejection.EntityNotExtracting, Assert.IsType<EtlEntityCompletionOutcome.Rejected>(unknown).Reason);
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => _store.CompleteEtlEntityExtractionAsync(runId, "clients", CursorJson(FinalClients), 0, CancellationToken.None));

        var entity = await EntityRowAsync(runId, "clients");
        Assert.Equal("extracting", entity!.Status);
        Assert.Null(entity.FinalWatermarkJson);
    }

    [Fact]
    public async Task Zero_row_entity_with_explicit_empty_batch_is_valid()
    {
        var runId = await NewRunAsync("clients");
        Assert.IsType<EtlEntityBeginOutcome.Begun>(await _store.BeginEtlEntityExtractionAsync(runId, Request("clients"), CancellationToken.None));
        Assert.IsType<EtlBatchRegistrationOutcome.Registered>(await _store.RegisterGuardedEtlBatchAsync(MakeBatch(runId, "clients", 0), CancellationToken.None));
        Assert.IsType<EtlEntityCompletionOutcome.Completed>(await _store.CompleteEtlEntityExtractionAsync(runId, "clients", CursorJson(FinalClients), 1, CancellationToken.None));

        var seal = await _store.SealEtlRunExtractionAsync(runId, CancellationToken.None);

        var sealedOutcome = Assert.IsType<EtlRunSealOutcome.Sealed>(seal);
        Assert.Equal(1, sealedOutcome.EntityCount);
        Assert.Equal(1, sealedOutcome.ExpectedBatchCount);
    }

    // ---------- seal ----------

    [Fact]
    public async Task Seal_commits_uploading_with_frozen_manifest_counts()
    {
        var runId = await NewRunAsync("clients", "orders");
        await BeginExtractCompleteAsync(runId, "clients", CursorJson(FinalClients), batchRows: 5);
        await BeginExtractCompleteAsync(runId, "orders", CursorJson(FinalOrders), batchRows: 7);

        var outcome = await _store.SealEtlRunExtractionAsync(runId, CancellationToken.None);

        var sealedOutcome = Assert.IsType<EtlRunSealOutcome.Sealed>(outcome);
        Assert.Equal(2, sealedOutcome.EntityCount);
        Assert.Equal(2, sealedOutcome.ExpectedBatchCount);
        var run = await RunRowAsync(runId);
        Assert.Equal("uploading", run.Status);
        Assert.NotNull(run.SealedAtUtc);
        Assert.Equal(2, run.SealedEntityCount);
        Assert.Equal(2, run.SealedExpectedBatchCount);
    }

    [Theory]
    [InlineData("[]")]
    [InlineData("{}")]
    [InlineData("[null]")]
    [InlineData("[\"clients\",\"clients\"]")]
    [InlineData("[\"clients\",\"\"]")]
    [InlineData("[\"clients\",null]")]
    [InlineData("[42]")]
    [InlineData("null")]
    [InlineData("\"clients\"")]
    [InlineData("not-json")]
    public async Task Seal_with_malformed_manifest_refuses(string manifest)
    {
        var runId = Guid.NewGuid();
        await InsertRunAsync(runId, manifest);
        // The saved manifest is part of run identity: capture itself refuses an
        // unprovable manifest before any entity row exists.
        Assert.Equal(EtlEntityBeginRejection.RunManifestInvalid,
            Assert.IsType<EtlEntityBeginOutcome.Rejected>(await _store.BeginEtlEntityExtractionAsync(runId, Request("clients"), CancellationToken.None)).Reason);

        var outcome = await _store.SealEtlRunExtractionAsync(runId, CancellationToken.None);

        Assert.Equal(EtlRunSealRejection.ManifestInvalid, Assert.IsType<EtlRunSealOutcome.Rejected>(outcome).Reason);
        Assert.Equal("running", await RunStatusAsync(runId));
    }

    [Fact]
    public async Task Seal_with_missing_or_extra_entity_rows_refuses()
    {
        var missing = await NewRunAsync("clients", "orders");
        await BeginExtractCompleteAsync(missing, "clients", CursorJson(FinalClients), batchRows: 5);
        Assert.Equal(EtlRunSealRejection.EntitySetMismatch, Assert.IsType<EtlRunSealOutcome.Rejected>(await _store.SealEtlRunExtractionAsync(missing, CancellationToken.None)).Reason);

        // An entity outside the saved manifest is refused at capture — the extra row can
        // only exist via corruption behind the API, which the seal still catches.
        var extra = await NewRunAsync("clients");
        await BeginExtractCompleteAsync(extra, "clients", CursorJson(FinalClients), batchRows: 5);
        Assert.Equal(EtlEntityBeginRejection.EntityNotInManifest,
            Assert.IsType<EtlEntityBeginOutcome.Rejected>(await _store.BeginEtlEntityExtractionAsync(extra, Request("orders"), CancellationToken.None)).Reason);
        await ExecuteSqlAsync("""
            INSERT INTO etl_run_entities(run_id,entity_name,entity_definition_json,domain_fingerprint,status,base_row_present,domain_status,created_at_utc,updated_at_utc,row_version)
            VALUES($run,'orders','{}','x','extracting',0,'absent',$now,$now,1);
            """, ("$run", extra.ToString("D")), ("$now", Now()));
        Assert.Equal(EtlRunSealRejection.EntitySetMismatch, Assert.IsType<EtlRunSealOutcome.Rejected>(await _store.SealEtlRunExtractionAsync(extra, CancellationToken.None)).Reason);
        Assert.Equal("running", await RunStatusAsync(extra));
    }

    [Fact]
    public async Task Seal_with_non_done_entity_or_invalid_stored_final_or_count_mismatch_refuses()
    {
        var notDone = await NewRunAsync("clients");
        Assert.IsType<EtlEntityBeginOutcome.Begun>(await _store.BeginEtlEntityExtractionAsync(notDone, Request("clients"), CancellationToken.None));
        Assert.Equal(EtlRunSealRejection.EntityNotDone, Assert.IsType<EtlRunSealOutcome.Rejected>(await _store.SealEtlRunExtractionAsync(notDone, CancellationToken.None)).Reason);

        var badFinal = await NewRunAsync("clients");
        await BeginExtractCompleteAsync(badFinal, "clients", CursorJson(FinalClients), batchRows: 5);
        // A malformed stored value can never satisfy the recheck — even one written behind the API.
        await ExecuteSqlAsync("UPDATE etl_run_entities SET final_watermark_json='{}' WHERE run_id=$run;", ("$run", badFinal.ToString("D")));
        Assert.Equal(EtlRunSealRejection.FinalWatermarkInvalid, Assert.IsType<EtlRunSealOutcome.Rejected>(await _store.SealEtlRunExtractionAsync(badFinal, CancellationToken.None)).Reason);

        var countOff = await NewRunAsync("clients");
        await BeginExtractCompleteAsync(countOff, "clients", CursorJson(FinalClients), batchRows: 5);
        await ExecuteSqlAsync("UPDATE etl_run_entities SET expected_batch_count=3 WHERE run_id=$run;", ("$run", countOff.ToString("D")));
        Assert.Equal(EtlRunSealRejection.ExpectedBatchCountMismatch, Assert.IsType<EtlRunSealOutcome.Rejected>(await _store.SealEtlRunExtractionAsync(countOff, CancellationToken.None)).Reason);
        Assert.Equal("running", await RunStatusAsync(countOff));
    }

    [Fact]
    public async Task Post_seal_and_terminal_mutations_are_all_fenced()
    {
        var runId = await NewRunAsync("clients", "orders");
        await BeginExtractCompleteAsync(runId, "clients", CursorJson(FinalClients), batchRows: 5);
        Assert.IsType<EtlEntityBeginOutcome.Begun>(await _store.BeginEtlEntityExtractionAsync(runId, Request("orders"), CancellationToken.None));

        var sealedOutcome = Assert.IsType<EtlRunSealOutcome.Rejected>(await _store.SealEtlRunExtractionAsync(runId, CancellationToken.None));
        Assert.Equal(EtlRunSealRejection.EntityNotDone, sealedOutcome.Reason);

        // Complete orders then seal for real, then every mutation API is fenced.
        Assert.IsType<EtlBatchRegistrationOutcome.Registered>(await _store.RegisterGuardedEtlBatchAsync(MakeBatch(runId, "orders", 2), CancellationToken.None));
        Assert.IsType<EtlEntityCompletionOutcome.Completed>(await _store.CompleteEtlEntityExtractionAsync(runId, "orders", CursorJson(FinalOrders), 1, CancellationToken.None));
        Assert.IsType<EtlRunSealOutcome.Sealed>(await _store.SealEtlRunExtractionAsync(runId, CancellationToken.None));

        Assert.Equal(EtlEntityBeginRejection.RunNotAcceptingEntities, Assert.IsType<EtlEntityBeginOutcome.Rejected>(await _store.BeginEtlEntityExtractionAsync(runId, Request("clients"), CancellationToken.None)).Reason);
        Assert.Equal(EtlBatchRegistrationRejection.RunNotAcceptingBatches, Assert.IsType<EtlBatchRegistrationOutcome.Rejected>(await _store.RegisterGuardedEtlBatchAsync(MakeBatch(runId, "clients", 1), CancellationToken.None)).Reason);
        Assert.Equal(EtlEntityCompletionRejection.EntityNotExtracting, Assert.IsType<EtlEntityCompletionOutcome.Rejected>(await _store.CompleteEtlEntityExtractionAsync(runId, "clients", CursorJson(FinalClients), 1, CancellationToken.None)).Reason);
        Assert.Equal(EtlRunSealRejection.RunNotRunningOrAlreadySealed, Assert.IsType<EtlRunSealOutcome.Rejected>(await _store.SealEtlRunExtractionAsync(runId, CancellationToken.None)).Reason);
        Assert.Equal(EtlRunTerminationRejection.RunNotRunning, Assert.IsType<EtlRunTerminationOutcome.Rejected>(await _store.FailEtlRunAsync(runId, "x", CancellationToken.None)).Reason);
        Assert.Equal(EtlRunTerminationRejection.RunNotRunning, Assert.IsType<EtlRunTerminationOutcome.Rejected>(await _store.BlockEtlRunAsync(runId, "X", "x", CancellationToken.None)).Reason);
        Assert.Equal("uploading", await RunStatusAsync(runId));
    }

    // ---------- fail / block ----------

    [Fact]
    public async Task Fail_fences_run_entities_batches_and_blocks_job()
    {
        var (runId, _) = await AcceptJobRunAsync("bootstrap_full");
        await ExecuteSqlAsync("UPDATE etl_runs SET status='running', started_at_utc=$now WHERE run_id=$run;", ("$now", Now()), ("$run", runId.ToString("D")));
        Assert.IsType<EtlEntityBeginOutcome.Begun>(await _store.BeginEtlEntityExtractionAsync(runId, Request("clients", queryMode: "bootstrap_full"), CancellationToken.None));
        Assert.IsType<EtlBatchRegistrationOutcome.Registered>(await _store.RegisterGuardedEtlBatchAsync(MakeBatch(runId, "clients", 5), CancellationToken.None));

        var outcome = await _store.FailEtlRunAsync(runId, "extraction blew up", CancellationToken.None);

        Assert.IsType<EtlRunTerminationOutcome.Applied>(outcome);
        Assert.Equal("failed", await RunStatusAsync(runId));
        Assert.Equal("failed", (await EntityRowAsync(runId, "clients"))!.Status);
        Assert.Equal(1, await ScalarAsync($"SELECT COUNT(*) FROM etl_batches WHERE run_id='{runId:D}' AND status='dead_letter' AND last_error='RUN_FAILED'"));
        Assert.Equal("blocked", await ScalarStringAsync($"SELECT status FROM etl_jobs WHERE run_id='{runId:D}'"));
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM watermarks"));
        // Fencing bounds future local dispatch only; it cannot undo remote effects.
        Assert.Equal(1, await ScalarAsync($"SELECT COUNT(*) FROM etl_run_entities WHERE run_id='{runId:D}'"));
    }

    [Fact]
    public async Task Block_records_conflict_and_never_finishes_job()
    {
        var (runId, _) = await AcceptJobRunAsync("bootstrap_full");
        await ExecuteSqlAsync("UPDATE etl_runs SET status='running', started_at_utc=$now WHERE run_id=$run;", ("$now", Now()), ("$run", runId.ToString("D")));
        Assert.IsType<EtlEntityBeginOutcome.Begun>(await _store.BeginEtlEntityExtractionAsync(runId, Request("clients", queryMode: "bootstrap_full"), CancellationToken.None));

        var outcome = await _store.BlockEtlRunAsync(runId, "DOMAIN_UNKNOWN", "base row has no domain evidence", CancellationToken.None);

        Assert.IsType<EtlRunTerminationOutcome.Applied>(outcome);
        var run = await RunRowAsync(runId);
        Assert.Equal("blocked", run.Status);
        Assert.Equal("DOMAIN_UNKNOWN", run.FinalizeConflictCode);
        Assert.Equal("blocked", await ScalarStringAsync($"SELECT status FROM etl_jobs WHERE run_id='{runId:D}'"));
    }

    // ---------- claim ----------

    [Fact]
    public async Task Claim_mints_guid_identity_builds_payload_once_and_moves_to_completing()
    {
        var runId = await NewRunAsync("clients", "orders");
        await BeginExtractCompleteAsync(runId, "clients", CursorJson(FinalClients), batchRows: 5);
        await BeginExtractCompleteAsync(runId, "orders", CursorJson(FinalOrders), batchRows: 7);
        Assert.IsType<EtlRunSealOutcome.Sealed>(await _store.SealEtlRunExtractionAsync(runId, CancellationToken.None));
        await AcknowledgeAllBatchesAsync(runId);

        var due = await _store.GetDueRunCompletionsAsync(10, DateTimeOffset.UtcNow, CancellationToken.None);
        var candidate = Assert.Single(due);
        Assert.Equal(runId, candidate.RunId);
        Assert.False(candidate.IsReclaim);

        var outcome = await _store.TryClaimRunCompletionAsync(runId, "owner-1", DateTimeOffset.UtcNow.AddMinutes(5), 8, CancellationToken.None);

        var claimed = Assert.IsType<EtlRunClaimOutcome.Claimed>(outcome);
        Assert.NotEqual(Guid.Empty, claimed.Claim.ClaimId);
        Assert.Equal(1, claimed.Claim.Attempt);
        var run = await RunRowAsync(runId);
        Assert.Equal("completing", run.Status);
        Assert.Equal(claimed.Claim.ClaimId.ToString("D"), run.CompletionClaimId);
        Assert.Equal("owner-1", run.CompletionClaimOwner);
        Assert.Equal(1, run.CompletionAttemptCount);
        Assert.Equal(claimed.Claim.CompletePayloadJson, run.CompletePayloadJson);

        using var payload = JsonDocument.Parse(claimed.Claim.CompletePayloadJson);
        Assert.Equal(runId.ToString("D"), payload.RootElement.GetProperty("runId").GetString());
        Assert.Equal("succeeded", payload.RootElement.GetProperty("status").GetString());
        Assert.Equal(12, payload.RootElement.GetProperty("rowsRead").GetInt64());
        Assert.Equal(2, payload.RootElement.GetProperty("batchesCreated").GetInt64());
        Assert.Equal(2, payload.RootElement.GetProperty("batchesAcknowledged").GetInt64());

        // A claimed run is not handed out again until the claim is released and due.
        Assert.Empty(await _store.GetDueRunCompletionsAsync(10, DateTimeOffset.UtcNow.AddHours(1), CancellationToken.None));
        Assert.IsType<EtlRunClaimOutcome.NotClaimed>(await _store.TryClaimRunCompletionAsync(runId, "owner-2", DateTimeOffset.UtcNow, 8, CancellationToken.None));
    }

    [Fact]
    public async Task Claim_while_uploads_in_flight_is_transient_not_claimed()
    {
        var runId = await NewRunAsync("clients");
        await BeginExtractCompleteAsync(runId, "clients", CursorJson(FinalClients), batchRows: 5);
        Assert.IsType<EtlRunSealOutcome.Sealed>(await _store.SealEtlRunExtractionAsync(runId, CancellationToken.None));

        var outcome = await _store.TryClaimRunCompletionAsync(runId, "owner-1", DateTimeOffset.UtcNow, 8, CancellationToken.None);

        Assert.IsType<EtlRunClaimOutcome.NotClaimed>(outcome);
        var run = await RunRowAsync(runId);
        Assert.Equal("uploading", run.Status);
        Assert.Null(run.CompletionClaimId);
        Assert.Equal(0, run.CompletionAttemptCount);
        Assert.Null(run.CompletePayloadJson);
    }

    [Fact]
    public async Task Reclaim_mints_new_identity_and_replays_identical_payload()
    {
        var runId = await NewRunAsync("clients");
        await BeginExtractCompleteAsync(runId, "clients", CursorJson(FinalClients), batchRows: 5);
        Assert.IsType<EtlRunSealOutcome.Sealed>(await _store.SealEtlRunExtractionAsync(runId, CancellationToken.None));
        await AcknowledgeAllBatchesAsync(runId);
        var first = Assert.IsType<EtlRunClaimOutcome.Claimed>(await _store.TryClaimRunCompletionAsync(runId, "owner-1", DateTimeOffset.UtcNow, 8, CancellationToken.None));

        var retry = await _store.MarkRunCompletionRetryAsync(runId, first.Claim.ClaimId, "timeout", DateTimeOffset.UtcNow.AddSeconds(-1), 8, CancellationToken.None);
        Assert.IsType<EtlRunCompletionRetryOutcome.Scheduled>(retry);

        var second = Assert.IsType<EtlRunClaimOutcome.Claimed>(await _store.TryClaimRunCompletionAsync(runId, "owner-2", DateTimeOffset.UtcNow, 8, CancellationToken.None));
        Assert.NotEqual(first.Claim.ClaimId, second.Claim.ClaimId);
        Assert.Equal(first.Claim.CompletePayloadJson, second.Claim.CompletePayloadJson);
        Assert.Equal(2, second.Claim.Attempt);
        var run = await RunRowAsync(runId);
        Assert.Equal(first.Claim.CompletePayloadJson, run.CompletePayloadJson);
        Assert.Equal(second.Claim.ClaimId.ToString("D"), run.CompletionClaimId);
    }

    [Fact]
    public async Task Legacy_uploading_run_is_blocked_and_preserved_never_derived()
    {
        // A pre-v6 'uploading' run: no seal, no entity rows — its batch graph cannot prove
        // generations or frozen definitions.
        var runId = await NewRunAsync("clients");
        await _store.RegisterBatchAsync(MakeBatch(runId, "clients", 5), CancellationToken.None);
        await _store.MarkEtlRunExtractedAsync(runId, CancellationToken.None);
        var batchId = await ScalarStringAsync($"SELECT batch_id FROM etl_batches WHERE run_id='{runId:D}'");
        await ExecuteSqlAsync("UPDATE etl_batches SET status='uploading' WHERE batch_id=$id;", ("$id", batchId!));
        await _store.AcknowledgeBatchAsync(Guid.Parse(batchId!), DateTimeOffset.UtcNow, CancellationToken.None);

        var due = await _store.GetDueRunCompletionsAsync(10, DateTimeOffset.UtcNow, CancellationToken.None);
        Assert.Contains(due, candidate => candidate.RunId == runId);

        var outcome = await _store.TryClaimRunCompletionAsync(runId, "owner-1", DateTimeOffset.UtcNow, 8, CancellationToken.None);

        var blocked = Assert.IsType<EtlRunClaimOutcome.Blocked>(outcome);
        Assert.Equal("LEGACY_UNRESOLVABLE", blocked.Code);
        var run = await RunRowAsync(runId);
        Assert.Equal("blocked", run.Status);
        Assert.Equal("LEGACY_UNRESOLVABLE", run.FinalizeConflictCode);
        Assert.Null(run.CompletionClaimId);
        Assert.Null(run.CompletePayloadJson);
        Assert.Equal(1, await ScalarAsync($"SELECT COUNT(*) FROM etl_batches WHERE run_id='{runId:D}' AND status='acknowledged'"));
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM watermarks"));
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM etl_run_entities"));
    }

    [Fact]
    public async Task Two_concurrent_claimants_exactly_one_wins()
    {
        var runId = await NewRunAsync("clients");
        await BeginExtractCompleteAsync(runId, "clients", CursorJson(FinalClients), batchRows: 5);
        Assert.IsType<EtlRunSealOutcome.Sealed>(await _store.SealEtlRunExtractionAsync(runId, CancellationToken.None));
        await AcknowledgeAllBatchesAsync(runId);

        var storeA = new SqliteAgentStore(_factory, new SqliteMigrator(_factory));
        var storeB = new SqliteAgentStore(_factory, new SqliteMigrator(_factory));
        var barrier = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var attemptA = Task.Run(async () => { await barrier.Task; return await storeA.TryClaimRunCompletionAsync(runId, "owner-A", DateTimeOffset.UtcNow, 8, CancellationToken.None); });
        var attemptB = Task.Run(async () => { await barrier.Task; return await storeB.TryClaimRunCompletionAsync(runId, "owner-B", DateTimeOffset.UtcNow, 8, CancellationToken.None); });
        barrier.SetResult(true);
        var outcomes = await Task.WhenAll(attemptA, attemptB).WaitAsync(GateTimeout);

        Assert.Single(outcomes.OfType<EtlRunClaimOutcome.Claimed>());
        Assert.Single(outcomes.OfType<EtlRunClaimOutcome.NotClaimed>());
        var run = await RunRowAsync(runId);
        Assert.Equal("completing", run.Status);
        Assert.Equal(1, run.CompletionAttemptCount);
    }

    // ---------- finalize ----------

    [Fact]
    public async Task Finalize_commits_all_watermarks_run_and_job_atomically()
    {
        var singleJob = await AcceptJobRunAsync("entity_reload");
        var singleRun = singleJob.RunId;
        await ExecuteSqlAsync("UPDATE etl_runs SET status='running', started_at_utc=$now WHERE run_id=$run;", ("$now", Now()), ("$run", singleRun.ToString("D")));
        var defJson = JsonSerializer.Serialize(singleJob.Entities[0], JsonOptions);
        Assert.IsType<EtlEntityBeginOutcome.Begun>(await _store.BeginEtlEntityExtractionAsync(singleRun, Request("clients", definitionJson: defJson, queryMode: "entity_reload"), CancellationToken.None));
        Assert.IsType<EtlBatchRegistrationOutcome.Registered>(await _store.RegisterGuardedEtlBatchAsync(MakeBatch(singleRun, "clients", 5), CancellationToken.None));
        Assert.IsType<EtlEntityCompletionOutcome.Completed>(await _store.CompleteEtlEntityExtractionAsync(singleRun, "clients", CursorJson(FinalClients), 1, CancellationToken.None));
        Assert.IsType<EtlRunSealOutcome.Sealed>(await _store.SealEtlRunExtractionAsync(singleRun, CancellationToken.None));
        await AcknowledgeAllBatchesAsync(singleRun);
        var claim = Assert.IsType<EtlRunClaimOutcome.Claimed>(await _store.TryClaimRunCompletionAsync(singleRun, "owner-1", DateTimeOffset.UtcNow, 8, CancellationToken.None));

        var outcome = await _store.FinalizeEtlRunAsync(singleRun, claim.Claim.ClaimId, CancellationToken.None);

        Assert.IsType<EtlRunFinalizeOutcome.Finalized>(outcome);
        var watermark = await WatermarkAsync("clients");
        Assert.Equal(CursorJson(FinalClients), watermark.CursorJson);
        Assert.Equal(1, watermark.Generation);
        Assert.Equal(Fingerprint("clients", queryMode: "entity_reload"), watermark.Fingerprint);
        Assert.Equal(singleRun.ToString("D"), watermark.LastRunId);
        var run = await RunRowAsync(singleRun);
        Assert.Equal("succeeded", run.Status);
        Assert.NotNull(run.FinishedAtUtc);
        Assert.NotNull(run.CompletionAcknowledgedAtUtc);
        Assert.Equal(claim.Claim.ClaimId.ToString("D"), run.CompletionClaimId);
        Assert.Null(run.CompletionClaimOwner);
        Assert.Equal("finished", await ScalarStringAsync($"SELECT status FROM etl_jobs WHERE run_id='{singleRun:D}'"));
    }

    [Fact]
    public async Task Finalize_applies_present_update_and_absent_insert_cas_shapes()
    {
        var runId = await NewRunAsync("clients", "orders");
        // clients: present row with matching domain (UPDATE path); orders: absent (INSERT path).
        await SeedWatermarkAsync("clients", CursorJson(CursorX), generation: 4, fingerprint: Fingerprint("clients"));
        await BeginExtractCompleteAsync(runId, "clients", CursorJson(FinalClients), batchRows: 5);
        await BeginExtractCompleteAsync(runId, "orders", CursorJson(FinalOrders), batchRows: 7);
        Assert.IsType<EtlRunSealOutcome.Sealed>(await _store.SealEtlRunExtractionAsync(runId, CancellationToken.None));
        await AcknowledgeAllBatchesAsync(runId);
        var claim = Assert.IsType<EtlRunClaimOutcome.Claimed>(await _store.TryClaimRunCompletionAsync(runId, "owner-1", DateTimeOffset.UtcNow, 8, CancellationToken.None));

        var outcome = await _store.FinalizeEtlRunAsync(runId, claim.Claim.ClaimId, CancellationToken.None);

        Assert.IsType<EtlRunFinalizeOutcome.Finalized>(outcome);
        var clients = await WatermarkAsync("clients");
        Assert.Equal(CursorJson(FinalClients), clients.CursorJson);
        Assert.Equal(5, clients.Generation); // 4 + 1
        Assert.Equal(Fingerprint("clients"), clients.Fingerprint);
        var orders = await WatermarkAsync("orders");
        Assert.Equal(CursorJson(FinalOrders), orders.CursorJson);
        Assert.Equal(1, orders.Generation);
        Assert.Equal(Fingerprint("orders"), orders.Fingerprint);
    }

    [Fact]
    public async Task Finalize_conflict_on_second_entity_rolls_back_every_watermark()
    {
        var runId = await NewRunAsync("clients", "orders");
        await SeedWatermarkAsync("clients", CursorJson(CursorX), generation: 1, fingerprint: Fingerprint("clients"));
        await SeedWatermarkAsync("orders", CursorJson(CursorX), generation: 2, fingerprint: Fingerprint("orders"));
        await BeginExtractCompleteAsync(runId, "clients", CursorJson(FinalClients), batchRows: 5);
        await BeginExtractCompleteAsync(runId, "orders", CursorJson(FinalOrders), batchRows: 7);
        Assert.IsType<EtlRunSealOutcome.Sealed>(await _store.SealEtlRunExtractionAsync(runId, CancellationToken.None));
        await AcknowledgeAllBatchesAsync(runId);
        var claim = Assert.IsType<EtlRunClaimOutcome.Claimed>(await _store.TryClaimRunCompletionAsync(runId, "owner-1", DateTimeOffset.UtcNow, 8, CancellationToken.None));

        // Controlled CAS mismatch on entity 2 ('orders'): another writer moved the cursor.
        await ExecuteSqlAsync("UPDATE watermarks SET committed_cursor_json=$cursor, generation=generation+1 WHERE entity_name='orders';", ("$cursor", CursorJson(CursorY)));

        var outcome = await _store.FinalizeEtlRunAsync(runId, claim.Claim.ClaimId, CancellationToken.None);

        var blocked = Assert.IsType<EtlRunFinalizeOutcome.Blocked>(outcome);
        Assert.Equal("GENERATION_MISMATCH", blocked.Code);
        // ALL watermark changes rolled back — including entity 1's already-applied CAS.
        var clients = await WatermarkAsync("clients");
        Assert.Equal(CursorJson(CursorX), clients.CursorJson);
        Assert.Equal(1, clients.Generation);
        var orders = await WatermarkAsync("orders");
        Assert.Equal(CursorJson(CursorY), orders.CursorJson);
        Assert.Equal(3, orders.Generation);
        var run = await RunRowAsync(runId);
        Assert.Equal("blocked", run.Status);
        Assert.Equal("GENERATION_MISMATCH", run.FinalizeConflictCode);
        Assert.Null(run.CompletionClaimId);
        Assert.Equal(claim.Claim.CompletePayloadJson, run.CompletePayloadJson);
    }

    [Fact]
    public async Task Aba_cycle_is_detected_by_generation_not_cursor_text()
    {
        var runId = await NewRunAsync("clients");
        await SeedWatermarkAsync("clients", CursorJson(CursorX), generation: 1, fingerprint: Fingerprint("clients"));
        await BeginExtractCompleteAsync(runId, "clients", CursorJson(FinalClients), batchRows: 5);
        Assert.IsType<EtlRunSealOutcome.Sealed>(await _store.SealEtlRunExtractionAsync(runId, CancellationToken.None));
        await AcknowledgeAllBatchesAsync(runId);
        var claim = Assert.IsType<EtlRunClaimOutcome.Claimed>(await _store.TryClaimRunCompletionAsync(runId, "owner-1", DateTimeOffset.UtcNow, 8, CancellationToken.None));

        // X -> Y -> X under generation-bumping writers: the cursor text returns to the
        // captured bytes but the generation moved twice.
        await ExecuteSqlAsync("UPDATE watermarks SET committed_cursor_json=$y, generation=generation+1 WHERE entity_name='clients';", ("$y", CursorJson(CursorY)));
        await ExecuteSqlAsync("UPDATE watermarks SET committed_cursor_json=$x, generation=generation+1 WHERE entity_name='clients';", ("$x", CursorJson(CursorX)));

        var outcome = await _store.FinalizeEtlRunAsync(runId, claim.Claim.ClaimId, CancellationToken.None);

        Assert.Equal("GENERATION_MISMATCH", Assert.IsType<EtlRunFinalizeOutcome.Blocked>(outcome).Code);
        var watermark = await WatermarkAsync("clients");
        Assert.Equal(CursorJson(CursorX), watermark.CursorJson);
        Assert.Equal(3, watermark.Generation);
        Assert.Equal("blocked", await RunStatusAsync(runId));
    }

    [Fact]
    public async Task Same_cursor_commit_is_detected_and_same_cursor_finalize_still_commits()
    {
        var runId = await NewRunAsync("clients");
        await SeedWatermarkAsync("clients", CursorJson(CursorX), generation: 1, fingerprint: Fingerprint("clients"));
        await BeginExtractCompleteAsync(runId, "clients", CursorJson(FinalClients), batchRows: 5);
        Assert.IsType<EtlRunSealOutcome.Sealed>(await _store.SealEtlRunExtractionAsync(runId, CancellationToken.None));
        await AcknowledgeAllBatchesAsync(runId);
        var claim = Assert.IsType<EtlRunClaimOutcome.Claimed>(await _store.TryClaimRunCompletionAsync(runId, "owner-1", DateTimeOffset.UtcNow, 8, CancellationToken.None));

        // Another writer committed the SAME cursor bytes under a new generation.
        await ExecuteSqlAsync("UPDATE watermarks SET generation=generation+1 WHERE entity_name='clients';", []);

        var blocked = Assert.IsType<EtlRunFinalizeOutcome.Blocked>(await _store.FinalizeEtlRunAsync(runId, claim.Claim.ClaimId, CancellationToken.None));
        Assert.Equal("GENERATION_MISMATCH", blocked.Code);

        // And the inverse: a final byte-identical to the captured base still commits —
        // generation must advance exactly once for the new finalize.
        var runId2 = await NewRunAsync("clients");
        var begun = Assert.IsType<EtlEntityBeginOutcome.Begun>(await _store.BeginEtlEntityExtractionAsync(runId2, Request("clients"), CancellationToken.None));
        Assert.Equal(2, begun.Base.ExpectedBaseGeneration); // foreign writer moved X to gen 2
        Assert.Equal(CursorJson(CursorX), begun.Base.CommittedCursorJson);
        Assert.IsType<EtlBatchRegistrationOutcome.Registered>(await _store.RegisterGuardedEtlBatchAsync(MakeBatch(runId2, "clients", 3), CancellationToken.None));
        Assert.IsType<EtlEntityCompletionOutcome.Completed>(await _store.CompleteEtlEntityExtractionAsync(runId2, "clients", CursorJson(CursorX), 1, CancellationToken.None));
        Assert.IsType<EtlRunSealOutcome.Sealed>(await _store.SealEtlRunExtractionAsync(runId2, CancellationToken.None));
        await AcknowledgeAllBatchesAsync(runId2);
        var claim2 = Assert.IsType<EtlRunClaimOutcome.Claimed>(await _store.TryClaimRunCompletionAsync(runId2, "owner-1", DateTimeOffset.UtcNow, 8, CancellationToken.None));
        Assert.IsType<EtlRunFinalizeOutcome.Finalized>(await _store.FinalizeEtlRunAsync(runId2, claim2.Claim.ClaimId, CancellationToken.None));
        var watermark = await WatermarkAsync("clients");
        Assert.Equal(CursorJson(CursorX), watermark.CursorJson);
        Assert.Equal(3, watermark.Generation); // captured base 2 + exactly one finalize increment
    }

    [Fact]
    public async Task Absent_then_present_and_present_then_vanished_fail_closed()
    {
        // Absent capture, row created before finalize: guarded INSERT refuses to overwrite.
        var runId = await NewRunAsync("clients");
        await BeginExtractCompleteAsync(runId, "clients", CursorJson(FinalClients), batchRows: 5);
        Assert.IsType<EtlRunSealOutcome.Sealed>(await _store.SealEtlRunExtractionAsync(runId, CancellationToken.None));
        await AcknowledgeAllBatchesAsync(runId);
        var claim = Assert.IsType<EtlRunClaimOutcome.Claimed>(await _store.TryClaimRunCompletionAsync(runId, "owner-1", DateTimeOffset.UtcNow, 8, CancellationToken.None));
        await ExecuteSqlAsync("INSERT INTO watermarks(entity_name,committed_cursor_json,generation,domain_fingerprint,updated_at_utc) VALUES('clients',$c,9,$fp,$now);",
            ("$c", CursorJson(CursorY)), ("$fp", Fingerprint("clients")), ("$now", Now()));

        var blocked = Assert.IsType<EtlRunFinalizeOutcome.Blocked>(await _store.FinalizeEtlRunAsync(runId, claim.Claim.ClaimId, CancellationToken.None));
        Assert.Equal("ROW_UNEXPECTEDLY_PRESENT", blocked.Code);
        Assert.Equal(CursorJson(CursorY), (await WatermarkAsync("clients")).CursorJson);

        // Present capture, row deleted before finalize: the pure UPDATE can never INSERT.
        var runId2 = await NewRunAsync("orders");
        await SeedWatermarkAsync("orders", CursorJson(CursorX), generation: 2, fingerprint: Fingerprint("orders"));
        await BeginExtractCompleteAsync(runId2, "orders", CursorJson(FinalOrders), batchRows: 4);
        Assert.IsType<EtlRunSealOutcome.Sealed>(await _store.SealEtlRunExtractionAsync(runId2, CancellationToken.None));
        await AcknowledgeAllBatchesAsync(runId2);
        var claim2 = Assert.IsType<EtlRunClaimOutcome.Claimed>(await _store.TryClaimRunCompletionAsync(runId2, "owner-1", DateTimeOffset.UtcNow, 8, CancellationToken.None));
        await ExecuteSqlAsync("DELETE FROM watermarks WHERE entity_name='orders';", []);

        var blocked2 = Assert.IsType<EtlRunFinalizeOutcome.Blocked>(await _store.FinalizeEtlRunAsync(runId2, claim2.Claim.ClaimId, CancellationToken.None));
        Assert.Equal("ROW_VANISHED", blocked2.Code);
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM watermarks WHERE entity_name='orders'"));
    }

    [Fact]
    public async Task Domain_fingerprint_change_after_capture_is_domain_mismatch()
    {
        var runId = await NewRunAsync("clients");
        await SeedWatermarkAsync("clients", CursorJson(CursorX), generation: 1, fingerprint: Fingerprint("clients"));
        await BeginExtractCompleteAsync(runId, "clients", CursorJson(FinalClients), batchRows: 5);
        Assert.IsType<EtlRunSealOutcome.Sealed>(await _store.SealEtlRunExtractionAsync(runId, CancellationToken.None));
        await AcknowledgeAllBatchesAsync(runId);
        var claim = Assert.IsType<EtlRunClaimOutcome.Claimed>(await _store.TryClaimRunCompletionAsync(runId, "owner-1", DateTimeOffset.UtcNow, 8, CancellationToken.None));
        await ExecuteSqlAsync("UPDATE watermarks SET domain_fingerprint='foreign-domain' WHERE entity_name='clients';", []);

        var blocked = Assert.IsType<EtlRunFinalizeOutcome.Blocked>(await _store.FinalizeEtlRunAsync(runId, claim.Claim.ClaimId, CancellationToken.None));
        Assert.Equal("DOMAIN_MISMATCH", blocked.Code);
        Assert.Equal(CursorJson(CursorX), (await WatermarkAsync("clients")).CursorJson);
    }

    [Fact]
    public async Task Non_monotonic_final_cursor_still_commits_exact_cas()
    {
        var runId = await NewRunAsync("clients");
        await SeedWatermarkAsync("clients", CursorJson(CursorX), generation: 1, fingerprint: Fingerprint("clients"));
        // Final is "older" than the captured base — no MAX/comparison participates.
        var older = new EtlCursor(CursorX.UpdatedAtUtc!.Value.AddHours(-5), "A0");
        await BeginExtractCompleteAsync(runId, "clients", CursorJson(older), batchRows: 5);
        Assert.IsType<EtlRunSealOutcome.Sealed>(await _store.SealEtlRunExtractionAsync(runId, CancellationToken.None));
        await AcknowledgeAllBatchesAsync(runId);
        var claim = Assert.IsType<EtlRunClaimOutcome.Claimed>(await _store.TryClaimRunCompletionAsync(runId, "owner-1", DateTimeOffset.UtcNow, 8, CancellationToken.None));

        var outcome = await _store.FinalizeEtlRunAsync(runId, claim.Claim.ClaimId, CancellationToken.None);

        Assert.IsType<EtlRunFinalizeOutcome.Finalized>(outcome);
        Assert.Equal(CursorJson(older), (await WatermarkAsync("clients")).CursorJson);
        Assert.Equal(2, (await WatermarkAsync("clients")).Generation);
    }

    [Fact]
    public async Task Stale_claim_and_replay_finalize_write_nothing()
    {
        var runId = await NewRunAsync("clients");
        await BeginExtractCompleteAsync(runId, "clients", CursorJson(FinalClients), batchRows: 5);
        Assert.IsType<EtlRunSealOutcome.Sealed>(await _store.SealEtlRunExtractionAsync(runId, CancellationToken.None));
        await AcknowledgeAllBatchesAsync(runId);
        var first = Assert.IsType<EtlRunClaimOutcome.Claimed>(await _store.TryClaimRunCompletionAsync(runId, "owner-1", DateTimeOffset.UtcNow, 8, CancellationToken.None));
        Assert.IsType<EtlRunCompletionRetryOutcome.Scheduled>(await _store.MarkRunCompletionRetryAsync(runId, first.Claim.ClaimId, "boom", DateTimeOffset.UtcNow.AddSeconds(-1), 8, CancellationToken.None));
        var second = Assert.IsType<EtlRunClaimOutcome.Claimed>(await _store.TryClaimRunCompletionAsync(runId, "owner-2", DateTimeOffset.UtcNow, 8, CancellationToken.None));

        // The superseded claim identity can write nothing.
        Assert.IsType<EtlRunFinalizeOutcome.ClaimLost>(await _store.FinalizeEtlRunAsync(runId, first.Claim.ClaimId, CancellationToken.None));
        Assert.IsType<EtlRunCompletionRetryOutcome.ClaimLost>(await _store.MarkRunCompletionRetryAsync(runId, first.Claim.ClaimId, "boom", DateTimeOffset.UtcNow, 8, CancellationToken.None));

        Assert.IsType<EtlRunFinalizeOutcome.Finalized>(await _store.FinalizeEtlRunAsync(runId, second.Claim.ClaimId, CancellationToken.None));
        var run = await RunRowAsync(runId);
        var watermarksBefore = await ScalarAsync("SELECT COUNT(*) FROM watermarks");
        var rowVersion = run.RowVersion;

        // Successful replay / stale claim after success: zero extra writes.
        Assert.IsType<EtlRunFinalizeOutcome.ClaimLost>(await _store.FinalizeEtlRunAsync(runId, second.Claim.ClaimId, CancellationToken.None));
        Assert.IsType<EtlRunFinalizeOutcome.ClaimLost>(await _store.FinalizeEtlRunAsync(runId, Guid.NewGuid(), CancellationToken.None));
        Assert.Equal(watermarksBefore, await ScalarAsync("SELECT COUNT(*) FROM watermarks"));
        Assert.Equal(rowVersion, (await RunRowAsync(runId)).RowVersion);
        Assert.Equal("succeeded", await RunStatusAsync(runId));
    }

    [Fact]
    public async Task Fault_at_kth_watermark_rolls_back_everything_and_retry_converges()
    {
        var runId = await NewRunAsync("clients", "orders");
        await SeedWatermarkAsync("clients", CursorJson(CursorX), generation: 1, fingerprint: Fingerprint("clients"));
        await SeedWatermarkAsync("orders", CursorJson(CursorX), generation: 1, fingerprint: Fingerprint("orders"));
        await BeginExtractCompleteAsync(runId, "clients", CursorJson(FinalClients), batchRows: 5);
        await BeginExtractCompleteAsync(runId, "orders", CursorJson(FinalOrders), batchRows: 7);
        Assert.IsType<EtlRunSealOutcome.Sealed>(await _store.SealEtlRunExtractionAsync(runId, CancellationToken.None));
        await AcknowledgeAllBatchesAsync(runId);
        var claim = Assert.IsType<EtlRunClaimOutcome.Claimed>(await _store.TryClaimRunCompletionAsync(runId, "owner-1", DateTimeOffset.UtcNow, 8, CancellationToken.None));

        // Abort the k-th CAS write (entity 'orders', second in deterministic order).
        await ExecuteSqlAsync("CREATE TRIGGER fail_cas_orders BEFORE UPDATE ON watermarks WHEN NEW.entity_name='orders' BEGIN SELECT RAISE(ABORT,'injected CAS fault'); END;", []);

        await Assert.ThrowsAsync<SqliteException>(() => _store.FinalizeEtlRunAsync(runId, claim.Claim.ClaimId, CancellationToken.None));

        // The whole transaction rolled back: claim preserved, outcome unknown, nothing committed.
        var run = await RunRowAsync(runId);
        Assert.Equal("completing", run.Status);
        Assert.Equal(claim.Claim.ClaimId.ToString("D"), run.CompletionClaimId);
        Assert.Equal(1, (await WatermarkAsync("clients")).Generation);
        Assert.Equal(CursorJson(CursorX), (await WatermarkAsync("clients")).CursorJson);
        Assert.Equal(1, (await WatermarkAsync("orders")).Generation);

        await ExecuteSqlAsync("DROP TRIGGER fail_cas_orders;", []);
        var retry = await _store.FinalizeEtlRunAsync(runId, claim.Claim.ClaimId, CancellationToken.None);
        Assert.IsType<EtlRunFinalizeOutcome.Finalized>(retry);
        Assert.Equal("succeeded", await RunStatusAsync(runId));
        Assert.Equal(2, (await WatermarkAsync("clients")).Generation);
        Assert.Equal(2, (await WatermarkAsync("orders")).Generation);
    }

    [Fact]
    public async Task Durable_final_wins_over_batch_timestamp_order()
    {
        var runId = await NewRunAsync("clients");
        Assert.IsType<EtlEntityBeginOutcome.Begun>(await _store.BeginEtlEntityExtractionAsync(runId, Request("clients"), CancellationToken.None));
        // Two batches with identical created_at_utc — ties cannot order a final.
        var same = DateTimeOffset.Parse("2026-09-19T10:00:01.0000000+00:00", CultureInfo.InvariantCulture);
        Assert.IsType<EtlBatchRegistrationOutcome.Registered>(await _store.RegisterGuardedEtlBatchAsync(MakeBatch(runId, "clients", 5, watermarkTo: CursorY, createdAtUtc: same), CancellationToken.None));
        Assert.IsType<EtlBatchRegistrationOutcome.Registered>(await _store.RegisterGuardedEtlBatchAsync(MakeBatch(runId, "clients", 5, watermarkTo: CursorX, createdAtUtc: same), CancellationToken.None));
        Assert.IsType<EtlEntityCompletionOutcome.Completed>(await _store.CompleteEtlEntityExtractionAsync(runId, "clients", CursorJson(FinalClients), 2, CancellationToken.None));
        Assert.IsType<EtlRunSealOutcome.Sealed>(await _store.SealEtlRunExtractionAsync(runId, CancellationToken.None));
        await AcknowledgeAllBatchesAsync(runId);
        var claim = Assert.IsType<EtlRunClaimOutcome.Claimed>(await _store.TryClaimRunCompletionAsync(runId, "owner-1", DateTimeOffset.UtcNow, 8, CancellationToken.None));

        Assert.IsType<EtlRunFinalizeOutcome.Finalized>(await _store.FinalizeEtlRunAsync(runId, claim.Claim.ClaimId, CancellationToken.None));

        // The explicitly stored final wins; batch created_at ordering was never consulted.
        Assert.Equal(CursorJson(FinalClients), (await WatermarkAsync("clients")).CursorJson);
    }

    // ---------- retry ----------

    [Fact]
    public async Task Retry_schedules_next_attempt_and_only_due_reclaims()
    {
        var runId = await NewRunAsync("clients");
        await BeginExtractCompleteAsync(runId, "clients", CursorJson(FinalClients), batchRows: 5);
        Assert.IsType<EtlRunSealOutcome.Sealed>(await _store.SealEtlRunExtractionAsync(runId, CancellationToken.None));
        await AcknowledgeAllBatchesAsync(runId);
        var first = Assert.IsType<EtlRunClaimOutcome.Claimed>(await _store.TryClaimRunCompletionAsync(runId, "owner-1", DateTimeOffset.UtcNow, 8, CancellationToken.None));

        Assert.IsType<EtlRunCompletionRetryOutcome.Scheduled>(
            await _store.MarkRunCompletionRetryAsync(runId, first.Claim.ClaimId, "timeout", DateTimeOffset.UtcNow.AddHours(1), 8, CancellationToken.None));

        // Claim released but not yet due: not listed, not claimable, payload preserved.
        Assert.Empty(await _store.GetDueRunCompletionsAsync(10, DateTimeOffset.UtcNow, CancellationToken.None));
        Assert.IsType<EtlRunClaimOutcome.NotClaimed>(await _store.TryClaimRunCompletionAsync(runId, "owner-2", DateTimeOffset.UtcNow, 8, CancellationToken.None));
        var waiting = await RunRowAsync(runId);
        Assert.Equal("completing", waiting.Status);
        Assert.Null(waiting.CompletionClaimId);
        Assert.Equal(first.Claim.CompletePayloadJson, waiting.CompletePayloadJson);

        await ExecuteSqlAsync("UPDATE etl_runs SET next_completion_attempt_at_utc=$past WHERE run_id=$run;",
            ("$past", DateTimeOffset.UtcNow.AddMinutes(-1).ToString("O")), ("$run", runId.ToString("D")));
        var due = Assert.Single(await _store.GetDueRunCompletionsAsync(10, DateTimeOffset.UtcNow, CancellationToken.None));
        Assert.Equal(runId, due.RunId);
        Assert.True(due.IsReclaim);

        var reclaim = Assert.IsType<EtlRunClaimOutcome.Claimed>(await _store.TryClaimRunCompletionAsync(runId, "owner-2", DateTimeOffset.UtcNow, 8, CancellationToken.None));
        Assert.NotEqual(first.Claim.ClaimId, reclaim.Claim.ClaimId);
        Assert.Equal(first.Claim.CompletePayloadJson, reclaim.Claim.CompletePayloadJson);
    }

    [Fact]
    public async Task Corrupted_sealed_evidence_blocks_the_run_at_claim()
    {
        var runId = await NewRunAsync("clients");
        await BeginExtractCompleteAsync(runId, "clients", CursorJson(FinalClients), batchRows: 5);
        Assert.IsType<EtlRunSealOutcome.Sealed>(await _store.SealEtlRunExtractionAsync(runId, CancellationToken.None));
        await AcknowledgeAllBatchesAsync(runId);

        // A sealed run whose persisted evidence no longer matches its own seal must never
        // be completed: fabricate corruption behind the API and claim.
        await ExecuteSqlAsync("UPDATE etl_runs SET requested_entities_json='[]' WHERE run_id=$run;", ("$run", runId.ToString("D")));

        var outcome = await _store.TryClaimRunCompletionAsync(runId, "owner-1", DateTimeOffset.UtcNow, 8, CancellationToken.None);

        var blocked = Assert.IsType<EtlRunClaimOutcome.Blocked>(outcome);
        Assert.Equal("SEAL_VIOLATED", blocked.Code);
        var run = await RunRowAsync(runId);
        Assert.Equal("blocked", run.Status);
        Assert.Equal("SEAL_VIOLATED", run.FinalizeConflictCode);
        Assert.Null(run.CompletePayloadJson);
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM watermarks"));
    }

    [Fact]
    public async Task Attempts_exhausted_blocks_run_and_job_preserving_payload()
    {
        var (runId, _) = await AcceptJobRunAsync("entity_reload");
        await ExecuteSqlAsync("UPDATE etl_runs SET status='running', started_at_utc=$now WHERE run_id=$run;", ("$now", Now()), ("$run", runId.ToString("D")));
        await BeginExtractCompleteAsync(runId, "clients", CursorJson(FinalClients), batchRows: 5, definitionJson: DefinitionJson("clients"), queryMode: "entity_reload");
        Assert.IsType<EtlRunSealOutcome.Sealed>(await _store.SealEtlRunExtractionAsync(runId, CancellationToken.None));
        await AcknowledgeAllBatchesAsync(runId);
        var claim = Assert.IsType<EtlRunClaimOutcome.Claimed>(await _store.TryClaimRunCompletionAsync(runId, "owner-1", DateTimeOffset.UtcNow, 1, CancellationToken.None));

        var outcome = await _store.MarkRunCompletionRetryAsync(runId, claim.Claim.ClaimId, "timeout", DateTimeOffset.UtcNow, maxAttempts: 1, CancellationToken.None);

        Assert.IsType<EtlRunCompletionRetryOutcome.Blocked>(outcome);
        var run = await RunRowAsync(runId);
        Assert.Equal("blocked", run.Status);
        Assert.Equal("COMPLETION_ATTEMPTS_EXHAUSTED", run.FinalizeConflictCode);
        Assert.Null(run.CompletionClaimId);
        Assert.Equal(claim.Claim.CompletePayloadJson, run.CompletePayloadJson);
        Assert.Equal("blocked", await ScalarStringAsync($"SELECT status FROM etl_jobs WHERE run_id='{runId:D}'"));
    }

    // ---------- recovery ----------

    [Fact]
    public async Task Recovery_releases_dead_claim_preserving_payload_and_reclaim_replays_it()
    {
        var runId = await NewRunAsync("clients");
        await BeginExtractCompleteAsync(runId, "clients", CursorJson(FinalClients), batchRows: 5);
        Assert.IsType<EtlRunSealOutcome.Sealed>(await _store.SealEtlRunExtractionAsync(runId, CancellationToken.None));
        await AcknowledgeAllBatchesAsync(runId);
        var claim = Assert.IsType<EtlRunClaimOutcome.Claimed>(await _store.TryClaimRunCompletionAsync(runId, "dead-owner", DateTimeOffset.UtcNow, 8, CancellationToken.None));

        // Existing RecoverAsync is unchanged: it does not touch the new claim.
        await _store.RecoverAsync(CancellationToken.None);
        Assert.Equal(claim.Claim.ClaimId.ToString("D"), (await RunRowAsync(runId)).CompletionClaimId);

        var recovered = await _store.RecoverInterruptedEtlRunsAsync(CancellationToken.None);
        Assert.Equal(1, recovered.ClaimsReleased);
        Assert.Equal(0, recovered.RunsBlocked);

        var run = await RunRowAsync(runId);
        Assert.Equal("completing", run.Status);
        Assert.Null(run.CompletionClaimId);
        Assert.Equal(claim.Claim.CompletePayloadJson, run.CompletePayloadJson);

        var reclaim = Assert.IsType<EtlRunClaimOutcome.Claimed>(await _store.TryClaimRunCompletionAsync(runId, "owner-2", DateTimeOffset.UtcNow, 8, CancellationToken.None));
        Assert.NotEqual(claim.Claim.ClaimId, reclaim.Claim.ClaimId);
        Assert.Equal(claim.Claim.CompletePayloadJson, reclaim.Claim.CompletePayloadJson);
    }

    [Fact]
    public async Task Recovery_blocks_interrupted_running_runs_and_fences_pending_batches()
    {
        var (runId, _) = await AcceptJobRunAsync("entity_reload");
        await ExecuteSqlAsync("UPDATE etl_runs SET status='running', started_at_utc=$now WHERE run_id=$run;", ("$now", Now()), ("$run", runId.ToString("D")));
        Assert.IsType<EtlEntityBeginOutcome.Begun>(await _store.BeginEtlEntityExtractionAsync(runId, Request("clients", queryMode: "entity_reload"), CancellationToken.None));
        Assert.IsType<EtlBatchRegistrationOutcome.Registered>(await _store.RegisterGuardedEtlBatchAsync(MakeBatch(runId, "clients", 5), CancellationToken.None));
        var pendingRun = await NewPendingRunAsync();

        var recovered = await _store.RecoverInterruptedEtlRunsAsync(CancellationToken.None);

        Assert.Equal(1, recovered.RunsBlocked);
        Assert.Equal(1, recovered.EntitiesFailed);
        Assert.Equal(1, recovered.BatchesFenced);
        Assert.Equal(1, recovered.JobsBlocked);
        var run = await RunRowAsync(runId);
        Assert.Equal("blocked", run.Status);
        Assert.Equal("INTERRUPTED_NO_CHECKPOINT", run.FinalizeConflictCode);
        Assert.Equal("failed", (await EntityRowAsync(runId, "clients"))!.Status);
        Assert.Equal(1, await ScalarAsync($"SELECT COUNT(*) FROM etl_batches WHERE run_id='{runId:D}' AND status='dead_letter' AND last_error='RUN_INTERRUPTED'"));
        Assert.Equal("blocked", await ScalarStringAsync($"SELECT status FROM etl_jobs WHERE run_id='{runId:D}'"));
        Assert.Equal("pending", await RunStatusAsync(pendingRun));
    }

    // ---------- root-review regressions: strict cursor parser ----------

    [Theory]
    // Broad-culture/non-JSON timestamps and empty/whitespace source ids can never be
    // written: the persisted cursor must deserialize through the SAME strict parser the
    // committed-watermark read path uses (System.Text.Json EtlCursor, ISO 8601 only).
    [InlineData("{\"updatedAtUtc\":\"09/25/2026\"}")]
    [InlineData("{\"updatedAtUtc\":\"09/25/2026 10:30:00 +00:00\"}")]
    [InlineData("{\"updatedAtUtc\":\"Sep 25, 2026\"}")]
    [InlineData("{\"updatedAtUtc\":null,\"sourceId\":\"\"}")]
    [InlineData("{\"updatedAtUtc\":null,\"sourceId\":\"   \"}")]
    [InlineData("{\"updatedAtUtc\":\"2026-09-19T09:59:00.0000000+00:00\",\"sourceId\":\"\"}")]
    public async Task Complete_with_non_iso_timestamp_or_empty_source_rejects_before_write(string finalJson)
    {
        var runId = await NewRunAsync("clients");
        Assert.IsType<EtlEntityBeginOutcome.Begun>(await _store.BeginEtlEntityExtractionAsync(runId, Request("clients"), CancellationToken.None));
        Assert.IsType<EtlBatchRegistrationOutcome.Registered>(await _store.RegisterGuardedEtlBatchAsync(MakeBatch(runId, "clients", 5), CancellationToken.None));

        await Assert.ThrowsAsync<ArgumentException>(() => _store.CompleteEtlEntityExtractionAsync(runId, "clients", finalJson, 1, CancellationToken.None));

        var entity = await EntityRowAsync(runId, "clients");
        Assert.Equal("extracting", entity!.Status);
        Assert.Null(entity.FinalWatermarkJson);
    }

    [Fact]
    public async Task Seal_rejects_persisted_final_the_read_path_cannot_parse()
    {
        var runId = await NewRunAsync("clients");
        await BeginExtractCompleteAsync(runId, "clients", CursorJson(FinalClients), batchRows: 5);
        // A broad-culture value smuggled behind the API must fail the persisted-read
        // guard exactly like a write-path value: it cannot survive a roundtrip read.
        await ExecuteSqlAsync("UPDATE etl_run_entities SET final_watermark_json=$final WHERE run_id=$run;",
            ("$final", "{\"updatedAtUtc\":\"09/25/2026\",\"sourceId\":null}"), ("$run", runId.ToString("D")));

        var outcome = await _store.SealEtlRunExtractionAsync(runId, CancellationToken.None);

        Assert.Equal(EtlRunSealRejection.FinalWatermarkInvalid, Assert.IsType<EtlRunSealOutcome.Rejected>(outcome).Reason);
        Assert.Equal("running", await RunStatusAsync(runId));
    }

    [Fact]
    public async Task Finalized_cursor_roundtrips_through_the_committed_watermark_read()
    {
        var runId = await NewRunAsync("clients");
        await BeginExtractCompleteAsync(runId, "clients", CursorJson(FinalClients), batchRows: 5);
        Assert.IsType<EtlRunSealOutcome.Sealed>(await _store.SealEtlRunExtractionAsync(runId, CancellationToken.None));
        await AcknowledgeAllBatchesAsync(runId);
        var claim = Assert.IsType<EtlRunClaimOutcome.Claimed>(await _store.TryClaimRunCompletionAsync(runId, "owner-1", DateTimeOffset.UtcNow, 8, CancellationToken.None));
        Assert.IsType<EtlRunFinalizeOutcome.Finalized>(await _store.FinalizeEtlRunAsync(runId, claim.Claim.ClaimId, CancellationToken.None));

        var committed = await _store.GetCommittedWatermarkAsync("clients", CancellationToken.None);

        Assert.Equal(FinalClients, committed);
    }

    // ---------- root-review regressions: capture identity guards ----------

    [Fact]
    public async Task Begin_rejects_query_mode_different_from_the_saved_run_mode()
    {
        var runId = await NewRunAsync("clients"); // saved mode 'incremental'

        var outcome = await _store.BeginEtlEntityExtractionAsync(runId,
            new EtlEntityExtractionRequest("clients", DefinitionJson("clients"), SourceNamespace, "bootstrap_full", null), CancellationToken.None);

        Assert.Equal(EtlEntityBeginRejection.RunModeMismatch, Assert.IsType<EtlEntityBeginOutcome.Rejected>(outcome).Reason);
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM etl_run_entities"));
    }

    [Fact]
    public async Task Begin_rejects_unsupported_saved_run_mode()
    {
        var runId = Guid.NewGuid();
        await ExecuteSqlAsync(
            "INSERT INTO etl_runs(run_id,mode,requested_entities_json,status,started_at_utc,created_at_utc,updated_at_utc,row_version) VALUES($run,'reconcile_keys','[\"clients\"]','running',$now,$now,$now,1);",
            ("$run", runId.ToString("D")), ("$now", Now()));

        var outcome = await _store.BeginEtlEntityExtractionAsync(runId,
            new EtlEntityExtractionRequest("clients", DefinitionJson("clients"), SourceNamespace, "reconcile_keys", null), CancellationToken.None);

        Assert.Equal(EtlEntityBeginRejection.RunModeMismatch, Assert.IsType<EtlEntityBeginOutcome.Rejected>(outcome).Reason);
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM etl_run_entities"));
    }

    [Fact]
    public async Task Begin_rejects_entity_outside_the_saved_requested_set()
    {
        var runId = await NewRunAsync("clients");

        var outcome = await _store.BeginEtlEntityExtractionAsync(runId, Request("orders"), CancellationToken.None);

        Assert.Equal(EtlEntityBeginRejection.EntityNotInManifest, Assert.IsType<EtlEntityBeginOutcome.Rejected>(outcome).Reason);
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM etl_run_entities"));
    }

    [Fact]
    public async Task Begin_rejects_definition_that_is_not_the_typed_definition_for_the_entity()
    {
        var runId = await NewRunAsync("clients");
        string[] invalid =
        [
            "{}",                                          // no usable typed definition
            "{\"entityCode\":\"clients\"}",                // missing required path/keyfields
            JsonSerializer.Serialize(MakeEntities()[1], JsonOptions), // valid 'orders' definition for a 'clients' request
        ];
        foreach (var definitionJson in invalid)
        {
            var outcome = await _store.BeginEtlEntityExtractionAsync(runId,
                new EtlEntityExtractionRequest("clients", definitionJson, SourceNamespace, QueryMode, null), CancellationToken.None);
            Assert.Equal(EtlEntityBeginRejection.EntityDefinitionInvalid, Assert.IsType<EtlEntityBeginOutcome.Rejected>(outcome).Reason);
        }
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM etl_run_entities"));
    }

    [Fact]
    public async Task Begin_rejects_when_associated_job_identity_is_inconsistent()
    {
        var (runId, entities) = await AcceptJobRunAsync("entity_reload");
        await ExecuteSqlAsync("UPDATE etl_runs SET status='running', started_at_utc=$now WHERE run_id=$run;", ("$now", Now()), ("$run", runId.ToString("D")));
        // Durable identity drift behind the API: the job's frozen mode no longer equals the run's.
        await ExecuteSqlAsync("UPDATE etl_jobs SET mode='bootstrap_full' WHERE run_id=$run;", ("$run", runId.ToString("D")));

        var outcome = await _store.BeginEtlEntityExtractionAsync(runId,
            new EtlEntityExtractionRequest("clients", JsonSerializer.Serialize(entities[0], JsonOptions), SourceNamespace, "entity_reload", null), CancellationToken.None);

        Assert.Equal(EtlEntityBeginRejection.JobInconsistent, Assert.IsType<EtlEntityBeginOutcome.Rejected>(outcome).Reason);
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM etl_run_entities"));

        var (runId2, entities2) = await AcceptJobRunAsync("entity_reload");
        await ExecuteSqlAsync("UPDATE etl_runs SET status='running', started_at_utc=$now WHERE run_id=$run;", ("$now", Now()), ("$run", runId2.ToString("D")));
        await ExecuteSqlAsync("UPDATE etl_jobs SET configuration_version=configuration_version+1 WHERE run_id=$run;", ("$run", runId2.ToString("D")));

        var outcome2 = await _store.BeginEtlEntityExtractionAsync(runId2,
            new EtlEntityExtractionRequest("clients", JsonSerializer.Serialize(entities2[0], JsonOptions), SourceNamespace, "entity_reload", null), CancellationToken.None);

        Assert.Equal(EtlEntityBeginRejection.JobInconsistent, Assert.IsType<EtlEntityBeginOutcome.Rejected>(outcome2).Reason);
        Assert.Equal(0, await ScalarAsync($"SELECT COUNT(*) FROM etl_run_entities WHERE run_id='{runId2:D}'"));
    }

    // ---------- root-review regressions: reclaim payload + seal recheck ----------

    [Theory]
    [InlineData("{}")]
    [InlineData("[1]")]
    [InlineData("{\"runId\":\"RUN\",\"status\":\"failed\",\"rowsRead\":5,\"batchesCreated\":1,\"batchesAcknowledged\":1,\"completedAtUtc\":\"2026-09-25T00:00:00.0000000+00:00\"}")] // wrong status
    [InlineData("{\"runId\":\"RUN\",\"status\":\"succeeded\",\"rowsRead\":999,\"batchesCreated\":1,\"batchesAcknowledged\":1,\"completedAtUtc\":\"2026-09-25T00:00:00.0000000+00:00\"}")] // wrong counters
    [InlineData("{\"runId\":\"RUN\",\"status\":\"succeeded\",\"rowsRead\":5,\"batchesCreated\":1,\"batchesAcknowledged\":0,\"completedAtUtc\":\"2026-09-25T00:00:00.0000000+00:00\"}")] // wrong ack count
    [InlineData("{\"runId\":\"RUN\",\"status\":\"succeeded\",\"rowsRead\":5,\"batchesCreated\":1,\"batchesAcknowledged\":1,\"completedAtUtc\":\"2026-09-25T00:00:00.0000000+00:00\",\"extra\":1}")] // extra field
    [InlineData("{\"runId\":\"RUN\",\"status\":\"succeeded\",\"rowsRead\":5,\"batchesCreated\":1,\"batchesAcknowledged\":1}")] // missing field
    [InlineData("{\"runId\":\"RUN\",\"RUNID\":\"other\",\"status\":\"succeeded\",\"rowsRead\":5,\"batchesCreated\":1,\"batchesAcknowledged\":1,\"completedAtUtc\":\"2026-09-25T00:00:00.0000000+00:00\"}")] // duplicate name by case
    public async Task Reclaim_rejects_corrupted_or_foreign_stored_payload_and_blocks(string payloadTemplate)
    {
        var runId = await NewRunAsync("clients");
        await BeginExtractCompleteAsync(runId, "clients", CursorJson(FinalClients), batchRows: 5);
        Assert.IsType<EtlRunSealOutcome.Sealed>(await _store.SealEtlRunExtractionAsync(runId, CancellationToken.None));
        await AcknowledgeAllBatchesAsync(runId);
        Assert.IsType<EtlRunClaimOutcome.Claimed>(await _store.TryClaimRunCompletionAsync(runId, "owner-1", DateTimeOffset.UtcNow, 8, CancellationToken.None));
        Assert.Equal(1, (await _store.RecoverInterruptedEtlRunsAsync(CancellationToken.None)).ClaimsReleased);

        var corrupted = payloadTemplate
            .Replace("RUNID", "runid", StringComparison.Ordinal)
            .Replace("RUN", runId.ToString("D"), StringComparison.Ordinal);
        await ExecuteSqlAsync("UPDATE etl_runs SET complete_payload_json=$payload WHERE run_id=$run;", ("$payload", corrupted), ("$run", runId.ToString("D")));

        var outcome = await _store.TryClaimRunCompletionAsync(runId, "owner-2", DateTimeOffset.UtcNow, 8, CancellationToken.None);

        var blocked = Assert.IsType<EtlRunClaimOutcome.Blocked>(outcome);
        Assert.Equal("SEAL_VIOLATED", blocked.Code);
        Assert.Equal("blocked", await RunStatusAsync(runId));
        Assert.Equal(corrupted, (await RunRowAsync(runId)).CompletePayloadJson); // evidence preserved verbatim
    }

    [Fact]
    public async Task Reclaim_rejects_a_payload_belonging_to_a_different_run()
    {
        var runId = await NewRunAsync("clients");
        await BeginExtractCompleteAsync(runId, "clients", CursorJson(FinalClients), batchRows: 5);
        Assert.IsType<EtlRunSealOutcome.Sealed>(await _store.SealEtlRunExtractionAsync(runId, CancellationToken.None));
        await AcknowledgeAllBatchesAsync(runId);
        Assert.IsType<EtlRunClaimOutcome.Claimed>(await _store.TryClaimRunCompletionAsync(runId, "owner-1", DateTimeOffset.UtcNow, 8, CancellationToken.None));
        await _store.RecoverInterruptedEtlRunsAsync(CancellationToken.None);

        var foreign = $"{{\"runId\":\"{Guid.NewGuid():D}\",\"status\":\"succeeded\",\"rowsRead\":5,\"batchesCreated\":1,\"batchesAcknowledged\":1,\"completedAtUtc\":\"2026-09-25T00:00:00.0000000+00:00\"}}";
        await ExecuteSqlAsync("UPDATE etl_runs SET complete_payload_json=$payload WHERE run_id=$run;", ("$payload", foreign), ("$run", runId.ToString("D")));

        var outcome = await _store.TryClaimRunCompletionAsync(runId, "owner-2", DateTimeOffset.UtcNow, 8, CancellationToken.None);

        Assert.IsType<EtlRunClaimOutcome.Blocked>(outcome);
        Assert.Equal("blocked", await RunStatusAsync(runId));
    }

    [Fact]
    public async Task Reclaim_after_recovery_blocks_when_seal_evidence_no_longer_holds()
    {
        var runId = await NewRunAsync("clients", "orders");
        await BeginExtractCompleteAsync(runId, "clients", CursorJson(FinalClients), batchRows: 5);
        await BeginExtractCompleteAsync(runId, "orders", CursorJson(FinalOrders), batchRows: 7);
        Assert.IsType<EtlRunSealOutcome.Sealed>(await _store.SealEtlRunExtractionAsync(runId, CancellationToken.None));
        await AcknowledgeAllBatchesAsync(runId);
        var claim = Assert.IsType<EtlRunClaimOutcome.Claimed>(await _store.TryClaimRunCompletionAsync(runId, "owner-1", DateTimeOffset.UtcNow, 8, CancellationToken.None));
        await _store.RecoverInterruptedEtlRunsAsync(CancellationToken.None);

        // Seal evidence regressed after the first claim: an entity row is gone — the
        // reclaim must re-verify the FULL seal, not merely remaining entity statuses.
        await ExecuteSqlAsync("DELETE FROM etl_run_entities WHERE run_id=$run AND entity_name='orders';", ("$run", runId.ToString("D")));

        var outcome = await _store.TryClaimRunCompletionAsync(runId, "owner-2", DateTimeOffset.UtcNow, 8, CancellationToken.None);

        Assert.IsType<EtlRunClaimOutcome.Blocked>(outcome);
        Assert.Equal("blocked", await RunStatusAsync(runId));
        Assert.Equal(claim.Claim.CompletePayloadJson, (await RunRowAsync(runId)).CompletePayloadJson);
    }

    [Fact]
    public async Task Reclaim_after_recovery_blocks_when_a_batch_is_no_longer_acknowledged()
    {
        var runId = await NewRunAsync("clients");
        await BeginExtractCompleteAsync(runId, "clients", CursorJson(FinalClients), batchRows: 5);
        Assert.IsType<EtlRunSealOutcome.Sealed>(await _store.SealEtlRunExtractionAsync(runId, CancellationToken.None));
        await AcknowledgeAllBatchesAsync(runId);
        Assert.IsType<EtlRunClaimOutcome.Claimed>(await _store.TryClaimRunCompletionAsync(runId, "owner-1", DateTimeOffset.UtcNow, 8, CancellationToken.None));
        await _store.RecoverInterruptedEtlRunsAsync(CancellationToken.None);

        // The ACK evidence the payload was built on changed after the first claim.
        await ExecuteSqlAsync("UPDATE etl_batches SET status='ready',acknowledged_at_utc=NULL WHERE run_id=$run;", ("$run", runId.ToString("D")));

        var outcome = await _store.TryClaimRunCompletionAsync(runId, "owner-2", DateTimeOffset.UtcNow, 8, CancellationToken.None);

        Assert.IsType<EtlRunClaimOutcome.Blocked>(outcome);
        Assert.Equal("blocked", await RunStatusAsync(runId));
    }

    [Fact]
    public async Task Reclaim_after_recovery_blocks_when_expected_base_shape_is_corrupt()
    {
        var runId = await NewRunAsync("clients");
        await SeedWatermarkAsync("clients", CursorJson(CursorX), generation: 4, fingerprint: Fingerprint("clients"));
        await BeginExtractCompleteAsync(runId, "clients", CursorJson(FinalClients), batchRows: 5);
        Assert.IsType<EtlRunSealOutcome.Sealed>(await _store.SealEtlRunExtractionAsync(runId, CancellationToken.None));
        await AcknowledgeAllBatchesAsync(runId);
        Assert.IsType<EtlRunClaimOutcome.Claimed>(await _store.TryClaimRunCompletionAsync(runId, "owner-1", DateTimeOffset.UtcNow, 8, CancellationToken.None));
        await _store.RecoverInterruptedEtlRunsAsync(CancellationToken.None);

        // A non-NULL expected base that cannot be a committed cursor is corrupt evidence.
        await ExecuteSqlAsync("UPDATE etl_run_entities SET expected_base_cursor_json='not-a-cursor' WHERE run_id=$run;", ("$run", runId.ToString("D")));

        var outcome = await _store.TryClaimRunCompletionAsync(runId, "owner-2", DateTimeOffset.UtcNow, 8, CancellationToken.None);

        Assert.IsType<EtlRunClaimOutcome.Blocked>(outcome);
        Assert.Equal("blocked", await RunStatusAsync(runId));
    }

    // ---------- root-review regressions: attempt bound at claim admission ----------

    [Fact]
    public async Task Claim_admission_enforces_the_attempt_bound_across_crash_recovery_cycles()
    {
        var runId = await NewRunAsync("clients");
        await BeginExtractCompleteAsync(runId, "clients", CursorJson(FinalClients), batchRows: 5);
        Assert.IsType<EtlRunSealOutcome.Sealed>(await _store.SealEtlRunExtractionAsync(runId, CancellationToken.None));
        await AcknowledgeAllBatchesAsync(runId);

        Assert.IsType<EtlRunClaimOutcome.Claimed>(await _store.TryClaimRunCompletionAsync(runId, "owner-1", DateTimeOffset.UtcNow, 2, CancellationToken.None));
        // Crash before the ERP call: recovery releases the claim; the bound must survive.
        Assert.Equal(1, (await _store.RecoverInterruptedEtlRunsAsync(CancellationToken.None)).ClaimsReleased);
        var second = Assert.IsType<EtlRunClaimOutcome.Claimed>(await _store.TryClaimRunCompletionAsync(runId, "owner-2", DateTimeOffset.UtcNow, 2, CancellationToken.None));
        Assert.Equal(1, (await _store.RecoverInterruptedEtlRunsAsync(CancellationToken.None)).ClaimsReleased);

        // Attempts already reached the bound: a third send is never admissible — the run
        // blocks with the immutable payload and evidence retained.
        var third = await _store.TryClaimRunCompletionAsync(runId, "owner-3", DateTimeOffset.UtcNow, 2, CancellationToken.None);

        var blocked = Assert.IsType<EtlRunClaimOutcome.Blocked>(third);
        Assert.Equal("COMPLETION_ATTEMPTS_EXHAUSTED", blocked.Code);
        var run = await RunRowAsync(runId);
        Assert.Equal("blocked", run.Status);
        Assert.Equal("COMPLETION_ATTEMPTS_EXHAUSTED", run.FinalizeConflictCode);
        Assert.Null(run.CompletionClaimId);
        Assert.Equal(second.Claim.CompletePayloadJson, run.CompletePayloadJson);
        Assert.Equal(2, await ScalarAsync($"SELECT completion_max_attempts FROM etl_runs WHERE run_id='{runId:D}'"));
        Assert.Equal(2, run.CompletionAttemptCount); // no phantom attempt was minted
    }

    // ---------- root-review regressions: job-associated atomicity ----------

    [Fact]
    public async Task Fault_at_final_job_update_rolls_back_watermarks_run_and_job_atomically()
    {
        var (runId, entities) = await AcceptJobRunAsync("entity_reload");
        await ExecuteSqlAsync("UPDATE etl_runs SET status='running', started_at_utc=$now WHERE run_id=$run;", ("$now", Now()), ("$run", runId.ToString("D")));
        await BeginExtractCompleteAsync(runId, "clients", CursorJson(FinalClients), batchRows: 5,
            definitionJson: JsonSerializer.Serialize(entities[0], JsonOptions), queryMode: "entity_reload");
        Assert.IsType<EtlRunSealOutcome.Sealed>(await _store.SealEtlRunExtractionAsync(runId, CancellationToken.None));
        await AcknowledgeAllBatchesAsync(runId);
        var claim = Assert.IsType<EtlRunClaimOutcome.Claimed>(await _store.TryClaimRunCompletionAsync(runId, "owner-1", DateTimeOffset.UtcNow, 8, CancellationToken.None));

        // Fault at the LAST durable write of finalize: the job 'finished' update aborts,
        // so every watermark + run + job write of the same transaction rolls back.
        await ExecuteSqlAsync("CREATE TRIGGER fail_job_finish BEFORE UPDATE ON etl_jobs WHEN NEW.status='finished' BEGIN SELECT RAISE(ABORT,'injected job-write fault'); END;", []);
        await Assert.ThrowsAsync<SqliteException>(() => _store.FinalizeEtlRunAsync(runId, claim.Claim.ClaimId, CancellationToken.None));

        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM watermarks"));
        var run = await RunRowAsync(runId);
        Assert.Equal("completing", run.Status);
        Assert.Equal(claim.Claim.ClaimId.ToString("D"), run.CompletionClaimId);
        Assert.Equal("pending", await ScalarStringAsync($"SELECT status FROM etl_jobs WHERE run_id='{runId:D}'"));

        await ExecuteSqlAsync("DROP TRIGGER fail_job_finish;", []);
        Assert.IsType<EtlRunFinalizeOutcome.Finalized>(await _store.FinalizeEtlRunAsync(runId, claim.Claim.ClaimId, CancellationToken.None));
        Assert.Equal("succeeded", await RunStatusAsync(runId));
        Assert.Equal("finished", await ScalarStringAsync($"SELECT status FROM etl_jobs WHERE run_id='{runId:D}'"));
        Assert.Equal(CursorJson(FinalClients), (await WatermarkAsync("clients")).CursorJson);
    }

    [Fact]
    public async Task Job_run_finalize_conflict_blocks_the_job_and_rolls_back_watermarks()
    {
        var (runId, entities) = await AcceptJobRunAsync("entity_reload");
        await ExecuteSqlAsync("UPDATE etl_runs SET status='running', started_at_utc=$now WHERE run_id=$run;", ("$now", Now()), ("$run", runId.ToString("D")));
        var defJson = JsonSerializer.Serialize(entities[0], JsonOptions);
        await SeedWatermarkAsync("clients", CursorJson(CursorX), generation: 3,
            fingerprint: EtlDomainFingerprint.Compute(SourceNamespace, "clients", defJson, "entity_reload"));
        await BeginExtractCompleteAsync(runId, "clients", CursorJson(FinalClients), batchRows: 5, definitionJson: defJson, queryMode: "entity_reload");
        Assert.IsType<EtlRunSealOutcome.Sealed>(await _store.SealEtlRunExtractionAsync(runId, CancellationToken.None));
        await AcknowledgeAllBatchesAsync(runId);
        var claim = Assert.IsType<EtlRunClaimOutcome.Claimed>(await _store.TryClaimRunCompletionAsync(runId, "owner-1", DateTimeOffset.UtcNow, 8, CancellationToken.None));

        // CAS conflict on the job-associated run: all watermark writes roll back and the
        // blocked run + conflict diagnostic + blocked job commit together.
        await ExecuteSqlAsync("UPDATE watermarks SET committed_cursor_json=$y, generation=generation+1 WHERE entity_name='clients';", ("$y", CursorJson(CursorY)));

        var blocked = Assert.IsType<EtlRunFinalizeOutcome.Blocked>(await _store.FinalizeEtlRunAsync(runId, claim.Claim.ClaimId, CancellationToken.None));

        Assert.Equal("GENERATION_MISMATCH", blocked.Code);
        var watermark = await WatermarkAsync("clients");
        Assert.Equal(CursorJson(CursorY), watermark.CursorJson);
        Assert.Equal(4, watermark.Generation);
        Assert.Equal("blocked", await RunStatusAsync(runId));
        Assert.Equal("GENERATION_MISMATCH", (await RunRowAsync(runId)).FinalizeConflictCode);
        Assert.Equal("blocked", await ScalarStringAsync($"SELECT status FROM etl_jobs WHERE run_id='{runId:D}'"));
    }

    // ---------- root-review regressions: immutable payload transition invariant ----------

    [Fact]
    public async Task Reclaim_after_recovery_with_lost_payload_blocks_and_never_regenerates()
    {
        var (runId, entities) = await AcceptJobRunAsync("entity_reload");
        await ExecuteSqlAsync("UPDATE etl_runs SET status='running', started_at_utc=$now WHERE run_id=$run;", ("$now", Now()), ("$run", runId.ToString("D")));
        await BeginExtractCompleteAsync(runId, "clients", CursorJson(FinalClients), batchRows: 5,
            definitionJson: JsonSerializer.Serialize(entities[0], JsonOptions), queryMode: "entity_reload");
        Assert.IsType<EtlRunSealOutcome.Sealed>(await _store.SealEtlRunExtractionAsync(runId, CancellationToken.None));
        await AcknowledgeAllBatchesAsync(runId);
        var claim = Assert.IsType<EtlRunClaimOutcome.Claimed>(await _store.TryClaimRunCompletionAsync(runId, "owner-1", DateTimeOffset.UtcNow, 8, CancellationToken.None));

        // Corrupt persisted evidence behind the API: the immutable body is gone. A
        // previously claimed run can never fabricate a replacement or authorize a send.
        await ExecuteSqlAsync("UPDATE etl_runs SET complete_payload_json=NULL WHERE run_id=$run;", ("$run", runId.ToString("D")));
        Assert.Equal(1, (await _store.RecoverInterruptedEtlRunsAsync(CancellationToken.None)).ClaimsReleased);

        var outcome = await _store.TryClaimRunCompletionAsync(runId, "owner-2", DateTimeOffset.UtcNow, 8, CancellationToken.None);

        var blocked = Assert.IsType<EtlRunClaimOutcome.Blocked>(outcome);
        Assert.Equal("SEAL_VIOLATED", blocked.Code);
        var run = await RunRowAsync(runId);
        Assert.Equal("blocked", run.Status);
        Assert.Equal("SEAL_VIOLATED", run.FinalizeConflictCode);
        Assert.Null(run.CompletePayloadJson); // the missing body is preserved as evidence — never regenerated
        Assert.Null(run.CompletionClaimId);
        Assert.NotEqual(claim.Claim.ClaimId.ToString("D"), run.CompletionClaimId);
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM watermarks"));
        Assert.NotEqual("finished", await ScalarStringAsync($"SELECT status FROM etl_jobs WHERE run_id='{runId:D}'"));
    }

    [Fact]
    public async Task Fresh_claim_with_fabricated_stored_payload_blocks()
    {
        var runId = await NewRunAsync("clients");
        await BeginExtractCompleteAsync(runId, "clients", CursorJson(FinalClients), batchRows: 5);
        Assert.IsType<EtlRunSealOutcome.Sealed>(await _store.SealEtlRunExtractionAsync(runId, CancellationToken.None));
        await AcknowledgeAllBatchesAsync(runId);

        // A schema-valid body planted behind the API before ANY claim: a first claim can
        // never inherit a payload — fabricated evidence blocks, it is never replayed.
        var fabricated = $"{{\"runId\":\"{runId:D}\",\"status\":\"succeeded\",\"rowsRead\":5,\"batchesCreated\":1,\"batchesAcknowledged\":1,\"completedAtUtc\":\"2026-09-25T00:00:00.0000000+00:00\"}}";
        await ExecuteSqlAsync("UPDATE etl_runs SET complete_payload_json=$payload WHERE run_id=$run;", ("$payload", fabricated), ("$run", runId.ToString("D")));

        var outcome = await _store.TryClaimRunCompletionAsync(runId, "owner-1", DateTimeOffset.UtcNow, 8, CancellationToken.None);

        var blocked = Assert.IsType<EtlRunClaimOutcome.Blocked>(outcome);
        Assert.Equal("SEAL_VIOLATED", blocked.Code);
        Assert.Equal("blocked", await RunStatusAsync(runId));
        Assert.Equal(fabricated, (await RunRowAsync(runId)).CompletePayloadJson); // evidence preserved verbatim
    }

    [Theory]
    [InlineData("NULL")]
    [InlineData("CORRUPT")]
    public async Task Finalize_with_missing_or_corrupted_payload_blocks_the_run(string corruption)
    {
        var runId = await NewRunAsync("clients");
        await BeginExtractCompleteAsync(runId, "clients", CursorJson(FinalClients), batchRows: 5);
        Assert.IsType<EtlRunSealOutcome.Sealed>(await _store.SealEtlRunExtractionAsync(runId, CancellationToken.None));
        await AcknowledgeAllBatchesAsync(runId);
        var claim = Assert.IsType<EtlRunClaimOutcome.Claimed>(await _store.TryClaimRunCompletionAsync(runId, "owner-1", DateTimeOffset.UtcNow, 8, CancellationToken.None));

        await ExecuteSqlAsync(corruption == "NULL"
            ? "UPDATE etl_runs SET complete_payload_json=NULL WHERE run_id=$run;"
            : "UPDATE etl_runs SET complete_payload_json='{}' WHERE run_id=$run;", ("$run", runId.ToString("D")));

        var outcome = await _store.FinalizeEtlRunAsync(runId, claim.Claim.ClaimId, CancellationToken.None);

        var blocked = Assert.IsType<EtlRunFinalizeOutcome.Blocked>(outcome);
        Assert.Equal("SEAL_VIOLATED", blocked.Code);
        Assert.Equal("blocked", await RunStatusAsync(runId));
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM watermarks"));
    }

    [Fact]
    public async Task Reclaim_with_a_different_attempt_bound_is_refused_without_writes()
    {
        var runId = await NewRunAsync("clients");
        await BeginExtractCompleteAsync(runId, "clients", CursorJson(FinalClients), batchRows: 5);
        Assert.IsType<EtlRunSealOutcome.Sealed>(await _store.SealEtlRunExtractionAsync(runId, CancellationToken.None));
        await AcknowledgeAllBatchesAsync(runId);
        var first = Assert.IsType<EtlRunClaimOutcome.Claimed>(await _store.TryClaimRunCompletionAsync(runId, "owner-1", DateTimeOffset.UtcNow, 8, CancellationToken.None));
        Assert.IsType<EtlRunCompletionRetryOutcome.Scheduled>(
            await _store.MarkRunCompletionRetryAsync(runId, first.Claim.ClaimId, "timeout", DateTimeOffset.UtcNow.AddSeconds(-1), 8, CancellationToken.None));

        // A later claim presenting a different policy limit is refused with zero writes —
        // no phantom attempt, the durable bound and original policy are preserved.
        Assert.IsType<EtlRunClaimOutcome.NotClaimed>(await _store.TryClaimRunCompletionAsync(runId, "owner-2", DateTimeOffset.UtcNow, 3, CancellationToken.None));
        var run = await RunRowAsync(runId);
        Assert.Equal("completing", run.Status);
        Assert.Null(run.CompletionClaimId);
        Assert.Equal(1, run.CompletionAttemptCount);
        Assert.Equal(8, await ScalarAsync($"SELECT completion_max_attempts FROM etl_runs WHERE run_id='{runId:D}'"));

        var reclaim = Assert.IsType<EtlRunClaimOutcome.Claimed>(await _store.TryClaimRunCompletionAsync(runId, "owner-3", DateTimeOffset.UtcNow, 8, CancellationToken.None));
        Assert.Equal(first.Claim.CompletePayloadJson, reclaim.Claim.CompletePayloadJson);
        Assert.Equal(2, reclaim.Claim.Attempt);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-3)]
    public async Task Reclaim_after_recovery_blocks_on_nonpositive_expected_base_generation(long corruptGeneration)
    {
        var runId = await NewRunAsync("clients");
        await SeedWatermarkAsync("clients", CursorJson(CursorX), generation: 4, fingerprint: Fingerprint("clients"));
        await BeginExtractCompleteAsync(runId, "clients", CursorJson(FinalClients), batchRows: 5);
        Assert.IsType<EtlRunSealOutcome.Sealed>(await _store.SealEtlRunExtractionAsync(runId, CancellationToken.None));
        await AcknowledgeAllBatchesAsync(runId);
        Assert.IsType<EtlRunClaimOutcome.Claimed>(await _store.TryClaimRunCompletionAsync(runId, "owner-1", DateTimeOffset.UtcNow, 8, CancellationToken.None));
        await _store.RecoverInterruptedEtlRunsAsync(CancellationToken.None);

        // A nonpositive expected generation is corrupt evidence — a real watermarks
        // generation is always >= 1.
        await ExecuteSqlAsync("UPDATE etl_run_entities SET expected_base_generation=$gen WHERE run_id=$run;", ("$gen", corruptGeneration), ("$run", runId.ToString("D")));

        var outcome = await _store.TryClaimRunCompletionAsync(runId, "owner-2", DateTimeOffset.UtcNow, 8, CancellationToken.None);

        Assert.IsType<EtlRunClaimOutcome.Blocked>(outcome);
        Assert.Equal("blocked", await RunStatusAsync(runId));
    }

    // ---------- helpers ----------

    private static string Now() => DateTimeOffset.UtcNow.ToString("O");
    private static string CursorJson(EtlCursor cursor) => JsonSerializer.Serialize(cursor, JsonOptions);
    private static string Fingerprint(string entity, string sourceNamespace = SourceNamespace, string queryMode = QueryMode) =>
        EtlDomainFingerprint.Compute(sourceNamespace, entity, DefinitionJson(entity), queryMode);

    private static string DefinitionJson(string entity) =>
        JsonSerializer.Serialize(MakeEntities().Single(e => e.EntityCode == entity), JsonOptions);

    private static EtlEntityDefinition[] MakeEntities() =>
    [
        new("clients", "Catalog_Clients", "Ref_Key", "UpdatedAt", "DeletionMark", ["Ref_Key", "UpdatedAt", "DeletionMark"], "incremental", 500, 10),
        new("orders", "Document_Orders", "Ref_Key", "Date", "Posted", ["Ref_Key", "Date", "Posted"], "incremental", 500, 10)
    ];

    private static EtlEntityExtractionRequest Request(string entity, string? definitionJson = null, string sourceNamespace = SourceNamespace, string queryMode = QueryMode) =>
        new(entity, definitionJson ?? DefinitionJson(entity), sourceNamespace, queryMode,
            JsonSerializer.Serialize(new EtlCursor(DateTimeOffset.UtcNow, null), JsonOptions));

    private static EtlBatch MakeBatch(Guid runId, string entity, int rowCount, EtlCursor? watermarkTo = null, DateTimeOffset? createdAtUtc = null) =>
        new(Guid.NewGuid(), runId, entity, 1, $"spool/{runId:N}-{entity}-{Guid.NewGuid():N}.gz", EtlBatchStatus.Ready, rowCount,
            null, watermarkTo ?? FinalClients, "hash-" + Guid.NewGuid().ToString("N"), 1000, 4000, 0, createdAtUtc ?? DateTimeOffset.UtcNow);

    private async Task<Guid> NewRunAsync(params string[] entities)
    {
        var runId = Guid.NewGuid();
        await _store.CreateEtlRunAsync(new EtlRun(runId, "incremental", entities, EtlRunStatus.Running), CancellationToken.None);
        return runId;
    }

    private async Task<Guid> NewPendingRunAsync()
    {
        var runId = Guid.NewGuid();
        await ExecuteSqlAsync(
            "INSERT INTO etl_runs(run_id,mode,requested_entities_json,status,configuration_version,created_at_utc,updated_at_utc,row_version) VALUES($run,'bootstrap_full','[\"clients\"]','pending',1,$now,$now,1);",
            ("$run", runId.ToString("D")), ("$now", Now()));
        return runId;
    }

    private async Task InsertRunAsync(Guid runId, string requestedEntitiesJson)
    {
        await ExecuteSqlAsync(
            "INSERT INTO etl_runs(run_id,mode,requested_entities_json,status,started_at_utc,created_at_utc,updated_at_utc,row_version) VALUES($run,'incremental',$entities,'running',$now,$now,$now,1);",
            ("$run", runId.ToString("D")), ("$entities", requestedEntitiesJson), ("$now", Now()));
    }

    // Accepts a real durable job via the v5 API and returns its pending run + frozen entities.
    private async Task<(Guid RunId, EtlEntityDefinition[] Entities)> AcceptJobRunAsync(string mode)
    {
        var entities = mode == "entity_reload" ? [MakeEntities()[0]] : MakeEntities();
        var payloadJson = mode == "entity_reload" ? "{\"entity\":\"clients\"}" : "{\"entities\":[\"clients\",\"orders\"]}";
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

    private async Task BeginExtractCompleteAsync(Guid runId, string entity, string finalJson, int batchRows, string? definitionJson = null, string queryMode = QueryMode)
    {
        Assert.IsType<EtlEntityBeginOutcome.Begun>(await _store.BeginEtlEntityExtractionAsync(runId, Request(entity, definitionJson, queryMode: queryMode), CancellationToken.None));
        Assert.IsType<EtlBatchRegistrationOutcome.Registered>(await _store.RegisterGuardedEtlBatchAsync(MakeBatch(runId, entity, batchRows), CancellationToken.None));
        Assert.IsType<EtlEntityCompletionOutcome.Completed>(await _store.CompleteEtlEntityExtractionAsync(runId, entity, finalJson, 1, CancellationToken.None));
    }

    private async Task SeedWatermarkAsync(string entity, string? cursorJson, long generation, string? fingerprint)
    {
        await ExecuteSqlAsync(
            "INSERT INTO watermarks(entity_name,committed_cursor_json,extracting_cursor_json,last_run_id,generation,domain_fingerprint,updated_at_utc) VALUES($entity,$cursor,NULL,$run,$gen,$fp,$now);",
            ("$entity", entity), ("$cursor", cursorJson), ("$run", Guid.NewGuid().ToString("D")), ("$gen", generation), ("$fp", fingerprint), ("$now", Now()));
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

    private sealed record EntitySnapshot(string Status, long BaseRowPresent, long? ExpectedBaseGeneration, string? ExpectedBaseCursorJson, string? ExpectedBaseDomainFingerprint, string DomainStatus, string DomainFingerprint, string EntityDefinitionJson, string? WatermarkFromJson, string? FinalWatermarkJson, long? ExpectedBatchCount, long RowsRead, long BatchesCreated, string? LastError);
    private sealed record RunSnapshot(string Status, string? SealedAtUtc, long? SealedEntityCount, long? SealedExpectedBatchCount, string? CompletionClaimId, string? CompletionClaimOwner, long CompletionAttemptCount, string? CompletePayloadJson, string? CompletionAcknowledgedAtUtc, string? FinalizeConflictCode, string? FinishedAtUtc, long RowVersion);
    private sealed record WatermarkSnapshot(string? CursorJson, long Generation, string? Fingerprint, string? LastRunId);

    private async Task<EntitySnapshot?> EntityRowAsync(Guid runId, string entity)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT status,base_row_present,expected_base_generation,expected_base_cursor_json,expected_base_domain_fingerprint,domain_status,domain_fingerprint,entity_definition_json,watermark_from_json,final_watermark_json,expected_batch_count,rows_read,batches_created,last_error FROM etl_run_entities WHERE run_id=$run AND entity_name=$entity;";
        command.Parameters.AddWithValue("$run", runId.ToString("D"));
        command.Parameters.AddWithValue("$entity", entity);
        await using var reader = await command.ExecuteReaderAsync(CancellationToken.None);
        if (!await reader.ReadAsync(CancellationToken.None)) return null;
        return new(reader.GetString(0), reader.GetInt64(1), NullableLong(reader, 2), Nullable(reader, 3), Nullable(reader, 4), reader.GetString(5), reader.GetString(6), reader.GetString(7), Nullable(reader, 8), Nullable(reader, 9), NullableLong(reader, 10), reader.GetInt64(11), reader.GetInt64(12), Nullable(reader, 13));
    }

    private async Task<RunSnapshot> RunRowAsync(Guid runId)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT status,sealed_at_utc,sealed_entity_count,sealed_expected_batch_count,completion_claim_id,completion_claim_owner_id,completion_attempt_count,complete_payload_json,completion_acknowledged_at_utc,finalize_conflict_code,finished_at_utc,row_version FROM etl_runs WHERE run_id=$run;";
        command.Parameters.AddWithValue("$run", runId.ToString("D"));
        await using var reader = await command.ExecuteReaderAsync(CancellationToken.None);
        Assert.True(await reader.ReadAsync(CancellationToken.None));
        return new(reader.GetString(0), Nullable(reader, 1), NullableLong(reader, 2), NullableLong(reader, 3), Nullable(reader, 4), Nullable(reader, 5), reader.GetInt64(6), Nullable(reader, 7), Nullable(reader, 8), Nullable(reader, 9), Nullable(reader, 10), reader.GetInt64(11));
    }

    private async Task<string?> RunStatusAsync(Guid runId) => await ScalarStringAsync($"SELECT status FROM etl_runs WHERE run_id='{runId:D}'");

    private async Task<WatermarkSnapshot> WatermarkAsync(string entity)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT committed_cursor_json,generation,domain_fingerprint,last_run_id FROM watermarks WHERE entity_name=$entity;";
        command.Parameters.AddWithValue("$entity", entity);
        await using var reader = await command.ExecuteReaderAsync(CancellationToken.None);
        Assert.True(await reader.ReadAsync(CancellationToken.None));
        return new(Nullable(reader, 0), reader.GetInt64(1), Nullable(reader, 2), Nullable(reader, 3));
    }

    private static string? Nullable(SqliteDataReader reader, int ordinal) => reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);
    private static long? NullableLong(SqliteDataReader reader, int ordinal) => reader.IsDBNull(ordinal) ? null : reader.GetInt64(ordinal);

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
