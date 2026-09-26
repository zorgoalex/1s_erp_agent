using System.Globalization;
using System.Text.Json;
using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Domain.Etl;
using ErpOnecAgent.Infrastructure.Persistence.Sqlite;
using Microsoft.Data.Sqlite;
using Xunit;

namespace ErpOnecAgent.IntegrationTests;

/// <summary>
/// D1 DARK domain-transition suite (schema v11, migration 011): the watermark cursor
/// CLASS fingerprint (bootstrap_full / entity_reload / incremental share
/// "watermark-cursor/v1"), the BaselineRequired refusal of an incremental read with no
/// committed watermark, and the attested domain reset — the explicit exit from
/// DOMAIN_CHANGED / DOMAIN_UNKNOWN — under a generation CAS while no active run owns
/// the entity: ONE transaction archives the row verbatim into watermark_domain_resets
/// and deletes it, refusing with zero writes when the row is missing, the generation
/// moved, or ownership is live. All against a real migrated SQLite database. DARK:
/// nothing here wires workers, the ERP client, or production dispatch.
/// </summary>
public sealed class EtlDomainTransitionD1Tests : IAsyncLifetime
{
    private const string SourceNamespace = "onec-infobase-a";
    private const string OtherNamespace = "onec-infobase-b";
    private const string QueryMode = "bootstrap_full";
    private const string ScheduledMode = "incremental";
    private const string RemoteVerification = "ERP staging reconciled: no rows landed for the unresolved work";
    private static readonly TimeSpan GateTimeout = TimeSpan.FromSeconds(15);
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    private static readonly EtlCursor CursorX = new(DateTimeOffset.Parse("2026-09-11T05:59:00.0000000+00:00", CultureInfo.InvariantCulture), "A7");
    private static readonly EtlCursor FinalClients = new(DateTimeOffset.Parse("2026-09-19T09:59:00.0000000+00:00", CultureInfo.InvariantCulture), "A10");

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

    // ---------- F1: the fingerprint binds the cursor class, not the read mode ----------

    [Fact]
    public void Fingerprint_is_identical_across_the_supported_watermark_cursor_modes()
    {
        var definition = DefinitionJson("clients");
        var full = EtlDomainFingerprint.Compute(SourceNamespace, "clients", definition, "bootstrap_full");
        var reload = EtlDomainFingerprint.Compute(SourceNamespace, "clients", definition, "entity_reload");
        var incremental = EtlDomainFingerprint.Compute(SourceNamespace, "clients", definition, "incremental");

        Assert.Equal(full, reload);
        Assert.Equal(full, incremental);
    }

    [Fact]
    public void Fingerprint_differs_for_another_namespace_entity_or_a_one_field_definition_change()
    {
        var definition = DefinitionJson("clients");
        var baseline = EtlDomainFingerprint.Compute(SourceNamespace, "clients", definition, "incremental");

        // A source change (namespace — including an exportEpoch after a restore) is a
        // different domain, whatever the read mode.
        Assert.NotEqual(baseline, EtlDomainFingerprint.Compute(OtherNamespace, "clients", definition, "bootstrap_full"));
        // A different entity is a different domain.
        Assert.NotEqual(baseline, EtlDomainFingerprint.Compute(SourceNamespace, "orders", DefinitionJson("orders"), "incremental"));
        // Any definition change that could alter cursor semantics is a different domain.
        var drifted = MakeEntities().Single(e => e.EntityCode == "clients") with { PageSize = 1000 };
        Assert.NotEqual(baseline, EtlDomainFingerprint.Compute(SourceNamespace, "clients", JsonSerializer.Serialize(drifted, JsonOptions), "incremental"));
    }

    [Fact]
    public void Fingerprint_of_an_unsupported_mode_stays_distinct_from_the_watermark_class()
    {
        var definition = DefinitionJson("clients");
        var supported = EtlDomainFingerprint.Compute(SourceNamespace, "clients", definition, "incremental");

        var unsupported = EtlDomainFingerprint.Compute(SourceNamespace, "clients", definition, "reconcile_keys");

        Assert.NotEqual(supported, unsupported);
        Assert.Equal("watermark-cursor/v1", EtlDomainFingerprint.CursorClass("bootstrap_full"));
        Assert.Equal("watermark-cursor/v1", EtlDomainFingerprint.CursorClass("entity_reload"));
        Assert.Equal("watermark-cursor/v1", EtlDomainFingerprint.CursorClass("incremental"));
        Assert.Equal("unsupported:reconcile_keys", EtlDomainFingerprint.CursorClass("reconcile_keys"));
    }

    // ---------- E1: epoch change — DomainChanged, resolve, reset, rebaseline ----------

    [Fact]
    public async Task Epoch_change_blocks_incremental_and_the_attested_reset_opens_a_new_baseline()
    {
        // The committed watermark was written under namespace A.
        var fingerprintA = Fingerprint("clients", SourceNamespace);
        var seedRunId = Guid.NewGuid().ToString("D");
        var seededAt = "2026-09-10T06:10:00.0000000+00:00";
        await SeedWatermarkAsync("clients", CursorJson(CursorX), generation: 4, fingerprint: fingerprintA, lastRunId: seedRunId, updatedAtUtc: seededAt);

        // A run under namespace B reads the same entity: the stored fingerprint no
        // longer matches — DomainChanged, the entity row is failed, the row is kept.
        var runA = await NewRunAsync("clients");
        var outcome = await _store.BeginEtlEntityExtractionAsync(runA, ClaimOf(runA), Request("clients", sourceNamespace: OtherNamespace), CancellationToken.None);
        Assert.Equal(EtlEntityBeginRejection.DomainChanged, Assert.IsType<EtlEntityBeginOutcome.Rejected>(outcome).Reason);
        Assert.Equal("failed", await ScalarStringAsync($"SELECT status FROM etl_run_entities WHERE run_id='{runA:D}' AND entity_name='clients'"));
        Assert.Equal("changed", await ScalarStringAsync($"SELECT domain_status FROM etl_run_entities WHERE run_id='{runA:D}' AND entity_name='clients'"));
        Assert.Equal("DOMAIN_CHANGED", await ScalarStringAsync($"SELECT last_error FROM etl_run_entities WHERE run_id='{runA:D}' AND entity_name='clients'"));
        Assert.Equal(CursorJson(CursorX), await ScalarStringAsync("SELECT committed_cursor_json FROM watermarks WHERE entity_name='clients'"));
        Assert.Equal(4, await ScalarAsync("SELECT generation FROM watermarks WHERE entity_name='clients'"));

        // The stale run is failed and resolved (R1): its lifetime ownership is released.
        Assert.IsType<EtlRunTerminationOutcome.Applied>(
            await _store.FailEtlRunAsync(runA, ClaimOf(runA), "domain changed — operator rebaseline", CancellationToken.None));
        Assert.IsType<EtlRunResolutionOutcome.Resolved>(
            await _store.ResolveEtlRunAsync(Resolution(runA), DateTimeOffset.UtcNow, CancellationToken.None));
        Assert.Equal("manual_release", await ScalarStringAsync("SELECT release_reason FROM etl_entity_ownership WHERE entity_name='clients'"));

        // The attested reset archives the row verbatim and removes it in one commit.
        var resetAt = DateTimeOffset.UtcNow;
        await ExecuteSqlAsync("UPDATE watermarks SET extracting_cursor_json='{\"legacy\":true}' WHERE entity_name='clients';");
        var reset = await _store.ResetEtlWatermarkDomainAsync(
            new EtlWatermarkDomainResetRequest("clients", 4, "operator-1", "source epoch changed after backup restore"), resetAt, CancellationToken.None);

        var record = Assert.IsType<EtlWatermarkDomainResetOutcome.Reset>(reset).Record;
        Assert.Equal("clients", record.EntityName);
        Assert.Equal(4, record.PriorGeneration);
        Assert.Equal(CursorJson(CursorX), record.PriorCursorJson);
        Assert.Equal(fingerprintA, record.PriorDomainFingerprint);
        Assert.Equal(resetAt, record.ResetAtUtc);
        Assert.Equal(1, await ScalarAsync("SELECT COUNT(*) FROM watermark_domain_resets"));
        Assert.Equal(record.ResetId.ToString("D"), await ScalarStringAsync("SELECT reset_id FROM watermark_domain_resets WHERE entity_name='clients'"));
        Assert.Equal("operator-1", await ScalarStringAsync("SELECT operator_id FROM watermark_domain_resets WHERE entity_name='clients'"));
        Assert.Equal("source epoch changed after backup restore", await ScalarStringAsync("SELECT reason FROM watermark_domain_resets WHERE entity_name='clients'"));
        // The archive is the removed row verbatim.
        Assert.Equal(CursorJson(CursorX), await ScalarStringAsync("SELECT prior_committed_cursor_json FROM watermark_domain_resets WHERE entity_name='clients'"));
        Assert.Equal(4, await ScalarAsync("SELECT prior_generation FROM watermark_domain_resets WHERE entity_name='clients'"));
        Assert.Equal(fingerprintA, await ScalarStringAsync("SELECT prior_domain_fingerprint FROM watermark_domain_resets WHERE entity_name='clients'"));
        Assert.Equal(seedRunId, await ScalarStringAsync("SELECT prior_last_run_id FROM watermark_domain_resets WHERE entity_name='clients'"));
        Assert.Equal(seededAt, await ScalarStringAsync("SELECT prior_updated_at_utc FROM watermark_domain_resets WHERE entity_name='clients'"));
        Assert.Equal("{\"legacy\":true}", await ScalarStringAsync("SELECT prior_extracting_cursor_json FROM watermark_domain_resets WHERE entity_name='clients'"));
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM watermarks WHERE entity_name='clients'"));

        // An incremental read must never establish the new domain: BaselineRequired.
        var (incrementalRun, incrementalClaim) = await ClaimedScheduledRunAsync("nightly", ["clients"]);
        var incremental = await _store.BeginEtlEntityExtractionAsync(incrementalRun, incrementalClaim, IncrementalRequest("clients"), CancellationToken.None);
        Assert.Equal(EtlEntityBeginRejection.BaselineRequired, Assert.IsType<EtlEntityBeginOutcome.Rejected>(incremental).Reason);
        // Partial runs: the refusal is recorded as a failed entity (skippable), never a watermark.
        Assert.Equal("BASELINE_REQUIRED", await ScalarStringAsync($"SELECT failure_code FROM etl_run_entities WHERE run_id='{incrementalRun:D}' AND entity_name='clients' AND status='failed'"));
        Assert.IsType<EtlRunTerminationOutcome.Applied>(
            await _store.FailEtlRunAsync(incrementalRun, incrementalClaim, "baseline required", CancellationToken.None));
        Assert.IsType<EtlRunResolutionOutcome.Resolved>(
            await _store.ResolveEtlRunAsync(Resolution(incrementalRun), DateTimeOffset.UtcNow, CancellationToken.None));

        // The new baseline under B: bootstrap_full begins 'absent' and the finalize
        // INSERT path establishes the domain at generation 1 with B's fingerprint.
        var baselineRun = await NewRunAsync("clients");
        var begun = Assert.IsType<EtlEntityBeginOutcome.Begun>(
            await _store.BeginEtlEntityExtractionAsync(baselineRun, ClaimOf(baselineRun), Request("clients", sourceNamespace: OtherNamespace), CancellationToken.None));
        Assert.False(begun.Base.BaseRowPresent);
        Assert.Equal("absent", begun.Base.DomainStatus);
        var batch = MakeBatch(baselineRun, "clients", 5);
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

        var fingerprintB = Fingerprint("clients", OtherNamespace);
        Assert.Equal(1, await ScalarAsync("SELECT generation FROM watermarks WHERE entity_name='clients'"));
        Assert.Equal(fingerprintB, await ScalarStringAsync("SELECT domain_fingerprint FROM watermarks WHERE entity_name='clients'"));
        Assert.Equal(baselineRun.ToString("D"), await ScalarStringAsync("SELECT last_run_id FROM watermarks WHERE entity_name='clients'"));
        Assert.Equal(CursorJson(FinalClients), await ScalarStringAsync("SELECT committed_cursor_json FROM watermarks WHERE entity_name='clients'"));
        // B's fingerprint is the same cursor class as A's — the namespace differs it.
        Assert.NotEqual(fingerprintA, fingerprintB);
        Assert.Equal(fingerprintB, EtlDomainFingerprint.Compute(OtherNamespace, "clients", DefinitionJson("clients"), "incremental"));
    }

    // ---------- L1: a legacy NULL-fingerprint row ----------

    [Fact]
    public async Task Legacy_null_fingerprint_row_blocks_begin_and_resets_with_a_null_prior_domain()
    {
        await SeedWatermarkAsync("clients", CursorJson(CursorX), generation: 3, fingerprint: null);

        var runId = await NewRunAsync("clients");
        var outcome = await _store.BeginEtlEntityExtractionAsync(runId, ClaimOf(runId), Request("clients"), CancellationToken.None);

        Assert.Equal(EtlEntityBeginRejection.DomainUnknown, Assert.IsType<EtlEntityBeginOutcome.Rejected>(outcome).Reason);
        Assert.Equal("failed", await ScalarStringAsync($"SELECT status FROM etl_run_entities WHERE run_id='{runId:D}' AND entity_name='clients'"));
        Assert.Equal("unknown", await ScalarStringAsync($"SELECT domain_status FROM etl_run_entities WHERE run_id='{runId:D}' AND entity_name='clients'"));
        Assert.Equal("DOMAIN_UNKNOWN", await ScalarStringAsync($"SELECT last_error FROM etl_run_entities WHERE run_id='{runId:D}' AND entity_name='clients'"));

        // Release the stale run's ownership, then reset the unknown domain.
        Assert.IsType<EtlRunTerminationOutcome.Applied>(
            await _store.FailEtlRunAsync(runId, ClaimOf(runId), "unknown domain — operator rebaseline", CancellationToken.None));
        Assert.IsType<EtlRunResolutionOutcome.Resolved>(
            await _store.ResolveEtlRunAsync(Resolution(runId), DateTimeOffset.UtcNow, CancellationToken.None));

        var reset = await _store.ResetEtlWatermarkDomainAsync(
            new EtlWatermarkDomainResetRequest("clients", 3, "operator-1", "legacy watermark without a domain fingerprint"), DateTimeOffset.UtcNow, CancellationToken.None);

        var record = Assert.IsType<EtlWatermarkDomainResetOutcome.Reset>(reset).Record;
        Assert.Equal(3, record.PriorGeneration);
        Assert.Equal(CursorJson(CursorX), record.PriorCursorJson);
        Assert.Null(record.PriorDomainFingerprint);
        Assert.Null(await ScalarStringAsync("SELECT prior_domain_fingerprint FROM watermark_domain_resets WHERE entity_name='clients'"));
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM watermarks WHERE entity_name='clients'"));
    }

    // ---------- R1: refusals and invalid requests write nothing ----------

    [Fact]
    public async Task Reset_refusals_and_invalid_requests_write_nothing()
    {
        await SeedWatermarkAsync("clients", CursorJson(CursorX), generation: 7, fingerprint: Fingerprint("clients"));
        await SeedWatermarkAsync("orders", CursorJson(CursorX), generation: 3, fingerprint: Fingerprint("orders"));
        // 'orders' is owned by an active claimed run.
        var owner = await NewRunAsync("orders");
        var watermarksBefore = await SnapshotRowsAsync("SELECT * FROM watermarks ORDER BY entity_name;");
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM watermark_domain_resets"));

        // Missing watermark row -> WatermarkMissing.
        var missing = await _store.ResetEtlWatermarkDomainAsync(
            new EtlWatermarkDomainResetRequest("never_seen", 1, "operator-1", "epoch change"), DateTimeOffset.UtcNow, CancellationToken.None);
        Assert.Equal(EtlWatermarkDomainResetRefusal.WatermarkMissing, Assert.IsType<EtlWatermarkDomainResetOutcome.Refused>(missing).Reason);

        // The observed generation moved -> GenerationMismatch.
        var mismatch = await _store.ResetEtlWatermarkDomainAsync(
            new EtlWatermarkDomainResetRequest("clients", 2, "operator-1", "epoch change"), DateTimeOffset.UtcNow, CancellationToken.None);
        Assert.Equal(EtlWatermarkDomainResetRefusal.GenerationMismatch, Assert.IsType<EtlWatermarkDomainResetOutcome.Refused>(mismatch).Reason);

        // An active run owns the entity -> EntityOwned.
        var owned = await _store.ResetEtlWatermarkDomainAsync(
            new EtlWatermarkDomainResetRequest("orders", 3, "operator-1", "epoch change"), DateTimeOffset.UtcNow, CancellationToken.None);
        Assert.Equal(EtlWatermarkDomainResetRefusal.EntityOwned, Assert.IsType<EtlWatermarkDomainResetOutcome.Refused>(owned).Reason);

        // Invalid requests throw — blank entity/operator/reason, generation below 1.
        await Assert.ThrowsAnyAsync<ArgumentException>(() => _store.ResetEtlWatermarkDomainAsync(
            new EtlWatermarkDomainResetRequest("  ", 1, "operator-1", "epoch change"), DateTimeOffset.UtcNow, CancellationToken.None));
        await Assert.ThrowsAnyAsync<ArgumentException>(() => _store.ResetEtlWatermarkDomainAsync(
            new EtlWatermarkDomainResetRequest("clients", 7, "", "epoch change"), DateTimeOffset.UtcNow, CancellationToken.None));
        await Assert.ThrowsAnyAsync<ArgumentException>(() => _store.ResetEtlWatermarkDomainAsync(
            new EtlWatermarkDomainResetRequest("clients", 7, "operator-1", "   "), DateTimeOffset.UtcNow, CancellationToken.None));
        await Assert.ThrowsAnyAsync<ArgumentException>(() => _store.ResetEtlWatermarkDomainAsync(
            new EtlWatermarkDomainResetRequest("clients", 0, "operator-1", "epoch change"), DateTimeOffset.UtcNow, CancellationToken.None));
        await Assert.ThrowsAnyAsync<ArgumentException>(() => _store.ResetEtlWatermarkDomainAsync(
            null!, DateTimeOffset.UtcNow, CancellationToken.None));

        // Zero writes through every refusal and rejection.
        Assert.Equal(watermarksBefore, await SnapshotRowsAsync("SELECT * FROM watermarks ORDER BY entity_name;"));
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM watermark_domain_resets"));
        Assert.Equal(owner.ToString("D"), await ScalarStringAsync("SELECT owner_run_id FROM etl_entity_ownership WHERE entity_name='orders' AND released_at_utc IS NULL"));
    }

    // ---------- R2: a stale owner fences the reset until resolution ----------

    [Fact]
    public async Task Reset_is_refused_while_a_stale_run_owns_the_entity_and_the_resolved_run_can_never_finalize()
    {
        // A stale 'running' run still owns 'clients' — its extraction captured nothing.
        var staleRun = await NewRunAsync("clients");
        await SeedWatermarkAsync("clients", CursorJson(CursorX), generation: 5, fingerprint: Fingerprint("clients"));

        var refused = await _store.ResetEtlWatermarkDomainAsync(
            new EtlWatermarkDomainResetRequest("clients", 5, "operator-1", "epoch change"), DateTimeOffset.UtcNow, CancellationToken.None);
        Assert.Equal(EtlWatermarkDomainResetRefusal.EntityOwned, Assert.IsType<EtlWatermarkDomainResetOutcome.Refused>(refused).Reason);
        Assert.Equal(1, await ScalarAsync("SELECT COUNT(*) FROM watermarks WHERE entity_name='clients'"));
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM watermark_domain_resets"));

        // Resolution releases the ownership; the reset then succeeds.
        Assert.IsType<EtlRunTerminationOutcome.Applied>(
            await _store.FailEtlRunAsync(staleRun, ClaimOf(staleRun), "superseded by operator reset", CancellationToken.None));
        Assert.IsType<EtlRunResolutionOutcome.Resolved>(
            await _store.ResolveEtlRunAsync(Resolution(staleRun), DateTimeOffset.UtcNow, CancellationToken.None));
        Assert.NotNull(await ScalarStringAsync("SELECT released_at_utc FROM etl_entity_ownership WHERE entity_name='clients'"));

        var reset = await _store.ResetEtlWatermarkDomainAsync(
            new EtlWatermarkDomainResetRequest("clients", 5, "operator-1", "epoch change"), DateTimeOffset.UtcNow, CancellationToken.None);
        Assert.IsType<EtlWatermarkDomainResetOutcome.Reset>(reset);
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM watermarks WHERE entity_name='clients'"));
        Assert.Equal(1, await ScalarAsync("SELECT COUNT(*) FROM watermark_domain_resets"));

        // The resolved run can never finalize afterwards — a failed run is not claimable.
        var claim = await _store.TryClaimRunCompletionAsync(staleRun, "finalizer", DateTimeOffset.UtcNow, 8, CancellationToken.None);
        Assert.IsType<EtlRunClaimOutcome.NotClaimed>(claim);
        Assert.Equal("failed", await RunStatusAsync(staleRun));
    }

    // ---------- C1: concurrent resets commit exactly one archive ----------

    [Fact]
    public async Task Concurrent_resets_of_the_same_generation_commit_exactly_one_reset()
    {
        await SeedWatermarkAsync("clients", CursorJson(CursorX), generation: 7, fingerprint: Fingerprint("clients"));
        var other = new SqliteAgentStore(_factory, new SqliteMigrator(_factory));
        var barrier = new Barrier(2);
        var now = DateTimeOffset.UtcNow;

        var first = Task.Run(async () =>
        {
            barrier.SignalAndWait();
            return await _store.ResetEtlWatermarkDomainAsync(
                new EtlWatermarkDomainResetRequest("clients", 7, "operator-a", "epoch change"), now, CancellationToken.None);
        });
        var second = Task.Run(async () =>
        {
            barrier.SignalAndWait();
            return await other.ResetEtlWatermarkDomainAsync(
                new EtlWatermarkDomainResetRequest("clients", 7, "operator-b", "epoch change"), now, CancellationToken.None);
        });
        var results = await Task.WhenAll(first, second).WaitAsync(GateTimeout);

        var winner = Assert.Single(results.OfType<EtlWatermarkDomainResetOutcome.Reset>());
        var loser = Assert.IsType<EtlWatermarkDomainResetOutcome.Refused>(
            Assert.Single(results, r => r is not EtlWatermarkDomainResetOutcome.Reset));
        Assert.True(loser.Reason is EtlWatermarkDomainResetRefusal.GenerationMismatch or EtlWatermarkDomainResetRefusal.WatermarkMissing,
            $"Expected GenerationMismatch or WatermarkMissing, got {loser.Reason}.");
        Assert.Equal(7, winner.Record.PriorGeneration);
        // Exactly one archive row; the watermark row is gone.
        Assert.Equal(1, await ScalarAsync("SELECT COUNT(*) FROM watermark_domain_resets"));
        Assert.Equal(winner.Record.ResetId.ToString("D"), await ScalarStringAsync("SELECT reset_id FROM watermark_domain_resets"));
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM watermarks WHERE entity_name='clients'"));
    }

    // ---------- helpers ----------

    private static string Now() => DateTimeOffset.UtcNow.ToString("O");
    private static string CursorJson(EtlCursor cursor) => JsonSerializer.Serialize(cursor, JsonOptions);
    private static string Fingerprint(string entity, string sourceNamespace = SourceNamespace, string queryMode = QueryMode) =>
        EtlDomainFingerprint.Compute(sourceNamespace, entity, DefinitionJson(entity), queryMode);
    private static string DefinitionJson(string entity) =>
        JsonSerializer.Serialize(MakeEntities().Single(e => e.EntityCode == entity), JsonOptions);
    private static string DefinitionsJson(string[] entities) =>
        JsonSerializer.Serialize(entities.Select(e => MakeEntities().Single(x => x.EntityCode == e)).ToArray(), JsonOptions);
    private static string ManifestJson(string[] entities) => JsonSerializer.Serialize(entities, JsonOptions);
    private static EtlEntityDefinition[] MakeEntities() =>
    [
        new("clients", "Catalog_Clients", "Ref_Key", "UpdatedAt", "DeletionMark", ["Ref_Key", "UpdatedAt", "DeletionMark"], "incremental", 500, 10),
        new("orders", "Document_Orders", "Ref_Key", "Date", "Posted", ["Ref_Key", "Date", "Posted"], "incremental", 500, 10),
        new("payments", "Document_Payments", "Ref_Key", "Date", "Posted", ["Ref_Key", "Date", "Posted"], "incremental", 500, 10)
    ];
    private static EtlEntityExtractionRequest Request(string entity, string? definitionJson = null, string sourceNamespace = SourceNamespace, string queryMode = QueryMode) =>
        new(entity, definitionJson ?? DefinitionJson(entity), sourceNamespace, queryMode,
            JsonSerializer.Serialize(new EtlCursor(DateTimeOffset.UtcNow, null), JsonOptions));
    private static EtlEntityExtractionRequest IncrementalRequest(string entity, string sourceNamespace = SourceNamespace) =>
        Request(entity, sourceNamespace: sourceNamespace, queryMode: ScheduledMode);
    private static EtlScheduledRunRequest EnsureRequest(string scheduleKey, string[] entities) =>
        new(scheduleKey, ScheduledMode, entities.Select(e => MakeEntities().Single(x => x.EntityCode == e)).ToArray(), 7);
    private static EtlBatch MakeBatch(Guid runId, string entity, int rowCount) =>
        new(Guid.NewGuid(), runId, entity, 1, $"spool/{runId:N}-{entity}-{Guid.NewGuid():N}.gz", EtlBatchStatus.Ready, rowCount,
            null, FinalClients, "hash-" + Guid.NewGuid().ToString("N"), 1000, 4000, 0, DateTimeOffset.UtcNow);
    private static EtlRunResolutionRequest Resolution(Guid runId) =>
        new(runId, "operator-1", EtlRunResolutionDecision.Rebaseline, RemoteVerification, true);

    // A pending run + pending durable job with a consistent frozen identity (not claimed).
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

    // A claimed manual-job run holding ownership of its manifest entities.
    private async Task<Guid> NewRunAsync(params string[] entities)
    {
        var (runId, jobId) = await NewPendingJobRunAsync(QueryMode, entities);
        var claimed = Assert.IsType<EtlJobClaimOutcome.Claimed>(
            await _store.TryClaimEtlJobAsync(jobId, "test-dispatcher", DateTimeOffset.UtcNow.AddMinutes(5), DateTimeOffset.UtcNow, CancellationToken.None));
        _extractionClaims[runId] = claimed.Claim.ExtractionClaimId;
        return runId;
    }

    // Ensure + claim: one committed scheduled run extraction claim.
    private async Task<(Guid RunId, Guid ExtractionClaimId)> ClaimedScheduledRunAsync(string scheduleKey, string[] entities)
    {
        var ensured = Assert.IsType<EtlScheduledRunEnsureOutcome.Created>(
            await _store.EnsureScheduledEtlRunAsync(EnsureRequest(scheduleKey, entities), DateTimeOffset.UtcNow, CancellationToken.None));
        var claimed = Assert.IsType<EtlScheduledRunClaimOutcome.Claimed>(
            await _store.TryClaimScheduledRunAsync(ensured.RunId, "scheduler-1", DateTimeOffset.UtcNow, CancellationToken.None));
        return (ensured.RunId, claimed.Claim.ExtractionClaimId);
    }

    private async Task SeedWatermarkAsync(string entity, string? cursorJson, long generation, string? fingerprint, string? lastRunId = null, string? updatedAtUtc = null)
    {
        await ExecuteSqlAsync(
            "INSERT INTO watermarks(entity_name,committed_cursor_json,extracting_cursor_json,last_run_id,generation,domain_fingerprint,updated_at_utc) VALUES($entity,$cursor,NULL,$run,$gen,$fp,$now);",
            ("$entity", entity), ("$cursor", cursorJson), ("$run", lastRunId ?? Guid.NewGuid().ToString("D")),
            ("$gen", generation), ("$fp", fingerprint), ("$now", updatedAtUtc ?? Now()));
    }

    private async Task<string?> RunStatusAsync(Guid runId) => await ScalarStringAsync($"SELECT status FROM etl_runs WHERE run_id='{runId:D}'");

    private async Task<List<string>> SnapshotRowsAsync(string sql)
    {
        var rows = new List<string>();
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        await using var reader = await command.ExecuteReaderAsync(CancellationToken.None);
        while (await reader.ReadAsync(CancellationToken.None))
        {
            var values = new string[reader.FieldCount];
            for (var i = 0; i < reader.FieldCount; i++) values[i] = reader.IsDBNull(i) ? "<null>" : reader.GetValue(i).ToString() ?? "";
            rows.Add(string.Join("|", values));
        }
        return rows;
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
