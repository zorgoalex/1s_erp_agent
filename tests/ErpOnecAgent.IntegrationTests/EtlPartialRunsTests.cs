using System.Globalization;
using System.Net;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.Json;
using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Application.Configuration;
using ErpOnecAgent.Application.Etl;
using ErpOnecAgent.Contracts.Erp;
using ErpOnecAgent.Contracts.OneC;
using ErpOnecAgent.Domain.Agent;
using ErpOnecAgent.Domain.Commands;
using ErpOnecAgent.Domain.Common;
using ErpOnecAgent.Domain.Etl;
using ErpOnecAgent.Infrastructure.OneC;
using ErpOnecAgent.Infrastructure.Persistence.Sqlite;
using ErpOnecAgent.Infrastructure.Spool;
using ErpOnecAgent.Service.Diagnostics;
using ErpOnecAgent.Service.Runtime;
using ErpOnecAgent.Service.Workers;
using ErpOnecAgent.Service.Workers.Etl;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace ErpOnecAgent.IntegrationTests;

/// <summary>
/// Behaviour suite for partial ETL runs (migration 012, commit "Partial ETL runs"): an
/// entity that fails at its source is durably marked failed with a failure code and the
/// run continues, seals and completes with a "partial_success" completion payload. The real
/// EtlExtractionWorker/EtlUploadWorker/EtlCompletionWorker run over a migrated SQLite
/// store and a real FileSpoolStore in a temp directory; only OData, ERP, identity and the
/// disk probe are faked. Log assertions go through capturing ILogger&lt;T&gt; sinks.
/// </summary>
// Shares a collection with the other ETL worker suites: worker delay statics are process-wide.
[Collection("EtlWorkerStatics")]
public sealed class EtlPartialRunsTests : IAsyncLifetime
{
    private const string QueryMode = "bootstrap_full";
    private const string IncrementalMode = "incremental";
    private const string StoreSourceNamespace = "partial-runs-source";
    private const long PlentyOfDisk = 100L * 1024 * 1024 * 1024;
    private static readonly TimeSpan BoundedWait = TimeSpan.FromSeconds(15);
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);
    private static readonly EtlCursor FinalCursor = new(DateTimeOffset.Parse("2026-09-19T09:59:00.0000000+00:00", CultureInfo.InvariantCulture), "last-9");

    private readonly SqliteTestDatabase _database = new();
    private readonly List<Harness> _harnesses = [];
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
        foreach (var harness in _harnesses) harness.Dispose();
        await _database.DisposeAsync();
    }

    // ---------- S1: migration 012 ----------

    [Fact]
    public async Task S1_Migration_012_adds_failure_columns_enforces_the_check_and_records_the_checksum()
    {
        // The additive columns exist on a freshly migrated database.
        var entityColumns = await TableColumnsAsync("etl_run_entities");
        Assert.Contains("failure_code", entityColumns);
        Assert.Contains("failure_message", entityColumns);
        Assert.Contains("failed_at_utc", entityColumns);
        Assert.Contains("sealed_failed_entity_count", await TableColumnsAsync("etl_runs"));

        // failure_code CHECK: NULL and a non-blank code are legal; '' and '  ' are not.
        var runId = Guid.NewGuid();
        await ExecuteSqlAsync(
            "INSERT INTO etl_runs(run_id,mode,requested_entities_json,status,configuration_version,created_at_utc,updated_at_utc,row_version) VALUES($run,'bootstrap_full','[\"clients\"]','pending',7,$now,$now,1);",
            ("$run", runId.ToString("D")), ("$now", Now()));
        const string insert = "INSERT INTO etl_run_entities(run_id,entity_name,entity_definition_json,domain_fingerprint,status,base_row_present,domain_status,failure_code,created_at_utc,updated_at_utc,row_version) VALUES($run,$entity,'{}','fp','extracting',0,'absent',$code,$now,$now,1);";
        await Assert.ThrowsAnyAsync<SqliteException>(() => ExecuteSqlAsync(insert,
            ("$run", runId.ToString("D")), ("$entity", "clients"), ("$code", ""), ("$now", Now())));
        await Assert.ThrowsAnyAsync<SqliteException>(() => ExecuteSqlAsync(insert,
            ("$run", runId.ToString("D")), ("$entity", "clients"), ("$code", "  "), ("$now", Now())));
        await ExecuteSqlAsync(insert,
            ("$run", runId.ToString("D")), ("$entity", "clients"), ("$code", null), ("$now", Now()));
        await ExecuteSqlAsync(insert,
            ("$run", runId.ToString("D")), ("$entity", "orders"), ("$code", "ODATA_READ"), ("$now", Now()));
        Assert.Equal(2, await ScalarAsync($"SELECT COUNT(*) FROM etl_run_entities WHERE run_id='{runId:D}'"));

        // The checksum ledger row for migration 12.
        Assert.Equal("012_etl_partial_runs.sql", await ScalarStringAsync("SELECT name FROM schema_migrations WHERE version=12"));
        Assert.Equal("7A597CDA38537AB2AC178962B5BBA427F413499F1036678624D5A40FE83D4B99",
            await ScalarStringAsync("SELECT checksum FROM schema_migrations WHERE version=12"));
    }

    // ---------- S2: one of three entities fails, baseline run ----------

    [Fact]
    public async Task S2_One_failed_entity_still_completes_a_partial_baseline_run()
    {
        var h = NewHarness(["clients", "orders", "payments"]);
        h.OData.Rows["payments"] = [Row("P1"), Row("P2")];
        h.OData.Rows["clients"] = [Row("C1")];
        h.OData.Scripts["orders"] = _ => ThrowingRowsAsync(new HttpRequestException("server exploded", null, HttpStatusCode.InternalServerError));

        // The manifest order is intentionally unsorted: the payload must sort entities by name.
        var runId = await AcceptFullSyncAsync(h, "payments", "orders", "clients");
        var run = runId.ToString("D");

        Assert.True(await RunExtractionPassAsync(h));

        // Sealed with one failed entity; 'orders' carries the failure evidence.
        Assert.Equal("uploading", await RunStatusAsync(runId));
        Assert.NotNull(await ScalarStringAsync($"SELECT sealed_at_utc FROM etl_runs WHERE run_id='{run}'"));
        Assert.Equal(1, await ScalarAsync($"SELECT sealed_failed_entity_count FROM etl_runs WHERE run_id='{run}'"));
        Assert.Equal(3, await ScalarAsync($"SELECT sealed_entity_count FROM etl_runs WHERE run_id='{run}'"));
        Assert.Equal("failed", await ScalarStringAsync($"SELECT status FROM etl_run_entities WHERE run_id='{run}' AND entity_name='orders'"));
        Assert.Equal("ODATA_HTTP_500", await ScalarStringAsync($"SELECT failure_code FROM etl_run_entities WHERE run_id='{run}' AND entity_name='orders'"));
        Assert.Equal(0, await ScalarAsync($"SELECT expected_batch_count FROM etl_run_entities WHERE run_id='{run}' AND entity_name='orders'"));
        Assert.Null(await ScalarStringAsync($"SELECT final_watermark_json FROM etl_run_entities WHERE run_id='{run}' AND entity_name='orders'"));
        Assert.NotNull(await ScalarStringAsync($"SELECT failed_at_utc FROM etl_run_entities WHERE run_id='{run}' AND entity_name='orders'"));
        Assert.Equal("done", await ScalarStringAsync($"SELECT status FROM etl_run_entities WHERE run_id='{run}' AND entity_name='clients'"));
        Assert.Equal("done", await ScalarStringAsync($"SELECT status FROM etl_run_entities WHERE run_id='{run}' AND entity_name='payments'"));
        Assert.Contains(h.ExtractionLog.Entries, e => e.Level == LogLevel.Error
            && e.Message.Contains("ETL_ENTITY_FAILED") && e.Message.Contains("orders") && e.Message.Contains("ODATA_HTTP_500"));

        // Two healthy batches upload and acknowledge; nothing is dead-lettered.
        Assert.Equal(2, await RunUploadPassAsync(h));
        Assert.Equal(2, await ScalarAsync($"SELECT COUNT(*) FROM etl_batches WHERE run_id='{run}' AND status='acknowledged'"));

        Assert.Equal(1, await RunCompletionPassAsync(h));
        Assert.Equal("succeeded", await RunStatusAsync(runId));
        Assert.Equal("finished", await ScalarStringAsync($"SELECT status FROM etl_jobs WHERE run_id='{run}'"));

        // Baseline run: A and C committed a watermark; B has no watermark row at all.
        Assert.Equal(1, await ScalarAsync("SELECT generation FROM watermarks WHERE entity_name='clients'"));
        Assert.Equal(1, await ScalarAsync("SELECT generation FROM watermarks WHERE entity_name='payments'"));
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM watermarks WHERE entity_name='orders'"));

        // Ownership of all three entities is released.
        Assert.Equal(0, await ScalarAsync($"SELECT COUNT(*) FROM etl_entity_ownership WHERE owner_run_id='{run}' AND released_at_utc IS NULL"));
        Assert.Equal(3, await ScalarAsync($"SELECT COUNT(*) FROM etl_entity_ownership WHERE owner_run_id='{run}' AND release_reason='finalized'"));

        // The ERP completion body is the stored partial payload verbatim.
        var body = Assert.Single(h.Erp.Completions);
        Assert.Equal(runId, body.RunId);
        Assert.Equal(await ScalarStringAsync($"SELECT complete_payload_json FROM etl_runs WHERE run_id='{run}'"), body.Payload);
        using var document = JsonDocument.Parse(body.Payload);
        var root = document.RootElement;
        Assert.Equal("partial_success", root.GetProperty("status").GetString());
        Assert.Equal(1, root.GetProperty("entitiesFailed").GetInt64());
        var entities = root.GetProperty("entities").EnumerateArray().ToArray();
        Assert.Equal(3, entities.Length);
        Assert.Equal("clients", entities[0].GetProperty("entity").GetString());
        Assert.Equal("done", entities[0].GetProperty("status").GetString());
        Assert.Equal(JsonValueKind.Null, entities[0].GetProperty("errorCode").ValueKind);
        Assert.Equal("orders", entities[1].GetProperty("entity").GetString());
        Assert.Equal("failed", entities[1].GetProperty("status").GetString());
        Assert.Equal("ODATA_HTTP_500", entities[1].GetProperty("errorCode").GetString());
        Assert.Equal(0, entities[1].GetProperty("batchesCreated").GetInt64());
        Assert.Equal(0, entities[1].GetProperty("rowsRead").GetInt64());
        Assert.Equal("payments", entities[2].GetProperty("entity").GetString());
        Assert.Equal("done", entities[2].GetProperty("status").GetString());

        // ETL_RUN_PARTIAL (Warning) replaced ETL_RUN_SUCCEEDED.
        Assert.Contains(h.CompletionLog.Entries, e => e.Level == LogLevel.Warning && e.Message.Contains("ETL_RUN_PARTIAL") && e.Message.Contains(run));
        Assert.DoesNotContain(h.CompletionLog.Entries, e => e.Message.Contains("ETL_RUN_SUCCEEDED"));
        Assert.NotNull(h.State.LastEtlSuccessAtUtc);
    }

    // ---------- S3: failure after registered batches ----------

    [Fact]
    public async Task S3_Entity_failing_after_two_registered_batches_keeps_them_uploaded_and_counted()
    {
        // A tiny batch target flushes every row into its own batch.
        var h = NewHarness(["clients", "orders"], targetBatchUncompressedBytes: 1);
        h.OData.Rows["clients"] = [Row("C1")];
        h.OData.Scripts["orders"] = ct => YieldThenThrowAsync(
            [Row("O1"), Row("O2")],
            new HttpRequestException("server exploded", null, HttpStatusCode.InternalServerError), ct);

        var runId = await AcceptFullSyncAsync(h, "clients", "orders");
        var run = runId.ToString("D");

        Assert.True(await RunExtractionPassAsync(h));

        // The two batches registered before the failure stay counted.
        Assert.Equal("failed", await ScalarStringAsync($"SELECT status FROM etl_run_entities WHERE run_id='{run}' AND entity_name='orders'"));
        Assert.Equal(2, await ScalarAsync($"SELECT batches_created FROM etl_run_entities WHERE run_id='{run}' AND entity_name='orders'"));
        Assert.Equal(2, await ScalarAsync($"SELECT rows_read FROM etl_run_entities WHERE run_id='{run}' AND entity_name='orders'"));
        Assert.Equal(2, await ScalarAsync($"SELECT expected_batch_count FROM etl_run_entities WHERE run_id='{run}' AND entity_name='orders'"));
        Assert.Equal(2, await ScalarAsync($"SELECT COUNT(*) FROM etl_batches WHERE run_id='{run}' AND entity_name='orders'"));

        // All three batches (1 client + 2 order) upload and acknowledge; none dead-letters.
        Assert.Equal(3, await RunUploadPassAsync(h));
        Assert.Equal(2, await ScalarAsync($"SELECT COUNT(*) FROM etl_batches WHERE run_id='{run}' AND entity_name='orders' AND status='acknowledged'"));
        Assert.Equal(0, await ScalarAsync($"SELECT COUNT(*) FROM etl_batches WHERE run_id='{run}' AND status='dead_letter'"));

        Assert.Equal(1, await RunCompletionPassAsync(h));
        Assert.Equal("succeeded", await RunStatusAsync(runId));

        // The payload reports the failed entity's registered work exactly.
        var payload = Assert.Single(h.Erp.Completions).Payload;
        using var document = JsonDocument.Parse(payload);
        Assert.Equal("partial_success", document.RootElement.GetProperty("status").GetString());
        var orders = document.RootElement.GetProperty("entities").EnumerateArray()
            .Single(e => e.GetProperty("entity").GetString() == "orders");
        Assert.Equal("failed", orders.GetProperty("status").GetString());
        Assert.Equal("ODATA_HTTP_500", orders.GetProperty("errorCode").GetString());
        Assert.Equal(2, orders.GetProperty("batchesCreated").GetInt64());
        Assert.Equal(2, orders.GetProperty("rowsRead").GetInt64());
    }

    // ---------- S4: failure-code mapping ----------

    [Theory]
    [InlineData("http404", "ODATA_HTTP_404")]
    [InlineData("http-no-status", "ODATA_TRANSPORT")]
    [InlineData("invalid-data", "ODATA_LIMIT")]
    [InlineData("json", "ODATA_JSON")]
    [InlineData("timeout", "ODATA_TIMEOUT")]
    [InlineData("io", "ODATA_TRANSPORT")]
    [InlineData("other", "ODATA_READ")]
    public void S4_Source_exceptions_map_to_failure_codes(string kind, string expected)
    {
        var error = kind switch
        {
            "http404" => (Exception)new HttpRequestException("not found", null, HttpStatusCode.NotFound),
            "http-no-status" => new HttpRequestException("connection reset"),
            "invalid-data" => new InvalidDataException("row over the limit"),
            "json" => new JsonException("bad json"),
            "timeout" => new OperationCanceledException("read timed out"),
            "io" => new IOException("unexpected eof"),
            _ => new InvalidOperationException("no idea"),
        };
        Assert.Equal(expected, EtlExtractionWorker.SourceFailureCode(error, CancellationToken.None));
    }

    [Fact]
    public async Task S4_Caller_cancellation_maps_to_no_failure_code()
    {
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();
        Assert.Null(EtlExtractionWorker.SourceFailureCode(new OperationCanceledException(), cts.Token));
        Assert.Null(EtlExtractionWorker.SourceFailureCode(new TaskCanceledException(), cts.Token));
        // The caller's token decides, not the exception type.
        Assert.Equal("ODATA_TIMEOUT", EtlExtractionWorker.SourceFailureCode(new TaskCanceledException(), CancellationToken.None));
    }

    [Fact]
    public async Task S4_A_row_missing_the_key_field_fails_the_entity_with_odata_cursor_end_to_end()
    {
        var h = NewHarness(["clients", "orders"]);
        h.OData.Rows["clients"] = [Row("C1")];
        // 'orders' requires Ref_Key — this row never reaches the batch stage.
        h.OData.Rows["orders"] = [JsonDocument.Parse("{\"UpdatedAt\":\"2026-09-19T09:00:00Z\",\"DeletionMark\":false}").RootElement];

        var runId = await AcceptFullSyncAsync(h, "clients", "orders");
        var run = runId.ToString("D");

        Assert.True(await RunExtractionPassAsync(h));

        Assert.Equal("failed", await ScalarStringAsync($"SELECT status FROM etl_run_entities WHERE run_id='{run}' AND entity_name='orders'"));
        Assert.Equal("ODATA_CURSOR", await ScalarStringAsync($"SELECT failure_code FROM etl_run_entities WHERE run_id='{run}' AND entity_name='orders'"));
        Assert.Equal(0, await ScalarAsync($"SELECT COUNT(*) FROM etl_batches WHERE run_id='{run}' AND entity_name='orders'"));

        Assert.Equal(1, await RunUploadPassAsync(h));
        Assert.Equal(1, await RunCompletionPassAsync(h));
        Assert.Equal("succeeded", await RunStatusAsync(runId));
        Assert.Equal("partial_success", JsonDocument.Parse(Assert.Single(h.Erp.Completions).Payload).RootElement.GetProperty("status").GetString());
    }

    // ---------- S5: spool exhaustion stays run-level ----------

    [Fact]
    public async Task S5_Spool_limit_while_writing_entity_B_blocks_the_whole_run()
    {
        // The FileSpoolStore's own byte limit trips on the second entity's batch; the
        // StorageOptions pre-claim check stays permissive so the run starts.
        var h = NewHarness(["clients", "orders"], spoolLimit: 8 * 1024);
        h.OData.Rows["clients"] = [Row("C1")];
        h.OData.Rows["orders"] = [Row("O1", blob: Convert.ToBase64String(System.Security.Cryptography.RandomNumberGenerator.GetBytes(64 * 1024)))];

        var runId = await AcceptFullSyncAsync(h, "clients", "orders");
        var run = runId.ToString("D");

        Assert.True(await RunExtractionPassAsync(h));

        // A run-level block: the first entity is done, the second was terminated WITHOUT a
        // failure code — a terminated entity is not a skipped entity.
        Assert.Equal("blocked", await RunStatusAsync(runId));
        Assert.Equal("SPOOL_LIMIT", await ScalarStringAsync($"SELECT finalize_conflict_code FROM etl_runs WHERE run_id='{run}'"));
        Assert.Equal("blocked", await ScalarStringAsync($"SELECT status FROM etl_jobs WHERE run_id='{run}'"));
        Assert.Equal("done", await ScalarStringAsync($"SELECT status FROM etl_run_entities WHERE run_id='{run}' AND entity_name='clients'"));
        Assert.Equal("failed", await ScalarStringAsync($"SELECT status FROM etl_run_entities WHERE run_id='{run}' AND entity_name='orders'"));
        Assert.Null(await ScalarStringAsync($"SELECT failure_code FROM etl_run_entities WHERE run_id='{run}' AND entity_name='orders'"));
        Assert.Null(await ScalarStringAsync($"SELECT sealed_failed_entity_count FROM etl_runs WHERE run_id='{run}'"));
        Assert.Empty(h.Erp.Completions);
    }

    // ---------- S6: every entity fails ----------

    [Fact]
    public async Task S6_All_entities_failing_fails_the_run_and_holds_the_schedule_key()
    {
        var h = NewHarness(["clients", "orders"]);

        // Baseline both entities so the scheduled manifest covers them.
        h.OData.Rows["clients"] = [Row("C1")];
        h.OData.Rows["orders"] = [Row("O1")];
        var baseline = await AcceptFullSyncAsync(h, "clients", "orders");
        Assert.True(await RunExtractionPassAsync(h));
        Assert.Equal(2, await RunUploadPassAsync(h));
        Assert.Equal(1, await RunCompletionPassAsync(h));
        Assert.Equal("succeeded", await RunStatusAsync(baseline));

        // Now every source read fails in the scheduled incremental run.
        h.OData.Scripts["clients"] = _ => ThrowingRowsAsync(new HttpRequestException("down", null, HttpStatusCode.ServiceUnavailable));
        h.OData.Scripts["orders"] = _ => ThrowingRowsAsync(new IOException("connection lost"));

        Assert.True(await RunExtractionPassAsync(h));

        var scheduled = (await ScalarStringAsync("SELECT run_id FROM etl_runs WHERE schedule_key='incremental'"))!;
        Assert.Equal("failed", await ScalarStringAsync($"SELECT status FROM etl_runs WHERE run_id='{scheduled}'"));
        Assert.Contains("ALL_ENTITIES_FAILED", await ScalarStringAsync($"SELECT last_error FROM etl_runs WHERE run_id='{scheduled}'"));
        Assert.Contains("ALL_ENTITIES_FAILED", await ScalarStringAsync($"SELECT finalize_conflict_message FROM etl_runs WHERE run_id='{scheduled}'"));
        Assert.Equal(2, await ScalarAsync($"SELECT COUNT(*) FROM etl_run_entities WHERE run_id='{scheduled}' AND status='failed' AND failure_code IS NOT NULL"));

        // No completion is ever sent for the failed run.
        Assert.DoesNotContain(h.Erp.Completions, c => c.RunId == Guid.Parse(scheduled));
        Assert.Contains(h.ExtractionLog.Entries, e => e.Level == LogLevel.Error && e.Message.Contains("ETL_RUN_FAILED") && e.Message.Contains("ALL_ENTITIES_FAILED"));

        // The failed run holds the schedule key: a fresh ensure reports it, no new run is
        // created, and no pass has work.
        var ensure = await _store.EnsureScheduledEtlRunAsync(
            new EtlScheduledRunRequest("incremental", IncrementalMode, [Entity("clients"), Entity("orders")], 0),
            DateTimeOffset.UtcNow, CancellationToken.None);
        var existing = Assert.IsType<EtlScheduledRunEnsureOutcome.ActiveExisting>(ensure);
        Assert.Equal("failed", existing.Status);
        Assert.False(await RunExtractionPassAsync(h));
        Assert.Equal(1, await ScalarAsync("SELECT COUNT(*) FROM etl_runs WHERE schedule_key='incremental'"));
    }

    // ---------- S7: scheduled incremental partial run retries from the old cursor ----------

    [Fact]
    public async Task S7_A_scheduled_partial_run_releases_the_key_and_retries_the_failed_entity_from_its_old_cursor()
    {
        var h = NewHarness(["clients", "orders"]);

        // Baseline A and B in one manual run.
        h.OData.Rows["clients"] = [Row("C1")];
        h.OData.Rows["orders"] = [Row("O1")];
        var baseline = await AcceptFullSyncAsync(h, "clients", "orders");
        Assert.True(await RunExtractionPassAsync(h));
        Assert.Equal(2, await RunUploadPassAsync(h));
        Assert.Equal(1, await RunCompletionPassAsync(h));
        Assert.Equal("succeeded", await RunStatusAsync(baseline));
        var oldOrdersCursor = JsonSerializer.Deserialize<EtlCursor>(
            (await ScalarStringAsync("SELECT committed_cursor_json FROM watermarks WHERE entity_name='orders'"))!, JsonOptions);
        var oldOrdersGeneration = await ScalarAsync("SELECT generation FROM watermarks WHERE entity_name='orders'");

        // The scheduled incremental run: B's read fails.
        h.OData.Scripts["orders"] = _ => ThrowingRowsAsync(new HttpRequestException("down", null, HttpStatusCode.InternalServerError));

        Assert.True(await RunExtractionPassAsync(h));
        var firstScheduled = (await ScalarStringAsync("SELECT run_id FROM etl_runs WHERE schedule_key='incremental'"))!;
        Assert.Equal("uploading", await ScalarStringAsync($"SELECT status FROM etl_runs WHERE run_id='{firstScheduled}'"));
        Assert.Equal("ODATA_HTTP_500", await ScalarStringAsync($"SELECT failure_code FROM etl_run_entities WHERE run_id='{firstScheduled}' AND entity_name='orders'"));

        Assert.Equal(1, await RunUploadPassAsync(h));
        Assert.Equal(1, await RunCompletionPassAsync(h));
        Assert.Equal("succeeded", await ScalarStringAsync($"SELECT status FROM etl_runs WHERE run_id='{firstScheduled}'"));
        Assert.Equal("partial_success", JsonDocument.Parse(h.Erp.Completions[^1].Payload).RootElement.GetProperty("status").GetString());

        // The partial run never touched B's watermark: generation and cursor unchanged.
        Assert.Equal(oldOrdersGeneration, await ScalarAsync("SELECT generation FROM watermarks WHERE entity_name='orders'"));
        Assert.Equal(oldOrdersCursor, JsonSerializer.Deserialize<EtlCursor>(
            (await ScalarStringAsync("SELECT committed_cursor_json FROM watermarks WHERE entity_name='orders'"))!, JsonOptions));

        // The key is released: the next tick creates and dispatches a new scheduled run in
        // the same pass, and B is re-read from its OLD committed cursor.
        h.OData.Scripts.Remove("orders");
        h.OData.Rows["orders"] = [Row("O2")];
        Assert.True(await RunExtractionPassAsync(h));
        Assert.Equal(2, await ScalarAsync("SELECT COUNT(*) FROM etl_runs WHERE schedule_key='incremental'"));

        var lastOrdersRead = h.OData.Snapshot().Last(c => c.EntityCode == "orders");
        Assert.False(lastOrdersRead.Full);
        Assert.Equal(oldOrdersCursor, lastOrdersRead.Committed);
    }

    // ---------- S8: the fail-entity fence writes nothing ----------

    [Fact]
    public async Task S8_Fail_entity_under_a_stale_claim_a_done_entity_or_a_sealed_run_writes_nothing()
    {
        var runId = await NewClaimedRunAsync("clients");
        var claim = ClaimOf(runId);
        var run = runId.ToString("D");
        Assert.IsType<EtlEntityBeginOutcome.Begun>(
            await _store.BeginEtlEntityExtractionAsync(runId, claim, Request("clients"), CancellationToken.None));

        // Stale claim: refused, no writes.
        var stale = await _store.FailEtlEntityExtractionAsync(runId, Guid.NewGuid(), "clients", "ODATA_READ", "boom", CancellationToken.None);
        Assert.Equal(EtlEntityCompletionRejection.ExtractionClaimLost, Assert.IsType<EtlEntityFailureOutcome.Rejected>(stale).Reason);
        Assert.Equal("extracting", await ScalarStringAsync($"SELECT status FROM etl_run_entities WHERE run_id='{run}' AND entity_name='clients'"));
        Assert.Null(await ScalarStringAsync($"SELECT failure_code FROM etl_run_entities WHERE run_id='{run}' AND entity_name='clients'"));
        Assert.Equal(1, await ScalarAsync($"SELECT row_version FROM etl_run_entities WHERE run_id='{run}' AND entity_name='clients'"));

        // Complete the entity, then fail it: the entity is not 'extracting' — refused.
        Assert.IsType<EtlBatchRegistrationOutcome.Registered>(
            await _store.RegisterGuardedEtlBatchAsync(MakeBatch(runId, "clients", 3), claim, CancellationToken.None));
        Assert.IsType<EtlEntityCompletionOutcome.Completed>(
            await _store.CompleteEtlEntityExtractionAsync(runId, claim, "clients", CursorJson(FinalCursor), 1, CancellationToken.None));
        var done = await _store.FailEtlEntityExtractionAsync(runId, claim, "clients", "ODATA_READ", "boom", CancellationToken.None);
        Assert.Equal(EtlEntityCompletionRejection.EntityNotExtracting, Assert.IsType<EtlEntityFailureOutcome.Rejected>(done).Reason);
        Assert.Equal("done", await ScalarStringAsync($"SELECT status FROM etl_run_entities WHERE run_id='{run}' AND entity_name='clients'"));
        Assert.Null(await ScalarStringAsync($"SELECT failure_code FROM etl_run_entities WHERE run_id='{run}' AND entity_name='clients'"));

        // Sealed run: the same refusal writes nothing.
        Assert.IsType<EtlRunSealOutcome.Sealed>(await _store.SealEtlRunExtractionAsync(runId, claim, CancellationToken.None));
        var sealedRun = await _store.FailEtlEntityExtractionAsync(runId, claim, "clients", "ODATA_READ", "boom", CancellationToken.None);
        Assert.Equal(EtlEntityCompletionRejection.EntityNotExtracting, Assert.IsType<EtlEntityFailureOutcome.Rejected>(sealedRun).Reason);
        Assert.Equal("uploading", await RunStatusAsync(runId));
        Assert.Equal("done", await ScalarStringAsync($"SELECT status FROM etl_run_entities WHERE run_id='{run}' AND entity_name='clients'"));
        Assert.Null(await ScalarStringAsync($"SELECT failure_code FROM etl_run_entities WHERE run_id='{run}' AND entity_name='clients'"));
        Assert.Equal(1, await ScalarAsync($"SELECT COUNT(*) FROM etl_batches WHERE run_id='{run}'"));
    }

    // ---------- S9: seal validation of failed entities ----------

    [Fact]
    public async Task S9_Seal_rejects_a_failed_row_without_a_failure_code()
    {
        var (runId, claim) = await PartiallyFailedRunAsync();
        var run = runId.ToString("D");

        // Erase the durable code: the failed row is no longer a skippable failure.
        await ExecuteSqlAsync(
            "UPDATE etl_run_entities SET failure_code=NULL WHERE run_id=$run AND entity_name='orders';",
            ("$run", run));

        var seal = await _store.SealEtlRunExtractionAsync(runId, claim, CancellationToken.None);
        Assert.Equal(EtlRunSealRejection.EntityNotDone, Assert.IsType<EtlRunSealOutcome.Rejected>(seal).Reason);
        Assert.Equal("running", await RunStatusAsync(runId));
        Assert.Null(await ScalarStringAsync($"SELECT sealed_at_utc FROM etl_runs WHERE run_id='{run}'"));
    }

    [Fact]
    public async Task S9_Seal_rejects_a_failed_row_whose_expected_batch_count_lies()
    {
        var (runId, claim) = await PartiallyFailedRunAsync();
        var run = runId.ToString("D");

        // The failed entity registered no batches; claim one expected anyway.
        await ExecuteSqlAsync(
            "UPDATE etl_run_entities SET expected_batch_count=1 WHERE run_id=$run AND entity_name='orders';",
            ("$run", run));

        var seal = await _store.SealEtlRunExtractionAsync(runId, claim, CancellationToken.None);
        Assert.Equal(EtlRunSealRejection.ExpectedBatchCountMismatch, Assert.IsType<EtlRunSealOutcome.Rejected>(seal).Reason);
        Assert.Equal("running", await RunStatusAsync(runId));
    }

    [Fact]
    public async Task S9_Seal_rejects_a_run_where_every_entity_failed()
    {
        var runId = await NewClaimedRunAsync("clients", "orders");
        var claim = ClaimOf(runId);
        Assert.IsType<EtlEntityBeginOutcome.Begun>(
            await _store.BeginEtlEntityExtractionAsync(runId, claim, Request("clients"), CancellationToken.None));
        Assert.IsType<EtlEntityBeginOutcome.Begun>(
            await _store.BeginEtlEntityExtractionAsync(runId, claim, Request("orders"), CancellationToken.None));
        Assert.IsType<EtlEntityFailureOutcome.Failed>(
            await _store.FailEtlEntityExtractionAsync(runId, claim, "clients", "ODATA_TRANSPORT", "down", CancellationToken.None));
        Assert.IsType<EtlEntityFailureOutcome.Failed>(
            await _store.FailEtlEntityExtractionAsync(runId, claim, "orders", "ODATA_JSON", "bad json", CancellationToken.None));

        var seal = await _store.SealEtlRunExtractionAsync(runId, claim, CancellationToken.None);
        Assert.Equal(EtlRunSealRejection.AllEntitiesFailed, Assert.IsType<EtlRunSealOutcome.Rejected>(seal).Reason);
        Assert.Equal("running", await RunStatusAsync(runId));
    }

    // ---------- S10: replay byte-identity and tamper ----------

    [Fact]
    public async Task S10_A_retried_completion_of_a_partial_run_sends_the_stored_payload_byte_identically()
    {
        var h = NewHarness(["clients", "orders"]);
        h.OData.Rows["clients"] = [Row("C1")];
        h.OData.Scripts["orders"] = _ => ThrowingRowsAsync(new HttpRequestException("down", null, HttpStatusCode.InternalServerError));
        var runId = await AcceptFullSyncAsync(h, "clients", "orders");
        var run = runId.ToString("D");
        Assert.True(await RunExtractionPassAsync(h));
        Assert.Equal(1, await RunUploadPassAsync(h));
        h.Erp.CompletionScript.Enqueue(new HttpRequestException("completion response lost"));
        using var completer = h.NewCompletionWorker();

        Assert.Equal(1, await completer.RunOnceAsync(CancellationToken.None));
        Assert.Equal("completing", await RunStatusAsync(runId));
        var stored = (await ScalarStringAsync($"SELECT complete_payload_json FROM etl_runs WHERE run_id='{run}'"))!;

        await ExecuteSqlAsync($"UPDATE etl_runs SET next_completion_attempt_at_utc=$past WHERE run_id='{run}';",
            ("$past", Now(DateTimeOffset.UtcNow.AddMinutes(-10))));

        Assert.Equal(1, await completer.RunOnceAsync(CancellationToken.None));

        Assert.Equal("succeeded", await RunStatusAsync(runId));
        Assert.Equal(2, h.Erp.Completions.Length);
        Assert.Equal(stored, h.Erp.Completions[0].Payload);
        Assert.Equal(stored, h.Erp.Completions[1].Payload);
        Assert.Equal(System.Text.Encoding.UTF8.GetBytes(stored), System.Text.Encoding.UTF8.GetBytes(h.Erp.Completions[1].Payload));
        Assert.Contains(h.CompletionLog.Entries, e => e.Level == LogLevel.Warning && e.Message.Contains("ETL_RUN_PARTIAL"));
    }

    [Fact]
    public async Task S10_A_tampered_entities_array_in_the_stored_payload_blocks_seal_violated()
    {
        var (h, runId, stored) = await PartialRunAwaitingRetryAsync();
        var run = runId.ToString("D");

        // Tamper with the recorded failure code inside the immutable body.
        var tampered = stored.Replace("\"errorCode\":\"ODATA_HTTP_500\"", "\"errorCode\":\"ODATA_HTTP_404\"", StringComparison.Ordinal);
        Assert.NotEqual(stored, tampered);
        await ExecuteSqlAsync(
            $"UPDATE etl_runs SET complete_payload_json=$payload, next_completion_attempt_at_utc=$past WHERE run_id='{run}';",
            ("$payload", tampered), ("$past", Now(DateTimeOffset.UtcNow.AddMinutes(-10))));
        using var completer = h.NewCompletionWorker();

        Assert.Equal(0, await completer.RunOnceAsync(CancellationToken.None));

        Assert.Equal("blocked", await RunStatusAsync(runId));
        Assert.Equal("SEAL_VIOLATED", await ScalarStringAsync($"SELECT finalize_conflict_code FROM etl_runs WHERE run_id='{run}'"));
        Assert.Single(h.Erp.Completions); // the tampered body is never sent
        Assert.Contains(h.CompletionLog.Entries, e => e.Level == LogLevel.Warning && e.Message.Contains("ETL_RUN_BLOCKED_AT_COMPLETION") && e.Message.Contains("SEAL_VIOLATED"));
    }

    [Fact]
    public async Task S10_A_legacy_six_property_payload_on_a_run_with_a_failed_entity_blocks_seal_violated()
    {
        var (h, runId, _) = await PartialRunAwaitingRetryAsync();
        var run = runId.ToString("D");
        var rowsRead = await ScalarAsync($"SELECT rows_read FROM etl_runs WHERE run_id='{run}'");
        var created = await ScalarAsync($"SELECT batches_created FROM etl_runs WHERE run_id='{run}'");
        var acknowledged = await ScalarAsync($"SELECT batches_acknowledged FROM etl_runs WHERE run_id='{run}'");

        // The legacy all-succeeded shape is not a valid payload for a run with a failure.
        var legacy = $"{{\"runId\":\"{runId:D}\",\"status\":\"succeeded\",\"rowsRead\":{rowsRead},\"batchesCreated\":{created},\"batchesAcknowledged\":{acknowledged},\"completedAtUtc\":\"{Now()}\"}}";
        await ExecuteSqlAsync(
            $"UPDATE etl_runs SET complete_payload_json=$payload, next_completion_attempt_at_utc=$past WHERE run_id='{run}';",
            ("$payload", legacy), ("$past", Now(DateTimeOffset.UtcNow.AddMinutes(-10))));
        using var completer = h.NewCompletionWorker();

        Assert.Equal(0, await completer.RunOnceAsync(CancellationToken.None));

        Assert.Equal("blocked", await RunStatusAsync(runId));
        Assert.Equal("SEAL_VIOLATED", await ScalarStringAsync($"SELECT finalize_conflict_code FROM etl_runs WHERE run_id='{run}'"));
        Assert.Single(h.Erp.Completions);
    }

    // ---------- S11: Begin domain rejections skip the entity ----------

    [Fact]
    public async Task S11_A_domain_changed_watermark_fails_the_entity_and_the_run_completes_partial()
    {
        var h = NewHarness(["clients", "orders"]);
        // A committed 'orders' watermark produced by a DIFFERENT domain (other source).
        var foreignFingerprint = EtlDomainFingerprint.Compute("other-source", "orders", DefinitionJson("orders"), QueryMode);
        var foreignCursor = new EtlCursor(DateTimeOffset.Parse("2026-09-10T06:00:00.0000000+00:00", CultureInfo.InvariantCulture), "X7");
        await SeedWatermarkAsync("orders", CursorJson(foreignCursor), generation: 3, fingerprint: foreignFingerprint);
        h.OData.Rows["clients"] = [Row("C1")];
        // Begin refuses 'orders' before a single source read.
        h.OData.Scripts["orders"] = _ => ThrowingRowsAsync(new InvalidOperationException("must never be read"));

        var runId = await AcceptFullSyncAsync(h, "clients", "orders");
        var run = runId.ToString("D");

        Assert.True(await RunExtractionPassAsync(h));

        Assert.Equal("failed", await ScalarStringAsync($"SELECT status FROM etl_run_entities WHERE run_id='{run}' AND entity_name='orders'"));
        Assert.Equal("DOMAIN_CHANGED", await ScalarStringAsync($"SELECT failure_code FROM etl_run_entities WHERE run_id='{run}' AND entity_name='orders'"));
        Assert.Equal(0, await ScalarAsync($"SELECT expected_batch_count FROM etl_run_entities WHERE run_id='{run}' AND entity_name='orders'"));
        Assert.Equal("changed", await ScalarStringAsync($"SELECT domain_status FROM etl_run_entities WHERE run_id='{run}' AND entity_name='orders'"));
        Assert.DoesNotContain(h.OData.Snapshot(), c => c.EntityCode == "orders");
        Assert.Contains(h.ExtractionLog.Entries, e => e.Level == LogLevel.Error
            && e.Message.Contains("ETL_ENTITY_FAILED") && e.Message.Contains("orders") && e.Message.Contains("DOMAIN_CHANGED"));

        Assert.Equal(1, await RunUploadPassAsync(h));
        Assert.Equal(1, await RunCompletionPassAsync(h));

        Assert.Equal("succeeded", await RunStatusAsync(runId));
        var payload = Assert.Single(h.Erp.Completions).Payload;
        using var document = JsonDocument.Parse(payload);
        Assert.Equal("partial_success", document.RootElement.GetProperty("status").GetString());
        Assert.Equal("DOMAIN_CHANGED", document.RootElement.GetProperty("entities").EnumerateArray()
            .Single(e => e.GetProperty("entity").GetString() == "orders").GetProperty("errorCode").GetString());
        // The foreign watermark is untouched — the failed entity's base is never committed.
        Assert.Equal(3, await ScalarAsync("SELECT generation FROM watermarks WHERE entity_name='orders'"));
        Assert.Equal(CursorJson(foreignCursor), await ScalarStringAsync("SELECT committed_cursor_json FROM watermarks WHERE entity_name='orders'"));
    }

    // ---------- S12: restart before seal keeps the failure code ----------

    [Fact]
    public async Task S12_Recovery_blocks_an_interrupted_run_and_keeps_the_failed_entity_code()
    {
        var runId = await NewClaimedRunAsync("clients", "orders");
        var claim = ClaimOf(runId);
        var run = runId.ToString("D");
        Assert.IsType<EtlEntityBeginOutcome.Begun>(
            await _store.BeginEtlEntityExtractionAsync(runId, claim, Request("clients"), CancellationToken.None));
        Assert.IsType<EtlEntityBeginOutcome.Begun>(
            await _store.BeginEtlEntityExtractionAsync(runId, claim, Request("orders"), CancellationToken.None));
        Assert.IsType<EtlEntityFailureOutcome.Failed>(
            await _store.FailEtlEntityExtractionAsync(runId, claim, "orders", "ODATA_TRANSPORT", "connection dropped", CancellationToken.None));

        var recovered = await _store.RecoverInterruptedEtlRunsAsync(CancellationToken.None);

        Assert.Equal("blocked", await RunStatusAsync(runId));
        Assert.Equal("INTERRUPTED_NO_CHECKPOINT", await ScalarStringAsync($"SELECT finalize_conflict_code FROM etl_runs WHERE run_id='{run}'"));
        Assert.True(recovered.RunsBlocked >= 1);
        // The already-failed entity keeps its source failure code — recovery only flips
        // still-'extracting' rows.
        Assert.Equal("failed", await ScalarStringAsync($"SELECT status FROM etl_run_entities WHERE run_id='{run}' AND entity_name='orders'"));
        Assert.Equal("ODATA_TRANSPORT", await ScalarStringAsync($"SELECT failure_code FROM etl_run_entities WHERE run_id='{run}' AND entity_name='orders'"));
        Assert.Equal("failed", await ScalarStringAsync($"SELECT status FROM etl_run_entities WHERE run_id='{run}' AND entity_name='clients'"));
        Assert.Null(await ScalarStringAsync($"SELECT failure_code FROM etl_run_entities WHERE run_id='{run}' AND entity_name='clients'"));
        Assert.Equal("RUN_INTERRUPTED", await ScalarStringAsync($"SELECT last_error FROM etl_run_entities WHERE run_id='{run}' AND entity_name='clients'"));
    }

    // ---------- S13: a partial run is resolved, never counted ----------

    [Fact]
    public async Task S13_A_completed_partial_run_is_not_counted_as_unresolved()
    {
        var h = NewHarness(["clients", "orders"]);
        h.OData.Rows["clients"] = [Row("C1")];
        h.OData.Scripts["orders"] = _ => ThrowingRowsAsync(new HttpRequestException("down", null, HttpStatusCode.InternalServerError));
        var runId = await AcceptFullSyncAsync(h, "clients", "orders");

        Assert.True(await RunExtractionPassAsync(h));
        Assert.Equal(1, await RunUploadPassAsync(h));
        Assert.Equal(1, await RunCompletionPassAsync(h));
        Assert.Equal("succeeded", await RunStatusAsync(runId));

        Assert.Equal(0, (await _store.GetQueueMetricsAsync(CancellationToken.None)).EtlRunsUnresolved);
    }

    // ---------- scenario helpers ----------

    /// <summary>Drives a stored command through the worker's real claim+route path.</summary>
    private async Task<Guid> AcceptFullSyncAsync(Harness h, params string[] entities)
    {
        var command = entities.Length == 0
            ? MakeCommand("start_full_sync", new { })
            : MakeCommand("start_full_sync", new { entities });
        await _store.StoreCommandAsync(command, DateTimeOffset.UtcNow, CancellationToken.None);
        await DriveAdminAsync(h, await ReadyForAsync(command.CommandId));
        var result = await ResultForAsync(command.CommandId);
        Assert.Equal("succeeded", result.GetProperty("status").GetString());
        Assert.True(result.GetProperty("data").GetProperty("accepted").GetBoolean());
        return Guid.Parse(result.GetProperty("data").GetProperty("runId").GetString()!);
    }

    /// <summary>A partial run that has been extracted, uploaded, then failed its first completion send — 'completing' with the immutable payload stored and the claim released.</summary>
    private async Task<(Harness H, Guid RunId, string StoredPayload)> PartialRunAwaitingRetryAsync()
    {
        var h = NewHarness(["clients", "orders"]);
        h.OData.Rows["clients"] = [Row("C1")];
        h.OData.Scripts["orders"] = _ => ThrowingRowsAsync(new HttpRequestException("down", null, HttpStatusCode.InternalServerError));
        var runId = await AcceptFullSyncAsync(h, "clients", "orders");
        Assert.True(await RunExtractionPassAsync(h));
        Assert.Equal(1, await RunUploadPassAsync(h));
        h.Erp.CompletionScript.Enqueue(new HttpRequestException("completion response lost"));
        using var completer = h.NewCompletionWorker();
        Assert.Equal(1, await completer.RunOnceAsync(CancellationToken.None));
        Assert.Equal("completing", await RunStatusAsync(runId));
        var stored = (await ScalarStringAsync($"SELECT complete_payload_json FROM etl_runs WHERE run_id='{runId:D}'"))!;
        return (h, runId, stored);
    }

    /// <summary>A claimed run whose 'clients' entity completed and 'orders' failed at the source.</summary>
    private async Task<(Guid RunId, Guid Claim)> PartiallyFailedRunAsync()
    {
        var runId = await NewClaimedRunAsync("clients", "orders");
        var claim = ClaimOf(runId);
        Assert.IsType<EtlEntityBeginOutcome.Begun>(
            await _store.BeginEtlEntityExtractionAsync(runId, claim, Request("clients"), CancellationToken.None));
        Assert.IsType<EtlEntityBeginOutcome.Begun>(
            await _store.BeginEtlEntityExtractionAsync(runId, claim, Request("orders"), CancellationToken.None));
        Assert.IsType<EtlBatchRegistrationOutcome.Registered>(
            await _store.RegisterGuardedEtlBatchAsync(MakeBatch(runId, "clients", 3), claim, CancellationToken.None));
        Assert.IsType<EtlEntityCompletionOutcome.Completed>(
            await _store.CompleteEtlEntityExtractionAsync(runId, claim, "clients", CursorJson(FinalCursor), 1, CancellationToken.None));
        Assert.IsType<EtlEntityFailureOutcome.Failed>(
            await _store.FailEtlEntityExtractionAsync(runId, claim, "orders", "ODATA_TRANSPORT", "connection dropped", CancellationToken.None));
        return (runId, claim);
    }

    /// <summary>One extraction pass on a fresh worker — a fresh instance always takes a schedule tick.</summary>
    private static async Task<bool> RunExtractionPassAsync(Harness h)
    {
        using var worker = h.NewExtractionWorker();
        return await worker.RunOnceAsync(CancellationToken.None);
    }

    private static async Task<int> RunUploadPassAsync(Harness h)
    {
        using var worker = h.NewUploadWorker();
        return await worker.RunOnceAsync(CancellationToken.None);
    }

    private static async Task<int> RunCompletionPassAsync(Harness h)
    {
        using var worker = h.NewCompletionWorker();
        return await worker.RunOnceAsync(CancellationToken.None);
    }

    private static async Task DriveAdminAsync(Harness h, StoredCommand command)
    {
        using var worker = h.NewCommandWorker();
        var method = typeof(CommandExecutionWorker).GetMethod("ProcessAsync", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("Command execution method was not found.");
        await (Task)(method.Invoke(worker, [command, CancellationToken.None])
            ?? throw new InvalidOperationException("Command execution method returned no task."));
    }

    // ---------- store helpers ----------

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
    private async Task<Guid> NewClaimedRunAsync(params string[] entities)
    {
        var (runId, jobId) = await NewPendingJobRunAsync(QueryMode, entities);
        var claimed = Assert.IsType<EtlJobClaimOutcome.Claimed>(
            await _store.TryClaimEtlJobAsync(jobId, "test-dispatcher", DateTimeOffset.UtcNow.AddMinutes(5), DateTimeOffset.UtcNow, CancellationToken.None));
        _extractionClaims[runId] = claimed.Claim.ExtractionClaimId;
        return runId;
    }

    private async Task SeedWatermarkAsync(string entity, string? cursorJson, long generation, string? fingerprint)
    {
        await ExecuteSqlAsync(
            "INSERT INTO watermarks(entity_name,committed_cursor_json,extracting_cursor_json,last_run_id,generation,domain_fingerprint,updated_at_utc) VALUES($entity,$cursor,NULL,$run,$gen,$fp,$now);",
            ("$entity", entity), ("$cursor", cursorJson), ("$run", Guid.NewGuid().ToString("D")),
            ("$gen", generation), ("$fp", fingerprint), ("$now", Now()));
    }

    private static EtlEntityDefinition Entity(string code) => Catalog().Single(e => e.EntityCode == code);
    private static EtlEntityDefinition[] Catalog() =>
    [
        new("clients", "Catalog_Clients", "Ref_Key", "UpdatedAt", "DeletionMark", ["Ref_Key", "UpdatedAt", "DeletionMark"], "incremental", 500, 10),
        new("orders", "Document_Orders", "Ref_Key", "Date", "Posted", ["Ref_Key", "Date", "Posted"], "incremental", 500, 10),
        new("payments", "Document_Payments", "Ref_Key", "Date", "Posted", ["Ref_Key", "Date", "Posted"], "incremental", 500, 10),
    ];

    private static string DefinitionJson(string entity) => JsonSerializer.Serialize(Entity(entity), JsonOptions);
    private static string DefinitionsJson(string[] entities) => JsonSerializer.Serialize(entities.Select(Entity).ToArray(), JsonOptions);
    private static string ManifestJson(string[] entities) => JsonSerializer.Serialize(entities, JsonOptions);
    private static string CursorJson(EtlCursor cursor) => JsonSerializer.Serialize(cursor, JsonOptions);
    private static EtlEntityExtractionRequest Request(string entity, string? sourceNamespace = null, string queryMode = QueryMode) =>
        new(entity, DefinitionJson(entity), sourceNamespace ?? StoreSourceNamespace, queryMode, CursorJson(new EtlCursor(DateTimeOffset.UtcNow, null)));
    private static EtlBatch MakeBatch(Guid runId, string entity, int rowCount) =>
        new(Guid.NewGuid(), runId, entity, 1, $"spool/{runId:N}-{entity}-{Guid.NewGuid():N}.gz", EtlBatchStatus.Ready, rowCount,
            null, FinalCursor, "hash-" + Guid.NewGuid().ToString("N"), 1000, 4000, 0, DateTimeOffset.UtcNow);

    // One valid catalog row; the optional blob inflates it for the spool-limit scenario.
    private static JsonElement Row(string id, string? blob = null)
    {
        var extra = blob is null ? "" : $",\"Blob\":\"{blob}\"";
        return JsonDocument.Parse($"{{\"Ref_Key\":\"{id}\",\"UpdatedAt\":\"2026-09-19T09:00:00Z\",\"DeletionMark\":false{extra}}}").RootElement;
    }

    private static CommandEnvelope MakeCommand(string commandType, object payload)
    {
        var body = JsonSerializer.SerializeToElement(payload);
        return new(Guid.NewGuid(), commandType, 1, 100, $"partial:{Guid.NewGuid():N}", null, DateTimeOffset.UtcNow, null, null, null, PayloadHasher.Compute(body), body);
    }

    private async Task<StoredCommand> ReadyForAsync(Guid commandId) =>
        (await _store.GetReadyCommandsAsync(10, DateTimeOffset.UtcNow.AddDays(1), CancellationToken.None)).Single(command => command.Envelope.CommandId == commandId);

    private async Task<JsonElement> ResultForAsync(Guid commandId)
    {
        var result = (await _store.GetPendingResultsAsync(10, DateTimeOffset.UtcNow, CancellationToken.None)).Single(item => item.CommandId == commandId);
        using var document = JsonDocument.Parse(result.PayloadJson);
        return document.RootElement.Clone();
    }

    // ---------- sql helpers ----------

    private static string Now() => DateTimeOffset.UtcNow.ToString("O");
    private static string Now(DateTimeOffset value) => value.ToUniversalTime().ToString("O");
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

    private async Task<List<string>> TableColumnsAsync(string table)
    {
        var columns = new List<string>();
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = $"SELECT name FROM pragma_table_info('{table}');";
        await using var reader = await command.ExecuteReaderAsync(CancellationToken.None);
        while (await reader.ReadAsync(CancellationToken.None)) columns.Add(reader.GetString(0));
        return columns;
    }

    private async Task ExecuteSqlAsync(string sql, params (string Name, object? Value)[] parameters)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        foreach (var (name, value) in parameters) command.Parameters.AddWithValue(name, value ?? DBNull.Value);
        await command.ExecuteNonQueryAsync(CancellationToken.None);
    }

    // ---------- harness ----------

    private Harness NewHarness(
        string[] entities,
        long spoolLimit = 1024L * 1024 * 1024,
        int targetBatchUncompressedBytes = 1024 * 1024)
    {
        var databaseId = Guid.NewGuid();
        var exportEpoch = Guid.NewGuid();
        var onec = new OnecOptions
        {
            ODataBaseUrl = "https://onec.example.test/odata",
            CommandApiBaseUrl = "https://onec.example.test/hs/cmd",
            SourceBinding = new OnecSourceBindingOptions
            {
                DatabaseId = databaseId.ToString("D"),
                ExportEpoch = exportEpoch.ToString("D"),
                Environment = "test",
                ODataEndpoint = "https://onec.example.test/odata"
            }
        };
        var etl = new EtlOptions
        {
            Enabled = true,
            RunOnStartup = true,
            IntervalMinutes = 60,
            SafetyLagSeconds = 0,
            MaxConcurrentBatchUploads = 4,
            MaxBatchUploadAttempts = 5,
            MaxRunCompletionAttempts = 20,
            TargetBatchUncompressedBytes = targetBatchUncompressedBytes,
            Entities = entities.Select(Entity).ToArray()
        };
        var storage = new StorageOptions
        {
            MinimumReservedBytesForCommands = 1024,
            MaxBatchCompressedBytes = 50L * 1024 * 1024,
            // The pre-claim pass check stays permissive; the FileSpoolStore's own
            // spoolLimit is what trips inside extraction (S5).
            MaxSpoolBytes = 1024L * 1024 * 1024
        };
        var spoolRoot = Path.Combine(_database.Root, "spool");
        var identity = new ScriptedIdentity([Match(databaseId, exportEpoch)]);
        var disk = new ScriptedDiskProbe([PlentyOfDisk]);
        var state = ReadyState();
        var harness = new Harness
        {
            Store = _store,
            SpoolRoot = spoolRoot,
            Spool = new FileSpoolStore(spoolRoot, storage.MaxBatchCompressedBytes, spoolLimit, disk, storage.MinimumReservedBytesForCommands),
            OData = new FakeOData(),
            Erp = new FakeErp(),
            Identity = identity,
            Disk = disk,
            State = state,
            Configuration = new DynamicConfigurationState(
                Options.Create(new CommandOptions { SupportedTypes = AdministrativeTypes }),
                Options.Create(etl)),
            Pause = new LocalEtlPauseController(_store, state),
            Onec = new CountingOnec(),
            Agent = Options.Create(new AgentOptions { AgentId = $"partial-{Guid.NewGuid():N}", SiteId = "test-site", DataDirectory = _database.Root }),
            Etl = Options.Create(etl),
            Storage = Options.Create(storage),
            OnecOptions = Options.Create(onec),
            IdentityGuard = new SourceIdentityGuard(identity, Options.Create(onec)),
            ExtractionLog = new CaptureLogger<EtlExtractionWorker>(),
            UploadLog = new CaptureLogger<EtlUploadWorker>(),
            CompletionLog = new CaptureLogger<EtlCompletionWorker>(),
            CommandLog = new CaptureLogger<CommandExecutionWorker>(),
        };
        _harnesses.Add(harness);
        return harness;
    }

    private static readonly string[] AdministrativeTypes =
    [
        "start_full_sync", "reload_entity", "reconcile_keys", "reconcile_totals",
        "pause_etl", "resume_etl", "collect_diagnostics", "rotate_certificate_hint", "run_connectivity_test"
    ];

    private static AgentRuntimeState ReadyState()
    {
        var state = new AgentRuntimeState();
        state.RestoreLocalEtlPause(false);
        state.SetRemoteMode(AgentMode.Normal);
        state.CompleteBootstrap();
        return state;
    }

    private static OnecIdentityFetchResult Match(Guid databaseId, Guid exportEpoch) =>
        OnecIdentityFetchResult.Ready(new OnecSourceIdentity(1, "ready", "whole-infobase", databaseId, exportEpoch, "test"));

    // ---------- fakes ----------

    private sealed class Harness : IDisposable
    {
        public required SqliteAgentStore Store { get; init; }
        public required string SpoolRoot { get; init; }
        public required FileSpoolStore Spool { get; init; }
        public required FakeOData OData { get; init; }
        public required FakeErp Erp { get; init; }
        public required ScriptedIdentity Identity { get; init; }
        public required ScriptedDiskProbe Disk { get; init; }
        public required AgentRuntimeState State { get; init; }
        public required DynamicConfigurationState Configuration { get; init; }
        public required LocalEtlPauseController Pause { get; init; }
        public required CountingOnec Onec { get; init; }
        public required IOptions<AgentOptions> Agent { get; init; }
        public required IOptions<EtlOptions> Etl { get; init; }
        public required IOptions<StorageOptions> Storage { get; init; }
        public required IOptions<OnecOptions> OnecOptions { get; init; }
        public required SourceIdentityGuard IdentityGuard { get; init; }
        public required CaptureLogger<EtlExtractionWorker> ExtractionLog { get; init; }
        public required CaptureLogger<EtlUploadWorker> UploadLog { get; init; }
        public required CaptureLogger<EtlCompletionWorker> CompletionLog { get; init; }
        public required CaptureLogger<CommandExecutionWorker> CommandLog { get; init; }

        public EtlExtractionWorker NewExtractionWorker() => new(
            Store, Spool, OData, IdentityGuard, Disk, State, Configuration, Etl, Storage, Agent, ExtractionLog);

        public EtlUploadWorker NewUploadWorker() => new(
            Store, Spool, Erp, State, Etl, Agent, UploadLog);

        public EtlCompletionWorker NewCompletionWorker() => new(
            Store, Erp, State, Etl, Agent, CompletionLog);

        public CommandExecutionWorker NewCommandWorker() => new(
            Store, Onec, new FakeOnecHealth(), Configuration, State, Pause,
            new DiagnosticsCollector(Store, Agent, Options.Create(new ErpOptions { RequireClientCertificate = false }), OnecOptions),
            Options.Create(new CommandOptions { MaxConcurrency = 4, MaxOperationalAttempts = 12, SupportedTypes = AdministrativeTypes }),
            CommandLog);

        public void Dispose() => Pause.Dispose();
    }

    /// <summary>ILogger&lt;T&gt; that records level + formatted message + exception for assertions.</summary>
    private sealed class CaptureLogger<T> : ILogger<T>
    {
        private readonly object _gate = new();
        private readonly List<(LogLevel Level, string Message, Exception? Error)> _entries = [];

        public (LogLevel Level, string Message, Exception? Error)[] Entries
        {
            get { lock (_gate) return _entries.ToArray(); }
        }

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            lock (_gate) _entries.Add((logLevel, formatter(state, exception), exception));
        }
    }

    private sealed record ODataReadCall(string EntityCode, EtlCursor? Committed, EtlCursor UpperBound, bool Full);

    /// <summary>Rows yield verbatim; Scripts replace an entity's stream wholesale (may throw mid-enumeration).</summary>
    private sealed class FakeOData : IOnecODataClient
    {
        private readonly object _gate = new();
        private readonly List<ODataReadCall> _calls = [];
        public Dictionary<string, List<JsonElement>> Rows { get; } = new(StringComparer.Ordinal);
        public Dictionary<string, Func<CancellationToken, IAsyncEnumerable<JsonElement>>> Scripts { get; } = new(StringComparer.Ordinal);

        public int CallCount
        {
            get { lock (_gate) return _calls.Count; }
        }

        public ODataReadCall[] Snapshot()
        {
            lock (_gate) return _calls.ToArray();
        }

        public IAsyncEnumerable<JsonElement> ReadEntityAsync(
            EtlEntityDefinition entity, EtlCursor? committedCursor, EtlCursor upperBound, bool full, CancellationToken cancellationToken)
        {
            lock (_gate) _calls.Add(new(entity.EntityCode, committedCursor, upperBound, full));
            return Scripts.TryGetValue(entity.EntityCode, out var script)
                ? script(cancellationToken)
                : DefaultRowsAsync(entity.EntityCode, cancellationToken);
        }

        private async IAsyncEnumerable<JsonElement> DefaultRowsAsync(string entity, [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            if (!Rows.TryGetValue(entity, out var rows)) yield break;
            foreach (var row in rows)
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return row;
            }
            await Task.CompletedTask;
        }

        public Task<bool> CheckAsync(CancellationToken cancellationToken) => Task.FromResult(true);
    }

    /// <summary>An entity stream that throws before the first row.</summary>
    private static async IAsyncEnumerable<JsonElement> ThrowingRowsAsync(Exception error)
    {
        await Task.CompletedTask;
        if (error is not null) throw error;
        yield break;
    }

    /// <summary>An entity stream that yields rows then throws mid-enumeration.</summary>
    private static async IAsyncEnumerable<JsonElement> YieldThenThrowAsync(IEnumerable<JsonElement> rows, Exception error, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        foreach (var row in rows)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return row;
        }
        await Task.CompletedTask;
        throw error;
    }

    private sealed class FakeErp : IErpClient
    {
        private readonly object _gate = new();
        private readonly List<(EtlBatch Batch, byte[] Content)> _uploads = [];
        private readonly List<(Guid RunId, string Payload)> _completions = [];

        public IReadOnlyList<(EtlBatch Batch, byte[] Content)> Uploads
        {
            get { lock (_gate) return _uploads.ToArray(); }
        }

        public (Guid RunId, string Payload)[] Completions
        {
            get { lock (_gate) return _completions.ToArray(); }
        }

        public Exception? UploadError { get; set; }
        public Queue<Exception?> CompletionScript { get; } = new();

        public async Task<BatchAcknowledgement> UploadBatchAsync(EtlBatch batch, Stream content, CancellationToken cancellationToken)
        {
            var memory = new MemoryStream();
            await content.CopyToAsync(memory, cancellationToken).ConfigureAwait(false);
            lock (_gate) _uploads.Add((batch, memory.ToArray()));
            if (UploadError is not null) throw UploadError;
            return new BatchAcknowledgement(batch.BatchId, "acknowledged", batch.RowCount, true, DateTimeOffset.UtcNow);
        }

        public Task CompleteEtlRunRawAsync(Guid runId, string completePayloadJson, CancellationToken cancellationToken)
        {
            Exception? error;
            lock (_gate)
            {
                _completions.Add((runId, completePayloadJson));
                error = CompletionScript.Count > 0 ? CompletionScript.Dequeue() : null;
            }
            return error is null ? Task.CompletedTask : Task.FromException(error);
        }

        public Task<SessionStartResponse> StartSessionAsync(SessionStartRequest request, CancellationToken cancellationToken) =>
            Task.FromResult(new SessionStartResponse(Guid.NewGuid(), DateTimeOffset.UtcNow, true, "1.0.0", 0, false));
        public Task<LeaseResponse> LeaseCommandAsync(LeaseRequest request, CancellationToken cancellationToken) =>
            Task.FromResult(new LeaseResponse(false, null, null, null));
        public Task AcknowledgeReceivedAsync(Guid commandId, CommandReceivedRequest request, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task AcknowledgeResultAsync(Guid commandId, string resultJson, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task CompleteEtlRunAsync(Guid runId, object summary, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task SendHeartbeatAsync(HeartbeatRequest request, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task<RemoteConfigurationResponse?> GetConfigurationAsync(long currentVersion, CancellationToken cancellationToken) =>
            Task.FromResult<RemoteConfigurationResponse?>(null);
    }

    private sealed class ScriptedIdentity(IEnumerable<OnecIdentityFetchResult> script) : IOnecIdentityClient
    {
        private readonly Queue<OnecIdentityFetchResult> _script = new(script);
        private OnecIdentityFetchResult _last = script.LastOrDefault()!;

        public Task<OnecIdentityFetchResult> GetIdentityAsync(CancellationToken cancellationToken)
        {
            lock (_script)
            {
                if (_script.Count > 0) _last = _script.Dequeue();
            }
            return Task.FromResult(_last);
        }
    }

    private sealed class ScriptedDiskProbe : IDiskSpaceProbe
    {
        private readonly Queue<long> _script;
        private long _last;

        public ScriptedDiskProbe(IEnumerable<long> script)
        {
            _script = new(script);
            if (_script.Count == 0) _script.Enqueue(PlentyOfDisk);
            _last = _script.Peek();
        }

        public long GetAvailableFreeBytes(string path)
        {
            lock (_script)
            {
                if (_script.Count > 0) _last = _script.Dequeue();
            }
            return _last;
        }
    }

    private sealed class CountingOnec : IOnecCommandClient
    {
        public Task<OnecExecutionResult> ExecuteAsync(CommandEnvelope command, CancellationToken cancellationToken) =>
            Task.FromResult(new OnecExecutionResult(OnecExecutionKind.Succeeded,
                new(command.CommandId, "succeeded", JsonSerializer.SerializeToElement(new { @ref = "unexpected" }), null, [], 1), 200, null, null));

        public Task<OnecExecutionResult> GetStatusAsync(Guid commandId, CancellationToken cancellationToken) =>
            Task.FromResult(new OnecExecutionResult(OnecExecutionKind.Succeeded,
                new(commandId, "succeeded", JsonSerializer.SerializeToElement(new { @ref = "unexpected" }), null, [], 1), 200, null, null));
    }

    private sealed class FakeOnecHealth : IOnecHealthClient
    {
        public Task<OnecHealthResponse?> CheckAsync(CancellationToken cancellationToken) => Task.FromResult<OnecHealthResponse?>(null);
    }
}
