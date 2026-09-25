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
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace ErpOnecAgent.IntegrationTests;

/// <summary>
/// C1 behaviour suite for the durable ETL pipeline after cutover: the real
/// EtlExtractionWorker/EtlUploadWorker/EtlCompletionWorker (constructed directly,
/// <c>RunOnceAsync</c> driven per pass; the pause scenario runs the real
/// BackgroundService loop with TCS barriers) against a migrated SQLite database and a
/// real FileSpoolStore in a temp directory, with scripted fakes for OData, ERP, the
/// identity endpoint and the disk probe. Administrative commands are driven through
/// CommandExecutionWorker.ProcessAsync exactly like the A07 tests.
/// </summary>
// Shares a collection with EtlC1ReviewFixTests: both set process-wide worker delay statics.
[Collection("EtlWorkerStatics")]
public sealed class EtlPipelineC1Tests : IAsyncLifetime
{
    private static readonly string[] ClientsOnly = ["clients"];
    private static readonly string[] BogusOnly = ["bogus"];
    private static readonly TimeSpan BoundedWait = TimeSpan.FromSeconds(15);
    private static readonly TimeSpan StopTimeout = TimeSpan.FromSeconds(10);
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);
    private const long PlentyOfDisk = 100L * 1024 * 1024 * 1024;

    private readonly SqliteTestDatabase _database = new();
    private readonly List<Harness> _harnesses = [];
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
        foreach (var harness in _harnesses) harness.Dispose();
        await _database.DisposeAsync();
    }

    // ---------- P1: manual baseline end to end ----------

    [Fact]
    public async Task P1_Manual_baseline_job_flows_through_extract_upload_and_complete()
    {
        var h = NewHarness(["clients"]);
        h.OData.Rows["clients"] =
        [
            ClientRow("A1", "2026-09-19T09:50:00Z"),
            ClientRow("A2", "2026-09-19T09:55:00Z"),
            ClientRow("A3", "2026-09-19T09:59:00Z"),
        ];

        // 1. Administrative acceptance through the real command path.
        var command = MakeCommand("start_full_sync", new { entities = ClientsOnly });
        await _store.StoreCommandAsync(command, DateTimeOffset.UtcNow, CancellationToken.None);
        await DriveAdminAsync(h, await ReadyForAsync(command.CommandId));

        var result = await ResultForAsync(command.CommandId);
        Assert.Equal("succeeded", result.GetProperty("status").GetString());
        Assert.True(result.GetProperty("data").GetProperty("accepted").GetBoolean());
        Assert.Equal("bootstrap_full", result.GetProperty("data").GetProperty("mode").GetString());
        var runId = Guid.Parse(result.GetProperty("data").GetProperty("runId").GetString()!);
        var run = runId.ToString("D");
        Assert.Equal("pending", await ScalarStringAsync($"SELECT status FROM etl_jobs WHERE command_id='{command.CommandId:D}'"));
        Assert.Equal("pending", await RunStatusAsync(runId));
        Assert.Equal(0, h.Onec.ExecuteCalls);
        Assert.Equal(0, h.Onec.StatusCalls);

        // 2. Extraction pass: claim -> begin -> OData -> spool -> register -> seal.
        Assert.True(await RunExtractionPassAsync(h));

        Assert.Equal("uploading", await RunStatusAsync(runId));
        Assert.NotNull(await ScalarStringAsync($"SELECT sealed_at_utc FROM etl_runs WHERE run_id='{run}'"));
        Assert.Null(await ScalarStringAsync($"SELECT extraction_claim_id FROM etl_runs WHERE run_id='{run}'"));
        Assert.Equal(1, await ScalarAsync($"SELECT COUNT(*) FROM etl_batches WHERE run_id='{run}' AND status='ready'"));
        Assert.Equal(3, await ScalarAsync($"SELECT row_count FROM etl_batches WHERE run_id='{run}'"));
        var filePath = (await ScalarStringAsync($"SELECT file_path FROM etl_batches WHERE run_id='{run}'"))!;
        Assert.True(File.Exists(filePath));
        Assert.Equal("done", await ScalarStringAsync($"SELECT status FROM etl_run_entities WHERE run_id='{run}' AND entity_name='clients'"));
        var definitionJson = (await ScalarStringAsync($"SELECT entity_definition_json FROM etl_run_entities WHERE run_id='{run}' AND entity_name='clients'"))!;
        Assert.Equal(
            EtlDomainFingerprint.Compute(h.SourceNamespace, "clients", definitionJson, "bootstrap_full"),
            await ScalarStringAsync($"SELECT domain_fingerprint FROM etl_run_entities WHERE run_id='{run}' AND entity_name='clients'"));
        var read = Assert.Single(h.OData.Snapshot());
        Assert.True(read.Full);
        Assert.Null(read.Committed);
        Assert.Equal(1, await ScalarAsync($"SELECT COUNT(*) FROM etl_entity_ownership WHERE owner_run_id='{run}' AND released_at_utc IS NULL"));

        // 3. Upload pass: the exact spool bytes go to ERP once per batch.
        Assert.Equal(1, await RunUploadPassAsync(h));

        Assert.Equal("acknowledged", await ScalarStringAsync($"SELECT status FROM etl_batches WHERE run_id='{run}'"));
        var sent = Assert.Single(h.Erp.Uploads);
        Assert.Equal(runId, sent.Batch.RunId);
        Assert.Equal("clients", sent.Batch.EntityName);
        var onDisk = await File.ReadAllBytesAsync(filePath, CancellationToken.None);
        Assert.True(onDisk.SequenceEqual(sent.Content), "The bytes sent to ERP differ from the spool file.");

        // 4. Completion pass: the stored payload goes out verbatim and the run finalizes.
        Assert.Equal(1, await RunCompletionPassAsync(h));

        Assert.Equal("succeeded", await RunStatusAsync(runId));
        Assert.Equal("finished", await ScalarStringAsync($"SELECT status FROM etl_jobs WHERE run_id='{run}'"));
        Assert.Equal(1, await ScalarAsync("SELECT generation FROM watermarks WHERE entity_name='clients'"));
        Assert.Equal(0, await ScalarAsync($"SELECT COUNT(*) FROM etl_entity_ownership WHERE owner_run_id='{run}' AND released_at_utc IS NULL"));
        Assert.Equal("finalized", await ScalarStringAsync($"SELECT release_reason FROM etl_entity_ownership WHERE owner_run_id='{run}'"));
        var storedPayload = (await ScalarStringAsync($"SELECT complete_payload_json FROM etl_runs WHERE run_id='{run}'"))!;
        var body = Assert.Single(h.Erp.Completions);
        Assert.Equal(runId, body.RunId);
        Assert.Equal(storedPayload, body.Payload);
        Assert.NotNull(h.State.LastEtlSuccessAtUtc);
    }

    // ---------- P2: scheduled incremental ----------

    [Fact]
    public async Task P2_Scheduled_tick_without_baseline_creates_no_run_and_reads_nothing()
    {
        var h = NewHarness(["clients"]);

        // D1 policy: an entity without a committed watermark is excluded from the schedule
        // (ETL_BASELINE_REQUIRED warning) instead of producing a run that blocks.
        Assert.False(await RunExtractionPassAsync(h));

        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM etl_runs"));
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM watermarks"));
        Assert.Empty(h.OData.Snapshot());
    }

    [Fact]
    public async Task P2_Scheduled_incremental_after_baseline_passes_committed_cursor_and_reaches_generation_2()
    {
        var h = NewHarness(["clients"]);

        // P1-style baseline via the administrative command + all three workers.
        h.OData.Rows["clients"] = [ClientRow("A1", "2026-09-19T09:50:00Z"), ClientRow("A2", "2026-09-19T09:59:00Z")];
        var baselineRun = await AcceptFullSyncAsync(h, "clients");
        Assert.True(await RunExtractionPassAsync(h));
        Assert.Equal("uploading", await RunStatusAsync(baselineRun));
        Assert.Equal(1, await RunUploadPassAsync(h));
        Assert.Equal(1, await RunCompletionPassAsync(h));
        Assert.Equal("succeeded", await RunStatusAsync(baselineRun));
        Assert.Equal(1, await ScalarAsync("SELECT generation FROM watermarks WHERE entity_name='clients'"));
        var committed = JsonSerializer.Deserialize<EtlCursor>(
            (await ScalarStringAsync("SELECT committed_cursor_json FROM watermarks WHERE entity_name='clients'"))!, JsonOptions);

        // A fresh worker ticks the schedule again: the new incremental run reads from
        // the committed cursor (full=false) and finalizes generation 2.
        h.OData.Rows["clients"] = [ClientRow("A3", "2026-09-20T09:59:00Z")];
        Assert.True(await RunExtractionPassAsync(h));

        var incrementalRunText = (await ScalarStringAsync(
            $"SELECT run_id FROM etl_runs WHERE schedule_key='incremental' AND run_id<>'{baselineRun:D}' AND status='uploading'"))!;
        var incrementalRun = Guid.Parse(incrementalRunText);
        var incrementalRead = h.OData.Snapshot()[^1];
        Assert.Equal("clients", incrementalRead.EntityCode);
        Assert.False(incrementalRead.Full);
        Assert.Equal(committed, incrementalRead.Committed);

        Assert.Equal(1, await RunUploadPassAsync(h));
        Assert.Equal(1, await RunCompletionPassAsync(h));

        Assert.Equal("succeeded", await RunStatusAsync(incrementalRun));
        Assert.Equal(2, await ScalarAsync("SELECT generation FROM watermarks WHERE entity_name='clients'"));
        Assert.Equal(1, await ScalarAsync("SELECT COUNT(*) FROM watermarks WHERE entity_name='clients'"));
    }

    // ---------- P3: source identity ----------

    [Fact]
    public async Task P3_Identity_mismatch_never_claims_runs_or_reads_odata()
    {
        var h = NewHarness(["clients"], identityScript: (db, epoch) => [Match(Guid.NewGuid(), epoch)]);
        var runId = await AcceptFullSyncAsync(h, "clients");
        var run = runId.ToString("D");

        Assert.False(await RunExtractionPassAsync(h));

        Assert.Equal("pending", await RunStatusAsync(runId));
        Assert.Equal("pending", await ScalarStringAsync($"SELECT status FROM etl_jobs WHERE run_id='{run}'"));
        Assert.Null(await ScalarStringAsync($"SELECT extraction_claim_id FROM etl_runs WHERE run_id='{run}'"));
        Assert.Equal(0, await ScalarAsync($"SELECT COUNT(*) FROM etl_run_entities WHERE run_id='{run}'"));
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM etl_entity_ownership"));
        // The identity gate precedes scheduling and dispatch: no other run exists.
        Assert.Equal(1, await ScalarAsync("SELECT COUNT(*) FROM etl_runs"));
        Assert.Equal(0, h.OData.CallCount);
        Assert.Equal(1, h.Identity.Calls);
    }

    [Fact]
    public async Task P3_After_check_unavailable_retries_then_match_seals_the_run()
    {
        var previousDelay = EtlExtractionWorker.AfterCheckRetryDelay;
        EtlExtractionWorker.AfterCheckRetryDelay = TimeSpan.FromMilliseconds(1);
        try
        {
            var h = NewHarness(["clients"], identityScript: (db, epoch) =>
            [
                Match(db, epoch),
                OnecIdentityFetchResult.Unavailable(null, "transient timeout"),
                OnecIdentityFetchResult.Unavailable(null, "transient timeout"),
                Match(db, epoch),
            ]);
            h.OData.Rows["clients"] = [ClientRow("A1", "2026-09-19T09:59:00Z")];
            var runId = await AcceptFullSyncAsync(h, "clients");

            Assert.True(await RunExtractionPassAsync(h));

            Assert.Equal("uploading", await RunStatusAsync(runId));
            Assert.Equal(4, h.Identity.Calls);
        }
        finally
        {
            EtlExtractionWorker.AfterCheckRetryDelay = previousDelay;
        }
    }

    [Fact]
    public async Task P3_After_check_epoch_change_blocks_the_run_without_committing_watermarks()
    {
        var previousDelay = EtlExtractionWorker.AfterCheckRetryDelay;
        EtlExtractionWorker.AfterCheckRetryDelay = TimeSpan.FromMilliseconds(1);
        try
        {
            var h = NewHarness(["clients"], identityScript: (db, epoch) => [Match(db, epoch), Match(db, Guid.NewGuid())]);
            h.OData.Rows["clients"] = [ClientRow("A1", "2026-09-19T09:59:00Z")];
            var runId = await AcceptFullSyncAsync(h, "clients");
            var run = runId.ToString("D");

            Assert.True(await RunExtractionPassAsync(h));

            Assert.Equal("blocked", await RunStatusAsync(runId));
            Assert.Equal("SOURCE_IDENTITY_CHANGED", await ScalarStringAsync($"SELECT finalize_conflict_code FROM etl_runs WHERE run_id='{run}'"));
            Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM watermarks"));
            Assert.Equal("dead_letter", await ScalarStringAsync($"SELECT status FROM etl_batches WHERE run_id='{run}'"));
            Assert.Equal("RUN_BLOCKED", await ScalarStringAsync($"SELECT quarantine_code FROM etl_batches WHERE run_id='{run}'"));
            // Ownership evidence is retained, never released, on a blocked run.
            Assert.Equal(1, await ScalarAsync($"SELECT COUNT(*) FROM etl_entity_ownership WHERE owner_run_id='{run}' AND released_at_utc IS NULL"));
            Assert.Equal(2, h.Identity.Calls);
        }
        finally
        {
            EtlExtractionWorker.AfterCheckRetryDelay = previousDelay;
        }
    }

    // ---------- P4: upload uncertainty ----------

    [Fact]
    public async Task P4_Upload_exception_dead_letters_the_batch_and_never_resends()
    {
        var h = NewHarness(["clients"]);
        h.OData.Rows["clients"] = [ClientRow("A1", "2026-09-19T09:59:00Z")];
        var runId = await ExtractBaselineAsync(h, "clients");
        var run = runId.ToString("D");
        var batch = (await ScalarStringAsync($"SELECT batch_id FROM etl_batches WHERE run_id='{run}'"))!;
        h.Erp.UploadError = new HttpRequestException("connection reset mid-response");

        Assert.Equal(1, await RunUploadPassAsync(h));

        Assert.Equal("dead_letter", await ScalarStringAsync($"SELECT status FROM etl_batches WHERE batch_id='{batch}'"));
        Assert.Equal("UPLOAD_OUTCOME_UNKNOWN", await ScalarStringAsync($"SELECT quarantine_code FROM etl_batches WHERE batch_id='{batch}'"));
        Assert.Equal("unknown", await ScalarStringAsync($"SELECT outcome FROM etl_batch_send_attempts WHERE batch_id='{batch}'"));
        Assert.Equal("blocked", await RunStatusAsync(runId));
        Assert.Equal("UPLOAD_OUTCOME_UNKNOWN", await ScalarStringAsync($"SELECT finalize_conflict_code FROM etl_runs WHERE run_id='{run}'"));
        Assert.Equal("blocked", await ScalarStringAsync($"SELECT status FROM etl_jobs WHERE run_id='{run}'"));
        Assert.Single(h.Erp.Uploads);

        // An invoked-but-unanswered send is never replayed: further passes do not call ERP.
        Assert.Equal(0, await RunUploadPassAsync(h));
        Assert.Single(h.Erp.Uploads);
    }

    // ---------- P5: upload precheck ----------

    [Fact]
    public async Task P5_Spool_precheck_failure_retries_then_exhausts_the_bound()
    {
        var h = NewHarness(["clients"], maxBatchUploadAttempts: 2);
        h.OData.Rows["clients"] = [ClientRow("A1", "2026-09-19T09:59:00Z")];
        var runId = await ExtractBaselineAsync(h, "clients");
        var run = runId.ToString("D");
        var batch = (await ScalarStringAsync($"SELECT batch_id FROM etl_batches WHERE run_id='{run}'"))!;
        var filePath = (await ScalarStringAsync($"SELECT file_path FROM etl_batches WHERE batch_id='{batch}'"))!;
        await File.WriteAllBytesAsync(filePath, [1, 2, 3], CancellationToken.None);
        using var uploader = h.NewUploadWorker();

        Assert.Equal(1, await uploader.RunOnceAsync(CancellationToken.None));

        Assert.Equal("retry_waiting", await ScalarStringAsync($"SELECT status FROM etl_batches WHERE batch_id='{batch}'"));
        Assert.Equal("precheck_failed", await ScalarStringAsync($"SELECT outcome FROM etl_batch_send_attempts WHERE batch_id='{batch}'"));
        Assert.Empty(h.Erp.Uploads);

        await ExecuteSqlAsync($"UPDATE etl_batches SET next_attempt_at_utc=$past WHERE batch_id='{batch}';", ("$past", Now(DateTimeOffset.UtcNow.AddMinutes(-10))));

        Assert.Equal(1, await uploader.RunOnceAsync(CancellationToken.None));

        Assert.Equal("dead_letter", await ScalarStringAsync($"SELECT status FROM etl_batches WHERE batch_id='{batch}'"));
        Assert.Equal("UPLOAD_ATTEMPTS_EXHAUSTED", await ScalarStringAsync($"SELECT quarantine_code FROM etl_batches WHERE batch_id='{batch}'"));
        Assert.Equal("blocked", await RunStatusAsync(runId));
        Assert.Equal("UPLOAD_ATTEMPTS_EXHAUSTED", await ScalarStringAsync($"SELECT finalize_conflict_code FROM etl_runs WHERE run_id='{run}'"));
        Assert.Equal(2, await ScalarAsync($"SELECT COUNT(*) FROM etl_batch_send_attempts WHERE batch_id='{batch}'"));
        Assert.Empty(h.Erp.Uploads);
    }

    // ---------- P6: completion retry byte identity ----------

    [Fact]
    public async Task P6_Completion_retry_replays_the_stored_payload_byte_identically()
    {
        var h = NewHarness(["clients"]);
        h.OData.Rows["clients"] = [ClientRow("A1", "2026-09-19T09:59:00Z")];
        var runId = await ExtractBaselineAsync(h, "clients");
        var run = runId.ToString("D");
        Assert.Equal(1, await RunUploadPassAsync(h));
        h.Erp.CompletionScript.Enqueue(new HttpRequestException("completion response lost"));
        using var completer = h.NewCompletionWorker();

        Assert.Equal(1, await completer.RunOnceAsync(CancellationToken.None));

        Assert.Equal("completing", await RunStatusAsync(runId));
        Assert.Null(await ScalarStringAsync($"SELECT completion_claim_id FROM etl_runs WHERE run_id='{run}'"));
        Assert.NotNull(await ScalarStringAsync($"SELECT next_completion_attempt_at_utc FROM etl_runs WHERE run_id='{run}'"));
        var storedPayload = (await ScalarStringAsync($"SELECT complete_payload_json FROM etl_runs WHERE run_id='{run}'"))!;

        await ExecuteSqlAsync($"UPDATE etl_runs SET next_completion_attempt_at_utc=$past WHERE run_id='{run}';", ("$past", Now(DateTimeOffset.UtcNow.AddMinutes(-10))));

        Assert.Equal(1, await completer.RunOnceAsync(CancellationToken.None));

        Assert.Equal("succeeded", await RunStatusAsync(runId));
        Assert.Equal("finished", await ScalarStringAsync($"SELECT status FROM etl_jobs WHERE run_id='{run}'"));
        Assert.Equal(2, h.Erp.Completions.Length);
        Assert.Equal(storedPayload, h.Erp.Completions[0].Payload);
        Assert.Equal(storedPayload, h.Erp.Completions[1].Payload);
        Assert.Equal(System.Text.Encoding.UTF8.GetBytes(storedPayload), System.Text.Encoding.UTF8.GetBytes(h.Erp.Completions[1].Payload));
    }

    // ---------- P7: disk admission ----------

    [Fact]
    public async Task P7_Low_disk_admission_refuses_the_pass_before_any_claim()
    {
        var h = NewHarness(["clients"], diskScript: [0L]);
        var runId = await AcceptFullSyncAsync(h, "clients");
        var run = runId.ToString("D");

        Assert.False(await RunExtractionPassAsync(h));

        Assert.Equal("pending", await RunStatusAsync(runId));
        Assert.Equal("pending", await ScalarStringAsync($"SELECT status FROM etl_jobs WHERE run_id='{run}'"));
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM etl_entity_ownership"));
        Assert.Equal(0, await ScalarAsync($"SELECT COUNT(*) FROM etl_run_entities WHERE run_id='{run}'"));
        Assert.Equal(0, h.OData.CallCount);
        // The disk gate runs before the identity check.
        Assert.Equal(0, h.Identity.Calls);
    }

    [Fact]
    public async Task P7_Disk_reserve_failure_mid_write_blocks_the_run_without_a_ready_file()
    {
        // Pass the worker admission probe and the spool pre-create probe, then drop below
        // the reserve inside the write (a single row above the 4 MiB check interval).
        var h = NewHarness(["clients"], diskScript: [PlentyOfDisk, PlentyOfDisk, 0L]);
        h.OData.Rows["clients"] = [ClientRow("B1", "2026-09-19T09:59:00Z", blob: new string('x', 5 * 1024 * 1024))];
        var runId = await AcceptFullSyncAsync(h, "clients");
        var run = runId.ToString("D");

        Assert.True(await RunExtractionPassAsync(h));

        Assert.Equal("blocked", await RunStatusAsync(runId));
        Assert.Equal("DISK_RESERVE", await ScalarStringAsync($"SELECT finalize_conflict_code FROM etl_runs WHERE run_id='{run}'"));
        Assert.Equal(0, await ScalarAsync($"SELECT COUNT(*) FROM etl_batches WHERE run_id='{run}'"));
        Assert.Empty(Directory.EnumerateFiles(Path.Combine(h.SpoolRoot, "ready")));
        Assert.Empty(Directory.EnumerateFiles(Path.Combine(h.SpoolRoot, "creating"), "*.tmp"));
    }

    // ---------- P8: pause at the entity boundary ----------

    [Fact]
    public async Task P8_Local_pause_at_the_entity_boundary_holds_the_claim_until_resume()
    {
        var h = NewHarness(["clients", "orders"]);
        h.OData.Rows["clients"] = [ClientRow("A1", "2026-09-19T09:50:00Z"), ClientRow("A2", "2026-09-19T09:59:00Z")];
        h.OData.Rows["orders"] = [OrderRow("O1", "2026-09-19T09:58:00Z")];
        var runId = await AcceptFullSyncAsync(h, "clients", "orders");
        var run = runId.ToString("D");

        var clientsRead = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseClients = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var ordersRead = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        h.OData.BeforeEntityReadAsync = async code =>
        {
            if (code == "clients")
            {
                clientsRead.TrySetResult(true);
                await releaseClients.Task.ConfigureAwait(false);
            }
            else if (code == "orders")
            {
                ordersRead.TrySetResult(true);
            }
        };

        using var worker = h.NewExtractionWorker();
        await worker.StartAsync(CancellationToken.None);
        try
        {
            await clientsRead.Task.WaitAsync(BoundedWait);
            // Set the durable local pause while the first entity read is in flight; it
            // takes effect at the entity boundary, deterministically before 'orders'.
            await h.Pause.SetAsync(true, CancellationToken.None);
            releaseClients.TrySetResult(true);

            await WaitUntilAsync(
                async () => await ScalarAsync($"SELECT COUNT(*) FROM etl_run_entities WHERE run_id='{run}' AND status='done'") == 1,
                "first entity to complete extraction");

            // The claim is held in-process: the run keeps running, the second entity is
            // never begun while paused.
            Assert.Equal("running", await RunStatusAsync(runId));
            Assert.NotNull(await ScalarStringAsync($"SELECT extraction_claim_id FROM etl_runs WHERE run_id='{run}'"));
            var beganWhilePaused = await Task.WhenAny(ordersRead.Task, Task.Delay(TimeSpan.FromMilliseconds(750))) == ordersRead.Task;
            Assert.False(beganWhilePaused, "The worker began the second entity while ETL was locally paused.");
            Assert.Equal(1, await ScalarAsync($"SELECT COUNT(*) FROM etl_run_entities WHERE run_id='{run}'"));
            Assert.Equal(1, h.OData.CallCount);

            await h.Pause.SetAsync(false, CancellationToken.None);

            await WaitUntilAsync(async () => "uploading" == await RunStatusAsync(runId), "run to seal after resume");
            await ordersRead.Task.WaitAsync(BoundedWait);
            Assert.Equal(2, h.OData.CallCount);
            Assert.Equal(2, await ScalarAsync($"SELECT COUNT(*) FROM etl_run_entities WHERE run_id='{run}' AND status='done'"));
        }
        finally
        {
            using var stop = new CancellationTokenSource(StopTimeout);
            await worker.StopAsync(stop.Token);
        }
    }

    // ---------- P9: startup recovery ----------

    [Fact]
    public async Task P9_Legacy_uploading_run_without_bindings_is_blocked_legacy_unresolved()
    {
        var h = NewHarness([]);
        var (runId, batchId) = await InsertLegacyRunAsync("uploading", "ready");
        var run = runId.ToString("D");

        await StartupRecoveryAsync(h.Spool);

        Assert.Equal("blocked", await RunStatusAsync(runId));
        Assert.Equal("LEGACY_UNRESOLVED", await ScalarStringAsync($"SELECT finalize_conflict_code FROM etl_runs WHERE run_id='{run}'"));
        Assert.Equal("dead_letter", await ScalarStringAsync($"SELECT status FROM etl_batches WHERE batch_id='{batchId:D}'"));
        Assert.Equal("RUN_BLOCKED", await ScalarStringAsync($"SELECT quarantine_code FROM etl_batches WHERE batch_id='{batchId:D}'"));
    }

    [Fact]
    public async Task P9_Legacy_running_run_without_bindings_is_blocked_legacy_unresolved()
    {
        var h = NewHarness([]);
        var (runId, batchId) = await InsertLegacyRunAsync("running", "ready");
        var run = runId.ToString("D");

        await StartupRecoveryAsync(h.Spool);

        // C1 design ("Upgrade of an existing database with legacy work"): a legacy run in
        // any in-flight status without ownership bindings is blocked LEGACY_UNRESOLVED.
        Assert.Equal("blocked", await RunStatusAsync(runId));
        Assert.Equal("LEGACY_UNRESOLVED", await ScalarStringAsync($"SELECT finalize_conflict_code FROM etl_runs WHERE run_id='{run}'"));
        Assert.Equal("dead_letter", await ScalarStringAsync($"SELECT status FROM etl_batches WHERE batch_id='{batchId:D}'"));
        Assert.Equal("RUN_BLOCKED", await ScalarStringAsync($"SELECT quarantine_code FROM etl_batches WHERE batch_id='{batchId:D}'"));
    }

    [Fact]
    public async Task P9_Legacy_run_with_an_in_flight_upload_is_legacy_unresolved_and_the_batch_outcome_unknown()
    {
        var h = NewHarness([]);
        var (runId, batchId) = await InsertLegacyRunAsync("uploading", "uploading");

        await StartupRecoveryAsync(h.Spool);

        Assert.Equal("blocked", await RunStatusAsync(runId));
        Assert.Equal("LEGACY_UNRESOLVED", await ScalarStringAsync($"SELECT finalize_conflict_code FROM etl_runs WHERE run_id='{runId:D}'"));
        Assert.Equal("dead_letter", await ScalarStringAsync($"SELECT status FROM etl_batches WHERE batch_id='{batchId:D}'"));
        Assert.Equal("UPLOAD_OUTCOME_UNKNOWN", await ScalarStringAsync($"SELECT quarantine_code FROM etl_batches WHERE batch_id='{batchId:D}'"));
    }

    [Fact]
    public async Task P9_Legacy_completing_run_is_legacy_unresolved()
    {
        var h = NewHarness([]);
        var (runId, batchId) = await InsertLegacyRunAsync("completing", "acknowledged");

        await StartupRecoveryAsync(h.Spool);

        Assert.Equal("blocked", await RunStatusAsync(runId));
        Assert.Equal("LEGACY_UNRESOLVED", await ScalarStringAsync($"SELECT finalize_conflict_code FROM etl_runs WHERE run_id='{runId:D}'"));
        Assert.Equal("acknowledged", await ScalarStringAsync($"SELECT status FROM etl_batches WHERE batch_id='{batchId:D}'"));
    }

    [Fact]
    public async Task P9_Startup_recovery_quarantines_unreferenced_ready_files()
    {
        var h = NewHarness([]);
        var ready = Path.Combine(h.SpoolRoot, "ready");
        Directory.CreateDirectory(ready);
        var orphan = Path.Combine(ready, $"orphan-{Guid.NewGuid():N}.ndjson.gz");
        await File.WriteAllTextAsync(orphan, "not referenced by any batch row", CancellationToken.None);

        await StartupRecoveryAsync(h.Spool);

        Assert.False(File.Exists(orphan));
        var quarantined = Directory.GetFiles(Path.Combine(h.SpoolRoot, "quarantine"));
        Assert.Single(quarantined);
        Assert.Contains(Path.GetFileName(orphan), Path.GetFileName(quarantined[0]));
    }

    [Fact]
    public async Task P9_Startup_never_resets_an_uploading_batch_and_quarantines_it()
    {
        var h = NewHarness(["clients"]);
        h.OData.Rows["clients"] = [ClientRow("A1", "2026-09-19T09:59:00Z")];
        var runId = await ExtractBaselineAsync(h, "clients");
        var run = runId.ToString("D");
        var batch = (await ScalarStringAsync($"SELECT batch_id FROM etl_batches WHERE run_id='{run}'"))!;
        var claim = Assert.IsType<EtlBatchUploadClaimOutcome.Claimed>(
            await _store.TryClaimBatchUploadAsync(Guid.Parse(batch), "dead-owner", DateTimeOffset.UtcNow, 5, CancellationToken.None));

        // The bootstrap order, step by step (BootstrapService.StartAsync).
        await _store.RecoverAsync(CancellationToken.None);

        // C1 removed the legacy uploading->ready reset: the batch stays 'uploading'.
        Assert.Equal("uploading", await ScalarStringAsync($"SELECT status FROM etl_batches WHERE batch_id='{batch}'"));

        await _store.BlockLegacyEtlRunsAsync(CancellationToken.None);
        await _store.RecoverInterruptedEtlRunsAsync(CancellationToken.None);
        await h.Spool.QuarantineTemporaryFilesAsync(CancellationToken.None);
        await h.Spool.QuarantineUnreferencedReadyFilesAsync(await _store.GetReferencedBatchFilePathsAsync(CancellationToken.None), CancellationToken.None);

        Assert.Equal("orphaned", await ScalarStringAsync($"SELECT outcome FROM etl_batch_send_attempts WHERE attempt_id='{claim.Claim.AttemptId:D}'"));
        Assert.Equal("dead_letter", await ScalarStringAsync($"SELECT status FROM etl_batches WHERE batch_id='{batch}'"));
        Assert.Equal("UPLOAD_OUTCOME_UNKNOWN", await ScalarStringAsync($"SELECT quarantine_code FROM etl_batches WHERE batch_id='{batch}'"));
        Assert.Equal("blocked", await RunStatusAsync(runId));
        Assert.Equal("UPLOAD_OUTCOME_UNKNOWN", await ScalarStringAsync($"SELECT finalize_conflict_code FROM etl_runs WHERE run_id='{run}'"));
    }

    // ---------- P10: administrative commands ----------

    [Fact]
    public async Task P10_Start_full_sync_with_an_unknown_entity_is_business_error_entity_unknown()
    {
        var h = NewHarness(["clients"]);
        var command = MakeCommand("start_full_sync", new { entities = BogusOnly });
        await _store.StoreCommandAsync(command, DateTimeOffset.UtcNow, CancellationToken.None);

        await DriveAdminAsync(h, await ReadyForAsync(command.CommandId));

        var result = await ResultForAsync(command.CommandId);
        Assert.Equal("business_error", result.GetProperty("status").GetString());
        Assert.Equal("ENTITY_UNKNOWN", result.GetProperty("error").GetProperty("code").GetString());
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM etl_jobs"));
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM etl_runs"));
        Assert.Equal(0, h.Onec.ExecuteCalls);
        Assert.Equal(0, h.Onec.StatusCalls);
    }

    [Fact]
    public async Task P10_Reconcile_keys_is_business_error_etl_mode_unsupported()
    {
        var h = NewHarness(["clients"]);
        var command = MakeCommand("reconcile_keys", new { });
        await _store.StoreCommandAsync(command, DateTimeOffset.UtcNow, CancellationToken.None);

        await DriveAdminAsync(h, await ReadyForAsync(command.CommandId));

        var result = await ResultForAsync(command.CommandId);
        Assert.Equal("business_error", result.GetProperty("status").GetString());
        Assert.Equal("ETL_MODE_UNSUPPORTED", result.GetProperty("error").GetProperty("code").GetString());
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM etl_jobs"));
        Assert.Equal(0, h.Onec.ExecuteCalls);
        Assert.Equal(0, h.Onec.StatusCalls);
    }

    [Fact]
    public async Task P10_Start_full_sync_accepts_a_durable_job_and_never_posts_to_onec()
    {
        var h = NewHarness(["clients"]);
        var command = MakeCommand("start_full_sync", new { });
        await _store.StoreCommandAsync(command, DateTimeOffset.UtcNow, CancellationToken.None);

        await DriveAdminAsync(h, await ReadyForAsync(command.CommandId));

        var result = await ResultForAsync(command.CommandId);
        Assert.Equal("succeeded", result.GetProperty("status").GetString());
        Assert.True(result.GetProperty("data").GetProperty("accepted").GetBoolean());
        Assert.Equal("bootstrap_full", result.GetProperty("data").GetProperty("mode").GetString());
        var jobId = (await ScalarStringAsync($"SELECT job_id FROM etl_jobs WHERE command_id='{command.CommandId:D}'"))!;
        Assert.Equal("pending", await ScalarStringAsync($"SELECT status FROM etl_jobs WHERE job_id='{jobId}'"));
        Assert.Equal("bootstrap_full", await ScalarStringAsync($"SELECT mode FROM etl_jobs WHERE job_id='{jobId}'"));
        var run = (await ScalarStringAsync($"SELECT run_id FROM etl_jobs WHERE job_id='{jobId}'"))!;
        Assert.Equal("pending", await ScalarStringAsync($"SELECT status FROM etl_runs WHERE run_id='{run}'"));
        Assert.Equal(0, h.Onec.ExecuteCalls);
        Assert.Equal(0, h.Onec.StatusCalls);
    }

    // ---------- P11: fenced legacy surface ----------

    [Fact]
    public void P11_Legacy_etl_store_members_and_worker_types_are_fenced()
    {
        // The pre-cutover writers/readers are off IAgentStore entirely.
        string[] fenced =
        [
            "CreateEtlRunAsync", "RegisterBatchAsync", "MarkEtlRunExtractedAsync", "GetPendingBatchesAsync",
            "MarkBatchRetryAsync", "AcknowledgeBatchAsync", "GetRunsReadyToCompleteAsync", "CommitWatermarkAsync",
            "CompleteEtlRunAsync"
        ];
        var declared = typeof(IAgentStore).GetMethods().Select(static method => method.Name).ToHashSet(StringComparer.Ordinal);
        foreach (var name in fenced)
            Assert.False(declared.Contains(name), $"IAgentStore still declares {name}");

        // Nothing in the Service assembly may hold the legacy store or the removed
        // pipeline types, which must not exist there at all.
        var removed = new HashSet<string>(StringComparer.Ordinal) { "EtlTrigger", "OnecEtlWorker", "EtlBatchUploadWorker" };
        var assembly = typeof(EtlExtractionWorker).Assembly;
        Assert.Equal("ErpOnecAgent", assembly.GetName().Name);
        foreach (var type in assembly.GetTypes())
        {
            Assert.False(removed.Contains(type.Name), $"Removed type {type.FullName} still exists in ErpOnecAgent.Service");
            foreach (var constructor in type.GetConstructors(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance))
                Assert.DoesNotContain(constructor.GetParameters(), parameter => IsForbiddenDependency(parameter.ParameterType));
            foreach (var field in type.GetFields(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Static))
                Assert.False(IsForbiddenDependency(field.FieldType), $"Type {type.FullName} holds forbidden dependency {field.FieldType.FullName}");
        }

        static bool IsForbiddenDependency(Type type) =>
            type == typeof(ILegacyEtlStore) || type.Name is "EtlTrigger" or "OnecEtlWorker" or "EtlBatchUploadWorker";
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

    /// <summary>Accepts a full-sync job and runs one extraction pass to a sealed run.</summary>
    private async Task<Guid> ExtractBaselineAsync(Harness h, params string[] entities)
    {
        var runId = await AcceptFullSyncAsync(h, entities);
        Assert.True(await RunExtractionPassAsync(h));
        Assert.Equal("uploading", await RunStatusAsync(runId));
        return runId;
    }

    /// <summary>One extraction pass on a fresh worker — a fresh instance always takes a schedule tick (the loop field stays MinValue outside ExecuteAsync).</summary>
    private static async Task<bool> RunExtractionPassAsync(Harness h)
    {
        using var worker = h.NewExtractionWorker();
        return await worker.RunOnceAsync(CancellationToken.None);
    }

    /// <summary>One upload pass; returns the number of due batches the pass attempted.</summary>
    private static async Task<int> RunUploadPassAsync(Harness h)
    {
        using var worker = h.NewUploadWorker();
        return await worker.RunOnceAsync(CancellationToken.None);
    }

    /// <summary>One completion pass; returns the number of due runs the pass attempted.</summary>
    private static async Task<int> RunCompletionPassAsync(Harness h)
    {
        using var worker = h.NewCompletionWorker();
        return await worker.RunOnceAsync(CancellationToken.None);
    }

    /// <summary>Runs the worker's claim+route pass for one stored command.</summary>
    private static async Task DriveAdminAsync(Harness h, StoredCommand command)
    {
        using var worker = h.NewCommandWorker();
        await ProcessAdminAsync(worker, command);
    }

    /// <summary>The BootstrapService recovery order: generic recovery, then the durable-ETL fence, then spool reconciliation.</summary>
    private async Task StartupRecoveryAsync(FileSpoolStore spool)
    {
        await _store.RecoverAsync(CancellationToken.None);
        await _store.BlockLegacyEtlRunsAsync(CancellationToken.None);
        await _store.RecoverInterruptedEtlRunsAsync(CancellationToken.None);
        await spool.QuarantineTemporaryFilesAsync(CancellationToken.None);
        await spool.QuarantineUnreferencedReadyFilesAsync(await _store.GetReferencedBatchFilePathsAsync(CancellationToken.None), CancellationToken.None);
        await _store.BlockRunsWithMissingSpoolFilesAsync(File.Exists, CancellationToken.None);
    }

    /// <summary>A pre-cutover run row: a status with no ownership bindings, plus one batch.</summary>
    private async Task<(Guid RunId, Guid BatchId)> InsertLegacyRunAsync(string runStatus, string batchStatus)
    {
        var runId = Guid.NewGuid();
        var batchId = Guid.NewGuid();
        var filePath = Path.Combine(_database.Root, "spool", "ready", $"legacy-{batchId:N}.ndjson.gz");
        await ExecuteSqlAsync(
            "INSERT INTO etl_runs(run_id,mode,requested_entities_json,status,started_at_utc,configuration_version,created_at_utc,updated_at_utc,row_version) VALUES($run,'bootstrap_full','[\"clients\"]',$status,$now,1,$now,$now,1);",
            ("$run", runId.ToString("D")), ("$status", runStatus), ("$now", Now(DateTimeOffset.UtcNow)));
        await ExecuteSqlAsync(
            "INSERT INTO etl_batches(batch_id,run_id,entity_name,schema_version,file_path,status,row_count,sha256,compressed_size,uncompressed_size,created_at_utc) VALUES($b,$run,'clients',1,$path,$status,3,'h',10,10,$now);",
            ("$b", batchId.ToString("D")), ("$run", runId.ToString("D")), ("$path", filePath), ("$status", batchStatus), ("$now", Now(DateTimeOffset.UtcNow)));
        return (runId, batchId);
    }

    // ---------- harness ----------

    private Harness NewHarness(
        string[] entities,
        Func<Guid, Guid, IEnumerable<OnecIdentityFetchResult>>? identityScript = null,
        IEnumerable<long>? diskScript = null,
        int maxBatchUploadAttempts = 5,
        int maxRunCompletionAttempts = 20)
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
            MaxBatchUploadAttempts = maxBatchUploadAttempts,
            MaxRunCompletionAttempts = maxRunCompletionAttempts,
            TargetBatchUncompressedBytes = 1024 * 1024,
            Entities = entities.Select(code => Catalog().Single(e => e.EntityCode == code)).ToArray()
        };
        var storage = new StorageOptions
        {
            MinimumReservedBytesForCommands = 1024,
            MaxBatchCompressedBytes = 50L * 1024 * 1024,
            MaxSpoolBytes = 1024L * 1024 * 1024
        };
        var spoolRoot = Path.Combine(_database.Root, "spool");
        var identity = new ScriptedIdentity(identityScript?.Invoke(databaseId, exportEpoch) ?? [Match(databaseId, exportEpoch)]);
        var disk = new ScriptedDiskProbe(diskScript ?? [PlentyOfDisk]);
        var state = ReadyState();
        var harness = new Harness
        {
            Store = _store,
            SpoolRoot = spoolRoot,
            Spool = new FileSpoolStore(spoolRoot, storage.MaxBatchCompressedBytes, storage.MaxSpoolBytes, disk, storage.MinimumReservedBytesForCommands),
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
            Agent = Options.Create(new AgentOptions { AgentId = $"c1-{Guid.NewGuid():N}", SiteId = "c1-site", DataDirectory = _database.Root }),
            Etl = Options.Create(etl),
            Storage = Options.Create(storage),
            OnecOptions = Options.Create(onec),
            IdentityGuard = new SourceIdentityGuard(identity, Options.Create(onec)),
            SourceNamespace = $"1c-identity:v1:{databaseId:D}:{exportEpoch:D}:test"
        };
        _harnesses.Add(harness);
        return harness;
    }

    private static readonly string[] AdministrativeTypes =
    [
        "start_full_sync", "reload_entity", "reconcile_keys", "reconcile_totals",
        "pause_etl", "resume_etl", "collect_diagnostics", "rotate_certificate_hint", "run_connectivity_test"
    ];

    private static AgentRuntimeState ReadyState(AgentMode mode = AgentMode.Normal, bool localPaused = false)
    {
        var state = new AgentRuntimeState();
        state.RestoreLocalEtlPause(localPaused);
        state.SetRemoteMode(mode);
        state.CompleteBootstrap();
        return state;
    }

    private static OnecIdentityFetchResult Match(Guid databaseId, Guid exportEpoch) =>
        OnecIdentityFetchResult.Ready(new OnecSourceIdentity(1, "ready", "whole-infobase", databaseId, exportEpoch, "test"));

    private static EtlEntityDefinition[] Catalog() =>
    [
        new("clients", "Catalog_Clients", "Ref_Key", "UpdatedAt", "DeletionMark", ["Ref_Key", "UpdatedAt", "DeletionMark"], "incremental", 500, 10),
        new("orders", "Document_Orders", "Ref_Key", "Date", "Posted", ["Ref_Key", "Date", "Posted"], "incremental", 500, 10),
    ];

    private static JsonElement ClientRow(string id, string updatedAt, bool deleted = false, string? blob = null)
    {
        var extra = blob is null ? "" : $",\"Blob\":\"{blob}\"";
        return JsonDocument.Parse($"{{\"Ref_Key\":\"{id}\",\"UpdatedAt\":\"{updatedAt}\",\"DeletionMark\":{(deleted ? "true" : "false")}{extra}}}").RootElement;
    }

    private static JsonElement OrderRow(string id, string date, bool posted = false) =>
        JsonDocument.Parse($"{{\"Ref_Key\":\"{id}\",\"Date\":\"{date}\",\"Posted\":{(posted ? "true" : "false")}}}").RootElement;

    private static CommandEnvelope MakeCommand(string commandType, object payload)
    {
        var body = JsonSerializer.SerializeToElement(payload);
        return new(Guid.NewGuid(), commandType, 1, 100, $"c1:{Guid.NewGuid():N}", null, DateTimeOffset.UtcNow, null, null, null, PayloadHasher.Compute(body), body);
    }

    private static async Task ProcessAdminAsync(CommandExecutionWorker worker, StoredCommand command)
    {
        var method = typeof(CommandExecutionWorker).GetMethod("ProcessAsync", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("Command execution method was not found.");
        await (Task)(method.Invoke(worker, [command, CancellationToken.None])
            ?? throw new InvalidOperationException("Command execution method returned no task."));
    }

    private async Task<StoredCommand> ReadyForAsync(Guid commandId) =>
        (await _store.GetReadyCommandsAsync(10, DateTimeOffset.UtcNow.AddDays(1), CancellationToken.None)).Single(command => command.Envelope.CommandId == commandId);

    private async Task<JsonElement> ResultForAsync(Guid commandId)
    {
        var result = (await _store.GetPendingResultsAsync(10, DateTimeOffset.UtcNow, CancellationToken.None)).Single(item => item.CommandId == commandId);
        using var document = JsonDocument.Parse(result.PayloadJson);
        return document.RootElement.Clone();
    }

    private static async Task WaitUntilAsync(Func<Task<bool>> condition, string description)
    {
        var deadline = DateTimeOffset.UtcNow + BoundedWait;
        while (DateTimeOffset.UtcNow < deadline)
        {
            if (await condition().ConfigureAwait(false)) return;
            await Task.Delay(25, CancellationToken.None).ConfigureAwait(false);
        }
        Assert.Fail($"Timed out waiting for: {description}");
    }

    private static string Now(DateTimeOffset value) => value.ToUniversalTime().ToString("O");
    private async Task<string?> RunStatusAsync(Guid runId) => await ScalarStringAsync($"SELECT status FROM etl_runs WHERE run_id='{runId:D}'");

    private async Task<long> ScalarAsync(string sql)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        return Convert.ToInt64(await command.ExecuteScalarAsync(CancellationToken.None), System.Globalization.CultureInfo.InvariantCulture);
    }

    private async Task<string?> ScalarStringAsync(string sql)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        var value = await command.ExecuteScalarAsync(CancellationToken.None);
        return value is null or DBNull ? null : Convert.ToString(value, System.Globalization.CultureInfo.InvariantCulture);
    }

    private async Task ExecuteSqlAsync(string sql, params (string Name, object? Value)[] parameters)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        foreach (var (name, value) in parameters) command.Parameters.AddWithValue(name, value ?? DBNull.Value);
        await command.ExecuteNonQueryAsync(CancellationToken.None);
    }

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
        public required string SourceNamespace { get; init; }

        public EtlExtractionWorker NewExtractionWorker() => new(
            Store, Spool, OData, IdentityGuard, Disk, State, Configuration, Etl, Storage, Agent,
            NullLogger<EtlExtractionWorker>.Instance);

        public EtlUploadWorker NewUploadWorker() => new(
            Store, Spool, Erp, State, Etl, Agent, NullLogger<EtlUploadWorker>.Instance);

        public EtlCompletionWorker NewCompletionWorker() => new(
            Store, Erp, State, Etl, Agent, NullLogger<EtlCompletionWorker>.Instance);

        public CommandExecutionWorker NewCommandWorker() => new(
            Store, Onec, new FakeOnecHealth(), Configuration, State, Pause,
            new DiagnosticsCollector(Store, Agent, Options.Create(new ErpOptions { RequireClientCertificate = false }), OnecOptions),
            Options.Create(new CommandOptions { MaxConcurrency = 4, MaxOperationalAttempts = 12, SupportedTypes = AdministrativeTypes }),
            NullLogger<CommandExecutionWorker>.Instance);

        public void Dispose() => Pause.Dispose();
    }

    private sealed record ODataReadCall(string EntityCode, EtlCursor? Committed, EtlCursor UpperBound, bool Full);

    private sealed class FakeOData : IOnecODataClient
    {
        private readonly object _gate = new();
        private readonly List<ODataReadCall> _calls = [];
        public Dictionary<string, List<JsonElement>> Rows { get; } = new(StringComparer.Ordinal);
        public Func<string, Task>? BeforeEntityReadAsync { get; set; }

        public int CallCount
        {
            get { lock (_gate) return _calls.Count; }
        }

        public ODataReadCall[] Snapshot()
        {
            lock (_gate) return _calls.ToArray();
        }

        public async IAsyncEnumerable<JsonElement> ReadEntityAsync(
            EtlEntityDefinition entity,
            EtlCursor? committedCursor,
            EtlCursor upperBound,
            bool full,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            lock (_gate) _calls.Add(new(entity.EntityCode, committedCursor, upperBound, full));
            if (BeforeEntityReadAsync is not null) await BeforeEntityReadAsync(entity.EntityCode).ConfigureAwait(false);
            if (!Rows.TryGetValue(entity.EntityCode, out var rows)) yield break;
            foreach (var row in rows)
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return row;
            }
        }

        public Task<bool> CheckAsync(CancellationToken cancellationToken) => Task.FromResult(true);
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
        private int _calls;

        public int Calls => Volatile.Read(ref _calls);

        public Task<OnecIdentityFetchResult> GetIdentityAsync(CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _calls);
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
        private int _calls;

        public ScriptedDiskProbe(IEnumerable<long> script)
        {
            _script = new(script);
            if (_script.Count == 0) _script.Enqueue(PlentyOfDisk);
            _last = _script.Peek();
        }

        public int Calls => Volatile.Read(ref _calls);

        public long GetAvailableFreeBytes(string path)
        {
            Interlocked.Increment(ref _calls);
            lock (_script)
            {
                if (_script.Count > 0) _last = _script.Dequeue();
            }
            return _last;
        }
    }

    private sealed class CountingOnec : IOnecCommandClient
    {
        private int _executeCalls;
        private int _statusCalls;

        public int ExecuteCalls => Volatile.Read(ref _executeCalls);
        public int StatusCalls => Volatile.Read(ref _statusCalls);

        public Task<OnecExecutionResult> ExecuteAsync(CommandEnvelope command, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _executeCalls);
            return Task.FromResult(new OnecExecutionResult(OnecExecutionKind.Succeeded,
                new(command.CommandId, "succeeded", JsonSerializer.SerializeToElement(new { @ref = "unexpected" }), null, [], 1), 200, null, null));
        }

        public Task<OnecExecutionResult> GetStatusAsync(Guid commandId, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _statusCalls);
            return Task.FromResult(new OnecExecutionResult(OnecExecutionKind.Succeeded,
                new(commandId, "succeeded", JsonSerializer.SerializeToElement(new { @ref = "unexpected" }), null, [], 1), 200, null, null));
        }
    }

    private sealed class FakeOnecHealth : IOnecHealthClient
    {
        public Task<OnecHealthResponse?> CheckAsync(CancellationToken cancellationToken) => Task.FromResult<OnecHealthResponse?>(null);
    }
}
