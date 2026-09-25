using System.Globalization;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Application.Configuration;
using ErpOnecAgent.Application.Etl;
using ErpOnecAgent.Contracts.Erp;
using ErpOnecAgent.Domain.Agent;
using ErpOnecAgent.Domain.Commands;
using ErpOnecAgent.Domain.Etl;
using ErpOnecAgent.Infrastructure.OneC;
using ErpOnecAgent.Infrastructure.Persistence.Sqlite;
using ErpOnecAgent.Infrastructure.Spool;
using ErpOnecAgent.Service.Runtime;
using ErpOnecAgent.Service.Workers.Etl;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;
using Xunit.Abstractions;

namespace ErpOnecAgent.IntegrationTests;

/// <summary>
/// C1 review-fix behaviour tests: the workers (EtlExtractionWorker.RunOnceAsync /
/// EtlUploadWorker.RunOnceAsync) over a real migrated SQLite store and a real
/// FileSpoolStore in a temp directory, with fakes only for IOnecODataClient,
/// IErpClient, the identity client and IDiskSpaceProbe. No production code is
/// touched; a failure here means the C1 fix under test is wrong.
/// </summary>
[Collection("EtlWorkerStatics")]
public sealed class EtlC1ReviewFixTests : IAsyncLifetime
{
    private const string QueryMode = "bootstrap_full";
    private const string ODataEndpoint = "http://onec.test/odata";
    private const string SourceNamespace = "c1-review-source";
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);
    private static readonly TimeSpan GateTimeout = TimeSpan.FromSeconds(15);

    private readonly ITestOutputHelper _output;
    private readonly SqliteTestDatabase _database = new();
    private readonly Dictionary<Guid, Guid> _extractionClaims = new();
    private readonly Guid _databaseId = Guid.NewGuid();
    private readonly Guid _exportEpoch = Guid.NewGuid();
    private SqliteConnectionFactory _factory = null!;
    private SqliteAgentStore _store = null!;
    private AgentRuntimeState _state = null!;
    private DynamicConfigurationState _configuration = null!;
    private AgentOptions _agentOptions = null!;

    public EtlC1ReviewFixTests(ITestOutputHelper output) => _output = output;

    public async Task InitializeAsync()
    {
        _factory = _database.CreateFactory(Path.Combine("data", "agent.db"));
        _store = new(_factory, new SqliteMigrator(_factory));
        await _store.InitializeAsync(CancellationToken.None);
        _state = new AgentRuntimeState();
        _state.RestoreLocalEtlPause(false);
        _state.SetRemoteMode(AgentMode.Normal);
        _state.CompleteBootstrap();
        _configuration = new DynamicConfigurationState(
            Options.Create(new CommandOptions()),
            Options.Create(new EtlOptions { Entities = [], IntervalMinutes = 60 }));
        _agentOptions = new AgentOptions
        {
            AgentId = $"c1-review-{Guid.NewGuid():N}",
            SiteId = "c1-site",
            DataDirectory = _database.Root
        };
    }

    public async Task DisposeAsync() => await _database.DisposeAsync();

    // ---------- R1: upload hot loop — a due but unclaimable batch contributes 0 ----------

    [Fact]
    public async Task R1_due_but_unclaimable_batch_returns_zero_and_never_calls_erp()
    {
        var spool = new FileSpoolStore(Path.Combine(_database.Root, "spool"));
        var erp = new FakeErpClient();
        var worker = UploadWorker(_store, spool, erp);

        // A batch that stays due-enumerable (its entity ownership row exists) but can
        // never be claimed: the run's binding epoch no longer matches ownership, so the
        // claim's full ownership-set gate fails while the due probe still lists it.
        var (blockedRunId, blockedBatches) = await SealedRunWithFileAsync(spool);
        await ExecuteSqlAsync("UPDATE etl_run_ownership_bindings SET expected_epoch=expected_epoch+1 WHERE run_id=$run;",
            ("$run", blockedRunId.ToString("D")));
        var dueProbe = await _store.GetDueBatchUploadsAsync(10, DateTimeOffset.UtcNow, CancellationToken.None);
        Assert.Contains(dueProbe, d => d.BatchId == blockedBatches[0].BatchId);

        var claimed = await worker.RunOnceAsync(CancellationToken.None);

        Assert.Equal(0, claimed);
        Assert.Equal(0, erp.UploadCalls);
        Assert.Equal("ready", await ScalarStringAsync($"SELECT status FROM etl_batches WHERE batch_id='{blockedBatches[0].BatchId:D}'"));
        Assert.Equal(0, await ScalarAsync($"SELECT COUNT(*) FROM etl_batch_send_attempts WHERE batch_id='{blockedBatches[0].BatchId:D}'"));

        // A normally due batch in the same pass is claimed and uploaded: the pass
        // reports the number of batches ACTUALLY claimed, not the due count.
        var (goodRunId, goodBatches) = await SealedRunWithFileAsync(spool, "orders");

        claimed = await worker.RunOnceAsync(CancellationToken.None);

        Assert.Equal(1, claimed);
        Assert.Equal(1, erp.UploadCalls);
        Assert.Equal(goodBatches[0].BatchId, erp.UploadedBatchIds.Single());
        Assert.Equal("acknowledged", await ScalarStringAsync($"SELECT status FROM etl_batches WHERE batch_id='{goodBatches[0].BatchId:D}'"));
        Assert.Equal("ready", await ScalarStringAsync($"SELECT status FROM etl_batches WHERE batch_id='{blockedBatches[0].BatchId:D}'"));
        Assert.Equal(0, await ScalarAsync($"SELECT COUNT(*) FROM etl_batch_send_attempts WHERE batch_id='{blockedBatches[0].BatchId:D}'"));
        _ = goodRunId;
    }

    // ---------- R2: cancellation during the precheck is a precheck_failed, never admitted ----------

    [Fact]
    public async Task R2_precheck_cancellation_records_precheck_failed_and_leaves_batch_due()
    {
        var inner = new FileSpoolStore(Path.Combine(_database.Root, "spool"));
        var (runId, batches) = await SealedRunWithFileAsync(inner);
        var batchId = batches[0].BatchId;
        var spool = new GatedOpenSpoolStore(inner);
        var erp = new FakeErpClient();
        var worker = UploadWorker(_store, spool, erp);
        using var cts = new CancellationTokenSource();

        var pass = worker.RunOnceAsync(cts.Token);
        await spool.FirstOpenEntered.Task.WaitAsync(GateTimeout);
        await cts.CancelAsync();
        spool.ReleaseOpen.TrySetResult(true);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await pass);

        Assert.Equal("precheck_failed", await ScalarStringAsync($"SELECT outcome FROM etl_batch_send_attempts WHERE batch_id='{batchId:D}'"));
        Assert.Equal(0, await ScalarAsync($"SELECT COUNT(*) FROM etl_batch_send_attempts WHERE batch_id='{batchId:D}' AND outcome='admitted'"));
        Assert.Equal("retry_waiting", await ScalarStringAsync($"SELECT status FROM etl_batches WHERE batch_id='{batchId:D}'"));
        Assert.StartsWith("PRECHECK:", await ScalarStringAsync($"SELECT last_error FROM etl_batch_send_attempts WHERE batch_id='{batchId:D}'"));
        // Due again immediately (the worker passes DateTimeOffset.UtcNow as the next-attempt time).
        var next = await ScalarStringAsync($"SELECT next_attempt_at_utc FROM etl_batches WHERE batch_id='{batchId:D}'");
        Assert.NotNull(next);
        Assert.True(DateTimeOffset.Parse(next!, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind) <= DateTimeOffset.UtcNow);
        Assert.Contains(await _store.GetDueBatchUploadsAsync(10, DateTimeOffset.UtcNow, CancellationToken.None), d => d.BatchId == batchId);
        Assert.Equal(0, erp.UploadCalls);
        _ = runId;
    }

    // ---------- R3: a missing spool file is SPOOL_FILE_MISSING, never sent ----------

    [Fact]
    public async Task R3_missing_spool_file_is_precheck_failed_spool_file_missing()
    {
        var spool = new FileSpoolStore(Path.Combine(_database.Root, "spool"));
        var (runId, batches) = await SealedRunWithFileAsync(spool);
        var batchId = batches[0].BatchId;
        File.Delete(batches[0].FilePath);
        var erp = new FakeErpClient();
        var worker = UploadWorker(_store, spool, erp);

        var claimed = await worker.RunOnceAsync(CancellationToken.None);

        Assert.Equal(1, claimed);
        Assert.Equal("precheck_failed", await ScalarStringAsync($"SELECT outcome FROM etl_batch_send_attempts WHERE batch_id='{batchId:D}'"));
        Assert.StartsWith("SPOOL_FILE_MISSING:", await ScalarStringAsync($"SELECT last_error FROM etl_batch_send_attempts WHERE batch_id='{batchId:D}'"));
        Assert.Equal("retry_waiting", await ScalarStringAsync($"SELECT status FROM etl_batches WHERE batch_id='{batchId:D}'"));
        Assert.Equal(0, erp.UploadCalls);
        _ = runId;
    }

    // ---------- R4: ACK evidence stores the hash of the exact body bytes and the HTTP status ----------

    [Fact]
    public async Task R4_ack_evidence_is_hex_sha256_of_exact_body_and_status()
    {
        var spool = new FileSpoolStore(Path.Combine(_database.Root, "spool"));
        var (runId, batches) = await SealedRunWithFileAsync(spool);
        var batchId = batches[0].BatchId;
        var body = Encoding.UTF8.GetBytes(
            " \r\n  {\t\"status\" :  \"acknowledged\",\r\n   \"batchId\" :   \"" + batchId.ToString("D") + "\" ,\n \"rowsAccepted\":  3 }   \r\n ");
        var erp = new FakeErpClient
        {
            Handler = (batch, content, ct) => Task.FromResult(new BatchUploadResponse(
                new BatchAcknowledgement(batch.BatchId, "acknowledged", batch.RowCount, true, DateTimeOffset.UtcNow),
                body, 201))
        };
        var worker = UploadWorker(_store, spool, erp);

        var claimed = await worker.RunOnceAsync(CancellationToken.None);

        Assert.Equal(1, claimed);
        Assert.Equal(1, erp.UploadCalls);
        var expectedHash = Convert.ToHexString(SHA256.HashData(body));
        Assert.Equal(expectedHash, await ScalarStringAsync($"SELECT ack_payload_hash FROM etl_batch_send_attempts WHERE batch_id='{batchId:D}'"));
        Assert.Equal("201", await ScalarStringAsync($"SELECT http_status FROM etl_batch_send_attempts WHERE batch_id='{batchId:D}'"));
        Assert.Equal("acknowledged", await ScalarStringAsync($"SELECT outcome FROM etl_batch_send_attempts WHERE batch_id='{batchId:D}'"));
        Assert.Equal("acknowledged", await ScalarStringAsync($"SELECT status FROM etl_batches WHERE batch_id='{batchId:D}'"));
        _ = runId;
    }

    // ---------- R5: the post-ERP ledger write is retried a bounded number of times ----------

    [Fact]
    public async Task R5_flaky_acknowledge_write_retries_then_acknowledges_once()
    {
        var spool = new FileSpoolStore(Path.Combine(_database.Root, "spool"));
        var (runId, batches) = await SealedRunWithFileAsync(spool);
        var batchId = batches[0].BatchId;
        var erp = new FakeErpClient();
        var control = new FailOnceAckControl(_store);
        var worker = UploadWorker(FailOnceAckProxy.Create(control), spool, erp);

        var originalDelay = EtlUploadWorker.StoreWriteRetryDelay;
        EtlUploadWorker.StoreWriteRetryDelay = TimeSpan.FromMilliseconds(5);
        try
        {
            var claimed = await worker.RunOnceAsync(CancellationToken.None);

            Assert.Equal(1, claimed);
            Assert.Equal(2, control.AckCalls);
            Assert.Equal(1, erp.UploadCalls);
            Assert.Equal("acknowledged", await ScalarStringAsync($"SELECT status FROM etl_batches WHERE batch_id='{batchId:D}'"));
            Assert.Equal("acknowledged", await ScalarStringAsync($"SELECT outcome FROM etl_batch_send_attempts WHERE batch_id='{batchId:D}'"));
        }
        finally
        {
            EtlUploadWorker.StoreWriteRetryDelay = originalDelay;
        }
        _ = runId;
    }

    [Fact]
    public async Task R5b_acknowledge_that_committed_then_threw_is_not_applied_twice()
    {
        var spool = new FileSpoolStore(Path.Combine(_database.Root, "spool"));
        var (_, batches) = await SealedRunWithFileAsync(spool);
        var batchId = batches[0].BatchId;
        var erp = new FakeErpClient();
        var control = new FailOnceAckControl(_store) { CommitBeforeFailing = true };
        var worker = UploadWorker(FailOnceAckProxy.Create(control), spool, erp);

        var originalDelay = EtlUploadWorker.StoreWriteRetryDelay;
        EtlUploadWorker.StoreWriteRetryDelay = TimeSpan.FromMilliseconds(5);
        try
        {
            Assert.Equal(1, await worker.RunOnceAsync(CancellationToken.None));
        }
        finally
        {
            EtlUploadWorker.StoreWriteRetryDelay = originalDelay;
        }

        Assert.Equal(2, control.AckCalls);
        Assert.Equal(1, erp.UploadCalls);
        Assert.Equal(1, await ScalarAsync($"SELECT COUNT(*) FROM etl_batch_send_attempts WHERE batch_id='{batchId:D}'"));
        Assert.Equal("acknowledged", await ScalarStringAsync($"SELECT status FROM etl_batches WHERE batch_id='{batchId:D}'"));
        Assert.Equal("acknowledged", await ScalarStringAsync($"SELECT outcome FROM etl_batch_send_attempts WHERE batch_id='{batchId:D}'"));
    }

    // ---------- R6: a stolen extraction claim fences registration AND the block (exploratory) ----------

    [Fact]
    public async Task R6_stolen_extraction_claim_refuses_registration_and_block()
    {
        var spool = new FileSpoolStore(Path.Combine(_database.Root, "spool"));
        var (runId, jobId) = await NewPendingJobRunAsync(QueryMode, ["clients"]);
        var stolenClaim = Guid.NewGuid();
        var odata = new FakeODataClient
        {
            Reader = (entity, ct) => StealClaimThenYieldAsync(entity.EntityCode, runId, stolenClaim, ct)
        };
        var worker = ExtractionWorker(spool, odata, new FakeDiskSpaceProbe(long.MaxValue));

        var worked = await worker.RunOnceAsync(CancellationToken.None);

        // The run was claimed, so the pass reports work — but the stolen claim must fence
        // both the batch registration and the subsequent block attempt.
        Assert.True(worked);
        Assert.Equal(0, await ScalarAsync($"SELECT COUNT(*) FROM etl_batches WHERE run_id='{runId:D}' AND status='ready'"));
        Assert.Equal(0, await ScalarAsync($"SELECT COUNT(*) FROM etl_batches WHERE run_id='{runId:D}'"));

        var status = await ScalarStringAsync($"SELECT status FROM etl_runs WHERE run_id='{runId:D}'");
        var claim = await ScalarStringAsync($"SELECT extraction_claim_id FROM etl_runs WHERE run_id='{runId:D}'");
        var jobStatus = await ScalarStringAsync($"SELECT status FROM etl_jobs WHERE job_id='{jobId:D}'");
        var entityStatus = await ScalarStringAsync($"SELECT status FROM etl_run_entities WHERE run_id='{runId:D}' AND entity_name='clients'")
            ?? "(no etl_run_entities table/row — see report)";
        var conflict = await ScalarStringAsync($"SELECT finalize_conflict_code FROM etl_runs WHERE run_id='{runId:D}'");
        _output.WriteLine($"R6 observed: run.status={status} extraction_claim_id={claim} (stolen={stolenClaim:D}) job.status={jobStatus} entity={entityStatus} finalize_conflict_code={conflict ?? "NULL"}");
        // Expected: the block was refused (claim lost), the run keeps the other claim and
        // stays 'running' until startup recovery blocks it INTERRUPTED.
        Assert.Equal(stolenClaim.ToString("D"), claim);
        Assert.Equal("running", status);
        Assert.Null(conflict);
        Assert.Equal("running", jobStatus);
    }

    // ---------- R7: spool limit — precheck before claim, and per-batch during extraction ----------

    [Fact]
    public async Task R7a_full_spool_defers_extraction_before_any_claim()
    {
        var spoolRoot = Path.Combine(_database.Root, "spool");
        var spool = new FileSpoolStore(spoolRoot);
        Directory.CreateDirectory(Path.Combine(spoolRoot, "ready"));
        await File.WriteAllBytesAsync(Path.Combine(spoolRoot, "ready", "backlog.bin"), new byte[256]);
        var (runId, jobId) = await NewPendingJobRunAsync(QueryMode, ["clients"]);
        var identity = new FakeIdentityClient(_databaseId, _exportEpoch);
        var worker = ExtractionWorker(spool, new FakeODataClient(), new FakeDiskSpaceProbe(long.MaxValue),
            identity: identity, storage: new StorageOptions { MaxSpoolBytes = 128 });

        var worked = await worker.RunOnceAsync(CancellationToken.None);

        Assert.False(worked);
        Assert.Equal(0, identity.Calls); // the spool check precedes the identity check
        Assert.Equal("pending", await ScalarStringAsync($"SELECT status FROM etl_runs WHERE run_id='{runId:D}'"));
        Assert.Equal("pending", await ScalarStringAsync($"SELECT status FROM etl_jobs WHERE job_id='{jobId:D}'"));
        Assert.Null(await ScalarStringAsync($"SELECT extraction_claim_id FROM etl_runs WHERE run_id='{runId:D}'"));
    }

    [Fact]
    public async Task R7b_batch_over_spool_limit_blocks_run_spool_limit()
    {
        // StorageOptions.MaxSpoolBytes stays huge so the pre-claim pass proceeds; the
        // FileSpoolStore's own limit trips on the first batch write.
        var spool = new FileSpoolStore(Path.Combine(_database.Root, "spool"), maxSpoolBytes: 16);
        var (runId, jobId) = await NewPendingJobRunAsync(QueryMode, ["clients"]);
        var odata = new FakeODataClient { Reader = (entity, ct) => RowsAsync(entity.EntityCode, 3, ct) };
        var worker = ExtractionWorker(spool, odata, new FakeDiskSpaceProbe(long.MaxValue),
            storage: new StorageOptions { MaxSpoolBytes = long.MaxValue });

        var worked = await worker.RunOnceAsync(CancellationToken.None);

        Assert.True(worked);
        Assert.Equal("blocked", await ScalarStringAsync($"SELECT status FROM etl_runs WHERE run_id='{runId:D}'"));
        Assert.Equal("SPOOL_LIMIT", await ScalarStringAsync($"SELECT finalize_conflict_code FROM etl_runs WHERE run_id='{runId:D}'"));
        Assert.Equal("blocked", await ScalarStringAsync($"SELECT status FROM etl_jobs WHERE job_id='{jobId:D}'"));
        Assert.Equal(0, await ScalarAsync($"SELECT COUNT(*) FROM etl_batches WHERE run_id='{runId:D}'"));
    }

    // ---------- R8: startup missing-file reconciliation blocks the run once ----------

    [Fact]
    public async Task R8_startup_missing_spool_reconciliation_dead_letters_and_blocks_once()
    {
        var spool = new FileSpoolStore(Path.Combine(_database.Root, "spool"));
        var (runId, batches) = await SealedRunWithFileAsync(spool, "clients", batchCount: 2);
        var (missing, kept) = (batches[0], batches[1]);

        // A second sealed run whose only batch is acknowledged — its deleted file must
        // be untouched (only 'ready'/'retry_waiting' rows are reconciled).
        var (runId2, batches2) = await SealedRunWithFileAsync(spool, "orders");
        var acked = batches2[0];
        var claim = Assert.IsType<EtlBatchUploadClaimOutcome.Claimed>(
            await _store.TryClaimBatchUploadAsync(acked.BatchId, "uploader-1", DateTimeOffset.UtcNow, 5, CancellationToken.None)).Claim;
        Assert.IsType<EtlBatchAckOutcome.Acknowledged>(await _store.AcknowledgeClaimedBatchAsync(
            acked.BatchId, claim.AttemptId,
            new EtlBatchAckEvidence("acknowledged", acked.BatchId, acked.RowCount, true, DateTimeOffset.UtcNow),
            "h", 200, CancellationToken.None));

        File.Delete(missing.FilePath);
        File.Delete(acked.FilePath);

        var blocked = await _store.BlockRunsWithMissingSpoolFilesAsync(File.Exists, CancellationToken.None);

        Assert.Equal(1, blocked);
        Assert.Equal("dead_letter", await ScalarStringAsync($"SELECT status FROM etl_batches WHERE batch_id='{missing.BatchId:D}'"));
        Assert.Equal("RUN_BLOCKED", await ScalarStringAsync($"SELECT quarantine_code FROM etl_batches WHERE batch_id='{missing.BatchId:D}'"));
        Assert.Equal("SPOOL_FILE_MISSING", await ScalarStringAsync($"SELECT last_error FROM etl_batches WHERE batch_id='{missing.BatchId:D}'"));
        Assert.Equal("dead_letter", await ScalarStringAsync($"SELECT status FROM etl_batches WHERE batch_id='{kept.BatchId:D}'"));
        Assert.Equal("RUN_BLOCKED", await ScalarStringAsync($"SELECT quarantine_code FROM etl_batches WHERE batch_id='{kept.BatchId:D}'"));
        Assert.Equal("blocked", await ScalarStringAsync($"SELECT status FROM etl_runs WHERE run_id='{runId:D}'"));
        Assert.Equal("SPOOL_FILE_MISSING", await ScalarStringAsync($"SELECT finalize_conflict_code FROM etl_runs WHERE run_id='{runId:D}'"));
        // Ownership evidence is retained — nothing is released by the reconciliation.
        Assert.Equal(1, await ScalarAsync($"SELECT COUNT(*) FROM etl_entity_ownership WHERE owner_run_id='{runId:D}' AND released_at_utc IS NULL"));
        // The acknowledged batch on the other run is untouched.
        Assert.Equal("acknowledged", await ScalarStringAsync($"SELECT status FROM etl_batches WHERE batch_id='{acked.BatchId:D}'"));
        Assert.Null(await ScalarStringAsync($"SELECT quarantine_code FROM etl_batches WHERE batch_id='{acked.BatchId:D}'"));
        Assert.Equal("uploading", await ScalarStringAsync($"SELECT status FROM etl_runs WHERE run_id='{runId2:D}'"));

        // Idempotent: everything reconciled is already dead_letter.
        Assert.Equal(0, await _store.BlockRunsWithMissingSpoolFilesAsync(File.Exists, CancellationToken.None));
    }

    // ---------- R9: a failing disk probe fails closed — nothing is claimed ----------

    [Fact]
    public async Task R9_disk_probe_failure_defers_extraction()
    {
        var spool = new FileSpoolStore(Path.Combine(_database.Root, "spool"));
        var (runId, jobId) = await NewPendingJobRunAsync(QueryMode, ["clients"]);
        var odata = new FakeODataClient();
        var identity = new FakeIdentityClient(_databaseId, _exportEpoch);
        var probe = new FakeDiskSpaceProbe(0) { Failure = new IOException("disk probe failed") };
        var worker = ExtractionWorker(spool, odata, probe, identity: identity);

        var worked = await worker.RunOnceAsync(CancellationToken.None);

        Assert.False(worked);
        Assert.Equal(0, identity.Calls);
        Assert.Equal(0, odata.ReadCalls);
        Assert.Equal("pending", await ScalarStringAsync($"SELECT status FROM etl_runs WHERE run_id='{runId:D}'"));
        Assert.Equal("pending", await ScalarStringAsync($"SELECT status FROM etl_jobs WHERE job_id='{jobId:D}'"));
        Assert.Null(await ScalarStringAsync($"SELECT extraction_claim_id FROM etl_runs WHERE run_id='{runId:D}'"));
    }

    // ---------- workers ----------

    private EtlUploadWorker UploadWorker(IAgentStore store, ISpoolStore spool, IErpClient erp) =>
        new(store, spool, erp, _state,
            Options.Create(new EtlOptions { Enabled = true, Entities = [], MaxConcurrentBatchUploads = 4 }),
            Options.Create(_agentOptions), NullLogger<EtlUploadWorker>.Instance);

    private EtlExtractionWorker ExtractionWorker(ISpoolStore spool, IOnecODataClient odata, IDiskSpaceProbe probe,
        IOnecIdentityClient? identity = null, EtlOptions? etl = null, StorageOptions? storage = null) =>
        new(_store, spool, odata,
            new SourceIdentityGuard(identity ?? new FakeIdentityClient(_databaseId, _exportEpoch), Options.Create(OnecOptions())),
            probe, _state, _configuration,
            Options.Create(etl ?? new EtlOptions { Enabled = true, Entities = [], IntervalMinutes = 60 }),
            Options.Create(storage ?? new StorageOptions()),
            Options.Create(_agentOptions), NullLogger<EtlExtractionWorker>.Instance);

    private OnecOptions OnecOptions() => new()
    {
        ODataBaseUrl = ODataEndpoint,
        SourceBinding = new OnecSourceBindingOptions
        {
            DatabaseId = _databaseId.ToString("D"),
            ExportEpoch = _exportEpoch.ToString("D"),
            Environment = "test",
            ODataEndpoint = ODataEndpoint
        }
    };

    // ---------- store helpers (same pattern as EtlSendLedgerO2Tests) ----------

    private async Task<(Guid RunId, Guid JobId)> NewPendingJobRunAsync(string mode, string[] entities)
    {
        var runId = Guid.NewGuid();
        var jobId = Guid.NewGuid();
        var created = Now();
        var manifest = JsonSerializer.Serialize(entities, JsonOptions);
        var definitions = JsonSerializer.Serialize(entities.Select(Entity).ToArray(), JsonOptions);
        await ExecuteSqlAsync(
            "INSERT INTO etl_runs(run_id,mode,requested_entities_json,status,configuration_version,created_at_utc,updated_at_utc,row_version) VALUES($run,$mode,$manifest,'pending',7,$now,$now,1);",
            ("$run", runId.ToString("D")), ("$mode", mode), ("$manifest", manifest), ("$now", created));
        await ExecuteSqlAsync(
            "INSERT INTO etl_jobs(job_id,command_id,run_id,mode,entities_json,configuration_version,status,command_payload_hash,acceptance_result_json,created_at_utc,updated_at_utc,row_version) VALUES($job,$cmd,$run,$mode,$defs,7,'pending','hash','{}',$now,$now,1);",
            ("$job", jobId.ToString("D")), ("$cmd", Guid.NewGuid().ToString("D")), ("$run", runId.ToString("D")), ("$mode", mode), ("$defs", definitions), ("$now", created));
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

    // Sealed run whose batches are REAL files inside the FileSpoolStore root: begin the
    // entity under the claim, write+register each batch, complete, seal to 'uploading'.
    private async Task<(Guid RunId, List<EtlBatch> Batches)> SealedRunWithFileAsync(FileSpoolStore spool, string entity = "clients", int batchCount = 1, int rowsPerBatch = 3)
    {
        var runId = await NewRunAsync(entity);
        var claim = _extractionClaims[runId];
        Assert.IsType<EtlEntityBeginOutcome.Begun>(
            await _store.BeginEtlEntityExtractionAsync(runId, claim, Request(entity), CancellationToken.None));
        var batches = new List<EtlBatch>();
        for (var index = 0; index < batchCount; index++)
        {
            var batch = await spool.WriteBatchAsync(runId, Entity(entity), Rows(entity, rowsPerBatch, index * 1000),
                null, new EtlCursor(DateTimeOffset.Parse("2026-09-19T09:59:00.0000000+00:00", CultureInfo.InvariantCulture), $"{entity}-last"), CancellationToken.None);
            Assert.IsType<EtlBatchRegistrationOutcome.Registered>(await _store.RegisterGuardedEtlBatchAsync(batch, claim, CancellationToken.None));
            batches.Add(batch);
        }
        var final = JsonSerializer.Serialize(new EtlCursor(DateTimeOffset.Parse("2026-09-19T09:59:00.0000000+00:00", CultureInfo.InvariantCulture), $"{entity}-last"), JsonOptions);
        Assert.IsType<EtlEntityCompletionOutcome.Completed>(
            await _store.CompleteEtlEntityExtractionAsync(runId, claim, entity, final, batchCount, CancellationToken.None));
        Assert.IsType<EtlRunSealOutcome.Sealed>(await _store.SealEtlRunExtractionAsync(runId, claim, CancellationToken.None));
        return (runId, batches);
    }

    private static EtlEntityDefinition Entity(string code) =>
        new(code, "Catalog_" + code, "Ref_Key", "UpdatedAt", "DeletionMark", ["Ref_Key", "UpdatedAt", "DeletionMark"], "incremental", 500, 10);

    private static EtlEntityExtractionRequest Request(string entity) =>
        new(entity, JsonSerializer.Serialize(Entity(entity), JsonOptions), SourceNamespace, QueryMode,
            JsonSerializer.Serialize(new EtlCursor(DateTimeOffset.UtcNow, null), JsonOptions));

    private static List<JsonElement> Rows(string entity, int count, int offset = 0)
    {
        var rows = new List<JsonElement>();
        for (var index = 0; index < count; index++)
            rows.Add(JsonDocument.Parse($"{{\"Ref_Key\":\"{entity}-{offset + index}\",\"UpdatedAt\":\"2026-09-19T09:00:00Z\",\"DeletionMark\":false}}").RootElement.Clone());
        return rows;
    }

    private static async IAsyncEnumerable<JsonElement> RowsAsync(string entity, int count, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        foreach (var row in Rows(entity, count))
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return row;
        }
        await Task.CompletedTask;
    }

    private async IAsyncEnumerable<JsonElement> StealClaimThenYieldAsync(string entity, Guid runId, Guid stolenClaim, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await ExecuteSqlAsync("UPDATE etl_runs SET extraction_claim_id=$claim, updated_at_utc=$now WHERE run_id=$run;",
            ("$claim", stolenClaim.ToString("D")), ("$run", runId.ToString("D")), ("$now", Now()));
        await foreach (var row in RowsAsync(entity, 2, cancellationToken)) yield return row;
    }

    private static string Now() => DateTimeOffset.UtcNow.ToString("O");

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

    // ---------- fakes ----------

    private sealed class FakeErpClient : IErpClient
    {
        private int _uploadCalls;
        public int UploadCalls => _uploadCalls;
        public List<Guid> UploadedBatchIds { get; } = [];
        public Func<EtlBatch, Stream, CancellationToken, Task<BatchUploadResponse>>? Handler { get; init; }

        public Task<BatchUploadResponse> UploadBatchWithEvidenceAsync(EtlBatch batch, Stream content, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _uploadCalls);
            UploadedBatchIds.Add(batch.BatchId);
            if (Handler is not null) return Handler(batch, content, cancellationToken);
            var ack = new BatchAcknowledgement(batch.BatchId, "acknowledged", batch.RowCount, true, DateTimeOffset.UtcNow);
            return Task.FromResult(new BatchUploadResponse(ack, JsonSerializer.SerializeToUtf8Bytes(ack, JsonOptions), 200));
        }

        public Task<BatchAcknowledgement> UploadBatchAsync(EtlBatch batch, Stream content, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task<SessionStartResponse> StartSessionAsync(SessionStartRequest request, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task<LeaseResponse> LeaseCommandAsync(LeaseRequest request, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task AcknowledgeReceivedAsync(Guid commandId, CommandReceivedRequest request, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task AcknowledgeResultAsync(Guid commandId, string resultJson, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task CompleteEtlRunAsync(Guid runId, object summary, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task SendHeartbeatAsync(HeartbeatRequest request, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task<RemoteConfigurationResponse?> GetConfigurationAsync(long currentVersion, CancellationToken cancellationToken) => throw new NotSupportedException();
    }

    /// <summary>Spool wrapper whose first OpenReadAsync waits on a gate, then honours cancellation.</summary>
    private sealed class GatedOpenSpoolStore(ISpoolStore inner) : ISpoolStore
    {
        public TaskCompletionSource<bool> FirstOpenEntered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource<bool> ReleaseOpen { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public async Task<Stream> OpenReadAsync(EtlBatch batch, CancellationToken cancellationToken)
        {
            FirstOpenEntered.TrySetResult(true);
            await ReleaseOpen.Task.ConfigureAwait(false);
            cancellationToken.ThrowIfCancellationRequested();
            return await inner.OpenReadAsync(batch, cancellationToken).ConfigureAwait(false);
        }

        public Task<EtlBatch> WriteBatchAsync(Guid runId, EtlEntityDefinition entity, IReadOnlyList<JsonElement> rows, EtlCursor? watermarkFrom, EtlCursor? watermarkTo, CancellationToken cancellationToken) =>
            inner.WriteBatchAsync(runId, entity, rows, watermarkFrom, watermarkTo, cancellationToken);
        public Task<long> GetSizeAsync(CancellationToken cancellationToken) => inner.GetSizeAsync(cancellationToken);
        public Task QuarantineTemporaryFilesAsync(CancellationToken cancellationToken) => inner.QuarantineTemporaryFilesAsync(cancellationToken);
        public Task DeleteAcknowledgedAsync(EtlBatch batch, CancellationToken cancellationToken) => inner.DeleteAcknowledgedAsync(batch, cancellationToken);
    }

    private sealed class FakeODataClient : IOnecODataClient
    {
        private int _readCalls;
        public int ReadCalls => _readCalls;
        public Func<EtlEntityDefinition, CancellationToken, IAsyncEnumerable<JsonElement>>? Reader { get; init; }

        public IAsyncEnumerable<JsonElement> ReadEntityAsync(EtlEntityDefinition entity, EtlCursor? committedCursor, EtlCursor upperBound, bool full, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _readCalls);
            return Reader?.Invoke(entity, cancellationToken) ?? EmptyAsync(cancellationToken);
        }

        public Task<bool> CheckAsync(CancellationToken cancellationToken) => Task.FromResult(true);

        private static async IAsyncEnumerable<JsonElement> EmptyAsync([EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await Task.CompletedTask;
            yield break;
        }
    }

    private sealed class FakeIdentityClient : IOnecIdentityClient
    {
        private int _calls;
        public int Calls => _calls;
        private readonly Guid _databaseId;
        private readonly Guid _exportEpoch;

        public FakeIdentityClient(Guid databaseId, Guid exportEpoch)
        {
            _databaseId = databaseId;
            _exportEpoch = exportEpoch;
        }

        public Task<OnecIdentityFetchResult> GetIdentityAsync(CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _calls);
            return Task.FromResult(OnecIdentityFetchResult.Ready(
                new OnecSourceIdentity(1, "ready", "whole-infobase", _databaseId, _exportEpoch, "test")));
        }
    }

    private sealed class FakeDiskSpaceProbe(long freeBytes) : IDiskSpaceProbe
    {
        public Exception? Failure { get; init; }
        public long GetAvailableFreeBytes(string path) => Failure is not null ? throw Failure : freeBytes;
    }

    /// <summary>Store proxy that fails the first AcknowledgeClaimedBatchAsync, then delegates.</summary>
    public sealed class FailOnceAckControl(IAgentStore inner)
    {
        private int _ackCalls;
        private int _failuresLeft = 1;
        public IAgentStore Inner { get; } = inner;
        public int AckCalls => _ackCalls;
        /// <summary>When set, the injected failure happens AFTER the real write committed.</summary>
        public bool CommitBeforeFailing { get; init; }

        /// <summary>Counts an ACK call; true while injected failures remain.</summary>
        public bool ShouldFailAck() => Interlocked.Increment(ref _ackCalls) >= 0 && Interlocked.Decrement(ref _failuresLeft) >= 0;
    }

    public class FailOnceAckProxy : DispatchProxy
    {
        private static readonly ConditionalWeakTable<DispatchProxy, FailOnceAckControl> Controls = new();

        public static IAgentStore Create(FailOnceAckControl control)
        {
            var proxy = DispatchProxy.Create<IAgentStore, FailOnceAckProxy>();
            Controls.Add((DispatchProxy)(object)proxy, control);
            return proxy;
        }

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            if (!Controls.TryGetValue(this, out var control)) throw new InvalidOperationException("Proxy state is missing.");
            if (targetMethod?.Name == nameof(IAgentStore.AcknowledgeClaimedBatchAsync) && control.ShouldFailAck())
                return control.CommitBeforeFailing
                    ? CommitThenFailAsync((Task<EtlBatchAckOutcome>)targetMethod.Invoke(control.Inner, args)!)
                    : Task.FromException<EtlBatchAckOutcome>(new InvalidOperationException("Injected transient store write failure."));
            return targetMethod!.Invoke(control.Inner, args);
        }

        private static async Task<EtlBatchAckOutcome> CommitThenFailAsync(Task<EtlBatchAckOutcome> write)
        {
            await write.ConfigureAwait(false);
            throw new InvalidOperationException("Injected failure after the write committed.");
        }
    }
}
