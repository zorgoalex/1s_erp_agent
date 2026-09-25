using System.Globalization;
using System.Text.Json;
using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Domain.Etl;
using ErpOnecAgent.Infrastructure.Persistence.Sqlite;
using Microsoft.Data.Sqlite;
using Xunit;

namespace ErpOnecAgent.IntegrationTests;

/// <summary>
/// R1 DARK resolution suite (schema v10, migration 010, design §8): attested manual
/// resolution of a failed/blocked ETL run. <c>Resolved</c> commits, in ONE
/// transaction, the immutable etl_run_resolutions record + resolved_at_utc +
/// dead_letter RUN_BLOCKED/RUN_RESOLVED fencing of remaining pre-acknowledgement
/// batches + 'manual_release' of the run's epoch-bound ownership — job stays
/// blocked, watermarks and attempt/batch evidence untouched, the schedule key
/// released. <c>AlreadyResolved</c> replays the original record with zero writes;
/// every <c>Refused</c> writes nothing; invalid requests throw
/// <see cref="ArgumentException"/>. All against a real migrated SQLite database.
/// DARK: nothing here wires workers, the ERP client, or production dispatch.
/// </summary>
public sealed class EtlRunResolutionR1Tests : IAsyncLifetime
{
    private const string SourceNamespace = "onec-infobase-a";
    private const string QueryMode = "bootstrap_full";
    private const string RemoteVerification = "ERP staging reconciled: no rows landed for the unresolved batches";
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);
    private static readonly EtlCursor FinalClients = new(DateTimeOffset.Parse("2026-09-19T09:59:00.0000000+00:00", CultureInfo.InvariantCulture), "A10");
    private static readonly EtlCursor FinalOrders = new(DateTimeOffset.Parse("2026-09-19T09:58:00.0000000+00:00", CultureInfo.InvariantCulture), "O8");
    private static readonly EtlCursor FinalPayments = new(DateTimeOffset.Parse("2026-09-19T09:57:00.0000000+00:00", CultureInfo.InvariantCulture), "P7");

    // The evidence surface a refusal must leave byte-identical.
    private static readonly string[] ZeroWriteSnapshots =
    [
        "SELECT * FROM etl_runs ORDER BY run_id;",
        "SELECT * FROM etl_jobs ORDER BY job_id;",
        "SELECT * FROM etl_batches ORDER BY batch_id;",
        "SELECT * FROM etl_batch_send_attempts ORDER BY attempt_id;",
        "SELECT * FROM etl_entity_ownership ORDER BY entity_name;",
        "SELECT * FROM etl_run_ownership_bindings ORDER BY run_id, entity_name;",
        "SELECT * FROM etl_run_resolutions ORDER BY run_id;",
        "SELECT * FROM watermarks ORDER BY entity_name;"
    ];

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

    // ---------- Resolved ----------

    [Fact]
    public async Task V1_Resolving_a_blocked_run_writes_the_record_and_releases_ownership()
    {
        var (runId, batchId, attemptId) = await BlockedRunAsync("clients");
        var watermarksBefore = await SnapshotRowsAsync("SELECT * FROM watermarks ORDER BY entity_name;");
        var batchesBefore = await SnapshotRowsAsync($"SELECT * FROM etl_batches WHERE run_id='{runId:D}' ORDER BY batch_id;");
        var attemptsBefore = await SnapshotRowsAsync("SELECT * FROM etl_batch_send_attempts ORDER BY attempt_id;");
        var epochBefore = await ScalarStringAsync("SELECT ownership_epoch FROM etl_entity_ownership WHERE entity_name='clients'");
        var now = DateTimeOffset.UtcNow;

        var outcome = await _store.ResolveEtlRunAsync(Resolution(runId, EtlRunResolutionDecision.Abandon), now, CancellationToken.None);

        var record = Assert.IsType<EtlRunResolutionOutcome.Resolved>(outcome).Record;
        Assert.Equal(runId, record.RunId);
        Assert.NotEqual(Guid.Empty, record.ResolutionId);
        Assert.Equal(now, record.ResolvedAtUtc);
        Assert.Equal("operator-1", record.OperatorId);
        Assert.Equal(EtlRunResolutionDecision.Abandon, record.Decision);
        Assert.Equal(RemoteVerification, record.RemoteVerification);
        Assert.Equal("blocked", record.PriorStatus);
        // OwnershipReleased equals the manifest size ('clients').
        Assert.Equal(1, record.OwnershipReleased);
        Assert.Equal(0, record.BatchesFenced);

        // ONE immutable record row with the exact columns.
        Assert.Equal(1, await ScalarAsync("SELECT COUNT(*) FROM etl_run_resolutions"));
        Assert.Equal(record.ResolutionId.ToString("D"), await ScalarStringAsync($"SELECT resolution_id FROM etl_run_resolutions WHERE run_id='{runId:D}'"));
        Assert.Equal(now.ToUniversalTime().ToString("O"), await ScalarStringAsync($"SELECT resolved_at_utc FROM etl_run_resolutions WHERE run_id='{runId:D}'"));
        Assert.Equal("operator-1", await ScalarStringAsync($"SELECT operator_id FROM etl_run_resolutions WHERE run_id='{runId:D}'"));
        Assert.Equal("abandon", await ScalarStringAsync($"SELECT decision FROM etl_run_resolutions WHERE run_id='{runId:D}'"));
        Assert.Equal(RemoteVerification, await ScalarStringAsync($"SELECT remote_verification FROM etl_run_resolutions WHERE run_id='{runId:D}'"));
        Assert.Equal("1", await ScalarStringAsync($"SELECT workers_quiesced FROM etl_run_resolutions WHERE run_id='{runId:D}'"));
        Assert.Equal("blocked", await ScalarStringAsync($"SELECT prior_status FROM etl_run_resolutions WHERE run_id='{runId:D}'"));
        Assert.Equal("1", await ScalarStringAsync($"SELECT ownership_released FROM etl_run_resolutions WHERE run_id='{runId:D}'"));
        Assert.Equal("0", await ScalarStringAsync($"SELECT batches_fenced FROM etl_run_resolutions WHERE run_id='{runId:D}'"));

        // The run is marked resolved; its status stays 'blocked' — never succeeded.
        Assert.Equal("blocked", await RunStatusAsync(runId));
        Assert.Equal(now.ToUniversalTime().ToString("O"), await ScalarStringAsync($"SELECT resolved_at_utc FROM etl_runs WHERE run_id='{runId:D}'"));

        // The run's ownership row is released manual_release; the epoch is the ABA
        // token — released, never incremented.
        Assert.NotNull(await ScalarStringAsync("SELECT released_at_utc FROM etl_entity_ownership WHERE entity_name='clients'"));
        Assert.Equal("manual_release", await ScalarStringAsync("SELECT release_reason FROM etl_entity_ownership WHERE entity_name='clients'"));
        Assert.Equal(epochBefore, await ScalarStringAsync("SELECT ownership_epoch FROM etl_entity_ownership WHERE entity_name='clients'"));
        Assert.Equal(runId.ToString("D"), await ScalarStringAsync("SELECT owner_run_id FROM etl_entity_ownership WHERE entity_name='clients'"));

        // Bindings are immutable evidence — intact.
        Assert.Equal(1, await ScalarAsync($"SELECT COUNT(*) FROM etl_run_ownership_bindings WHERE run_id='{runId:D}'"));

        // The job stays 'blocked' — a resolved run never becomes 'finished'.
        Assert.Equal("blocked", await ScalarStringAsync($"SELECT status FROM etl_jobs WHERE run_id='{runId:D}'"));

        // Watermarks, batch and attempt rows are untouched evidence.
        Assert.Equal(watermarksBefore, await SnapshotRowsAsync("SELECT * FROM watermarks ORDER BY entity_name;"));
        Assert.Equal(batchesBefore, await SnapshotRowsAsync($"SELECT * FROM etl_batches WHERE run_id='{runId:D}' ORDER BY batch_id;"));
        Assert.Equal(attemptsBefore, await SnapshotRowsAsync("SELECT * FROM etl_batch_send_attempts ORDER BY attempt_id;"));
    }

    [Fact]
    public async Task V2_Resolving_a_failed_run_with_retry_frees_the_entity_and_the_epoch_moves_on_reacquire()
    {
        // A run failed mid-extraction with a registered 'ready' batch that
        // TerminateRun fenced to dead_letter RUN_FAILED.
        var runId = await NewRunAsync("clients");
        Assert.IsType<EtlEntityBeginOutcome.Begun>(await _store.BeginEtlEntityExtractionAsync(runId, ClaimOf(runId), Request("clients"), CancellationToken.None));
        Assert.IsType<EtlBatchRegistrationOutcome.Registered>(await _store.RegisterGuardedEtlBatchAsync(MakeBatch(runId, "clients", 5), ClaimOf(runId), CancellationToken.None));
        Assert.IsType<EtlRunTerminationOutcome.Applied>(await _store.FailEtlRunAsync(runId, ClaimOf(runId), "extraction failed", CancellationToken.None));
        Assert.Equal("failed", await RunStatusAsync(runId));
        Assert.Equal("RUN_FAILED", await ScalarStringAsync($"SELECT quarantine_code FROM etl_batches WHERE run_id='{runId:D}'"));

        // While the unresolved failure still owns 'clients', a new job only defers.
        var (newRunId, newJobId) = await NewPendingJobRunAsync(QueryMode, ["clients"]);
        var deferred = Assert.IsType<EtlJobClaimOutcome.Deferred>(
            await _store.TryClaimEtlJobAsync(newJobId, "dispatcher-2", DateTimeOffset.UtcNow.AddSeconds(-1), DateTimeOffset.UtcNow, CancellationToken.None));
        Assert.Equal(EtlJobDeferralReason.BusyEntity, deferred.Reason);

        var outcome = await _store.ResolveEtlRunAsync(Resolution(runId, EtlRunResolutionDecision.Retry), DateTimeOffset.UtcNow, CancellationToken.None);

        var record = Assert.IsType<EtlRunResolutionOutcome.Resolved>(outcome).Record;
        Assert.Equal("failed", record.PriorStatus);
        Assert.Equal(1, record.OwnershipReleased);
        Assert.Equal("retry", await ScalarStringAsync($"SELECT decision FROM etl_run_resolutions WHERE run_id='{runId:D}'"));
        Assert.Equal("manual_release", await ScalarStringAsync("SELECT release_reason FROM etl_entity_ownership WHERE entity_name='clients'"));

        // The NEW manual job claims the freed entity — reacquisition bumps the epoch.
        var claimed = Assert.IsType<EtlJobClaimOutcome.Claimed>(
            await _store.TryClaimEtlJobAsync(newJobId, "dispatcher-2", DateTimeOffset.UtcNow.AddMinutes(5), DateTimeOffset.UtcNow, CancellationToken.None));
        Assert.Equal(newRunId, claimed.Claim.RunId);
        Assert.Equal(newRunId.ToString("D"), await ScalarStringAsync("SELECT owner_run_id FROM etl_entity_ownership WHERE entity_name='clients'"));
        Assert.Equal("2", await ScalarStringAsync("SELECT ownership_epoch FROM etl_entity_ownership WHERE entity_name='clients'"));
        Assert.Null(await ScalarStringAsync("SELECT released_at_utc FROM etl_entity_ownership WHERE entity_name='clients'"));
        Assert.Equal("2", await ScalarStringAsync($"SELECT expected_epoch FROM etl_run_ownership_bindings WHERE run_id='{newRunId:D}' AND entity_name='clients'"));
    }

    [Fact]
    public async Task V3_Resolution_fences_a_still_ready_batch_and_leaves_terminal_batches_untouched()
    {
        var (runId, terminalBatchId, _) = await BlockedRunAsync("clients");
        var terminalBefore = await SnapshotRowsAsync($"SELECT * FROM etl_batches WHERE batch_id='{terminalBatchId:D}'");
        // A stray 'ready' batch the eager block never reached (injected evidence of a
        // writer that bypassed the block).
        var stray = Guid.NewGuid();
        await ExecuteSqlAsync(
            "INSERT INTO etl_batches(batch_id,run_id,entity_name,schema_version,file_path,status,row_count,sha256,compressed_size,uncompressed_size,created_at_utc) VALUES($b,$run,'clients',1,'spool/stray.gz','ready',3,'h',10,10,$now);",
            ("$b", stray.ToString("D")), ("$run", runId.ToString("D")), ("$now", Now()));

        var outcome = await _store.ResolveEtlRunAsync(Resolution(runId), DateTimeOffset.UtcNow, CancellationToken.None);

        var record = Assert.IsType<EtlRunResolutionOutcome.Resolved>(outcome).Record;
        Assert.Equal(1, record.BatchesFenced);
        Assert.Equal("dead_letter", await ScalarStringAsync($"SELECT status FROM etl_batches WHERE batch_id='{stray:D}'"));
        Assert.Equal("RUN_BLOCKED", await ScalarStringAsync($"SELECT quarantine_code FROM etl_batches WHERE batch_id='{stray:D}'"));
        Assert.Equal("RUN_RESOLVED", await ScalarStringAsync($"SELECT last_error FROM etl_batches WHERE batch_id='{stray:D}'"));
        Assert.Equal("1", await ScalarStringAsync($"SELECT batches_fenced FROM etl_run_resolutions WHERE run_id='{runId:D}'"));
        // Already-terminal evidence is never rewritten.
        Assert.Equal(terminalBefore, await SnapshotRowsAsync($"SELECT * FROM etl_batches WHERE batch_id='{terminalBatchId:D}'"));
    }

    [Fact]
    public async Task V4_Resolving_a_blocked_scheduled_run_releases_the_schedule_key()
    {
        // A real scheduled claim: 'running' with ownership (owner_job_id NULL) and
        // bindings; dead-process recovery blocks it — an unresolved blocked
        // scheduled run still holds its key.
        var ensured = Assert.IsType<EtlScheduledRunEnsureOutcome.Created>(
            await _store.EnsureScheduledEtlRunAsync(EnsureRequest("nightly", ["clients"]), DateTimeOffset.UtcNow, CancellationToken.None));
        var runId = ensured.RunId;
        Assert.IsType<EtlScheduledRunClaimOutcome.Claimed>(
            await _store.TryClaimScheduledRunAsync(runId, "scheduler-1", DateTimeOffset.UtcNow, CancellationToken.None));
        var recovered = await _store.RecoverInterruptedEtlRunsAsync(CancellationToken.None);
        Assert.Equal(1, recovered.RunsBlocked);
        Assert.Equal("blocked", await RunStatusAsync(runId));
        Assert.Equal(1, await ScalarAsync($"SELECT COUNT(*) FROM etl_entity_ownership WHERE owner_run_id='{runId:D}' AND released_at_utc IS NULL"));

        var before = Assert.IsType<EtlScheduledRunEnsureOutcome.ActiveExisting>(
            await _store.EnsureScheduledEtlRunAsync(EnsureRequest("nightly", ["clients"]), DateTimeOffset.UtcNow, CancellationToken.None));
        Assert.Equal(runId, before.RunId);
        Assert.Equal("blocked", before.Status);

        var record = Assert.IsType<EtlRunResolutionOutcome.Resolved>(
            await _store.ResolveEtlRunAsync(Resolution(runId), DateTimeOffset.UtcNow, CancellationToken.None)).Record;
        Assert.Equal("blocked", record.PriorStatus);
        Assert.Equal(1, record.OwnershipReleased);
        Assert.Equal("manual_release", await ScalarStringAsync("SELECT release_reason FROM etl_entity_ownership WHERE entity_name='clients'"));

        // The key is released: the next tick mints a fresh pending successor.
        var after = Assert.IsType<EtlScheduledRunEnsureOutcome.Created>(
            await _store.EnsureScheduledEtlRunAsync(EnsureRequest("nightly", ["clients"]), DateTimeOffset.UtcNow, CancellationToken.None));
        Assert.NotEqual(runId, after.RunId);
        Assert.Equal("pending", await RunStatusAsync(after.RunId));
    }

    [Fact]
    public async Task V5_Ownership_release_is_epoch_exact_and_never_touches_another_runs_rows()
    {
        var (runA, _, _) = await BlockedRunAsync("clients", "orders");
        // A second run owns 'payments' — evidence of another live owner.
        var runB = await NewRunAsync("payments");
        var paymentsBefore = await SnapshotRowsAsync("SELECT * FROM etl_entity_ownership WHERE entity_name='payments'");

        // runA's 'orders' row was released then re-acquired by a NEWER run — the
        // epoch moved past runA's binding; resolution must not release it.
        await ExecuteSqlAsync(
            "UPDATE etl_entity_ownership SET released_at_utc=$now, release_reason='manual_release', updated_at_utc=$now WHERE entity_name='orders' AND owner_run_id=$run;",
            ("$run", runA.ToString("D")), ("$now", Now()));
        var (runC, jobC) = await NewPendingJobRunAsync(QueryMode, ["orders"]);
        Assert.IsType<EtlJobClaimOutcome.Claimed>(
            await _store.TryClaimEtlJobAsync(jobC, "dispatcher-2", DateTimeOffset.UtcNow.AddMinutes(5), DateTimeOffset.UtcNow, CancellationToken.None));
        Assert.Equal("2", await ScalarStringAsync("SELECT ownership_epoch FROM etl_entity_ownership WHERE entity_name='orders'"));
        var ordersBefore = await SnapshotRowsAsync("SELECT * FROM etl_entity_ownership WHERE entity_name='orders'");

        var outcome = await _store.ResolveEtlRunAsync(Resolution(runA), DateTimeOffset.UtcNow, CancellationToken.None);

        var record = Assert.IsType<EtlRunResolutionOutcome.Resolved>(outcome).Record;
        // Only 'clients' — the epoch-bound active row of the resolved run.
        Assert.Equal(1, record.OwnershipReleased);
        Assert.Equal("1", await ScalarStringAsync($"SELECT ownership_released FROM etl_run_resolutions WHERE run_id='{runA:D}'"));
        Assert.Equal("manual_release", await ScalarStringAsync("SELECT release_reason FROM etl_entity_ownership WHERE entity_name='clients'"));
        Assert.NotNull(await ScalarStringAsync("SELECT released_at_utc FROM etl_entity_ownership WHERE entity_name='clients'"));
        // The re-acquired row of the other run is untouched.
        Assert.Equal(ordersBefore, await SnapshotRowsAsync("SELECT * FROM etl_entity_ownership WHERE entity_name='orders'"));
        Assert.Equal(runC.ToString("D"), await ScalarStringAsync("SELECT owner_run_id FROM etl_entity_ownership WHERE entity_name='orders'"));
        Assert.Equal("2", await ScalarStringAsync("SELECT ownership_epoch FROM etl_entity_ownership WHERE entity_name='orders'"));
        Assert.Null(await ScalarStringAsync("SELECT released_at_utc FROM etl_entity_ownership WHERE entity_name='orders'"));
        Assert.Equal(paymentsBefore, await SnapshotRowsAsync("SELECT * FROM etl_entity_ownership WHERE entity_name='payments'"));
        Assert.Equal(runB.ToString("D"), await ScalarStringAsync("SELECT owner_run_id FROM etl_entity_ownership WHERE entity_name='payments'"));
        // Both bindings of the resolved run stay as evidence.
        Assert.Equal(2, await ScalarAsync($"SELECT COUNT(*) FROM etl_run_ownership_bindings WHERE run_id='{runA:D}'"));
    }

    [Fact]
    public async Task V6_A_legacy_failed_run_without_ownership_or_bindings_resolves_cleanly()
    {
        // Pre-ownership shape: a failed run with no job, no ownership, no bindings.
        var runId = Guid.NewGuid();
        await ExecuteSqlAsync(
            "INSERT INTO etl_runs(run_id,mode,requested_entities_json,status,started_at_utc,finished_at_utc,error_count,last_error,created_at_utc,updated_at_utc) VALUES($run,'incremental','[\"clients\"]','failed',$now,$now,1,'legacy failure',$now,$now);",
            ("$run", runId.ToString("D")), ("$now", Now()));

        var outcome = await _store.ResolveEtlRunAsync(Resolution(runId), DateTimeOffset.UtcNow, CancellationToken.None);

        var record = Assert.IsType<EtlRunResolutionOutcome.Resolved>(outcome).Record;
        Assert.Equal("failed", record.PriorStatus);
        Assert.Equal(0, record.OwnershipReleased);
        Assert.Equal(0, record.BatchesFenced);
        Assert.Equal(1, await ScalarAsync($"SELECT COUNT(*) FROM etl_run_resolutions WHERE run_id='{runId:D}'"));
        Assert.NotNull(await ScalarStringAsync($"SELECT resolved_at_utc FROM etl_runs WHERE run_id='{runId:D}'"));
        Assert.Equal("failed", await RunStatusAsync(runId));
    }

    // ---------- AlreadyResolved ----------

    [Fact]
    public async Task V7_A_second_resolve_replays_the_original_record_with_zero_writes()
    {
        var (runId, _, _) = await BlockedRunAsync("clients");
        var first = Assert.IsType<EtlRunResolutionOutcome.Resolved>(
            await _store.ResolveEtlRunAsync(Resolution(runId, EtlRunResolutionDecision.Abandon, "operator-1"), DateTimeOffset.UtcNow, CancellationToken.None)).Record;
        var writesBefore = await SnapshotWritesAsync();

        // Even a different decision/operator can never create a second record.
        var second = await _store.ResolveEtlRunAsync(
            Resolution(runId, EtlRunResolutionDecision.Retry, "operator-2", "different verification"),
            DateTimeOffset.UtcNow.AddMinutes(1), CancellationToken.None);

        var replayed = Assert.IsType<EtlRunResolutionOutcome.AlreadyResolved>(second).Record;
        Assert.Equal(first, replayed);
        Assert.Equal("operator-1", await ScalarStringAsync($"SELECT operator_id FROM etl_run_resolutions WHERE run_id='{runId:D}'"));
        Assert.Equal("abandon", await ScalarStringAsync($"SELECT decision FROM etl_run_resolutions WHERE run_id='{runId:D}'"));
        await AssertZeroWritesAsync(writesBefore);
    }

    [Fact]
    public async Task V8_Concurrent_resolves_on_two_stores_commit_exactly_one_record()
    {
        var (runId, _, _) = await BlockedRunAsync("clients");
        var other = new SqliteAgentStore(_factory, new SqliteMigrator(_factory));
        var barrier = new Barrier(2);
        var now = DateTimeOffset.UtcNow;

        var first = Task.Run(async () =>
        {
            barrier.SignalAndWait();
            return await _store.ResolveEtlRunAsync(Resolution(runId, EtlRunResolutionDecision.Abandon, "operator-a"), now, CancellationToken.None);
        });
        var second = Task.Run(async () =>
        {
            barrier.SignalAndWait();
            return await other.ResolveEtlRunAsync(Resolution(runId, EtlRunResolutionDecision.Abandon, "operator-b"), now, CancellationToken.None);
        });
        var results = await Task.WhenAll(first, second);

        var resolved = Assert.Single(results, r => r is EtlRunResolutionOutcome.Resolved);
        var replayed = Assert.Single(results, r => r is EtlRunResolutionOutcome.AlreadyResolved);
        var resolvedRecord = ((EtlRunResolutionOutcome.Resolved)resolved).Record;
        var replayedRecord = ((EtlRunResolutionOutcome.AlreadyResolved)replayed).Record;
        Assert.Equal(resolvedRecord, replayedRecord);
        Assert.Equal(1, await ScalarAsync("SELECT COUNT(*) FROM etl_run_resolutions"));
        Assert.Equal(1, await ScalarAsync($"SELECT COUNT(*) FROM etl_run_resolutions WHERE run_id='{runId:D}' AND resolution_id='{resolvedRecord.ResolutionId:D}'"));
    }

    // ---------- Refused (typed reason + zero writes) ----------

    [Fact]
    public async Task F1_An_unknown_run_is_refused_with_zero_writes()
    {
        await BlockedRunAsync("clients");
        var writesBefore = await SnapshotWritesAsync();

        var outcome = await _store.ResolveEtlRunAsync(Resolution(Guid.NewGuid()), DateTimeOffset.UtcNow, CancellationToken.None);

        Assert.Equal(EtlRunResolutionRefusal.RunNotFound, Assert.IsType<EtlRunResolutionOutcome.Refused>(outcome).Reason);
        await AssertZeroWritesAsync(writesBefore);
    }

    [Theory]
    [InlineData("pending")]
    [InlineData("running")]
    [InlineData("uploading")]
    [InlineData("completing")]
    [InlineData("succeeded")]
    [InlineData("cancelled")]
    public async Task F2_A_non_failed_non_blocked_run_is_never_resolved(string status)
    {
        var (runId, _) = await NewPendingJobRunAsync(QueryMode, ["clients"]);
        // Settle the job out of the live-dispatch set so only the run-status
        // precondition can refuse.
        await ExecuteSqlAsync("UPDATE etl_jobs SET status='blocked' WHERE run_id=$run;", ("$run", runId.ToString("D")));
        if (!string.Equals(status, "pending", StringComparison.Ordinal))
        {
            await ExecuteSqlAsync("UPDATE etl_runs SET status=$status WHERE run_id=$run;", ("$status", status), ("$run", runId.ToString("D")));
        }
        var writesBefore = await SnapshotWritesAsync();

        var outcome = await _store.ResolveEtlRunAsync(Resolution(runId), DateTimeOffset.UtcNow, CancellationToken.None);

        Assert.Equal(EtlRunResolutionRefusal.RunNotResolvable, Assert.IsType<EtlRunResolutionOutcome.Refused>(outcome).Reason);
        Assert.Equal(status, await RunStatusAsync(runId));
        await AssertZeroWritesAsync(writesBefore);
    }

    [Fact]
    public async Task F3_A_blocked_run_with_an_admitted_send_attempt_is_refused()
    {
        var (runId, _, attemptId) = await BlockedRunAsync("clients");
        // A sender was admitted and never recorded an outcome — its remote effect is
        // unknowable; durable quiescence is NOT proven.
        await ExecuteSqlAsync(
            "UPDATE etl_batch_send_attempts SET outcome='admitted', finished_at_utc=NULL WHERE attempt_id=$a;",
            ("$a", attemptId.ToString("D")));
        var writesBefore = await SnapshotWritesAsync();

        var outcome = await _store.ResolveEtlRunAsync(Resolution(runId), DateTimeOffset.UtcNow, CancellationToken.None);

        Assert.Equal(EtlRunResolutionRefusal.AdmittedSendAttempt, Assert.IsType<EtlRunResolutionOutcome.Refused>(outcome).Reason);
        await AssertZeroWritesAsync(writesBefore);
    }

    [Fact]
    public async Task F4_A_blocked_run_with_a_ledger_less_uploading_batch_is_refused()
    {
        var (runId, _, _) = await BlockedRunAsync("clients");
        // A legacy in-flight batch: 'uploading' with no send-attempt ledger row.
        var inFlight = Guid.NewGuid();
        await ExecuteSqlAsync(
            "INSERT INTO etl_batches(batch_id,run_id,entity_name,schema_version,file_path,status,row_count,sha256,compressed_size,uncompressed_size,created_at_utc) VALUES($b,$run,'clients',1,'spool/inflight.gz','uploading',3,'h',10,10,$now);",
            ("$b", inFlight.ToString("D")), ("$run", runId.ToString("D")), ("$now", Now()));
        var writesBefore = await SnapshotWritesAsync();

        var outcome = await _store.ResolveEtlRunAsync(Resolution(runId), DateTimeOffset.UtcNow, CancellationToken.None);

        Assert.Equal(EtlRunResolutionRefusal.BatchInFlight, Assert.IsType<EtlRunResolutionOutcome.Refused>(outcome).Reason);
        await AssertZeroWritesAsync(writesBefore);
    }

    [Fact]
    public async Task F5_A_blocked_run_with_a_live_extraction_claim_is_refused()
    {
        var (runId, _, _) = await BlockedRunAsync("clients");
        await ExecuteSqlAsync(
            "UPDATE etl_runs SET extraction_claim_id=$claim, extraction_claim_owner_id='ghost-worker', extraction_claim_acquired_at_utc=$now WHERE run_id=$run;",
            ("$claim", Guid.NewGuid().ToString("D")), ("$run", runId.ToString("D")), ("$now", Now()));
        var writesBefore = await SnapshotWritesAsync();

        var outcome = await _store.ResolveEtlRunAsync(Resolution(runId), DateTimeOffset.UtcNow, CancellationToken.None);

        Assert.Equal(EtlRunResolutionRefusal.LiveExtractionClaim, Assert.IsType<EtlRunResolutionOutcome.Refused>(outcome).Reason);
        await AssertZeroWritesAsync(writesBefore);
    }

    [Fact]
    public async Task F6_A_blocked_run_with_a_live_completion_claim_is_refused()
    {
        var (runId, _, _) = await BlockedRunAsync("clients");
        await ExecuteSqlAsync(
            "UPDATE etl_runs SET completion_claim_id=$claim, completion_claim_owner_id='ghost-finalizer', completion_claim_acquired_at_utc=$now WHERE run_id=$run;",
            ("$claim", Guid.NewGuid().ToString("D")), ("$run", runId.ToString("D")), ("$now", Now()));
        var writesBefore = await SnapshotWritesAsync();

        var outcome = await _store.ResolveEtlRunAsync(Resolution(runId), DateTimeOffset.UtcNow, CancellationToken.None);

        Assert.Equal(EtlRunResolutionRefusal.LiveCompletionClaim, Assert.IsType<EtlRunResolutionOutcome.Refused>(outcome).Reason);
        await AssertZeroWritesAsync(writesBefore);
    }

    [Theory]
    [InlineData("pending")]
    [InlineData("deferred")]
    [InlineData("running")]
    public async Task F7_A_blocked_run_whose_job_is_still_dispatchable_is_refused(string jobStatus)
    {
        var (runId, _, _) = await BlockedRunAsync("clients");
        await ExecuteSqlAsync(
            "UPDATE etl_jobs SET status=$status WHERE run_id=$run;",
            ("$status", jobStatus), ("$run", runId.ToString("D")));
        var writesBefore = await SnapshotWritesAsync();

        var outcome = await _store.ResolveEtlRunAsync(Resolution(runId), DateTimeOffset.UtcNow, CancellationToken.None);

        Assert.Equal(EtlRunResolutionRefusal.LiveJobDispatch, Assert.IsType<EtlRunResolutionOutcome.Refused>(outcome).Reason);
        await AssertZeroWritesAsync(writesBefore);
    }

    // ---------- Invalid requests ----------

    [Theory]
    [InlineData("null_request")]
    [InlineData("blank_operator")]
    [InlineData("whitespace_operator")]
    [InlineData("blank_remote")]
    [InlineData("not_quiesced")]
    [InlineData("undefined_decision")]
    public async Task I1_Invalid_resolution_requests_throw_with_zero_writes(string kind)
    {
        var (runId, _, _) = await BlockedRunAsync("clients");
        var request = kind switch
        {
            "null_request" => null,
            "blank_operator" => Resolution(runId, operatorId: ""),
            "whitespace_operator" => Resolution(runId, operatorId: "   "),
            "blank_remote" => Resolution(runId, remoteVerification: ""),
            "not_quiesced" => Resolution(runId, quiesced: false),
            "undefined_decision" => Resolution(runId, decision: (EtlRunResolutionDecision)99),
            _ => throw new InvalidOperationException(kind)
        };
        var writesBefore = await SnapshotWritesAsync();

        await Assert.ThrowsAnyAsync<ArgumentException>(
            () => _store.ResolveEtlRunAsync(request!, DateTimeOffset.UtcNow, CancellationToken.None));

        await AssertZeroWritesAsync(writesBefore);
    }

    // ---------- recovery interaction ----------

    [Fact]
    public async Task X1_Recovery_after_resolution_changes_nothing_on_the_resolved_run()
    {
        var (runId, _, _) = await BlockedRunAsync("clients");
        Assert.IsType<EtlRunResolutionOutcome.Resolved>(
            await _store.ResolveEtlRunAsync(Resolution(runId), DateTimeOffset.UtcNow, CancellationToken.None));
        var writesBefore = await SnapshotWritesAsync();

        var recovered = await _store.RecoverInterruptedEtlRunsAsync(CancellationToken.None);

        // A resolved 'blocked' run holds no live claim, no running state, no admitted
        // attempt — recovery can only leave it (and everything else) untouched.
        Assert.Equal(0, recovered.RunsBlocked);
        Assert.Equal(0, recovered.AttemptsOrphaned);
        Assert.Equal("blocked", await RunStatusAsync(runId));
        Assert.NotNull(await ScalarStringAsync($"SELECT resolved_at_utc FROM etl_runs WHERE run_id='{runId:D}'"));
        await AssertZeroWritesAsync(writesBefore);
    }

    // ---------- helpers ----------

    private Guid ClaimOf(Guid runId) => _extractionClaims[runId];
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
        new("orders", "Document_Orders", "Ref_Key", "Date", "Posted", ["Ref_Key", "Date", "Posted"], "incremental", 500, 10),
        new("payments", "Document_Payments", "Ref_Key", "Date", "Posted", ["Ref_Key", "Date", "Posted"], "incremental", 500, 10)
    ];
    private static EtlEntityExtractionRequest Request(string entity) =>
        new(entity, DefinitionJson(entity), SourceNamespace, QueryMode,
            JsonSerializer.Serialize(new EtlCursor(DateTimeOffset.UtcNow, null), JsonOptions));
    private static EtlScheduledRunRequest EnsureRequest(string scheduleKey, string[] entities) =>
        new(scheduleKey, "incremental", entities.Select(e => MakeEntities().Single(x => x.EntityCode == e)).ToArray(), 7);
    private static EtlBatch MakeBatch(Guid runId, string entity, int rowCount) =>
        new(Guid.NewGuid(), runId, entity, 1, $"spool/{runId:N}-{entity}-{Guid.NewGuid():N}.gz", EtlBatchStatus.Ready, rowCount,
            null, FinalClients, "hash-" + Guid.NewGuid().ToString("N"), 1000, 4000, 0, DateTimeOffset.UtcNow);
    private static EtlRunResolutionRequest Resolution(Guid runId, EtlRunResolutionDecision decision = EtlRunResolutionDecision.Abandon,
        string operatorId = "operator-1", string remoteVerification = RemoteVerification, bool quiesced = true) =>
        new(runId, operatorId, decision, remoteVerification, quiesced);

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

    // Full new-path pipeline: claim → extract one batch per entity → seal → run
    // 'uploading' with 'ready' batches and live bound-epoch ownership.
    private async Task<Guid> SealedRunAsync(params string[] entities)
    {
        var runId = await NewRunAsync(entities);
        var finals = new Dictionary<string, EtlCursor>(StringComparer.Ordinal)
        {
            ["clients"] = FinalClients,
            ["orders"] = FinalOrders,
            ["payments"] = FinalPayments
        };
        foreach (var entity in entities)
        {
            await BeginExtractCompleteAsync(runId, entity, CursorJson(finals[entity]), 5, ClaimOf(runId));
        }
        Assert.IsType<EtlRunSealOutcome.Sealed>(await _store.SealEtlRunExtractionAsync(runId, ClaimOf(runId), CancellationToken.None));
        return runId;
    }

    // The canonical resolvable state: a sealed run whose only send was admitted then
    // reported with an uncertain outcome — attempt 'unknown', batch dead_letter
    // UPLOAD_OUTCOME_UNKNOWN, run + job 'blocked', ownership retained.
    private async Task<(Guid RunId, Guid BatchId, Guid AttemptId)> BlockedRunAsync(params string[] entities)
    {
        var runId = await SealedRunAsync(entities);
        var batchId = await ReadyBatchIdAsync(runId);
        var claim = Assert.IsType<EtlBatchUploadClaimOutcome.Claimed>(
            await _store.TryClaimBatchUploadAsync(batchId, "uploader-1", DateTimeOffset.UtcNow, 5, CancellationToken.None)).Claim;
        var blocked = Assert.IsType<EtlBatchSendFailureOutcome.Blocked>(
            await _store.FailClaimedBatchSendAsync(batchId, claim.AttemptId, "timeout after 30s — response never arrived", null, CancellationToken.None));
        Assert.Equal("UPLOAD_OUTCOME_UNKNOWN", blocked.Code);
        Assert.Equal("unknown", await ScalarStringAsync($"SELECT outcome FROM etl_batch_send_attempts WHERE attempt_id='{claim.AttemptId:D}'"));
        Assert.Equal("blocked", await RunStatusAsync(runId));
        Assert.Equal("blocked", await ScalarStringAsync($"SELECT status FROM etl_jobs WHERE run_id='{runId:D}'"));
        return (runId, batchId, claim.AttemptId);
    }

    private async Task BeginExtractCompleteAsync(Guid runId, string entity, string finalJson, int batchRows, Guid claim)
    {
        Assert.IsType<EtlEntityBeginOutcome.Begun>(await _store.BeginEtlEntityExtractionAsync(runId, claim, Request(entity), CancellationToken.None));
        Assert.IsType<EtlBatchRegistrationOutcome.Registered>(await _store.RegisterGuardedEtlBatchAsync(MakeBatch(runId, entity, batchRows), claim, CancellationToken.None));
        Assert.IsType<EtlEntityCompletionOutcome.Completed>(await _store.CompleteEtlEntityExtractionAsync(runId, claim, entity, finalJson, 1, CancellationToken.None));
    }

    private async Task<Guid> ReadyBatchIdAsync(Guid runId, string? entity = null)
    {
        var filter = entity is null ? "" : $" AND entity_name='{entity}'";
        return Guid.Parse((await ScalarStringAsync($"SELECT batch_id FROM etl_batches WHERE run_id='{runId:D}' AND status='ready'{filter}"))!);
    }

    private async Task<string?> RunStatusAsync(Guid runId) => await ScalarStringAsync($"SELECT status FROM etl_runs WHERE run_id='{runId:D}'");

    private async Task<List<string>[]> SnapshotWritesAsync()
    {
        var snapshots = new List<string>[ZeroWriteSnapshots.Length];
        for (var index = 0; index < ZeroWriteSnapshots.Length; index++)
        {
            snapshots[index] = await SnapshotRowsAsync(ZeroWriteSnapshots[index]);
        }
        return snapshots;
    }

    private async Task AssertZeroWritesAsync(List<string>[] before)
    {
        for (var index = 0; index < ZeroWriteSnapshots.Length; index++)
        {
            Assert.Equal(before[index], await SnapshotRowsAsync(ZeroWriteSnapshots[index]));
        }
    }

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
