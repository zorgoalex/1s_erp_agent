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
/// O1 ownership regressions (schema v7): durable per-entity ownership + immutable epoch
/// bindings + the run-level extraction claim fence, fair manual-job claim/enumeration
/// (eligibility before LIMIT, elder-overlap authority inside direct claims), all-entity
/// acquisition savepoint rollback, exact manifest==bindings==active-ownership gates on
/// every extraction/completion mutation, and finalize-time ownership release semantics
/// (RAISE(IGNORE) controlled mismatch vs. RAISE(ABORT) whole-transaction rollback).
/// All against a real migrated temporary SQLite database; nothing is wired to workers.
/// </summary>
public sealed class EtlOwnershipO1Tests : IAsyncLifetime
{
    private const string SourceNamespace = "onec-infobase-a";
    private const string QueryMode = "bootstrap_full";
    private static readonly TimeSpan GateTimeout = TimeSpan.FromSeconds(15);
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);
    private static readonly EtlCursor FinalClients = new(DateTimeOffset.Parse("2026-09-19T09:59:00.0000000+00:00", CultureInfo.InvariantCulture), "A10");
    private static readonly EtlCursor FinalOrders = new(DateTimeOffset.Parse("2026-09-19T09:58:00.0000000+00:00", CultureInfo.InvariantCulture), "O8");

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

    // ---------- claim + enumeration ----------

    [Fact]
    public async Task Claim_mints_fresh_extraction_guid_and_acquires_full_manifest_ownership()
    {
        var (runId, jobId) = await NewPendingJobRunAsync("bootstrap_full", ["clients", "orders"]);

        var outcome = await _store.TryClaimEtlJobAsync(jobId, "dispatcher-1", DateTimeOffset.UtcNow.AddMinutes(5), DateTimeOffset.UtcNow, CancellationToken.None);

        var claimed = Assert.IsType<EtlJobClaimOutcome.Claimed>(outcome);
        Assert.Equal(runId, claimed.Claim.RunId);
        Assert.NotEqual(Guid.Empty, claimed.Claim.ExtractionClaimId);
        Assert.Equal("running", await RunStatusAsync(runId));
        Assert.Equal(claimed.Claim.ExtractionClaimId.ToString("D"), await ScalarStringAsync($"SELECT extraction_claim_id FROM etl_runs WHERE run_id='{runId:D}'"));
        Assert.Equal(2, await ScalarAsync($"SELECT COUNT(*) FROM etl_entity_ownership WHERE owner_run_id='{runId:D}' AND released_at_utc IS NULL AND ownership_epoch=1"));
        Assert.Equal(2, await ScalarAsync($"SELECT COUNT(*) FROM etl_run_ownership_bindings WHERE run_id='{runId:D}' AND expected_epoch=1"));
        Assert.Equal(1, await ScalarAsync($"SELECT claim_attempt_count FROM etl_jobs WHERE job_id='{jobId:D}'"));
        Assert.Equal("running", await ScalarStringAsync($"SELECT status FROM etl_jobs WHERE job_id='{jobId:D}'"));
        // A second claim of the same job is read-only NotClaimable — never a second mint.
        Assert.IsType<EtlJobClaimOutcome.NotClaimable>(await _store.TryClaimEtlJobAsync(jobId, "dispatcher-2", DateTimeOffset.UtcNow, DateTimeOffset.UtcNow, CancellationToken.None));
        Assert.Equal(1, await ScalarAsync($"SELECT claim_attempt_count FROM etl_jobs WHERE job_id='{jobId:D}'"));
    }

    [Fact]
    public async Task Enumeration_applies_eligibility_before_limit_busy_head_cannot_hide_disjoint_work()
    {
        // Claimed run owns 'clients'; pending job A overlaps (busy head), pending job B
        // is disjoint. LIMIT 1 must still surface B — eligibility is evaluated in SQL
        // before LIMIT, ranked by run (created_at_utc, run_id).
        await NewRunAsync("clients");
        var (_, busyJobId) = await NewPendingJobRunAsync("bootstrap_full", ["clients"], createdAtUtc: "2026-09-20T00:00:00.0000000+00:00");
        var (runB, jobB) = await NewPendingJobRunAsync("bootstrap_full", ["orders"], createdAtUtc: "2026-09-20T00:00:01.0000000+00:00");

        var page = await _store.GetDispatchableEtlJobsAsync(1, DateTimeOffset.UtcNow, CancellationToken.None);

        var job = Assert.Single(page.Jobs);
        Assert.Equal(jobB, job.JobId);
        Assert.Equal(runB, job.RunId);
        Assert.DoesNotContain(page.Jobs, j => j.JobId == busyJobId);
    }

    [Fact]
    public async Task Direct_claim_of_younger_overlapping_run_defers_until_elder_resolves()
    {
        // Same older-overlap authority inside direct claims as in enumeration — the
        // enumerator can never be used to bypass it. Order key is (created_at_utc,
        // run_id), a deterministic total order, not arrival FIFO.
        var (elderRun, _) = await NewPendingJobRunAsync("bootstrap_full", ["clients"], createdAtUtc: "2026-09-20T00:00:00.0000000+00:00");
        var (youngerRun, youngerJob) = await NewPendingJobRunAsync("bootstrap_full", ["clients"], createdAtUtc: "2026-09-20T00:00:01.0000000+00:00");

        var outcome = await _store.TryClaimEtlJobAsync(youngerJob, "dispatcher-1", DateTimeOffset.UtcNow.AddMinutes(5), DateTimeOffset.UtcNow, CancellationToken.None);

        var deferred = Assert.IsType<EtlJobClaimOutcome.Deferred>(outcome);
        Assert.Equal(EtlJobDeferralReason.QueuedOverlap, deferred.Reason);
        Assert.Equal("pending", await RunStatusAsync(youngerRun));
        Assert.Equal("queued_overlap", await ScalarStringAsync($"SELECT deferral_code FROM etl_jobs WHERE job_id='{youngerJob:D}'"));
        Assert.Equal(0, await ScalarAsync($"SELECT claim_attempt_count FROM etl_jobs WHERE job_id='{youngerJob:D}'"));
        Assert.Equal(0, await ScalarAsync($"SELECT COUNT(*) FROM etl_entity_ownership WHERE owner_run_id='{youngerRun:D}'"));
        Assert.Equal("pending", await RunStatusAsync(elderRun));
    }

    [Fact]
    public async Task Older_deferred_not_due_elder_still_holds_overlap_for_newer_claim()
    {
        // An older pending run whose job is deferred-not-yet-due keeps its overlap
        // reservation: pending status is the durable admission hold.
        var (elderRun, elderJob) = await NewPendingJobRunAsync("bootstrap_full", ["clients"], createdAtUtc: "2026-09-20T00:00:00.0000000+00:00");
        await ExecuteSqlAsync("UPDATE etl_jobs SET status='deferred', deferral_code='busy_entity', available_at_utc=$future WHERE job_id=$job;",
            ("$job", elderJob.ToString("D")), ("$future", DateTimeOffset.UtcNow.AddHours(1).ToString("O")));
        var (_, youngerJob) = await NewPendingJobRunAsync("bootstrap_full", ["clients"], createdAtUtc: "2026-09-20T00:00:01.0000000+00:00");

        var outcome = await _store.TryClaimEtlJobAsync(youngerJob, "dispatcher-1", DateTimeOffset.UtcNow.AddMinutes(5), DateTimeOffset.UtcNow, CancellationToken.None);

        Assert.Equal(EtlJobDeferralReason.QueuedOverlap, Assert.IsType<EtlJobClaimOutcome.Deferred>(outcome).Reason);
        Assert.Equal("pending", await RunStatusAsync(elderRun));
        Assert.Empty((await _store.GetDispatchableEtlJobsAsync(10, DateTimeOffset.UtcNow, CancellationToken.None)).Jobs);
    }

    [Fact]
    public async Task Direct_out_of_order_disjoint_claims_on_two_stores_both_commit()
    {
        // Disjoint manifests are never held by order: claiming the YOUNGER job first and
        // then the elder through a second store instance both commit — no retry masking,
        // just two sequential committed claims.
        var (elderRun, elderJob) = await NewPendingJobRunAsync("bootstrap_full", ["clients"], createdAtUtc: "2026-09-20T00:00:00.0000000+00:00");
        var (youngerRun, youngerJob) = await NewPendingJobRunAsync("bootstrap_full", ["orders"], createdAtUtc: "2026-09-20T00:00:01.0000000+00:00");
        var storeB = new SqliteAgentStore(_factory, new SqliteMigrator(_factory));

        var younger = await _store.TryClaimEtlJobAsync(youngerJob, "dispatcher-1", DateTimeOffset.UtcNow.AddMinutes(5), DateTimeOffset.UtcNow, CancellationToken.None);
        var elder = await storeB.TryClaimEtlJobAsync(elderJob, "dispatcher-2", DateTimeOffset.UtcNow.AddMinutes(5), DateTimeOffset.UtcNow, CancellationToken.None);

        Assert.IsType<EtlJobClaimOutcome.Claimed>(younger);
        Assert.IsType<EtlJobClaimOutcome.Claimed>(elder);
        Assert.Equal("running", await RunStatusAsync(elderRun));
        Assert.Equal("running", await RunStatusAsync(youngerRun));
        Assert.Equal(2, await ScalarAsync("SELECT COUNT(*) FROM etl_entity_ownership WHERE released_at_utc IS NULL"));
    }

    [Fact]
    public async Task Concurrent_claims_of_the_same_job_mint_exactly_one_extraction_claim()
    {
        var (runId, jobId) = await NewPendingJobRunAsync("bootstrap_full", ["clients"]);
        var storeA = new SqliteAgentStore(_factory, new SqliteMigrator(_factory));
        var storeB = new SqliteAgentStore(_factory, new SqliteMigrator(_factory));
        var barrier = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var attemptA = Task.Run(async () => { await barrier.Task; return await storeA.TryClaimEtlJobAsync(jobId, "dispatcher-A", DateTimeOffset.UtcNow.AddMinutes(5), DateTimeOffset.UtcNow, CancellationToken.None); });
        var attemptB = Task.Run(async () => { await barrier.Task; return await storeB.TryClaimEtlJobAsync(jobId, "dispatcher-B", DateTimeOffset.UtcNow.AddMinutes(5), DateTimeOffset.UtcNow, CancellationToken.None); });
        barrier.SetResult(true);
        var outcomes = await Task.WhenAll(attemptA, attemptB).WaitAsync(GateTimeout);

        Assert.Single(outcomes.OfType<EtlJobClaimOutcome.Claimed>());
        Assert.Single(outcomes.OfType<EtlJobClaimOutcome.NotClaimable>());
        Assert.Equal("running", await RunStatusAsync(runId));
        Assert.Equal(1, await ScalarAsync($"SELECT claim_attempt_count FROM etl_jobs WHERE job_id='{jobId:D}'"));
        Assert.Equal(1, await ScalarAsync($"SELECT COUNT(*) FROM etl_entity_ownership WHERE owner_run_id='{runId:D}'"));
        Assert.Equal(1, await ScalarAsync($"SELECT COUNT(*) FROM etl_run_ownership_bindings WHERE run_id='{runId:D}'"));
    }

    [Fact]
    public async Task Partial_acquisition_conflict_rolls_back_every_acquired_row_and_binding()
    {
        // 'orders' is owned by a claimed run; claiming {clients, orders} must defer
        // busy_entity and leave entity 1 ('clients') untouched — the savepoint rollback
        // restores the partial acquisition completely.
        await NewRunAsync("orders");
        var (runId, jobId) = await NewPendingJobRunAsync("bootstrap_full", ["clients", "orders"], createdAtUtc: "2026-09-20T00:00:00.0000000+00:00");

        var outcome = await _store.TryClaimEtlJobAsync(jobId, "dispatcher-1", DateTimeOffset.UtcNow.AddMinutes(5), DateTimeOffset.UtcNow, CancellationToken.None);

        var deferred = Assert.IsType<EtlJobClaimOutcome.Deferred>(outcome);
        Assert.Equal(EtlJobDeferralReason.BusyEntity, deferred.Reason);
        Assert.Equal("pending", await RunStatusAsync(runId));
        Assert.Equal("deferred", await ScalarStringAsync($"SELECT status FROM etl_jobs WHERE job_id='{jobId:D}'"));
        Assert.Equal("busy_entity", await ScalarStringAsync($"SELECT deferral_code FROM etl_jobs WHERE job_id='{jobId:D}'"));
        Assert.Equal(0, await ScalarAsync($"SELECT claim_attempt_count FROM etl_jobs WHERE job_id='{jobId:D}'"));
        Assert.Equal(0, await ScalarAsync($"SELECT COUNT(*) FROM etl_entity_ownership WHERE owner_run_id='{runId:D}'"));
        Assert.Equal(0, await ScalarAsync($"SELECT COUNT(*) FROM etl_run_ownership_bindings WHERE run_id='{runId:D}'"));
        // 'clients' was never acquired by runId — but a younger claim over it is still
        // held: the deferred elder pending run keeps its overlap reservation.
        var (runId2, jobId2) = await NewPendingJobRunAsync("bootstrap_full", ["clients"], createdAtUtc: "2026-09-20T00:00:01.0000000+00:00");
        var claimed2 = await _store.TryClaimEtlJobAsync(jobId2, "dispatcher-1", DateTimeOffset.UtcNow.AddMinutes(5), DateTimeOffset.UtcNow, CancellationToken.None);
        Assert.Equal(EtlJobDeferralReason.QueuedOverlap, Assert.IsType<EtlJobClaimOutcome.Deferred>(claimed2).Reason);
        Assert.Equal(0, await ScalarAsync($"SELECT COUNT(*) FROM etl_entity_ownership WHERE owner_run_id='{runId2:D}'"));
    }

    // ---------- extraction claim fence ----------

    [Fact]
    public async Task Wrong_or_stale_extraction_claim_rejects_every_mutation_with_zero_writes()
    {
        var runId = await NewRunAsync("clients");
        var wrong = Guid.NewGuid();
        var request = Request("clients");

        Assert.Equal(EtlEntityBeginRejection.ExtractionClaimLost,
            Assert.IsType<EtlEntityBeginOutcome.Rejected>(await _store.BeginEtlEntityExtractionAsync(runId, wrong, request, CancellationToken.None)).Reason);

        // A live 'extracting' entity exists (begun under the real claim): the stale-claim
        // rejections below exercise the claim fence, never a missing-entity shortcut.
        Assert.IsType<EtlEntityBeginOutcome.Begun>(await _store.BeginEtlEntityExtractionAsync(runId, ClaimOf(runId), request, CancellationToken.None));

        Assert.Equal(EtlBatchRegistrationRejection.ExtractionClaimLost,
            Assert.IsType<EtlBatchRegistrationOutcome.Rejected>(await _store.RegisterGuardedEtlBatchAsync(MakeBatch(runId, "clients", 5), wrong, CancellationToken.None)).Reason);
        Assert.Equal(EtlEntityCompletionRejection.ExtractionClaimLost,
            Assert.IsType<EtlEntityCompletionOutcome.Rejected>(await _store.CompleteEtlEntityExtractionAsync(runId, wrong, "clients", CursorJson(FinalClients), 1, CancellationToken.None)).Reason);
        Assert.Equal(EtlRunSealRejection.ExtractionClaimLost,
            Assert.IsType<EtlRunSealOutcome.Rejected>(await _store.SealEtlRunExtractionAsync(runId, wrong, CancellationToken.None)).Reason);
        Assert.Equal(EtlRunTerminationRejection.ExtractionClaimLost,
            Assert.IsType<EtlRunTerminationOutcome.Rejected>(await _store.FailEtlRunAsync(runId, wrong, "x", CancellationToken.None)).Reason);
        Assert.Equal(EtlRunTerminationRejection.ExtractionClaimLost,
            Assert.IsType<EtlRunTerminationOutcome.Rejected>(await _store.BlockEtlRunAsync(runId, wrong, "X", "x", CancellationToken.None)).Reason);

        // Zero stray writes: the run is still 'running' with the original live claim and
        // the entity untouched in 'extracting'.
        Assert.Equal("running", await RunStatusAsync(runId));
        Assert.Equal(ClaimOf(runId).ToString("D"), await ScalarStringAsync($"SELECT extraction_claim_id FROM etl_runs WHERE run_id='{runId:D}'"));
        Assert.Equal("extracting", await ScalarStringAsync($"SELECT status FROM etl_run_entities WHERE run_id='{runId:D}'"));
        Assert.Equal(0, await ScalarAsync($"SELECT COUNT(*) FROM etl_batches WHERE run_id='{runId:D}'"));
    }

    [Fact]
    public async Task Seal_clears_the_extraction_fence_so_the_old_claim_is_dead()
    {
        var runId = await NewRunAsync("clients");
        var claim = ClaimOf(runId);
        await BeginExtractCompleteAsync(runId, "clients", CursorJson(FinalClients), batchRows: 5, claim);

        Assert.IsType<EtlRunSealOutcome.Sealed>(await _store.SealEtlRunExtractionAsync(runId, claim, CancellationToken.None));
        Assert.Null(await ScalarStringAsync($"SELECT extraction_claim_id FROM etl_runs WHERE run_id='{runId:D}'"));

        // The dead claim on the now-'uploading' run classifies as not-mutable — the old
        // fence identity can never write again either way.
        Assert.Equal(EtlRunSealRejection.RunNotRunningOrAlreadySealed,
            Assert.IsType<EtlRunSealOutcome.Rejected>(await _store.SealEtlRunExtractionAsync(runId, claim, CancellationToken.None)).Reason);
        Assert.Equal(EtlBatchRegistrationRejection.RunNotAcceptingBatches,
            Assert.IsType<EtlBatchRegistrationOutcome.Rejected>(await _store.RegisterGuardedEtlBatchAsync(MakeBatch(runId, "clients", 1), claim, CancellationToken.None)).Reason);
        Assert.Equal(EtlRunTerminationRejection.RunNotRunning,
            Assert.IsType<EtlRunTerminationOutcome.Rejected>(await _store.FailEtlRunAsync(runId, claim, "x", CancellationToken.None)).Reason);
    }

    // ---------- exact ownership set ----------

    [Fact]
    public async Task Equal_count_substitution_fails_closed_while_touching_a_retained_entity()
    {
        // Manifest {clients,orders} claimed; corruption swaps the manifest to
        // {clients,warehouses} — the count is unchanged. Touching 'clients' must still
        // fail: the predicate checks the FULL set in both directions, not the row.
        var runId = await NewRunAsync("clients", "orders");
        await ExecuteSqlAsync("UPDATE etl_runs SET requested_entities_json='[\"clients\",\"warehouses\"]' WHERE run_id=$run;", ("$run", runId.ToString("D")));

        var outcome = await _store.BeginEtlEntityExtractionAsync(runId, ClaimOf(runId), Request("clients"), CancellationToken.None);

        Assert.Equal(EtlEntityBeginRejection.OwnershipSetMismatch, Assert.IsType<EtlEntityBeginOutcome.Rejected>(outcome).Reason);
        Assert.Equal(0, await ScalarAsync($"SELECT COUNT(*) FROM etl_run_entities WHERE run_id='{runId:D}'"));
    }

    [Theory]
    // Missing binding, missing ownership row, extra binding, extra ownership, released
    // row — every deviation from manifest==bindings==active-ownership fails closed.
    [InlineData("DELETE FROM etl_run_ownership_bindings WHERE run_id=$run AND entity_name='clients'")]
    [InlineData("DELETE FROM etl_entity_ownership WHERE owner_run_id=$run AND entity_name='clients'")]
    [InlineData("INSERT INTO etl_run_ownership_bindings(run_id,entity_name,expected_epoch,acquired_at_utc) VALUES($run,'phantom',1,'2026-09-20T00:00:00.0000000+00:00')")]
    [InlineData("UPDATE etl_entity_ownership SET released_at_utc='2026-09-20T00:00:00.0000000+00:00', release_reason='manual_release' WHERE owner_run_id=$run AND entity_name='clients'")]
    [InlineData("UPDATE etl_entity_ownership SET ownership_epoch=ownership_epoch+1 WHERE owner_run_id=$run AND entity_name='clients'")]
    public async Task Any_deviation_from_the_exact_ownership_set_rejects_extraction(string corruption)
    {
        var runId = await NewRunAsync("clients");
        await ExecuteSqlAsync(corruption, ("$run", runId.ToString("D")));

        Assert.Equal(EtlEntityBeginRejection.OwnershipSetMismatch,
            Assert.IsType<EtlEntityBeginOutcome.Rejected>(await _store.BeginEtlEntityExtractionAsync(runId, ClaimOf(runId), Request("clients"), CancellationToken.None)).Reason);
        Assert.Equal(EtlRunSealRejection.OwnershipSetMismatch,
            Assert.IsType<EtlRunSealOutcome.Rejected>(await _store.SealEtlRunExtractionAsync(runId, ClaimOf(runId), CancellationToken.None)).Reason);
        Assert.Equal("running", await RunStatusAsync(runId));
    }

    [Fact]
    public async Task Released_then_reacquired_row_fails_the_old_owner_even_at_same_owner()
    {
        // Epoch ABA: run A owns 'clients' at epoch 1; its row is manually released and
        // re-acquired by run B (epoch 2). A's bindings pin epoch 1, so A's claim fails
        // closed — and even if the row is handed back to A at epoch 3 the bound epoch
        // still differs: same owner, wrong generation.
        var runA = await NewRunAsync("clients");
        await ReleaseOwnershipAsync();
        var runB = await NewRunAsync("clients");
        Assert.Equal(2, await ScalarAsync($"SELECT ownership_epoch FROM etl_entity_ownership WHERE entity_name='clients'"));

        Assert.Equal(EtlEntityBeginRejection.OwnershipSetMismatch,
            Assert.IsType<EtlEntityBeginOutcome.Rejected>(await _store.BeginEtlEntityExtractionAsync(runA, ClaimOf(runA), Request("clients"), CancellationToken.None)).Reason);

        // Hand the row back to A at epoch 3 — same owner_run_id, wrong bound epoch.
        await ExecuteSqlAsync("UPDATE etl_entity_ownership SET owner_run_id=$a, ownership_epoch=ownership_epoch+1 WHERE entity_name='clients';", ("$a", runA.ToString("D")));
        Assert.Equal(EtlEntityBeginRejection.OwnershipSetMismatch,
            Assert.IsType<EtlEntityBeginOutcome.Rejected>(await _store.BeginEtlEntityExtractionAsync(runA, ClaimOf(runA), Request("clients"), CancellationToken.None)).Reason);
        Assert.Equal(EtlEntityBeginRejection.OwnershipSetMismatch,
            Assert.IsType<EtlEntityBeginOutcome.Rejected>(await _store.BeginEtlEntityExtractionAsync(runB, ClaimOf(runB), Request("clients"), CancellationToken.None)).Reason);
    }

    [Fact]
    public async Task Successful_reacquisition_after_finalize_increments_the_epoch()
    {
        var runA = await NewRunAsync("clients");
        await BeginExtractCompleteAsync(runA, "clients", CursorJson(FinalClients), batchRows: 5, ClaimOf(runA));
        Assert.IsType<EtlRunSealOutcome.Sealed>(await _store.SealEtlRunExtractionAsync(runA, ClaimOf(runA), CancellationToken.None));
        await AcknowledgeAllBatchesAsync(runA);
        var claimA = Assert.IsType<EtlRunClaimOutcome.Claimed>(await _store.TryClaimRunCompletionAsync(runA, "owner-1", DateTimeOffset.UtcNow, 8, CancellationToken.None));
        Assert.IsType<EtlRunFinalizeOutcome.Finalized>(await _store.FinalizeEtlRunAsync(runA, claimA.Claim.ClaimId, CancellationToken.None));
        Assert.Equal(1, await ScalarAsync($"SELECT COUNT(*) FROM etl_entity_ownership WHERE entity_name='clients' AND released_at_utc IS NOT NULL AND release_reason='finalized'"));

        var runB = await NewRunAsync("clients");

        Assert.Equal(2, await ScalarAsync($"SELECT ownership_epoch FROM etl_entity_ownership WHERE entity_name='clients'"));
        Assert.Equal(2, await ScalarAsync($"SELECT expected_epoch FROM etl_run_ownership_bindings WHERE run_id='{runB:D}'"));
        // The re-acquiring run extracts normally under the new epoch.
        Assert.IsType<EtlEntityBeginOutcome.Begun>(await _store.BeginEtlEntityExtractionAsync(runB, ClaimOf(runB), Request("clients"), CancellationToken.None));
    }

    // ---------- retention + termination ----------

    [Fact]
    public async Task Ownership_is_retained_through_failure_block_and_exclusive_host_recovery()
    {
        // Failure retains ownership: the blocked/failed run never frees its entities.
        var failed = await NewRunAsync("clients");
        Assert.IsType<EtlRunTerminationOutcome.Applied>(await _store.FailEtlRunAsync(failed, ClaimOf(failed), "boom", CancellationToken.None));
        Assert.Equal(1, await ScalarAsync($"SELECT COUNT(*) FROM etl_entity_ownership WHERE owner_run_id='{failed:D}' AND released_at_utc IS NULL"));

        // Exclusive-host recovery retains ownership of interrupted runs while clearing
        // their dead extraction claims.
        var interrupted = await NewRunAsync("orders");
        var interruptedClaim = ClaimOf(interrupted);
        var recovered = await _store.RecoverInterruptedEtlRunsAsync(CancellationToken.None);
        Assert.Equal(1, recovered.ClaimsReleased);
        Assert.Equal(1, recovered.RunsBlocked);
        Assert.Equal("blocked", await RunStatusAsync(interrupted));
        Assert.Null(await ScalarStringAsync($"SELECT extraction_claim_id FROM etl_runs WHERE run_id='{interrupted:D}'"));
        Assert.Equal(1, await ScalarAsync($"SELECT COUNT(*) FROM etl_entity_ownership WHERE owner_run_id='{interrupted:D}' AND released_at_utc IS NULL"));
        // The cleared claim is dead forever.
        Assert.Equal(EtlRunTerminationRejection.RunNotRunning,
            Assert.IsType<EtlRunTerminationOutcome.Rejected>(await _store.FailEtlRunAsync(interrupted, interruptedClaim, "x", CancellationToken.None)).Reason);
    }

    [Fact]
    public async Task Foreign_claim_failure_cannot_block_a_live_run()
    {
        var runId = await NewRunAsync("clients");

        var outcome = await _store.FailEtlRunAsync(runId, Guid.NewGuid(), "foreign", CancellationToken.None);

        Assert.Equal(EtlRunTerminationRejection.ExtractionClaimLost, Assert.IsType<EtlRunTerminationOutcome.Rejected>(outcome).Reason);
        Assert.Equal("running", await RunStatusAsync(runId));
        Assert.Equal(1, await ScalarAsync($"SELECT COUNT(*) FROM etl_entity_ownership WHERE owner_run_id='{runId:D}' AND released_at_utc IS NULL"));
    }

    // ---------- finalize ownership release ----------

    [Fact]
    public async Task RaiseIgnore_on_release_rolls_back_watermarks_and_partial_release_then_blocks()
    {
        var runId = await NewRunAsync("clients", "orders");
        await BeginExtractCompleteAsync(runId, "clients", CursorJson(FinalClients), batchRows: 5, ClaimOf(runId));
        await BeginExtractCompleteAsync(runId, "orders", CursorJson(FinalOrders), batchRows: 7, ClaimOf(runId));
        Assert.IsType<EtlRunSealOutcome.Sealed>(await _store.SealEtlRunExtractionAsync(runId, ClaimOf(runId), CancellationToken.None));
        await AcknowledgeAllBatchesAsync(runId);
        var claim = Assert.IsType<EtlRunClaimOutcome.Claimed>(await _store.TryClaimRunCompletionAsync(runId, "owner-1", DateTimeOffset.UtcNow, 8, CancellationToken.None));

        // RAISE(IGNORE) silently drops the 'orders' release row write: the release count
        // (1) no longer equals the sealed entity count (2) — a controlled mismatch that
        // rolls back ALL watermark writes AND the 'clients' release, then blocks.
        await ExecuteSqlAsync("CREATE TRIGGER swallow_release BEFORE UPDATE ON etl_entity_ownership WHEN NEW.entity_name='orders' AND NEW.release_reason='finalized' BEGIN SELECT RAISE(IGNORE); END;", []);

        var outcome = await _store.FinalizeEtlRunAsync(runId, claim.Claim.ClaimId, CancellationToken.None);

        var blocked = Assert.IsType<EtlRunFinalizeOutcome.Blocked>(outcome);
        Assert.Equal("OWNERSHIP_RELEASE_MISMATCH", blocked.Code);
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM watermarks"));
        // BOTH ownership rows are still active — the partial release was rolled back.
        Assert.Equal(2, await ScalarAsync($"SELECT COUNT(*) FROM etl_entity_ownership WHERE owner_run_id='{runId:D}' AND released_at_utc IS NULL"));
        Assert.Equal("blocked", await RunStatusAsync(runId));
        Assert.Equal("OWNERSHIP_RELEASE_MISMATCH", await ScalarStringAsync($"SELECT finalize_conflict_code FROM etl_runs WHERE run_id='{runId:D}'"));
        Assert.Equal("blocked", await ScalarStringAsync($"SELECT status FROM etl_jobs WHERE run_id='{runId:D}'"));
        await ExecuteSqlAsync("DROP TRIGGER swallow_release;", []);
    }

    [Fact]
    public async Task RaiseAbort_on_release_rolls_back_the_whole_transaction_and_retry_converges()
    {
        var runId = await NewRunAsync("clients");
        await BeginExtractCompleteAsync(runId, "clients", CursorJson(FinalClients), batchRows: 5, ClaimOf(runId));
        Assert.IsType<EtlRunSealOutcome.Sealed>(await _store.SealEtlRunExtractionAsync(runId, ClaimOf(runId), CancellationToken.None));
        await AcknowledgeAllBatchesAsync(runId);
        var claim = Assert.IsType<EtlRunClaimOutcome.Claimed>(await _store.TryClaimRunCompletionAsync(runId, "owner-1", DateTimeOffset.UtcNow, 8, CancellationToken.None));

        await ExecuteSqlAsync("CREATE TRIGGER abort_release BEFORE UPDATE ON etl_entity_ownership WHEN NEW.release_reason='finalized' BEGIN SELECT RAISE(ABORT,'injected release fault'); END;", []);

        await Assert.ThrowsAsync<SqliteException>(() => _store.FinalizeEtlRunAsync(runId, claim.Claim.ClaimId, CancellationToken.None));

        // Whole transaction rolled back: completing claim and ownership preserved.
        Assert.Equal("completing", await RunStatusAsync(runId));
        Assert.Equal(claim.Claim.ClaimId.ToString("D"), await ScalarStringAsync($"SELECT completion_claim_id FROM etl_runs WHERE run_id='{runId:D}'"));
        Assert.Equal(1, await ScalarAsync($"SELECT COUNT(*) FROM etl_entity_ownership WHERE owner_run_id='{runId:D}' AND released_at_utc IS NULL"));
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM watermarks"));

        await ExecuteSqlAsync("DROP TRIGGER abort_release;", []);
        Assert.IsType<EtlRunFinalizeOutcome.Finalized>(await _store.FinalizeEtlRunAsync(runId, claim.Claim.ClaimId, CancellationToken.None));
        Assert.Equal("succeeded", await RunStatusAsync(runId));
        Assert.Equal(1, await ScalarAsync($"SELECT COUNT(*) FROM etl_entity_ownership WHERE owner_run_id='{runId:D}' AND released_at_utc IS NOT NULL AND release_reason='finalized'"));
    }

    // ---------- manifest quarantine + admission hold ----------

    [Fact]
    public async Task Provably_inert_invalid_elder_is_quarantined_and_the_younger_claim_commits()
    {
        // Corrupt the elder's frozen manifest — the run never started (no claims, no
        // batches, no bindings, no ownership): a younger claim quarantines it
        // (MANIFEST_INVALID) and proceeds.
        var (elderRun, elderJob) = await NewPendingJobRunAsync("bootstrap_full", ["clients"], createdAtUtc: "2026-09-20T00:00:00.0000000+00:00");
        await ExecuteSqlAsync("UPDATE etl_runs SET requested_entities_json='not-json' WHERE run_id=$run;", ("$run", elderRun.ToString("D")));
        var (youngerRun, youngerJob) = await NewPendingJobRunAsync("bootstrap_full", ["clients"], createdAtUtc: "2026-09-20T00:00:01.0000000+00:00");

        var outcome = await _store.TryClaimEtlJobAsync(youngerJob, "dispatcher-1", DateTimeOffset.UtcNow.AddMinutes(5), DateTimeOffset.UtcNow, CancellationToken.None);

        Assert.IsType<EtlJobClaimOutcome.Claimed>(outcome);
        Assert.Equal("blocked", await RunStatusAsync(elderRun));
        Assert.Equal("MANIFEST_INVALID", await ScalarStringAsync($"SELECT finalize_conflict_code FROM etl_runs WHERE run_id='{elderRun:D}'"));
        Assert.Equal("blocked", await ScalarStringAsync($"SELECT status FROM etl_jobs WHERE job_id='{elderJob:D}'"));
        Assert.Equal("running", await RunStatusAsync(youngerRun));
    }

    [Fact]
    public async Task Invalid_elder_with_unproven_effects_holds_admission_even_after_self_block()
    {
        // Root-review proof: malformed elder WITH durable effects (a batch row proves it
        // ran) holds younger admission; directly claiming the elder blocks its job
        // diagnostically but keeps the pending run as the durable admission hold — the
        // younger claim is STILL held afterwards.
        var (elderRun, elderJob) = await NewPendingJobRunAsync("bootstrap_full", ["clients"], createdAtUtc: "2026-09-20T00:00:00.0000000+00:00");
        await ExecuteSqlAsync("UPDATE etl_runs SET requested_entities_json='not-json' WHERE run_id=$run;", ("$run", elderRun.ToString("D")));
        await ExecuteSqlAsync(
            "INSERT INTO etl_batches(batch_id,run_id,entity_name,schema_version,file_path,status,row_count,sha256,compressed_size,uncompressed_size,created_at_utc) VALUES($b,$run,'clients',1,'spool/x.gz','acknowledged',5,'h',10,10,$now);",
            ("$b", Guid.NewGuid().ToString("D")), ("$run", elderRun.ToString("D")), ("$now", Now()));
        var (_, youngerJob) = await NewPendingJobRunAsync("bootstrap_full", ["clients"], createdAtUtc: "2026-09-20T00:00:01.0000000+00:00");

        var held = Assert.IsType<EtlJobClaimOutcome.Deferred>(await _store.TryClaimEtlJobAsync(youngerJob, "dispatcher-1", DateTimeOffset.UtcNow.AddMinutes(5), DateTimeOffset.UtcNow, CancellationToken.None));
        Assert.Equal(EtlJobDeferralReason.ElderManifestInvalid, held.Reason);
        Assert.Equal("pending", await RunStatusAsync(elderRun));

        // Claim the elder's own job directly: diagnostic block on the job, but the run
        // keeps 'pending' — unresolved effects are never retired silently.
        var self = Assert.IsType<EtlJobClaimOutcome.Blocked>(await _store.TryClaimEtlJobAsync(elderJob, "dispatcher-1", DateTimeOffset.UtcNow.AddMinutes(5), DateTimeOffset.UtcNow, CancellationToken.None));
        Assert.Equal("JOB_MANIFEST_INCONSISTENT", self.Code);
        Assert.Equal("blocked", await ScalarStringAsync($"SELECT status FROM etl_jobs WHERE job_id='{elderJob:D}'"));
        Assert.Equal("pending", await RunStatusAsync(elderRun));
        // No ownership was ever created or released for the unresolved elder.
        Assert.Equal(0, await ScalarAsync($"SELECT COUNT(*) FROM etl_entity_ownership WHERE owner_run_id='{elderRun:D}'"));

        // The younger claim is STILL held — the admission hold survived the diagnostic block.
        var stillHeld = Assert.IsType<EtlJobClaimOutcome.Deferred>(await _store.TryClaimEtlJobAsync(youngerJob, "dispatcher-1", DateTimeOffset.UtcNow.AddMinutes(5), DateTimeOffset.UtcNow.AddMinutes(6), CancellationToken.None));
        Assert.Equal(EtlJobDeferralReason.ElderManifestInvalid, stillHeld.Reason);
    }

    [Fact]
    public async Task Provably_inert_inconsistent_job_quarantines_self_and_releases_the_reservation()
    {
        // Same inconsistency with NO durable effects: the direct claim quarantines job
        // AND pending run — the reservation is released, the younger overlapping claim
        // commits.
        var (elderRun, elderJob) = await NewPendingJobRunAsync("bootstrap_full", ["clients"], createdAtUtc: "2026-09-20T00:00:00.0000000+00:00");
        await ExecuteSqlAsync("UPDATE etl_jobs SET mode='entity_reload' WHERE job_id=$job;", ("$job", elderJob.ToString("D")));
        var (_, youngerJob) = await NewPendingJobRunAsync("bootstrap_full", ["clients"], createdAtUtc: "2026-09-20T00:00:01.0000000+00:00");

        var self = Assert.IsType<EtlJobClaimOutcome.Blocked>(await _store.TryClaimEtlJobAsync(elderJob, "dispatcher-1", DateTimeOffset.UtcNow.AddMinutes(5), DateTimeOffset.UtcNow, CancellationToken.None));
        Assert.Equal("JOB_MANIFEST_INCONSISTENT", self.Code);
        Assert.Equal("blocked", await RunStatusAsync(elderRun));

        var younger = await _store.TryClaimEtlJobAsync(youngerJob, "dispatcher-1", DateTimeOffset.UtcNow.AddMinutes(5), DateTimeOffset.UtcNow, CancellationToken.None);
        Assert.IsType<EtlJobClaimOutcome.Claimed>(younger);
    }

    [Theory]
    [InlineData("not-json")]
    [InlineData("{}")]
    [InlineData("[]")]
    [InlineData("null")]
    [InlineData("[42]")]
    [InlineData("[\"clients\",\"clients\"]")]
    [InlineData("[\"clients\",null]")]
    public async Task Invalid_job_manifest_shapes_are_quarantined_or_held_never_dispatched(string manifest)
    {
        var (runId, jobId) = await NewPendingJobRunAsync("bootstrap_full", ["clients"]);
        await ExecuteSqlAsync("UPDATE etl_runs SET requested_entities_json=$manifest WHERE run_id=$run;", ("$manifest", manifest), ("$run", runId.ToString("D")));

        var outcome = await _store.TryClaimEtlJobAsync(jobId, "dispatcher-1", DateTimeOffset.UtcNow.AddMinutes(5), DateTimeOffset.UtcNow, CancellationToken.None);

        // Provably never started: quarantine blocks job AND pending run — corrupt
        // durable evidence is never dispatched and the reservation is released.
        Assert.IsType<EtlJobClaimOutcome.Blocked>(outcome);
        Assert.Equal("blocked", await RunStatusAsync(runId));
        Assert.Equal(0, await ScalarAsync($"SELECT COUNT(*) FROM etl_entity_ownership WHERE owner_run_id='{runId:D}'"));
        Assert.Equal(0, await ScalarAsync($"SELECT COUNT(*) FROM etl_run_entities WHERE run_id='{runId:D}'"));
    }

    [Fact]
    public async Task Enumeration_surfaces_corrupt_pending_runs_as_quarantine_diagnostics()
    {
        var (corruptRun, _) = await NewPendingJobRunAsync("bootstrap_full", ["clients"], createdAtUtc: "2026-09-20T00:00:00.0000000+00:00");
        await ExecuteSqlAsync("UPDATE etl_runs SET requested_entities_json='[42]' WHERE run_id=$run;", ("$run", corruptRun.ToString("D")));
        var (_, eligibleJob) = await NewPendingJobRunAsync("bootstrap_full", ["orders"], createdAtUtc: "2026-09-20T00:00:01.0000000+00:00");

        var page = await _store.GetDispatchableEtlJobsAsync(10, DateTimeOffset.UtcNow, CancellationToken.None);

        // A corrupt head never starves disjoint eligible work, and it is surfaced.
        Assert.Contains(page.Jobs, j => j.JobId == eligibleJob);
        var quarantine = Assert.Single(page.Quarantined);
        Assert.Equal(corruptRun, quarantine.RunId);
        Assert.Equal("MANIFEST_INVALID", quarantine.Code);
    }

    [Fact]
    public async Task Malformed_job_definition_head_never_hides_disjoint_eligible_work()
    {
        // The head job's frozen entities_json is corrupt while its run manifest stays
        // valid: it is ineligible BEFORE LIMIT (typed job identity is part of
        // eligibility) and is surfaced as a quarantine diagnostic — the disjoint job B
        // is still returned under LIMIT 1.
        var (corruptRun, corruptJob) = await NewPendingJobRunAsync("bootstrap_full", ["clients"], createdAtUtc: "2026-09-20T00:00:00.0000000+00:00");
        await ExecuteSqlAsync("UPDATE etl_jobs SET entities_json='{corrupt' WHERE job_id=$job;", ("$job", corruptJob.ToString("D")));
        var (_, eligibleJob) = await NewPendingJobRunAsync("bootstrap_full", ["orders"], createdAtUtc: "2026-09-20T00:00:01.0000000+00:00");

        var page = await _store.GetDispatchableEtlJobsAsync(1, DateTimeOffset.UtcNow, CancellationToken.None);

        var job = Assert.Single(page.Jobs);
        Assert.Equal(eligibleJob, job.JobId);
        Assert.DoesNotContain(page.Jobs, j => j.JobId == corruptJob);
        var quarantine = Assert.Single(page.Quarantined);
        Assert.Equal(corruptRun, quarantine.RunId);
        Assert.Equal(corruptJob, quarantine.JobId);
        Assert.Equal("MANIFEST_INVALID", quarantine.Code);
    }

    [Fact]
    public async Task Mode_drift_in_frozen_job_identity_omits_and_reports_the_corrupt_head()
    {
        // The job's frozen mode drifts from the run's durable mode while the manifest
        // stays valid: the head is ineligible before LIMIT and surfaced for quarantine.
        var (corruptRun, corruptJob) = await NewPendingJobRunAsync("bootstrap_full", ["clients"], createdAtUtc: "2026-09-20T00:00:00.0000000+00:00");
        await ExecuteSqlAsync("UPDATE etl_jobs SET mode='entity_reload' WHERE job_id=$job;", ("$job", corruptJob.ToString("D")));
        var (_, eligibleJob) = await NewPendingJobRunAsync("bootstrap_full", ["orders"], createdAtUtc: "2026-09-20T00:00:01.0000000+00:00");

        var page = await _store.GetDispatchableEtlJobsAsync(1, DateTimeOffset.UtcNow, CancellationToken.None);

        var job = Assert.Single(page.Jobs);
        Assert.Equal(eligibleJob, job.JobId);
        Assert.DoesNotContain(page.Jobs, j => j.JobId == corruptJob);
        var quarantine = Assert.Single(page.Quarantined);
        Assert.Equal(corruptRun, quarantine.RunId);
        Assert.Equal(corruptJob, quarantine.JobId);
    }

    [Fact]
    public async Task Swallowed_elder_quarantine_write_aborts_the_whole_claim_transaction()
    {
        // RAISE(IGNORE) silently drops the elder's pending-run block: the quarantine is
        // a guarded expected-one transition — a zero-row write must roll back the ENTIRE
        // claim transaction, never commit a partial quarantine and claim the younger.
        var (elderRun, elderJob) = await NewPendingJobRunAsync("bootstrap_full", ["clients"], createdAtUtc: "2026-09-20T00:00:00.0000000+00:00");
        await ExecuteSqlAsync("UPDATE etl_runs SET requested_entities_json='not-json' WHERE run_id=$run;", ("$run", elderRun.ToString("D")));
        var (youngerRun, youngerJob) = await NewPendingJobRunAsync("bootstrap_full", ["clients"], createdAtUtc: "2026-09-20T00:00:01.0000000+00:00");
        await ExecuteSqlAsync("CREATE TRIGGER swallow_quarantine BEFORE UPDATE ON etl_runs WHEN NEW.status='blocked' BEGIN SELECT RAISE(IGNORE); END;", []);
        try
        {
            await Assert.ThrowsAsync<InvalidOperationException>(() => _store.TryClaimEtlJobAsync(youngerJob, "dispatcher-1", DateTimeOffset.UtcNow.AddMinutes(5), DateTimeOffset.UtcNow, CancellationToken.None));
        }
        finally
        {
            await ExecuteSqlAsync("DROP TRIGGER IF EXISTS swallow_quarantine;", []);
        }

        // Nothing committed: elder stays pending with its job untouched, the younger
        // keeps its pending job+run, and zero ownership/bindings exist.
        Assert.Equal("pending", await RunStatusAsync(elderRun));
        Assert.Equal("pending", await ScalarStringAsync($"SELECT status FROM etl_jobs WHERE job_id='{elderJob:D}'"));
        Assert.Equal("pending", await RunStatusAsync(youngerRun));
        Assert.Equal("pending", await ScalarStringAsync($"SELECT status FROM etl_jobs WHERE job_id='{youngerJob:D}'"));
        Assert.Equal(0, await ScalarAsync($"SELECT claim_attempt_count FROM etl_jobs WHERE job_id='{youngerJob:D}'"));
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM etl_entity_ownership"));
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM etl_run_ownership_bindings"));
    }

    [Fact]
    public async Task Enumeration_ranks_by_run_created_then_run_id_deterministically()
    {
        // Same created_at_utc: run_id breaks the tie — a deterministic total order.
        var (runA, jobA) = await NewPendingJobRunAsync("bootstrap_full", ["clients"], createdAtUtc: "2026-09-20T00:00:00.0000000+00:00");
        var (runB, jobB) = await NewPendingJobRunAsync("bootstrap_full", ["orders"], createdAtUtc: "2026-09-20T00:00:00.0000000+00:00");

        var page = await _store.GetDispatchableEtlJobsAsync(10, DateTimeOffset.UtcNow, CancellationToken.None);

        Assert.Equal(2, page.Jobs.Count);
        var expectedFirst = string.CompareOrdinal(runA.ToString("D"), runB.ToString("D")) < 0 ? jobA : jobB;
        Assert.Equal(expectedFirst, page.Jobs[0].JobId);
    }

    [Fact]
    public async Task Claim_on_missing_or_terminal_job_is_read_only_not_claimable()
    {
        var runId = await NewRunAsync("clients");
        var jobId = await ScalarStringAsync($"SELECT job_id FROM etl_jobs WHERE run_id='{runId:D}'");

        Assert.IsType<EtlJobClaimOutcome.NotClaimable>(await _store.TryClaimEtlJobAsync(Guid.NewGuid(), "dispatcher-1", DateTimeOffset.UtcNow, DateTimeOffset.UtcNow, CancellationToken.None));
        Assert.IsType<EtlJobClaimOutcome.NotClaimable>(await _store.TryClaimEtlJobAsync(Guid.Parse(jobId!), "dispatcher-1", DateTimeOffset.UtcNow, DateTimeOffset.UtcNow, CancellationToken.None));
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

    // Pending run + pending durable job with consistent frozen identity (not claimed).
    private async Task<(Guid RunId, Guid JobId)> NewPendingJobRunAsync(string mode, string[] entities, string? createdAtUtc = null)
    {
        var runId = Guid.NewGuid();
        var jobId = Guid.NewGuid();
        var created = createdAtUtc ?? Now();
        var manifest = JsonSerializer.Serialize(entities, JsonOptions);
        var definitions = JsonSerializer.Serialize(entities.Select(e => MakeEntities().Single(x => x.EntityCode == e)).ToArray(), JsonOptions);
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

    private async Task BeginExtractCompleteAsync(Guid runId, string entity, string finalJson, int batchRows, Guid claim)
    {
        Assert.IsType<EtlEntityBeginOutcome.Begun>(await _store.BeginEtlEntityExtractionAsync(runId, claim, Request(entity), CancellationToken.None));
        Assert.IsType<EtlBatchRegistrationOutcome.Registered>(await _store.RegisterGuardedEtlBatchAsync(MakeBatch(runId, entity, batchRows), claim, CancellationToken.None));
        Assert.IsType<EtlEntityCompletionOutcome.Completed>(await _store.CompleteEtlEntityExtractionAsync(runId, claim, entity, finalJson, 1, CancellationToken.None));
    }

    // SQL FIXTURE SETUP ONLY — this is direct corruption injection to stage the
    // released-row/epoch-ABA scenario, NOT the attested manual-resolution path (O1
    // implements no release API and no attestation flow).
    private async Task ReleaseOwnershipAsync()
    {
        await ExecuteSqlAsync(
            "UPDATE etl_entity_ownership SET released_at_utc=$now, release_reason='manual_release', updated_at_utc=$now WHERE released_at_utc IS NULL;",
            ("$now", Now()));
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
