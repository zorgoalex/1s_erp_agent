using System.Globalization;
using System.Text.Json;
using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Domain.Etl;
using ErpOnecAgent.Infrastructure.Persistence.Sqlite;
using Microsoft.Data.Sqlite;
using Xunit;

namespace ErpOnecAgent.IntegrationTests;

/// <summary>
/// O3 DARK scheduled-run suite (schema v9): the idempotent schedule-key Ensure
/// (created vs. active/unresolved dedup, frozen identity never replaced, argument
/// validation with zero writes, single-winner concurrency), the scheduled claim
/// through the SAME transaction shape as manual jobs (fresh extraction fence GUID,
/// owner_job_id NULL ownership at epoch 1, immutable bindings, shared elder-overlap
/// priority both directions, busy-entity savepoint rollback, corrupt frozen-identity
/// quarantine vs. admission hold), Begin binding to the frozen resolved definitions
/// (RunDefinitionMismatch / JobInconsistent), the eligibility enumeration applied in
/// SQL before LIMIT, and dead-process recovery keeping the unresolved-key hold.
/// All against a real migrated SQLite database. DARK: nothing here wires workers,
/// the ERP client, or production dispatch.
/// </summary>
public sealed class EtlScheduledRunsO3Tests : IAsyncLifetime
{
    private const string SourceNamespace = "onec-infobase-a";
    private const string QueryMode = "bootstrap_full";
    // Scheduled runs are incremental only (design §4.4); manual jobs keep QueryMode.
    private const string ScheduledMode = "incremental";
    private static readonly TimeSpan GateTimeout = TimeSpan.FromSeconds(15);
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);
    private static readonly EtlCursor FinalClients = new(DateTimeOffset.Parse("2026-09-19T09:59:00.0000000+00:00", CultureInfo.InvariantCulture), "A10");
    private static readonly EtlCursor CommittedClients = new(DateTimeOffset.Parse("2026-09-18T09:59:00.0000000+00:00", CultureInfo.InvariantCulture), "A5");

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

    // ---------- ensure: create + dedup ----------

    [Fact]
    public async Task Ensure_creates_one_pending_jobless_run_with_the_frozen_identity()
    {
        var now = DateTimeOffset.UtcNow;
        var request = EnsureRequest("nightly", ["clients", "orders"]);

        var outcome = await _store.EnsureScheduledEtlRunAsync(request, now, CancellationToken.None);

        var created = Assert.IsType<EtlScheduledRunEnsureOutcome.Created>(outcome);
        var run = created.RunId.ToString("D");
        Assert.Equal("pending", await ScalarStringAsync($"SELECT status FROM etl_runs WHERE run_id='{run}'"));
        Assert.Equal("nightly", await ScalarStringAsync($"SELECT schedule_key FROM etl_runs WHERE run_id='{run}'"));
        Assert.Equal("incremental", await ScalarStringAsync($"SELECT mode FROM etl_runs WHERE run_id='{run}'"));
        // The ordered entity codes become the run manifest, in request order.
        var manifest = JsonSerializer.Deserialize<string[]>(
            (await ScalarStringAsync($"SELECT requested_entities_json FROM etl_runs WHERE run_id='{run}'"))!, JsonOptions);
        Assert.Equal(["clients", "orders"], manifest!);
        // resolved_entities_json freezes the FULL effective definitions verbatim.
        var resolvedText = await ScalarStringAsync($"SELECT resolved_entities_json FROM etl_runs WHERE run_id='{run}'");
        var resolved = JsonSerializer.Deserialize<EtlEntityDefinition[]>(resolvedText!, JsonOptions);
        Assert.NotNull(resolved);
        Assert.Equal(request.Entities.Count, resolved!.Length);
        for (var index = 0; index < resolved.Length; index++)
        {
            AssertDefinitionsEqual(request.Entities[index], resolved[index]);
        }
        Assert.Equal("7", await ScalarStringAsync($"SELECT configuration_version FROM etl_runs WHERE run_id='{run}'"));
        Assert.Equal(now.ToUniversalTime().ToString("O"), await ScalarStringAsync($"SELECT created_at_utc FROM etl_runs WHERE run_id='{run}'"));
        Assert.Null(await ScalarStringAsync($"SELECT started_at_utc FROM etl_runs WHERE run_id='{run}'"));
        // Scheduled work never becomes an etl_jobs row.
        Assert.Equal(0, await ScalarAsync($"SELECT COUNT(*) FROM etl_jobs WHERE run_id='{run}'"));
        Assert.Equal(1, await ScalarAsync("SELECT COUNT(*) FROM etl_runs"));
    }

    [Theory]
    [InlineData("pending")]
    [InlineData("running")]
    [InlineData("uploading")]
    [InlineData("completing")]
    public async Task Ensure_returns_the_active_holder_with_zero_writes_and_never_replaces_its_identity(string status)
    {
        var request = EnsureRequest("nightly", ["clients"]);
        var first = Assert.IsType<EtlScheduledRunEnsureOutcome.Created>(
            await _store.EnsureScheduledEtlRunAsync(request, DateTimeOffset.UtcNow, CancellationToken.None));
        var run = first.RunId.ToString("D");
        if (!string.Equals(status, "pending", StringComparison.Ordinal))
        {
            // Marker row_version 42: a dedup write would change it back.
            await ExecuteSqlAsync("UPDATE etl_runs SET status=$status, row_version=42 WHERE run_id=$run;",
                ("$status", status), ("$run", run));
        }
        var resolvedBefore = await ScalarStringAsync($"SELECT resolved_entities_json FROM etl_runs WHERE run_id='{run}'");
        var manifestBefore = await ScalarStringAsync($"SELECT requested_entities_json FROM etl_runs WHERE run_id='{run}'");
        var rowVersionBefore = await ScalarStringAsync($"SELECT row_version FROM etl_runs WHERE run_id='{run}'");

        // A different definition set and configuration version can never replace the
        // frozen identity of the active holder.
        var other = new EtlScheduledRunRequest("nightly", "incremental",
            [MakeEntities().Single(e => e.EntityCode == "payments")], 99);
        var second = await _store.EnsureScheduledEtlRunAsync(other, DateTimeOffset.UtcNow, CancellationToken.None);

        var existing = Assert.IsType<EtlScheduledRunEnsureOutcome.ActiveExisting>(second);
        Assert.Equal(first.RunId, existing.RunId);
        Assert.Equal(status, existing.Status);
        Assert.Equal(1, await ScalarAsync("SELECT COUNT(*) FROM etl_runs"));
        Assert.Equal(rowVersionBefore, await ScalarStringAsync($"SELECT row_version FROM etl_runs WHERE run_id='{run}'"));
        Assert.Equal(resolvedBefore, await ScalarStringAsync($"SELECT resolved_entities_json FROM etl_runs WHERE run_id='{run}'"));
        Assert.Equal(manifestBefore, await ScalarStringAsync($"SELECT requested_entities_json FROM etl_runs WHERE run_id='{run}'"));
        Assert.Equal("7", await ScalarStringAsync($"SELECT configuration_version FROM etl_runs WHERE run_id='{run}'"));
    }

    [Theory]
    [InlineData("failed")]
    [InlineData("blocked")]
    public async Task Ensure_holds_the_key_on_an_unresolved_terminal_run_and_releases_it_on_resolution(string status)
    {
        var runId = await NewScheduledRunAsync("nightly", ["clients"], status: status);
        var run = runId.ToString("D");

        var held = await _store.EnsureScheduledEtlRunAsync(EnsureRequest("nightly", ["clients"]), DateTimeOffset.UtcNow, CancellationToken.None);

        var existing = Assert.IsType<EtlScheduledRunEnsureOutcome.ActiveExisting>(held);
        Assert.Equal(runId, existing.RunId);
        Assert.Equal(status, existing.Status);
        Assert.Equal(1, await ScalarAsync("SELECT COUNT(*) FROM etl_runs"));

        // resolved_at_utc releases the key: the next tick mints a successor.
        await ExecuteSqlAsync("UPDATE etl_runs SET resolved_at_utc=$now WHERE run_id=$run;", ("$now", Now()), ("$run", run));
        var next = await _store.EnsureScheduledEtlRunAsync(EnsureRequest("nightly", ["clients"]), DateTimeOffset.UtcNow, CancellationToken.None);

        var created = Assert.IsType<EtlScheduledRunEnsureOutcome.Created>(next);
        Assert.NotEqual(runId, created.RunId);
        Assert.Equal(2, await ScalarAsync("SELECT COUNT(*) FROM etl_runs"));
        Assert.Equal("pending", await ScalarStringAsync($"SELECT status FROM etl_runs WHERE run_id='{created.RunId:D}'"));
    }

    [Theory]
    [InlineData("paused")]
    [InlineData("partial_success")]
    [InlineData("some_future_status")]
    public async Task Ensure_holds_the_key_for_every_status_that_is_not_explicitly_released(string status)
    {
        // Fail closed: only succeeded, cancelled and a resolved failed/blocked run release
        // the key — a paused, partial or unknown status never mints a successor.
        var runId = await NewScheduledRunAsync("nightly", ["clients"], status: status);

        var outcome = await _store.EnsureScheduledEtlRunAsync(EnsureRequest("nightly", ["clients"]), DateTimeOffset.UtcNow, CancellationToken.None);

        Assert.Equal(runId, Assert.IsType<EtlScheduledRunEnsureOutcome.ActiveExisting>(outcome).RunId);
        Assert.Equal(1, await ScalarAsync("SELECT COUNT(*) FROM etl_runs"));
        await Assert.ThrowsAsync<SqliteException>(() => NewScheduledRunAsync("nightly", ["clients"]));
    }

    [Fact]
    public async Task Ensure_after_cancelled_mints_a_new_run_for_the_same_key()
    {
        var old = await NewScheduledRunAsync("nightly", ["clients"], status: "cancelled");

        var created = Assert.IsType<EtlScheduledRunEnsureOutcome.Created>(
            await _store.EnsureScheduledEtlRunAsync(EnsureRequest("nightly", ["clients"]), DateTimeOffset.UtcNow, CancellationToken.None));

        Assert.NotEqual(old, created.RunId);
    }

    [Fact]
    public async Task Job_dispatch_diagnostics_report_a_corrupt_pending_scheduled_run()
    {
        // The scheduled enumeration omits a corrupt run; the quarantine diagnostic is
        // surfaced by the dispatch page (root disposition: diagnostics even when nothing
        // is eligible), so a corrupt scheduled elder never blocks younger work silently.
        var runId = await NewScheduledRunAsync("nightly", ["clients"], resolvedEntitiesJson: "[]");

        var page = await _store.GetDispatchableEtlJobsAsync(10, DateTimeOffset.UtcNow, CancellationToken.None);

        var diagnostic = Assert.Single(page.Quarantined);
        Assert.Equal(runId, diagnostic.RunId);
        Assert.Null(diagnostic.JobId);
        Assert.Equal("MANIFEST_INVALID", diagnostic.Code);
        Assert.Empty(await _store.GetDueScheduledRunsAsync(10, DateTimeOffset.UtcNow, CancellationToken.None));
    }

    [Theory]
    [InlineData("bootstrap_full")]
    [InlineData("entity_reload")]
    public async Task Stored_non_incremental_scheduled_run_never_dispatches_anywhere(string mode)
    {
        // A scheduled row whose mode is not incremental (e.g. left by an earlier build) is
        // corrupt frozen identity: never due, never claimed, and as an elder it is
        // quarantined for a younger overlapping manual job.
        var elder = await NewScheduledRunAsync("legacy-mode", ["clients"], createdAtUtc: "2026-09-20T00:00:00.0000000+00:00", mode: mode);
        Assert.Empty(await _store.GetDueScheduledRunsAsync(10, DateTimeOffset.UtcNow, CancellationToken.None));
        Assert.Contains((await _store.GetDispatchableEtlJobsAsync(10, DateTimeOffset.UtcNow, CancellationToken.None)).Quarantined, q => q.RunId == elder);

        var (_, youngerJob) = await NewPendingJobRunAsync(QueryMode, ["clients"], createdAtUtc: "2026-09-20T00:00:01.0000000+00:00");
        Assert.IsType<EtlJobClaimOutcome.Claimed>(await _store.TryClaimEtlJobAsync(youngerJob, "dispatcher-1", DateTimeOffset.UtcNow.AddMinutes(5), DateTimeOffset.UtcNow, CancellationToken.None));
        Assert.Equal("blocked", await RunStatusAsync(elder));
        Assert.Equal("MANIFEST_INVALID", await ScalarStringAsync($"SELECT finalize_conflict_code FROM etl_runs WHERE run_id='{elder:D}'"));

        var direct = await NewScheduledRunAsync("legacy-mode-2", ["orders"], mode: mode);
        Assert.Equal("SCHEDULED_RUN_INCONSISTENT", Assert.IsType<EtlScheduledRunClaimOutcome.Blocked>(
            await _store.TryClaimScheduledRunAsync(direct, "scheduler-1", DateTimeOffset.UtcNow, CancellationToken.None)).Code);
    }

    [Theory]
    [InlineData("cancelled", false)]
    [InlineData("blocked", true)]
    [InlineData("failed", true)]
    public async Task Index_releases_the_key_for_cancelled_and_resolved_terminal_runs(string status, bool resolved)
    {
        var first = await NewScheduledRunAsync("nightly", ["clients"], status: status);
        if (resolved) await ExecuteSqlAsync("UPDATE etl_runs SET resolved_at_utc=$now WHERE run_id=$run;", ("$now", Now()), ("$run", first.ToString("D")));

        // A second pending row for the same key is accepted at the storage level.
        var second = await NewScheduledRunAsync("nightly", ["clients"]);

        Assert.NotEqual(first, second);
        Assert.Equal(2, await ScalarAsync("SELECT COUNT(*) FROM etl_runs WHERE schedule_key='nightly'"));
    }

    [Fact]
    public async Task Ensure_after_succeeded_mints_a_new_run_for_the_same_key()
    {
        var old = await NewScheduledRunAsync("nightly", ["clients"], status: "succeeded");

        var outcome = await _store.EnsureScheduledEtlRunAsync(EnsureRequest("nightly", ["clients"]), DateTimeOffset.UtcNow, CancellationToken.None);

        var created = Assert.IsType<EtlScheduledRunEnsureOutcome.Created>(outcome);
        Assert.NotEqual(old, created.RunId);
        Assert.Equal(2, await ScalarAsync("SELECT COUNT(*) FROM etl_runs"));
    }

    [Fact]
    public async Task Different_schedule_keys_are_independent()
    {
        var a = Assert.IsType<EtlScheduledRunEnsureOutcome.Created>(
            await _store.EnsureScheduledEtlRunAsync(EnsureRequest("nightly", ["clients"]), DateTimeOffset.UtcNow, CancellationToken.None));
        var b = Assert.IsType<EtlScheduledRunEnsureOutcome.Created>(
            await _store.EnsureScheduledEtlRunAsync(EnsureRequest("hourly", ["orders"]), DateTimeOffset.UtcNow, CancellationToken.None));

        Assert.NotEqual(a.RunId, b.RunId);
        Assert.Equal(2, await ScalarAsync("SELECT COUNT(*) FROM etl_runs WHERE schedule_key IN ('nightly','hourly') AND status='pending'"));
    }

    [Theory]
    [InlineData("blank_key")]
    [InlineData("whitespace_key")]
    [InlineData("null_entities")]
    [InlineData("empty_entities")]
    [InlineData("duplicate_codes")]
    [InlineData("invalid_definition_pagesize")]
    [InlineData("disabled_entity")]
    [InlineData("unsupported_mode")]
    [InlineData("full_mode_is_manual_only")]
    [InlineData("reload_mode_is_manual_only")]
    [InlineData("padded_key")]
    [InlineData("leading_space_key")]
    [InlineData("tab_key")]
    [InlineData("negative_version")]
    public async Task Ensure_rejects_invalid_requests_with_an_argument_exception_and_zero_writes(string violation)
    {
        var clients = MakeEntities().Single(e => e.EntityCode == "clients");
        var request = violation switch
        {
            "blank_key" => new EtlScheduledRunRequest("", "incremental", [clients], 7),
            "whitespace_key" => new EtlScheduledRunRequest("   ", "incremental", [clients], 7),
            "null_entities" => new EtlScheduledRunRequest("nightly", "incremental", null!, 7),
            "empty_entities" => new EtlScheduledRunRequest("nightly", "incremental", [], 7),
            "duplicate_codes" => new EtlScheduledRunRequest("nightly", "incremental", [clients, clients], 7),
            "invalid_definition_pagesize" => new EtlScheduledRunRequest("nightly", "incremental", [clients with { PageSize = 0 }], 7),
            "disabled_entity" => new EtlScheduledRunRequest("nightly", "incremental", [clients with { Enabled = false }], 7),
            "unsupported_mode" => new EtlScheduledRunRequest("nightly", "reconcile_keys", [clients], 7),
            "full_mode_is_manual_only" => new EtlScheduledRunRequest("nightly", "bootstrap_full", [clients], 7),
            "reload_mode_is_manual_only" => new EtlScheduledRunRequest("nightly", "entity_reload", [clients], 7),
            "padded_key" => new EtlScheduledRunRequest("nightly ", "incremental", [clients], 7),
            "leading_space_key" => new EtlScheduledRunRequest(" nightly", "incremental", [clients], 7),
            "tab_key" => new EtlScheduledRunRequest("nightly	", "incremental", [clients], 7),
            "negative_version" => new EtlScheduledRunRequest("nightly", "incremental", [clients], -1),
            _ => throw new ArgumentOutOfRangeException(nameof(violation))
        };

        await Assert.ThrowsAnyAsync<ArgumentException>(() => _store.EnsureScheduledEtlRunAsync(request, DateTimeOffset.UtcNow, CancellationToken.None));

        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM etl_runs"));
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM etl_jobs"));
    }

    [Fact]
    public async Task Concurrent_ensures_on_two_stores_mint_exactly_one_run()
    {
        var other = new SqliteAgentStore(_factory, new SqliteMigrator(_factory));
        var barrier = new Barrier(2);
        var now = DateTimeOffset.UtcNow;
        var request = EnsureRequest("nightly", ["clients"]);

        var first = Task.Run(async () =>
        {
            barrier.SignalAndWait();
            return await _store.EnsureScheduledEtlRunAsync(request, now, CancellationToken.None);
        });
        var second = Task.Run(async () =>
        {
            barrier.SignalAndWait();
            return await other.EnsureScheduledEtlRunAsync(request, now, CancellationToken.None);
        });
        var results = await Task.WhenAll(first, second).WaitAsync(GateTimeout);

        var created = Assert.Single(results.OfType<EtlScheduledRunEnsureOutcome.Created>());
        var existing = Assert.Single(results.OfType<EtlScheduledRunEnsureOutcome.ActiveExisting>());
        Assert.Equal(created.RunId, existing.RunId);
        Assert.Equal(1, await ScalarAsync("SELECT COUNT(*) FROM etl_runs"));
    }

    // ---------- claim ----------

    [Fact]
    public async Task Claim_commits_running_transition_ownership_bindings_and_a_fresh_fence_in_one_shot()
    {
        var runId = await NewScheduledRunAsync("nightly", ["clients", "orders"]);
        var run = runId.ToString("D");
        var resolvedBefore = await ScalarStringAsync($"SELECT resolved_entities_json FROM etl_runs WHERE run_id='{run}'");

        var outcome = await _store.TryClaimScheduledRunAsync(runId, "scheduler-1", DateTimeOffset.UtcNow, CancellationToken.None);

        var claim = Assert.IsType<EtlScheduledRunClaimOutcome.Claimed>(outcome).Claim;
        Assert.Equal(runId, claim.RunId);
        Assert.Equal("nightly", claim.ScheduleKey);
        Assert.Equal("incremental", claim.Mode);
        Assert.Equal(7, claim.ConfigurationVersion);
        Assert.Equal(resolvedBefore, claim.ResolvedEntitiesJson);
        // The extraction fence is a fresh GUID — never the owner string.
        Assert.NotEqual(Guid.Empty, claim.ExtractionClaimId);
        Assert.Equal(claim.ExtractionClaimId.ToString("D"), await ScalarStringAsync($"SELECT extraction_claim_id FROM etl_runs WHERE run_id='{run}'"));
        Assert.NotEqual("scheduler-1", await ScalarStringAsync($"SELECT extraction_claim_id FROM etl_runs WHERE run_id='{run}'"));
        Assert.Equal("running", await RunStatusAsync(runId));
        Assert.NotNull(await ScalarStringAsync($"SELECT started_at_utc FROM etl_runs WHERE run_id='{run}'"));
        // Ownership for every manifest entity at epoch 1 — owner_job_id stays NULL for
        // a scheduled claim; bindings record the committed epoch set.
        Assert.Equal(2, await ScalarAsync($"SELECT COUNT(*) FROM etl_entity_ownership WHERE owner_run_id='{run}' AND owner_job_id IS NULL AND ownership_epoch=1 AND released_at_utc IS NULL"));
        Assert.Equal(2, await ScalarAsync($"SELECT COUNT(*) FROM etl_run_ownership_bindings WHERE run_id='{run}' AND expected_epoch=1"));
        Assert.Equal(0, await ScalarAsync($"SELECT COUNT(*) FROM etl_jobs WHERE run_id='{run}'"));
    }

    [Fact]
    public async Task Claim_is_read_only_not_claimable_for_unknown_non_pending_or_non_scheduled_runs()
    {
        var runId = await NewScheduledRunAsync("nightly", ["clients"]);
        var runningId = await NewScheduledRunAsync("hourly", ["orders"], status: "running");
        var (jobRunId, _) = await NewPendingJobRunAsync(QueryMode, ["payments"]);
        var legacyId = await NewBareRunAsync(["shipments"]);
        var owner = "scheduler-1";
        var now = DateTimeOffset.UtcNow;

        Assert.IsType<EtlScheduledRunClaimOutcome.NotClaimable>(await _store.TryClaimScheduledRunAsync(Guid.NewGuid(), owner, now, CancellationToken.None));
        Assert.IsType<EtlScheduledRunClaimOutcome.NotClaimable>(await _store.TryClaimScheduledRunAsync(runningId, owner, now, CancellationToken.None));
        Assert.IsType<EtlScheduledRunClaimOutcome.NotClaimable>(await _store.TryClaimScheduledRunAsync(jobRunId, owner, now, CancellationToken.None));
        Assert.IsType<EtlScheduledRunClaimOutcome.NotClaimable>(await _store.TryClaimScheduledRunAsync(legacyId, owner, now, CancellationToken.None));

        // Zero writes: every row untouched, no ownership, bindings or claim minted.
        Assert.Equal("pending", await RunStatusAsync(runId));
        Assert.Equal("running", await RunStatusAsync(runningId));
        Assert.Equal("pending", await RunStatusAsync(jobRunId));
        Assert.Equal("pending", await RunStatusAsync(legacyId));
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM etl_entity_ownership"));
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM etl_run_ownership_bindings"));
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM etl_runs WHERE extraction_claim_id IS NOT NULL"));
    }

    [Fact]
    public async Task Claim_defers_busy_entity_and_the_partial_acquisition_writes_nothing()
    {
        // 'orders' is owned by an active manual-job run; the scheduled manifest
        // {clients, orders} can never be partially acquired.
        var ownerRun = await NewRunAsync("orders");
        var scheduledId = await NewScheduledRunAsync("nightly", ["clients", "orders"]);
        var run = scheduledId.ToString("D");
        var ownerRowBefore = await ScalarStringAsync("SELECT row_version FROM etl_entity_ownership WHERE entity_name='orders'");

        var outcome = await _store.TryClaimScheduledRunAsync(scheduledId, "scheduler-1", DateTimeOffset.UtcNow, CancellationToken.None);

        var deferred = Assert.IsType<EtlScheduledRunClaimOutcome.Deferred>(outcome);
        Assert.Equal(EtlJobDeferralReason.BusyEntity, deferred.Reason);
        Assert.Equal("pending", await RunStatusAsync(scheduledId));
        // The claimant's probe bump rolls back: a deferral writes nothing on the run row.
        Assert.Equal("1", await ScalarStringAsync($"SELECT row_version FROM etl_runs WHERE run_id='{run}'"));
        Assert.Equal(0, await ScalarAsync($"SELECT COUNT(*) FROM etl_entity_ownership WHERE owner_run_id='{run}'"));
        Assert.Equal(0, await ScalarAsync($"SELECT COUNT(*) FROM etl_run_ownership_bindings WHERE run_id='{run}'"));
        // 'clients' was never acquired by the scheduled run; the other owner row is untouched.
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM etl_entity_ownership WHERE entity_name='clients'"));
        Assert.Equal(ownerRowBefore, await ScalarStringAsync("SELECT row_version FROM etl_entity_ownership WHERE entity_name='orders'"));
        Assert.Equal(ownerRun.ToString("D"), await ScalarStringAsync("SELECT owner_run_id FROM etl_entity_ownership WHERE entity_name='orders'"));
    }

    [Fact]
    public async Task Elder_pending_scheduled_run_reserves_its_overlap_against_a_younger_manual_job()
    {
        var elderId = await NewScheduledRunAsync("nightly", ["clients"], createdAtUtc: "2026-09-20T00:00:00.0000000+00:00");
        var (youngerRun, youngerJob) = await NewPendingJobRunAsync(QueryMode, ["clients"], createdAtUtc: "2026-09-20T00:00:01.0000000+00:00");

        var outcome = await _store.TryClaimEtlJobAsync(youngerJob, "dispatcher-1", DateTimeOffset.UtcNow.AddMinutes(5), DateTimeOffset.UtcNow, CancellationToken.None);

        var deferred = Assert.IsType<EtlJobClaimOutcome.Deferred>(outcome);
        Assert.Equal(EtlJobDeferralReason.QueuedOverlap, deferred.Reason);
        Assert.Equal("pending", await RunStatusAsync(youngerRun));
        Assert.Equal("queued_overlap", await ScalarStringAsync($"SELECT deferral_code FROM etl_jobs WHERE job_id='{youngerJob:D}'"));
        Assert.Equal("pending", await RunStatusAsync(elderId));
        Assert.Equal(0, await ScalarAsync($"SELECT COUNT(*) FROM etl_entity_ownership WHERE owner_run_id='{youngerRun:D}'"));
    }

    [Fact]
    public async Task Elder_pending_manual_job_reserves_its_overlap_against_a_younger_scheduled_run()
    {
        var (elderRun, _) = await NewPendingJobRunAsync(QueryMode, ["orders"], createdAtUtc: "2026-09-20T00:00:00.0000000+00:00");
        var youngerId = await NewScheduledRunAsync("nightly", ["orders"], createdAtUtc: "2026-09-20T00:00:01.0000000+00:00");

        var outcome = await _store.TryClaimScheduledRunAsync(youngerId, "scheduler-1", DateTimeOffset.UtcNow, CancellationToken.None);

        var deferred = Assert.IsType<EtlScheduledRunClaimOutcome.Deferred>(outcome);
        Assert.Equal(EtlJobDeferralReason.QueuedOverlap, deferred.Reason);
        Assert.Equal("pending", await RunStatusAsync(youngerId));
        Assert.Equal("pending", await RunStatusAsync(elderRun));
        Assert.Equal(0, await ScalarAsync($"SELECT COUNT(*) FROM etl_entity_ownership WHERE owner_run_id='{youngerId:D}'"));
    }

    [Fact]
    public async Task Disjoint_pending_scheduled_and_manual_runs_are_never_blocked_by_order()
    {
        var scheduledId = await NewScheduledRunAsync("nightly", ["clients"], createdAtUtc: "2026-09-20T00:00:00.0000000+00:00");
        var (_, jobId) = await NewPendingJobRunAsync(QueryMode, ["orders"], createdAtUtc: "2026-09-20T00:00:01.0000000+00:00");

        Assert.IsType<EtlJobClaimOutcome.Claimed>(
            await _store.TryClaimEtlJobAsync(jobId, "dispatcher-1", DateTimeOffset.UtcNow.AddMinutes(5), DateTimeOffset.UtcNow, CancellationToken.None));
        Assert.IsType<EtlScheduledRunClaimOutcome.Claimed>(
            await _store.TryClaimScheduledRunAsync(scheduledId, "scheduler-1", DateTimeOffset.UtcNow, CancellationToken.None));

        Assert.Equal("running", await RunStatusAsync(scheduledId));
        Assert.Equal(2, await ScalarAsync("SELECT COUNT(*) FROM etl_entity_ownership WHERE released_at_utc IS NULL"));
    }

    [Fact]
    public async Task Once_the_elder_is_claimed_the_overlapping_younger_defers_busy_entity_not_queued_overlap()
    {
        // Elder scheduled run claimed first: its ownership makes the younger manual job busy.
        var elderId = await NewScheduledRunAsync("nightly", ["clients"], createdAtUtc: "2026-09-20T00:00:00.0000000+00:00");
        var (_, youngerJob) = await NewPendingJobRunAsync(QueryMode, ["clients"], createdAtUtc: "2026-09-20T00:00:01.0000000+00:00");
        Assert.IsType<EtlScheduledRunClaimOutcome.Claimed>(
            await _store.TryClaimScheduledRunAsync(elderId, "scheduler-1", DateTimeOffset.UtcNow, CancellationToken.None));

        var jobOutcome = await _store.TryClaimEtlJobAsync(youngerJob, "dispatcher-1", DateTimeOffset.UtcNow.AddMinutes(5), DateTimeOffset.UtcNow, CancellationToken.None);

        Assert.Equal(EtlJobDeferralReason.BusyEntity, Assert.IsType<EtlJobClaimOutcome.Deferred>(jobOutcome).Reason);
        Assert.Equal("busy_entity", await ScalarStringAsync($"SELECT deferral_code FROM etl_jobs WHERE job_id='{youngerJob:D}'"));

        // Symmetric: elder manual job claimed first makes the younger scheduled run busy.
        var (elderJobRun, elderJob) = await NewPendingJobRunAsync(QueryMode, ["orders"], createdAtUtc: "2026-09-20T00:00:00.0000000+00:00");
        var youngerScheduled = await NewScheduledRunAsync("hourly", ["orders"], createdAtUtc: "2026-09-20T00:00:01.0000000+00:00");
        Assert.IsType<EtlJobClaimOutcome.Claimed>(
            await _store.TryClaimEtlJobAsync(elderJob, "dispatcher-1", DateTimeOffset.UtcNow.AddMinutes(5), DateTimeOffset.UtcNow, CancellationToken.None));

        var scheduledOutcome = await _store.TryClaimScheduledRunAsync(youngerScheduled, "scheduler-1", DateTimeOffset.UtcNow, CancellationToken.None);

        Assert.Equal(EtlJobDeferralReason.BusyEntity, Assert.IsType<EtlScheduledRunClaimOutcome.Deferred>(scheduledOutcome).Reason);
        Assert.Equal("pending", await RunStatusAsync(youngerScheduled));
        Assert.Equal("running", await RunStatusAsync(elderJobRun));
    }

    [Theory]
    [InlineData("codes_mismatch")]
    [InlineData("malformed")]
    [InlineData("empty_array")]
    [InlineData("null")]
    public async Task Corrupt_frozen_identity_never_dispatches_never_started_runs_are_blocked(string corruption)
    {
        var resolved = corruption switch
        {
            "codes_mismatch" => DefinitionsJson(["orders"]),
            "malformed" => "{corrupt",
            "empty_array" => "[]",
            "null" => null,
            _ => throw new ArgumentOutOfRangeException(nameof(corruption))
        };

        // Provably never started: quarantine blocks the pending run. The blocked run is
        // unresolved, so it KEEPS the schedule key (fail closed) — no successor is minted.
        var inertId = await NewScheduledRunAsync($"k-{corruption}-inert", ["clients"], resolvedEntitiesJson: resolved);
        var inertOutcome = await _store.TryClaimScheduledRunAsync(inertId, "scheduler-1", DateTimeOffset.UtcNow, CancellationToken.None);
        var inertBlocked = Assert.IsType<EtlScheduledRunClaimOutcome.Blocked>(inertOutcome);
        Assert.Equal("SCHEDULED_RUN_INCONSISTENT", inertBlocked.Code);
        Assert.Equal("blocked", await RunStatusAsync(inertId));
        Assert.Equal("SCHEDULED_RUN_INCONSISTENT", await ScalarStringAsync($"SELECT finalize_conflict_code FROM etl_runs WHERE run_id='{inertId:D}'"));
        Assert.Equal(0, await ScalarAsync($"SELECT COUNT(*) FROM etl_entity_ownership WHERE owner_run_id='{inertId:D}'"));
        var heldKey = await _store.EnsureScheduledEtlRunAsync(EnsureRequest($"k-{corruption}-inert", ["clients"]), DateTimeOffset.UtcNow, CancellationToken.None);
        var holder = Assert.IsType<EtlScheduledRunEnsureOutcome.ActiveExisting>(heldKey);
        Assert.Equal(inertId, holder.RunId);
        Assert.Equal("blocked", holder.Status);
        Assert.Equal(1, await ScalarAsync($"SELECT COUNT(*) FROM etl_runs WHERE schedule_key='k-{corruption}-inert'"));

        // With an unproven prior effect the run keeps its pending admission hold —
        // corrupt evidence is reported but nothing is written and nothing is retired.
        var effectId = await NewScheduledRunAsync($"k-{corruption}-effect", ["clients"], resolvedEntitiesJson: resolved);
        await ExecuteSqlAsync("UPDATE etl_runs SET rows_read=1, row_version=7 WHERE run_id=$run;", ("$run", effectId.ToString("D")));
        var effectOutcome = await _store.TryClaimScheduledRunAsync(effectId, "scheduler-1", DateTimeOffset.UtcNow, CancellationToken.None);
        Assert.Equal("SCHEDULED_RUN_INCONSISTENT", Assert.IsType<EtlScheduledRunClaimOutcome.Blocked>(effectOutcome).Code);
        Assert.Equal("pending", await RunStatusAsync(effectId));
        Assert.Equal("7", await ScalarStringAsync($"SELECT row_version FROM etl_runs WHERE run_id='{effectId:D}'"));
        Assert.Equal("1", await ScalarStringAsync($"SELECT rows_read FROM etl_runs WHERE run_id='{effectId:D}'"));
        Assert.Equal(0, await ScalarAsync($"SELECT COUNT(*) FROM etl_entity_ownership WHERE owner_run_id='{effectId:D}'"));
    }

    [Fact]
    public async Task Corrupt_scheduled_elder_is_quarantined_or_holds_admission_for_a_younger_job_claim()
    {
        // Inert corrupt elder: quarantined MANIFEST_INVALID, the overlapping younger
        // manual job claims.
        var inertElder = await NewScheduledRunAsync("k-inert", ["clients"], createdAtUtc: "2026-09-20T00:00:00.0000000+00:00", resolvedEntitiesJson: "{corrupt");
        var (youngerRun, youngerJob) = await NewPendingJobRunAsync(QueryMode, ["clients"], createdAtUtc: "2026-09-20T00:00:01.0000000+00:00");

        var outcome = await _store.TryClaimEtlJobAsync(youngerJob, "dispatcher-1", DateTimeOffset.UtcNow.AddMinutes(5), DateTimeOffset.UtcNow, CancellationToken.None);

        Assert.IsType<EtlJobClaimOutcome.Claimed>(outcome);
        Assert.Equal("blocked", await RunStatusAsync(inertElder));
        Assert.Equal("MANIFEST_INVALID", await ScalarStringAsync($"SELECT finalize_conflict_code FROM etl_runs WHERE run_id='{inertElder:D}'"));
        Assert.Equal("running", await RunStatusAsync(youngerRun));

        // Elder with an unproven effect: the younger defers elder_manifest_invalid and
        // the elder's admission hold is untouched.
        var effectElder = await NewScheduledRunAsync("k-effect", ["orders"], createdAtUtc: "2026-09-20T00:00:00.0000000+00:00", resolvedEntitiesJson: "{corrupt");
        await ExecuteSqlAsync("UPDATE etl_runs SET rows_read=1, row_version=7 WHERE run_id=$run;", ("$run", effectElder.ToString("D")));
        var (_, heldJob) = await NewPendingJobRunAsync(QueryMode, ["orders"], createdAtUtc: "2026-09-20T00:00:01.0000000+00:00");

        var held = await _store.TryClaimEtlJobAsync(heldJob, "dispatcher-1", DateTimeOffset.UtcNow.AddMinutes(5), DateTimeOffset.UtcNow, CancellationToken.None);

        Assert.Equal(EtlJobDeferralReason.ElderManifestInvalid, Assert.IsType<EtlJobClaimOutcome.Deferred>(held).Reason);
        Assert.Equal("pending", await RunStatusAsync(effectElder));
        Assert.Equal("7", await ScalarStringAsync($"SELECT row_version FROM etl_runs WHERE run_id='{effectElder:D}'"));
    }

    [Fact]
    public async Task Concurrent_claims_of_the_same_scheduled_run_mint_exactly_one_claim()
    {
        var runId = await NewScheduledRunAsync("nightly", ["clients", "orders"]);
        var storeA = new SqliteAgentStore(_factory, new SqliteMigrator(_factory));
        var storeB = new SqliteAgentStore(_factory, new SqliteMigrator(_factory));
        var barrier = new Barrier(2);
        var now = DateTimeOffset.UtcNow;

        var attemptA = Task.Run(async () =>
        {
            barrier.SignalAndWait();
            return await storeA.TryClaimScheduledRunAsync(runId, "scheduler-a", now, CancellationToken.None);
        });
        var attemptB = Task.Run(async () =>
        {
            barrier.SignalAndWait();
            return await storeB.TryClaimScheduledRunAsync(runId, "scheduler-b", now, CancellationToken.None);
        });
        var results = await Task.WhenAll(attemptA, attemptB).WaitAsync(GateTimeout);

        Assert.Single(results.OfType<EtlScheduledRunClaimOutcome.Claimed>());
        Assert.Single(results.OfType<EtlScheduledRunClaimOutcome.NotClaimable>());
        Assert.Equal("running", await RunStatusAsync(runId));
        Assert.Equal(2, await ScalarAsync($"SELECT COUNT(*) FROM etl_entity_ownership WHERE owner_run_id='{runId:D}'"));
        Assert.Equal(2, await ScalarAsync($"SELECT COUNT(*) FROM etl_run_ownership_bindings WHERE run_id='{runId:D}'"));
        Assert.Equal(1, await ScalarAsync($"SELECT COUNT(*) FROM etl_runs WHERE run_id='{runId:D}'"));
    }

    [Fact]
    public async Task Ensure_returns_the_running_holder_after_a_claim()
    {
        var runId = await NewScheduledRunAsync("nightly", ["clients"]);
        Assert.IsType<EtlScheduledRunClaimOutcome.Claimed>(
            await _store.TryClaimScheduledRunAsync(runId, "scheduler-1", DateTimeOffset.UtcNow, CancellationToken.None));

        var outcome = await _store.EnsureScheduledEtlRunAsync(EnsureRequest("nightly", ["clients"]), DateTimeOffset.UtcNow, CancellationToken.None);

        var existing = Assert.IsType<EtlScheduledRunEnsureOutcome.ActiveExisting>(outcome);
        Assert.Equal(runId, existing.RunId);
        Assert.Equal("running", existing.Status);
        Assert.Equal(1, await ScalarAsync("SELECT COUNT(*) FROM etl_runs"));
    }

    // ---------- begin: frozen-definition binding ----------

    [Fact]
    public async Task Begin_with_the_exact_frozen_definition_begins_extraction()
    {
        // D1: a scheduled (incremental) read never establishes a domain — seed the
        // committed baseline watermark of this domain (generation 1) so Begin sees
        // 'same', not absent.
        await SeedBaselineAsync("clients", CursorJson(CommittedClients));
        var (runId, claim) = await ClaimedScheduledRunAsync("nightly", ["clients"]);

        var outcome = await _store.BeginEtlEntityExtractionAsync(runId, claim.ExtractionClaimId, Request("clients", ScheduledMode), CancellationToken.None);

        var begun = Assert.IsType<EtlEntityBeginOutcome.Begun>(outcome);
        Assert.Equal("same", begun.Base.DomainStatus);
        Assert.Equal(CursorJson(CommittedClients), begun.Base.CommittedCursorJson);
        Assert.Equal(1, begun.Base.ExpectedBaseGeneration);
        Assert.Equal("extracting", await ScalarStringAsync($"SELECT status FROM etl_run_entities WHERE run_id='{runId:D}' AND entity_name='clients'"));
    }

    [Fact]
    public async Task Begin_on_a_scheduled_incremental_without_a_baseline_is_rejected_and_recorded_as_a_failed_entity()
    {
        // D1: an incremental read on an entity with no committed watermark row can
        // never establish the domain — a full baseline must come first.
        var (runId, claim) = await ClaimedScheduledRunAsync("nightly", ["clients"]);

        var outcome = await _store.BeginEtlEntityExtractionAsync(runId, claim.ExtractionClaimId, Request("clients", ScheduledMode), CancellationToken.None);

        Assert.Equal(EtlEntityBeginRejection.BaselineRequired, Assert.IsType<EtlEntityBeginOutcome.Rejected>(outcome).Reason);
        // Partial runs: a failed, skippable entity row with its code and no expected batches.
        Assert.Equal(1, await ScalarAsync($"SELECT COUNT(*) FROM etl_run_entities WHERE run_id='{runId:D}' AND status='failed' AND failure_code='BASELINE_REQUIRED' AND expected_batch_count=0"));
        Assert.Equal("running", await RunStatusAsync(runId));
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM watermarks WHERE entity_name='clients'"));
    }

    [Fact]
    public async Task Begin_with_a_different_definition_is_rejected_run_definition_mismatch_with_zero_writes()
    {
        var (runId, claim) = await ClaimedScheduledRunAsync("nightly", ["clients"]);
        var drifted = MakeEntities().Single(e => e.EntityCode == "clients") with { PageSize = 1000 };
        var request = new EtlEntityExtractionRequest("clients",
            JsonSerializer.Serialize(drifted, JsonOptions), SourceNamespace, ScheduledMode,
            JsonSerializer.Serialize(new EtlCursor(DateTimeOffset.UtcNow, null), JsonOptions));

        var outcome = await _store.BeginEtlEntityExtractionAsync(runId, claim.ExtractionClaimId, request, CancellationToken.None);

        Assert.Equal(EtlEntityBeginRejection.RunDefinitionMismatch,
            Assert.IsType<EtlEntityBeginOutcome.Rejected>(outcome).Reason);
        Assert.Equal(0, await ScalarAsync($"SELECT COUNT(*) FROM etl_run_entities WHERE run_id='{runId:D}'"));
        Assert.Equal("running", await RunStatusAsync(runId));
    }

    [Fact]
    public async Task Begin_on_a_scheduled_run_tampered_with_an_etl_jobs_row_is_rejected_job_inconsistent()
    {
        // A scheduled run must never carry an etl_jobs row; injected post-claim, Begin
        // fails closed instead of mixing the two identity sources.
        var (runId, claim) = await ClaimedScheduledRunAsync("nightly", ["clients"]);
        await ExecuteSqlAsync(
            "INSERT INTO etl_jobs(job_id,command_id,run_id,mode,entities_json,configuration_version,status,command_payload_hash,acceptance_result_json,created_at_utc,updated_at_utc,row_version) VALUES($job,$cmd,$run,'bootstrap_full',$defs,7,'pending','hash','{}',$now,$now,1);",
            ("$job", Guid.NewGuid().ToString("D")), ("$cmd", Guid.NewGuid().ToString("D")), ("$run", runId.ToString("D")),
            ("$defs", DefinitionsJson(["clients"])), ("$now", Now()));

        var outcome = await _store.BeginEtlEntityExtractionAsync(runId, claim.ExtractionClaimId, Request("clients", ScheduledMode), CancellationToken.None);

        Assert.Equal(EtlEntityBeginRejection.JobInconsistent,
            Assert.IsType<EtlEntityBeginOutcome.Rejected>(outcome).Reason);
        Assert.Equal(0, await ScalarAsync($"SELECT COUNT(*) FROM etl_run_entities WHERE run_id='{runId:D}'"));
    }

    [Fact]
    public async Task Full_scheduled_pipeline_extracts_uploads_finalizes_and_releases_the_key()
    {
        // D1: the scheduled incremental continues an established baseline — the
        // committed watermark row of the same domain at generation 1.
        await SeedBaselineAsync("clients", CursorJson(CommittedClients));
        var now = DateTimeOffset.UtcNow;
        var request = EnsureRequest("nightly", ["clients"]);
        var ensured = Assert.IsType<EtlScheduledRunEnsureOutcome.Created>(
            await _store.EnsureScheduledEtlRunAsync(request, now, CancellationToken.None));
        var runId = ensured.RunId;
        var run = runId.ToString("D");
        var claim = Assert.IsType<EtlScheduledRunClaimOutcome.Claimed>(
            await _store.TryClaimScheduledRunAsync(runId, "scheduler-1", now, CancellationToken.None)).Claim.ExtractionClaimId;

        var begun = Assert.IsType<EtlEntityBeginOutcome.Begun>(
            await _store.BeginEtlEntityExtractionAsync(runId, claim, Request("clients", ScheduledMode), CancellationToken.None));
        Assert.Equal("same", begun.Base.DomainStatus);
        Assert.Equal(CursorJson(CommittedClients), begun.Base.CommittedCursorJson);
        Assert.Equal(1, begun.Base.ExpectedBaseGeneration);
        var batch = MakeBatch(runId, "clients", 5);
        Assert.IsType<EtlBatchRegistrationOutcome.Registered>(
            await _store.RegisterGuardedEtlBatchAsync(batch, claim, CancellationToken.None));
        Assert.IsType<EtlEntityCompletionOutcome.Completed>(
            await _store.CompleteEtlEntityExtractionAsync(runId, claim, "clients", CursorJson(FinalClients), 1, CancellationToken.None));
        Assert.IsType<EtlRunSealOutcome.Sealed>(
            await _store.SealEtlRunExtractionAsync(runId, claim, CancellationToken.None));

        var upload = Assert.IsType<EtlBatchUploadClaimOutcome.Claimed>(
            await _store.TryClaimBatchUploadAsync(batch.BatchId, "uploader-1", DateTimeOffset.UtcNow, 5, CancellationToken.None));
        Assert.IsType<EtlBatchAckOutcome.Acknowledged>(
            await _store.AcknowledgeClaimedBatchAsync(batch.BatchId, upload.Claim.AttemptId,
                new EtlBatchAckEvidence("acknowledged", batch.BatchId, 5, true, DateTimeOffset.UtcNow), "ack-hash", 200, CancellationToken.None));

        var completion = Assert.IsType<EtlRunClaimOutcome.Claimed>(
            await _store.TryClaimRunCompletionAsync(runId, "finalizer", DateTimeOffset.UtcNow, 8, CancellationToken.None));
        Assert.IsType<EtlRunFinalizeOutcome.Finalized>(
            await _store.FinalizeEtlRunAsync(runId, completion.Claim.ClaimId, CancellationToken.None));

        Assert.Equal("succeeded", await RunStatusAsync(runId));
        Assert.Equal(1, await ScalarAsync($"SELECT COUNT(*) FROM etl_entity_ownership WHERE owner_run_id='{run}' AND released_at_utc IS NOT NULL AND release_reason='finalized'"));
        Assert.Equal(CursorJson(FinalClients), await ScalarStringAsync("SELECT committed_cursor_json FROM watermarks WHERE entity_name='clients'"));
        Assert.Equal(run, await ScalarStringAsync("SELECT last_run_id FROM watermarks WHERE entity_name='clients'"));
        // The seeded baseline took the UPDATE CAS path: generation 1 -> 2 in the SAME
        // domain — the fingerprint equals the seeded one byte-for-byte.
        Assert.Equal(2, await ScalarAsync("SELECT generation FROM watermarks WHERE entity_name='clients'"));
        Assert.Equal(
            EtlDomainFingerprint.Compute(SourceNamespace, "clients", DefinitionJson("clients"), ScheduledMode),
            await ScalarStringAsync("SELECT domain_fingerprint FROM watermarks WHERE entity_name='clients'"));

        // 'succeeded' released the schedule key: the next tick mints a new run.
        var next = Assert.IsType<EtlScheduledRunEnsureOutcome.Created>(
            await _store.EnsureScheduledEtlRunAsync(request, DateTimeOffset.UtcNow, CancellationToken.None));
        Assert.NotEqual(runId, next.RunId);
        Assert.Equal(2, await ScalarAsync("SELECT COUNT(*) FROM etl_runs"));
    }

    // ---------- due enumeration ----------

    [Fact]
    public async Task Due_enumeration_returns_only_eligible_scheduled_runs_in_run_order()
    {
        var t0 = "2026-09-20T00:00:00.0000000+00:00";
        var eligibleA = await NewScheduledRunAsync("k-alpha", ["clients"], createdAtUtc: t0);
        var eligibleB = await NewScheduledRunAsync("k-beta", ["payments"], createdAtUtc: t0);
        var elder = await NewScheduledRunAsync("k-elder", ["inventory"], createdAtUtc: "2026-09-20T00:00:01.0000000+00:00");
        // Ineligible: a younger overlapped pending run, a running run, a corrupt frozen
        // identity, a busy-entity run, and a job-backed (non-scheduled) run.
        await NewScheduledRunAsync("k-overlapped", ["inventory"], createdAtUtc: "2026-09-20T00:00:02.0000000+00:00");
        await NewScheduledRunAsync("k-running", ["warehouses"], createdAtUtc: t0, status: "running");
        await NewScheduledRunAsync("k-corrupt", ["shipments"], createdAtUtc: t0, resolvedEntitiesJson: "{corrupt");
        await NewRunAsync("orders");
        await NewScheduledRunAsync("k-busy", ["orders"], createdAtUtc: t0);
        await NewPendingJobRunAsync(QueryMode, ["returns"], createdAtUtc: t0);

        var due = await _store.GetDueScheduledRunsAsync(10, DateTimeOffset.UtcNow, CancellationToken.None);

        var head = new[] { eligibleA, eligibleB }.OrderBy(id => id.ToString("D"), StringComparer.Ordinal).ToArray();
        Assert.Equal([head[0], head[1], elder], due.Select(d => d.RunId).ToArray());
        Assert.Equal(due[0].RunId == eligibleA ? "k-alpha" : "k-beta", due[0].ScheduleKey);
        Assert.Equal("k-elder", due[2].ScheduleKey);
        Assert.All(due, d =>
        {
            Assert.Equal("incremental", d.Mode);
            Assert.Equal(7, d.ConfigurationVersion);
        });
    }

    [Fact]
    public async Task Due_enumeration_applies_eligibility_before_limit_and_is_read_only()
    {
        // A busy head can never hide disjoint eligible work under LIMIT.
        await NewRunAsync("orders");
        var busyHead = await NewScheduledRunAsync("k-head", ["orders"], createdAtUtc: "2026-09-20T00:00:00.0000000+00:00");
        var eligible = await NewScheduledRunAsync("k-tail", ["clients"], createdAtUtc: "2026-09-20T00:00:01.0000000+00:00");
        var runsBefore = await SnapshotRunsAsync();

        var due = await _store.GetDueScheduledRunsAsync(1, DateTimeOffset.UtcNow, CancellationToken.None);

        var run = Assert.Single(due);
        Assert.Equal(eligible, run.RunId);
        Assert.Equal("k-tail", run.ScheduleKey);
        // Read-only: nothing about any run row changed.
        Assert.Equal(runsBefore, await SnapshotRunsAsync());
        Assert.Equal("pending", await RunStatusAsync(busyHead));
    }

    // ---------- recovery ----------

    [Fact]
    public async Task Recovery_blocks_a_running_scheduled_run_and_the_unresolved_key_keeps_dedup()
    {
        // A scheduled run claimed by a now-dead host: 'running' with a live extraction
        // fence and retained ownership.
        var runId = await NewScheduledRunAsync("nightly", ["clients"], status: "running");
        var run = runId.ToString("D");
        var claim = Guid.NewGuid();
        await ExecuteSqlAsync(
            "UPDATE etl_runs SET started_at_utc=$now, extraction_claim_id=$claim, extraction_claim_owner_id='scheduler-1', extraction_claim_acquired_at_utc=$now WHERE run_id=$run;",
            ("$now", Now()), ("$claim", claim.ToString("D")), ("$run", run));
        await ExecuteSqlAsync(
            "INSERT INTO etl_entity_ownership(entity_name,owner_run_id,owner_job_id,ownership_epoch,acquired_at_utc,updated_at_utc,row_version) VALUES('clients',$run,NULL,1,$now,$now,1);",
            ("$run", run), ("$now", Now()));
        await ExecuteSqlAsync(
            "INSERT INTO etl_run_ownership_bindings(run_id,entity_name,expected_epoch,acquired_at_utc) VALUES($run,'clients',1,$now);",
            ("$run", run), ("$now", Now()));

        var recovered = await _store.RecoverInterruptedEtlRunsAsync(CancellationToken.None);

        Assert.Equal(1, recovered.RunsBlocked);
        Assert.Equal("blocked", await RunStatusAsync(runId));
        Assert.Equal("INTERRUPTED_NO_CHECKPOINT", await ScalarStringAsync($"SELECT finalize_conflict_code FROM etl_runs WHERE run_id='{run}'"));
        Assert.Null(await ScalarStringAsync($"SELECT extraction_claim_id FROM etl_runs WHERE run_id='{run}'"));
        // Ownership evidence is retained — recovery never releases it.
        Assert.Equal(1, await ScalarAsync($"SELECT COUNT(*) FROM etl_entity_ownership WHERE owner_run_id='{run}' AND released_at_utc IS NULL"));

        // The unresolved blocked run still holds the schedule key: no successor.
        var ensure = await _store.EnsureScheduledEtlRunAsync(EnsureRequest("nightly", ["clients"]), DateTimeOffset.UtcNow, CancellationToken.None);
        var existing = Assert.IsType<EtlScheduledRunEnsureOutcome.ActiveExisting>(ensure);
        Assert.Equal(runId, existing.RunId);
        Assert.Equal("blocked", existing.Status);
        Assert.Equal(1, await ScalarAsync("SELECT COUNT(*) FROM etl_runs"));
    }

    // ---------- helpers ----------

    private const string AutoResolved = "AUTO";

    private static string Now() => DateTimeOffset.UtcNow.ToString("O");
    private static string CursorJson(EtlCursor cursor) => JsonSerializer.Serialize(cursor, JsonOptions);
    private static string ManifestJson(string[] entities) => JsonSerializer.Serialize(entities, JsonOptions);
    private static string DefinitionsJson(string[] entities) =>
        JsonSerializer.Serialize(entities.Select(e => MakeEntities().Single(x => x.EntityCode == e)).ToArray(), JsonOptions);
    private static string DefinitionJson(string entity) =>
        JsonSerializer.Serialize(MakeEntities().Single(e => e.EntityCode == entity), JsonOptions);

    private static EtlEntityDefinition[] MakeEntities() =>
    [
        new("clients", "Catalog_Clients", "Ref_Key", "UpdatedAt", "DeletionMark", ["Ref_Key", "UpdatedAt", "DeletionMark"], "incremental", 500, 10),
        new("orders", "Document_Orders", "Ref_Key", "Date", "Posted", ["Ref_Key", "Date", "Posted"], "incremental", 500, 10),
        new("payments", "Document_Payments", "Ref_Key", "Date", "Posted", ["Ref_Key", "Date", "Posted"], "incremental", 500, 10),
        new("inventory", "Document_Inventory", "Ref_Key", "Date", "Posted", ["Ref_Key", "Date", "Posted"], "incremental", 500, 10),
        new("shipments", "Document_Shipments", "Ref_Key", "Date", "Posted", ["Ref_Key", "Date", "Posted"], "incremental", 500, 10),
        new("warehouses", "Catalog_Warehouses", "Ref_Key", "UpdatedAt", "DeletionMark", ["Ref_Key", "UpdatedAt", "DeletionMark"], "incremental", 500, 10),
        new("returns", "Document_Returns", "Ref_Key", "Date", "Posted", ["Ref_Key", "Date", "Posted"], "incremental", 500, 10)
    ];

    private static EtlScheduledRunRequest EnsureRequest(string scheduleKey, string[] entities, string mode = "incremental", long configurationVersion = 7) =>
        new(scheduleKey, mode, entities.Select(e => MakeEntities().Single(x => x.EntityCode == e)).ToArray(), configurationVersion);

    private static EtlEntityExtractionRequest Request(string entity, string queryMode = QueryMode) =>
        new(entity, DefinitionJson(entity), SourceNamespace, queryMode,
            JsonSerializer.Serialize(new EtlCursor(DateTimeOffset.UtcNow, null), JsonOptions));

    private static EtlBatch MakeBatch(Guid runId, string entity, int rowCount) =>
        new(Guid.NewGuid(), runId, entity, 1, $"spool/{runId:N}-{entity}-{Guid.NewGuid():N}.gz", EtlBatchStatus.Ready, rowCount,
            null, FinalClients, "hash-" + Guid.NewGuid().ToString("N"), 1000, 4000, 0, DateTimeOffset.UtcNow);

    private static void AssertDefinitionsEqual(EtlEntityDefinition expected, EtlEntityDefinition actual)
    {
        Assert.Equal(expected.EntityCode, actual.EntityCode);
        Assert.Equal(expected.ODataPath, actual.ODataPath);
        Assert.Equal(expected.KeyField, actual.KeyField);
        Assert.Equal(expected.UpdatedAtField, actual.UpdatedAtField);
        Assert.Equal(expected.DeletedField, actual.DeletedField);
        Assert.Equal(expected.Select, actual.Select);
        Assert.Equal(expected.SyncMode, actual.SyncMode);
        Assert.Equal(expected.PageSize, actual.PageSize);
        Assert.Equal(expected.OverlapMinutes, actual.OverlapMinutes);
        Assert.Equal(expected.SchemaVersion, actual.SchemaVersion);
        Assert.Equal(expected.ODataVersion, actual.ODataVersion);
        Assert.Equal(expected.Enabled, actual.Enabled);
        Assert.Equal(expected.KeyFields, actual.KeyFields);
        Assert.Equal(expected.UpdatedAtEdmType, actual.UpdatedAtEdmType);
    }

    // A pending scheduled (jobless) run staged directly in SQL — the durable shape
    // Ensure commits: manifest in request order, frozen resolved definitions, no job.
    private async Task<Guid> NewScheduledRunAsync(string scheduleKey, string[] entities, string? createdAtUtc = null,
        string status = "pending", string mode = "incremental", long configurationVersion = 7, string? resolvedEntitiesJson = AutoResolved)
    {
        var runId = Guid.NewGuid();
        var created = createdAtUtc ?? Now();
        var resolved = ReferenceEquals(resolvedEntitiesJson, AutoResolved) ? DefinitionsJson(entities) : resolvedEntitiesJson;
        await ExecuteSqlAsync(
            "INSERT INTO etl_runs(run_id,mode,requested_entities_json,status,configuration_version,created_at_utc,updated_at_utc,row_version,schedule_key,resolved_entities_json) VALUES($run,$mode,$manifest,$status,$config,$now,$now,1,$key,$resolved);",
            ("$run", runId.ToString("D")), ("$mode", mode), ("$manifest", ManifestJson(entities)), ("$status", status),
            ("$config", configurationVersion), ("$now", created), ("$key", scheduleKey), ("$resolved", resolved));
        return runId;
    }

    // A pending jobless run with no schedule key — pre-O3/legacy shape, never scheduled.
    private async Task<Guid> NewBareRunAsync(string[] entities, string? createdAtUtc = null)
    {
        var runId = Guid.NewGuid();
        var created = createdAtUtc ?? Now();
        await ExecuteSqlAsync(
            "INSERT INTO etl_runs(run_id,mode,requested_entities_json,status,configuration_version,created_at_utc,updated_at_utc,row_version) VALUES($run,$mode,$manifest,'pending',7,$now,$now,1);",
            ("$run", runId.ToString("D")), ("$mode", QueryMode), ("$manifest", ManifestJson(entities)), ("$now", created));
        return runId;
    }

    // Pending run + pending durable job with consistent frozen identity (not claimed).
    private async Task<(Guid RunId, Guid JobId)> NewPendingJobRunAsync(string mode, string[] entities, string? createdAtUtc = null)
    {
        var runId = Guid.NewGuid();
        var jobId = Guid.NewGuid();
        var created = createdAtUtc ?? Now();
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
        Assert.IsType<EtlJobClaimOutcome.Claimed>(
            await _store.TryClaimEtlJobAsync(jobId, "test-dispatcher", DateTimeOffset.UtcNow.AddMinutes(5), DateTimeOffset.UtcNow, CancellationToken.None));
        return runId;
    }

    // D1: an established baseline — a committed watermark row in the entity's domain
    // (the fingerprint over the frozen definition; cursor class is mode-independent).
    private async Task SeedBaselineAsync(string entity, string? cursorJson, long generation = 1)
    {
        await ExecuteSqlAsync(
            "INSERT INTO watermarks(entity_name,committed_cursor_json,extracting_cursor_json,last_run_id,generation,domain_fingerprint,updated_at_utc) VALUES($entity,$cursor,NULL,$run,$gen,$fp,$now);",
            ("$entity", entity), ("$cursor", cursorJson), ("$run", Guid.NewGuid().ToString("D")), ("$gen", generation),
            ("$fp", EtlDomainFingerprint.Compute(SourceNamespace, entity, DefinitionJson(entity), ScheduledMode)), ("$now", Now()));
    }

    // Ensure + claim: one committed scheduled run extraction claim.
    private async Task<(Guid RunId, EtlScheduledRunClaim Claim)> ClaimedScheduledRunAsync(string scheduleKey, string[] entities, string mode = ScheduledMode)
    {
        var ensured = Assert.IsType<EtlScheduledRunEnsureOutcome.Created>(
            await _store.EnsureScheduledEtlRunAsync(EnsureRequest(scheduleKey, entities, mode), DateTimeOffset.UtcNow, CancellationToken.None));
        var claimed = Assert.IsType<EtlScheduledRunClaimOutcome.Claimed>(
            await _store.TryClaimScheduledRunAsync(ensured.RunId, "scheduler-1", DateTimeOffset.UtcNow, CancellationToken.None));
        return (ensured.RunId, claimed.Claim);
    }

    private async Task<List<string>> SnapshotRunsAsync()
    {
        var rows = new List<string>();
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT run_id,status,row_version,schedule_key,resolved_entities_json FROM etl_runs ORDER BY run_id;";
        await using var reader = await command.ExecuteReaderAsync(CancellationToken.None);
        while (await reader.ReadAsync(CancellationToken.None))
        {
            var values = new string[reader.FieldCount];
            for (var i = 0; i < reader.FieldCount; i++) values[i] = reader.IsDBNull(i) ? "<null>" : reader.GetValue(i).ToString() ?? "";
            rows.Add(string.Join("|", values));
        }
        return rows;
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
