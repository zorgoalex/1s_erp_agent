using System.Globalization;
using System.Text.Json;
using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Domain.Commands;
using ErpOnecAgent.Domain.Common;
using ErpOnecAgent.Domain.Etl;
using ErpOnecAgent.Infrastructure.Persistence.Sqlite;
using Microsoft.Data.Sqlite;
using Xunit;

namespace ErpOnecAgent.IntegrationTests;

/// <summary>
/// Durable manual-ETL-job acceptance foundation (schema v5): the typed store API accepts a job under
/// the CURRENT persisted execution owner in ONE transaction (pending run + job + result_pending +
/// outbox), replays the original acceptance identity for repeated commandIds, refuses foreign/stale/
/// missing owners and any possible-POST state without mutation, and keeps all acceptance evidence
/// alive through ERP result ACK and aged cleanup while the job is unresolved. All cases run against
/// a real migrated temporary SQLite database.
/// </summary>
public sealed class EtlJobFoundationTests : IAsyncLifetime
{
    private static readonly TimeSpan GateTimeout = TimeSpan.FromSeconds(10);
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);
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

    [Fact]
    public async Task Acceptance_commits_pending_job_run_result_and_outbox_in_one_transaction()
    {
        var command = MakeCommand();
        var owner = await StoreAndClaimAsync(command);
        var entities = MakeEntities();

        var outcome = await AcceptAsync(command.CommandId, owner, new("bootstrap_full", entities, 7));

        var applied = Assert.IsType<EtlJobAcceptanceOutcome.Applied>(outcome);
        var job = await JobAsync(command.CommandId);
        Assert.NotNull(job);
        Assert.Equal(applied.Job.JobId.ToString("D"), job.JobId);
        Assert.Equal(applied.Job.RunId.ToString("D"), job.RunId);
        Assert.Equal("bootstrap_full", job.Mode);
        Assert.Equal("pending", job.Status);
        Assert.Equal(7, job.ConfigVersion);
        Assert.Equal(JsonSerializer.Serialize(entities, JsonOptions), job.EntitiesJson);
        Assert.Equal(command.PayloadHash, job.PayloadHash);
        Assert.Equal(applied.Job.AcceptanceResultJson, job.AcceptanceResultJson);
        Assert.Equal(job.CreatedAtUtc, job.UpdatedAtUtc);
        Assert.Equal(1, job.RowVersion);

        var run = await RunAsync(job.RunId);
        Assert.NotNull(run);
        Assert.Equal("pending", run.Status);
        Assert.Equal("bootstrap_full", run.Mode);
        Assert.Equal("[\"clients\",\"orders\"]", run.RequestedEntitiesJson);
        Assert.Equal(7, run.ConfigVersion);
        Assert.Null(run.StartedAtUtc);
        Assert.Null(run.FinishedAtUtc);
        Assert.Equal(job.CreatedAtUtc, run.CreatedAtUtc);
        Assert.Equal(1, run.RowVersion);

        var inbox = await CommandAsync(command.CommandId);
        Assert.NotNull(inbox);
        Assert.Equal("result_pending", inbox.Status);
        Assert.Equal("succeeded_local", inbox.ResultStatus);
        Assert.Equal(job.AcceptanceResultJson, inbox.ResultJson);
        Assert.Null(inbox.ClaimOwner);
        Assert.Null(inbox.ClaimAcquiredAt);
        Assert.NotNull(inbox.FinishedAtUtc);
        Assert.Null(inbox.NextAttemptAtUtc);

        var outbox = await OutboxAsync(command.CommandId);
        Assert.NotNull(outbox);
        Assert.Equal("pending", outbox.Status);
        Assert.Equal(job.AcceptanceResultJson, outbox.PayloadJson);
        Assert.Equal(PayloadHasher.ComputeBytes(System.Text.Encoding.UTF8.GetBytes(job.AcceptanceResultJson)), outbox.PayloadHash);

        using var document = JsonDocument.Parse(job.AcceptanceResultJson);
        Assert.Equal("succeeded", document.RootElement.GetProperty("status").GetString());
        var data = document.RootElement.GetProperty("data");
        Assert.True(data.GetProperty("accepted").GetBoolean());
        Assert.Equal(job.RunId, data.GetProperty("runId").GetString());
        Assert.Equal("bootstrap_full", data.GetProperty("mode").GetString());
        Assert.Equal(command.CommandId.ToString("D"), document.RootElement.GetProperty("commandId").GetString());

        var pending = Assert.Single(await _store.GetPendingResultsAsync(10, DateTimeOffset.UtcNow, CancellationToken.None));
        Assert.Equal(command.CommandId, pending.CommandId);
        Assert.Equal(job.AcceptanceResultJson, pending.PayloadJson);
    }

    [Theory]
    [InlineData("result")]
    [InlineData("run")]
    [InlineData("job")]
    [InlineData("outbox")]
    public async Task Acceptance_rolls_back_all_four_writes_when_any_stage_fails(string stage)
    {
        var command = MakeCommand();
        var owner = await StoreAndClaimAsync(command);
        var trigger = stage switch
        {
            "result" => "CREATE TRIGGER fail_accept_result BEFORE UPDATE ON commands_inbox BEGIN SELECT RAISE(ABORT,'injected result failure'); END;",
            "run" => "CREATE TRIGGER fail_accept_run BEFORE INSERT ON etl_runs BEGIN SELECT RAISE(ABORT,'injected run failure'); END;",
            "job" => "CREATE TRIGGER fail_accept_job BEFORE INSERT ON etl_jobs BEGIN SELECT RAISE(ABORT,'injected job failure'); END;",
            _ => "CREATE TRIGGER fail_accept_outbox BEFORE INSERT ON results_outbox BEGIN SELECT RAISE(ABORT,'injected outbox failure'); END;"
        };
        var drop = stage switch
        {
            "result" => "DROP TRIGGER fail_accept_result;",
            "run" => "DROP TRIGGER fail_accept_run;",
            "job" => "DROP TRIGGER fail_accept_job;",
            _ => "DROP TRIGGER fail_accept_outbox;"
        };
        await ExecuteSqlAsync(trigger);

        await Assert.ThrowsAsync<SqliteException>(() => AcceptAsync(command.CommandId, owner, new("bootstrap_full", MakeEntities(), 7)));

        var inbox = await CommandAsync(command.CommandId);
        Assert.NotNull(inbox);
        Assert.Equal("queued", inbox.Status);
        Assert.Equal(owner, inbox.ClaimOwner);
        Assert.Null(inbox.ResultStatus);
        Assert.Null(inbox.ResultJson);
        Assert.Equal(2, inbox.RowVersion);
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM etl_runs"));
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM etl_jobs"));
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM results_outbox"));

        // The rollback leaves the claimed never-sent row fully accept-able again.
        await ExecuteSqlAsync(drop);
        var retry = await AcceptAsync(command.CommandId, owner, new("bootstrap_full", MakeEntities(), 7));
        Assert.IsType<EtlJobAcceptanceOutcome.Applied>(retry);
        Assert.Equal(1, await ScalarAsync("SELECT COUNT(*) FROM etl_jobs"));
        Assert.Equal(1, await ScalarAsync("SELECT COUNT(*) FROM etl_runs WHERE status='pending'"));
        Assert.Equal(1, await ScalarAsync("SELECT COUNT(*) FROM results_outbox"));
    }

    [Fact]
    public async Task Seventeen_independently_claimed_commands_create_seventeen_durable_jobs()
    {
        var runIds = new List<Guid>();
        for (var index = 0; index < 17; index++)
        {
            var command = MakeCommand();
            var owner = await StoreAndClaimAsync(command);
            var outcome = await AcceptAsync(command.CommandId, owner, new("bootstrap_full", MakeEntities(), 7));
            var applied = Assert.IsType<EtlJobAcceptanceOutcome.Applied>(outcome);
            runIds.Add(applied.Job.RunId);
        }

        Assert.Equal(17, runIds.Distinct().Count());
        Assert.Equal(17, await ScalarAsync("SELECT COUNT(*) FROM etl_jobs"));
        Assert.Equal(17, await ScalarAsync("SELECT COUNT(*) FROM etl_jobs WHERE status='pending'"));
        Assert.Equal(17, await ScalarAsync("SELECT COUNT(*) FROM etl_runs WHERE status='pending'"));
        Assert.Equal(17, await ScalarAsync("SELECT COUNT(DISTINCT run_id) FROM etl_runs"));
        Assert.Equal(17, await ScalarAsync("SELECT COUNT(*) FROM commands_inbox WHERE status='result_pending' AND result_status='succeeded_local'"));
        Assert.Equal(17, await ScalarAsync("SELECT COUNT(*) FROM results_outbox WHERE status='pending'"));
    }

    [Fact]
    public async Task Repeated_acceptance_replays_original_identity_and_exact_result()
    {
        var command = MakeCommand(commandType: "reload_entity", payloadJson: "{\"entity\":\"clients\"}");
        var owner = await StoreAndClaimAsync(command);
        var request = new EtlJobAcceptanceRequest("entity_reload", [MakeEntities()[0]], 3);
        var applied = Assert.IsType<EtlJobAcceptanceOutcome.Applied>(await AcceptAsync(command.CommandId, owner, request));
        var inboxBefore = await CommandAsync(command.CommandId);
        var outboxBefore = await OutboxAsync(command.CommandId);
        var jobBefore = await JobAsync(command.CommandId);

        var repeat = await AcceptAsync(command.CommandId, owner, request);

        var replayed = Assert.IsType<EtlJobAcceptanceOutcome.AlreadyAccepted>(repeat);
        Assert.Equal(applied.Job, replayed.Job);
        Assert.Equal(inboxBefore, await CommandAsync(command.CommandId));
        Assert.Equal(outboxBefore, await OutboxAsync(command.CommandId));
        Assert.Equal(jobBefore, await JobAsync(command.CommandId));
        Assert.Equal(1, await ScalarAsync("SELECT COUNT(*) FROM etl_jobs"));
        Assert.Equal(1, await ScalarAsync("SELECT COUNT(*) FROM etl_runs"));
        Assert.Equal(1, await ScalarAsync("SELECT COUNT(*) FROM results_outbox"));
    }

    [Fact]
    public async Task Changed_valid_request_on_same_command_replays_original_job_unchanged()
    {
        var command = MakeCommand();
        var owner = await StoreAndClaimAsync(command);
        var applied = Assert.IsType<EtlJobAcceptanceOutcome.Applied>(
            await AcceptAsync(command.CommandId, owner, new("bootstrap_full", MakeEntities(), 7)));
        var jobBefore = await JobAsync(command.CommandId);
        var inboxBefore = await CommandAsync(command.CommandId);
        var outboxBefore = await OutboxAsync(command.CommandId);

        // A different — but independently valid — request (other selection, other config version)
        // cannot overwrite the original acceptance: identity, entities, config, and result stay frozen.
        var repeat = await AcceptAsync(command.CommandId, owner, new("bootstrap_full", [MakeEntities()[0]], 9));

        var replayed = Assert.IsType<EtlJobAcceptanceOutcome.AlreadyAccepted>(repeat);
        Assert.Equal(applied.Job, replayed.Job);
        Assert.Equal(jobBefore, await JobAsync(command.CommandId));
        Assert.Equal(inboxBefore, await CommandAsync(command.CommandId));
        Assert.Equal(outboxBefore, await OutboxAsync(command.CommandId));
        Assert.Equal(1, await ScalarAsync("SELECT COUNT(*) FROM etl_jobs"));
        Assert.Equal(1, await ScalarAsync("SELECT COUNT(*) FROM etl_runs"));
        Assert.Equal(1, await ScalarAsync("SELECT COUNT(*) FROM results_outbox"));
    }

    [Fact]
    public async Task Foreign_owner_never_creates_job_and_never_mutates_the_claim()
    {
        var command = MakeCommand();
        var owner = await StoreAndClaimAsync(command);
        var before = await CommandAsync(command.CommandId);

        var outcome = await AcceptAsync(command.CommandId, "foreign-owner", new("bootstrap_full", MakeEntities(), 7));

        var rejected = Assert.IsType<EtlJobAcceptanceOutcome.NotApplied>(outcome);
        Assert.Equal(EtlJobAcceptanceRejection.ClaimNotOwned, rejected.Reason);
        Assert.Equal(before, await CommandAsync(command.CommandId));
        Assert.Equal(owner, (await CommandAsync(command.CommandId))!.ClaimOwner);
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM etl_jobs"));
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM etl_runs"));
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM results_outbox"));
    }

    [Fact]
    public async Task Released_or_missing_owner_never_creates_job()
    {
        var released = MakeCommand();
        var releasedOwner = await StoreAndClaimAsync(released);
        await _store.ReleaseCommandExecutionClaimAsync(released.CommandId, releasedOwner, CancellationToken.None);
        var unclaimed = MakeCommand();
        await _store.StoreCommandAsync(unclaimed, DateTimeOffset.UtcNow, CancellationToken.None);

        var releasedOutcome = await AcceptAsync(released.CommandId, releasedOwner, new("bootstrap_full", MakeEntities(), 7));
        var unclaimedOutcome = await AcceptAsync(unclaimed.CommandId, "any-owner", new("bootstrap_full", MakeEntities(), 7));

        Assert.Equal(EtlJobAcceptanceRejection.ClaimNotOwned, Assert.IsType<EtlJobAcceptanceOutcome.NotApplied>(releasedOutcome).Reason);
        Assert.Equal(EtlJobAcceptanceRejection.ClaimNotOwned, Assert.IsType<EtlJobAcceptanceOutcome.NotApplied>(unclaimedOutcome).Reason);
        Assert.Equal("queued", (await CommandAsync(released.CommandId))!.Status);
        Assert.Equal("queued", (await CommandAsync(unclaimed.CommandId))!.Status);
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM etl_jobs"));
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM etl_runs"));
    }

    [Fact]
    public async Task Missing_command_is_not_applied()
    {
        var outcome = await AcceptAsync(Guid.NewGuid(), "any-owner", new("bootstrap_full", MakeEntities(), 7));

        Assert.Equal(EtlJobAcceptanceRejection.CommandNotFound, Assert.IsType<EtlJobAcceptanceOutcome.NotApplied>(outcome).Reason);
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM etl_jobs"));
    }

    [Fact]
    public async Task Post_evidence_prohibits_acceptance()
    {
        var command = MakeCommand();
        var owner = await StoreAndClaimAsync(command);
        Assert.NotNull(await _store.ClaimPostAttemptAsync(command.CommandId, owner, CancellationToken.None));
        var before = await CommandAsync(command.CommandId);

        var outcome = await AcceptAsync(command.CommandId, owner, new("bootstrap_full", MakeEntities(), 7));

        Assert.Equal(EtlJobAcceptanceRejection.CommandNotAcceptable, Assert.IsType<EtlJobAcceptanceOutcome.NotApplied>(outcome).Reason);
        Assert.Equal(before, await CommandAsync(command.CommandId));
        Assert.Equal(1, await ScalarAsync("SELECT COUNT(*) FROM command_attempts WHERE command_id='" + command.CommandId.ToString("D") + "'"));
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM etl_jobs"));
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM etl_runs"));
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM results_outbox"));
    }

    [Fact]
    public async Task Unknown_result_prohibits_acceptance()
    {
        var command = MakeCommand();
        var owner = await StoreAndClaimAsync(command);
        Assert.NotNull(await _store.ClaimPostAttemptAsync(command.CommandId, owner, CancellationToken.None));
        Assert.NotNull(await _store.ClaimLookupAttemptAsync(command.CommandId, owner, "STATUS_PENDING", "1C result is still unknown.", DateTimeOffset.UtcNow.AddMinutes(1), CancellationToken.None));
        var before = await CommandAsync(command.CommandId);
        Assert.Equal("unknown_result", before!.Status);

        var outcome = await AcceptAsync(command.CommandId, owner, new("bootstrap_full", MakeEntities(), 7));

        Assert.Equal(EtlJobAcceptanceRejection.CommandNotAcceptable, Assert.IsType<EtlJobAcceptanceOutcome.NotApplied>(outcome).Reason);
        Assert.Equal(before, await CommandAsync(command.CommandId));
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM etl_jobs"));
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM etl_runs"));
    }

    [Fact]
    public async Task Changed_saved_payload_against_existing_job_is_refused_without_any_write()
    {
        var command = MakeCommand();
        var owner = await StoreAndClaimAsync(command);
        Assert.IsType<EtlJobAcceptanceOutcome.Applied>(await AcceptAsync(command.CommandId, owner, new("bootstrap_full", MakeEntities(), 7)));
        var jobBefore = await JobAsync(command.CommandId);
        var inboxBefore = await CommandAsync(command.CommandId);
        // Simulate a changed re-admission reaching the row while the frozen job hash survives.
        await ExecuteSqlAsync("UPDATE commands_inbox SET payload_hash='changed-incoming-hash' WHERE command_id=$id;", ("$id", command.CommandId.ToString("D")));

        var outcome = await AcceptAsync(command.CommandId, owner, new("bootstrap_full", MakeEntities(), 7));

        var rejected = Assert.IsType<EtlJobAcceptanceOutcome.NotApplied>(outcome);
        Assert.Equal(EtlJobAcceptanceRejection.PayloadConflict, rejected.Reason);
        // NotApplied is strictly read-only: no conflict event, no job/result/outbox rewrite —
        // conflict evidence belongs to the admission path (004), not to this API.
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM command_payload_conflicts WHERE command_id='" + command.CommandId.ToString("D") + "'"));
        Assert.Equal(jobBefore, await JobAsync(command.CommandId));
        Assert.Equal(inboxBefore!.ResultJson, (await CommandAsync(command.CommandId))!.ResultJson);
        Assert.Equal(1, await ScalarAsync("SELECT COUNT(*) FROM etl_jobs"));
        Assert.Equal(1, await ScalarAsync("SELECT COUNT(*) FROM etl_runs"));
        Assert.Equal(1, await ScalarAsync("SELECT COUNT(*) FROM results_outbox"));

        // A foreign owner probing the same conflicting stored identity is likewise refused
        // read-only — nothing is recorded for them either.
        var foreign = await AcceptAsync(command.CommandId, "foreign-owner", new("bootstrap_full", MakeEntities(), 7));
        Assert.Equal(EtlJobAcceptanceRejection.PayloadConflict, Assert.IsType<EtlJobAcceptanceOutcome.NotApplied>(foreign).Reason);
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM command_payload_conflicts WHERE command_id='" + command.CommandId.ToString("D") + "'"));
        Assert.Equal(jobBefore, await JobAsync(command.CommandId));
    }

    [Fact]
    public async Task Cleanup_preserves_unresolved_job_evidence_after_erp_ack_and_aged_cutoff()
    {
        var accepted = MakeCommand();
        var owner = await StoreAndClaimAsync(accepted);
        var applied = Assert.IsType<EtlJobAcceptanceOutcome.Applied>(await AcceptAsync(accepted.CommandId, owner, new("bootstrap_full", MakeEntities(), 7)));
        Assert.True(await _store.AcknowledgeResultAsync(accepted.CommandId, DateTimeOffset.UtcNow, CancellationToken.None));
        Assert.Equal("completed", (await CommandAsync(accepted.CommandId))!.Status);

        // Control row: same completed+acknowledged shape but no job — cleanup must still delete it.
        var control = MakeCommand();
        var controlOwner = await StoreAndClaimAsync(control);
        var attempt = await _store.ClaimPostAttemptAsync(control.CommandId, controlOwner, CancellationToken.None);
        Assert.NotNull(attempt);
        Assert.True(await _store.CompleteLocallyAsync(control.CommandId, controlOwner, CommandStatus.SucceededLocal, "{\"status\":\"succeeded\"}", "ref", "42", attempt.AttemptId, CancellationToken.None));
        Assert.True(await _store.AcknowledgeResultAsync(control.CommandId, DateTimeOffset.UtcNow, CancellationToken.None));

        await _store.CleanupAsync(DateTimeOffset.UtcNow.AddDays(1), DateTimeOffset.UtcNow.AddDays(1), CancellationToken.None);

        Assert.Null(await CommandAsync(control.CommandId));
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM command_attempts WHERE command_id='" + control.CommandId.ToString("D") + "'"));
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM results_outbox WHERE command_id='" + control.CommandId.ToString("D") + "'"));

        // NOTE: a legitimately accepted command can never hold attempt rows (the acceptance guard
        // requires zero attempts), so command_attempts preservation under the unresolved-job guard
        // is verifiable here only by inspection of CleanupAsync; the control row proves attempts
        // still delete normally for non-job commands.
        var inbox = await CommandAsync(accepted.CommandId);
        Assert.NotNull(inbox);
        Assert.Equal(applied.Job.AcceptanceResultJson, inbox.ResultJson);
        Assert.NotNull(await OutboxAsync(accepted.CommandId));
        var job = await JobAsync(accepted.CommandId);
        Assert.NotNull(job);
        Assert.Equal(applied.Job.AcceptanceResultJson, job.AcceptanceResultJson);
        Assert.Equal(accepted.PayloadHash, job.PayloadHash);
        Assert.Equal("pending", job.Status);
        Assert.Equal("pending", (await RunAsync(job.RunId))!.Status);

        var replay = await AcceptAsync(accepted.CommandId, owner, new("bootstrap_full", MakeEntities(), 7));
        Assert.Equal(applied.Job, Assert.IsType<EtlJobAcceptanceOutcome.AlreadyAccepted>(replay).Job);
    }

    [Fact]
    public async Task Fresh_store_restart_and_recover_preserve_pending_job_and_identity()
    {
        var command = MakeCommand(commandType: "reload_entity", payloadJson: "{\"entity\":\"clients\"}");
        var owner = await StoreAndClaimAsync(command);
        var applied = Assert.IsType<EtlJobAcceptanceOutcome.Applied>(await AcceptAsync(command.CommandId, owner, new("entity_reload", [MakeEntities()[0]], 5)));

        await SqliteTestDatabase.ClearPoolAsync(_factory);
        var restarted = new SqliteAgentStore(_factory, new SqliteMigrator(_factory));
        await restarted.InitializeAsync(CancellationToken.None);
        await restarted.RecoverAsync(CancellationToken.None);

        var job = await JobAsync(command.CommandId);
        Assert.NotNull(job);
        Assert.Equal(applied.Job.JobId.ToString("D"), job.JobId);
        Assert.Equal(applied.Job.RunId.ToString("D"), job.RunId);
        Assert.Equal("pending", job.Status);
        Assert.Equal(applied.Job.AcceptanceResultJson, job.AcceptanceResultJson);
        Assert.Equal(command.PayloadHash, job.PayloadHash);
        Assert.Equal("pending", (await RunAsync(job.RunId))!.Status);
        Assert.Equal("result_pending", (await CommandAsync(command.CommandId))!.Status);
        var pending = Assert.Single(await restarted.GetPendingResultsAsync(10, DateTimeOffset.UtcNow, CancellationToken.None));
        Assert.Equal(applied.Job.AcceptanceResultJson, pending.PayloadJson);
    }

    // Two INDEPENDENT store instances over the same factory/file race on one command through
    // direct API calls — no retry wrapper. The write-first transaction serializes them on the
    // SQLite write lock: exactly one applies, the other replays the committed identity. A lock
    // error would surface here as an honest failure, not be masked by test-only retries.
    [Fact]
    public async Task Concurrent_accept_calls_create_exactly_one_job()
    {
        var command = MakeCommand();
        var owner = await StoreAndClaimAsync(command);
        var secondStore = new SqliteAgentStore(_factory, new SqliteMigrator(_factory));
        var request = new EtlJobAcceptanceRequest("bootstrap_full", MakeEntities(), 7);
        var barrier = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        var first = Task.Run(async () => { await barrier.Task; return await _store.AcceptEtlJobAndCompleteCommandAsync(command.CommandId, owner, request, CancellationToken.None); });
        var second = Task.Run(async () => { await barrier.Task; return await secondStore.AcceptEtlJobAndCompleteCommandAsync(command.CommandId, owner, request, CancellationToken.None); });
        barrier.SetResult(true);
        var outcomes = await Task.WhenAll(first, second).WaitAsync(GateTimeout);

        var applied = Assert.Single(outcomes.OfType<EtlJobAcceptanceOutcome.Applied>());
        var replayed = Assert.Single(outcomes.OfType<EtlJobAcceptanceOutcome.AlreadyAccepted>());
        Assert.Equal(applied.Job, replayed.Job);
        Assert.Equal(1, await ScalarAsync("SELECT COUNT(*) FROM etl_jobs"));
        Assert.Equal(1, await ScalarAsync("SELECT COUNT(*) FROM etl_runs"));
        Assert.Equal(1, await ScalarAsync("SELECT COUNT(*) FROM results_outbox"));
        Assert.Equal("result_pending", (await CommandAsync(command.CommandId))!.Status);
    }

    [Theory]
    [InlineData("reconcile_keys")]
    [InlineData("reconcile_totals")]
    [InlineData("incremental")]
    [InlineData("window_reload")]
    public async Task Unsupported_or_reconcile_mode_is_rejected_before_any_write(string mode)
    {
        var command = MakeCommand();
        var owner = await StoreAndClaimAsync(command);

        await Assert.ThrowsAsync<ArgumentException>(() => AcceptAsync(command.CommandId, owner, new(mode, MakeEntities(), 7)));

        var inbox = await CommandAsync(command.CommandId);
        Assert.NotNull(inbox);
        Assert.Equal("queued", inbox.Status);
        Assert.Equal(owner, inbox.ClaimOwner);
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM etl_jobs"));
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM etl_runs"));
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM results_outbox"));
    }

    [Fact]
    public async Task Empty_or_invalid_entity_set_is_rejected_before_any_write()
    {
        var command = MakeCommand();
        var owner = await StoreAndClaimAsync(command);
        var disabled = MakeEntities()[0] with { Enabled = false };
        var blankPath = MakeEntities()[0] with { ODataPath = " " };
        var mismatchedKeys = MakeEntities()[0] with { KeyFields = ["Other_Key"] };

        await Assert.ThrowsAsync<ArgumentException>(() => AcceptAsync(command.CommandId, owner, new("bootstrap_full", [], 7)));
        await Assert.ThrowsAsync<ArgumentException>(() => AcceptAsync(command.CommandId, owner, new("bootstrap_full", [disabled], 7)));
        await Assert.ThrowsAsync<ArgumentException>(() => AcceptAsync(command.CommandId, owner, new("bootstrap_full", [blankPath], 7)));
        await Assert.ThrowsAsync<ArgumentException>(() => AcceptAsync(command.CommandId, owner, new("bootstrap_full", [mismatchedKeys], 7)));
        await Assert.ThrowsAsync<ArgumentException>(() => AcceptAsync(command.CommandId, owner, new("bootstrap_full", [MakeEntities()[0], MakeEntities()[0]], 7)));
        await Assert.ThrowsAsync<ArgumentNullException>(() => AcceptAsync(command.CommandId, owner, new("bootstrap_full", null!, 7)));

        Assert.Equal("queued", (await CommandAsync(command.CommandId))!.Status);
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM etl_jobs"));
    }

    [Fact]
    public async Task Entity_reload_persists_frozen_definition_verbatim()
    {
        var command = MakeCommand(commandType: "reload_entity", payloadJson: "{\"entity\":\"clients\"}");
        var owner = await StoreAndClaimAsync(command);
        var entity = MakeEntities()[0];

        var outcome = await AcceptAsync(command.CommandId, owner, new("entity_reload", [entity], 11));

        var applied = Assert.IsType<EtlJobAcceptanceOutcome.Applied>(outcome);
        var job = await JobAsync(command.CommandId);
        Assert.NotNull(job);
        Assert.Equal("entity_reload", job.Mode);
        Assert.Equal(11, job.ConfigVersion);
        var frozen = JsonSerializer.Deserialize<List<EtlEntityDefinition>>(job.EntitiesJson, JsonOptions);
        var restored = Assert.Single(frozen!);
        Assert.Equal(entity.EntityCode, restored.EntityCode);
        Assert.Equal(entity.ODataPath, restored.ODataPath);
        Assert.Equal(entity.KeyField, restored.KeyField);
        Assert.Equal(entity.UpdatedAtField, restored.UpdatedAtField);
        Assert.Equal(entity.DeletedField, restored.DeletedField);
        Assert.Equal(entity.Select, restored.Select);
        Assert.Equal(entity.SyncMode, restored.SyncMode);
        Assert.Equal(entity.PageSize, restored.PageSize);
        Assert.Equal(entity.OverlapMinutes, restored.OverlapMinutes);
        Assert.Equal(entity.SchemaVersion, restored.SchemaVersion);
        Assert.Equal(entity.ODataVersion, restored.ODataVersion);
        Assert.Equal(entity.Enabled, restored.Enabled);
        Assert.Equal(entity.EffectiveKeyFields(), restored.EffectiveKeyFields());
        Assert.Equal(entity.UpdatedAtEdmType, restored.UpdatedAtEdmType);
        Assert.Equal("[\"clients\"]", (await RunAsync(job.RunId))!.RequestedEntitiesJson);
    }

    [Fact]
    public async Task Saved_reconcile_command_with_bootstrap_full_request_creates_no_job()
    {
        var command = MakeCommand(commandType: "reconcile_keys", payloadJson: "{}");
        var owner = await StoreAndClaimAsync(command);

        var outcome = await AcceptAsync(command.CommandId, owner, new("bootstrap_full", MakeEntities(), 7));

        Assert.Equal(EtlJobAcceptanceRejection.TypeModeMismatch, Assert.IsType<EtlJobAcceptanceOutcome.NotApplied>(outcome).Reason);
        await AssertAcceptanceLeftNoTraceAsync(command.CommandId, owner);
    }

    [Fact]
    public async Task Saved_business_command_with_bootstrap_full_request_creates_no_job()
    {
        var command = MakeCommand(commandType: "create_customer_order", payloadJson: "{\"amount\":10}");
        var owner = await StoreAndClaimAsync(command);

        var outcome = await AcceptAsync(command.CommandId, owner, new("bootstrap_full", MakeEntities(), 7));

        Assert.Equal(EtlJobAcceptanceRejection.TypeModeMismatch, Assert.IsType<EtlJobAcceptanceOutcome.NotApplied>(outcome).Reason);
        await AssertAcceptanceLeftNoTraceAsync(command.CommandId, owner);
    }

    [Fact]
    public async Task Saved_start_full_sync_requested_as_entity_reload_creates_no_job()
    {
        var command = MakeCommand();
        var owner = await StoreAndClaimAsync(command);

        var outcome = await AcceptAsync(command.CommandId, owner, new("entity_reload", [MakeEntities()[0]], 7));

        Assert.Equal(EtlJobAcceptanceRejection.TypeModeMismatch, Assert.IsType<EtlJobAcceptanceOutcome.NotApplied>(outcome).Reason);
        await AssertAcceptanceLeftNoTraceAsync(command.CommandId, owner);
    }

    [Fact]
    public async Task Saved_reload_entity_requested_as_bootstrap_full_creates_no_job()
    {
        var command = MakeCommand(commandType: "reload_entity", payloadJson: "{\"entity\":\"clients\"}");
        var owner = await StoreAndClaimAsync(command);

        var outcome = await AcceptAsync(command.CommandId, owner, new("bootstrap_full", [MakeEntities()[0]], 7));

        Assert.Equal(EtlJobAcceptanceRejection.TypeModeMismatch, Assert.IsType<EtlJobAcceptanceOutcome.NotApplied>(outcome).Reason);
        await AssertAcceptanceLeftNoTraceAsync(command.CommandId, owner);
    }

    [Fact]
    public async Task Saved_reload_entity_with_mismatched_resolved_entity_creates_no_job()
    {
        var command = MakeCommand(commandType: "reload_entity", payloadJson: "{\"entity\":\"clients\"}");
        var owner = await StoreAndClaimAsync(command);

        var outcome = await AcceptAsync(command.CommandId, owner, new("entity_reload", [MakeEntities()[1]], 7));

        Assert.Equal(EtlJobAcceptanceRejection.TypeModeMismatch, Assert.IsType<EtlJobAcceptanceOutcome.NotApplied>(outcome).Reason);
        await AssertAcceptanceLeftNoTraceAsync(command.CommandId, owner);
    }

    [Fact]
    public async Task Saved_reload_entity_with_multiple_resolved_entities_is_rejected()
    {
        var command = MakeCommand(commandType: "reload_entity", payloadJson: "{\"entity\":\"clients\"}");
        var owner = await StoreAndClaimAsync(command);

        await Assert.ThrowsAsync<ArgumentException>(() => AcceptAsync(command.CommandId, owner, new("entity_reload", MakeEntities(), 7)));

        await AssertAcceptanceLeftNoTraceAsync(command.CommandId, owner);
    }

    [Fact]
    public async Task Saved_start_full_sync_resolved_superset_of_payload_entities_creates_no_job()
    {
        var command = MakeCommand(payloadJson: "{\"entities\":[\"clients\"]}");
        var owner = await StoreAndClaimAsync(command);

        var outcome = await AcceptAsync(command.CommandId, owner, new("bootstrap_full", MakeEntities(), 7));

        Assert.Equal(EtlJobAcceptanceRejection.TypeModeMismatch, Assert.IsType<EtlJobAcceptanceOutcome.NotApplied>(outcome).Reason);
        await AssertAcceptanceLeftNoTraceAsync(command.CommandId, owner);
    }

    [Fact]
    public async Task Saved_start_full_sync_resolved_subset_of_payload_entities_creates_no_job()
    {
        var command = MakeCommand();
        var owner = await StoreAndClaimAsync(command);

        var outcome = await AcceptAsync(command.CommandId, owner, new("bootstrap_full", [MakeEntities()[0]], 7));

        Assert.Equal(EtlJobAcceptanceRejection.TypeModeMismatch, Assert.IsType<EtlJobAcceptanceOutcome.NotApplied>(outcome).Reason);
        await AssertAcceptanceLeftNoTraceAsync(command.CommandId, owner);
    }

    // Conservative saved-payload shape: root must be a JSON object; an explicit entities field
    // must be an ARRAY of non-empty STRING codes matching the resolved set. NULL elements exploit
    // three-valued NOT IN logic; objects/scalars/explicit null must refuse — not behave as omitted.
    [Theory]
    [InlineData("{\"entities\":null}")]
    [InlineData("{\"entities\":{}}")]
    [InlineData("{\"entities\":5}")]
    [InlineData("{\"entities\":true}")]
    [InlineData("{\"entities\":\"clients\"}")]
    [InlineData("{\"entities\":[null]}")]
    [InlineData("{\"entities\":[\"clients\",null]}")]
    [InlineData("{\"entities\":[42]}")]
    [InlineData("{\"entities\":[\"\"]}")]
    [InlineData("{\"entities\":[\"clients\",\"orders\",\"extra\"]}")]
    public async Task Saved_start_full_sync_invalid_entities_shape_refused(string payloadJson)
    {
        var command = MakeCommand(payloadJson: payloadJson);
        var owner = await StoreAndClaimAsync(command);

        var outcome = await AcceptAsync(command.CommandId, owner, new("bootstrap_full", MakeEntities(), 7));

        Assert.Equal(EtlJobAcceptanceRejection.TypeModeMismatch, Assert.IsType<EtlJobAcceptanceOutcome.NotApplied>(outcome).Reason);
        await AssertAcceptanceLeftNoTraceAsync(command.CommandId, owner);
    }

    [Theory]
    [InlineData("{}")]
    [InlineData("{\"entities\":[]}")]
    [InlineData("{\"entities\":[\"orders\",\"clients\"]}")]
    public async Task Saved_start_full_sync_valid_entities_shapes_accept(string payloadJson)
    {
        var command = MakeCommand(payloadJson: payloadJson);
        var owner = await StoreAndClaimAsync(command);

        var outcome = await AcceptAsync(command.CommandId, owner, new("bootstrap_full", MakeEntities(), 7));

        Assert.IsType<EtlJobAcceptanceOutcome.Applied>(outcome);
        Assert.Equal(1, await ScalarAsync("SELECT COUNT(*) FROM etl_jobs"));
    }

    [Theory]
    [InlineData("{\"entity\":42}")]
    [InlineData("{\"entity\":null}")]
    [InlineData("{\"entity\":{}}")]
    [InlineData("{\"entity\":\"\"}")]
    [InlineData("{\"entity\":[\"clients\"]}")]
    [InlineData("{}")]
    public async Task Saved_reload_entity_nonstring_or_missing_entity_refused(string payloadJson)
    {
        var command = MakeCommand(commandType: "reload_entity", payloadJson: payloadJson);
        var owner = await StoreAndClaimAsync(command);

        var outcome = await AcceptAsync(command.CommandId, owner, new("entity_reload", [MakeEntities()[0]], 7));

        Assert.Equal(EtlJobAcceptanceRejection.TypeModeMismatch, Assert.IsType<EtlJobAcceptanceOutcome.NotApplied>(outcome).Reason);
        await AssertAcceptanceLeftNoTraceAsync(command.CommandId, owner);
    }

    [Fact]
    public async Task Saved_start_full_sync_exact_entity_selection_is_accepted()
    {
        var command = MakeCommand(payloadJson: "{\"entities\":[\"clients\"]}");
        var owner = await StoreAndClaimAsync(command);

        var outcome = await AcceptAsync(command.CommandId, owner, new("bootstrap_full", [MakeEntities()[0]], 7));

        Assert.IsType<EtlJobAcceptanceOutcome.Applied>(outcome);
        Assert.Equal(1, await ScalarAsync("SELECT COUNT(*) FROM etl_jobs"));
    }

    [Fact]
    public async Task Saved_start_full_sync_omitted_entities_accepts_caller_resolved_set()
    {
        var command = MakeCommand(payloadJson: "{}");
        var owner = await StoreAndClaimAsync(command);

        var outcome = await AcceptAsync(command.CommandId, owner, new("bootstrap_full", MakeEntities(), 7));

        Assert.IsType<EtlJobAcceptanceOutcome.Applied>(outcome);
        var job = await JobAsync(command.CommandId);
        Assert.NotNull(job);
        Assert.Equal(2, JsonSerializer.Deserialize<List<EtlEntityDefinition>>(job.EntitiesJson, JsonOptions)!.Count);
    }

    [Fact]
    public async Task Expired_never_sent_command_is_rejected_and_still_expires_normally()
    {
        var command = MakeCommand(expiresAtUtc: DateTimeOffset.UtcNow.AddMinutes(-5));
        var owner = await StoreAndClaimAsync(command);

        var outcome = await AcceptAsync(command.CommandId, owner, new("bootstrap_full", MakeEntities(), 7));

        Assert.Equal(EtlJobAcceptanceRejection.Expired, Assert.IsType<EtlJobAcceptanceOutcome.NotApplied>(outcome).Reason);
        await AssertAcceptanceLeftNoTraceAsync(command.CommandId, owner);

        // The refused command is untouched, so the normal executor expiry path still owns it.
        var expiredResult = JsonSerializer.Serialize(new { commandId = command.CommandId, status = "expired", completedAtUtc = DateTimeOffset.UtcNow, error = new { code = "COMMAND_EXPIRED", message = "Command expired before execution.", retryable = false, details = new { } }, resultVersion = 1 }, JsonOptions);
        Assert.True(await _store.CompleteLocallyAsync(command.CommandId, owner, CommandStatus.Expired, expiredResult, null, null, null, CancellationToken.None));
        Assert.Equal("result_pending", (await CommandAsync(command.CommandId))!.Status);
    }

    private async Task AssertAcceptanceLeftNoTraceAsync(Guid commandId, string owner)
    {
        var inbox = await CommandAsync(commandId);
        Assert.NotNull(inbox);
        Assert.Equal("queued", inbox.Status);
        Assert.Null(inbox.ResultStatus);
        Assert.Null(inbox.ResultJson);
        Assert.Equal(owner, inbox.ClaimOwner);
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM etl_jobs WHERE command_id='" + commandId.ToString("D") + "'"));
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM etl_runs"));
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM results_outbox WHERE command_id='" + commandId.ToString("D") + "'"));
    }

    private async Task<string> StoreAndClaimAsync(CommandEnvelope command)
    {
        await _store.StoreCommandAsync(command, DateTimeOffset.UtcNow, CancellationToken.None);
        var owner = "job-owner-" + Guid.NewGuid().ToString("N");
        var claim = await _store.TryAcquireCommandExecutionClaimAsync(command.CommandId, owner, DateTimeOffset.UtcNow, DateTimeOffset.MinValue, CancellationToken.None);
        Assert.NotNull(claim);
        return owner;
    }

    private Task<EtlJobAcceptanceOutcome> AcceptAsync(Guid commandId, string owner, EtlJobAcceptanceRequest request) =>
        _store.AcceptEtlJobAndCompleteCommandAsync(commandId, owner, request, CancellationToken.None);

    private static CommandEnvelope MakeCommand(string commandType = "start_full_sync", string payloadJson = "{\"entities\":[\"clients\",\"orders\"]}", DateTimeOffset? expiresAtUtc = null)
    {
        using var document = JsonDocument.Parse(payloadJson);
        var payload = document.RootElement.Clone();
        return new(Guid.NewGuid(), commandType, 1, 100, null, null, DateTimeOffset.UtcNow, null, expiresAtUtc, null, PayloadHasher.Compute(payload), payload);
    }

    private static EtlEntityDefinition[] MakeEntities() =>
    [
        new("clients", "Catalog_Clients", "Ref_Key", "UpdatedAt", "DeletionMark", ["Ref_Key", "UpdatedAt", "DeletionMark"], "incremental", 500, 10),
        new("orders", "Document_Orders", "Ref_Key", "Date", "Posted", ["Ref_Key", "Date", "Posted"], "incremental", 500, 10)
    ];

    private sealed record CommandSnapshot(string Status, string? ResultStatus, string? ResultJson, string? ClaimOwner, string? ClaimAcquiredAt, long AttemptCount, long PostAttempts, long LookupAttempts, string? FirstSentAtUtc, string? FinishedAtUtc, string? NextAttemptAtUtc, long RowVersion);
    private sealed record JobSnapshot(string JobId, string RunId, string Mode, string EntitiesJson, long ConfigVersion, string Status, string PayloadHash, string AcceptanceResultJson, string CreatedAtUtc, string UpdatedAtUtc, long RowVersion);
    private sealed record RunSnapshot(string RunId, string Mode, string? RequestedEntitiesJson, string Status, string? StartedAtUtc, string? FinishedAtUtc, long? ConfigVersion, string? CreatedAtUtc, string? UpdatedAtUtc, long RowVersion);
    private sealed record OutboxSnapshot(string ResultId, string PayloadJson, string PayloadHash, string Status, long AttemptCount, string? NextAttemptAtUtc, string CreatedAtUtc, string? SentAtUtc, string? AcknowledgedAtUtc);

    private async Task<CommandSnapshot?> CommandAsync(Guid commandId)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT status,result_status,result_json,exec_claim_owner_id,exec_claim_acquired_at_utc,attempt_count,post_attempt_count,lookup_attempt_count,first_sent_at_utc,finished_at_utc,next_attempt_at_utc,row_version FROM commands_inbox WHERE command_id=$id;";
        command.Parameters.AddWithValue("$id", commandId.ToString("D"));
        await using var reader = await command.ExecuteReaderAsync(CancellationToken.None);
        if (!await reader.ReadAsync(CancellationToken.None)) return null;
        return new(reader.GetString(0), Nullable(reader, 1), Nullable(reader, 2), Nullable(reader, 3), Nullable(reader, 4),
            reader.GetInt64(5), reader.GetInt64(6), reader.GetInt64(7), Nullable(reader, 8), Nullable(reader, 9), Nullable(reader, 10), reader.GetInt64(11));
    }

    private async Task<JobSnapshot?> JobAsync(Guid commandId)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT job_id,run_id,mode,entities_json,configuration_version,status,command_payload_hash,acceptance_result_json,created_at_utc,updated_at_utc,row_version FROM etl_jobs WHERE command_id=$id;";
        command.Parameters.AddWithValue("$id", commandId.ToString("D"));
        await using var reader = await command.ExecuteReaderAsync(CancellationToken.None);
        if (!await reader.ReadAsync(CancellationToken.None)) return null;
        return new(reader.GetString(0), reader.GetString(1), reader.GetString(2), reader.GetString(3), reader.GetInt64(4),
            reader.GetString(5), reader.GetString(6), reader.GetString(7), reader.GetString(8), reader.GetString(9), reader.GetInt64(10));
    }

    private async Task<RunSnapshot?> RunAsync(string runId)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT run_id,mode,requested_entities_json,status,started_at_utc,finished_at_utc,configuration_version,created_at_utc,updated_at_utc,row_version FROM etl_runs WHERE run_id=$id;";
        command.Parameters.AddWithValue("$id", runId);
        await using var reader = await command.ExecuteReaderAsync(CancellationToken.None);
        if (!await reader.ReadAsync(CancellationToken.None)) return null;
        return new(reader.GetString(0), reader.GetString(1), Nullable(reader, 2), reader.GetString(3), Nullable(reader, 4), Nullable(reader, 5),
            reader.IsDBNull(6) ? null : reader.GetInt64(6), Nullable(reader, 7), Nullable(reader, 8), reader.GetInt64(9));
    }

    private async Task<OutboxSnapshot?> OutboxAsync(Guid commandId)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT result_id,payload_json,payload_hash,status,attempt_count,next_attempt_at_utc,created_at_utc,sent_at_utc,acknowledged_at_utc FROM results_outbox WHERE command_id=$id;";
        command.Parameters.AddWithValue("$id", commandId.ToString("D"));
        await using var reader = await command.ExecuteReaderAsync(CancellationToken.None);
        if (!await reader.ReadAsync(CancellationToken.None)) return null;
        return new(reader.GetString(0), reader.GetString(1), reader.GetString(2), reader.GetString(3), reader.GetInt64(4),
            Nullable(reader, 5), reader.GetString(6), Nullable(reader, 7), Nullable(reader, 8));
    }

    private static string? Nullable(SqliteDataReader reader, int ordinal) => reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);

    private async Task<long> ScalarAsync(string sql)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        return Convert.ToInt64(await command.ExecuteScalarAsync(CancellationToken.None), CultureInfo.InvariantCulture);
    }

    private async Task ExecuteSqlAsync(string sql, params (string Name, object Value)[] parameters)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        foreach (var (name, value) in parameters) command.Parameters.AddWithValue(name, value);
        await command.ExecuteNonQueryAsync(CancellationToken.None);
    }
}
