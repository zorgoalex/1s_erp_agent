using System.Text.Json;
using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Application.Commands;
using ErpOnecAgent.Contracts.OneC;
using ErpOnecAgent.Domain.Commands;
using ErpOnecAgent.Domain.Common;
using ErpOnecAgent.Infrastructure.Persistence.Sqlite;
using Xunit;

namespace ErpOnecAgent.IntegrationTests;

public sealed class CommandExecutionTests : IAsyncLifetime
{
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
    public async Task A07_command_expired_by_the_erp_clock_is_not_executed_even_if_the_local_clock_lags()
    {
        var onec = new FakeOnec();
        // The local clock is 5 minutes behind ERP: by ERP time this command expired 3 minutes ago.
        var service = new CommandExecutionService(_store, onec, static () => 12, expiryNow: static now => now.AddMinutes(5));
        var command = MakeCommand(expiresAtUtc: DateTimeOffset.UtcNow.AddMinutes(2));
        await _store.StoreCommandAsync(command, DateTimeOffset.UtcNow, CancellationToken.None);

        await service.ProcessAsync(await SingleReadyAsync(), NeverAdministrative, CancellationToken.None);

        Assert.Equal(0, onec.ExecuteCalls);
        var result = await SingleResultAsync();
        Assert.Equal("COMMAND_EXPIRED", result.GetProperty("error").GetProperty("code").GetString());
    }

    [Fact]
    public async Task Expired_never_sent_expires_without_lookup_or_post()
    {
        var onec = new FakeOnec();
        var service = new CommandExecutionService(_store, onec, static () => 12);
        var command = MakeCommand(expiresAtUtc: DateTimeOffset.UtcNow.AddMinutes(-5));
        await _store.StoreCommandAsync(command, DateTimeOffset.UtcNow, CancellationToken.None);

        await service.ProcessAsync(await SingleReadyAsync(), NeverAdministrative, CancellationToken.None);

        Assert.Equal(0, onec.StatusCalls);
        Assert.Equal(0, onec.ExecuteCalls);
        var result = await SingleResultAsync();
        Assert.Equal("expired", result.GetProperty("status").GetString());
        Assert.Equal("COMMAND_EXPIRED", result.GetProperty("error").GetProperty("code").GetString());
    }

    [Fact]
    public async Task Expired_unknown_resolves_via_status_lookup_success_without_post()
    {
        var onec = new FakeOnec { StatusKind = OnecExecutionKind.Succeeded };
        var service = new CommandExecutionService(_store, onec, static () => 12);
        var command = MakeCommand(expiresAtUtc: DateTimeOffset.UtcNow.AddMinutes(-5));
        await _store.StoreCommandAsync(command, DateTimeOffset.UtcNow, CancellationToken.None);
        var postAttempt = await _store.ClaimPostAttemptAsync(command.CommandId, CancellationToken.None);
        await _store.MarkUnknownResultAsync(command.CommandId, postAttempt!.AttemptId, "STATUS_PENDING", "1C result is still unknown.", null, CancellationToken.None);
        await MakeRetryDueAsync(command.CommandId); // scheduler simulation: the claim is due-guarded

        await service.ProcessAsync(await SingleReadyAsync(), NeverAdministrative, CancellationToken.None);

        Assert.Equal(1, onec.StatusCalls);
        Assert.Equal(0, onec.ExecuteCalls);
        var result = await SingleResultAsync();
        Assert.Equal("succeeded", result.GetProperty("status").GetString());
    }

    [Fact]
    public async Task Recovered_executing_after_send_looks_up_status_despite_expiry()
    {
        var onec = new FakeOnec { StatusKind = OnecExecutionKind.Succeeded };
        var service = new CommandExecutionService(_store, onec, static () => 12);
        var command = MakeCommand(expiresAtUtc: DateTimeOffset.UtcNow.AddMinutes(-5));
        await _store.StoreCommandAsync(command, DateTimeOffset.UtcNow, CancellationToken.None);
        await _store.MarkExecutingAsync(command.CommandId, CancellationToken.None);
        await _store.RecoverAsync(CancellationToken.None);
        var stored = await SingleReadyAsync();
        Assert.Equal(CommandStatus.UnknownResult, stored.Status);

        await service.ProcessAsync(stored, NeverAdministrative, CancellationToken.None);

        Assert.Equal(1, onec.StatusCalls);
        Assert.Equal(0, onec.ExecuteCalls);
        var result = await SingleResultAsync();
        Assert.Equal("succeeded", result.GetProperty("status").GetString());
    }

    [Fact]
    public async Task Pending_status_stays_unknown_and_is_never_reported_expired()
    {
        var onec = new FakeOnec { StatusKind = OnecExecutionKind.Processing };
        var service = new CommandExecutionService(_store, onec, static () => 12);
        var command = MakeCommand(expiresAtUtc: DateTimeOffset.UtcNow.AddMinutes(-5));
        await _store.StoreCommandAsync(command, DateTimeOffset.UtcNow, CancellationToken.None);
        await _store.MarkExecutingAsync(command.CommandId, CancellationToken.None);
        await _store.RecoverAsync(CancellationToken.None);

        for (var i = 0; i < 3; i++)
        {
            await MakeRetryDueAsync(command.CommandId); // explicit scheduler dispatch (due-guarded claim)
            await service.ProcessAsync(await SingleReadyAsync(), NeverAdministrative, CancellationToken.None);
        }

        Assert.Equal(3, onec.StatusCalls);
        Assert.Equal(0, onec.ExecuteCalls);
        Assert.Empty(await _store.GetPendingResultsAsync(10, DateTimeOffset.UtcNow, CancellationToken.None));
        var stillUnknown = await SingleReadyAsync();
        Assert.Equal(CommandStatus.UnknownResult, stillUnknown.Status);
    }

    [Fact]
    public async Task Retry_waiting_with_prior_send_resolves_status_before_retry_post()
    {
        var onec = new FakeOnec { StatusKind = OnecExecutionKind.Succeeded };
        var service = new CommandExecutionService(_store, onec, static () => 12);
        var command = MakeCommand(expiresAtUtc: DateTimeOffset.UtcNow.AddMinutes(-5));
        await _store.StoreCommandAsync(command, DateTimeOffset.UtcNow, CancellationToken.None);
        var postAttempt = await _store.ClaimPostAttemptAsync(command.CommandId, CancellationToken.None);
        Assert.NotNull(postAttempt);
        await _store.ScheduleRetryAsync(command.CommandId, "TECHNICAL", "transient", DateTimeOffset.UtcNow.AddSeconds(-1), CancellationToken.None);
        var stored = await SingleReadyAsync();
        Assert.Equal(CommandStatus.RetryWaiting, stored.Status);

        await service.ProcessAsync(stored, NeverAdministrative, CancellationToken.None);

        Assert.Equal(1, onec.StatusCalls);
        Assert.Equal(0, onec.ExecuteCalls);
        var result = await SingleResultAsync();
        Assert.Equal("succeeded", result.GetProperty("status").GetString());
    }

    [Fact]
    public async Task Unknown_result_with_business_error_preserves_business_semantics_despite_expiry()
    {
        var onec = new FakeOnec { StatusKind = OnecExecutionKind.BusinessError };
        var service = new CommandExecutionService(_store, onec, static () => 12);
        var command = MakeCommand(expiresAtUtc: DateTimeOffset.UtcNow.AddMinutes(-5));
        await _store.StoreCommandAsync(command, DateTimeOffset.UtcNow, CancellationToken.None);
        await _store.MarkExecutingAsync(command.CommandId, CancellationToken.None);
        await _store.RecoverAsync(CancellationToken.None);

        await service.ProcessAsync(await SingleReadyAsync(), NeverAdministrative, CancellationToken.None);

        Assert.Equal(1, onec.StatusCalls);
        Assert.Equal(0, onec.ExecuteCalls);
        var result = await SingleResultAsync();
        Assert.Equal("business_error", result.GetProperty("status").GetString());
    }

    [Fact]
    public async Task NotFound_after_send_retries_same_command_id_without_minting_new_id()
    {
        var onec = new FakeOnec { StatusKind = OnecExecutionKind.NotFound };
        var service = new CommandExecutionService(_store, onec, static () => 12);
        var command = MakeCommand(expiresAtUtc: DateTimeOffset.UtcNow.AddMinutes(30));
        await _store.StoreCommandAsync(command, DateTimeOffset.UtcNow, CancellationToken.None);
        var postAttempt = await _store.ClaimPostAttemptAsync(command.CommandId, CancellationToken.None);
        await _store.MarkUnknownResultAsync(command.CommandId, postAttempt!.AttemptId, "UNKNOWN_RESULT", "1C execution result is unknown.", null, CancellationToken.None);
        await MakeRetryDueAsync(command.CommandId); // explicit scheduler dispatch (due-guarded claim)

        await service.ProcessAsync(await SingleReadyAsync(), NeverAdministrative, CancellationToken.None);

        Assert.Equal(1, onec.StatusCalls);
        Assert.Equal(1, onec.ExecuteCalls);
        Assert.Equal(command.CommandId, onec.LastExecutedId);
    }

    [Fact]
    public async Task Expired_unknown_with_status_notfound_retries_same_command_id()
    {
        var onec = new FakeOnec { StatusKind = OnecExecutionKind.NotFound };
        var service = new CommandExecutionService(_store, onec, static () => 12);
        var command = MakeCommand(expiresAtUtc: DateTimeOffset.UtcNow.AddMinutes(-5));
        await _store.StoreCommandAsync(command, DateTimeOffset.UtcNow, CancellationToken.None);
        await _store.MarkExecutingAsync(command.CommandId, CancellationToken.None);
        await _store.RecoverAsync(CancellationToken.None);

        await service.ProcessAsync(await SingleReadyAsync(), NeverAdministrative, CancellationToken.None);

        Assert.Equal(1, onec.StatusCalls);
        Assert.Equal(1, onec.ExecuteCalls);
        Assert.Equal(command.CommandId, onec.LastExecutedId);
        var result = await SingleResultAsync();
        Assert.NotEqual("expired", result.GetProperty("status").GetString());
    }

    [Fact]
    public async Task Attempt_is_persisted_before_onec_execute_post()
    {
        long attemptCountInsidePost = -1;
        long attemptRowsInsidePost = -1;
        var onec = new FakeOnec
        {
            OnExecute = async (command, cancellationToken) =>
            {
                attemptCountInsidePost = await ScalarAsync("SELECT attempt_count FROM commands_inbox WHERE command_id=$id", command.CommandId, cancellationToken);
                attemptRowsInsidePost = await ScalarAsync("SELECT COUNT(*) FROM command_attempts WHERE command_id=$id", command.CommandId, cancellationToken);
                return SucceededResult(command.CommandId);
            }
        };
        var service = new CommandExecutionService(_store, onec, static () => 12);
        var command = MakeCommand(expiresAtUtc: null);
        await _store.StoreCommandAsync(command, DateTimeOffset.UtcNow, CancellationToken.None);

        await service.ProcessAsync(await SingleReadyAsync(), NeverAdministrative, CancellationToken.None);

        Assert.Equal(1, attemptCountInsidePost);
        Assert.Equal(1, attemptRowsInsidePost);
        Assert.Equal(1, onec.ExecuteCalls);
    }

    [Fact]
    public async Task Success_notification_fires_after_result_is_persisted()
    {
        var pendingWhenNotified = -1;
        string? notifiedRef = null;
        var hooks = new CommandExecutionHooks(
            OnExecutionSucceeded: async (commandId, externalRef) =>
            {
                notifiedRef = externalRef;
                pendingWhenNotified = (await _store.GetPendingResultsAsync(10, DateTimeOffset.UtcNow, CancellationToken.None)).Count;
            });
        var onec = new FakeOnec();
        var service = new CommandExecutionService(_store, onec, static () => 12, hooks);
        var command = MakeCommand(expiresAtUtc: null);
        await _store.StoreCommandAsync(command, DateTimeOffset.UtcNow, CancellationToken.None);

        await service.ProcessAsync(await SingleReadyAsync(), NeverAdministrative, CancellationToken.None);

        Assert.Equal("synthetic-ref", notifiedRef);
        Assert.Equal(1, pendingWhenNotified);
    }

    [Fact]
    public async Task Terminal_completion_rejection_does_not_fire_success_hook()
    {
        var notifications = 0;
        const string firstResult = "{\"status\":\"succeeded\",\"marker\":\"first\"}";
        var hooks = new CommandExecutionHooks(OnExecutionSucceeded: (_, _) => { notifications++; return Task.CompletedTask; });
        var onec = new FakeOnec
        {
            OnExecute = async (command, cancellationToken) =>
            {
                var owner = await StringAsync("SELECT exec_claim_owner_id FROM commands_inbox WHERE command_id=$id", command.CommandId, cancellationToken);
                Assert.NotNull(owner);
                await _store.ReleaseCommandExecutionClaimAsync(command.CommandId, owner!, cancellationToken);
                Assert.True(await _store.CompleteLocallyAsync(command.CommandId, CommandStatus.SucceededLocal, firstResult, "first-ref", "first-number", null, cancellationToken));
                return new(OnecExecutionKind.Succeeded, new(command.CommandId, "succeeded", JsonSerializer.SerializeToElement(new { @ref = "late-ref" }), null, [], 1), 200, null, null);
            }
        };
        var service = new CommandExecutionService(_store, onec, static () => 12, hooks);
        var command = MakeCommand(expiresAtUtc: null);
        await _store.StoreCommandAsync(command, DateTimeOffset.UtcNow, CancellationToken.None);

        await service.ProcessAsync(await SingleReadyAsync(), NeverAdministrative, CancellationToken.None);

        var pending = Assert.Single(await _store.GetPendingResultsAsync(10, DateTimeOffset.UtcNow, CancellationToken.None));
        Assert.Equal(0, notifications);
        Assert.Equal(firstResult, pending.PayloadJson);
        Assert.Equal(1, await ScalarAsync("SELECT COUNT(*) FROM command_attempts WHERE command_id=$id AND finished_at_utc IS NULL", command.CommandId, CancellationToken.None));
    }

    [Fact]
    public async Task Terminal_completion_rejection_does_not_fire_business_failed_hook()
    {
        var notifications = 0;
        const string firstResult = "{\"status\":\"business_error\",\"marker\":\"first\"}";
        var hooks = new CommandExecutionHooks(OnBusinessFailed: (_, _) => notifications++);
        var onec = new FakeOnec
        {
            OnExecute = async (command, cancellationToken) =>
            {
                var owner = await StringAsync("SELECT exec_claim_owner_id FROM commands_inbox WHERE command_id=$id", command.CommandId, cancellationToken);
                Assert.NotNull(owner);
                await _store.ReleaseCommandExecutionClaimAsync(command.CommandId, owner!, cancellationToken);
                Assert.True(await _store.CompleteLocallyAsync(command.CommandId, CommandStatus.BusinessFailedLocal, firstResult, null, null, null, cancellationToken));
                return new(OnecExecutionKind.BusinessError, new(command.CommandId, "business_failed", null, new("LATE_ERROR", "late", false, null), [], 1), 200, "LATE_ERROR", "late");
            }
        };
        var service = new CommandExecutionService(_store, onec, static () => 12, hooks);
        var command = MakeCommand(expiresAtUtc: null);
        await _store.StoreCommandAsync(command, DateTimeOffset.UtcNow, CancellationToken.None);

        await service.ProcessAsync(await SingleReadyAsync(), NeverAdministrative, CancellationToken.None);

        var pending = Assert.Single(await _store.GetPendingResultsAsync(10, DateTimeOffset.UtcNow, CancellationToken.None));
        Assert.Equal(0, notifications);
        Assert.Equal(firstResult, pending.PayloadJson);
        Assert.Equal(1, await ScalarAsync("SELECT COUNT(*) FROM command_attempts WHERE command_id=$id AND finished_at_utc IS NULL", command.CommandId, CancellationToken.None));
    }

    [Fact]
    public async Task Success_notification_on_status_resolve_fires_after_result_is_persisted()
    {
        var pendingWhenNotified = -1;
        var hooks = new CommandExecutionHooks(
            OnExecutionSucceeded: async (commandId, externalRef) =>
            {
                pendingWhenNotified = (await _store.GetPendingResultsAsync(10, DateTimeOffset.UtcNow, CancellationToken.None)).Count;
            });
        var onec = new FakeOnec { StatusKind = OnecExecutionKind.Succeeded };
        var service = new CommandExecutionService(_store, onec, static () => 12, hooks);
        var command = MakeCommand(expiresAtUtc: DateTimeOffset.UtcNow.AddMinutes(-5));
        await _store.StoreCommandAsync(command, DateTimeOffset.UtcNow, CancellationToken.None);
        await _store.MarkExecutingAsync(command.CommandId, CancellationToken.None);
        await _store.RecoverAsync(CancellationToken.None);

        await service.ProcessAsync(await SingleReadyAsync(), NeverAdministrative, CancellationToken.None);

        Assert.Equal(1, pendingWhenNotified);
        Assert.Equal(0, onec.ExecuteCalls);
    }

    [Fact]
    public async Task Dynamic_limits_replaced_by_separate_lookup_and_post_budgets()
    {
        // A09: the old shared dynamic attempt limit is intentionally replaced by separate
        // persisted lookup and post attempt budgets (this test supersedes
        // Max_operational_attempts_is_read_dynamically_per_processing).
        var lookupLimit = 100;
        var postLimit = 100;
        var onec = new FakeOnec { StatusKind = OnecExecutionKind.Processing };
        var service = new CommandExecutionService(_store, onec, static () => 12, null,
            () => new CommandExecutionOptions { MaxLookupAttempts = lookupLimit, MaxPostAttempts = postLimit });
        var command = MakeCommand(expiresAtUtc: null);
        await _store.StoreCommandAsync(command, DateTimeOffset.UtcNow, CancellationToken.None);
        await _store.MarkExecutingAsync(command.CommandId, CancellationToken.None);
        await _store.RecoverAsync(CancellationToken.None);

        await service.ProcessAsync(await SingleReadyAsync(), NeverAdministrative, CancellationToken.None);
        Assert.Equal(CommandStatus.UnknownResult, (await SingleReadyAsync()).Status);

        lookupLimit = 1;
        await MakeRetryDueAsync(command.CommandId); // explicit scheduler dispatch (due-guarded claim)
        await service.ProcessAsync(await SingleReadyAsync(), NeverAdministrative, CancellationToken.None);
        var result = await SingleResultAsync();
        Assert.Equal("dead_letter", result.GetProperty("status").GetString());
        Assert.Equal("UNKNOWN_RESULT_LIMIT", result.GetProperty("error").GetProperty("code").GetString());
        Assert.True(result.GetProperty("error").GetProperty("details").GetProperty("outcomeUnknown").GetBoolean());
    }

    // ---- A09 follow-up items 1-3: atomic durable lookup claim, attempt lifecycle across the
    //      network call, and active-state (terminal) guard before any lookup ----

    [Fact]
    public async Task Lookup_claim_is_durable_attempt_open_counter_and_schedule_before_network_and_completes_after()
    {
        // Item 1 + 2: one atomic durable claim commits the counter, the backoff schedule and an
        // UNFINISHED attempt row BEFORE GetStatusAsync; the attempt closes with the audited outcome
        // only AFTER the call. Honest scope note: the "crash" here is proven by the durable pre-network
        // state captured inside the fake (a real process-kill crash test is not performed).
        long lookupCounterDuringCall = -1, openLookupAttemptsDuringCall = -1, closedLookupAttemptsDuringCall = -1;
        DateTimeOffset? nextAttemptDuringCall = null;
        var onec = new FakeOnec
        {
            StatusKind = OnecExecutionKind.Succeeded,
            OnStatus = async (commandId, cancellationToken) =>
            {
                lookupCounterDuringCall = await ScalarAsync("SELECT lookup_attempt_count FROM commands_inbox WHERE command_id=$id", commandId, cancellationToken);
                nextAttemptDuringCall = await DateScalarAsync("SELECT next_attempt_at_utc FROM commands_inbox WHERE command_id=$id", commandId, cancellationToken);
                openLookupAttemptsDuringCall = await ScalarAsync("SELECT COUNT(*) FROM command_attempts WHERE command_id=$id AND attempt_kind='lookup' AND finished_at_utc IS NULL", commandId, cancellationToken);
                closedLookupAttemptsDuringCall = await ScalarAsync("SELECT COUNT(*) FROM command_attempts WHERE command_id=$id AND attempt_kind='lookup' AND finished_at_utc IS NOT NULL", commandId, cancellationToken);
                return new(OnecExecutionKind.Succeeded, new(commandId, "succeeded", JsonSerializer.SerializeToElement(new { @ref = "synthetic-ref" }), null, [], 1), 200, null, null);
            }
        };
        var service = new CommandExecutionService(_store, onec, static () => 12, null,
            static () => new CommandExecutionOptions { MaxLookupAttempts = 3, MaxPostAttempts = 12, RetryBaseDelaySeconds = 60, RetryMaxDelaySeconds = 600 });
        var command = MakeCommand(expiresAtUtc: null);
        await _store.StoreCommandAsync(command, DateTimeOffset.UtcNow, CancellationToken.None);
        await _store.MarkExecutingAsync(command.CommandId, CancellationToken.None);
        await _store.RecoverAsync(CancellationToken.None);

        await service.ProcessAsync(await ReadyForAsync(command.CommandId), NeverAdministrative, CancellationToken.None);

        // BEFORE the network call the claim is already durable: counter consumed, backoff scheduled,
        // attempt left UNFINISHED so the audit spans the network call (a crash here cannot lose the
        // backoff and cannot silently finish the attempt early).
        Assert.Equal(1, lookupCounterDuringCall);
        Assert.NotNull(nextAttemptDuringCall);
        Assert.True(nextAttemptDuringCall > DateTimeOffset.UtcNow.AddSeconds(30));
        Assert.Equal(1, openLookupAttemptsDuringCall);
        Assert.Equal(0, closedLookupAttemptsDuringCall);

        // AFTER the call: the successful outcome is recorded on the same attempt (completed audit).
        Assert.Equal(1, await ScalarAsync("SELECT COUNT(*) FROM command_attempts WHERE command_id=$id AND attempt_kind='lookup' AND finished_at_utc IS NOT NULL AND outcome='succeeded_local'", command.CommandId, CancellationToken.None));
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM command_attempts WHERE command_id=$id AND attempt_kind='lookup' AND finished_at_utc IS NULL", command.CommandId, CancellationToken.None));
        var result = await ResultForAsync(command.CommandId);
        Assert.Equal("succeeded", result.GetProperty("status").GetString());
    }

    [Fact]
    public async Task Lookup_attempt_spans_pending_lookup_and_closes_as_unknown_with_committed_backoff()
    {
        // Item 1 + 2: for a pending/error status the same attempt row stays open across the network
        // call and only closes (outcome 'unknown_result') after it, while the counter and backoff were
        // already committed before it.
        long lookupCounterDuringCall = -1, openLookupAttemptsDuringCall = -1, closedLookupAttemptsDuringCall = -1;
        DateTimeOffset? nextAttemptDuringCall = null;
        var onec = new FakeOnec
        {
            StatusKind = OnecExecutionKind.Processing,
            OnStatus = async (commandId, cancellationToken) =>
            {
                lookupCounterDuringCall = await ScalarAsync("SELECT lookup_attempt_count FROM commands_inbox WHERE command_id=$id", commandId, cancellationToken);
                nextAttemptDuringCall = await DateScalarAsync("SELECT next_attempt_at_utc FROM commands_inbox WHERE command_id=$id", commandId, cancellationToken);
                openLookupAttemptsDuringCall = await ScalarAsync("SELECT COUNT(*) FROM command_attempts WHERE command_id=$id AND attempt_kind='lookup' AND finished_at_utc IS NULL", commandId, cancellationToken);
                closedLookupAttemptsDuringCall = await ScalarAsync("SELECT COUNT(*) FROM command_attempts WHERE command_id=$id AND attempt_kind='lookup' AND finished_at_utc IS NOT NULL", commandId, cancellationToken);
                return new(OnecExecutionKind.Processing, null, 202, null, null);
            }
        };
        var service = new CommandExecutionService(_store, onec, static () => 12, null,
            static () => new CommandExecutionOptions { MaxLookupAttempts = 3, MaxPostAttempts = 12, RetryBaseDelaySeconds = 60, RetryMaxDelaySeconds = 600 });
        var command = MakeCommand(expiresAtUtc: null);
        await _store.StoreCommandAsync(command, DateTimeOffset.UtcNow, CancellationToken.None);
        await _store.MarkExecutingAsync(command.CommandId, CancellationToken.None);
        await _store.RecoverAsync(CancellationToken.None);

        await service.ProcessAsync(await ReadyForAsync(command.CommandId), NeverAdministrative, CancellationToken.None);

        // Durable BEFORE the network call (atomic claim): counter + backoff + open attempt.
        Assert.Equal(1, lookupCounterDuringCall);
        Assert.NotNull(nextAttemptDuringCall);
        Assert.True(nextAttemptDuringCall > DateTimeOffset.UtcNow.AddSeconds(30));
        Assert.Equal(1, openLookupAttemptsDuringCall);
        Assert.Equal(0, closedLookupAttemptsDuringCall);

        // AFTER the call: closed as still-unknown; no premature result/outbox row.
        Assert.Equal(1, await ScalarAsync("SELECT COUNT(*) FROM command_attempts WHERE command_id=$id AND attempt_kind='lookup' AND finished_at_utc IS NOT NULL AND outcome='unknown_result'", command.CommandId, CancellationToken.None));
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM command_attempts WHERE command_id=$id AND attempt_kind='lookup' AND finished_at_utc IS NULL", command.CommandId, CancellationToken.None));
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM results_outbox WHERE command_id=$id", command.CommandId, CancellationToken.None));
        Assert.Equal(CommandStatus.UnknownResult, await StatusAsync(command.CommandId));
    }

    [Fact]
    public async Task Terminal_result_pending_row_is_not_claimable_and_service_skips_network()
    {
        // Item 3: a terminal result_pending row must not claim a lookup (no counter/attempt mutations)
        // and a failed claim must prevent any network call.
        var command = MakeCommand(expiresAtUtc: null);
        await _store.StoreCommandAsync(command, DateTimeOffset.UtcNow, CancellationToken.None);

        // Drive the row terminal: fresh POST success => result_pending (terminal for execution).
        var post = new FakeOnec();
        var postService = new CommandExecutionService(_store, post, static () => 12);
        await postService.ProcessAsync(await ReadyForAsync(command.CommandId), NeverAdministrative, CancellationToken.None);
        Assert.Equal(CommandStatus.ResultPending, await StatusAsync(command.CommandId));

        // The store-level lookup claim itself must reject the terminal row with zero mutations.
        Assert.Null(await _store.ClaimLookupAttemptAsync(command.CommandId, "STATUS_PENDING", "1C result is still unknown.", DateTimeOffset.UtcNow.AddMinutes(1), CancellationToken.None));
        Assert.Equal(0, await ScalarAsync("SELECT lookup_attempt_count FROM commands_inbox WHERE command_id=$id", command.CommandId, CancellationToken.None));
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM command_attempts WHERE command_id=$id AND attempt_kind='lookup'", command.CommandId, CancellationToken.None));

        // Even if a caller hands a terminal row straight to the executor (bypassing the ready query),
        // the failed claim must prevent any lookup network call and any mutation.
        var terminalStored = await StoredForAsync(command.CommandId);
        Assert.Equal(CommandStatus.ResultPending, terminalStored.Status);
        var lookup = new FakeOnec { StatusKind = OnecExecutionKind.Succeeded };
        var lookupService = new CommandExecutionService(_store, lookup, static () => 12);
        await lookupService.ProcessAsync(terminalStored, NeverAdministrative, CancellationToken.None);

        Assert.Equal(0, lookup.StatusCalls);
        Assert.Equal(0, lookup.ExecuteCalls);
        Assert.Equal(CommandStatus.ResultPending, await StatusAsync(command.CommandId));
        Assert.Equal(0, await ScalarAsync("SELECT lookup_attempt_count FROM commands_inbox WHERE command_id=$id", command.CommandId, CancellationToken.None));
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM command_attempts WHERE command_id=$id AND attempt_kind='lookup'", command.CommandId, CancellationToken.None));
    }

    [Fact]
    public async Task Completed_row_is_never_reopened_by_lookup_claim()
    {
        // A01/A02 preserved: a terminal 'completed' row must never claim a lookup or be re-opened.
        var command = MakeCommand(expiresAtUtc: null);
        await _store.StoreCommandAsync(command, DateTimeOffset.UtcNow, CancellationToken.None);
        var attempt = await _store.ClaimPostAttemptAsync(command.CommandId, CancellationToken.None);
        Assert.NotNull(attempt);
        Assert.True(await _store.CompleteLocallyAsync(command.CommandId, CommandStatus.SucceededLocal, "{\"status\":\"succeeded\"}", null, null, attempt.AttemptId, CancellationToken.None));
        Assert.True(await _store.AcknowledgeResultAsync(command.CommandId, DateTimeOffset.UtcNow, CancellationToken.None));
        Assert.Equal(CommandStatus.Completed, await StatusAsync(command.CommandId));

        Assert.Null(await _store.ClaimLookupAttemptAsync(command.CommandId, "STATUS_PENDING", "1C result is still unknown.", DateTimeOffset.UtcNow.AddMinutes(1), CancellationToken.None));
        Assert.Equal(CommandStatus.Completed, await StatusAsync(command.CommandId));
        Assert.Equal(0, await ScalarAsync("SELECT lookup_attempt_count FROM commands_inbox WHERE command_id=$id", command.CommandId, CancellationToken.None));
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM command_attempts WHERE command_id=$id AND attempt_kind='lookup'", command.CommandId, CancellationToken.None));
    }
    // ---- A09 attempt-audit review: exact attempt identity, monotonic per-command attempt_no ----
    // (1) NotFound lookup must not be marked dead_letter while the command is still being worked;
    // (2) an ambiguous POST attempt must not be left dangling-unfinished;
    // (3) a terminal outcome closes only the CURRENT attempt, never borrowing onto historical rows;
    // (4) CompleteAttempt closes only the targeted attempt, never overwriting historical rows.

    [Fact]
    public async Task NotFound_lookup_is_still_unknown_and_not_dead_letter_then_reposts_same_id()
    {
        // NotFound is non-terminal: the lookup attempt closes as 'unknown_result' with NOT_FOUND (never
        // dead_letter) and the service immediately re-POSTs the SAME command id.
        var onec = new FakeOnec { StatusKind = OnecExecutionKind.NotFound, ExecuteKind = OnecExecutionKind.Succeeded };
        var service = new CommandExecutionService(_store, onec, static () => 12, null,
            static () => new CommandExecutionOptions { MaxLookupAttempts = 24, MaxPostAttempts = 12 });
        var command = MakeCommand(expiresAtUtc: null);
        await _store.StoreCommandAsync(command, DateTimeOffset.UtcNow, CancellationToken.None);
        var initial = await _store.ClaimPostAttemptAsync(command.CommandId, CancellationToken.None);
        await _store.MarkUnknownResultAsync(command.CommandId, initial!.AttemptId, "UNKNOWN_RESULT", "1C execution result is unknown.", null, CancellationToken.None);
        await MakeRetryDueAsync(command.CommandId); // explicit scheduler dispatch (due-guarded claim)

        await service.ProcessAsync(await ReadyForAsync(command.CommandId), NeverAdministrative, CancellationToken.None);

        Assert.Equal(1, onec.StatusCalls);
        Assert.Equal(1, onec.ExecuteCalls);
        Assert.Equal(command.CommandId, onec.LastExecutedId);
        Assert.Equal(1, await ScalarAsync("SELECT COUNT(*) FROM command_attempts WHERE command_id=$id AND attempt_kind='lookup' AND outcome='unknown_result' AND error_code='NOT_FOUND'", command.CommandId, CancellationToken.None));
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM command_attempts WHERE command_id=$id AND outcome='dead_letter'", command.CommandId, CancellationToken.None));
    }

    [Fact]
    public async Task Retry_after_ambiguous_post_closes_that_attempt_and_no_attempt_stays_unfinished()
    {
        // An ambiguous POST (Processing) result is still-unknown: the POST attempt is closed as
        // 'unknown_result' (not left dangling-unfinished) and the row is scheduled for resolve-before-
        // retry. Zero attempts remain unfinished after the POST returns.
        var onec = new FakeOnec { StatusKind = OnecExecutionKind.Processing, ExecuteKind = OnecExecutionKind.Processing };
        var service = new CommandExecutionService(_store, onec, static () => 12, null,
            static () => new CommandExecutionOptions { MaxLookupAttempts = 24, MaxPostAttempts = 12 });
        var command = MakeCommand(expiresAtUtc: null);
        await _store.StoreCommandAsync(command, DateTimeOffset.UtcNow, CancellationToken.None);

        await service.ProcessAsync(await ReadyForAsync(command.CommandId), NeverAdministrative, CancellationToken.None);

        Assert.Equal(0, onec.StatusCalls);
        Assert.Equal(CommandStatus.UnknownResult, await StatusAsync(command.CommandId));
        Assert.Equal(1, await ScalarAsync("SELECT COUNT(*) FROM command_attempts WHERE command_id=$id AND attempt_kind='post' AND outcome='unknown_result'", command.CommandId, CancellationToken.None));
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM command_attempts WHERE command_id=$id AND finished_at_utc IS NULL", command.CommandId, CancellationToken.None));

        await MakeRetryDueAsync(command.CommandId); // explicit scheduler dispatch (due-guarded claim)
        await service.ProcessAsync(await ReadyForAsync(command.CommandId), NeverAdministrative, CancellationToken.None);
        Assert.Equal(1, onec.StatusCalls);
        Assert.Equal(1, await ScalarAsync("SELECT COUNT(*) FROM command_attempts WHERE command_id=$id AND attempt_kind='lookup' AND outcome='unknown_result'", command.CommandId, CancellationToken.None));
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM command_attempts WHERE command_id=$id AND finished_at_utc IS NULL", command.CommandId, CancellationToken.None));
    }


    [Fact]
    public async Task Terminal_outcome_closes_only_current_attempt_and_preserves_prior_unfinished()
    {
        // A historical unfinished (crashed) attempt must stay distinctly unresolved — a later terminal
        // outcome must NOT borrow its result onto that older row. On the resolve path the current
        // lookup attempt is completed BEFORE the terminal command result is saved, so the terminal
        // close must touch only the remaining prior-open rows, never rewriting the resolved one.
        var onec = new FakeOnec { StatusKind = OnecExecutionKind.Succeeded, ExecuteKind = OnecExecutionKind.Succeeded };
        var service = new CommandExecutionService(_store, onec, static () => 12);
        var command = MakeCommand(expiresAtUtc: null);
        await _store.StoreCommandAsync(command, DateTimeOffset.UtcNow, CancellationToken.None);
        // Prior attempt #1: POST started (committed) but never finished (simulated crash before outcome).
        var crashed = await _store.ClaimPostAttemptAsync(command.CommandId, CancellationToken.None);
        Assert.NotNull(crashed);
        await _store.RecoverAsync(CancellationToken.None); // real restart makes the row resolvable again

        await service.ProcessAsync(await ReadyForAsync(command.CommandId), NeverAdministrative, CancellationToken.None);

        // The current lookup attempt (#2) is closed as succeeded_local (its own audited outcome).
        Assert.Equal(1, await ScalarAsync("SELECT COUNT(*) FROM command_attempts WHERE command_id=$id AND attempt_kind='lookup' AND attempt_no=2 AND outcome='succeeded_local' AND finished_at_utc IS NOT NULL", command.CommandId, CancellationToken.None));
        // The PRIOR crashed attempt (#1) is preserved as unfinished (distinctly unresolved) and never
        // borrows the later success outcome.
        Assert.Equal(1, await ScalarAsync("SELECT COUNT(*) FROM command_attempts WHERE command_id=$id AND attempt_kind='post' AND attempt_no=1 AND finished_at_utc IS NULL", command.CommandId, CancellationToken.None));
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM command_attempts WHERE command_id=$id AND attempt_no=1 AND outcome IS NOT NULL", command.CommandId, CancellationToken.None));
        Assert.Equal("succeeded", (await ResultForAsync(command.CommandId)).GetProperty("status").GetString());
    }

    [Fact]
    public async Task CompleteAttempt_closes_only_target_and_never_overwrites_historical_unknown_rows()
    {
        // Exact attempt identity: completing one attempt must never overwrite a prior historical
        // unknown/crashed attempt row (its outcome/error/http stay exactly as first recorded).
        await _store.StoreCommandAsync(MakeCommand(expiresAtUtc: null), DateTimeOffset.UtcNow, CancellationToken.None);
        var commandId = (await SingleReadyAsync()).Envelope.CommandId;
        var first = await _store.ClaimPostAttemptAsync(commandId, CancellationToken.None);
        await _store.CompleteAttemptAsync(first!.AttemptId, CommandStatus.UnknownResult, "UNKNOWN_RESULT", "ambiguous", 502, CancellationToken.None);
        var second = await _store.ClaimLookupAttemptAsync(commandId, "STATUS_PENDING", "pending", DateTimeOffset.UtcNow.AddMinutes(1), CancellationToken.None);

        await _store.CompleteAttemptAsync(second!.AttemptId, CommandStatus.SucceededLocal, null, null, 200, CancellationToken.None);

        // Historical unknown row is untouched (still unknown_result / UNKNOWN_RESULT / 502).
        Assert.Equal(1, await ScalarAsync("SELECT COUNT(*) FROM command_attempts WHERE command_id=$id AND attempt_kind='post' AND outcome='unknown_result' AND error_code='UNKNOWN_RESULT' AND http_status=502", commandId, CancellationToken.None));
        // Only the targeted lookup attempt is closed as succeeded_local / 200.
        Assert.Equal(1, await ScalarAsync("SELECT COUNT(*) FROM command_attempts WHERE command_id=$id AND attempt_kind='lookup' AND outcome='succeeded_local' AND http_status=200", commandId, CancellationToken.None));
        // Monotonic per-command attempt_no with exact identity (no 1_000_000 offset), 2 distinct rows.
        Assert.Equal(2, await ScalarAsync("SELECT COUNT(*) FROM command_attempts WHERE command_id=$id AND attempt_no IN (1,2)", commandId, CancellationToken.None));
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM command_attempts WHERE command_id=$id AND attempt_no >= 1000000", commandId, CancellationToken.None));
        Assert.Equal(2, await ScalarAsync("SELECT COUNT(DISTINCT attempt_id) FROM command_attempts WHERE command_id=$id", commandId, CancellationToken.None));
    }

    // ---- A09 retry budgets: separate lookup / POST attempt budgets + persisted backoff ----

    [Fact]
    public async Task Pending_status_hits_lookup_attempt_budget_with_explicit_unknown_result_dead_letter()
    {
        var onec = new FakeOnec { StatusKind = OnecExecutionKind.Processing };
        var service = new CommandExecutionService(_store, onec, static () => 12, null,
            static () => new CommandExecutionOptions { MaxLookupAttempts = 3, MaxPostAttempts = 12 });
        var command = MakeCommand(expiresAtUtc: null);
        await _store.StoreCommandAsync(command, DateTimeOffset.UtcNow, CancellationToken.None);
        await _store.MarkExecutingAsync(command.CommandId, CancellationToken.None);
        await _store.RecoverAsync(CancellationToken.None);

        for (var i = 0; i < 3; i++)
        {
            await MakeRetryDueAsync(command.CommandId); // explicit scheduler dispatch (due-guarded claim)
            await service.ProcessAsync(await ReadyForAsync(command.CommandId), NeverAdministrative, CancellationToken.None);
        }
        // DeadLetter is terminal and, once its result is enqueued, the row is 'result_pending'
        // (terminal for execution) — excluded from the ready query by design. Assert via the DB/result.
        Assert.Equal(CommandStatus.ResultPending, await StatusAsync(command.CommandId));
        Assert.Empty(await ReadyAsync());

        var result = await ResultForAsync(command.CommandId);
        Assert.Equal("dead_letter", result.GetProperty("status").GetString());
        var error = result.GetProperty("error");
        Assert.Equal("UNKNOWN_RESULT_LIMIT", error.GetProperty("code").GetString());
        Assert.Contains("manual investigation", error.GetProperty("message").GetString(), StringComparison.OrdinalIgnoreCase);
        Assert.True(error.GetProperty("details").GetProperty("outcomeUnknown").GetBoolean());
        Assert.Equal(3, onec.StatusCalls);
        Assert.Equal(0, onec.ExecuteCalls);
    }

    [Fact]
    public async Task NotFound_after_send_reaches_post_attempt_budget_with_explicit_unknown_result_dead_letter()
    {
        // Lookup reports NotFound (result unknown); re-POST also yields a non-terminal outcome so the
        // command stays re-resolvable and the POST budget (not lookup) is what bounds it.
        var onec = new FakeOnec { StatusKind = OnecExecutionKind.NotFound, ExecuteKind = OnecExecutionKind.Processing };
        var service = new CommandExecutionService(_store, onec, static () => 12, null,
            static () => new CommandExecutionOptions { MaxLookupAttempts = 24, MaxPostAttempts = 2 });
        var command = MakeCommand(expiresAtUtc: DateTimeOffset.UtcNow.AddMinutes(30));
        await _store.StoreCommandAsync(command, DateTimeOffset.UtcNow, CancellationToken.None);
        var postAttempt = await _store.ClaimPostAttemptAsync(command.CommandId, CancellationToken.None);
        await _store.MarkUnknownResultAsync(command.CommandId, postAttempt!.AttemptId, "UNKNOWN_RESULT", "1C execution result is unknown.", null, CancellationToken.None);

        // Each pass: NotFound -> re-POST same id (POST budget bounded). A POST leaves the row
        // 'executing'; RecoverAsync restores it to 'unknown_result' so the next pass re-resolves.
        for (var i = 0; i < 2; i++)
        {
            await MakeRetryDueAsync(command.CommandId); // explicit scheduler dispatch (due-guarded claim)
            await service.ProcessAsync(await ReadyForAsync(command.CommandId), NeverAdministrative, CancellationToken.None);
            await _store.RecoverAsync(CancellationToken.None);
        }

        var result = await ResultForAsync(command.CommandId);
        Assert.Equal("dead_letter", result.GetProperty("status").GetString());
        var error = result.GetProperty("error");
        Assert.Equal("UNKNOWN_RESULT_LIMIT", error.GetProperty("code").GetString());
        Assert.Contains("manual investigation", error.GetProperty("message").GetString(), StringComparison.OrdinalIgnoreCase);
        Assert.True(error.GetProperty("details").GetProperty("outcomeUnknown").GetBoolean());
        Assert.Equal(command.CommandId, onec.LastExecutedId);
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM commands_inbox WHERE command_id <> $id", command.CommandId, CancellationToken.None));
        Assert.Equal(2, await ScalarAsync("SELECT post_attempt_count FROM commands_inbox WHERE command_id=$id", command.CommandId, CancellationToken.None));
        Assert.Equal(2, await ScalarAsync("SELECT lookup_attempt_count FROM commands_inbox WHERE command_id=$id", command.CommandId, CancellationToken.None));
    }

    [Fact]
    public async Task Lookup_technical_errors_are_bounded_by_lookup_budget()
    {
        var onec = new FakeOnec { StatusKind = OnecExecutionKind.TechnicalError };
        var service = new CommandExecutionService(_store, onec, static () => 12, null,
            static () => new CommandExecutionOptions { MaxLookupAttempts = 2, MaxPostAttempts = 12 });
        var command = MakeCommand(expiresAtUtc: null);
        await _store.StoreCommandAsync(command, DateTimeOffset.UtcNow, CancellationToken.None);
        await _store.MarkExecutingAsync(command.CommandId, CancellationToken.None);
        await _store.RecoverAsync(CancellationToken.None);

        for (var i = 0; i < 2; i++)
        {
            await MakeRetryDueAsync(command.CommandId); // explicit scheduler dispatch (due-guarded claim)
            await service.ProcessAsync(await ReadyForAsync(command.CommandId), NeverAdministrative, CancellationToken.None);
        }

        var result = await ResultForAsync(command.CommandId);
        Assert.Equal("dead_letter", result.GetProperty("status").GetString());
        var error = result.GetProperty("error");
        Assert.Equal("UNKNOWN_RESULT_LIMIT", error.GetProperty("code").GetString());
        Assert.Contains("manual investigation", error.GetProperty("message").GetString(), StringComparison.OrdinalIgnoreCase);
        Assert.True(error.GetProperty("details").GetProperty("outcomeUnknown").GetBoolean());
        Assert.Equal(2, onec.StatusCalls);
        Assert.Equal(0, onec.ExecuteCalls);
        Assert.Equal(2, await ScalarAsync("SELECT lookup_attempt_count FROM commands_inbox WHERE command_id=$id", command.CommandId, CancellationToken.None));
    }

    [Fact]
    public async Task Success_and_business_results_remain_unchanged()
    {
        var onec = new FakeOnec();
        var service = new CommandExecutionService(_store, onec, static () => 12, null,
            static () => new CommandExecutionOptions { MaxLookupAttempts = 3, MaxPostAttempts = 2 });
        var ok = MakeCommand(expiresAtUtc: null, orderingKey: "order:ok");
        await _store.StoreCommandAsync(ok, DateTimeOffset.UtcNow, CancellationToken.None);
        await service.ProcessAsync(await ReadyForAsync(ok.CommandId), NeverAdministrative, CancellationToken.None);
        var result = await ResultForAsync(ok.CommandId);
        Assert.Equal("succeeded", result.GetProperty("status").GetString());

        await _store.AcknowledgeResultAsync(ok.CommandId, DateTimeOffset.UtcNow, CancellationToken.None);

        var failing = new FakeOnec { ExecuteKind = OnecExecutionKind.BusinessError };
        var service2 = new CommandExecutionService(_store, failing, static () => 12, null,
            static () => new CommandExecutionOptions { MaxLookupAttempts = 3, MaxPostAttempts = 2 });
        var bad = MakeCommand(expiresAtUtc: null, orderingKey: "order:bad");
        await _store.StoreCommandAsync(bad, DateTimeOffset.UtcNow, CancellationToken.None);
        await service2.ProcessAsync(await ReadyForAsync(bad.CommandId), NeverAdministrative, CancellationToken.None);
        var badResult = await ResultForAsync(bad.CommandId);
        Assert.Equal("business_error", badResult.GetProperty("status").GetString());
    }

    [Fact]
    public async Task Restart_preserves_budget_counters_and_next_attempt_time()
    {
        var onec = new FakeOnec { StatusKind = OnecExecutionKind.Processing };
        var service = new CommandExecutionService(_store, onec, static () => 12, null,
            static () => new CommandExecutionOptions { MaxLookupAttempts = 3, MaxPostAttempts = 12, RetryBaseDelaySeconds = 120, RetryMaxDelaySeconds = 3600 });
        var command = MakeCommand(expiresAtUtc: null);
        await _store.StoreCommandAsync(command, DateTimeOffset.UtcNow, CancellationToken.None);
        await _store.MarkExecutingAsync(command.CommandId, CancellationToken.None);
        await _store.RecoverAsync(CancellationToken.None);

        await service.ProcessAsync(await SingleReadyAsync(), NeverAdministrative, CancellationToken.None);
        Assert.Equal(1, await ScalarAsync("SELECT lookup_attempt_count FROM commands_inbox WHERE command_id=$id", command.CommandId, CancellationToken.None));
        Assert.Empty(await _store.GetReadyCommandsAsync(10, DateTimeOffset.UtcNow.AddSeconds(1), CancellationToken.None));
        var nextBefore = await DateScalarAsync("SELECT next_attempt_at_utc FROM commands_inbox WHERE command_id=$id", command.CommandId, CancellationToken.None);
        Assert.NotNull(nextBefore);
        Assert.True(nextBefore > DateTimeOffset.UtcNow.AddSeconds(30));

        // restart: budgets and NextAttemptAt must survive
        await SqliteTestDatabase.ClearPoolAsync(_factory);
        _store = new(_factory, new SqliteMigrator(_factory));
        await _store.InitializeAsync(CancellationToken.None);
        await _store.RecoverAsync(CancellationToken.None);

        Assert.Equal(1, await ScalarAsync("SELECT lookup_attempt_count FROM commands_inbox WHERE command_id=$id", command.CommandId, CancellationToken.None));
        var nextAfter = await DateScalarAsync("SELECT next_attempt_at_utc FROM commands_inbox WHERE command_id=$id", command.CommandId, CancellationToken.None);
        Assert.Equal(nextBefore, nextAfter);

        // ready again only after NextAttemptAt elapses and never resets the budget
        var ready = await _store.GetReadyCommandsAsync(10, DateTimeOffset.UtcNow.AddHours(1), CancellationToken.None);
        var stored = Assert.Single(ready);
        await MakeRetryDueAsync(command.CommandId); // explicit dispatch at the simulated due time (due-guarded claim)
        await service.ProcessAsync(stored, NeverAdministrative, CancellationToken.None);
        Assert.Equal(2, await ScalarAsync("SELECT lookup_attempt_count FROM commands_inbox WHERE command_id=$id", command.CommandId, CancellationToken.None));
    }

    [Fact]
    public async Task Backoff_is_persisted_before_lookup_network_call_and_respected_by_scheduler()
    {
        DateTimeOffset? lookupNextDuringCall = null;
        var onec = new FakeOnec
        {
            StatusKind = OnecExecutionKind.Processing,
            OnStatus = async (commandId, cancellationToken) =>
            {
                lookupNextDuringCall = await DateScalarAsync("SELECT next_attempt_at_utc FROM commands_inbox WHERE command_id=$id", commandId, cancellationToken);
                return new(OnecExecutionKind.Processing, null, 202, null, null);
            }
        };
        var service = new CommandExecutionService(_store, onec, static () => 12, null,
            static () => new CommandExecutionOptions { MaxLookupAttempts = 3, MaxPostAttempts = 12, RetryBaseDelaySeconds = 60, RetryMaxDelaySeconds = 600 });
        var command = MakeCommand(expiresAtUtc: null);
        await _store.StoreCommandAsync(command, DateTimeOffset.UtcNow, CancellationToken.None);
        await _store.MarkExecutingAsync(command.CommandId, CancellationToken.None);
        await _store.RecoverAsync(CancellationToken.None);

        await service.ProcessAsync(await ReadyForAsync(command.CommandId), NeverAdministrative, CancellationToken.None);

        Assert.NotNull(lookupNextDuringCall);
        Assert.True(lookupNextDuringCall > DateTimeOffset.UtcNow.AddSeconds(30));
        Assert.Equal(1, await ScalarAsync("SELECT lookup_attempt_count FROM commands_inbox WHERE command_id=$id", command.CommandId, CancellationToken.None));
        Assert.Empty(await _store.GetReadyCommandsAsync(10, DateTimeOffset.UtcNow.AddSeconds(5), CancellationToken.None));
    }

    [Fact]
    public async Task First_send_timestamp_is_not_overwritten_on_retries()
    {
        // Lookup reports NotFound (result unknown); re-POST yields a non-terminal outcome so the same
        // command_id is retried and first_sent_at_utc must not be overwritten across those retries.
        var onec = new FakeOnec { StatusKind = OnecExecutionKind.NotFound, ExecuteKind = OnecExecutionKind.Processing };
        var service = new CommandExecutionService(_store, onec, static () => 12, null,
            static () => new CommandExecutionOptions { MaxLookupAttempts = 24, MaxPostAttempts = 3 });
        var command = MakeCommand(expiresAtUtc: null);
        await _store.StoreCommandAsync(command, DateTimeOffset.UtcNow, CancellationToken.None);
        var postAttempt = await _store.ClaimPostAttemptAsync(command.CommandId, CancellationToken.None);
        await _store.MarkUnknownResultAsync(command.CommandId, postAttempt!.AttemptId, "UNKNOWN_RESULT", "1C execution result is unknown.", null, CancellationToken.None);
        var firstSent = await DateScalarAsync("SELECT first_sent_at_utc FROM commands_inbox WHERE command_id=$id", command.CommandId, CancellationToken.None);
        Assert.NotNull(firstSent);

        await Task.Delay(20);
        // Each pass: NotFound -> re-POST same id; first_sent_at_utc must be preserved across them.
        // A POST leaves the row 'executing'; RecoverAsync restores it to 'unknown_result' for re-resolve.
        for (var i = 0; i < 2; i++)
        {
            await MakeRetryDueAsync(command.CommandId); // explicit scheduler dispatch (due-guarded claim)
            await service.ProcessAsync(await ReadyForAsync(command.CommandId), NeverAdministrative, CancellationToken.None);
            await _store.RecoverAsync(CancellationToken.None);
        }

        var stillFirstSent = await DateScalarAsync("SELECT first_sent_at_utc FROM commands_inbox WHERE command_id=$id", command.CommandId, CancellationToken.None);
        Assert.Equal(firstSent, stillFirstSent);
    }

    [Fact]
    public async Task Resolution_age_limit_expires_with_explicit_unknown_result_dead_letter()
    {
        var onec = new FakeOnec { StatusKind = OnecExecutionKind.Processing };
        var service = new CommandExecutionService(_store, onec, static () => 12, null,
            static () => new CommandExecutionOptions { MaxLookupAttempts = 24, MaxPostAttempts = 12, MaxResolutionAgeHours = 1 });
        var command = MakeCommand(expiresAtUtc: null);
        await _store.StoreCommandAsync(command, DateTimeOffset.UtcNow, CancellationToken.None);
        var postAttempt = await _store.ClaimPostAttemptAsync(command.CommandId, CancellationToken.None);
        await _store.MarkUnknownResultAsync(command.CommandId, postAttempt!.AttemptId, "STATUS_PENDING", "1C result is still unknown.", null, CancellationToken.None);

        // age the first-send timestamp past the configured resolution-age limit
        await ExecuteSqlAsync("UPDATE commands_inbox SET first_sent_at_utc=$old WHERE command_id=$id", ("$old", DateTimeOffset.UtcNow.AddHours(-2).ToString("O")), ("$id", command.CommandId.ToString("D")));

        await MakeRetryDueAsync(command.CommandId); // explicit scheduler dispatch (due-guarded claim)
        await service.ProcessAsync(await ReadyForAsync(command.CommandId), NeverAdministrative, CancellationToken.None);

        Assert.Equal(0, onec.StatusCalls);
        // After DeadLetter the row is enqueued for delivery as 'result_pending' (terminal for execution);
        // the ready query must never return it again.
        Assert.Equal(CommandStatus.ResultPending, await StatusAsync(command.CommandId));
        Assert.Empty(await ReadyAsync());
        var result = await ResultForAsync(command.CommandId);
        Assert.Equal("dead_letter", result.GetProperty("status").GetString());
        var error = result.GetProperty("error");
        Assert.Equal("UNKNOWN_RESULT_LIMIT", error.GetProperty("code").GetString());
        Assert.Contains("manual investigation", error.GetProperty("message").GetString(), StringComparison.OrdinalIgnoreCase);
        Assert.True(error.GetProperty("details").GetProperty("outcomeUnknown").GetBoolean());
    }

    [Fact]
    public async Task Migrated_existing_database_preserves_command_attempts_and_unknown_data()
    {
        var factory = _database.CreateFactory(Path.Combine("data", "legacy.db"));
        var migration001Sql = await File.ReadAllTextAsync(Path.Combine(AppContext.BaseDirectory, "Migrations", "001_initial.sql"));
        var migration001Checksum = Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(System.Text.Encoding.UTF8.GetBytes(migration001Sql)));
        await using (var connection = await factory.OpenAsync(CancellationToken.None))
        {
            await using var migrator001 = connection.CreateCommand();
            migrator001.CommandText = "CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY, name TEXT NOT NULL, checksum TEXT NOT NULL, applied_at_utc TEXT NOT NULL);";
            await migrator001.ExecuteNonQueryAsync(CancellationToken.None);
            await using var apply = connection.CreateCommand();
            apply.CommandText = migration001Sql;
            await apply.ExecuteNonQueryAsync(CancellationToken.None);
            await using var record = connection.CreateCommand();
            record.CommandText = "INSERT INTO schema_migrations(version,name,checksum,applied_at_utc) VALUES(1,'001_initial.sql',$checksum,$now);";
            record.Parameters.AddWithValue("$checksum", migration001Checksum);
            record.Parameters.AddWithValue("$now", DateTimeOffset.UtcNow.ToString("O"));
            await record.ExecuteNonQueryAsync(CancellationToken.None);

            var id = Guid.NewGuid().ToString("D");
            await using var seedCommand = connection.CreateCommand();
            seedCommand.CommandText = """
                INSERT INTO commands_inbox(command_id,command_type,payload_version,priority,ordering_key,correlation_id,payload_json,payload_hash,status,created_at_utc,received_at_utc,attempt_count,next_attempt_at_utc,last_error_code,last_error_message)
                VALUES($id,'create_customer_order',1,100,'order:42',NULL,'{"amount":10}','hash','unknown_result',$now,$now,3,$now,'STATUS_PENDING','1C result is still unknown.');
                INSERT INTO command_attempts(attempt_id,command_id,attempt_no,started_at_utc,request_hash,finished_at_utc,outcome,error_code,error_message)
                VALUES($a1,$id,1,$now,'hash',$now,'unknown_result','STATUS_PENDING','1C result is still unknown.'),
                       ($a2,$id,2,$now,'hash',$now,'unknown_result','STATUS_PENDING','1C result is still unknown.'),
                       ($a3,$id,3,$now,'hash',$now,'unknown_result','STATUS_PENDING','1C result is still unknown.');
                """;
            seedCommand.Parameters.AddWithValue("$id", id);
            seedCommand.Parameters.AddWithValue("$a1", Guid.NewGuid().ToString("D"));
            seedCommand.Parameters.AddWithValue("$a2", Guid.NewGuid().ToString("D"));
            seedCommand.Parameters.AddWithValue("$a3", Guid.NewGuid().ToString("D"));
            seedCommand.Parameters.AddWithValue("$now", DateTimeOffset.UtcNow.ToString("O"));
            await seedCommand.ExecuteNonQueryAsync(CancellationToken.None);
        }
        await SqliteTestDatabase.ClearPoolAsync(factory);

        // migration must be non-destructive and checksum-verified; legacy row keeps its data
        var store = new SqliteAgentStore(factory, new SqliteMigrator(factory));
        await store.InitializeAsync(CancellationToken.None);
        var ready = await store.GetReadyCommandsAsync(10, DateTimeOffset.UtcNow.AddMinutes(1), CancellationToken.None);
        var stored = Assert.Single(ready);
        Assert.Equal(CommandStatus.UnknownResult, stored.Status);
        Assert.Equal(3, stored.AttemptCount);
        Assert.Equal("STATUS_PENDING", stored.LastErrorCode);
        Assert.Equal(3, await CountAsync(factory, "SELECT COUNT(*) FROM command_attempts"));
        Assert.Equal("post", await StringAsync(factory, "SELECT attempt_kind FROM command_attempts LIMIT 1"));
        Assert.Equal(migration001Checksum, await StringAsync(factory, "SELECT checksum FROM schema_migrations WHERE version=1"));
        Assert.Equal(12, await CountAsync(factory, "SELECT COUNT(*) FROM schema_migrations"));
    }

    private static async Task<long> CountAsync(SqliteConnectionFactory factory, string sql)
    {
        await using var connection = await factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        return Convert.ToInt64(await command.ExecuteScalarAsync(CancellationToken.None), System.Globalization.CultureInfo.InvariantCulture);
    }

    private static async Task<string?> StringAsync(SqliteConnectionFactory factory, string sql)
    {
        await using var connection = await factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        return await command.ExecuteScalarAsync(CancellationToken.None) as string;
    }

    private static OnecExecutionResult SucceededResult(Guid id) =>
        new(OnecExecutionKind.Succeeded, new(id, "succeeded", JsonSerializer.SerializeToElement(new { @ref = "synthetic-ref" }), null, [], 1), 200, null, null);

    private async Task<CommandStatus> StatusAsync(Guid commandId)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT status FROM commands_inbox WHERE command_id=$id";
        command.Parameters.AddWithValue("$id", commandId.ToString("D"));
        var status = (string?)await command.ExecuteScalarAsync(CancellationToken.None);
        return status is null ? throw new InvalidOperationException($"Command {commandId} not found.") : ParseStatus(status);
    }

    private static CommandStatus ParseStatus(string status) => status switch
    {
        "queued" => CommandStatus.Queued,
        "retry_waiting" => CommandStatus.RetryWaiting,
        "unknown_result" => CommandStatus.UnknownResult,
        "dead_letter" => CommandStatus.DeadLetter,
        "result_pending" => CommandStatus.ResultPending,
        "succeeded_local" => CommandStatus.SucceededLocal,
        "business_failed_local" => CommandStatus.BusinessFailedLocal,
        _ => Enum.Parse<CommandStatus>(status.Replace("_", string.Empty, StringComparison.Ordinal), true)
    };

    private async Task<string?> StringAsync(string sql, Guid commandId, CancellationToken cancellationToken)
    {
        await using var connection = await _factory.OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.Parameters.AddWithValue("$id", commandId.ToString("D"));
        return await command.ExecuteScalarAsync(cancellationToken) as string;
    }

    private async Task<long> ScalarAsync(string sql, Guid commandId, CancellationToken cancellationToken)
    {
        await using var connection = await _factory.OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.Parameters.AddWithValue("$id", commandId.ToString("D"));
        return Convert.ToInt64(await command.ExecuteScalarAsync(cancellationToken), System.Globalization.CultureInfo.InvariantCulture);
    }

    private async Task<DateTimeOffset?> DateScalarAsync(string sql, Guid commandId, CancellationToken cancellationToken)
    {
        await using var connection = await _factory.OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.Parameters.AddWithValue("$id", commandId.ToString("D"));
        var value = await command.ExecuteScalarAsync(cancellationToken);
        return value is string text ? DateTimeOffset.Parse(text, System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.RoundtripKind).ToUniversalTime() : null;
    }

    private async Task ExecuteSqlAsync(string sql, params (string Name, object Value)[] parameters)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        foreach (var (name, value) in parameters) command.Parameters.AddWithValue(name, value);
        await command.ExecuteNonQueryAsync(CancellationToken.None);
    }

    // Multi-pass tests simulate the scheduler: the per-pass claim acquisition is due-guarded, so a
    // pass that must run NOW makes its retry explicitly due here instead of relying on the bypass
    // clock of ReadyForAsync (the due guard itself is never removed to go green).
    private Task MakeRetryDueAsync(Guid commandId) =>
        ExecuteSqlAsync("UPDATE commands_inbox SET next_attempt_at_utc=$due WHERE command_id=$id",
            ("$due", DateTimeOffset.UtcNow.AddSeconds(-30).ToString("O")), ("$id", commandId.ToString("D")));

    private static Task<bool> NeverAdministrative(Domain.Commands.CommandEnvelope _, string __, CancellationToken ___) => Task.FromResult(false);

    private async Task<StoredCommand> SingleReadyAsync() => (await ReadyAsync()).Single();

    private async Task<StoredCommand> StoredForAsync(Guid commandId) =>
        (await ReadyAsync()).Concat(await TerminalStoredAsync(commandId)).Single(c => c.Envelope.CommandId == commandId);

    private async Task<IReadOnlyList<StoredCommand>> TerminalStoredAsync(Guid commandId)
    {
        // Build a StoredCommand for a terminal row (excluded from the ready query) so tests can hand it
        // straight to CommandExecutionService.ProcessAsync and assert the store-level guard rejects it.
        // AttemptCount is set > 0 so the command is treated as already-sent and routed to the
        // status-lookup path (where the lookup claim guard lives) rather than the fresh-POST path.
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT status,lookup_attempt_count,post_attempt_count,attempt_count FROM commands_inbox WHERE command_id=$id";
        command.Parameters.AddWithValue("$id", commandId.ToString("D"));
        await using var reader = await command.ExecuteReaderAsync(CancellationToken.None);
        if (!await reader.ReadAsync(CancellationToken.None)) throw new InvalidOperationException($"Command {commandId} not found.");
        var stored = new StoredCommand(
            new CommandEnvelope(commandId, "create_customer_order", 1, 100, "order:42", null, DateTimeOffset.UtcNow, null, null, null, string.Empty, default),
            ParseStatus(reader.GetString(0)),
            DateTimeOffset.UtcNow,
            Math.Max(1, reader.GetInt32(3)),
            null,
            null,
            null,
            reader.GetInt32(1),
            reader.GetInt32(2),
            null);
        return new[] { stored };
    }

    private async Task<IReadOnlyList<StoredCommand>> ReadyAsync()
    {
        // Look far enough ahead so backoff-scheduled NextAttemptAt values are visible to tests;
        // the scheduler itself filters by real "now" at runtime. Terminal rows are excluded by
        // design (never make a terminal command runnable) and must be verified via the DB/result.
        return await _store.GetReadyCommandsAsync(10, DateTimeOffset.UtcNow.AddDays(1), CancellationToken.None);
    }

    private async Task<StoredCommand> ReadyForAsync(Guid commandId) =>
        (await ReadyAsync()).Single(c => c.Envelope.CommandId == commandId);

    private async Task<JsonElement> ResultForAsync(Guid commandId)
    {
        var pending = await _store.GetPendingResultsAsync(10, DateTimeOffset.UtcNow, CancellationToken.None);
        var match = pending.Single(p => p.CommandId == commandId);
        using var document = JsonDocument.Parse(match.PayloadJson);
        return document.RootElement.Clone();
    }

    private async Task<JsonElement> SingleResultAsync()
    {
        // Single-command tests: read the one persisted result row directly (works even after the
        // command leaves the ready set as terminal).
        var pending = (await _store.GetPendingResultsAsync(10, DateTimeOffset.UtcNow, CancellationToken.None)).Single();
        using var document = JsonDocument.Parse(pending.PayloadJson);
        return document.RootElement.Clone();
    }

    private static CommandEnvelope MakeCommand(DateTimeOffset? expiresAtUtc, string? orderingKey = null)
    {
        using var document = JsonDocument.Parse("{\"amount\":10}");
        var payload = document.RootElement.Clone();
        return new(Guid.NewGuid(), "create_customer_order", 1, 100, orderingKey ?? "order:42", null, DateTimeOffset.UtcNow, null, expiresAtUtc, null, PayloadHasher.Compute(payload), payload);
    }

    private sealed class FakeOnec : IOnecCommandClient
    {
        public int ExecuteCalls { get; private set; }
        public int StatusCalls { get; private set; }
        public Guid LastExecutedId { get; private set; }
        public OnecExecutionKind StatusKind { get; init; } = OnecExecutionKind.Succeeded;
        public OnecExecutionKind ExecuteKind { get; init; } = OnecExecutionKind.Succeeded;
        public Func<CommandEnvelope, CancellationToken, Task<OnecExecutionResult>>? OnExecute { get; init; }
        public Func<Guid, CancellationToken, Task<OnecExecutionResult>>? OnStatus { get; init; }

        public Task<OnecExecutionResult> ExecuteAsync(CommandEnvelope command, CancellationToken cancellationToken)
        {
            ExecuteCalls++;
            LastExecutedId = command.CommandId;
            return OnExecute?.Invoke(command, cancellationToken) ?? Task.FromResult(ForKind(ExecuteKind, command.CommandId));
        }

        public Task<OnecExecutionResult> GetStatusAsync(Guid commandId, CancellationToken cancellationToken)
        {
            StatusCalls++;
            return OnStatus?.Invoke(commandId, cancellationToken) ?? Task.FromResult(ForKind(StatusKind, commandId));
        }

        private static OnecExecutionResult ForKind(OnecExecutionKind kind, Guid id) => kind switch
        {
            OnecExecutionKind.Succeeded => Success(id),
            OnecExecutionKind.BusinessError => new(OnecExecutionKind.BusinessError,
                new(id, "business_failed", null, new("ONEC_BUSINESS_ERROR", "rejected", false, null), [], 1), 200, "ONEC_BUSINESS_ERROR", "rejected"),
            _ => new(kind, null, 202, null, null)
        };

        private static OnecExecutionResult Success(Guid id) =>
            new(OnecExecutionKind.Succeeded, new(id, "succeeded", JsonSerializer.SerializeToElement(new { @ref = "synthetic-ref" }), null, [], 1), 200, null, null);
    }
}
