using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.Json;
using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Application.Commands;
using ErpOnecAgent.Application.Configuration;
using ErpOnecAgent.Contracts.OneC;
using ErpOnecAgent.Domain.Agent;
using ErpOnecAgent.Domain.Commands;
using ErpOnecAgent.Domain.Common;
using ErpOnecAgent.Infrastructure.Persistence.Sqlite;
using ErpOnecAgent.Service.Diagnostics;
using ErpOnecAgent.Service.Runtime;
using ErpOnecAgent.Service.Workers;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace ErpOnecAgent.IntegrationTests;

/// <summary>
/// A07b B2/B7 resolution/admission split matrix (GREEN after the slice):
/// lookups of already-sent business commands run once ready under every command restriction;
/// every POST and every administrative side effect is denied while <c>CanExecuteCommands</c> is
/// false, including the NotFound same-id re-POST and POSTs reached after an async admin callback.
/// </summary>
public sealed class A07CommandBoundaryTests : IAsyncLifetime
{
    private static readonly TimeSpan BoundedWait = TimeSpan.FromSeconds(2);
    private static readonly TimeSpan StopTimeout = TimeSpan.FromSeconds(5);
    private readonly SqliteTestDatabase _database = new();
    private SqliteConnectionFactory _factory = null!;
    private SqliteAgentStore _store = null!;

    public async Task InitializeAsync()
    {
        _factory = _database.CreateFactory(Path.Combine("data", "a07-boundary-matrix.db"));
        _store = new(_factory, new SqliteMigrator(_factory));
        await _store.InitializeAsync(CancellationToken.None);
    }

    public async Task DisposeAsync() => await _database.DisposeAsync();

    // ---- Every restriction source still resolves sent business work (B2) ----

    [Theory]
    [InlineData(AgentMode.PauseCommands)]
    [InlineData(AgentMode.Maintenance)]
    [InlineData(AgentMode.Disabled)]
    public async Task Restrictive_remote_mode_still_resolves_sent_business_command(AgentMode mode)
    {
        var state = ReadyState(mode);
        var onec = new FakeOnec();
        var command = MakeBusinessCommand();
        await SeedSentUnknownAsync(command);
        using var worker = CreateWorker(_store, state, onec);

        try
        {
            await worker.StartAsync(CancellationToken.None);
            var observed = await Task.WhenAny(onec.LookupObserved.Task, Task.Delay(BoundedWait)) == onec.LookupObserved.Task;
            Assert.True(observed, $"Sent business command was not status-resolved under {mode}.");
            Assert.Equal(1, onec.StatusCalls);
            Assert.Equal(0, onec.ExecuteCalls);
            Assert.Equal("succeeded", (await WaitForResultAsync(command.CommandId)).GetProperty("status").GetString());
        }
        finally
        {
            await StopWorkerAsync(worker);
        }
    }

    [Fact]
    public async Task Handshake_maintenance_still_resolves_sent_business_command()
    {
        var state = ReadyState(AgentMode.Normal, handshake: true);
        var onec = new FakeOnec();
        var command = MakeBusinessCommand();
        await SeedSentUnknownAsync(command);
        using var worker = CreateWorker(_store, state, onec);

        try
        {
            await worker.StartAsync(CancellationToken.None);
            var observed = await Task.WhenAny(onec.LookupObserved.Task, Task.Delay(BoundedWait)) == onec.LookupObserved.Task;
            Assert.True(observed, "Sent business command was not status-resolved under handshake maintenance.");
            Assert.Equal(1, onec.StatusCalls);
            Assert.Equal(0, onec.ExecuteCalls);
        }
        finally
        {
            await StopWorkerAsync(worker);
        }
    }

    // ---- Never-sent work is fully denied under every restriction ----

    [Theory]
    [InlineData(AgentMode.PauseCommands)]
    [InlineData(AgentMode.Maintenance)]
    [InlineData(AgentMode.Disabled)]
    public async Task Restrictive_remote_mode_denies_never_sent_command(AgentMode mode)
    {
        var state = ReadyState(mode);
        var onec = new FakeOnec();
        var counter = new QueryCounter();
        var store = QueryCountingStoreProxy.Create(_store, counter);
        var command = MakeBusinessCommand();
        await _store.StoreCommandAsync(command, DateTimeOffset.UtcNow, CancellationToken.None);
        using var worker = CreateWorker(store, state, onec);

        try
        {
            await worker.StartAsync(CancellationToken.None);
            // Positive barrier: two sent-only polls prove the worker is cycling under restriction.
            var polled = await Task.WhenAny(counter.SecondSentQuery, Task.Delay(BoundedWait)) == counter.SecondSentQuery;
            Assert.True(polled, "Worker did not poll the sent-only query under restriction.");
            Assert.Equal(0, counter.Full);
            Assert.Equal(0, onec.ExecuteCalls);
            Assert.Equal(0, onec.StatusCalls);
            Assert.Equal("queued", await StatusStringAsync(command.CommandId));
            Assert.Equal(0, await ScalarAsync("SELECT attempt_count FROM commands_inbox WHERE command_id=$id", command.CommandId));
            Assert.Equal(0, await ScalarAsync("SELECT post_attempt_count FROM commands_inbox WHERE command_id=$id", command.CommandId));
        }
        finally
        {
            await StopWorkerAsync(worker);
        }
    }

    [Fact]
    public async Task Handshake_maintenance_denies_never_sent_command()
    {
        var state = ReadyState(AgentMode.Normal, handshake: true);
        var onec = new FakeOnec();
        var counter = new QueryCounter();
        var store = QueryCountingStoreProxy.Create(_store, counter);
        var command = MakeBusinessCommand();
        await _store.StoreCommandAsync(command, DateTimeOffset.UtcNow, CancellationToken.None);
        using var worker = CreateWorker(store, state, onec);

        try
        {
            await worker.StartAsync(CancellationToken.None);
            var polled = await Task.WhenAny(counter.SecondSentQuery, Task.Delay(BoundedWait)) == counter.SecondSentQuery;
            Assert.True(polled, "Worker did not poll the sent-only query under handshake maintenance.");
            Assert.Equal(0, onec.ExecuteCalls);
            Assert.Equal(0, onec.StatusCalls);
            Assert.Equal("queued", await StatusStringAsync(command.CommandId));
        }
        finally
        {
            await StopWorkerAsync(worker);
        }
    }

    // ---- Drain and local ETL pause alone still admit new work ----

    [Fact]
    public async Task Drain_still_executes_fresh_command()
    {
        var state = ReadyState(AgentMode.Drain);
        var onec = new FakeOnec();
        var command = MakeBusinessCommand();
        await _store.StoreCommandAsync(command, DateTimeOffset.UtcNow, CancellationToken.None);
        using var worker = CreateWorker(_store, state, onec);

        try
        {
            await worker.StartAsync(CancellationToken.None);
            var posted = await Task.WhenAny(onec.PostObserved.Task, Task.Delay(BoundedWait)) == onec.PostObserved.Task;
            Assert.True(posted, "Drain must still admit fresh command POSTs.");
            Assert.Equal(1, onec.ExecuteCalls);
            Assert.Equal("succeeded", (await WaitForResultAsync(command.CommandId)).GetProperty("status").GetString());
        }
        finally
        {
            await StopWorkerAsync(worker);
        }
    }

    [Fact]
    public async Task Local_etl_pause_alone_still_executes_fresh_command()
    {
        var state = ReadyState(AgentMode.Normal, localPaused: true);
        var onec = new FakeOnec();
        var command = MakeBusinessCommand();
        await _store.StoreCommandAsync(command, DateTimeOffset.UtcNow, CancellationToken.None);
        using var worker = CreateWorker(_store, state, onec);

        try
        {
            await worker.StartAsync(CancellationToken.None);
            var posted = await Task.WhenAny(onec.PostObserved.Task, Task.Delay(BoundedWait)) == onec.PostObserved.Task;
            Assert.True(posted, "Local ETL pause must never gate command admission.");
            Assert.Equal(1, onec.ExecuteCalls);
        }
        finally
        {
            await StopWorkerAsync(worker);
        }
    }

    // ---- NotFound under restriction: audited close, backoff, no POST budget, re-lookup after lift ----

    [Fact]
    public async Task NotFound_under_pause_closes_lookup_attempt_and_reschedules_then_lifts_to_repost()
    {
        var state = ReadyState(AgentMode.PauseCommands);
        var onec = new FakeOnec { StatusKind = OnecExecutionKind.NotFound };
        var service = NewService(onec);
        var command = MakeBusinessCommand();
        await _store.StoreCommandAsync(command, DateTimeOffset.UtcNow, CancellationToken.None);
        var postAttempt = await _store.ClaimPostAttemptAsync(command.CommandId, CancellationToken.None);
        Assert.NotNull(postAttempt);
        Assert.True(await _store.MarkUnknownResultAsync(command.CommandId, postAttempt.AttemptId, "UNKNOWN_RESULT", "1C execution result is unknown.", null, CancellationToken.None));
        await MakeRetryDueAsync(command.CommandId);
        var admit = () => state.Snapshot.CanExecuteCommands;

        await service.ProcessAsync(await DueSentForAsync(command.CommandId), NeverAdministrative, CancellationToken.None, admit);

        // Audited: exactly the claimed lookup attempt closed as still-unknown NOT_FOUND with the
        // deferred text; uncertainty/backoff retained via owner-aware scheduling; zero POST budget.
        Assert.Equal(1, onec.StatusCalls);
        Assert.Equal(0, onec.ExecuteCalls);
        Assert.Equal(1, await ScalarAsync("SELECT COUNT(*) FROM command_attempts WHERE command_id=$id AND attempt_kind='lookup' AND outcome='unknown_result' AND error_code='NOT_FOUND' AND finished_at_utc IS NOT NULL", command.CommandId));
        Assert.Equal(1, await ScalarAsync("SELECT post_attempt_count FROM commands_inbox WHERE command_id=$id", command.CommandId));
        Assert.Equal("NOT_FOUND", await StringAsync("SELECT last_error_code FROM commands_inbox WHERE command_id=$id", command.CommandId));
        Assert.Contains("deferred", (await StringAsync("SELECT last_error_message FROM commands_inbox WHERE command_id=$id", command.CommandId))!);
        var nextAttempt = await DateScalarAsync("SELECT next_attempt_at_utc FROM commands_inbox WHERE command_id=$id", command.CommandId);
        Assert.NotNull(nextAttempt);
        Assert.True(nextAttempt > DateTimeOffset.UtcNow.AddSeconds(30), "Denied NotFound must keep backoff uncertainty, not become immediately due.");
        Assert.Equal(CommandStatus.UnknownResult, await StatusAsync(command.CommandId));

        // Lift: the next pass looks up again (resolve-before-retry) and then re-POSTs the same id.
        state.SetRemoteMode(AgentMode.Normal);
        await MakeRetryDueAsync(command.CommandId);
        await service.ProcessAsync(await DueSentForAsync(command.CommandId), NeverAdministrative, CancellationToken.None, admit);

        Assert.Equal(2, onec.StatusCalls);
        Assert.Equal(1, onec.ExecuteCalls);
        Assert.Equal(command.CommandId, onec.LastExecutedId);
        Assert.Equal(2, await ScalarAsync("SELECT post_attempt_count FROM commands_inbox WHERE command_id=$id", command.CommandId));
        Assert.Equal("succeeded", (await ResultForAsync(command.CommandId)).GetProperty("status").GetString());
    }

    // ---- Starvation: sent-only filter applies BEFORE LIMIT ----

    [Fact]
    public async Task Blocked_fresh_rows_ahead_of_due_sent_row_do_not_starve_resolution()
    {
        var state = ReadyState(AgentMode.PauseCommands);
        var onec = new FakeOnec();
        const int maxConcurrency = 4;
        // MaxConcurrency+1 fresh higher-priority rows on DISTINCT ordering keys ahead of the sent row:
        // a post-fetch filter would starve the unknown row (the whole fetched page is ineligible).
        for (var i = 0; i < maxConcurrency + 1; i++)
        {
            await _store.StoreCommandAsync(MakeBusinessCommand(priority: 200), DateTimeOffset.UtcNow, CancellationToken.None);
        }
        var sent = MakeBusinessCommand(priority: 100);
        await SeedSentUnknownAsync(sent);
        using var worker = CreateWorker(_store, state, onec, maxConcurrency);

        try
        {
            await worker.StartAsync(CancellationToken.None);
            var observed = await Task.WhenAny(onec.LookupObserved.Task, Task.Delay(BoundedWait)) == onec.LookupObserved.Task;
            Assert.True(observed, "Due sent row was starved by blocked fresh rows ahead of it in queue order.");
            Assert.Equal(1, onec.StatusCalls);
            Assert.Equal(0, onec.ExecuteCalls);
        }
        finally
        {
            await StopWorkerAsync(worker);
        }
    }

    // ---- Ordering preserved: same-key predecessor still blocks resolution ----

    [Fact]
    public async Task Same_key_active_predecessor_still_blocks_sent_row_in_sent_only_query()
    {
        var orderingKey = $"order:{Guid.NewGuid():N}";
        var predecessor = MakeBusinessCommand(orderingKey: orderingKey);
        await _store.StoreCommandAsync(predecessor, DateTimeOffset.UtcNow, CancellationToken.None);
        var sent = MakeBusinessCommand(orderingKey: orderingKey);
        await SeedSentUnknownAsync(sent);

        var sentOnly = await _store.GetDueSentCommandsAsync(10, DateTimeOffset.UtcNow.AddDays(1), CancellationToken.None);
        Assert.Empty(sentOnly);
        var full = await _store.GetReadyCommandsAsync(10, DateTimeOffset.UtcNow.AddDays(1), CancellationToken.None);
        var head = Assert.Single(full);
        Assert.Equal(predecessor.CommandId, head.Envelope.CommandId);
    }

    // ---- All-fresh restricted queue polls bounded (no empty-batch spin) ----

    [Fact]
    public async Task All_fresh_queue_under_restriction_polls_on_delay_cadence_not_spin()
    {
        var state = ReadyState(AgentMode.PauseCommands);
        var onec = new FakeOnec();
        var counter = new QueryCounter();
        var store = QueryCountingStoreProxy.Create(_store, counter);
        for (var i = 0; i < 3; i++)
        {
            await _store.StoreCommandAsync(MakeBusinessCommand(), DateTimeOffset.UtcNow, CancellationToken.None);
        }
        using var worker = CreateWorker(store, state, onec);

        try
        {
            await worker.StartAsync(CancellationToken.None);
            await Task.Delay(700, CancellationToken.None);
            // The 250 ms empty-batch delay bounds query cadence; a WhenAll(empty) spin would issue
            // hundreds of queries inside this window.
            Assert.InRange(counter.Sent, 1, 5);
            Assert.Equal(0, counter.Full);
            Assert.Equal(0, onec.ExecuteCalls);
            Assert.Equal(0, onec.StatusCalls);
        }
        finally
        {
            await StopWorkerAsync(worker);
        }
    }

    // ---- Admission re-checked after an asynchronous admin callback returning false ----

    [Fact]
    public async Task Restriction_landing_during_async_admin_callback_suppresses_fresh_post()
    {
        var state = ReadyState(AgentMode.Normal);
        var onec = new FakeOnec();
        var service = NewService(onec);
        var command = MakeBusinessCommand();
        await _store.StoreCommandAsync(command, DateTimeOffset.UtcNow, CancellationToken.None);
        var tryAdmin = async (CommandEnvelope _, string _, CancellationToken _) =>
        {
            await Task.Yield();
            state.SetRemoteMode(AgentMode.PauseCommands);
            return false;
        };

        await service.ProcessAsync(await ReadyForAsync(command.CommandId), tryAdmin, CancellationToken.None, () => state.Snapshot.CanExecuteCommands);

        Assert.Equal(0, onec.ExecuteCalls);
        Assert.Equal(0, onec.StatusCalls);
        Assert.Equal("queued", await StatusStringAsync(command.CommandId));
        Assert.Equal(0, await ScalarAsync("SELECT attempt_count FROM commands_inbox WHERE command_id=$id", command.CommandId));
        Assert.Equal(0, await ScalarAsync("SELECT post_attempt_count FROM commands_inbox WHERE command_id=$id", command.CommandId));
        Assert.Null(await DateScalarAsync("SELECT first_sent_at_utc FROM commands_inbox WHERE command_id=$id", command.CommandId));
        Assert.Null(await StringAsync("SELECT exec_claim_owner_id FROM commands_inbox WHERE command_id=$id", command.CommandId));
    }

    // ---- Fresh administrative callbacks are new work: denied under restriction ----

    [Fact]
    public async Task Fresh_admin_command_under_pause_is_denied_without_callback()
    {
        var state = ReadyState(AgentMode.PauseCommands);
        var onec = new FakeOnec();
        var counter = new QueryCounter();
        var store = QueryCountingStoreProxy.Create(_store, counter);
        var command = MakeAdminCommand("pause_etl");
        await _store.StoreCommandAsync(command, DateTimeOffset.UtcNow, CancellationToken.None);
        using var worker = CreateWorker(store, state, onec);

        try
        {
            await worker.StartAsync(CancellationToken.None);
            var polled = await Task.WhenAny(counter.SecondSentQuery, Task.Delay(BoundedWait)) == counter.SecondSentQuery;
            Assert.True(polled, "Worker did not poll the sent-only query under restriction.");
            Assert.Equal(0, onec.ExecuteCalls);
            Assert.Equal(0, onec.StatusCalls);
            Assert.Equal("queued", await StatusStringAsync(command.CommandId));
            Assert.False(state.Snapshot.LocalEtlPaused);
            Assert.Empty(await _store.GetPendingResultsAsync(10, DateTimeOffset.UtcNow, CancellationToken.None));
        }
        finally
        {
            await StopWorkerAsync(worker);
        }
    }

    // ---- Sent admin row resolves ONLY via local quarantine: zero 1C calls, zero callback ----

    [Fact]
    public async Task Sent_admin_command_under_pause_quarantines_locally_with_zero_onec_calls()
    {
        var state = ReadyState(AgentMode.PauseCommands);
        var onec = new FakeOnec();
        var command = MakeAdminCommand("pause_etl");
        await SeedSentUnknownAsync(command);
        using var worker = CreateWorker(_store, state, onec);

        try
        {
            await worker.StartAsync(CancellationToken.None);
            var result = await WaitForResultAsync(command.CommandId);
            Assert.Equal("dead_letter", result.GetProperty("status").GetString());
            Assert.Equal("ADMINISTRATIVE_EXECUTION_UNKNOWN", result.GetProperty("error").GetProperty("code").GetString());
            Assert.Equal(0, onec.ExecuteCalls);
            Assert.Equal(0, onec.StatusCalls);
            // The admin callback must never re-run for a sent-evidence row.
            Assert.False(state.Snapshot.LocalEtlPaused);
        }
        finally
        {
            await StopWorkerAsync(worker);
        }
    }

    // ---- Sent-evidence predicate: post-only and first-sent-only rows route to lookup ----

    [Fact]
    public async Task Retry_waiting_with_post_evidence_resolves_via_lookup_even_when_admission_denied()
    {
        var onec = new FakeOnec();
        var service = NewService(onec);
        var command = MakeBusinessCommand();
        await _store.StoreCommandAsync(command, DateTimeOffset.UtcNow, CancellationToken.None);
        var postAttempt = await _store.ClaimPostAttemptAsync(command.CommandId, CancellationToken.None);
        Assert.NotNull(postAttempt);
        Assert.True(await _store.ScheduleRetryAsync(command.CommandId, "TECHNICAL", "transient", DateTimeOffset.UtcNow.AddSeconds(-1), CancellationToken.None));
        var stored = await DueSentForAsync(command.CommandId);
        Assert.Equal(CommandStatus.RetryWaiting, stored.Status);

        await service.ProcessAsync(stored, NeverAdministrative, CancellationToken.None, mayStartNewWork: static () => false);

        Assert.Equal(1, onec.StatusCalls);
        Assert.Equal(0, onec.ExecuteCalls);
        Assert.Equal("succeeded", (await ResultForAsync(command.CommandId)).GetProperty("status").GetString());
    }

    [Fact]
    public async Task First_sent_evidence_alone_routes_to_lookup_in_query_and_live_routing()
    {
        var onec = new FakeOnec();
        var service = NewService(onec);
        var command = MakeBusinessCommand();
        await _store.StoreCommandAsync(command, DateTimeOffset.UtcNow, CancellationToken.None);
        // Only the timestamp evidence exists (no attempt counters, status still 'queued'): the
        // consistent SQL + live predicate must still classify the row as sent.
        await ExecuteSqlAsync("UPDATE commands_inbox SET first_sent_at_utc=$t WHERE command_id=$id",
            ("$t", DateTimeOffset.UtcNow.AddMinutes(-1).ToString("O")), ("$id", command.CommandId.ToString("D")));
        var stored = await DueSentForAsync(command.CommandId);

        await service.ProcessAsync(stored, NeverAdministrative, CancellationToken.None, mayStartNewWork: static () => false);

        Assert.Equal(1, onec.StatusCalls);
        Assert.Equal(0, onec.ExecuteCalls);
        Assert.Equal("succeeded", (await ResultForAsync(command.CommandId)).GetProperty("status").GetString());
    }

    // ---- Readiness gate precedes any query selection ----

    [Fact]
    public async Task Not_ready_agent_issues_no_command_queries_or_onec_calls()
    {
        var state = new AgentRuntimeState();
        state.SetRemoteMode(AgentMode.Normal); // never CompleteBootstrap: IsReady stays false
        var onec = new FakeOnec();
        var counter = new QueryCounter();
        var store = QueryCountingStoreProxy.Create(_store, counter);
        var command = MakeBusinessCommand();
        await SeedSentUnknownAsync(command);
        using var worker = CreateWorker(store, state, onec);

        try
        {
            await worker.StartAsync(CancellationToken.None);
            await Task.Delay(600, CancellationToken.None);
            Assert.Equal(0, counter.Sent);
            Assert.Equal(0, counter.Full);
            Assert.Equal(0, onec.ExecuteCalls);
            Assert.Equal(0, onec.StatusCalls);
        }
        finally
        {
            await StopWorkerAsync(worker);
        }
    }

    // ---- Expiry is new work: denied pass leaves the never-sent row untouched ----

    [Fact]
    public async Task Expired_never_sent_under_denied_admission_stays_queued_then_expires_after_lift()
    {
        var state = ReadyState(AgentMode.PauseCommands);
        var onec = new FakeOnec();
        var service = NewService(onec);
        var command = MakeBusinessCommand(expiresAtUtc: DateTimeOffset.UtcNow.AddMinutes(-5));
        await _store.StoreCommandAsync(command, DateTimeOffset.UtcNow, CancellationToken.None);
        var admit = () => state.Snapshot.CanExecuteCommands;

        await service.ProcessAsync(await ReadyForAsync(command.CommandId), NeverAdministrative, CancellationToken.None, admit);

        Assert.Equal(0, onec.ExecuteCalls);
        Assert.Equal(0, onec.StatusCalls);
        Assert.Equal("queued", await StatusStringAsync(command.CommandId));
        Assert.Empty(await _store.GetPendingResultsAsync(10, DateTimeOffset.UtcNow, CancellationToken.None));
        Assert.Null(await StringAsync("SELECT exec_claim_owner_id FROM commands_inbox WHERE command_id=$id", command.CommandId));

        state.SetRemoteMode(AgentMode.Normal);
        await service.ProcessAsync(await ReadyForAsync(command.CommandId), NeverAdministrative, CancellationToken.None, admit);

        Assert.Equal("expired", (await ResultForAsync(command.CommandId)).GetProperty("status").GetString());
    }

    // ---- helpers ----

    private CommandExecutionService NewService(FakeOnec onec) =>
        new(_store, onec, static () => 12, null,
            static () => new CommandExecutionOptions { MaxLookupAttempts = 24, MaxPostAttempts = 12, RetryBaseDelaySeconds = 60, RetryMaxDelaySeconds = 600 });

    private async Task SeedSentUnknownAsync(CommandEnvelope command)
    {
        await _store.StoreCommandAsync(command, DateTimeOffset.UtcNow, CancellationToken.None);
        var postAttempt = await _store.ClaimPostAttemptAsync(command.CommandId, CancellationToken.None);
        Assert.NotNull(postAttempt);
        Assert.True(await _store.MarkUnknownResultAsync(command.CommandId, postAttempt.AttemptId, "UNKNOWN_RESULT", "1C execution result is unknown.", DateTimeOffset.UtcNow.AddSeconds(-5), CancellationToken.None));
    }

    private Task MakeRetryDueAsync(Guid commandId) =>
        ExecuteSqlAsync("UPDATE commands_inbox SET next_attempt_at_utc=$due WHERE command_id=$id",
            ("$due", DateTimeOffset.UtcNow.AddSeconds(-30).ToString("O")), ("$id", commandId.ToString("D")));

    private async Task<StoredCommand> DueSentForAsync(Guid commandId) =>
        (await _store.GetDueSentCommandsAsync(10, DateTimeOffset.UtcNow.AddDays(1), CancellationToken.None)).Single(command => command.Envelope.CommandId == commandId);

    private async Task<StoredCommand> ReadyForAsync(Guid commandId) =>
        (await _store.GetReadyCommandsAsync(10, DateTimeOffset.UtcNow.AddDays(1), CancellationToken.None)).Single(command => command.Envelope.CommandId == commandId);

    private async Task<JsonElement> ResultForAsync(Guid commandId)
    {
        var pending = await _store.GetPendingResultsAsync(10, DateTimeOffset.UtcNow, CancellationToken.None);
        using var document = JsonDocument.Parse(pending.Single(item => item.CommandId == commandId).PayloadJson);
        return document.RootElement.Clone();
    }

    private async Task<JsonElement> WaitForResultAsync(Guid commandId)
    {
        var deadline = DateTimeOffset.UtcNow + BoundedWait;
        while (DateTimeOffset.UtcNow < deadline)
        {
            var pending = await _store.GetPendingResultsAsync(10, DateTimeOffset.UtcNow, CancellationToken.None);
            var match = pending.FirstOrDefault(item => item.CommandId == commandId);
            if (match is not null)
            {
                using var document = JsonDocument.Parse(match.PayloadJson);
                return document.RootElement.Clone();
            }
            await Task.Delay(25, CancellationToken.None);
        }
        throw new InvalidOperationException($"No result was persisted for command {commandId} within {BoundedWait}.");
    }

    private async Task<CommandStatus> StatusAsync(Guid commandId) => ParseStatus((await StatusStringAsync(commandId))!);

    private async Task<string?> StatusStringAsync(Guid commandId) =>
        await StringAsync("SELECT status FROM commands_inbox WHERE command_id=$id", commandId);

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

    private async Task<string?> StringAsync(string sql, Guid commandId)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.Parameters.AddWithValue("$id", commandId.ToString("D"));
        return await command.ExecuteScalarAsync(CancellationToken.None) as string;
    }

    private async Task<long> ScalarAsync(string sql, Guid commandId)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.Parameters.AddWithValue("$id", commandId.ToString("D"));
        return Convert.ToInt64(await command.ExecuteScalarAsync(CancellationToken.None), System.Globalization.CultureInfo.InvariantCulture);
    }

    private async Task<DateTimeOffset?> DateScalarAsync(string sql, Guid commandId)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.Parameters.AddWithValue("$id", commandId.ToString("D"));
        var value = await command.ExecuteScalarAsync(CancellationToken.None);
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

    private static AgentRuntimeState ReadyState(AgentMode mode, bool localPaused = false, bool handshake = false)
    {
        var state = new AgentRuntimeState();
        state.RestoreLocalEtlPause(localPaused);
        state.SetRemoteMode(mode);
        state.SetHandshakeMaintenance(handshake);
        state.CompleteBootstrap();
        return state;
    }

    private static CommandExecutionWorker CreateWorker(IAgentStore store, AgentRuntimeState state, FakeOnec onec, int maxConcurrency = 4)
    {
        var agent = Options.Create(new AgentOptions { AgentId = $"a07-boundary-{Guid.NewGuid():N}", SiteId = "a07-site", DataDirectory = Path.GetTempPath() });
        var diagnostics = new DiagnosticsCollector(
            store,
            agent,
            Options.Create(new ErpOptions { RequireClientCertificate = false }),
            Options.Create(new OnecOptions()));
        return new CommandExecutionWorker(
            store,
            onec,
            new FakeOnecHealthClient(),
            new EtlTrigger(),
            state,
            new LocalEtlPauseController(store, state),
            diagnostics,
            Options.Create(new CommandOptions { MaxConcurrency = maxConcurrency, MaxOperationalAttempts = 12, SupportedTypes = ["synthetic"] }),
            NullLogger<CommandExecutionWorker>.Instance);
    }

    private static async Task StopWorkerAsync(CommandExecutionWorker worker)
    {
        using var stop = new CancellationTokenSource(StopTimeout);
        await worker.StopAsync(stop.Token);
    }

    private static CommandEnvelope MakeBusinessCommand(int priority = 100, string? orderingKey = null, DateTimeOffset? expiresAtUtc = null)
    {
        using var document = JsonDocument.Parse("{\"amount\":10}");
        var payload = document.RootElement.Clone();
        return new(Guid.NewGuid(), "create_customer_order", 1, priority, orderingKey ?? $"order:{Guid.NewGuid():N}", null, DateTimeOffset.UtcNow, null, expiresAtUtc, null, PayloadHasher.Compute(payload), payload);
    }

    private static CommandEnvelope MakeAdminCommand(string commandType)
    {
        var payload = JsonSerializer.SerializeToElement(new { mode = "administrative" });
        return new(Guid.NewGuid(), commandType, 1, 100, $"admin:{Guid.NewGuid():N}", null, DateTimeOffset.UtcNow, null, null, null, PayloadHasher.Compute(payload), payload);
    }

    private static Task<bool> NeverAdministrative(CommandEnvelope _, string __, CancellationToken ___) => Task.FromResult(false);

    private sealed class FakeOnec : IOnecCommandClient
    {
        private int _executeCalls;
        private int _statusCalls;

        public OnecExecutionKind StatusKind { get; init; } = OnecExecutionKind.Succeeded;
        public TaskCompletionSource<bool> LookupObserved { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource<bool> PostObserved { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public int ExecuteCalls => Volatile.Read(ref _executeCalls);
        public int StatusCalls => Volatile.Read(ref _statusCalls);
        public Guid LastExecutedId { get; private set; }

        public Task<OnecExecutionResult> ExecuteAsync(CommandEnvelope command, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _executeCalls);
            LastExecutedId = command.CommandId;
            PostObserved.TrySetResult(true);
            return Task.FromResult(new OnecExecutionResult(OnecExecutionKind.Succeeded, new(command.CommandId, "succeeded", JsonSerializer.SerializeToElement(new { @ref = "synthetic-ref" }), null, [], 1), 200, null, null));
        }

        public Task<OnecExecutionResult> GetStatusAsync(Guid commandId, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _statusCalls);
            LookupObserved.TrySetResult(true);
            var result = StatusKind is OnecExecutionKind.Succeeded
                ? new OnecExecutionResult(OnecExecutionKind.Succeeded, new(commandId, "succeeded", JsonSerializer.SerializeToElement(new { @ref = "synthetic-ref" }), null, [], 1), 200, null, null)
                : new OnecExecutionResult(StatusKind, null, 202, null, null);
            return Task.FromResult(result);
        }
    }

    private sealed class FakeOnecHealthClient : IOnecHealthClient
    {
        public Task<OnecHealthResponse?> CheckAsync(CancellationToken cancellationToken) => Task.FromResult<OnecHealthResponse?>(null);
    }

    public sealed class QueryCounter
    {
        private int _full;
        private int _sent;
        private readonly TaskCompletionSource<bool> _secondSent = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public int Full => Volatile.Read(ref _full);
        public int Sent => Volatile.Read(ref _sent);
        public Task SecondSentQuery => _secondSent.Task;
        public void FullQuery() => Interlocked.Increment(ref _full);
        public void SentQuery()
        {
            if (Interlocked.Increment(ref _sent) >= 2) _secondSent.TrySetResult(true);
        }
    }

    /// <summary>IAgentStore pass-through counting ready-query selection (full vs sent-only).</summary>
    public class QueryCountingStoreProxy : DispatchProxy
    {
        private sealed record ProxyTarget(IAgentStore Inner, QueryCounter Counter);
        private static readonly ConditionalWeakTable<DispatchProxy, ProxyTarget> Targets = new();

        public static IAgentStore Create(IAgentStore inner, QueryCounter counter)
        {
            var proxy = DispatchProxy.Create<IAgentStore, QueryCountingStoreProxy>();
            Targets.Add((DispatchProxy)(object)proxy, new(inner, counter));
            return proxy;
        }

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            if (!Targets.TryGetValue(this, out var target)) throw new InvalidOperationException("Proxy state is missing.");
            if (targetMethod?.Name == nameof(IAgentStore.GetReadyCommandsAsync)) target.Counter.FullQuery();
            else if (targetMethod?.Name == nameof(IAgentStore.GetDueSentCommandsAsync)) target.Counter.SentQuery();
            return targetMethod!.Invoke(target.Inner, args);
        }
    }
}
