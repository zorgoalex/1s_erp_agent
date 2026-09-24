using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.Json;
using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Application.Commands;
using ErpOnecAgent.Contracts.OneC;
using ErpOnecAgent.Domain.Commands;
using ErpOnecAgent.Domain.Common;
using ErpOnecAgent.Infrastructure.Persistence.Sqlite;
using Xunit;

namespace ErpOnecAgent.IntegrationTests;

public sealed class AdministrativeClaimTests : IAsyncLifetime
{
    private static readonly TimeSpan GateTimeout = TimeSpan.FromSeconds(5);
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
    public async Task Stale_terminal_snapshot_does_not_invoke_administrative_side_effect()
    {
        var now = DateTimeOffset.UtcNow;
        var command = MakeCommand(now);
        await _store.StoreCommandAsync(command, now, CancellationToken.None);
        var snapshot = await ReadyForAsync(command.CommandId);
        Assert.True(await _store.CompleteLocallyAsync(command.CommandId, CommandStatus.SucceededLocal, SuccessResult(command.CommandId), null, null, null, CancellationToken.None));

        var administrativeCalls = 0;
        var onec = new FakeOnec();
        var service = new CommandExecutionService(_store, onec, static () => 12);

        await service.ProcessAsync(snapshot, (_, _, _) =>
        {
            Interlocked.Increment(ref administrativeCalls);
            return Task.FromResult(true);
        }, CancellationToken.None);

        Assert.Equal(0, administrativeCalls);
        Assert.Equal(0, onec.StatusCalls);
        Assert.Equal(0, onec.ExecuteCalls);
        Assert.Equal("succeeded_local", await ResultStatusAsync(command.CommandId));
        Assert.Equal(1, await CountOutboxAsync(command.CommandId));
    }

    [Fact]
    public async Task Future_retry_schedule_snapshot_does_not_invoke_administrative_side_effect()
    {
        var now = DateTimeOffset.UtcNow;
        var command = MakeCommand(now);
        await _store.StoreCommandAsync(command, now, CancellationToken.None);
        var snapshot = await ReadyForAsync(command.CommandId);
        await ExecuteSqlAsync(
            "UPDATE commands_inbox SET next_attempt_at_utc=$future WHERE command_id=$id",
            ("$future", now.AddHours(1).ToString("O")),
            ("$id", command.CommandId.ToString("D")));

        var administrativeCalls = 0;
        var onec = new FakeOnec();
        var service = new CommandExecutionService(_store, onec, static () => 12);

        await service.ProcessAsync(snapshot, (_, _, _) =>
        {
            Interlocked.Increment(ref administrativeCalls);
            return Task.FromResult(true);
        }, CancellationToken.None);

        Assert.Equal(0, administrativeCalls);
        Assert.Equal(0, onec.StatusCalls);
        Assert.Equal(0, onec.ExecuteCalls);
        Assert.Equal(0, await CountOutboxAsync(command.CommandId));
    }

    [Fact]
    public async Task Future_not_before_snapshot_does_not_invoke_administrative_side_effect()
    {
        var now = DateTimeOffset.UtcNow;
        var command = MakeCommand(now, notBeforeUtc: now.AddHours(1));
        await _store.StoreCommandAsync(command, now, CancellationToken.None);
        var snapshot = await ReadyForAsync(command.CommandId, now.AddHours(2));

        var administrativeCalls = 0;
        var onec = new FakeOnec();
        var service = new CommandExecutionService(_store, onec, static () => 12);

        await service.ProcessAsync(snapshot, (_, _, _) =>
        {
            Interlocked.Increment(ref administrativeCalls);
            return Task.FromResult(true);
        }, CancellationToken.None);

        Assert.Equal(0, administrativeCalls);
        Assert.Equal(0, onec.StatusCalls);
        Assert.Equal(0, onec.ExecuteCalls);
        Assert.Equal(0, await CountOutboxAsync(command.CommandId));
    }

    [Fact]
    public async Task Ordering_blocked_snapshot_does_not_invoke_administrative_side_effect()
    {
        var now = DateTimeOffset.UtcNow;
        var orderingKey = "admin-order:blocked";
        var predecessor = MakeCommand(now.AddMinutes(-2), orderingKey: orderingKey);
        var successor = MakeCommand(now.AddMinutes(-1), orderingKey: orderingKey);
        await _store.StoreCommandAsync(predecessor, now.AddMinutes(-2), CancellationToken.None);
        await _store.StoreCommandAsync(successor, now.AddMinutes(-1), CancellationToken.None);
        var snapshot = new StoredCommand(successor, CommandStatus.Queued, now.AddMinutes(-1), 0, null, null, null);

        var administrativeCalls = 0;
        var onec = new FakeOnec();
        var service = new CommandExecutionService(_store, onec, static () => 12);

        await service.ProcessAsync(snapshot, (_, _, _) =>
        {
            Interlocked.Increment(ref administrativeCalls);
            return Task.FromResult(true);
        }, CancellationToken.None);

        Assert.Equal(0, administrativeCalls);
        Assert.Equal(0, onec.StatusCalls);
        Assert.Equal(0, onec.ExecuteCalls);
        Assert.Equal(0, await CountOutboxAsync(successor.CommandId));
    }

    [Fact]
    public async Task Concurrent_gated_administrative_passes_invoke_exactly_one_side_effect()
    {
        var now = DateTimeOffset.UtcNow;
        var command = MakeCommand(now);
        await _store.StoreCommandAsync(command, now, CancellationToken.None);
        var snapshot = await ReadyForAsync(command.CommandId);
        var firstEntered = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondEntered = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var enteredCalls = 0;
        var sideEffects = 0;
        var onec = new FakeOnec();
        var service = new CommandExecutionService(_store, onec, static () => 12, executorId: "admin-concurrent");

        async Task<bool> Administrative(CommandEnvelope envelope, string claimOwner, CancellationToken cancellationToken)
        {
            var call = Interlocked.Increment(ref enteredCalls);
            if (call == 1) firstEntered.TrySetResult(true);
            if (call == 2) secondEntered.TrySetResult(true);
            await release.Task.WaitAsync(cancellationToken);
            Interlocked.Increment(ref sideEffects);
            await _store.CompleteLocallyAsync(envelope.CommandId, claimOwner, CommandStatus.SucceededLocal, SuccessResult(envelope.CommandId), null, null, null, CancellationToken.None);
            return true;
        }

        var first = service.ProcessAsync(snapshot, Administrative, CancellationToken.None);
        await firstEntered.Task.WaitAsync(GateTimeout);
        var second = service.ProcessAsync(snapshot, Administrative, CancellationToken.None);
        await Task.WhenAny(second, secondEntered.Task).WaitAsync(GateTimeout);
        release.TrySetResult(true);
        await Task.WhenAll(first, second).WaitAsync(GateTimeout);

        Assert.Equal(1, Volatile.Read(ref sideEffects));
        Assert.Equal(0, onec.StatusCalls);
        Assert.Equal(0, onec.ExecuteCalls);
        Assert.Equal(1, await CountOutboxAsync(command.CommandId));
    }

    [Fact]
    public async Task Administrative_completion_uses_claim_owner_and_does_not_call_onec()
    {
        var now = DateTimeOffset.UtcNow;
        var command = MakeCommand(now);
        await _store.StoreCommandAsync(command, now, CancellationToken.None);
        var snapshot = await ReadyForAsync(command.CommandId);
        var onec = new FakeOnec();
        var service = new CommandExecutionService(_store, onec, static () => 12, executorId: "admin-success");

        await service.ProcessAsync(snapshot, async (envelope, claimOwner, _) =>
        {
            Assert.Equal(claimOwner, await ReadClaimOwnerAsync(envelope.CommandId, CancellationToken.None));
            return await _store.CompleteLocallyAsync(envelope.CommandId, claimOwner, CommandStatus.SucceededLocal, SuccessResult(envelope.CommandId), null, null, null, CancellationToken.None);
        }, CancellationToken.None);

        Assert.Equal(0, onec.StatusCalls);
        Assert.Equal(0, onec.ExecuteCalls);
        Assert.Equal(0, await CountAsync("SELECT post_attempt_count FROM commands_inbox WHERE command_id=$id", command.CommandId));
        Assert.Equal(0, await CountAsync("SELECT lookup_attempt_count FROM commands_inbox WHERE command_id=$id", command.CommandId));
        Assert.Null(await ReadClaimOwnerAsync(command.CommandId, CancellationToken.None));
        var result = await ResultForAsync(command.CommandId);
        Assert.Equal("succeeded", result.GetProperty("status").GetString());
        Assert.True(result.GetProperty("data").GetProperty("accepted").GetBoolean());
        Assert.Equal(0, result.GetProperty("warnings").GetArrayLength());
    }

    [Fact]
    public async Task Expired_never_sent_administrative_command_expires_without_side_effect()
    {
        var now = DateTimeOffset.UtcNow;
        var command = MakeCommand(now, expiresAtUtc: now.AddMinutes(-5));
        await _store.StoreCommandAsync(command, now, CancellationToken.None);
        var snapshot = await ReadyForAsync(command.CommandId);

        var administrativeCalls = 0;
        var onec = new FakeOnec();
        var service = new CommandExecutionService(_store, onec, static () => 12);

        await service.ProcessAsync(snapshot, (_, _, _) =>
        {
            Interlocked.Increment(ref administrativeCalls);
            return Task.FromResult(true);
        }, CancellationToken.None);

        Assert.Equal(0, administrativeCalls);
        Assert.Equal(0, onec.StatusCalls);
        Assert.Equal(0, onec.ExecuteCalls);
        var result = await ResultForAsync(command.CommandId);
        Assert.Equal("expired", result.GetProperty("status").GetString());
        Assert.Equal("COMMAND_EXPIRED", result.GetProperty("error").GetProperty("code").GetString());
    }

    [Fact]
    public async Task Thrown_administrative_callback_releases_claim()
    {
        var now = DateTimeOffset.UtcNow;
        var command = MakeCommand(now);
        await _store.StoreCommandAsync(command, now, CancellationToken.None);
        var snapshot = await ReadyForAsync(command.CommandId);
        var onec = new FakeOnec();
        var service = new CommandExecutionService(_store, onec, static () => 12, executorId: "admin-throw");
        string? observedOwner = null;

        await Assert.ThrowsAsync<InvalidOperationException>(() => service.ProcessAsync(snapshot, async (envelope, _, _) =>
        {
            observedOwner = await ReadClaimOwnerAsync(envelope.CommandId, CancellationToken.None);
            throw new InvalidOperationException("administrative callback failed");
        }, CancellationToken.None));

        Assert.NotNull(observedOwner);
        Assert.StartsWith("admin-throw:", observedOwner, StringComparison.Ordinal);
        Assert.Null(await ReadClaimOwnerAsync(command.CommandId, CancellationToken.None));
        Assert.Equal(0, onec.StatusCalls);
        Assert.Equal(0, onec.ExecuteCalls);
    }

    [Theory]
    [InlineData("pause_etl")]
    [InlineData("start_full_sync")]
    public async Task Persisted_unknown_administrative_command_is_quarantined_without_onec(string commandType)
    {
        var now = DateTimeOffset.UtcNow;
        var command = MakeCommand(now, commandType: commandType);
        await _store.StoreCommandAsync(command, now, CancellationToken.None);
        Assert.True(await _store.MarkUnknownResultAsync(command.CommandId, "UNHANDLED_EXECUTION_ERROR", "legacy worker fallback", now.AddSeconds(-1), CancellationToken.None));
        Assert.Equal(0, await CountAsync("SELECT post_attempt_count FROM commands_inbox WHERE command_id=$id", command.CommandId));
        Assert.Equal(0, await CountAsync("SELECT lookup_attempt_count FROM commands_inbox WHERE command_id=$id", command.CommandId));
        var snapshot = await ReadyForAsync(command.CommandId);
        var administrativeCalls = 0;
        var onec = new FakeOnec { StatusKind = OnecExecutionKind.NotFound, ExecuteKind = OnecExecutionKind.Succeeded };
        var service = new CommandExecutionService(_store, onec, static () => 12, executorId: "admin-persisted-unknown");

        await service.ProcessAsync(snapshot, (_, _, _) =>
        {
            Interlocked.Increment(ref administrativeCalls);
            return Task.FromResult(true);
        }, CancellationToken.None);

        Assert.Equal(0, administrativeCalls);
        Assert.Equal(0, onec.StatusCalls);
        Assert.Equal(0, onec.ExecuteCalls);
        Assert.Equal(0, await CountAsync("SELECT post_attempt_count FROM commands_inbox WHERE command_id=$id", command.CommandId));
        Assert.Equal(0, await CountAsync("SELECT lookup_attempt_count FROM commands_inbox WHERE command_id=$id", command.CommandId));
        var result = await ResultForAsync(command.CommandId);
        Assert.Equal("dead_letter", result.GetProperty("status").GetString());
        Assert.Equal("ADMINISTRATIVE_EXECUTION_UNKNOWN", result.GetProperty("error").GetProperty("code").GetString());
        Assert.True(result.GetProperty("error").GetProperty("details").GetProperty("outcomeUnknown").GetBoolean());
    }

    [Fact]
    public async Task Stale_queued_snapshot_with_live_unknown_administrative_row_is_quarantined_without_onec()
    {
        var now = DateTimeOffset.UtcNow;
        var command = MakeCommand(now, commandType: "start_full_sync");
        await _store.StoreCommandAsync(command, now, CancellationToken.None);
        var snapshot = await ReadyForAsync(command.CommandId);
        Assert.True(await _store.MarkUnknownResultAsync(command.CommandId, "UNHANDLED_EXECUTION_ERROR", "row changed after snapshot", now.AddSeconds(-1), CancellationToken.None));
        var administrativeCalls = 0;
        var onec = new FakeOnec { StatusKind = OnecExecutionKind.NotFound, ExecuteKind = OnecExecutionKind.Succeeded };
        var service = new CommandExecutionService(_store, onec, static () => 12, executorId: "admin-stale-unknown");

        await service.ProcessAsync(snapshot, (_, _, _) =>
        {
            Interlocked.Increment(ref administrativeCalls);
            return Task.FromResult(true);
        }, CancellationToken.None);

        Assert.Equal(0, administrativeCalls);
        Assert.Equal(0, onec.StatusCalls);
        Assert.Equal(0, onec.ExecuteCalls);
        var result = await ResultForAsync(command.CommandId);
        Assert.Equal("dead_letter", result.GetProperty("status").GetString());
        Assert.Equal("ADMINISTRATIVE_EXECUTION_UNKNOWN", result.GetProperty("error").GetProperty("code").GetString());
    }

    [Fact]
    public async Task Stale_sent_administrative_snapshot_is_quarantined_without_onec()
    {
        var now = DateTimeOffset.UtcNow;
        var command = MakeCommand(now, commandType: "pause_etl");
        await _store.StoreCommandAsync(command, now, CancellationToken.None);
        var current = await ReadyForAsync(command.CommandId);
        var stale = current with { Status = CommandStatus.UnknownResult, AttemptCount = 1 };
        var administrativeCalls = 0;
        var onec = new FakeOnec { StatusKind = OnecExecutionKind.NotFound, ExecuteKind = OnecExecutionKind.Succeeded };
        var service = new CommandExecutionService(_store, onec, static () => 12, executorId: "admin-stale-sent");

        await service.ProcessAsync(stale, (_, _, _) =>
        {
            Interlocked.Increment(ref administrativeCalls);
            return Task.FromResult(true);
        }, CancellationToken.None);

        Assert.Equal(0, administrativeCalls);
        Assert.Equal(0, onec.StatusCalls);
        Assert.Equal(0, onec.ExecuteCalls);
        var result = await ResultForAsync(command.CommandId);
        Assert.Equal("dead_letter", result.GetProperty("status").GetString());
        Assert.Equal("ADMINISTRATIVE_EXECUTION_UNKNOWN", result.GetProperty("error").GetProperty("code").GetString());
    }

    [Fact]
    public async Task Simulated_worker_wrapper_fallback_after_failure_persistence_never_reaches_onec()
    {
        var now = DateTimeOffset.UtcNow;
        var command = MakeCommand(now, commandType: "pause_etl");
        await _store.StoreCommandAsync(command, now, CancellationToken.None);
        var snapshot = await ReadyForAsync(command.CommandId);
        var faultingStore = FailingStoreProxy.Create(_store);
        var onec = new FakeOnec { StatusKind = OnecExecutionKind.NotFound, ExecuteKind = OnecExecutionKind.Succeeded };
        var service = new CommandExecutionService(faultingStore, onec, static () => 12, executorId: "admin-failure-persistence");
        var triggerCalls = 0;

        await Assert.ThrowsAsync<InvalidOperationException>(() => service.ProcessAsync(snapshot, (_, _, _) =>
        {
            Interlocked.Increment(ref triggerCalls);
            throw new InvalidOperationException("administrative trigger failed");
        }, CancellationToken.None));

        Assert.Equal(1, triggerCalls);
        Assert.Null(await ReadClaimOwnerAsync(command.CommandId, CancellationToken.None));
        Assert.True(await _store.MarkUnknownResultAsync(command.CommandId, "UNHANDLED_EXECUTION_ERROR", "simulated worker wrapper", now.AddSeconds(-1), CancellationToken.None));
        var retryService = new CommandExecutionService(_store, onec, static () => 12, executorId: "admin-after-wrapper");
        await retryService.ProcessAsync(snapshot, (_, _, _) =>
        {
            Interlocked.Increment(ref triggerCalls);
            return Task.FromResult(true);
        }, CancellationToken.None);

        Assert.Equal(1, triggerCalls);
        Assert.Equal(0, onec.StatusCalls);
        Assert.Equal(0, onec.ExecuteCalls);
        var result = await ResultForAsync(command.CommandId);
        Assert.Equal("dead_letter", result.GetProperty("status").GetString());
        Assert.Equal("ADMINISTRATIVE_EXECUTION_UNKNOWN", result.GetProperty("error").GetProperty("code").GetString());
    }

    [Fact]
    public async Task Failed_administrative_callback_is_not_rerouted_to_onec_on_next_pass()
    {
        var now = DateTimeOffset.UtcNow;
        var command = MakeCommand(now);
        await _store.StoreCommandAsync(command, now, CancellationToken.None);
        var snapshot = await ReadyForAsync(command.CommandId);
        var onec = new FakeOnec();
        var service = new CommandExecutionService(_store, onec, static () => 12, executorId: "admin-failure");

        await Assert.ThrowsAsync<InvalidOperationException>(() => service.ProcessAsync(snapshot, (_, _, _) =>
            throw new InvalidOperationException("administrative callback failed"), CancellationToken.None));

        await service.ProcessAsync(snapshot, (_, _, _) => Task.FromResult(false), CancellationToken.None);

        Assert.Equal(0, onec.StatusCalls);
        Assert.Equal(0, onec.ExecuteCalls);
        var result = await ResultForAsync(command.CommandId);
        Assert.Equal("dead_letter", result.GetProperty("status").GetString());
        Assert.Equal("ADMINISTRATIVE_EXECUTION_UNKNOWN", result.GetProperty("error").GetProperty("code").GetString());
        Assert.Contains("manual investigation", result.GetProperty("error").GetProperty("message").GetString(), StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("administrative callback failed", result.GetProperty("error").GetProperty("message").GetString(), StringComparison.Ordinal);
        Assert.True(result.GetProperty("error").GetProperty("details").GetProperty("outcomeUnknown").GetBoolean());
    }

    [Fact]
    public async Task Cancelled_administrative_callback_releases_claim()
    {
        var now = DateTimeOffset.UtcNow;
        var command = MakeCommand(now);
        await _store.StoreCommandAsync(command, now, CancellationToken.None);
        var snapshot = await ReadyForAsync(command.CommandId);
        var onec = new FakeOnec();
        var service = new CommandExecutionService(_store, onec, static () => 12, executorId: "admin-cancel");
        var entered = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var never = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        string? observedOwner = null;
        using var cancellation = new CancellationTokenSource();

        var pass = service.ProcessAsync(snapshot, async (envelope, _, token) =>
        {
            observedOwner = await ReadClaimOwnerAsync(envelope.CommandId, CancellationToken.None);
            entered.TrySetResult(true);
            await never.Task.WaitAsync(token);
            return true;
        }, cancellation.Token);
        await entered.Task.WaitAsync(GateTimeout);
        cancellation.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pass);
        Assert.NotNull(observedOwner);
        Assert.StartsWith("admin-cancel:", observedOwner, StringComparison.Ordinal);
        Assert.Null(await ReadClaimOwnerAsync(command.CommandId, CancellationToken.None));
        Assert.Equal(0, onec.StatusCalls);
        Assert.Equal(0, onec.ExecuteCalls);
    }

    [Fact]
    public async Task Accepted_administrative_callback_never_falls_through_when_completion_refused()
    {
        var now = DateTimeOffset.UtcNow;
        var command = MakeCommand(now);
        await _store.StoreCommandAsync(command, now, CancellationToken.None);
        var snapshot = await ReadyForAsync(command.CommandId);
        var businessFailures = 0;
        var hooks = new CommandExecutionHooks(OnBusinessFailed: (_, _) => businessFailures++);
        var onec = new FakeOnec();
        var service = new CommandExecutionService(_store, onec, static () => 12, hooks, executorId: "admin-refused");
        var completionChanged = false;

        await service.ProcessAsync(snapshot, async (envelope, claimOwner, _) =>
        {
            await _store.ReleaseCommandExecutionClaimAsync(envelope.CommandId, claimOwner, CancellationToken.None);
            completionChanged = await _store.CompleteLocallyAsync(envelope.CommandId, claimOwner, CommandStatus.SucceededLocal, SuccessResult(envelope.CommandId), null, null, null, CancellationToken.None);
            return true;
        }, CancellationToken.None);

        Assert.False(completionChanged);
        Assert.Equal(0, businessFailures);
        Assert.Equal(0, onec.StatusCalls);
        Assert.Equal(0, onec.ExecuteCalls);
    }

    private async Task<StoredCommand> ReadyForAsync(Guid commandId, DateTimeOffset? nowUtc = null) =>
        (await _store.GetReadyCommandsAsync(10, nowUtc ?? DateTimeOffset.UtcNow.AddDays(1), CancellationToken.None)).Single(command => command.Envelope.CommandId == commandId);

    private async Task<JsonElement> ResultForAsync(Guid commandId)
    {
        var result = (await _store.GetPendingResultsAsync(10, DateTimeOffset.UtcNow, CancellationToken.None)).Single(item => item.CommandId == commandId);
        using var document = JsonDocument.Parse(result.PayloadJson);
        return document.RootElement.Clone();
    }

    private async Task<string?> ReadClaimOwnerAsync(Guid commandId, CancellationToken cancellationToken)
    {
        await using var connection = await _factory.OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT exec_claim_owner_id FROM commands_inbox WHERE command_id=$id";
        command.Parameters.AddWithValue("$id", commandId.ToString("D"));
        return await command.ExecuteScalarAsync(cancellationToken) as string;
    }

    private async Task<long> CountOutboxAsync(Guid commandId) =>
        await CountAsync("SELECT COUNT(*) FROM results_outbox WHERE command_id=$id", commandId);

    private async Task<long> CountAsync(string sql, Guid commandId)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.Parameters.AddWithValue("$id", commandId.ToString("D"));
        return Convert.ToInt64(await command.ExecuteScalarAsync(CancellationToken.None), System.Globalization.CultureInfo.InvariantCulture);
    }

    private async Task<string> ResultStatusAsync(Guid commandId)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT result_status FROM commands_inbox WHERE command_id=$id";
        command.Parameters.AddWithValue("$id", commandId.ToString("D"));
        return (string?)await command.ExecuteScalarAsync(CancellationToken.None) ?? throw new InvalidOperationException("Result status is missing.");
    }

    private async Task ExecuteSqlAsync(string sql, params (string Name, object Value)[] parameters)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        foreach (var (name, value) in parameters) command.Parameters.AddWithValue(name, value);
        await command.ExecuteNonQueryAsync(CancellationToken.None);
    }

    private static CommandEnvelope MakeCommand(DateTimeOffset now, string? orderingKey = null, DateTimeOffset? notBeforeUtc = null, DateTimeOffset? expiresAtUtc = null, string commandType = "pause_etl")
    {
        using var document = JsonDocument.Parse("{\"mode\":\"administrative\"}");
        var payload = document.RootElement.Clone();
        return new(Guid.NewGuid(), commandType, 1, 100, orderingKey ?? "admin-order:" + Guid.NewGuid().ToString("N"), null, now, notBeforeUtc, expiresAtUtc, null, PayloadHasher.Compute(payload), payload);
    }

    private static string SuccessResult(Guid commandId) =>
        JsonSerializer.Serialize(new { commandId, status = "succeeded", completedAtUtc = DateTimeOffset.UtcNow, data = new { accepted = true }, warnings = Array.Empty<string>(), resultVersion = 1 });

    public class FailingStoreProxy : DispatchProxy
    {
        private static readonly ConditionalWeakTable<DispatchProxy, SqliteAgentStore> Stores = new();
        private int _failed;

        public static IAgentStore Create(SqliteAgentStore inner)
        {
            var proxy = DispatchProxy.Create<IAgentStore, FailingStoreProxy>();
            Stores.Add((DispatchProxy)(object)proxy, inner);
            return proxy;
        }

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            if (!Stores.TryGetValue(this, out var inner)) throw new InvalidOperationException("Proxy state is missing.");
            if (targetMethod?.Name == nameof(IAgentStore.CompleteLocallyAsync) && Interlocked.Exchange(ref _failed, 1) == 0)
                throw new InvalidOperationException("Injected administrative failure persistence.");
            return targetMethod!.Invoke(inner, args);
        }
    }

    private sealed class FakeOnec : IOnecCommandClient
    {
        private int _executeCalls;
        private int _statusCalls;

        public int ExecuteCalls => Volatile.Read(ref _executeCalls);
        public int StatusCalls => Volatile.Read(ref _statusCalls);
        public OnecExecutionKind StatusKind { get; init; } = OnecExecutionKind.Succeeded;
        public OnecExecutionKind ExecuteKind { get; init; } = OnecExecutionKind.Succeeded;

        public Task<OnecExecutionResult> ExecuteAsync(CommandEnvelope command, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _executeCalls);
            return Task.FromResult(Result(ExecuteKind, command.CommandId));
        }

        public Task<OnecExecutionResult> GetStatusAsync(Guid commandId, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _statusCalls);
            return Task.FromResult(Result(StatusKind, commandId));
        }

        private static OnecExecutionResult Result(OnecExecutionKind kind, Guid commandId) => kind switch
        {
            OnecExecutionKind.Succeeded => new(kind, new(commandId, "succeeded", JsonSerializer.SerializeToElement(new { @ref = "unexpected" }), null, [], 1), 200, null, null),
            OnecExecutionKind.NotFound => new(kind, null, 404, null, null),
            _ => new(kind, null, 202, null, null)
        };
    }
}
