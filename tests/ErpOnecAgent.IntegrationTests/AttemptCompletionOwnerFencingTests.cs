using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Application.Commands;
using ErpOnecAgent.Contracts.OneC;
using ErpOnecAgent.Domain.Commands;
using ErpOnecAgent.Domain.Common;
using ErpOnecAgent.Infrastructure.Persistence.Sqlite;
using Microsoft.Data.Sqlite;
using Xunit;

namespace ErpOnecAgent.IntegrationTests;

public sealed class AttemptCompletionOwnerFencingTests : IAsyncLifetime
{
    private static readonly DateTimeOffset GenerationA = new(2026, 9, 24, 10, 0, 0, TimeSpan.Zero);
    private static readonly DateTimeOffset GenerationB = GenerationA.AddMinutes(1);
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
    public async Task Legacy_post_claim_cannot_bypass_live_owner()
    {
        var command = await StoreCommandAsync();
        Assert.NotNull(await AcquireAsync(command.CommandId, "owner-a", GenerationA));
        var before = await SnapshotAsync(command.CommandId);

        var claim = await _store.ClaimPostAttemptAsync(command.CommandId, CancellationToken.None);

        Assert.Null(claim);
        Assert.Equal(before, await SnapshotAsync(command.CommandId));
    }

    [Fact]
    public async Task Legacy_lookup_claim_cannot_bypass_live_owner()
    {
        var command = await StoreCommandAsync();
        Assert.NotNull(await AcquireAsync(command.CommandId, "owner-a", GenerationA));
        var before = await SnapshotAsync(command.CommandId);

        var claim = await _store.ClaimLookupAttemptAsync(command.CommandId, "STATUS_PENDING", "pending", GenerationB.AddHours(1), CancellationToken.None);

        Assert.Null(claim);
        Assert.Equal(before, await SnapshotAsync(command.CommandId));
    }

    [Fact]
    public async Task Legacy_completion_cannot_bypass_live_owner()
    {
        var command = await StoreCommandAsync();
        Assert.NotNull(await AcquireAsync(command.CommandId, "owner-a", GenerationA));
        var before = await SnapshotAsync(command.CommandId);

        var changed = await _store.CompleteLocallyAsync(command.CommandId, CommandStatus.SucceededLocal, "{\"status\":\"succeeded\"}", "ref", "1", null, CancellationToken.None);

        Assert.False(changed);
        Assert.Equal(before, await SnapshotAsync(command.CommandId));
    }

    [Fact]
    public async Task Released_generation_a_cannot_mutate_generation_b_attempts_or_completion()
    {
        var command = await StoreCommandAsync();
        Assert.NotNull(await AcquireAsync(command.CommandId, "owner-a", GenerationA));
        var postA = await _store.ClaimPostAttemptAsync(command.CommandId, "owner-a", CancellationToken.None);
        Assert.NotNull(postA);
        await _store.CompleteAttemptAsync(postA.AttemptId, CommandStatus.UnknownResult, "UNKNOWN", "unknown", null, CancellationToken.None);
        Assert.True(await _store.MarkUnknownResultAsync(command.CommandId, postA.AttemptId, "owner-a", "UNKNOWN", "unknown", DateTimeOffset.UtcNow.AddMinutes(-1), CancellationToken.None));
        Assert.NotNull(await AcquireAsync(command.CommandId, "owner-b", DateTimeOffset.UtcNow));
        var before = await SnapshotAsync(command.CommandId);

        var post = await _store.ClaimPostAttemptAsync(command.CommandId, "owner-a", CancellationToken.None);
        var lookup = await _store.ClaimLookupAttemptAsync(command.CommandId, "owner-a", "STALE", "stale", GenerationB.AddHours(1), CancellationToken.None);
        var completed = await _store.CompleteLocallyAsync(command.CommandId, "owner-a", CommandStatus.SucceededLocal, "{\"status\":\"succeeded\"}", "ref", "1", null, CancellationToken.None);

        Assert.Null(post);
        Assert.Null(lookup);
        Assert.False(completed);
        Assert.Equal(before, await SnapshotAsync(command.CommandId));
    }

    [Fact]
    public async Task Released_owner_calls_are_noops_on_an_unclaimed_active_row()
    {
        var command = await StoreCommandAsync();
        Assert.NotNull(await AcquireAsync(command.CommandId, "owner-a", GenerationA));
        await _store.ReleaseCommandExecutionClaimAsync(command.CommandId, "owner-a", CancellationToken.None);
        var before = await SnapshotAsync(command.CommandId);

        var post = await _store.ClaimPostAttemptAsync(command.CommandId, "owner-a", CancellationToken.None);
        var lookup = await _store.ClaimLookupAttemptAsync(command.CommandId, "owner-a", "RELEASED", "released", GenerationA.AddHours(1), CancellationToken.None);
        var completed = await _store.CompleteLocallyAsync(command.CommandId, "owner-a", CommandStatus.SucceededLocal, "{\"status\":\"succeeded\"}", null, null, null, CancellationToken.None);

        Assert.Null(post);
        Assert.Null(lookup);
        Assert.False(completed);
        Assert.Equal(before, await SnapshotAsync(command.CommandId));
    }

    [Fact]
    public async Task Current_owner_can_claim_post_and_complete_exact_attempt()
    {
        var command = await StoreCommandAsync();
        Assert.NotNull(await AcquireAsync(command.CommandId, "owner-current", GenerationA));
        var post = await _store.ClaimPostAttemptAsync(command.CommandId, "owner-current", CancellationToken.None);
        Assert.NotNull(post);

        var changed = await _store.CompleteLocallyAsync(command.CommandId, "owner-current", CommandStatus.SucceededLocal, "{\"status\":\"succeeded\",\"owner\":\"current\"}", "ref", "1", post.AttemptId, CancellationToken.None);

        var state = await SnapshotAsync(command.CommandId);
        Assert.True(changed);
        Assert.Equal("result_pending", state.Status);
        Assert.NotNull(state.Outbox);
        Assert.Contains("succeeded_local", state.Attempts!, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Current_owner_can_claim_lookup_and_complete_exact_attempt()
    {
        var command = await StoreCommandAsync();
        var post = await _store.ClaimPostAttemptAsync(command.CommandId, CancellationToken.None);
        Assert.NotNull(post);
        await _store.CompleteAttemptAsync(post.AttemptId, CommandStatus.UnknownResult, "UNKNOWN", "unknown", null, CancellationToken.None);
        Assert.True(await _store.MarkUnknownResultAsync(command.CommandId, post.AttemptId, "UNKNOWN", "unknown", DateTimeOffset.UtcNow.AddMinutes(-1), CancellationToken.None));
        Assert.NotNull(await AcquireAsync(command.CommandId, "owner-current", DateTimeOffset.UtcNow));

        var lookup = await _store.ClaimLookupAttemptAsync(command.CommandId, "owner-current", "STATUS_PENDING", "pending", GenerationA.AddHours(1), CancellationToken.None);
        Assert.NotNull(lookup);
        var changed = await _store.CompleteLocallyAsync(command.CommandId, "owner-current", CommandStatus.SucceededLocal, "{\"status\":\"succeeded\",\"owner\":\"current\"}", "ref", "1", lookup.AttemptId, CancellationToken.None);

        var state = await SnapshotAsync(command.CommandId);
        Assert.True(changed);
        Assert.Equal("result_pending", state.Status);
        Assert.Contains("lookup", state.Attempts!, StringComparison.Ordinal);
        Assert.Contains("succeeded_local", state.Attempts!, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Explicit_blank_or_null_owner_is_rejected_without_mutation()
    {
        var command = await StoreCommandAsync();
        var before = await SnapshotAsync(command.CommandId);

        await Assert.ThrowsAsync<ArgumentNullException>(() => _store.ClaimPostAttemptAsync(command.CommandId, null!, CancellationToken.None));
        await Assert.ThrowsAsync<ArgumentException>(() => _store.ClaimLookupAttemptAsync(command.CommandId, " ", "STATUS", "pending", GenerationA, CancellationToken.None));
        await Assert.ThrowsAsync<ArgumentNullException>(() => _store.CompleteLocallyAsync(command.CommandId, null!, CommandStatus.SucceededLocal, "{}", null, null, null, CancellationToken.None));
        await Assert.ThrowsAsync<ArgumentNullException>(() => _store.MarkUnknownResultAsync(command.CommandId, null!, "STATUS", "status", null, CancellationToken.None));
        await Assert.ThrowsAsync<ArgumentException>(() => _store.ScheduleRetryAsync(command.CommandId, " ", "STATUS", "status", GenerationA, CancellationToken.None));

        Assert.Equal(before, await SnapshotAsync(command.CommandId));
    }

    [Fact]
    public async Task Foreign_resolved_attempt_cannot_be_closed_by_terminal_completion()
    {
        var first = await StoreCommandAsync();
        var second = await StoreCommandAsync();
        Assert.NotNull(await AcquireAsync(first.CommandId, "owner-first", GenerationA));
        Assert.NotNull(await AcquireAsync(second.CommandId, "owner-second", GenerationA));
        var firstAttempt = await _store.ClaimPostAttemptAsync(first.CommandId, "owner-first", CancellationToken.None);
        var secondAttempt = await _store.ClaimPostAttemptAsync(second.CommandId, "owner-second", CancellationToken.None);
        Assert.NotNull(firstAttempt);
        Assert.NotNull(secondAttempt);
        var foreignBefore = await SnapshotAsync(second.CommandId);

        var changed = await _store.CompleteLocallyAsync(first.CommandId, "owner-first", CommandStatus.SucceededLocal, "{\"status\":\"succeeded\"}", null, null, secondAttempt.AttemptId, CancellationToken.None);

        Assert.True(changed);
        Assert.Equal(foreignBefore, await SnapshotAsync(second.CommandId));
        var firstState = await SnapshotAsync(first.CommandId);
        Assert.Equal("result_pending", firstState.Status);
        Assert.DoesNotContain("succeeded_local", firstState.Attempts!, StringComparison.Ordinal);
    }

    [Fact]
    public async Task MarkExecuting_does_not_take_over_a_live_claim()
    {
        var command = await StoreCommandAsync();
        // A live claim is not stealable merely because it predates the old five-minute timeout.
        Assert.NotNull(await AcquireAsync(command.CommandId, "owner-live", DateTimeOffset.UtcNow.AddMinutes(-10)));
        var before = await SnapshotAsync(command.CommandId);

        await Assert.ThrowsAsync<InvalidOperationException>(() => _store.MarkExecutingAsync(command.CommandId, CancellationToken.None));

        Assert.Equal(before, await SnapshotAsync(command.CommandId));
    }

    [Fact]
    public async Task Owner_aware_completion_cannot_overwrite_terminal_or_acknowledged_state()
    {
        var command = await StoreCommandAsync();
        Assert.NotNull(await AcquireAsync(command.CommandId, "owner-current", DateTimeOffset.UtcNow));
        var post = await _store.ClaimPostAttemptAsync(command.CommandId, "owner-current", CancellationToken.None);
        Assert.NotNull(post);
        Assert.True(await _store.CompleteLocallyAsync(command.CommandId, "owner-current", CommandStatus.SucceededLocal, "{\"status\":\"succeeded\",\"original\":true}", "ref", "1", post.AttemptId, CancellationToken.None));
        var pending = await SnapshotAsync(command.CommandId);

        Assert.False(await _store.CompleteLocallyAsync(command.CommandId, "owner-current", CommandStatus.DeadLetter, "{\"status\":\"dead_letter\"}", null, null, post.AttemptId, CancellationToken.None));
        Assert.Equal(pending, await SnapshotAsync(command.CommandId));

        await _store.AcknowledgeResultAsync(command.CommandId, DateTimeOffset.UtcNow, CancellationToken.None);
        var acknowledged = await SnapshotAsync(command.CommandId);
        Assert.False(await _store.CompleteLocallyAsync(command.CommandId, "owner-current", CommandStatus.Cancelled, "{\"status\":\"cancelled\"}", null, null, post.AttemptId, CancellationToken.None));
        Assert.Equal(acknowledged, await SnapshotAsync(command.CommandId));
    }

    [Fact]
    public async Task MarkExecuting_records_an_owned_post_attempt_without_premature_release()
    {
        var command = await StoreCommandAsync();

        var operationalAttempt = await _store.MarkExecutingAsync(command.CommandId, CancellationToken.None);

        var state = await SnapshotAsync(command.CommandId);
        Assert.Equal(1, operationalAttempt);
        Assert.Equal("executing", state.Status);
        Assert.StartsWith("legacy:", state.Owner!, StringComparison.Ordinal);
        Assert.Equal(1, state.PostCount);
        Assert.Contains("post", state.Attempts!, StringComparison.Ordinal);
        await _store.RecoverAsync(CancellationToken.None);
        var recovered = await SnapshotAsync(command.CommandId);
        Assert.Null(recovered.Owner);
        Assert.Equal("unknown_result", recovered.Status);
        Assert.Equal(1, recovered.AttemptCount);
    }

    [Fact]
    public async Task MarkExecuting_releases_its_generated_claim_when_post_claim_fails()
    {
        var command = await StoreCommandAsync();
        await using (var connection = await _factory.OpenAsync(CancellationToken.None))
        await using (var trigger = connection.CreateCommand())
        {
            trigger.CommandText = $"CREATE TRIGGER fail_post_claim BEFORE UPDATE OF status ON commands_inbox WHEN NEW.command_id='{command.CommandId:D}' AND NEW.status='executing' BEGIN SELECT RAISE(ABORT, 'post claim failed'); END;";
            await trigger.ExecuteNonQueryAsync(CancellationToken.None);
        }

        await Assert.ThrowsAsync<SqliteException>(() => _store.MarkExecutingAsync(command.CommandId, CancellationToken.None));

        var state = await SnapshotAsync(command.CommandId);
        Assert.Equal("queued", state.Status);
        Assert.Null(state.Owner);
        Assert.Equal(0, state.PostCount);
        Assert.Null(state.Attempts);
        await using (var connection = await _factory.OpenAsync(CancellationToken.None))
        await using (var drop = connection.CreateCommand())
        {
            drop.CommandText = "DROP TRIGGER fail_post_claim";
            await drop.ExecuteNonQueryAsync(CancellationToken.None);
        }
        Assert.NotNull(await AcquireAsync(command.CommandId, "recovered-owner", DateTimeOffset.UtcNow));
    }

    [Fact]
    public async Task Service_does_not_post_after_owner_replacement_before_attempt_claim()
    {
        var command = await StoreCommandAsync();
        var onec = new FakeOnec();
        var started = 0;
        var hooks = new CommandExecutionHooks(OnExecutionStarted: (_, _, _) => started++);
        var proxy = OwnerChangingStoreProxy.Create(_store, nameof(IAgentStore.ClaimPostAttemptAsync), () => ReplaceOwnerAsync(command.CommandId, "owner-b"));
        var service = new CommandExecutionService(proxy, onec, static () => 12, hooks, executorId: "service-owner");

        await service.ProcessAsync(await ReadyAsync(command.CommandId), NeverAdministrative, CancellationToken.None);

        Assert.Equal(0, onec.ExecuteCalls);
        Assert.Equal(0, started);
        Assert.Equal("owner-b", await ClaimOwnerAsync(command.CommandId));
    }

    [Fact]
    public async Task Service_does_not_get_after_owner_replacement_before_lookup_claim()
    {
        var command = await StoreCommandAsync();
        var post = await _store.ClaimPostAttemptAsync(command.CommandId, CancellationToken.None);
        Assert.NotNull(post);
        await _store.CompleteAttemptAsync(post.AttemptId, CommandStatus.UnknownResult, "UNKNOWN", "unknown", null, CancellationToken.None);
        Assert.True(await _store.MarkUnknownResultAsync(command.CommandId, post.AttemptId, "UNKNOWN", "unknown", DateTimeOffset.UtcNow.AddMinutes(-1), CancellationToken.None));
        var onec = new FakeOnec();
        var proxy = OwnerChangingStoreProxy.Create(_store, nameof(IAgentStore.ClaimLookupAttemptAsync), () => ReplaceOwnerAsync(command.CommandId, "owner-b"));
        var service = new CommandExecutionService(proxy, onec, static () => 12, executorId: "lookup-owner");

        await service.ProcessAsync(await ReadyAsync(command.CommandId), NeverAdministrative, CancellationToken.None);

        Assert.Equal(0, onec.StatusCalls);
        Assert.Equal("owner-b", await ClaimOwnerAsync(command.CommandId));
        var state = await SnapshotAsync(command.CommandId);
        Assert.Equal(0, state.LookupCount);
        Assert.Null(state.Outbox);
    }

    [Fact]
    public async Task Service_does_not_fire_completion_hook_after_owner_replacement()
    {
        var command = await StoreCommandAsync();
        var notifications = 0;
        var hooks = new CommandExecutionHooks(OnExecutionSucceeded: (_, _) => { notifications++; return Task.CompletedTask; });
        var onec = new FakeOnec();
        var proxy = OwnerChangingStoreProxy.Create(_store, nameof(IAgentStore.CompleteLocallyAsync), () => ReplaceOwnerAfterAttemptAsync(command.CommandId));
        var service = new CommandExecutionService(proxy, onec, static () => 12, hooks, executorId: "completion-owner");

        await service.ProcessAsync(await ReadyAsync(command.CommandId), NeverAdministrative, CancellationToken.None);

        Assert.Equal(1, onec.ExecuteCalls);
        Assert.Equal(0, notifications);
        Assert.Equal("owner-b", await ClaimOwnerAsync(command.CommandId));
        Assert.Empty(await _store.GetPendingResultsAsync(10, DateTimeOffset.UtcNow, CancellationToken.None));
    }

    private async Task ReplaceOwnerAsync(Guid commandId, string owner)
    {
        var currentOwner = await ClaimOwnerAsync(commandId);
        Assert.NotNull(currentOwner);
        await _store.ReleaseCommandExecutionClaimAsync(commandId, currentOwner!, CancellationToken.None);
        Assert.NotNull(await AcquireAsync(commandId, owner, DateTimeOffset.UtcNow));
    }

    private async Task ReplaceOwnerAfterAttemptAsync(Guid commandId)
    {
        var currentOwner = await ClaimOwnerAsync(commandId);
        Assert.NotNull(currentOwner);
        Assert.True(await _store.MarkUnknownResultAsync(commandId, currentOwner!, "OWNER_LOST", "owner lost", DateTimeOffset.UtcNow.AddMinutes(-1), CancellationToken.None));
        Assert.NotNull(await AcquireAsync(commandId, "owner-b", DateTimeOffset.UtcNow));
    }

    private async Task<CommandEnvelope> StoreCommandAsync()
    {
        var command = MakeCommand();
        Assert.Equal(StoreCommandOutcome.Stored, await _store.StoreCommandAsync(command, GenerationA, CancellationToken.None));
        return command;
    }

    private async Task<ExecutionClaim?> AcquireAsync(Guid commandId, string owner, DateTimeOffset acquiredAt) =>
        await _store.TryAcquireCommandExecutionClaimAsync(commandId, owner, acquiredAt, DateTimeOffset.MinValue, CancellationToken.None);

    private async Task<string?> ClaimOwnerAsync(Guid commandId)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT exec_claim_owner_id FROM commands_inbox WHERE command_id=$id";
        command.Parameters.AddWithValue("$id", commandId.ToString("D"));
        return (string?)await command.ExecuteScalarAsync(CancellationToken.None);
    }

    private async Task<StoredCommand> ReadyAsync(Guid commandId) =>
        (await _store.GetReadyCommandsAsync(10, DateTimeOffset.UtcNow.AddDays(1), CancellationToken.None)).Single(command => command.Envelope.CommandId == commandId);

    private async Task<Snapshot> SnapshotAsync(Guid commandId)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        string? owner;
        string status;
        int attemptCount;
        int lookupCount;
        int postCount;
        long rowVersion;
        await using (var command = connection.CreateCommand())
        {
            command.CommandText = "SELECT status,exec_claim_owner_id,attempt_count,lookup_attempt_count,post_attempt_count,row_version FROM commands_inbox WHERE command_id=$id";
            command.Parameters.AddWithValue("$id", commandId.ToString("D"));
            await using var reader = await command.ExecuteReaderAsync(CancellationToken.None);
            Assert.True(await reader.ReadAsync(CancellationToken.None));
            status = reader.GetString(0);
            owner = reader.IsDBNull(1) ? null : reader.GetString(1);
            attemptCount = reader.GetInt32(2);
            lookupCount = reader.GetInt32(3);
            postCount = reader.GetInt32(4);
            rowVersion = reader.GetInt64(5);
        }

        var attempts = new StringBuilder();
        await using (var command = connection.CreateCommand())
        {
            command.CommandText = "SELECT attempt_id,attempt_no,attempt_kind,started_at_utc,finished_at_utc,request_hash,http_status,outcome,error_code,error_message,duration_ms FROM command_attempts WHERE command_id=$id ORDER BY attempt_no";
            command.Parameters.AddWithValue("$id", commandId.ToString("D"));
            await using var reader = await command.ExecuteReaderAsync(CancellationToken.None);
            while (await reader.ReadAsync(CancellationToken.None)) attempts.Append(string.Join("|", Enumerable.Range(0, reader.FieldCount).Select(index => Value(reader, index)))).Append(';');
        }

        string? outbox;
        await using (var command = connection.CreateCommand())
        {
            command.CommandText = "SELECT result_id,payload_json,payload_hash,status,attempt_count,next_attempt_at_utc,created_at_utc,sent_at_utc,acknowledged_at_utc,last_error FROM results_outbox WHERE command_id=$id";
            command.Parameters.AddWithValue("$id", commandId.ToString("D"));
            await using var reader = await command.ExecuteReaderAsync(CancellationToken.None);
            outbox = await reader.ReadAsync(CancellationToken.None) ? string.Join("|", Enumerable.Range(0, reader.FieldCount).Select(index => Value(reader, index))) : null;
        }

        return new(status, owner, attemptCount, lookupCount, postCount, rowVersion, attempts.Length == 0 ? null : attempts.ToString(), outbox);
    }

    private static string Value(SqliteDataReader reader, int ordinal) => reader.IsDBNull(ordinal) ? "<null>" : Convert.ToString(reader.GetValue(ordinal), System.Globalization.CultureInfo.InvariantCulture) ?? "<null>";

    private static Task<bool> NeverAdministrative(CommandEnvelope _, string __, CancellationToken ___) => Task.FromResult(false);

    private static CommandEnvelope MakeCommand()
    {
        using var document = System.Text.Json.JsonDocument.Parse("{\"amount\":10}");
        var payload = document.RootElement.Clone();
        return new(Guid.NewGuid(), "create_customer_order", 1, 100, "order:" + Guid.NewGuid().ToString("N"), null, DateTimeOffset.UtcNow, null, null, null, PayloadHasher.Compute(payload), payload);
    }

    private sealed class FakeOnec : IOnecCommandClient
    {
        public int ExecuteCalls { get; private set; }
        public int StatusCalls { get; private set; }

        public Task<OnecExecutionResult> ExecuteAsync(CommandEnvelope command, CancellationToken cancellationToken)
        {
            ExecuteCalls++;
            return Task.FromResult(new OnecExecutionResult(OnecExecutionKind.Succeeded, new(command.CommandId, "succeeded", System.Text.Json.JsonSerializer.SerializeToElement(new { @ref = "ref" }), null, [], 1), 200, null, null));
        }

        public Task<OnecExecutionResult> GetStatusAsync(Guid commandId, CancellationToken cancellationToken)
        {
            StatusCalls++;
            return Task.FromResult(new OnecExecutionResult(OnecExecutionKind.Processing, null, 202, null, null));
        }
    }

    public class OwnerChangingStoreProxy : DispatchProxy
    {
        private static readonly ConditionalWeakTable<DispatchProxy, ProxyState> States = new();
        private int _changed;

        public static IAgentStore Create(SqliteAgentStore inner, string methodName, Func<Task> beforeMethod)
        {
            var proxy = DispatchProxy.Create<IAgentStore, OwnerChangingStoreProxy>();
            States.Add((DispatchProxy)(object)proxy, new(inner, methodName, beforeMethod));
            return proxy;
        }

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            if (!States.TryGetValue(this, out var state)) throw new InvalidOperationException("Proxy state is missing.");
            if (targetMethod?.Name == state.MethodName && Interlocked.Exchange(ref _changed, 1) == 0)
            {
                state.BeforeMethod().GetAwaiter().GetResult();
            }
            return targetMethod!.Invoke(state.Inner, args);
        }

        private sealed record ProxyState(SqliteAgentStore Inner, string MethodName, Func<Task> BeforeMethod);
    }

    private sealed record Snapshot(string Status, string? Owner, int AttemptCount, int LookupCount, int PostCount, long RowVersion, string? Attempts, string? Outbox);
}
