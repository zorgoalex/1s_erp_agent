using System.Globalization;
using System.Text.Json;
using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Application.Commands;
using ErpOnecAgent.Domain.Commands;
using ErpOnecAgent.Domain.Common;
using ErpOnecAgent.Infrastructure.Persistence.Sqlite;
using Microsoft.Data.Sqlite;
using Xunit;

namespace ErpOnecAgent.IntegrationTests;

public sealed class CancellationStateGuardTests : IAsyncLifetime
{
    private const string CancellationResultJson = "{\"status\":\"cancelled\",\"reason\":\"requested\"}";
    private static readonly DateTimeOffset NoTimeTakeover = DateTimeOffset.MinValue;
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
    public async Task Unclaimed_never_sent_queued_command_can_be_cancelled_once()
    {
        var command = await StoreCommandAsync();

        Assert.True(await _store.CompleteLocallyAsync(command.CommandId, CommandStatus.Cancelled, CancellationResultJson, null, null, null, CancellationToken.None));

        var state = await SnapshotAsync(command.CommandId);
        var pending = Assert.Single(await _store.GetPendingResultsAsync(10, DateTimeOffset.UtcNow, CancellationToken.None));
        Assert.Equal("result_pending", await StringAsync(command.CommandId, "status"));
        Assert.Equal("cancelled", await StringAsync(command.CommandId, "result_status"));
        Assert.Equal(CancellationResultJson, pending.PayloadJson);
        Assert.Single(state.OutboxRows);
        Assert.Empty(state.AttemptRows);
        Assert.Equal("0", await StringAsync(command.CommandId, "attempt_count"));
        Assert.Equal("0", await StringAsync(command.CommandId, "post_attempt_count"));
        Assert.Null(await StringAsync(command.CommandId, "exec_claim_owner_id"));
    }

    [Fact]
    public async Task Exact_owner_can_cancel_claimed_queued_command_before_post()
    {
        var command = await StoreCommandAsync();
        const string owner = "cancel-before-post-owner";
        Assert.NotNull(await AcquireAsync(command.CommandId, owner));

        Assert.True(await _store.CompleteLocallyAsync(command.CommandId, owner, CommandStatus.Cancelled, CancellationResultJson, null, null, null, CancellationToken.None));

        var state = await SnapshotAsync(command.CommandId);
        Assert.Equal("result_pending", await StringAsync(command.CommandId, "status"));
        Assert.Equal("cancelled", await StringAsync(command.CommandId, "result_status"));
        Assert.Null(await StringAsync(command.CommandId, "exec_claim_owner_id"));
        Assert.Single(state.OutboxRows);
        Assert.Empty(state.AttemptRows);
    }

    [Fact]
    public async Task Never_sent_retry_waiting_command_can_be_cancelled()
    {
        var command = await StoreCommandAsync();
        Assert.True(await _store.ScheduleRetryAsync(command.CommandId, "RETRY_BEFORE_SEND", "retry before send", DateTimeOffset.UtcNow.AddMinutes(1), CancellationToken.None));

        Assert.True(await _store.CompleteLocallyAsync(command.CommandId, CommandStatus.Cancelled, CancellationResultJson, null, null, null, CancellationToken.None));

        Assert.Equal("result_pending", await StringAsync(command.CommandId, "status"));
        Assert.Equal("cancelled", await StringAsync(command.CommandId, "result_status"));
        Assert.Single(await _store.GetPendingResultsAsync(10, DateTimeOffset.UtcNow, CancellationToken.None));
        Assert.Empty((await SnapshotAsync(command.CommandId)).AttemptRows);
    }

    [Fact]
    public async Task Ownerless_cancellation_cannot_mutate_claimed_queued_command()
    {
        var command = await StoreCommandAsync();
        const string owner = "claimed-cancel-owner";
        Assert.NotNull(await AcquireAsync(command.CommandId, owner));
        var before = await SnapshotAsync(command.CommandId);

        Assert.False(await _store.CompleteLocallyAsync(command.CommandId, CommandStatus.Cancelled, CancellationResultJson, null, null, null, CancellationToken.None));

        AssertSnapshotUnchanged(before, await SnapshotAsync(command.CommandId));
    }

    [Fact]
    public async Task Cancellation_after_post_attempt_is_refused_and_preserves_all_persisted_state()
    {
        var command = await StoreCommandAsync();
        const string owner = "post-cancel-owner";
        Assert.NotNull(await AcquireAsync(command.CommandId, owner));
        var post = await _store.ClaimPostAttemptAsync(command.CommandId, owner, CancellationToken.None);
        Assert.NotNull(post);
        var before = await SnapshotAsync(command.CommandId);

        var changed = await _store.CompleteLocallyAsync(command.CommandId, owner, CommandStatus.Cancelled, CancellationResultJson, null, null, post.AttemptId, CancellationToken.None);
        var after = await SnapshotAsync(command.CommandId);

        Assert.False(changed);
        AssertSnapshotUnchanged(before, after);
    }

    [Fact]
    public async Task Cancellation_after_recovered_unknown_post_is_refused_and_preserves_all_persisted_state()
    {
        var command = await StoreCommandAsync();
        const string postOwner = "recovered-post-owner";
        Assert.NotNull(await AcquireAsync(command.CommandId, postOwner));
        var post = await _store.ClaimPostAttemptAsync(command.CommandId, postOwner, CancellationToken.None);
        Assert.NotNull(post);
        await _store.RecoverAsync(CancellationToken.None);
        var before = await SnapshotAsync(command.CommandId);

        var changed = await _store.CompleteLocallyAsync(command.CommandId, CommandStatus.Cancelled, CancellationResultJson, null, null, post.AttemptId, CancellationToken.None);
        var after = await SnapshotAsync(command.CommandId);

        Assert.False(changed);
        AssertSnapshotUnchanged(before, after);
    }

    [Fact]
    public async Task Unresolved_unknown_with_zero_send_counters_is_not_cancellable()
    {
        var command = await StoreCommandAsync();
        Assert.True(await _store.MarkUnknownResultAsync(command.CommandId, "UNHANDLED_EXECUTION_ERROR", "outcome unknown", DateTimeOffset.UtcNow.AddMinutes(-1), CancellationToken.None));
        var before = await SnapshotAsync(command.CommandId);

        var changed = await _store.CompleteLocallyAsync(command.CommandId, CommandStatus.Cancelled, CancellationResultJson, null, null, null, CancellationToken.None);
        var after = await SnapshotAsync(command.CommandId);

        Assert.False(changed);
        AssertSnapshotUnchanged(before, after);
    }

    [Fact]
    public async Task Not_found_resolution_after_send_is_not_cancellable()
    {
        var command = await StoreCommandAsync();
        const string postOwner = "not-found-post-owner";
        Assert.NotNull(await AcquireAsync(command.CommandId, postOwner));
        var post = await _store.ClaimPostAttemptAsync(command.CommandId, postOwner, CancellationToken.None);
        Assert.NotNull(post);
        await _store.CompleteAttemptAsync(post.AttemptId, CommandStatus.UnknownResult, "UNKNOWN_RESULT", "1C result is unknown", null, CancellationToken.None);
        Assert.True(await _store.MarkUnknownResultAsync(command.CommandId, post.AttemptId, postOwner, "UNKNOWN_RESULT", "1C result is unknown", DateTimeOffset.UtcNow.AddMinutes(-1), CancellationToken.None));

        const string lookupOwner = "not-found-lookup-owner";
        Assert.NotNull(await AcquireAsync(command.CommandId, lookupOwner));
        var lookup = await _store.ClaimLookupAttemptAsync(command.CommandId, lookupOwner, "NOT_FOUND", "1C returned not found", DateTimeOffset.UtcNow.AddMinutes(1), CancellationToken.None);
        Assert.NotNull(lookup);
        await _store.CompleteAttemptAsync(lookup.AttemptId, CommandStatus.UnknownResult, "NOT_FOUND", "1C returned not found", 404, CancellationToken.None);
        Assert.True(await _store.MarkUnknownResultAsync(command.CommandId, lookup.AttemptId, lookupOwner, "NOT_FOUND", "1C returned not found", DateTimeOffset.UtcNow.AddMinutes(-1), CancellationToken.None));
        var before = await SnapshotAsync(command.CommandId);

        var changed = await _store.CompleteLocallyAsync(command.CommandId, CommandStatus.Cancelled, CancellationResultJson, null, null, lookup.AttemptId, CancellationToken.None);
        var after = await SnapshotAsync(command.CommandId);

        Assert.False(changed);
        AssertSnapshotUnchanged(before, after);
    }

    [Fact]
    public async Task Queued_row_with_durable_first_sent_evidence_is_not_cancellable()
    {
        var command = await StoreCommandAsync();
        var post = await _store.ClaimPostAttemptAsync(command.CommandId, CancellationToken.None);
        Assert.NotNull(post);
        await using (var connection = await _factory.OpenAsync(CancellationToken.None))
        await using (var staleState = connection.CreateCommand())
        {
            staleState.CommandText = "UPDATE commands_inbox SET status='queued',started_at_utc=NULL,attempt_count=0,post_attempt_count=0 WHERE command_id=$id";
            staleState.Parameters.AddWithValue("$id", command.CommandId.ToString("D"));
            await staleState.ExecuteNonQueryAsync(CancellationToken.None);
        }
        var before = await SnapshotAsync(command.CommandId);

        var changed = await _store.CompleteLocallyAsync(command.CommandId, CommandStatus.Cancelled, CancellationResultJson, null, null, post.AttemptId, CancellationToken.None);
        var after = await SnapshotAsync(command.CommandId);

        Assert.False(changed);
        AssertSnapshotUnchanged(before, after);
    }

    [Fact]
    public async Task Future_not_before_command_can_be_cancelled_before_execution()
    {
        var now = DateTimeOffset.UtcNow;
        var command = await StoreCommandAsync(MakeCommand(now.AddHours(1), now.AddHours(2)));
        Assert.Empty(await _store.GetReadyCommandsAsync(10, now, CancellationToken.None));

        Assert.True(await _store.CompleteLocallyAsync(command.CommandId, CommandStatus.Cancelled, CancellationResultJson, null, null, null, CancellationToken.None));

        Assert.Equal("result_pending", await StringAsync(command.CommandId, "status"));
        Assert.Equal("cancelled", await StringAsync(command.CommandId, "result_status"));
        Assert.Single(await _store.GetPendingResultsAsync(10, DateTimeOffset.UtcNow.AddMinutes(1), CancellationToken.None));
    }

    [Fact]
    public async Task Nonexistent_cancellation_is_a_noop()
    {
        var commandId = Guid.NewGuid();
        var before = await SnapshotAsync(commandId);

        Assert.False(await _store.CompleteLocallyAsync(commandId, CommandStatus.Cancelled, CancellationResultJson, null, null, Guid.NewGuid(), CancellationToken.None));

        AssertSnapshotUnchanged(before, await SnapshotAsync(commandId));
    }

    [Fact]
    public async Task Terminal_cancellation_is_a_noop()
    {
        var command = await StoreCommandAsync();
        const string owner = "terminal-normal-owner";
        Assert.NotNull(await AcquireAsync(command.CommandId, owner));
        var post = await _store.ClaimPostAttemptAsync(command.CommandId, owner, CancellationToken.None);
        Assert.NotNull(post);
        const string successResult = "{\"status\":\"succeeded\"}";
        Assert.True(await _store.CompleteLocallyAsync(command.CommandId, owner, CommandStatus.SucceededLocal, successResult, "ref-1", "42", post.AttemptId, CancellationToken.None));
        var before = await SnapshotAsync(command.CommandId);

        Assert.False(await _store.CompleteLocallyAsync(command.CommandId, CommandStatus.Cancelled, CancellationResultJson, "cancel-ref", "cancel-number", post.AttemptId, CancellationToken.None));

        AssertSnapshotUnchanged(before, await SnapshotAsync(command.CommandId));
    }

    [Fact]
    public async Task Duplicate_cancellation_is_a_noop()
    {
        var command = await StoreCommandAsync();
        Assert.True(await _store.CompleteLocallyAsync(command.CommandId, CommandStatus.Cancelled, CancellationResultJson, null, null, null, CancellationToken.None));
        var before = await SnapshotAsync(command.CommandId);

        Assert.False(await _store.CompleteLocallyAsync(command.CommandId, CommandStatus.Cancelled, CancellationResultJson, "late-ref", "late-number", Guid.NewGuid(), CancellationToken.None));

        AssertSnapshotUnchanged(before, await SnapshotAsync(command.CommandId));
    }

    [Fact]
    public async Task Cancellation_racing_execution_claim_never_claims_cancelled_after_post_claim()
    {
        var command = await StoreCommandAsync();
        const string owner = "cancel-race-owner";
        var cancellationReady = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var executionReady = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var start = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var cancellation = Task.Run(async () =>
        {
            cancellationReady.SetResult(true);
            await start.Task;
            return await _store.CompleteLocallyAsync(command.CommandId, CommandStatus.Cancelled, CancellationResultJson, null, null, null, CancellationToken.None);
        });
        var execution = Task.Run(async () =>
        {
            executionReady.SetResult(true);
            await start.Task;
            var claim = await AcquireAsync(command.CommandId, owner);
            if (claim is null) return new PostClaimRace(null, null);
            var post = await _store.ClaimPostAttemptAsync(command.CommandId, owner, CancellationToken.None);
            return new PostClaimRace(claim, post);
        });
        await Task.WhenAll(cancellationReady.Task, executionReady.Task).WaitAsync(GateTimeout);
        start.SetResult(true);
        var cancelled = await cancellation.WaitAsync(GateTimeout);
        var race = await execution.WaitAsync(GateTimeout);
        var state = await SnapshotAsync(command.CommandId);

        if (race.Post is null)
        {
            Assert.True(cancelled);
            Assert.Null(race.Claim);
            Assert.Equal("result_pending", await StringAsync(command.CommandId, "status"));
            Assert.Equal("cancelled", await StringAsync(command.CommandId, "result_status"));
            Assert.Single(state.OutboxRows);
            Assert.Empty(state.AttemptRows);
        }
        else
        {
            Assert.False(cancelled);
            Assert.NotNull(race.Claim);
            Assert.Equal("executing", await StringAsync(command.CommandId, "status"));
            Assert.Null(await StringAsync(command.CommandId, "result_status"));
            Assert.Empty(state.OutboxRows);
            Assert.Single(state.AttemptRows);
            Assert.Equal("1", await StringAsync(command.CommandId, "post_attempt_count"));
        }
    }

    [Fact]
    public async Task Normal_successful_completion_remains_available()
    {
        var command = await StoreCommandAsync();
        const string owner = "normal-completion-owner";
        Assert.NotNull(await AcquireAsync(command.CommandId, owner));
        var post = await _store.ClaimPostAttemptAsync(command.CommandId, owner, CancellationToken.None);
        Assert.NotNull(post);
        const string successResult = "{\"status\":\"succeeded\"}";

        Assert.True(await _store.CompleteLocallyAsync(command.CommandId, owner, CommandStatus.SucceededLocal, successResult, "ref-1", "42", post.AttemptId, CancellationToken.None));

        var pending = Assert.Single(await _store.GetPendingResultsAsync(10, DateTimeOffset.UtcNow, CancellationToken.None));
        Assert.Equal(successResult, pending.PayloadJson);
        Assert.Equal("succeeded_local", await StringAsync(command.CommandId, "result_status"));
        Assert.Single((await SnapshotAsync(command.CommandId)).OutboxRows);
    }

    private async Task<CommandEnvelope> StoreCommandAsync(CommandEnvelope? command = null)
    {
        command ??= MakeCommand();
        Assert.Equal(StoreCommandOutcome.Stored, await _store.StoreCommandAsync(command, DateTimeOffset.UtcNow, CancellationToken.None));
        return command;
    }

    private Task<ExecutionClaim?> AcquireAsync(Guid commandId, string owner) =>
        _store.TryAcquireCommandExecutionClaimAsync(commandId, owner, DateTimeOffset.UtcNow, NoTimeTakeover, CancellationToken.None);

    private async Task<string?> StringAsync(Guid commandId, string column)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = $"SELECT {column} FROM commands_inbox WHERE command_id=$id";
        command.Parameters.AddWithValue("$id", commandId.ToString("D"));
        var value = await command.ExecuteScalarAsync(CancellationToken.None);
        return value is null or DBNull ? null : Convert.ToString(value, CultureInfo.InvariantCulture);
    }

    private async Task<PersistenceSnapshot> SnapshotAsync(Guid commandId)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        var commands = await ReadRowsAsync(connection, "SELECT * FROM commands_inbox WHERE command_id=$id ORDER BY command_id", commandId);
        var outbox = await ReadRowsAsync(connection, "SELECT * FROM results_outbox WHERE command_id=$id ORDER BY result_id", commandId);
        var attempts = await ReadRowsAsync(connection, "SELECT * FROM command_attempts WHERE command_id=$id ORDER BY attempt_no,attempt_id", commandId);
        return new(commands, outbox, attempts);
    }

    private static async Task<string[]> ReadRowsAsync(SqliteConnection connection, string sql, Guid commandId)
    {
        var rows = new List<string>();
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.Parameters.AddWithValue("$id", commandId.ToString("D"));
        await using var reader = await command.ExecuteReaderAsync(CancellationToken.None);
        while (await reader.ReadAsync(CancellationToken.None))
        {
            var fields = new string[reader.FieldCount];
            for (var index = 0; index < reader.FieldCount; index++)
            {
                var value = reader.IsDBNull(index) ? null : reader.GetValue(index);
                fields[index] = $"{reader.GetName(index)}={(value is null ? "<null>" : Convert.ToString(value, CultureInfo.InvariantCulture))}";
            }
            rows.Add(string.Join('', fields));
        }
        return rows.ToArray();
    }

    private static void AssertSnapshotUnchanged(PersistenceSnapshot expected, PersistenceSnapshot actual)
    {
        Assert.Equal(expected.CommandRows, actual.CommandRows);
        Assert.Equal(expected.OutboxRows, actual.OutboxRows);
        Assert.Equal(expected.AttemptRows, actual.AttemptRows);
    }

    private static CommandEnvelope MakeCommand(DateTimeOffset? notBeforeUtc = null, DateTimeOffset? expiresAtUtc = null)
    {
        using var document = JsonDocument.Parse("{\"amount\":10}");
        var payload = document.RootElement.Clone();
        var now = DateTimeOffset.UtcNow;
        return new(Guid.NewGuid(), "create_customer_order", 1, 100, "order:" + Guid.NewGuid().ToString("N"), null, now, notBeforeUtc, expiresAtUtc ?? now.AddHours(1), null, PayloadHasher.Compute(payload), payload);
    }

    private sealed record PersistenceSnapshot(string[] CommandRows, string[] OutboxRows, string[] AttemptRows);

    private sealed record PostClaimRace(ExecutionClaim? Claim, CommandAttemptId? Post);
}
