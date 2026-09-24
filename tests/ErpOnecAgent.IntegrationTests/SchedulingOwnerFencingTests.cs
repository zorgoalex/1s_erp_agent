using System.Globalization;
using System.Text;
using System.Text.Json;
using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Application.Commands;
using ErpOnecAgent.Contracts.OneC;
using ErpOnecAgent.Domain.Commands;
using ErpOnecAgent.Domain.Common;
using ErpOnecAgent.Infrastructure.Persistence.Sqlite;
using Microsoft.Data.Sqlite;
using Xunit;

namespace ErpOnecAgent.IntegrationTests;

public sealed class SchedulingOwnerFencingTests : IAsyncLifetime
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
    public async Task Released_generation_a_cannot_apply_mark_unknown_to_generation_b()
    {
        var command = await StoreCommandAsync();
        Assert.NotNull(await AcquireAsync(command, "owner-a", GenerationA));
        await _store.ReleaseCommandExecutionClaimAsync(command.CommandId, "owner-a", CancellationToken.None);
        Assert.NotNull(await AcquireAsync(command, "owner-b", GenerationB));
        var attemptB = await _store.ClaimPostAttemptAsync(command.CommandId, "owner-b", CancellationToken.None);
        Assert.NotNull(attemptB);
        var before = await SnapshotAsync(command.CommandId);

        var changed = await _store.MarkUnknownResultAsync(command.CommandId, attemptB.AttemptId, "owner-a", "STALE_UNKNOWN", "stale unknown", GenerationB.AddHours(1), CancellationToken.None);

        Assert.False(changed);
        Assert.Equal(before, await SnapshotAsync(command.CommandId));
    }

    [Fact]
    public async Task Wrong_generation_cannot_apply_scheduling_only_mark_unknown_or_retry()
    {
        var command = await StoreCommandAsync();
        Assert.NotNull(await AcquireAsync(command, "owner-b", GenerationB));
        Assert.NotNull(await _store.ClaimPostAttemptAsync(command.CommandId, "owner-b", CancellationToken.None));
        var before = await SnapshotAsync(command.CommandId);

        var unknownChanged = await _store.MarkUnknownResultAsync(command.CommandId, "owner-a", "STALE_UNKNOWN", "stale unknown", GenerationB.AddHours(1), CancellationToken.None);
        var retryChanged = await _store.ScheduleRetryAsync(command.CommandId, "owner-a", "STALE_RETRY", "stale retry", GenerationB.AddHours(2), CancellationToken.None);

        Assert.False(unknownChanged);
        Assert.False(retryChanged);
        Assert.Equal(before, await SnapshotAsync(command.CommandId));
    }

    [Fact]
    public async Task Worker_legacy_catch_boundary_is_unclaimed_only()
    {
        var command = await StoreCommandAsync();
        Assert.NotNull(await AcquireAsync(command, "owner-b", GenerationB));
        var attempt = await _store.ClaimPostAttemptAsync(command.CommandId, "owner-b", CancellationToken.None);
        Assert.NotNull(attempt);
        var before = await SnapshotAsync(command.CommandId);

        var attemptChanged = await _store.MarkUnknownResultAsync(command.CommandId, attempt.AttemptId, "LEGACY_UNKNOWN", "legacy unknown", GenerationB.AddHours(1), CancellationToken.None);
        var schedulingChanged = await _store.MarkUnknownResultAsync(command.CommandId, "LEGACY_UNKNOWN", "legacy unknown", GenerationB.AddHours(1), CancellationToken.None);
        var retryChanged = await _store.ScheduleRetryAsync(command.CommandId, "LEGACY_RETRY", "legacy retry", GenerationB.AddHours(2), CancellationToken.None);

        Assert.False(attemptChanged);
        Assert.False(schedulingChanged);
        Assert.False(retryChanged);
        Assert.Equal(before, await SnapshotAsync(command.CommandId));
    }

    [Fact]
    public async Task Legacy_scheduling_call_remains_valid_for_an_unclaimed_active_row()
    {
        var command = await StoreCommandAsync();
        var unknownAt = GenerationA.AddHours(1);
        var unknownChanged = await _store.MarkUnknownResultAsync(command.CommandId, "LEGACY_UNKNOWN", "legacy unknown", unknownAt, CancellationToken.None);
        var unknownState = await SnapshotAsync(command.CommandId);
        Assert.True(unknownChanged);
        Assert.Equal("unknown_result", unknownState.Status);
        Assert.Null(unknownState.ClaimOwner);

        var retryAt = GenerationA.AddHours(2);
        var retryChanged = await _store.ScheduleRetryAsync(command.CommandId, "LEGACY_RETRY", "legacy retry", retryAt, CancellationToken.None);
        var retryState = await SnapshotAsync(command.CommandId);
        Assert.True(retryChanged);
        Assert.Equal("retry_waiting", retryState.Status);
        Assert.Equal(retryAt.ToUniversalTime().ToString("O"), retryState.NextAttemptAtUtc);
    }

    [Fact]
    public async Task Released_owner_cannot_mutate_an_unclaimed_active_row()
    {
        var command = await StoreCommandAsync();
        Assert.NotNull(await AcquireAsync(command, "owner-a", GenerationA));
        var attempt = await _store.ClaimPostAttemptAsync(command.CommandId, "owner-a", CancellationToken.None);
        Assert.NotNull(attempt);
        await _store.ReleaseCommandExecutionClaimAsync(command.CommandId, "owner-a", CancellationToken.None);
        var before = await SnapshotAsync(command.CommandId);

        var unknownChanged = await _store.MarkUnknownResultAsync(command.CommandId, attempt.AttemptId, "owner-a", "RELEASED_UNKNOWN", "released unknown", GenerationA.AddHours(1), CancellationToken.None);
        var retryChanged = await _store.ScheduleRetryAsync(command.CommandId, "owner-a", "RELEASED_RETRY", "released retry", GenerationA.AddHours(2), CancellationToken.None);

        Assert.False(unknownChanged);
        Assert.False(retryChanged);
        Assert.Equal(before, await SnapshotAsync(command.CommandId));
    }

    [Fact]
    public async Task Matching_owner_can_schedule_and_release_mark_unknown()
    {
        var command = await StoreCommandAsync();
        Assert.NotNull(await AcquireAsync(command, "owner-a", GenerationA));
        var attempt = await _store.ClaimPostAttemptAsync(command.CommandId, "owner-a", CancellationToken.None);
        Assert.NotNull(attempt);
        var before = await SnapshotAsync(command.CommandId);
        var retryAt = GenerationB.AddHours(1);

        var changed = await _store.MarkUnknownResultAsync(command.CommandId, attempt.AttemptId, "owner-a", "UNKNOWN", "unknown", retryAt, CancellationToken.None);

        var after = await SnapshotAsync(command.CommandId);
        Assert.True(changed);
        Assert.Equal("unknown_result", after.Status);
        Assert.Null(after.ClaimOwner);
        Assert.Null(after.ClaimAcquiredAtUtc);
        Assert.Equal(retryAt.ToUniversalTime().ToString("O"), after.NextAttemptAtUtc);
        Assert.Equal("UNKNOWN", after.LastErrorCode);
        Assert.Equal("unknown", after.LastErrorMessage);
        Assert.Equal(before.AttemptCount, after.AttemptCount);
        Assert.Equal(before.LookupAttemptCount, after.LookupAttemptCount);
        Assert.Equal(before.PostAttemptCount, after.PostAttemptCount);
        Assert.Equal(before.FirstSentAtUtc, after.FirstSentAtUtc);
        Assert.Equal(before.AttemptSignature, after.AttemptSignature);
        Assert.Equal(before.OutboxSignature, after.OutboxSignature);
        Assert.Equal(before.RowVersion + 1, after.RowVersion);
    }

    [Fact]
    public async Task Matching_owner_can_schedule_retry_and_release_its_claim()
    {
        var command = await StoreCommandAsync();
        Assert.NotNull(await AcquireAsync(command, "owner-b", GenerationB));
        var before = await SnapshotAsync(command.CommandId);
        var retryAt = GenerationB.AddHours(1);

        var changed = await _store.ScheduleRetryAsync(command.CommandId, "owner-b", "TECHNICAL", "transient", retryAt, CancellationToken.None);

        var after = await SnapshotAsync(command.CommandId);
        Assert.True(changed);
        Assert.Equal("retry_waiting", after.Status);
        Assert.Null(after.ClaimOwner);
        Assert.Null(after.ClaimAcquiredAtUtc);
        Assert.Equal(retryAt.ToUniversalTime().ToString("O"), after.NextAttemptAtUtc);
        Assert.Equal("TECHNICAL", after.LastErrorCode);
        Assert.Equal("transient", after.LastErrorMessage);
        Assert.Equal(before.AttemptCount, after.AttemptCount);
        Assert.Equal(before.LookupAttemptCount, after.LookupAttemptCount);
        Assert.Equal(before.PostAttemptCount, after.PostAttemptCount);
        Assert.Equal(before.FirstSentAtUtc, after.FirstSentAtUtc);
        Assert.Equal(before.AttemptSignature, after.AttemptSignature);
        Assert.Equal(before.OutboxSignature, after.OutboxSignature);
        Assert.Equal(before.RowVersion + 1, after.RowVersion);
    }

    [Fact]
    public async Task All_scheduling_variants_are_noop_after_terminal_completion_and_erp_ack()
    {
        var command = await StoreCommandAsync();
        Assert.NotNull(await AcquireAsync(command, "owner-b", GenerationB));
        var attempt = await _store.ClaimPostAttemptAsync(command.CommandId, "owner-b", CancellationToken.None);
        Assert.NotNull(attempt);
        Assert.True(await _store.CompleteLocallyAsync(command.CommandId, "owner-b", CommandStatus.SucceededLocal, "{\"status\":\"succeeded\",\"marker\":\"terminal\"}", "ref", "1", attempt.AttemptId, CancellationToken.None));
        var terminal = await SnapshotAsync(command.CommandId);

        await AssertAllSchedulingVariantsNoopAsync(command.CommandId, attempt.AttemptId, terminal);

        await _store.AcknowledgeResultAsync(command.CommandId, GenerationB.AddHours(3), CancellationToken.None);
        var acknowledged = await SnapshotAsync(command.CommandId);
        await AssertAllSchedulingVariantsNoopAsync(command.CommandId, attempt.AttemptId, acknowledged);
    }

    [Fact]
    public async Task Ambiguous_executor_post_persists_backoff_and_releases_its_claim()
    {
        var command = await StoreCommandAsync();
        var onec = new FakeOnec { ExecuteKind = OnecExecutionKind.Processing };
        var service = new CommandExecutionService(_store, onec, static () => 12, executorId: "ambiguous-executor");

        await service.ProcessAsync(await ReadyAsync(command.CommandId), NeverAdministrative, CancellationToken.None);

        var state = await SnapshotAsync(command.CommandId);
        Assert.Equal(1, onec.ExecuteCalls);
        Assert.Equal(0, onec.StatusCalls);
        Assert.Equal("unknown_result", state.Status);
        Assert.Null(state.ClaimOwner);
        Assert.NotNull(state.NextAttemptAtUtc);
        Assert.Equal(1, state.PostAttemptCount);
        Assert.Equal(0, state.LookupAttemptCount);
        Assert.Equal(1, state.AttemptRows);
        Assert.Contains("unknown_result", state.AttemptSignature, StringComparison.Ordinal);
        Assert.Null(state.OutboxSignature);
        Assert.Equal(0, await OpenAttemptCountAsync(command.CommandId));
    }

    [Fact]
    public async Task Pending_executor_lookup_persists_backoff_and_releases_its_claim()
    {
        var command = await StoreCommandAsync();
        var post = await _store.ClaimPostAttemptAsync(command.CommandId, CancellationToken.None);
        Assert.NotNull(post);
        await _store.CompleteAttemptAsync(post.AttemptId, CommandStatus.UnknownResult, "UNKNOWN_RESULT", "ambiguous", 202, CancellationToken.None);
        Assert.True(await _store.MarkUnknownResultAsync(command.CommandId, post.AttemptId, "UNKNOWN_RESULT", "ambiguous", null, CancellationToken.None));
        await MakeDueAsync(command.CommandId);
        var onec = new FakeOnec { StatusKind = OnecExecutionKind.Processing };
        var service = new CommandExecutionService(_store, onec, static () => 12, executorId: "lookup-executor");

        await service.ProcessAsync(await ReadyAsync(command.CommandId), NeverAdministrative, CancellationToken.None);

        var state = await SnapshotAsync(command.CommandId);
        Assert.Equal(0, onec.ExecuteCalls);
        Assert.Equal(1, onec.StatusCalls);
        Assert.Equal("unknown_result", state.Status);
        Assert.Null(state.ClaimOwner);
        Assert.NotNull(state.NextAttemptAtUtc);
        Assert.Equal(1, state.PostAttemptCount);
        Assert.Equal(1, state.LookupAttemptCount);
        Assert.Equal(2, state.AttemptRows);
        Assert.Contains("lookup", state.AttemptSignature, StringComparison.Ordinal);
        Assert.Contains("unknown_result", state.AttemptSignature, StringComparison.Ordinal);
        Assert.Null(state.OutboxSignature);
        Assert.Equal(0, await OpenAttemptCountAsync(command.CommandId));
    }

    private async Task AssertAllSchedulingVariantsNoopAsync(Guid commandId, Guid attemptId, Snapshot expected)
    {
        var unknownAttempt = await _store.MarkUnknownResultAsync(commandId, attemptId, "owner-b", "LATE_UNKNOWN", "late", GenerationB.AddHours(4), CancellationToken.None);
        var unknownScheduling = await _store.MarkUnknownResultAsync(commandId, "owner-b", "LATE_UNKNOWN", "late", GenerationB.AddHours(4), CancellationToken.None);
        var retryOwner = await _store.ScheduleRetryAsync(commandId, "owner-b", "LATE_RETRY", "late", GenerationB.AddHours(5), CancellationToken.None);
        var unknownLegacyAttempt = await _store.MarkUnknownResultAsync(commandId, attemptId, "LATE_UNKNOWN", "late", GenerationB.AddHours(4), CancellationToken.None);
        var unknownLegacyScheduling = await _store.MarkUnknownResultAsync(commandId, "LATE_UNKNOWN", "late", GenerationB.AddHours(4), CancellationToken.None);
        var retryLegacy = await _store.ScheduleRetryAsync(commandId, "LATE_RETRY", "late", GenerationB.AddHours(5), CancellationToken.None);

        Assert.False(unknownAttempt);
        Assert.False(unknownScheduling);
        Assert.False(retryOwner);
        Assert.False(unknownLegacyAttempt);
        Assert.False(unknownLegacyScheduling);
        Assert.False(retryLegacy);
        Assert.Equal(expected, await SnapshotAsync(commandId));
    }

    private async Task<CommandEnvelope> StoreCommandAsync()
    {
        var command = MakeCommand();
        Assert.Equal(StoreCommandOutcome.Stored, await _store.StoreCommandAsync(command, GenerationA, CancellationToken.None));
        return command;
    }

    private async Task<ExecutionClaim?> AcquireAsync(CommandEnvelope command, string owner, DateTimeOffset acquiredAt) =>
        await _store.TryAcquireCommandExecutionClaimAsync(command.CommandId, owner, acquiredAt, DateTimeOffset.MinValue, CancellationToken.None);

    private async Task<StoredCommand> ReadyAsync(Guid commandId) =>
        (await _store.GetReadyCommandsAsync(10, DateTimeOffset.UtcNow.AddDays(1), CancellationToken.None)).Single(command => command.Envelope.CommandId == commandId);

    private async Task MakeDueAsync(Guid commandId)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = "UPDATE commands_inbox SET next_attempt_at_utc=$due WHERE command_id=$id";
        command.Parameters.AddWithValue("$due", DateTimeOffset.UtcNow.AddMinutes(-1).ToUniversalTime().ToString("O"));
        command.Parameters.AddWithValue("$id", commandId.ToString("D"));
        await command.ExecuteNonQueryAsync(CancellationToken.None);
    }

    private async Task<long> OpenAttemptCountAsync(Guid commandId)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT COUNT(*) FROM command_attempts WHERE command_id=$id AND finished_at_utc IS NULL";
        command.Parameters.AddWithValue("$id", commandId.ToString("D"));
        return Convert.ToInt64(await command.ExecuteScalarAsync(CancellationToken.None), CultureInfo.InvariantCulture);
    }

    private async Task<Snapshot> SnapshotAsync(Guid commandId)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        Snapshot state;
        await using (var command = connection.CreateCommand())
        {
            command.CommandText = """
                SELECT status,result_status,result_json,external_ref,external_number,exec_claim_owner_id,exec_claim_acquired_at_utc,row_version,attempt_count,lookup_attempt_count,post_attempt_count,next_attempt_at_utc,last_error_code,last_error_message,first_sent_at_utc,finished_at_utc,erp_acknowledged_at_utc
                FROM commands_inbox WHERE command_id=$id;
                """;
            command.Parameters.AddWithValue("$id", commandId.ToString("D"));
            await using var reader = await command.ExecuteReaderAsync(CancellationToken.None);
            Assert.True(await reader.ReadAsync(CancellationToken.None));
            state = new(
                reader.GetString(0), NullableString(reader, 1), NullableString(reader, 2), NullableString(reader, 3), NullableString(reader, 4), NullableString(reader, 5), NullableString(reader, 6),
                reader.GetInt64(7), reader.GetInt32(8), reader.GetInt32(9), reader.GetInt32(10), NullableString(reader, 11), NullableString(reader, 12), NullableString(reader, 13), NullableString(reader, 14), NullableString(reader, 15), NullableString(reader, 16),
                null, 0, null);
        }

        string? outboxSignature = null;
        await using (var command = connection.CreateCommand())
        {
            command.CommandText = "SELECT result_id,payload_json,payload_hash,status,attempt_count,next_attempt_at_utc,created_at_utc,sent_at_utc,acknowledged_at_utc,last_error FROM results_outbox WHERE command_id=$id";
            command.Parameters.AddWithValue("$id", commandId.ToString("D"));
            await using var reader = await command.ExecuteReaderAsync(CancellationToken.None);
            if (await reader.ReadAsync(CancellationToken.None)) outboxSignature = string.Join("|", Enumerable.Range(0, reader.FieldCount).Select(index => Value(reader, index)));
        }

        var attemptRows = 0;
        var attemptSignature = new StringBuilder();
        await using (var command = connection.CreateCommand())
        {
            command.CommandText = "SELECT attempt_id,attempt_no,attempt_kind,started_at_utc,finished_at_utc,request_hash,http_status,outcome,error_code,error_message,duration_ms FROM command_attempts WHERE command_id=$id ORDER BY attempt_no";
            command.Parameters.AddWithValue("$id", commandId.ToString("D"));
            await using var reader = await command.ExecuteReaderAsync(CancellationToken.None);
            while (await reader.ReadAsync(CancellationToken.None))
            {
                attemptRows++;
                if (attemptSignature.Length > 0) attemptSignature.Append('|');
                attemptSignature.Append(string.Join(":", Enumerable.Range(0, reader.FieldCount).Select(index => Value(reader, index))));
            }
        }
        return state with { OutboxSignature = outboxSignature, AttemptRows = attemptRows, AttemptSignature = attemptSignature.Length == 0 ? null : attemptSignature.ToString() };
    }

    private static string Value(SqliteDataReader reader, int ordinal) => reader.IsDBNull(ordinal) ? "<null>" : Convert.ToString(reader.GetValue(ordinal), CultureInfo.InvariantCulture) ?? "<null>";

    private static string? NullableString(SqliteDataReader reader, int ordinal) => reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);

    private static Task<bool> NeverAdministrative(CommandEnvelope _, string __, CancellationToken ___) => Task.FromResult(false);

    private static CommandEnvelope MakeCommand()
    {
        using var document = JsonDocument.Parse("{\"amount\":10}");
        var payload = document.RootElement.Clone();
        return new(Guid.NewGuid(), "create_customer_order", 1, 100, "order:" + Guid.NewGuid().ToString("N"), null, DateTimeOffset.UtcNow, null, null, null, PayloadHasher.Compute(payload), payload);
    }

    private sealed class FakeOnec : IOnecCommandClient
    {
        private int _executeCalls;
        private int _statusCalls;

        public int ExecuteCalls => Volatile.Read(ref _executeCalls);
        public int StatusCalls => Volatile.Read(ref _statusCalls);
        public OnecExecutionKind ExecuteKind { get; init; } = OnecExecutionKind.Processing;
        public OnecExecutionKind StatusKind { get; init; } = OnecExecutionKind.Processing;

        public Task<OnecExecutionResult> ExecuteAsync(CommandEnvelope command, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _executeCalls);
            return Task.FromResult(new OnecExecutionResult(ExecuteKind, null, 202, null, null));
        }

        public Task<OnecExecutionResult> GetStatusAsync(Guid commandId, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _statusCalls);
            return Task.FromResult(new OnecExecutionResult(StatusKind, null, 202, null, null));
        }
    }

    private sealed record Snapshot(
        string Status,
        string? ResultStatus,
        string? ResultJson,
        string? ExternalRef,
        string? ExternalNumber,
        string? ClaimOwner,
        string? ClaimAcquiredAtUtc,
        long RowVersion,
        int AttemptCount,
        int LookupAttemptCount,
        int PostAttemptCount,
        string? NextAttemptAtUtc,
        string? LastErrorCode,
        string? LastErrorMessage,
        string? FirstSentAtUtc,
        string? FinishedAtUtc,
        string? ErpAcknowledgedAtUtc,
        string? OutboxSignature,
        int AttemptRows,
        string? AttemptSignature);
}
