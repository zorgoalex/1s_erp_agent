using System.Text.Json;
using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Application.Commands;
using ErpOnecAgent.Domain.Commands;
using ErpOnecAgent.Domain.Common;
using ErpOnecAgent.Infrastructure.Persistence.Sqlite;
using Microsoft.Data.Sqlite;
using Xunit;

namespace ErpOnecAgent.IntegrationTests;

public sealed class ResultDeliveryGuardsTests : IAsyncLifetime
{
    private static readonly DateTimeOffset CreatedAt = new(2026, 9, 24, 8, 0, 0, TimeSpan.Zero);
    private readonly SqliteTestDatabase _database = new();
    private SqliteConnectionFactory _factory = null!;
    private SqliteAgentStore _store = null!;

    public async Task InitializeAsync()
    {
        _factory = _database.CreateFactory(Path.Combine("data", "agent.db"));
        _store = new(_factory, new SqliteMigrator(_factory));
        await _store.InitializeAsync(CancellationToken.None);
    }

    public async Task DisposeAsync() => await _database.DisposeAsync();

    [Fact]
    public async Task First_ack_updates_command_and_outbox()
    {
        var command = await CompleteAsync();
        var before = await SnapshotAsync(command.CommandId);
        var acknowledgedAt = new DateTimeOffset(2026, 9, 24, 9, 0, 0, TimeSpan.Zero);

        Assert.True(await _store.AcknowledgeResultAsync(command.CommandId, acknowledgedAt, CancellationToken.None));

        var after = await SnapshotAsync(command.CommandId);
        Assert.Equal("completed", after.Command.Status);
        Assert.Equal("acknowledged", after.Outbox!.Status);
        Assert.Equal(acknowledgedAt.ToString("O"), after.Command.ErpAcknowledgedAtUtc);
        Assert.Equal(acknowledgedAt.ToString("O"), after.Outbox.AcknowledgedAtUtc);
        Assert.Equal(acknowledgedAt.ToString("O"), after.Outbox.SentAtUtc);
        Assert.Equal(before.Command.RowVersion + 1, after.Command.RowVersion);
        Assert.Equal(before.Attempts, after.Attempts);
        Assert.Equal(before.Outbox!.ResultId, after.Outbox.ResultId);
        Assert.Equal(before.Outbox.PayloadHash, after.Outbox.PayloadHash);
        Assert.Equal(before.Outbox.PayloadJson, after.Outbox.PayloadJson);
    }

    [Fact]
    public async Task Acknowledge_accepts_sending_outbox()
    {
        var command = await CompleteAsync();
        await SetOutboxStatusAsync(command.CommandId, "sending");
        var before = await SnapshotAsync(command.CommandId);
        var acknowledgedAt = new DateTimeOffset(2026, 9, 24, 9, 30, 0, TimeSpan.Zero);

        Assert.True(await _store.AcknowledgeResultAsync(command.CommandId, acknowledgedAt, CancellationToken.None));

        var after = await SnapshotAsync(command.CommandId);
        Assert.Equal("completed", after.Command.Status);
        Assert.Equal("acknowledged", after.Outbox!.Status);
        Assert.Equal(acknowledgedAt.ToString("O"), after.Outbox.AcknowledgedAtUtc);
        Assert.Equal(before.Command.RowVersion + 1, after.Command.RowVersion);
        Assert.Equal(before.Attempts, after.Attempts);
        Assert.Equal(before.Outbox!.ResultId, after.Outbox.ResultId);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Acknowledge_rejects_active_command_without_result(bool executing)
    {
        var command = executing ? await StoreExecutingAsync() : await StoreQueuedAsync();
        var before = await SnapshotAsync(command.CommandId);

        Assert.False(await _store.AcknowledgeResultAsync(command.CommandId, DateTimeOffset.UtcNow, CancellationToken.None));

        AssertSnapshotsEqual(before, await SnapshotAsync(command.CommandId));
    }

    [Theory]
    [InlineData("queued")]
    [InlineData("executing")]
    public async Task Acknowledge_rejects_deliverable_outbox_for_active_command(string status)
    {
        var command = await CompleteAsync();
        await SetCommandStatusAsync(command.CommandId, status);
        var before = await SnapshotAsync(command.CommandId);

        Assert.False(await _store.AcknowledgeResultAsync(command.CommandId, DateTimeOffset.UtcNow, CancellationToken.None));

        AssertSnapshotsEqual(before, await SnapshotAsync(command.CommandId));
    }

    [Fact]
    public async Task Duplicate_ack_is_noop_and_preserves_timestamps_row_version_and_audit()
    {
        var command = await CompleteAsync();
        var firstAcknowledgedAt = new DateTimeOffset(2026, 9, 24, 9, 0, 0, TimeSpan.Zero);
        Assert.True(await _store.AcknowledgeResultAsync(command.CommandId, firstAcknowledgedAt, CancellationToken.None));
        var before = await SnapshotAsync(command.CommandId);

        Assert.False(await _store.AcknowledgeResultAsync(command.CommandId, firstAcknowledgedAt.AddHours(1), CancellationToken.None));

        AssertSnapshotsEqual(before, await SnapshotAsync(command.CommandId));
    }

    [Fact]
    public async Task Explicit_duplicate_replay_ack_refreshes_last_confirmation()
    {
        var command = await CompleteAsync();
        var firstAcknowledgedAt = new DateTimeOffset(2026, 9, 24, 9, 0, 0, TimeSpan.Zero);
        Assert.True(await _store.AcknowledgeResultAsync(command.CommandId, firstAcknowledgedAt, CancellationToken.None));
        Assert.Equal(StoreCommandOutcome.Duplicate, await _store.StoreCommandAsync(command, firstAcknowledgedAt.AddHours(1), CancellationToken.None));
        var replayed = await SnapshotAsync(command.CommandId);
        var secondAcknowledgedAt = firstAcknowledgedAt.AddHours(2);

        Assert.True(await _store.AcknowledgeResultAsync(command.CommandId, secondAcknowledgedAt, CancellationToken.None));

        var after = await SnapshotAsync(command.CommandId);
        Assert.Equal("completed", after.Command.Status);
        Assert.Equal("acknowledged", after.Outbox!.Status);
        Assert.Equal(secondAcknowledgedAt.ToString("O"), after.Command.ErpAcknowledgedAtUtc);
        Assert.Equal(secondAcknowledgedAt.ToString("O"), after.Outbox.AcknowledgedAtUtc);
        Assert.Equal(secondAcknowledgedAt.ToString("O"), after.Outbox.SentAtUtc);
        Assert.Equal(replayed.Command.RowVersion + 1, after.Command.RowVersion);
        Assert.Equal(replayed.Attempts, after.Attempts);
        Assert.Equal(replayed.Outbox!.ResultId, after.Outbox.ResultId);
        Assert.Equal(replayed.Outbox.PayloadHash, after.Outbox.PayloadHash);
        Assert.Equal(replayed.Outbox.PayloadJson, after.Outbox.PayloadJson);
    }

    [Fact]
    public async Task Late_retry_after_ack_is_noop()
    {
        var command = await CompleteAsync();
        Assert.True(await _store.AcknowledgeResultAsync(command.CommandId, new DateTimeOffset(2026, 9, 24, 9, 0, 0, TimeSpan.Zero), CancellationToken.None));
        var before = await SnapshotAsync(command.CommandId);
        var retryAt = new DateTimeOffset(2026, 9, 24, 10, 0, 0, TimeSpan.Zero);

        Assert.False(await _store.MarkResultRetryAsync(command.CommandId, "late ERP failure", retryAt, CancellationToken.None));

        AssertSnapshotsEqual(before, await SnapshotAsync(command.CommandId));
    }

    [Fact]
    public async Task Retry_updates_only_deliverable_outbox_and_preserves_backoff_identity_and_command()
    {
        var command = await CompleteAsync();
        await SetOutboxStatusAsync(command.CommandId, "sending");
        var before = await SnapshotAsync(command.CommandId);
        var retryAt = new DateTimeOffset(2026, 9, 24, 11, 0, 0, TimeSpan.Zero);
        const string error = "ERP unavailable";

        Assert.True(await _store.MarkResultRetryAsync(command.CommandId, error, retryAt, CancellationToken.None));

        var after = await SnapshotAsync(command.CommandId);
        Assert.Equal(before.Command, after.Command);
        Assert.Equal(before.Attempts, after.Attempts);
        Assert.Equal("retry_waiting", after.Outbox!.Status);
        Assert.Equal(before.Outbox!.AttemptCount + 1, after.Outbox.AttemptCount);
        Assert.Equal(retryAt.ToString("O"), after.Outbox.NextAttemptAtUtc);
        Assert.Equal(error, after.Outbox.LastError);
        Assert.Equal(before.Outbox.ResultId, after.Outbox.ResultId);
        Assert.Equal(before.Outbox.PayloadHash, after.Outbox.PayloadHash);
        Assert.Equal(before.Outbox.PayloadJson, after.Outbox.PayloadJson);
        Assert.Equal(before.Outbox.SentAtUtc, after.Outbox.SentAtUtc);
    }

    [Theory]
    [InlineData("queued")]
    [InlineData("executing")]
    public async Task Retry_rejects_deliverable_outbox_for_active_command(string status)
    {
        var command = await CompleteAsync();
        await SetCommandStatusAsync(command.CommandId, status);
        var before = await SnapshotAsync(command.CommandId);
        var retryAt = new DateTimeOffset(2026, 9, 24, 11, 0, 0, TimeSpan.Zero);

        Assert.False(await _store.MarkResultRetryAsync(command.CommandId, "late failure", retryAt, CancellationToken.None));

        AssertSnapshotsEqual(before, await SnapshotAsync(command.CommandId));
    }

    [Fact]
    public async Task Nonexistent_id_ack_and_retry_are_noop()
    {
        var commandId = Guid.NewGuid();

        Assert.False(await _store.AcknowledgeResultAsync(commandId, DateTimeOffset.UtcNow, CancellationToken.None));
        Assert.False(await _store.MarkResultRetryAsync(commandId, "missing", DateTimeOffset.UtcNow, CancellationToken.None));

        Assert.Equal((0L, 0L, 0L), await CountsAsync());
    }

    [Fact]
    public async Task Ack_transaction_rolls_back_command_when_outbox_update_fails()
    {
        var command = await CompleteAsync();
        var before = await SnapshotAsync(command.CommandId);
        await InstallOutboxFailureTriggerAsync(command.CommandId);

        await Assert.ThrowsAsync<SqliteException>(() => _store.AcknowledgeResultAsync(command.CommandId, DateTimeOffset.UtcNow, CancellationToken.None));

        AssertSnapshotsEqual(before, await SnapshotAsync(command.CommandId));
    }

    private static void AssertSnapshotsEqual(Snapshot expected, Snapshot actual)
    {
        Assert.Equal(expected.Command, actual.Command);
        Assert.Equal(expected.Attempts, actual.Attempts);
        Assert.Equal(expected.Outbox, actual.Outbox);
    }

    private async Task<CommandEnvelope> StoreQueuedAsync()
    {
        var command = MakeCommand();
        Assert.Equal(StoreCommandOutcome.Stored, await _store.StoreCommandAsync(command, CreatedAt, CancellationToken.None));
        return command;
    }

    private async Task<CommandEnvelope> StoreExecutingAsync()
    {
        var command = await StoreQueuedAsync();
        const string owner = "result-delivery-active-owner";
        Assert.NotNull(await _store.TryAcquireCommandExecutionClaimAsync(command.CommandId, owner, CreatedAt.AddMinutes(1), DateTimeOffset.MinValue, CancellationToken.None));
        Assert.NotNull(await _store.ClaimPostAttemptAsync(command.CommandId, owner, CancellationToken.None));
        return command;
    }

    private async Task<CommandEnvelope> CompleteAsync()
    {
        var command = await StoreQueuedAsync();
        var attempt = await _store.ClaimPostAttemptAsync(command.CommandId, CancellationToken.None);
        Assert.NotNull(attempt);
        var resultJson = $"{{\"commandId\":\"{command.CommandId:D}\",\"status\":\"succeeded\",\"resultVersion\":1}}";
        Assert.True(await _store.CompleteLocallyAsync(command.CommandId, CommandStatus.SucceededLocal, resultJson, "ref", "42", attempt.AttemptId, CancellationToken.None));
        return command;
    }

    private async Task SetCommandStatusAsync(Guid commandId, string status)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = "UPDATE commands_inbox SET status=$status WHERE command_id=$id;";
        command.Parameters.AddWithValue("$status", status);
        command.Parameters.AddWithValue("$id", commandId.ToString("D"));
        Assert.Equal(1, await command.ExecuteNonQueryAsync(CancellationToken.None));
    }

    private async Task SetOutboxStatusAsync(Guid commandId, string status)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = "UPDATE results_outbox SET status=$status WHERE command_id=$id;";
        command.Parameters.AddWithValue("$status", status);
        command.Parameters.AddWithValue("$id", commandId.ToString("D"));
        Assert.Equal(1, await command.ExecuteNonQueryAsync(CancellationToken.None));
    }

    private async Task InstallOutboxFailureTriggerAsync(Guid commandId)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = $"CREATE TRIGGER fail_result_delivery_ack BEFORE UPDATE OF status ON results_outbox WHEN NEW.command_id = '{commandId:D}' AND NEW.status = 'acknowledged' BEGIN SELECT RAISE(ABORT, 'injected result delivery failure'); END;";
        await command.ExecuteNonQueryAsync(CancellationToken.None);
    }

    private async Task<Snapshot> SnapshotAsync(Guid commandId)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        CommandSnapshot commandState;
        await using (var command = connection.CreateCommand())
        {
            command.CommandText = """
                SELECT command_id,command_type,payload_version,priority,ordering_key,correlation_id,payload_json,payload_hash,status,created_at_utc,received_at_utc,not_before_utc,expires_at_utc,started_at_utc,finished_at_utc,attempt_count,next_attempt_at_utc,result_status,result_json,external_ref,external_number,last_error_code,last_error_message,erp_acknowledged_at_utc,row_version,first_sent_at_utc,lookup_attempt_count,post_attempt_count,queue_sequence,exec_claim_owner_id,exec_claim_acquired_at_utc
                FROM commands_inbox WHERE command_id=$id;
                """;
            command.Parameters.AddWithValue("$id", commandId.ToString("D"));
            await using var reader = await command.ExecuteReaderAsync(CancellationToken.None);
            Assert.True(await reader.ReadAsync(CancellationToken.None));
            commandState = new(
                reader.GetString(0), reader.GetString(1), reader.GetInt32(2), reader.GetInt32(3), NullableString(reader, 4), NullableString(reader, 5),
                reader.GetString(6), reader.GetString(7), reader.GetString(8), reader.GetString(9), reader.GetString(10), NullableString(reader, 11),
                NullableString(reader, 12), NullableString(reader, 13), NullableString(reader, 14), reader.GetInt64(15), NullableString(reader, 16),
                NullableString(reader, 17), NullableString(reader, 18), NullableString(reader, 19), NullableString(reader, 20), NullableString(reader, 21),
                NullableString(reader, 22), NullableString(reader, 23), reader.GetInt64(24), NullableString(reader, 25), reader.GetInt32(26),
                reader.GetInt32(27), reader.GetInt64(28), NullableString(reader, 29), NullableString(reader, 30));
        }

        var attempts = new List<AttemptSnapshot>();
        await using (var command = connection.CreateCommand())
        {
            command.CommandText = """
                SELECT attempt_id,attempt_no,started_at_utc,finished_at_utc,request_hash,http_status,outcome,error_code,error_message,duration_ms,attempt_kind
                FROM command_attempts WHERE command_id=$id ORDER BY attempt_no;
                """;
            command.Parameters.AddWithValue("$id", commandId.ToString("D"));
            await using var reader = await command.ExecuteReaderAsync(CancellationToken.None);
            while (await reader.ReadAsync(CancellationToken.None))
            {
                attempts.Add(new(
                    reader.GetString(0), reader.GetInt32(1), reader.GetString(2), NullableString(reader, 3), reader.GetString(4),
                    NullableInt(reader, 5), NullableString(reader, 6), NullableString(reader, 7), NullableString(reader, 8), NullableInt(reader, 9), reader.GetString(10)));
            }
        }

        OutboxSnapshot? outbox = null;
        await using (var command = connection.CreateCommand())
        {
            command.CommandText = """
                SELECT result_id,command_id,payload_json,payload_hash,status,attempt_count,next_attempt_at_utc,created_at_utc,sent_at_utc,acknowledged_at_utc,last_error
                FROM results_outbox WHERE command_id=$id;
                """;
            command.Parameters.AddWithValue("$id", commandId.ToString("D"));
            await using var reader = await command.ExecuteReaderAsync(CancellationToken.None);
            if (await reader.ReadAsync(CancellationToken.None))
            {
                outbox = new(
                    reader.GetString(0), reader.GetString(1), reader.GetString(2), reader.GetString(3), reader.GetString(4), reader.GetInt64(5),
                    NullableString(reader, 6), reader.GetString(7), NullableString(reader, 8), NullableString(reader, 9), NullableString(reader, 10));
            }
        }

        return new(commandState, attempts.ToArray(), outbox);
    }

    private async Task<(long Commands, long Outbox, long Attempts)> CountsAsync()
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT (SELECT COUNT(*) FROM commands_inbox),(SELECT COUNT(*) FROM results_outbox),(SELECT COUNT(*) FROM command_attempts);";
        await using var reader = await command.ExecuteReaderAsync(CancellationToken.None);
        Assert.True(await reader.ReadAsync(CancellationToken.None));
        return (reader.GetInt64(0), reader.GetInt64(1), reader.GetInt64(2));
    }

    private static CommandEnvelope MakeCommand()
    {
        using var document = JsonDocument.Parse("{\"amount\":10}");
        var payload = document.RootElement.Clone();
        return new(
            Guid.NewGuid(), "create_customer_order", 1, 100, "result-delivery:" + Guid.NewGuid().ToString("N"), null, CreatedAt,
            null, null, null, PayloadHasher.Compute(payload), payload);
    }

    private static string? NullableString(SqliteDataReader reader, int ordinal) => reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);

    private static int? NullableInt(SqliteDataReader reader, int ordinal) => reader.IsDBNull(ordinal) ? null : reader.GetInt32(ordinal);

    private sealed record Snapshot(CommandSnapshot Command, AttemptSnapshot[] Attempts, OutboxSnapshot? Outbox);

    private sealed record CommandSnapshot(
        string CommandId,
        string CommandType,
        int PayloadVersion,
        int Priority,
        string? OrderingKey,
        string? CorrelationId,
        string PayloadJson,
        string PayloadHash,
        string Status,
        string CreatedAtUtc,
        string ReceivedAtUtc,
        string? NotBeforeUtc,
        string? ExpiresAtUtc,
        string? StartedAtUtc,
        string? FinishedAtUtc,
        long AttemptCount,
        string? NextAttemptAtUtc,
        string? ResultStatus,
        string? ResultJson,
        string? ExternalRef,
        string? ExternalNumber,
        string? LastErrorCode,
        string? LastErrorMessage,
        string? ErpAcknowledgedAtUtc,
        long RowVersion,
        string? FirstSentAtUtc,
        int LookupAttemptCount,
        int PostAttemptCount,
        long QueueSequence,
        string? ClaimOwner,
        string? ClaimAcquiredAtUtc);

    private sealed record AttemptSnapshot(
        string AttemptId,
        int AttemptNo,
        string StartedAtUtc,
        string? FinishedAtUtc,
        string RequestHash,
        int? HttpStatus,
        string? Outcome,
        string? ErrorCode,
        string? ErrorMessage,
        int? DurationMs,
        string AttemptKind);

    private sealed record OutboxSnapshot(
        string ResultId,
        string CommandId,
        string PayloadJson,
        string PayloadHash,
        string Status,
        long AttemptCount,
        string? NextAttemptAtUtc,
        string CreatedAtUtc,
        string? SentAtUtc,
        string? AcknowledgedAtUtc,
        string? LastError);
}
