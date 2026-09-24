using System.Globalization;
using System.Text;
using System.Text.Json;
using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Application.Commands;
using ErpOnecAgent.Domain.Commands;
using ErpOnecAgent.Domain.Common;
using ErpOnecAgent.Infrastructure.Persistence.Sqlite;
using Microsoft.Data.Sqlite;
using Xunit;

namespace ErpOnecAgent.IntegrationTests;

public sealed class DeadLetterRetentionTests : IAsyncLifetime
{
    private static readonly DateTimeOffset CreatedAt = new(2026, 8, 1, 8, 0, 0, TimeSpan.Zero);
    private static readonly DateTimeOffset OldAcknowledgedAt = new(2026, 9, 1, 8, 0, 0, TimeSpan.Zero);
    private static readonly DateTimeOffset CleanupCutoff = new(2026, 9, 20, 8, 0, 0, TimeSpan.Zero);
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
    public async Task Acknowledged_dead_letter_history_survives_cleanup_and_remains_counted()
    {
        var command = MakeCommand();
        var resultJson = MakeResult(command.CommandId, "dead_letter");
        await CompleteAndAcknowledgeAsync(command, resultJson, CommandStatus.DeadLetter, OldAcknowledgedAt);
        var before = Assert.IsType<HistorySnapshot>(await SnapshotAsync(command.CommandId));
        Assert.Equal(1L, (await _store.GetQueueMetricsAsync(CancellationToken.None)).CommandsDeadLetter);

        await _store.CleanupAsync(CleanupCutoff, CleanupCutoff, CancellationToken.None);

        var after = Assert.IsType<HistorySnapshot>(await SnapshotAsync(command.CommandId));
        AssertHistoryUnchanged(before, after);
        Assert.Equal("dead_letter", after.Command.ResultStatus);
        Assert.Equal("completed", after.Command.Status);
        Assert.Equal("acknowledged", after.Outbox?.Status);
        Assert.Single(after.Attempts);
        Assert.Equal((1L, 1L, 1L), await CountsAsync());
        var metrics = await _store.GetQueueMetricsAsync(CancellationToken.None);
        Assert.Equal(1L, metrics.CommandsDeadLetter);
        Assert.Equal(1L, metrics.DeadLetters);
    }

    [Fact]
    public async Task Rejected_admission_dead_letter_history_survives_cleanup()
    {
        var command = MakeCommand(payloadHash: "wrong-payload-hash");
        var validation = CommandValidator.Validate(command, [command.CommandType], 4096);
        Assert.False(validation.IsValid);
        Assert.Equal("PAYLOAD_HASH_MISMATCH", validation.ErrorCode);
        Assert.Equal(StoreCommandOutcome.Rejected, await _store.AdmitCommandAsync(command, CreatedAt, validation, CancellationToken.None));
        Assert.True(await _store.AcknowledgeResultAsync(command.CommandId, OldAcknowledgedAt, CancellationToken.None));
        var before = Assert.IsType<HistorySnapshot>(await SnapshotAsync(command.CommandId));

        await _store.CleanupAsync(CleanupCutoff, CleanupCutoff, CancellationToken.None);

        var after = Assert.IsType<HistorySnapshot>(await SnapshotAsync(command.CommandId));
        AssertHistoryUnchanged(before, after);
        Assert.Empty(after.Attempts);
        Assert.Equal("dead_letter", after.Command.ResultStatus);
        Assert.Equal("completed", after.Command.Status);
        Assert.Equal("acknowledged", after.Outbox?.Status);
        Assert.Equal((1L, 1L, 0L), await CountsAsync());
        Assert.Equal(1L, (await _store.GetQueueMetricsAsync(CancellationToken.None)).CommandsDeadLetter);
    }

    [Fact]
    public async Task Unacknowledged_dead_letter_history_survives_cleanup()
    {
        var command = MakeCommand();
        await CompleteLocallyAsync(command, MakeResult(command.CommandId, "dead_letter"), CommandStatus.DeadLetter);
        var before = Assert.IsType<HistorySnapshot>(await SnapshotAsync(command.CommandId));

        await _store.CleanupAsync(CleanupCutoff, CleanupCutoff, CancellationToken.None);

        var after = Assert.IsType<HistorySnapshot>(await SnapshotAsync(command.CommandId));
        AssertHistoryUnchanged(before, after);
        Assert.Equal("result_pending", after.Command.Status);
        Assert.Equal("pending", after.Outbox?.Status);
        Assert.Null(after.Command.ErpAcknowledgedAtUtc);
        Assert.Single(after.Attempts);
        Assert.Equal((1L, 1L, 1L), await CountsAsync());
        Assert.Equal(1L, (await _store.GetQueueMetricsAsync(CancellationToken.None)).CommandsDeadLetter);
    }

    [Fact]
    public async Task Acknowledged_dead_letter_duplicate_replay_survives_late_cleanup()
    {
        var command = MakeCommand();
        var resultJson = MakeResult(command.CommandId, "dead_letter");
        await CompleteAndAcknowledgeAsync(command, resultJson, CommandStatus.DeadLetter, OldAcknowledgedAt);
        Assert.Equal(StoreCommandOutcome.Duplicate, await _store.StoreCommandAsync(command, OldAcknowledgedAt.AddDays(1), CancellationToken.None));
        var before = Assert.IsType<HistorySnapshot>(await SnapshotAsync(command.CommandId));
        Assert.Equal("pending", before.Outbox?.Status);

        await _store.CleanupAsync(CleanupCutoff, CleanupCutoff, CancellationToken.None);

        var after = Assert.IsType<HistorySnapshot>(await SnapshotAsync(command.CommandId));
        AssertHistoryUnchanged(before, after);
        Assert.Equal(Encoding.UTF8.GetBytes(resultJson), Encoding.UTF8.GetBytes(Assert.IsType<OutboxSnapshot>(after.Outbox).PayloadJson));
        Assert.Equal("pending", after.Outbox?.Status);
        Assert.Single(after.Attempts);
        Assert.Equal((1L, 1L, 1L), await CountsAsync());
        Assert.Equal(1L, (await _store.GetQueueMetricsAsync(CancellationToken.None)).CommandsDeadLetter);
    }

    [Fact]
    public async Task Acknowledged_dead_letter_survives_repeated_cleanup_and_restart()
    {
        var command = MakeCommand();
        await CompleteAndAcknowledgeAsync(command, MakeResult(command.CommandId, "dead_letter"), CommandStatus.DeadLetter, OldAcknowledgedAt);
        var before = Assert.IsType<HistorySnapshot>(await SnapshotAsync(command.CommandId));

        await _store.CleanupAsync(CleanupCutoff, CleanupCutoff, CancellationToken.None);
        await SqliteTestDatabase.ClearPoolAsync(_factory);
        _store = new(_factory, new SqliteMigrator(_factory));
        await _store.InitializeAsync(CancellationToken.None);
        await _store.RecoverAsync(CancellationToken.None);
        await _store.CleanupAsync(CleanupCutoff, CleanupCutoff, CancellationToken.None);

        var after = Assert.IsType<HistorySnapshot>(await SnapshotAsync(command.CommandId));
        AssertHistoryUnchanged(before, after);
        Assert.Equal("dead_letter", after.Command.ResultStatus);
        Assert.Equal("completed", after.Command.Status);
        Assert.Equal("acknowledged", after.Outbox?.Status);
        Assert.Single(after.Attempts);
        Assert.Equal((1L, 1L, 1L), await CountsAsync());
        Assert.Equal(1L, (await _store.GetQueueMetricsAsync(CancellationToken.None)).CommandsDeadLetter);
    }

    [Fact]
    public async Task Ordinary_acknowledged_success_history_is_still_purged()
    {
        var command = MakeCommand();
        await CompleteAndAcknowledgeAsync(command, MakeResult(command.CommandId, "succeeded"), CommandStatus.SucceededLocal, OldAcknowledgedAt);
        Assert.Equal(0L, (await _store.GetQueueMetricsAsync(CancellationToken.None)).CommandsDeadLetter);

        await _store.CleanupAsync(CleanupCutoff, CleanupCutoff, CancellationToken.None);

        Assert.Null(await SnapshotAsync(command.CommandId));
        Assert.Equal((0L, 0L, 0L), await CountsAsync());
        Assert.Equal(0L, (await _store.GetQueueMetricsAsync(CancellationToken.None)).CommandsDeadLetter);
    }

    private async Task CompleteAndAcknowledgeAsync(CommandEnvelope command, string resultJson, CommandStatus localStatus, DateTimeOffset acknowledgedAtUtc)
    {
        await CompleteLocallyAsync(command, resultJson, localStatus);
        Assert.True(await _store.AcknowledgeResultAsync(command.CommandId, acknowledgedAtUtc, CancellationToken.None));
    }

    private async Task CompleteLocallyAsync(CommandEnvelope command, string resultJson, CommandStatus localStatus)
    {
        Assert.Equal(StoreCommandOutcome.Stored, await _store.StoreCommandAsync(command, CreatedAt, CancellationToken.None));
        var owner = "dead-letter-owner-" + Guid.NewGuid().ToString("N");
        Assert.NotNull(await _store.TryAcquireCommandExecutionClaimAsync(command.CommandId, owner, CreatedAt.AddMinutes(1), DateTimeOffset.MinValue, CancellationToken.None));
        var attempt = await _store.ClaimPostAttemptAsync(command.CommandId, owner, CancellationToken.None);
        Assert.NotNull(attempt);
        Assert.True(await _store.CompleteLocallyAsync(command.CommandId, owner, localStatus, resultJson, "DOC-42", "42", attempt.AttemptId, CancellationToken.None));
    }

    private static void AssertHistoryUnchanged(HistorySnapshot expected, HistorySnapshot actual)
    {
        Assert.Equal(expected.Command, actual.Command);
        Assert.Equal(expected.Outbox, actual.Outbox);
        var expectedAttempts = expected.Attempts.OrderBy(static attempt => attempt.AttemptNo).ToList();
        var actualAttempts = actual.Attempts.OrderBy(static attempt => attempt.AttemptNo).ToList();
        Assert.Equal(expectedAttempts.Count, actualAttempts.Count);
        for (var index = 0; index < expectedAttempts.Count; index++)
        {
            Assert.Equal(expectedAttempts[index], actualAttempts[index]);
        }
    }

    private async Task<HistorySnapshot?> SnapshotAsync(Guid commandId)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        CommandSnapshot? commandState = null;
        await using (var command = connection.CreateCommand())
        {
            command.CommandText = """
                SELECT command_id,command_type,payload_version,priority,ordering_key,correlation_id,payload_json,payload_hash,status,created_at_utc,received_at_utc,not_before_utc,expires_at_utc,started_at_utc,finished_at_utc,attempt_count,next_attempt_at_utc,result_status,result_json,external_ref,external_number,last_error_code,last_error_message,erp_acknowledged_at_utc,row_version,first_sent_at_utc,lookup_attempt_count,post_attempt_count,queue_sequence,exec_claim_owner_id,exec_claim_acquired_at_utc
                FROM commands_inbox
                WHERE command_id=$id;
                """;
            command.Parameters.AddWithValue("$id", commandId.ToString("D"));
            await using var reader = await command.ExecuteReaderAsync(CancellationToken.None);
            if (await reader.ReadAsync(CancellationToken.None))
            {
                commandState = new(
                    reader.GetString(0), reader.GetString(1), reader.GetInt32(2), reader.GetInt32(3), NullableString(reader, 4), NullableString(reader, 5),
                    reader.GetString(6), reader.GetString(7), reader.GetString(8), reader.GetString(9), reader.GetString(10), NullableString(reader, 11),
                    NullableString(reader, 12), NullableString(reader, 13), NullableString(reader, 14), reader.GetInt64(15), NullableString(reader, 16),
                    NullableString(reader, 17), NullableString(reader, 18), NullableString(reader, 19), NullableString(reader, 20), NullableString(reader, 21),
                    NullableString(reader, 22), NullableString(reader, 23), reader.GetInt64(24), NullableString(reader, 25), reader.GetInt32(26),
                    reader.GetInt32(27), reader.GetInt64(28), NullableString(reader, 29), NullableString(reader, 30));
            }
        }

        if (commandState is null) return null;

        var attempts = new List<AttemptSnapshot>();
        await using (var command = connection.CreateCommand())
        {
            command.CommandText = """
                SELECT attempt_id,attempt_no,started_at_utc,finished_at_utc,request_hash,http_status,outcome,error_code,error_message,duration_ms,attempt_kind
                FROM command_attempts
                WHERE command_id=$id
                ORDER BY attempt_no;
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
                FROM results_outbox
                WHERE command_id=$id;
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

        return new(commandState, attempts, outbox);
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

    private static CommandEnvelope MakeCommand(string? payloadHash = null)
    {
        using var document = JsonDocument.Parse("{\"amount\":10}");
        var payload = document.RootElement.Clone();
        return new(
            Guid.NewGuid(), "create_customer_order", 1, 100, "dead-letter-retention:" + Guid.NewGuid().ToString("N"), null, CreatedAt,
            null, null, null, payloadHash ?? PayloadHasher.Compute(payload), payload);
    }

    private static string MakeResult(Guid commandId, string status) =>
        $$"""{"commandId":"{{commandId:D}}","status":"{{status}}","completedAtUtc":"2026-08-01T08:05:00+00:00","document":{"ref":"DOC-42","number":"42"},"warnings":[],"resultVersion":1}""";

    private static string? NullableString(SqliteDataReader reader, int ordinal) => reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);

    private static int? NullableInt(SqliteDataReader reader, int ordinal) => reader.IsDBNull(ordinal) ? null : reader.GetInt32(ordinal);

    private sealed record HistorySnapshot(CommandSnapshot Command, IReadOnlyList<AttemptSnapshot> Attempts, OutboxSnapshot? Outbox);

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
