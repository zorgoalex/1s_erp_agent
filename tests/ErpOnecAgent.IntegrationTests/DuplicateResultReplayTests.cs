using System.Text;
using System.Text.Json;
using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Application.Commands;
using ErpOnecAgent.Contracts.Erp;
using ErpOnecAgent.Domain.Commands;
using ErpOnecAgent.Domain.Common;
using ErpOnecAgent.Domain.Etl;
using ErpOnecAgent.Infrastructure.Persistence.Sqlite;
using Microsoft.Data.Sqlite;
using Xunit;

namespace ErpOnecAgent.IntegrationTests;

public sealed class DuplicateResultReplayTests : IAsyncLifetime
{
    private static readonly DateTimeOffset CommandCreatedAt = new(2026, 9, 24, 8, 0, 0, TimeSpan.Zero);
    private static readonly DateTimeOffset InitialAcknowledgedAt = new(2026, 9, 1, 8, 0, 0, TimeSpan.Zero);
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
    public async Task Completed_duplicate_requeues_exact_result_before_received_ack()
    {
        var command = MakeCommand();
        var resultJson = MakeResult(command.CommandId, "succeeded");
        await CompleteAndAcknowledgeAsync(command, resultJson, CommandStatus.SucceededLocal, InitialAcknowledgedAt);
        var acknowledged = await SnapshotAsync(command.CommandId);
        var originalOutbox = Assert.IsType<OutboxSnapshot>(acknowledged.Outbox);
        var successor = MakeCommand(payloadJson: "{\"amount\":20}");
        Assert.Equal(StoreCommandOutcome.Stored, await _store.StoreCommandAsync(successor, CommandCreatedAt.AddMinutes(2), CancellationToken.None));
        Assert.Equal(successor.CommandId, Assert.Single(await _store.GetReadyCommandsAsync(10, DateTimeOffset.UtcNow.AddDays(1), CancellationToken.None)).Envelope.CommandId);
        var ackEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseAck = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var erp = new FakeErpClient
        {
            BeforeAcknowledge = async () =>
            {
                ackEntered.SetResult();
                await releaseAck.Task;
            }
        };

        var intakeTask = IntakeAsync(command, erp, InitialAcknowledgedAt.AddDays(2));
        await ackEntered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        var duringAck = await SnapshotAsync(command.CommandId);

        Assert.Equal(acknowledged.Command, duringAck.Command);
        Assert.Equal(acknowledged.Attempts, duringAck.Attempts);
        var pending = Assert.Single(await _store.GetPendingResultsAsync(10, DateTimeOffset.UtcNow, CancellationToken.None));
        Assert.Equal(Encoding.UTF8.GetBytes(resultJson), Encoding.UTF8.GetBytes(pending.PayloadJson));
        Assert.Equal(resultJson, Assert.IsType<OutboxSnapshot>(duringAck.Outbox).PayloadJson);

        releaseAck.SetResult();
        var result = await intakeTask;

        Assert.Equal(StoreCommandOutcome.Duplicate, result.Outcome);
        Assert.True(result.Acknowledged);
        var replayed = await SnapshotAsync(command.CommandId);
        AssertCompletedReplay(acknowledged, replayed, resultJson);
        Assert.Equal(successor.CommandId, Assert.Single(await _store.GetReadyCommandsAsync(10, DateTimeOffset.UtcNow.AddDays(1), CancellationToken.None)).Envelope.CommandId);
        var replayOutbox = Assert.IsType<OutboxSnapshot>(replayed.Outbox);
        Assert.Equal(originalOutbox.ResultId, replayOutbox.ResultId);
        Assert.Equal(originalOutbox.PayloadHash, replayOutbox.PayloadHash);
        Assert.Equal(originalOutbox.CreatedAtUtc, replayOutbox.CreatedAtUtc);
        Assert.Equal(InitialAcknowledgedAt.ToString("O"), replayOutbox.AcknowledgedAtUtc);
    }

    [Fact]
    public async Task Repeated_duplicate_keeps_one_outbox_and_preserves_retry_backoff_and_execution_audit()
    {
        var command = MakeCommand();
        var resultJson = MakeResult(command.CommandId, "succeeded");
        await CompleteAndAcknowledgeAsync(command, resultJson, CommandStatus.SucceededLocal, InitialAcknowledgedAt);
        var firstReplay = await _store.StoreCommandAsync(command, InitialAcknowledgedAt.AddDays(2), CancellationToken.None);
        Assert.Equal(StoreCommandOutcome.Duplicate, firstReplay);
        Assert.Equal(Encoding.UTF8.GetBytes(resultJson), Encoding.UTF8.GetBytes(Assert.Single(await _store.GetPendingResultsAsync(10, DateTimeOffset.UtcNow, CancellationToken.None)).PayloadJson));
        var retryAt = DateTimeOffset.UtcNow.AddHours(1);
        await _store.MarkResultRetryAsync(command.CommandId, "ERP unavailable", retryAt, CancellationToken.None);
        var before = await SnapshotAsync(command.CommandId);

        var storeReplay = await _store.StoreCommandAsync(command, InitialAcknowledgedAt.AddDays(3), CancellationToken.None);
        var intakeReplay = await IntakeAsync(command, new FakeErpClient(), InitialAcknowledgedAt.AddDays(4));

        Assert.Equal(StoreCommandOutcome.Duplicate, storeReplay);
        Assert.Equal(StoreCommandOutcome.Duplicate, intakeReplay.Outcome);
        var after = await SnapshotAsync(command.CommandId);
        AssertCommandAndAttemptsUnchanged(before, after);
        Assert.Equal(before.Outbox, after.Outbox);
        var outbox = Assert.IsType<OutboxSnapshot>(after.Outbox);
        Assert.Equal("retry_waiting", outbox.Status);
        Assert.Equal(1, outbox.AttemptCount);
        Assert.Equal(retryAt.ToString("O"), outbox.NextAttemptAtUtc);
        Assert.Equal(1, await OutboxCountAsync(command.CommandId));
        Assert.Empty(await _store.GetReadyCommandsAsync(10, DateTimeOffset.UtcNow.AddDays(1), CancellationToken.None));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Active_duplicate_without_result_does_not_mutate_execution(bool executing)
    {
        var command = MakeCommand();
        var validation = CommandValidator.Validate(command, [command.CommandType], 4096);
        Assert.True(validation.IsValid);
        Assert.Equal(StoreCommandOutcome.Stored, await _store.AdmitCommandAsync(command, CommandCreatedAt, validation, CancellationToken.None));
        if (executing)
        {
            const string owner = "active-duplicate-owner";
            Assert.NotNull(await _store.TryAcquireCommandExecutionClaimAsync(command.CommandId, owner, CommandCreatedAt.AddMinutes(1), DateTimeOffset.MinValue, CancellationToken.None));
            Assert.NotNull(await _store.ClaimPostAttemptAsync(command.CommandId, owner, CancellationToken.None));
        }
        var before = await SnapshotAsync(command.CommandId);

        var result = await IntakeAsync(command, new FakeErpClient(), InitialAcknowledgedAt.AddDays(2));

        Assert.Equal(StoreCommandOutcome.Duplicate, result.Outcome);
        var after = await SnapshotAsync(command.CommandId);
        AssertCommandAndAttemptsUnchanged(before, after);
        Assert.Null(after.Outbox);
        Assert.Equal(executing ? "executing" : "queued", after.Command.Status);
        Assert.Equal(executing ? 1 : 0, after.Command.AttemptCount);
        Assert.Equal(executing ? 0 : 1, (await _store.GetReadyCommandsAsync(10, DateTimeOffset.UtcNow.AddDays(1), CancellationToken.None)).Count);
    }

    [Fact]
    public async Task Conflicting_duplicate_does_not_replay_or_overwrite_original()
    {
        var original = MakeCommand();
        var resultJson = MakeResult(original.CommandId, "succeeded");
        await CompleteAndAcknowledgeAsync(original, resultJson, CommandStatus.SucceededLocal, InitialAcknowledgedAt);
        var before = await SnapshotAsync(original.CommandId);
        var conflict = MakeCommand(original.CommandId, "{\"amount\":11}");

        var result = await IntakeAsync(conflict, new FakeErpClient(), InitialAcknowledgedAt.AddDays(2));

        Assert.Equal(StoreCommandOutcome.PayloadConflict, result.Outcome);
        var after = await SnapshotAsync(original.CommandId);
        AssertCommandAndAttemptsUnchanged(before, after);
        Assert.Equal(before.Outbox, after.Outbox);
        Assert.Equal("acknowledged", Assert.IsType<OutboxSnapshot>(after.Outbox).Status);
        Assert.Empty(await _store.GetPendingResultsAsync(10, DateTimeOffset.UtcNow, CancellationToken.None));
    }

    [Fact]
    public async Task Rejected_dead_letter_duplicate_replays_original_validation_result()
    {
        var command = MakeCommand(payloadHash: "wrong-payload-hash");
        var admission = await IntakeAsync(command, new FakeErpClient(), CommandCreatedAt);
        Assert.Equal(StoreCommandOutcome.Rejected, admission.Outcome);
        var original = await SnapshotAsync(command.CommandId);
        var originalResult = Assert.IsType<OutboxSnapshot>(original.Outbox).PayloadJson;
        await _store.AcknowledgeResultAsync(command.CommandId, InitialAcknowledgedAt, CancellationToken.None);
        var acknowledged = await SnapshotAsync(command.CommandId);

        var replay = await IntakeAsync(command, new FakeErpClient(), InitialAcknowledgedAt.AddDays(2));

        Assert.Equal(StoreCommandOutcome.Duplicate, replay.Outcome);
        var after = await SnapshotAsync(command.CommandId);
        AssertCompletedReplay(acknowledged, after, originalResult);
        Assert.Equal("dead_letter", after.Command.ResultStatus);
        Assert.Equal("completed", after.Command.Status);
        Assert.Empty(after.Attempts);
        var pending = Assert.Single(await _store.GetPendingResultsAsync(10, DateTimeOffset.UtcNow, CancellationToken.None));
        Assert.Equal(Encoding.UTF8.GetBytes(originalResult), Encoding.UTF8.GetBytes(pending.PayloadJson));
    }

    [Fact]
    public async Task Dead_letter_execution_duplicate_replays_original_terminal_result()
    {
        var command = MakeCommand();
        var resultJson = MakeResult(command.CommandId, "dead_letter");
        await CompleteAndAcknowledgeAsync(command, resultJson, CommandStatus.DeadLetter, InitialAcknowledgedAt);
        var acknowledged = await SnapshotAsync(command.CommandId);

        var replay = await IntakeAsync(command, new FakeErpClient(), InitialAcknowledgedAt.AddDays(2));

        Assert.Equal(StoreCommandOutcome.Duplicate, replay.Outcome);
        var after = await SnapshotAsync(command.CommandId);
        AssertCompletedReplay(acknowledged, after, resultJson);
        Assert.Equal("dead_letter", after.Command.ResultStatus);
        Assert.Equal("completed", after.Command.Status);
    }

    [Fact]
    public async Task Failed_received_ack_after_replay_remains_deliverable_after_restart()
    {
        var command = MakeCommand();
        var resultJson = MakeResult(command.CommandId, "succeeded");
        await CompleteAndAcknowledgeAsync(command, resultJson, CommandStatus.SucceededLocal, InitialAcknowledgedAt);
        var erp = new FakeErpClient { ThrowOnAcknowledge = true };

        var admission = await IntakeAsync(command, erp, InitialAcknowledgedAt.AddDays(2));

        Assert.Equal(StoreCommandOutcome.Duplicate, admission.Outcome);
        Assert.False(admission.Acknowledged);
        Assert.IsType<HttpRequestException>(admission.AckError);
        var beforeRestart = await SnapshotAsync(command.CommandId);
        Assert.Equal("pending", Assert.IsType<OutboxSnapshot>(beforeRestart.Outbox).Status);

        await SqliteTestDatabase.ClearPoolAsync(_factory);
        _store = new(_factory, new SqliteMigrator(_factory));
        await _store.InitializeAsync(CancellationToken.None);
        await _store.RecoverAsync(CancellationToken.None);

        var afterRestart = await SnapshotAsync(command.CommandId);
        AssertCommandAndAttemptsUnchanged(beforeRestart, afterRestart);
        Assert.Equal(beforeRestart.Outbox, afterRestart.Outbox);
        var pending = Assert.Single(await _store.GetPendingResultsAsync(10, DateTimeOffset.UtcNow, CancellationToken.None));
        Assert.Equal(Encoding.UTF8.GetBytes(resultJson), Encoding.UTF8.GetBytes(pending.PayloadJson));
        Assert.Equal(PayloadHasher.ComputeBytes(Encoding.UTF8.GetBytes(resultJson)), Assert.IsType<OutboxSnapshot>(afterRestart.Outbox).PayloadHash);
    }

    [Fact]
    public async Task Delivery_ack_stops_replay_and_updates_last_confirmation_before_cleanup()
    {
        var command = MakeCommand();
        var resultJson = MakeResult(command.CommandId, "succeeded");
        await CompleteAndAcknowledgeAsync(command, resultJson, CommandStatus.SucceededLocal, InitialAcknowledgedAt);
        Assert.Equal(StoreCommandOutcome.Duplicate, (await IntakeAsync(command, new FakeErpClient(), InitialAcknowledgedAt.AddDays(2))).Outcome);
        Assert.Equal(Encoding.UTF8.GetBytes(resultJson), Encoding.UTF8.GetBytes(Assert.Single(await _store.GetPendingResultsAsync(10, DateTimeOffset.UtcNow, CancellationToken.None)).PayloadJson));
        var newAcknowledgedAt = InitialAcknowledgedAt.AddDays(2);

        await _store.AcknowledgeResultAsync(command.CommandId, newAcknowledgedAt, CancellationToken.None);

        var acknowledged = await SnapshotAsync(command.CommandId);
        Assert.Equal("completed", acknowledged.Command.Status);
        Assert.Equal(newAcknowledgedAt.ToString("O"), acknowledged.Command.ErpAcknowledgedAtUtc);
        var outbox = Assert.IsType<OutboxSnapshot>(acknowledged.Outbox);
        Assert.Equal("acknowledged", outbox.Status);
        Assert.Equal(newAcknowledgedAt.ToString("O"), outbox.AcknowledgedAtUtc);
        Assert.Equal(newAcknowledgedAt.ToString("O"), outbox.SentAtUtc);
        Assert.Empty(await _store.GetPendingResultsAsync(10, DateTimeOffset.UtcNow, CancellationToken.None));

        await _store.CleanupAsync(newAcknowledgedAt.AddSeconds(1), DateTimeOffset.UtcNow, CancellationToken.None);

        Assert.Equal((0L, 0L, 0L), await CountsAsync());
    }

    [Fact]
    public async Task Cleanup_with_old_ack_retains_completed_command_attempt_and_pending_replay()
    {
        var command = MakeCommand();
        var resultJson = MakeResult(command.CommandId, "succeeded");
        await CompleteAndAcknowledgeAsync(command, resultJson, CommandStatus.SucceededLocal, InitialAcknowledgedAt);
        Assert.Equal(StoreCommandOutcome.Duplicate, (await IntakeAsync(command, new FakeErpClient(), InitialAcknowledgedAt.AddDays(2))).Outcome);
        var pendingReplay = await SnapshotAsync(command.CommandId);

        await _store.CleanupAsync(InitialAcknowledgedAt.AddDays(1), DateTimeOffset.UtcNow, CancellationToken.None);

        var retained = await SnapshotAsync(command.CommandId);
        AssertCommandAndAttemptsUnchanged(pendingReplay, retained);
        Assert.Equal(pendingReplay.Outbox, retained.Outbox);
        Assert.Equal("completed", retained.Command.Status);
        Assert.Equal("pending", Assert.IsType<OutboxSnapshot>(retained.Outbox).Status);
        Assert.Equal(InitialAcknowledgedAt.ToString("O"), retained.Command.ErpAcknowledgedAtUtc);
        Assert.Equal(InitialAcknowledgedAt.ToString("O"), retained.Outbox.AcknowledgedAtUtc);
        Assert.Single(retained.Attempts);
        Assert.Equal((1L, 1L, 1L), await CountsAsync());
    }

    [Fact]
    public async Task Cleanup_with_sending_replay_retains_history_and_restart_delivers_exact_result()
    {
        var command = MakeCommand();
        var resultJson = MakeResult(command.CommandId, "succeeded");
        await CompleteAndAcknowledgeAsync(command, resultJson, CommandStatus.SucceededLocal, InitialAcknowledgedAt);
        Assert.Equal(StoreCommandOutcome.Duplicate, (await IntakeAsync(command, new FakeErpClient(), InitialAcknowledgedAt.AddDays(2))).Outcome);
        var beforeSending = await SnapshotAsync(command.CommandId);
        var originalOutbox = Assert.IsType<OutboxSnapshot>(beforeSending.Outbox);

        await SetOutboxStatusAsync(command.CommandId, "sending");
        Assert.Equal("sending", Assert.IsType<OutboxSnapshot>((await SnapshotAsync(command.CommandId)).Outbox).Status);
        await _store.CleanupAsync(InitialAcknowledgedAt.AddDays(1), DateTimeOffset.UtcNow, CancellationToken.None);

        await SqliteTestDatabase.ClearPoolAsync(_factory);
        _store = new(_factory, new SqliteMigrator(_factory));
        await _store.InitializeAsync(CancellationToken.None);
        await _store.RecoverAsync(CancellationToken.None);

        var recovered = await SnapshotAsync(command.CommandId);
        AssertCommandAndAttemptsUnchanged(beforeSending, recovered);
        var recoveredOutbox = Assert.IsType<OutboxSnapshot>(recovered.Outbox);
        Assert.Equal(originalOutbox.ResultId, recoveredOutbox.ResultId);
        Assert.Equal(Encoding.UTF8.GetBytes(resultJson), Encoding.UTF8.GetBytes(recoveredOutbox.PayloadJson));
        Assert.Equal(originalOutbox.PayloadHash, recoveredOutbox.PayloadHash);
        Assert.Equal(originalOutbox.AttemptCount, recoveredOutbox.AttemptCount);
        Assert.Equal(originalOutbox.CreatedAtUtc, recoveredOutbox.CreatedAtUtc);
        Assert.Equal(originalOutbox.AcknowledgedAtUtc, recoveredOutbox.AcknowledgedAtUtc);
        Assert.Equal("pending", recoveredOutbox.Status);
        var pending = Assert.Single(await _store.GetPendingResultsAsync(10, DateTimeOffset.UtcNow, CancellationToken.None));
        Assert.Equal(Encoding.UTF8.GetBytes(resultJson), Encoding.UTF8.GetBytes(pending.PayloadJson));
        Assert.Single(recovered.Attempts);
        Assert.Equal((1L, 1L, 1L), await CountsAsync());
    }

    [Fact]
    public async Task Missing_outbox_is_reconstructed_only_from_persisted_command_result()
    {
        var command = MakeCommand();
        var resultJson = MakeResult(command.CommandId, "succeeded");
        await CompleteAndAcknowledgeAsync(command, resultJson, CommandStatus.SucceededLocal, InitialAcknowledgedAt);
        var before = await SnapshotAsync(command.CommandId);
        await DeleteOutboxAsync(command.CommandId);
        Assert.Null((await SnapshotAsync(command.CommandId)).Outbox);

        var replay = await IntakeAsync(command, new FakeErpClient(), InitialAcknowledgedAt.AddDays(2));

        Assert.Equal(StoreCommandOutcome.Duplicate, replay.Outcome);
        var after = await SnapshotAsync(command.CommandId);
        AssertCommandAndAttemptsUnchanged(before, after);
        var reconstructed = Assert.IsType<OutboxSnapshot>(after.Outbox);
        Assert.NotEmpty(reconstructed.ResultId);
        Assert.Equal(Encoding.UTF8.GetBytes(resultJson), Encoding.UTF8.GetBytes(reconstructed.PayloadJson));
        Assert.Equal(PayloadHasher.ComputeBytes(Encoding.UTF8.GetBytes(resultJson)), reconstructed.PayloadHash);
        Assert.Equal("pending", reconstructed.Status);
        Assert.Equal(1, await OutboxCountAsync(command.CommandId));
    }

    private async Task CompleteAndAcknowledgeAsync(CommandEnvelope command, string resultJson, CommandStatus localStatus, DateTimeOffset acknowledgedAt)
    {
        var validation = CommandValidator.Validate(command, [command.CommandType], 4096);
        Assert.True(validation.IsValid);
        Assert.Equal(StoreCommandOutcome.Stored, await _store.AdmitCommandAsync(command, CommandCreatedAt, validation, CancellationToken.None));
        var owner = "replay-owner-" + Guid.NewGuid().ToString("N");
        Assert.NotNull(await _store.TryAcquireCommandExecutionClaimAsync(command.CommandId, owner, CommandCreatedAt.AddMinutes(1), DateTimeOffset.MinValue, CancellationToken.None));
        var attempt = await _store.ClaimPostAttemptAsync(command.CommandId, owner, CancellationToken.None);
        Assert.NotNull(attempt);
        Assert.True(await _store.CompleteLocallyAsync(command.CommandId, owner, localStatus, resultJson, "ref-1", "42", attempt.AttemptId, CancellationToken.None));
        await _store.AcknowledgeResultAsync(command.CommandId, acknowledgedAt, CancellationToken.None);
    }

    private Task<CommandIntakeResult> IntakeAsync(CommandEnvelope command, FakeErpClient erp, DateTimeOffset receivedAtUtc)
    {
        var leaseCommand = JsonSerializer.SerializeToElement(command, JsonOptions);
        return new CommandIntakeService(erp, _store).IntakeAsync(Guid.NewGuid(), leaseCommand, [command.CommandType], 4096, receivedAtUtc, CancellationToken.None);
    }

    private static void AssertCompletedReplay(Snapshot acknowledged, Snapshot replayed, string resultJson)
    {
        AssertCommandAndAttemptsUnchanged(acknowledged, replayed);
        var before = Assert.IsType<OutboxSnapshot>(acknowledged.Outbox);
        var after = Assert.IsType<OutboxSnapshot>(replayed.Outbox);
        Assert.Equal(before.ResultId, after.ResultId);
        Assert.Equal(Encoding.UTF8.GetBytes(resultJson), Encoding.UTF8.GetBytes(after.PayloadJson));
        Assert.Equal(before.PayloadHash, after.PayloadHash);
        Assert.Equal(before.AttemptCount, after.AttemptCount);
        Assert.Equal(before.CreatedAtUtc, after.CreatedAtUtc);
        Assert.Equal(before.AcknowledgedAtUtc, after.AcknowledgedAtUtc);
        Assert.Equal("acknowledged", before.Status);
        Assert.Equal("pending", after.Status);
    }

    private static void AssertCommandAndAttemptsUnchanged(Snapshot expected, Snapshot actual)
    {
        Assert.Equal(expected.Command, actual.Command);
        Assert.Equal(expected.Attempts, actual.Attempts);
    }

    private async Task<Snapshot> SnapshotAsync(Guid commandId)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        CommandState commandState;
        await using (var command = connection.CreateCommand())
        {
            command.CommandText = """
                SELECT command_id,command_type,payload_version,priority,ordering_key,correlation_id,payload_json,payload_hash,status,created_at_utc,received_at_utc,not_before_utc,expires_at_utc,started_at_utc,finished_at_utc,attempt_count,next_attempt_at_utc,result_status,result_json,external_ref,external_number,last_error_code,last_error_message,erp_acknowledged_at_utc,row_version,first_sent_at_utc,lookup_attempt_count,post_attempt_count,queue_sequence,exec_claim_owner_id,exec_claim_acquired_at_utc
                FROM commands_inbox
                WHERE command_id=$id;
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
        return new(commandState, attempts.ToArray(), outbox);
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

    private async Task DeleteOutboxAsync(Guid commandId)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = "DELETE FROM results_outbox WHERE command_id=$id;";
        command.Parameters.AddWithValue("$id", commandId.ToString("D"));
        Assert.Equal(1, await command.ExecuteNonQueryAsync(CancellationToken.None));
    }

    private async Task<long> OutboxCountAsync(Guid commandId)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT COUNT(*) FROM results_outbox WHERE command_id=$id;";
        command.Parameters.AddWithValue("$id", commandId.ToString("D"));
        return Convert.ToInt64(await command.ExecuteScalarAsync(CancellationToken.None), System.Globalization.CultureInfo.InvariantCulture);
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

    private static CommandEnvelope MakeCommand(Guid? commandId = null, string payloadJson = "{\"amount\":10}", string? payloadHash = null)
    {
        using var document = JsonDocument.Parse(payloadJson);
        var payload = document.RootElement.Clone();
        return new(
            commandId ?? Guid.NewGuid(), "create_customer_order", 1, 100, "order:replay-42", null, CommandCreatedAt,
            null, CommandCreatedAt.AddHours(1), null, payloadHash ?? PayloadHasher.Compute(payload), payload);
    }

    private static string MakeResult(Guid commandId, string status) =>
        $"{{\"commandId\":\"{commandId:D}\",\"status\":\"{status}\",\"completedAtUtc\":\"2026-09-24T08:05:00+00:00\",\"document\":{{\"ref\":\"DOC-42\",\"number\":\"42\"}},\"warnings\":[],\"resultVersion\":1}}";

    private static string? NullableString(SqliteDataReader reader, int ordinal) => reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);

    private static int? NullableInt(SqliteDataReader reader, int ordinal) => reader.IsDBNull(ordinal) ? null : reader.GetInt32(ordinal);

    private sealed record Snapshot(CommandState Command, AttemptSnapshot[] Attempts, OutboxSnapshot? Outbox);

    private sealed record CommandState(
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

    private sealed class FakeErpClient : IErpClient
    {
        public Func<Task>? BeforeAcknowledge { get; set; }
        public bool ThrowOnAcknowledge { get; set; }
        public List<(Guid CommandId, CommandReceivedRequest Request)> Acks { get; } = [];

        public Task AcknowledgeReceivedAsync(Guid commandId, CommandReceivedRequest request, CancellationToken cancellationToken)
        {
            Acks.Add((commandId, request));
            if (ThrowOnAcknowledge) return Task.FromException(new HttpRequestException("received ACK failed"));
            return BeforeAcknowledge?.Invoke() ?? Task.CompletedTask;
        }

        public Task<LeaseResponse> LeaseCommandAsync(LeaseRequest request, CancellationToken cancellationToken) =>
            Task.FromResult(new LeaseResponse(false, null, null, null));

        public Task<SessionStartResponse> StartSessionAsync(SessionStartRequest request, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public Task AcknowledgeResultAsync(Guid commandId, string resultJson, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public Task<BatchAcknowledgement> UploadBatchAsync(EtlBatch batch, Stream content, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public Task CompleteEtlRunAsync(Guid runId, object summary, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public Task SendHeartbeatAsync(HeartbeatRequest request, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public Task<RemoteConfigurationResponse?> GetConfigurationAsync(long currentVersion, CancellationToken cancellationToken) =>
            throw new NotSupportedException();
    }
}
