using System.Globalization;
using System.Text.Json;
using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Domain.Commands;
using ErpOnecAgent.Domain.Common;
using ErpOnecAgent.Infrastructure.Persistence.Sqlite;
using Microsoft.Data.Sqlite;
using Xunit;

namespace ErpOnecAgent.IntegrationTests;

public sealed class CommandClaimRecoveryTests : IAsyncLifetime
{
    private static readonly DateTimeOffset NoTimeTakeover = DateTimeOffset.MinValue;

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
    public async Task Queued_claim_crash_before_send_recovers_without_creating_send_evidence()
    {
        var command = MakeCommand("queued", expiresAtUtc: UtcNow().AddMinutes(-1));
        await _store.StoreCommandAsync(command, command.CreatedAtUtc, CancellationToken.None);

        var claim = await AcquireAsync(command.CommandId, "crashed-queued", UtcNow());
        Assert.NotNull(claim);
        Assert.Equal(CommandStatus.Queued, claim.CurrentStatus);
        Assert.Equal(0, claim.CurrentAttemptCount);
        Assert.Equal(0, claim.CurrentLookupAttemptCount);
        Assert.Equal(0, claim.CurrentPostAttemptCount);
        Assert.Null(claim.CurrentFirstSentAtUtc);

        var crashed = await SnapshotAsync(command.CommandId);
        Assert.Equal("crashed-queued", crashed.ClaimOwner);
        await RestartAndRecoverAsync();

        var recovered = await SnapshotAsync(command.CommandId);
        var expected = crashed with
        {
            ClaimOwner = null,
            ClaimAcquiredAtUtc = null,
            RowVersion = crashed.RowVersion + 1,
        };
        AssertSnapshotsEqual(expected, recovered);
        Assert.Equal("queued", recovered.Status);
        Assert.Null(recovered.FirstSentAtUtc);
        Assert.Empty(recovered.Attempts);
        Assert.Equal(0, recovered.AttemptCount);
        Assert.Equal(0, recovered.LookupAttemptCount);
        Assert.Equal(0, recovered.PostAttemptCount);
        Assert.Equal(command.ExpiresAtUtc, ParseNullableDate(recovered.ExpiresAtUtc));
        await AssertSecondRecoveryIsNoOpAsync(command.CommandId);

        var reacquired = await AcquireAsync(command.CommandId, "restarted-queued", UtcNow());
        Assert.NotNull(reacquired);
    }

    [Fact]
    public async Task Retry_waiting_claim_releases_and_preserves_prior_send_and_schedule()
    {
        var retryAt = UtcNow().AddMinutes(-1);
        var command = MakeCommand("retry");
        await _store.StoreCommandAsync(command, command.CreatedAtUtc, CancellationToken.None);
        var priorClaim = await AcquireAsync(command.CommandId, "prior-pass", UtcNow());
        Assert.NotNull(priorClaim);
        var post = await _store.ClaimPostAttemptAsync(command.CommandId, "prior-pass", CancellationToken.None);
        Assert.NotNull(post);
        await _store.CompleteAttemptAsync(post.AttemptId, CommandStatus.UnknownResult, "TECHNICAL", "transient", 503, CancellationToken.None);
        Assert.True(await _store.ScheduleRetryAsync(command.CommandId, "prior-pass", "TECHNICAL", "transient", retryAt, CancellationToken.None));

        var crashedClaim = await AcquireAsync(command.CommandId, "crashed-retry", UtcNow());
        Assert.NotNull(crashedClaim);
        Assert.Equal(CommandStatus.RetryWaiting, crashedClaim.CurrentStatus);
        var crashed = await SnapshotAsync(command.CommandId);
        Assert.Equal("crashed-retry", crashed.ClaimOwner);

        await RestartAndRecoverAsync();

        var recovered = await SnapshotAsync(command.CommandId);
        var expected = crashed with
        {
            ClaimOwner = null,
            ClaimAcquiredAtUtc = null,
            RowVersion = crashed.RowVersion + 1,
        };
        AssertSnapshotsEqual(expected, recovered);
        Assert.Equal("retry_waiting", recovered.Status);
        Assert.Equal(retryAt.ToString("O"), recovered.NextAttemptAtUtc);
        Assert.NotNull(recovered.FirstSentAtUtc);
        Assert.Equal(1, recovered.AttemptCount);
        Assert.Equal(1, recovered.PostAttemptCount);
        Assert.Single(recovered.Attempts);
        await AssertSecondRecoveryIsNoOpAsync(command.CommandId);

        var reacquired = await AcquireAsync(command.CommandId, "restarted-retry", UtcNow());
        Assert.NotNull(reacquired);
        Assert.Equal(CommandStatus.RetryWaiting, reacquired.CurrentStatus);
    }

    [Fact]
    public async Task Unknown_result_lookup_claim_releases_without_bypassing_future_backoff()
    {
        var dueAt = UtcNow().AddMinutes(-1);
        var futureBackoff = UtcNow().AddHours(1);
        var command = MakeCommand("unknown");
        await _store.StoreCommandAsync(command, command.CreatedAtUtc, CancellationToken.None);
        var priorClaim = await AcquireAsync(command.CommandId, "prior-unknown", UtcNow());
        Assert.NotNull(priorClaim);
        var post = await _store.ClaimPostAttemptAsync(command.CommandId, "prior-unknown", CancellationToken.None);
        Assert.NotNull(post);
        await _store.CompleteAttemptAsync(post.AttemptId, CommandStatus.UnknownResult, "AMBIGUOUS", "outcome unknown", null, CancellationToken.None);
        Assert.True(await _store.MarkUnknownResultAsync(command.CommandId, post.AttemptId, "prior-unknown", "AMBIGUOUS", "outcome unknown", dueAt, CancellationToken.None));

        var lookupOwnerClaim = await AcquireAsync(command.CommandId, "crashed-lookup", UtcNow());
        Assert.NotNull(lookupOwnerClaim);
        Assert.Equal(CommandStatus.UnknownResult, lookupOwnerClaim.CurrentStatus);
        var lookup = await _store.ClaimLookupAttemptAsync(command.CommandId, "crashed-lookup", "STATUS_PENDING", "1C result is still unknown.", futureBackoff, CancellationToken.None);
        Assert.NotNull(lookup);
        var crashed = await SnapshotAsync(command.CommandId);
        Assert.Equal("crashed-lookup", crashed.ClaimOwner);

        await RestartAndRecoverAsync();

        var recovered = await SnapshotAsync(command.CommandId);
        var expected = crashed with
        {
            ClaimOwner = null,
            ClaimAcquiredAtUtc = null,
            RowVersion = crashed.RowVersion + 1,
        };
        AssertSnapshotsEqual(expected, recovered);
        Assert.Equal("unknown_result", recovered.Status);
        Assert.Equal(futureBackoff.ToString("O"), recovered.NextAttemptAtUtc);
        Assert.Equal(1, recovered.LookupAttemptCount);
        Assert.Equal(1, recovered.PostAttemptCount);
        Assert.Equal(2, recovered.Attempts.Length);
        var recoveredLookup = Assert.Single(recovered.Attempts, attempt => attempt.AttemptKind == "lookup");
        Assert.Equal(lookup.AttemptId.ToString("D"), recoveredLookup.AttemptId);
        Assert.Null(recoveredLookup.FinishedAtUtc);
        await AssertSecondRecoveryIsNoOpAsync(command.CommandId);

        Assert.Null(await AcquireAsync(command.CommandId, "too-early", UtcNow()));
        var reacquired = await AcquireAsync(command.CommandId, "restarted-unknown", futureBackoff);
        Assert.NotNull(reacquired);
        Assert.Equal(1, reacquired.CurrentLookupAttemptCount);
    }

    [Fact]
    public async Task Executing_claim_recovers_to_unknown_result_and_preserves_first_send_evidence()
    {
        var command = MakeCommand("executing");
        await _store.StoreCommandAsync(command, command.CreatedAtUtc, CancellationToken.None);
        var claim = await AcquireAsync(command.CommandId, "crashed-executing", UtcNow());
        Assert.NotNull(claim);
        var post = await _store.ClaimPostAttemptAsync(command.CommandId, "crashed-executing", CancellationToken.None);
        Assert.NotNull(post);
        var crashed = await SnapshotAsync(command.CommandId);
        Assert.Equal("executing", crashed.Status);
        Assert.Equal("crashed-executing", crashed.ClaimOwner);

        var recoveryStartedAt = UtcNow();
        await RestartAndRecoverAsync();
        var recoveryFinishedAt = UtcNow();

        var recovered = await SnapshotAsync(command.CommandId);
        var expected = crashed with
        {
            Status = "unknown_result",
            NextAttemptAtUtc = recovered.NextAttemptAtUtc,
            LastErrorCode = "PROCESS_RESTART",
            LastErrorMessage = "Agent restarted during command execution",
            ClaimOwner = null,
            ClaimAcquiredAtUtc = null,
            RowVersion = crashed.RowVersion + 1,
        };
        AssertSnapshotsEqual(expected, recovered);
        Assert.NotNull(recovered.NextAttemptAtUtc);
        var recoveredAt = ParseDate(recovered.NextAttemptAtUtc);
        Assert.InRange(recoveredAt, recoveryStartedAt, recoveryFinishedAt);
        Assert.Equal(crashed.StartedAtUtc, recovered.StartedAtUtc);
        Assert.Equal(crashed.FirstSentAtUtc, recovered.FirstSentAtUtc);
        Assert.Equal(1, recovered.AttemptCount);
        Assert.Equal(1, recovered.PostAttemptCount);
        Assert.Single(recovered.Attempts);
        Assert.Null(recovered.Outbox);
        await AssertSecondRecoveryIsNoOpAsync(command.CommandId);

        var reacquired = await AcquireAsync(command.CommandId, "restarted-executing", recoveredAt);
        Assert.NotNull(reacquired);
        Assert.Equal(CommandStatus.UnknownResult, reacquired.CurrentStatus);
    }

    [Fact]
    public async Task Terminal_commands_results_and_outboxes_remain_exactly_unchanged()
    {
        var pending = MakeCommand("terminal-pending");
        await _store.StoreCommandAsync(pending, pending.CreatedAtUtc, CancellationToken.None);
        var pendingClaim = await AcquireAsync(pending.CommandId, "pending-terminal", UtcNow());
        Assert.NotNull(pendingClaim);
        var pendingPost = await _store.ClaimPostAttemptAsync(pending.CommandId, "pending-terminal", CancellationToken.None);
        Assert.NotNull(pendingPost);
        const string pendingResult = "{\"status\":\"succeeded\",\"marker\":\"terminal-pending\"}";
        Assert.True(await _store.CompleteLocallyAsync(pending.CommandId, "pending-terminal", CommandStatus.SucceededLocal, pendingResult, "ref-pending", "number-pending", pendingPost.AttemptId, CancellationToken.None));

        var acknowledged = MakeCommand("terminal-acknowledged");
        await _store.StoreCommandAsync(acknowledged, acknowledged.CreatedAtUtc, CancellationToken.None);
        var acknowledgedClaim = await AcquireAsync(acknowledged.CommandId, "acknowledged-terminal", UtcNow());
        Assert.NotNull(acknowledgedClaim);
        var acknowledgedPost = await _store.ClaimPostAttemptAsync(acknowledged.CommandId, "acknowledged-terminal", CancellationToken.None);
        Assert.NotNull(acknowledgedPost);
        const string acknowledgedResult = "{\"status\":\"business_error\",\"marker\":\"terminal-acknowledged\"}";
        Assert.True(await _store.CompleteLocallyAsync(acknowledged.CommandId, "acknowledged-terminal", CommandStatus.BusinessFailedLocal, acknowledgedResult, null, null, acknowledgedPost.AttemptId, CancellationToken.None));
        await _store.AcknowledgeResultAsync(acknowledged.CommandId, UtcNow(), CancellationToken.None);

        var before = await SnapshotsAsync(pending.CommandId, acknowledged.CommandId);
        await RestartAndRecoverAsync();
        var after = await SnapshotsAsync(pending.CommandId, acknowledged.CommandId);
        AssertSnapshotsEqual(before, after);
        var pendingAfter = Assert.Single(after, snapshot => snapshot.CommandId == pending.CommandId.ToString("D"));
        var acknowledgedAfter = Assert.Single(after, snapshot => snapshot.CommandId == acknowledged.CommandId.ToString("D"));
        Assert.Equal("result_pending", pendingAfter.Status);
        Assert.Equal("completed", acknowledgedAfter.Status);
        Assert.Equal("pending", pendingAfter.Outbox?.Status);
        Assert.Equal("acknowledged", acknowledgedAfter.Outbox?.Status);
        await AssertSecondRecoveryIsNoOpAsync(pending.CommandId, acknowledged.CommandId);
    }

    private async Task<ExecutionClaim?> AcquireAsync(Guid commandId, string ownerId, DateTimeOffset acquiredAtUtc) =>
        await _store.TryAcquireCommandExecutionClaimAsync(commandId, ownerId, acquiredAtUtc, NoTimeTakeover, CancellationToken.None);

    private async Task RestartAndRecoverAsync()
    {
        await SqliteTestDatabase.ClearPoolAsync(_factory);
        _store = new(_factory, new SqliteMigrator(_factory));
        await _store.InitializeAsync(CancellationToken.None);
        await _store.RecoverAsync(CancellationToken.None);
    }

    private async Task AssertSecondRecoveryIsNoOpAsync(params Guid[] commandIds)
    {
        var before = await SnapshotsAsync(commandIds);
        await _store.RecoverAsync(CancellationToken.None);
        var after = await SnapshotsAsync(commandIds);
        AssertSnapshotsEqual(before, after);
    }

    private async Task<CommandSnapshot[]> SnapshotsAsync(params Guid[] commandIds)
    {
        var snapshots = new List<CommandSnapshot>(commandIds.Length);
        foreach (var commandId in commandIds.Distinct().Order())
        {
            snapshots.Add(await SnapshotAsync(commandId));
        }
        return snapshots.ToArray();
    }

    private async Task<CommandSnapshot> SnapshotAsync(Guid commandId)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        CommandSnapshot commandState;
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
                reader.GetInt32(27), reader.GetInt64(28), NullableString(reader, 29), NullableString(reader, 30), [], null);
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
                    NullableInt(reader, 5), NullableString(reader, 6), NullableString(reader, 7), NullableString(reader, 8),
                    NullableInt(reader, 9), reader.GetString(10)));
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
        return commandState with { Attempts = attempts.ToArray(), Outbox = outbox };
    }

    private static void AssertSnapshotsEqual(CommandSnapshot expected, CommandSnapshot actual)
    {
        Assert.Equal(expected with { Attempts = [], Outbox = null }, actual with { Attempts = [], Outbox = null });
        Assert.Equal(expected.Attempts.Length, actual.Attempts.Length);
        for (var index = 0; index < expected.Attempts.Length; index++)
        {
            Assert.Equal(expected.Attempts[index], actual.Attempts[index]);
        }
        Assert.Equal(expected.Outbox, actual.Outbox);
    }

    private static void AssertSnapshotsEqual(CommandSnapshot[] expected, CommandSnapshot[] actual)
    {
        Assert.Equal(expected.Length, actual.Length);
        for (var index = 0; index < expected.Length; index++)
        {
            AssertSnapshotsEqual(expected[index], actual[index]);
        }
    }

    private static CommandEnvelope MakeCommand(string name, DateTimeOffset? expiresAtUtc = null)
    {
        using var document = JsonDocument.Parse("{\"amount\":10}");
        var payload = document.RootElement.Clone();
        var receivedAt = UtcNow().AddMinutes(-5);
        return new(
            Guid.NewGuid(), "create_customer_order", 1, 100, $"claim-recovery:{name}", null, receivedAt,
            receivedAt.AddMinutes(-1), expiresAtUtc, null, PayloadHasher.Compute(payload), payload);
    }

    private static string? NullableString(SqliteDataReader reader, int ordinal) => reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);

    private static int? NullableInt(SqliteDataReader reader, int ordinal) => reader.IsDBNull(ordinal) ? null : reader.GetInt32(ordinal);

    private static DateTimeOffset UtcNow() => DateTimeOffset.UtcNow;

    private static DateTimeOffset ParseDate(string value) => DateTimeOffset.Parse(value, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind).ToUniversalTime();

    private static DateTimeOffset? ParseNullableDate(string? value) => value is null ? null : ParseDate(value);

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
        string? ClaimAcquiredAtUtc,
        AttemptSnapshot[] Attempts,
        OutboxSnapshot? Outbox);
}
