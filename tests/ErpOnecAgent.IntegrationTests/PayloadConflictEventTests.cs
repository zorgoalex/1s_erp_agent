using System.Globalization;
using System.Security.Cryptography;
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

public sealed class PayloadConflictEventTests : IAsyncLifetime
{
    private static readonly DateTimeOffset FirstSeen = new(2026, 9, 24, 10, 0, 0, TimeSpan.Zero);
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
    public async Task Real_sqlite_conflict_is_returned_and_event_is_durable()
    {
        var original = MakeCommand();
        var conflict = MakeCommand(original.CommandId, "{\"amount\":11}");

        Assert.Equal(StoreCommandOutcome.Stored, await _store.StoreCommandAsync(original, DateTimeOffset.UtcNow, CancellationToken.None));
        Assert.Equal(StoreCommandOutcome.PayloadConflict, await _store.StoreCommandAsync(conflict, DateTimeOffset.UtcNow, CancellationToken.None));

        Assert.True(await HasConflictTableAsync());
        Assert.Equal(1L, await ScalarAsync("SELECT COUNT(*) FROM command_payload_conflicts"));
    }

    [Theory]
    [InlineData(false, "queued")]
    [InlineData(false, "executing")]
    [InlineData(false, "result_pending")]
    [InlineData(false, "completed")]
    [InlineData(true, "queued")]
    [InlineData(true, "executing")]
    [InlineData(true, "result_pending")]
    [InlineData(true, "completed")]
    public async Task Conflict_preserves_original_across_store_and_admit_paths_and_states(bool admit, string originalStatus)
    {
        var original = MakeCommand();
        Assert.Equal(StoreCommandOutcome.Stored, await _store.StoreCommandAsync(original, FirstSeen, CancellationToken.None));
        await PrepareOriginalStatusAsync(original, originalStatus);
        var before = await SnapshotOriginalStateAsync(original.CommandId);
        var conflict = MakeCommand(original.CommandId, "{\"amount\":11}");
        var receivedAt = FirstSeen.AddHours(4);

        var outcome = admit
            ? await _store.AdmitCommandAsync(conflict, receivedAt, ValidationResult.Success, CancellationToken.None)
            : await _store.StoreCommandAsync(conflict, receivedAt, CancellationToken.None);

        Assert.Equal(StoreCommandOutcome.PayloadConflict, outcome);
        Assert.Equal(before, await SnapshotOriginalStateAsync(original.CommandId));
        var recorded = Assert.Single(await _store.GetCommandPayloadConflictEventsAsync(10, CancellationToken.None));
        Assert.Equal(original.CommandId, recorded.CommandId);
        Assert.Equal("COMMAND_PAYLOAD_CONFLICT", recorded.Code);
        Assert.Equal("critical", recorded.Severity);
        Assert.Equal(originalStatus, recorded.OriginalCommandStatus);
        Assert.Equal(Fingerprint(original.PayloadHash), recorded.OriginalDeclaredHashFingerprint);
        Assert.Equal(Fingerprint(conflict.PayloadHash), recorded.IncomingDeclaredHashFingerprint);
        Assert.Equal(receivedAt, recorded.FirstSeenAtUtc);
        Assert.Equal(receivedAt, recorded.LastSeenAtUtc);
        Assert.Equal(1, recorded.OccurrenceCount);
    }

    [Fact]
    public async Task Same_hash_duplicates_do_not_create_conflict_events()
    {
        var original = MakeCommand();
        Assert.Equal(StoreCommandOutcome.Stored, await _store.AdmitCommandAsync(original, FirstSeen, ValidationResult.Success, CancellationToken.None));

        Assert.Equal(StoreCommandOutcome.Duplicate, await _store.StoreCommandAsync(original, FirstSeen.AddHours(1), CancellationToken.None));
        Assert.Equal(StoreCommandOutcome.Duplicate, await _store.AdmitCommandAsync(original, FirstSeen.AddHours(2), ValidationResult.Success, CancellationToken.None));

        Assert.Empty(await _store.GetCommandPayloadConflictEventsAsync(10, CancellationToken.None));
    }

    [Fact]
    public async Task Repeated_pair_aggregates_without_timestamp_regression_and_distinct_pair_is_separate()
    {
        var original = MakeCommand();
        var firstConflict = MakeCommand(original.CommandId, "{\"amount\":11}");
        var secondConflict = MakeCommand(original.CommandId, "{\"amount\":12}");
        Assert.Equal(StoreCommandOutcome.Stored, await _store.StoreCommandAsync(original, FirstSeen, CancellationToken.None));

        Assert.Equal(StoreCommandOutcome.PayloadConflict, await _store.StoreCommandAsync(firstConflict, FirstSeen, CancellationToken.None));
        Assert.Equal(StoreCommandOutcome.PayloadConflict, await _store.StoreCommandAsync(firstConflict, FirstSeen.AddHours(2), CancellationToken.None));
        Assert.Equal(StoreCommandOutcome.PayloadConflict, await _store.StoreCommandAsync(firstConflict, FirstSeen.AddHours(1), CancellationToken.None));
        Assert.Equal(StoreCommandOutcome.PayloadConflict, await _store.StoreCommandAsync(secondConflict, FirstSeen.AddHours(3), CancellationToken.None));

        var events = await _store.GetCommandPayloadConflictEventsAsync(10, CancellationToken.None);
        Assert.Equal(2, events.Count);
        var second = events[0];
        var first = events[1];
        Assert.Equal(Fingerprint(secondConflict.PayloadHash), second.IncomingDeclaredHashFingerprint);
        Assert.Equal(1, second.OccurrenceCount);
        Assert.Equal(FirstSeen.AddHours(3), second.FirstSeenAtUtc);
        Assert.Equal(FirstSeen.AddHours(3), second.LastSeenAtUtc);
        Assert.Equal(Fingerprint(firstConflict.PayloadHash), first.IncomingDeclaredHashFingerprint);
        Assert.Equal(3, first.OccurrenceCount);
        Assert.Equal(FirstSeen, first.FirstSeenAtUtc);
        Assert.Equal(FirstSeen.AddHours(2), first.LastSeenAtUtc);
        Assert.Equal(second.EventId, Assert.Single(await _store.GetCommandPayloadConflictEventsAsync(1, CancellationToken.None)).EventId);
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => _store.GetCommandPayloadConflictEventsAsync(0, CancellationToken.None));
    }

    [Fact]
    public async Task Conflict_evidence_survives_store_restart()
    {
        var original = MakeCommand();
        var conflict = MakeCommand(original.CommandId, "{\"amount\":11}");
        Assert.Equal(StoreCommandOutcome.Stored, await _store.StoreCommandAsync(original, FirstSeen, CancellationToken.None));
        Assert.Equal(StoreCommandOutcome.PayloadConflict, await _store.StoreCommandAsync(conflict, FirstSeen.AddHours(1), CancellationToken.None));
        var before = Assert.Single(await _store.GetCommandPayloadConflictEventsAsync(10, CancellationToken.None));

        await SqliteTestDatabase.ClearPoolAsync(_factory);
        _store = new(_factory, new SqliteMigrator(_factory));
        await _store.InitializeAsync(CancellationToken.None);
        await _store.RecoverAsync(CancellationToken.None);

        Assert.Equal(before, Assert.Single(await _store.GetCommandPayloadConflictEventsAsync(10, CancellationToken.None)));
    }

    [Fact]
    public async Task Failed_event_insert_rolls_back_and_intake_does_not_ack()
    {
        var original = MakeCommand();
        var conflict = MakeCommand(original.CommandId, "{\"amount\":11}");
        Assert.Equal(StoreCommandOutcome.Stored, await _store.StoreCommandAsync(original, FirstSeen, CancellationToken.None));
        var before = await SnapshotOriginalStateAsync(original.CommandId);
        await ExecuteAsync("CREATE TRIGGER fail_payload_conflict_insert AFTER INSERT ON command_payload_conflicts BEGIN SELECT RAISE(ABORT,'injected event insert failure'); END;");
        var erp = new RecordingErpClient();
        var intake = new CommandIntakeService(erp, _store);
        var leaseCommand = JsonSerializer.SerializeToElement(conflict, JsonOptions);

        var error = await Assert.ThrowsAsync<SqliteException>(() => intake.IntakeAsync(
            Guid.NewGuid(), leaseCommand, [conflict.CommandType], 4096, FirstSeen.AddHours(1), CancellationToken.None));

        Assert.Contains("injected event insert failure", error.Message, StringComparison.Ordinal);
        Assert.Equal(0, erp.AcknowledgeCalls);
        Assert.Equal(0L, await ScalarAsync("SELECT COUNT(*) FROM command_payload_conflicts"));
        Assert.Equal(before, await SnapshotOriginalStateAsync(original.CommandId));
    }

    [Fact]
    public async Task Ordinary_cleanup_deletes_command_history_but_preserves_conflict_evidence()
    {
        var original = MakeCommand();
        var conflict = MakeCommand(original.CommandId, "{\"amount\":11}");
        var acknowledgedAt = new DateTimeOffset(2026, 9, 20, 8, 0, 0, TimeSpan.Zero);
        Assert.Equal(StoreCommandOutcome.Stored, await _store.StoreCommandAsync(original, FirstSeen, CancellationToken.None));
        await CompleteAndAcknowledgeAsync(original, acknowledgedAt);
        Assert.Equal(StoreCommandOutcome.PayloadConflict, await _store.StoreCommandAsync(conflict, FirstSeen.AddHours(1), CancellationToken.None));
        var before = Assert.Single(await _store.GetCommandPayloadConflictEventsAsync(10, CancellationToken.None));

        await _store.CleanupAsync(acknowledgedAt.AddSeconds(1), DateTimeOffset.UtcNow, CancellationToken.None);

        Assert.False(await CommandExistsAsync(original.CommandId));
        Assert.Equal((0L, 0L, 0L), await OriginalStateCountsAsync(original.CommandId));
        Assert.Equal(before, Assert.Single(await _store.GetCommandPayloadConflictEventsAsync(10, CancellationToken.None)));
    }

    [Fact]
    public async Task Invalid_large_declared_hashes_are_stored_only_as_fixed_fingerprints_without_payload()
    {
        const string payloadMarker = "payload-secret-marker";
        const string userMarker = "requested-user-marker";
        var originalHash = "original-attacker-text-" + new string('o', 16_384) + "-secret-marker";
        var incomingHash = "incoming-attacker-text-" + new string('i', 32_768) + "-secret-marker";
        var original = MakeCommand(payloadJson: $"{{\"pii\":\"{payloadMarker}\"}}", payloadHash: originalHash, requestedBy: new RequestedBy(userMarker, userMarker));
        var conflict = MakeCommand(original.CommandId, $"{{\"pii\":\"{payloadMarker}-incoming\"}}", payloadHash: incomingHash, requestedBy: new RequestedBy(userMarker + "-incoming", userMarker + "-incoming"));
        Assert.Equal(StoreCommandOutcome.Stored, await _store.StoreCommandAsync(original, FirstSeen, CancellationToken.None));

        Assert.Equal(StoreCommandOutcome.PayloadConflict, await _store.StoreCommandAsync(conflict, FirstSeen.AddHours(1), CancellationToken.None));

        Assert.Equal(originalHash, await StringAsync("SELECT payload_hash FROM commands_inbox WHERE command_id=$id", original.CommandId));
        var recorded = Assert.Single(await _store.GetCommandPayloadConflictEventsAsync(10, CancellationToken.None));
        Assert.Equal(Fingerprint(originalHash), recorded.OriginalDeclaredHashFingerprint);
        Assert.Equal(Fingerprint(incomingHash), recorded.IncomingDeclaredHashFingerprint);
        Assert.Equal(64, recorded.OriginalDeclaredHashFingerprint.Length);
        Assert.Equal(64, recorded.IncomingDeclaredHashFingerprint.Length);
        Assert.Equal(
        [
            "event_id", "command_id", "event_code", "severity", "original_declared_hash_fingerprint",
            "incoming_declared_hash_fingerprint", "original_command_status", "first_seen_at_utc", "last_seen_at_utc", "occurrence_count"
        ], await ColumnNamesAsync("command_payload_conflicts"));
        var eventText = await EventTextAsync();
        Assert.DoesNotContain(originalHash, eventText, StringComparison.Ordinal);
        Assert.DoesNotContain(incomingHash, eventText, StringComparison.Ordinal);
        Assert.DoesNotContain(payloadMarker, eventText, StringComparison.Ordinal);
        Assert.DoesNotContain(userMarker, eventText, StringComparison.Ordinal);
    }

    private async Task PrepareOriginalStatusAsync(CommandEnvelope command, string status)
    {
        if (status == "queued") return;
        var owner = "payload-conflict-owner-" + Guid.NewGuid().ToString("N");
        Assert.NotNull(await _store.TryAcquireCommandExecutionClaimAsync(command.CommandId, owner, FirstSeen.AddMinutes(1), DateTimeOffset.MinValue, CancellationToken.None));
        var attempt = await _store.ClaimPostAttemptAsync(command.CommandId, owner, CancellationToken.None);
        Assert.NotNull(attempt);
        if (status == "executing") return;
        Assert.True(await _store.CompleteLocallyAsync(command.CommandId, owner, CommandStatus.SucceededLocal, "{\"status\":\"succeeded\"}", "ref", "42", attempt.AttemptId, CancellationToken.None));
        Assert.Equal("result_pending", await StringAsync("SELECT status FROM commands_inbox WHERE command_id=$id", command.CommandId));
        if (status == "completed") Assert.True(await _store.AcknowledgeResultAsync(command.CommandId, FirstSeen.AddHours(2), CancellationToken.None));
    }

    private async Task CompleteAndAcknowledgeAsync(CommandEnvelope command, DateTimeOffset acknowledgedAt)
    {
        var owner = "cleanup-owner-" + Guid.NewGuid().ToString("N");
        Assert.NotNull(await _store.TryAcquireCommandExecutionClaimAsync(command.CommandId, owner, FirstSeen.AddMinutes(1), DateTimeOffset.MinValue, CancellationToken.None));
        var attempt = await _store.ClaimPostAttemptAsync(command.CommandId, owner, CancellationToken.None);
        Assert.NotNull(attempt);
        Assert.True(await _store.CompleteLocallyAsync(command.CommandId, owner, CommandStatus.SucceededLocal, "{\"status\":\"succeeded\"}", "ref", "42", attempt.AttemptId, CancellationToken.None));
        Assert.True(await _store.AcknowledgeResultAsync(command.CommandId, acknowledgedAt, CancellationToken.None));
    }

    private async Task<string> SnapshotOriginalStateAsync(Guid commandId)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT * FROM commands_inbox WHERE command_id=$id;
            SELECT * FROM results_outbox WHERE command_id=$id ORDER BY result_id;
            SELECT * FROM command_attempts WHERE command_id=$id ORDER BY attempt_no;
            """;
        command.Parameters.AddWithValue("$id", commandId.ToString("D"));
        var rows = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(CancellationToken.None);
        do
        {
            while (await reader.ReadAsync(CancellationToken.None))
            {
                var fields = new string[reader.FieldCount];
                for (var index = 0; index < reader.FieldCount; index++)
                {
                    fields[index] = $"{reader.GetName(index)}={(reader.IsDBNull(index) ? "<null>" : Convert.ToString(reader.GetValue(index), CultureInfo.InvariantCulture))}";
                }
                rows.Add(string.Join("\u001F", fields));
            }
        } while (await reader.NextResultAsync(CancellationToken.None));
        return string.Join("\u001E", rows);
    }

    private async Task<bool> HasConflictTableAsync() =>
        await ScalarAsync("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='command_payload_conflicts';") == 1;

    private async Task<long> ScalarAsync(string sql)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        return Convert.ToInt64(await command.ExecuteScalarAsync(CancellationToken.None), CultureInfo.InvariantCulture);
    }

    private async Task<string?> StringAsync(string sql, Guid commandId)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.Parameters.AddWithValue("$id", commandId.ToString("D"));
        return await command.ExecuteScalarAsync(CancellationToken.None) as string;
    }

    private async Task ExecuteAsync(string sql)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync(CancellationToken.None);
    }

    private async Task<bool> CommandExistsAsync(Guid commandId) =>
        await ScalarWithIdAsync("SELECT COUNT(*) FROM commands_inbox WHERE command_id=$id", commandId) != 0;

    private async Task<(long Commands, long Outbox, long Attempts)> OriginalStateCountsAsync(Guid commandId) =>
        (
            await ScalarWithIdAsync("SELECT COUNT(*) FROM commands_inbox WHERE command_id=$id", commandId),
            await ScalarWithIdAsync("SELECT COUNT(*) FROM results_outbox WHERE command_id=$id", commandId),
            await ScalarWithIdAsync("SELECT COUNT(*) FROM command_attempts WHERE command_id=$id", commandId)
        );

    private async Task<long> ScalarWithIdAsync(string sql, Guid commandId)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.Parameters.AddWithValue("$id", commandId.ToString("D"));
        return Convert.ToInt64(await command.ExecuteScalarAsync(CancellationToken.None), CultureInfo.InvariantCulture);
    }

    private async Task<IReadOnlyList<string>> ColumnNamesAsync(string table)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = $"PRAGMA table_info({table});";
        var names = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(CancellationToken.None);
        while (await reader.ReadAsync(CancellationToken.None)) names.Add(reader.GetString(1));
        return names;
    }

    private async Task<string> EventTextAsync()
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT event_id,command_id,event_code,severity,original_declared_hash_fingerprint,incoming_declared_hash_fingerprint,original_command_status,first_seen_at_utc,last_seen_at_utc,occurrence_count FROM command_payload_conflicts;";
        await using var reader = await command.ExecuteReaderAsync(CancellationToken.None);
        Assert.True(await reader.ReadAsync(CancellationToken.None));
        var fields = new string[reader.FieldCount];
        for (var index = 0; index < reader.FieldCount; index++) fields[index] = reader.IsDBNull(index) ? string.Empty : Convert.ToString(reader.GetValue(index), CultureInfo.InvariantCulture) ?? string.Empty;
        return string.Join("\u001F", fields);
    }

    private static string Fingerprint(string declaredHash) => Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(declaredHash)));

    private static CommandEnvelope MakeCommand(
        Guid? commandId = null,
        string payloadJson = "{\"amount\":10}",
        string? payloadHash = null,
        RequestedBy? requestedBy = null)
    {
        using var document = JsonDocument.Parse(payloadJson);
        var payload = document.RootElement.Clone();
        return new(
            commandId ?? Guid.NewGuid(), "create_customer_order", 1, 100, "order:conflict", null, DateTimeOffset.UtcNow,
            null, DateTimeOffset.UtcNow.AddHours(1), requestedBy, payloadHash ?? PayloadHasher.Compute(payload), payload);
    }

    private sealed class RecordingErpClient : IErpClient
    {
        public int AcknowledgeCalls { get; private set; }

        public Task AcknowledgeReceivedAsync(Guid commandId, CommandReceivedRequest request, CancellationToken cancellationToken)
        {
            AcknowledgeCalls++;
            return Task.CompletedTask;
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
