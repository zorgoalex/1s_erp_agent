using System.IO.Compression;
using System.Text.Json;
using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Application.Commands;
using ErpOnecAgent.Domain.Commands;
using ErpOnecAgent.Domain.Common;
using ErpOnecAgent.Domain.Etl;
using ErpOnecAgent.Infrastructure.Persistence.Sqlite;
using ErpOnecAgent.Infrastructure.Spool;
using Microsoft.Data.Sqlite;
using Xunit;

namespace ErpOnecAgent.IntegrationTests;

public sealed class SqliteStoreTests : IAsyncLifetime
{
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
    public async Task Duplicate_command_is_idempotent_and_changed_payload_conflicts()
    {
        var first = MakeCommand(Guid.NewGuid(), "{\"amount\":10}");
        var duplicate = first;
        var changed = MakeCommand(first.CommandId, "{\"amount\":11}");
        Assert.Equal(StoreCommandOutcome.Stored, await _store.StoreCommandAsync(first, DateTimeOffset.UtcNow, CancellationToken.None));
        Assert.Equal(StoreCommandOutcome.Duplicate, await _store.StoreCommandAsync(duplicate, DateTimeOffset.UtcNow, CancellationToken.None));
        Assert.Equal(StoreCommandOutcome.PayloadConflict, await _store.StoreCommandAsync(changed, DateTimeOffset.UtcNow, CancellationToken.None));
    }

    [Fact]
    public async Task Restart_recovers_executing_command_as_unknown_result()
    {
        var command = MakeCommand(Guid.NewGuid(), "{\"order\":42}");
        await _store.StoreCommandAsync(command, DateTimeOffset.UtcNow, CancellationToken.None);
        await _store.MarkExecutingAsync(command.CommandId, CancellationToken.None);
        await _store.RecoverAsync(CancellationToken.None);
        var ready = await _store.GetReadyCommandsAsync(10, DateTimeOffset.UtcNow.AddSeconds(5), CancellationToken.None);
        Assert.Single(ready); Assert.Equal(CommandStatus.UnknownResult, ready[0].Status);
    }

    [Fact]
    public async Task Result_outbox_survives_and_is_acknowledged_transactionally()
    {
        var command = MakeCommand(Guid.NewGuid(), "{}");
        await _store.StoreCommandAsync(command, DateTimeOffset.UtcNow, CancellationToken.None);
        const string owner = "store-owner";
        Assert.NotNull(await _store.TryAcquireCommandExecutionClaimAsync(command.CommandId, owner, DateTimeOffset.UtcNow, DateTimeOffset.MinValue, CancellationToken.None));
        var attempt = await _store.ClaimPostAttemptAsync(command.CommandId, owner, CancellationToken.None);
        Assert.NotNull(attempt);
        Assert.True(await _store.CompleteLocallyAsync(command.CommandId, owner, CommandStatus.SucceededLocal, "{\"status\":\"succeeded\"}", "ref", "1", attempt.AttemptId, CancellationToken.None));
        Assert.Single(await _store.GetPendingResultsAsync(10, DateTimeOffset.UtcNow, CancellationToken.None));
        await _store.AcknowledgeResultAsync(command.CommandId, DateTimeOffset.UtcNow, CancellationToken.None);
        Assert.Empty(await _store.GetPendingResultsAsync(10, DateTimeOffset.UtcNow, CancellationToken.None));
    }

    [Fact]
    public async Task Terminal_completion_active_row_writes_one_result_outbox_pair_atomically()
    {
        var command = MakeCommand(Guid.NewGuid(), "{}");
        await _store.StoreCommandAsync(command, DateTimeOffset.UtcNow, CancellationToken.None);
        var attempt = await _store.ClaimPostAttemptAsync(command.CommandId, CancellationToken.None);
        var resultJson = "{\"status\":\"succeeded\",\"marker\":\"original\"}";

        Assert.True(await _store.CompleteLocallyAsync(command.CommandId, CommandStatus.SucceededLocal, resultJson, "ref-1", "42", attempt!.AttemptId, CancellationToken.None));

        var state = await CompletionSnapshotAsync(command.CommandId);
        var pending = Assert.Single(await _store.GetPendingResultsAsync(10, DateTimeOffset.UtcNow, CancellationToken.None));
        Assert.Equal("result_pending", state.Status);
        Assert.Equal("succeeded_local", state.ResultStatus);
        Assert.Equal(resultJson, state.ResultJson);
        Assert.Equal(resultJson, pending.PayloadJson);
        Assert.Equal("ref-1", state.ExternalRef);
        Assert.Equal("42", state.ExternalNumber);
        Assert.Equal(PayloadHasher.ComputeBytes(System.Text.Encoding.UTF8.GetBytes(resultJson)), state.OutboxPayloadHash);
        Assert.Equal("pending", state.OutboxStatus);
        var audit = Assert.Single(state.Attempts);
        Assert.Equal(attempt.AttemptId.ToString("D"), audit.AttemptId);
        Assert.Equal("succeeded_local", audit.Outcome);
        Assert.NotNull(audit.FinishedAtUtc);
    }

    [Fact]
    public async Task Terminal_completion_late_different_outcome_is_noop()
    {
        var command = MakeCommand(Guid.NewGuid(), "{}");
        await _store.StoreCommandAsync(command, DateTimeOffset.UtcNow, CancellationToken.None);
        var historical = await _store.ClaimPostAttemptAsync(command.CommandId, CancellationToken.None);
        await _store.RecoverAsync(CancellationToken.None);
        var current = await _store.ClaimLookupAttemptAsync(command.CommandId, "STATUS_PENDING", "pending", DateTimeOffset.UtcNow.AddMinutes(1), CancellationToken.None);
        Assert.True(await _store.CompleteLocallyAsync(command.CommandId, CommandStatus.SucceededLocal, "{\"status\":\"succeeded\",\"marker\":\"first\"}", "first-ref", "first-number", current!.AttemptId, CancellationToken.None));
        var before = await CompletionSnapshotAsync(command.CommandId);

        Assert.False(await _store.CompleteLocallyAsync(command.CommandId, CommandStatus.DeadLetter, "{\"status\":\"dead_letter\",\"marker\":\"late\"}", "late-ref", "late-number", historical!.AttemptId, CancellationToken.None));

        var after = await CompletionSnapshotAsync(command.CommandId);
        AssertCompletionUnchanged(before, after);
        Assert.Equal("succeeded_local", after.ResultStatus);
        Assert.Equal("{\"status\":\"succeeded\",\"marker\":\"first\"}", after.ResultJson);
        Assert.Equal(1, after.Attempts.Count(a => a.FinishedAtUtc is null));
    }

    [Fact]
    public async Task Terminal_completion_duplicate_same_outcome_is_noop()
    {
        var command = MakeCommand(Guid.NewGuid(), "{}");
        await _store.StoreCommandAsync(command, DateTimeOffset.UtcNow, CancellationToken.None);
        var attempt = await _store.ClaimPostAttemptAsync(command.CommandId, CancellationToken.None);
        const string resultJson = "{\"status\":\"succeeded\",\"marker\":\"same\"}";
        Assert.True(await _store.CompleteLocallyAsync(command.CommandId, CommandStatus.SucceededLocal, resultJson, "same-ref", "same-number", attempt!.AttemptId, CancellationToken.None));
        var before = await CompletionSnapshotAsync(command.CommandId);

        Assert.False(await _store.CompleteLocallyAsync(command.CommandId, CommandStatus.SucceededLocal, resultJson, "same-ref", "same-number", attempt.AttemptId, CancellationToken.None));

        var after = await CompletionSnapshotAsync(command.CommandId);
        AssertCompletionUnchanged(before, after);
        Assert.Equal("succeeded_local", after.ResultStatus);
        Assert.Equal(resultJson, after.ResultJson);
        Assert.Equal(PayloadHasher.ComputeBytes(System.Text.Encoding.UTF8.GetBytes(resultJson)), after.OutboxPayloadHash);
    }

    [Fact]
    public async Task Terminal_completion_after_acknowledgement_preserves_result_outbox_and_audit()
    {
        var command = MakeCommand(Guid.NewGuid(), "{}");
        await _store.StoreCommandAsync(command, DateTimeOffset.UtcNow, CancellationToken.None);
        var historical = await _store.ClaimPostAttemptAsync(command.CommandId, CancellationToken.None);
        await _store.RecoverAsync(CancellationToken.None);
        var current = await _store.ClaimLookupAttemptAsync(command.CommandId, "STATUS_PENDING", "pending", DateTimeOffset.UtcNow.AddMinutes(1), CancellationToken.None);
        Assert.True(await _store.CompleteLocallyAsync(command.CommandId, CommandStatus.SucceededLocal, "{\"status\":\"succeeded\",\"marker\":\"acked\"}", "acked-ref", "acked-number", current!.AttemptId, CancellationToken.None));
        var acknowledgedAt = new DateTimeOffset(2026, 9, 24, 12, 0, 0, TimeSpan.Zero);
        await _store.AcknowledgeResultAsync(command.CommandId, acknowledgedAt, CancellationToken.None);
        var before = await CompletionSnapshotAsync(command.CommandId);

        Assert.False(await _store.CompleteLocallyAsync(command.CommandId, CommandStatus.Cancelled, "{\"status\":\"cancelled\",\"marker\":\"late\"}", "late-ref", "late-number", historical!.AttemptId, CancellationToken.None));

        var after = await CompletionSnapshotAsync(command.CommandId);
        AssertCompletionUnchanged(before, after);
        Assert.Equal("completed", after.Status);
        Assert.Equal("acknowledged", after.OutboxStatus);
        Assert.Equal(acknowledgedAt.ToString("O"), after.OutboxAcknowledgedAtUtc);
        Assert.Equal(acknowledgedAt.ToString("O"), after.OutboxSentAtUtc);
        Assert.Equal(acknowledgedAt.ToString("O"), after.ErpAcknowledgedAtUtc);
        Assert.Equal("{\"status\":\"succeeded\",\"marker\":\"acked\"}", after.ResultJson);
        Assert.Equal(1, after.Attempts.Count(a => a.FinishedAtUtc is null));
    }

    [Fact]
    public async Task Terminal_completion_nonexistent_id_is_noop_without_rows()
    {
        var commandId = Guid.NewGuid();
        var attemptId = Guid.NewGuid();

        Assert.False(await _store.CompleteLocallyAsync(commandId, CommandStatus.SucceededLocal, "{\"status\":\"succeeded\"}", "ref", "1", attemptId, CancellationToken.None));

        var counts = await CompletionCountsAsync();
        Assert.Equal((0L, 0L, 0L), counts);
    }

    [Fact]
    public async Task Spool_writes_atomic_gzip_ndjson_and_run_becomes_completable_after_ack()
    {
        var spool = new FileSpoolStore(Path.Combine(_database.Root, "spool"));
        var entity = new EtlEntityDefinition("clients", "Catalog_Clients", "Ref_Key", "UpdatedAt", "DeletionMark", ["Ref_Key", "UpdatedAt", "DeletionMark"], "incremental", 500, 10);
        using var document = JsonDocument.Parse("{\"Ref_Key\":\"A1\",\"UpdatedAt\":\"2026-08-29T10:00:00Z\",\"DeletionMark\":false}");
        var runId = Guid.NewGuid(); await _store.CreateEtlRunAsync(new(runId, "incremental", ["clients"], EtlRunStatus.Running), CancellationToken.None);
        var cursor = new EtlCursor(DateTimeOffset.Parse("2026-08-29T10:00:00Z", System.Globalization.CultureInfo.InvariantCulture), "A1");
        var batch = await spool.WriteBatchAsync(runId, entity, [document.RootElement.Clone()], null, cursor, CancellationToken.None);
        Assert.True(File.Exists(batch.FilePath)); Assert.False(File.Exists(batch.FilePath + ".tmp"));
        await using (var raw = await spool.OpenReadAsync(batch, CancellationToken.None))
        await using (var gzip = new GZipStream(raw, CompressionMode.Decompress))
        using (var reader = new StreamReader(gzip)) Assert.Contains("\"sourceId\":\"A1\"", await reader.ReadToEndAsync());
        await _store.RegisterBatchAsync(batch, CancellationToken.None); await _store.MarkEtlRunExtractedAsync(runId, CancellationToken.None);
        Assert.Empty(await _store.GetRunsReadyToCompleteAsync(CancellationToken.None));
        var claimed = Assert.Single(await _store.GetPendingBatchesAsync(1, DateTimeOffset.UtcNow, CancellationToken.None));
        Assert.Equal(batch.BatchId, claimed.BatchId);
        await _store.AcknowledgeBatchAsync(batch.BatchId, DateTimeOffset.UtcNow, CancellationToken.None);
        var completion = Assert.Single(await _store.GetRunsReadyToCompleteAsync(CancellationToken.None));
        Assert.Equal(cursor, completion.Watermarks["clients"]);
        // Retention releases acknowledged batches only after the parent run has succeeded.
        Assert.Empty(await _store.GetAcknowledgedBatchesAsync(DateTimeOffset.UtcNow.AddSeconds(1), CancellationToken.None));
        foreach (var watermark in completion.Watermarks) await _store.CommitWatermarkAsync(watermark.Key, watermark.Value, completion.RunId, CancellationToken.None);
        await _store.CompleteEtlRunAsync(completion.RunId, EtlRunStatus.Succeeded, null, CancellationToken.None);
        Assert.Single(await _store.GetAcknowledgedBatchesAsync(DateTimeOffset.UtcNow.AddSeconds(1), CancellationToken.None));
        await spool.DeleteAcknowledgedAsync(batch, CancellationToken.None); await _store.MarkBatchDeletedAsync(batch.BatchId, CancellationToken.None);
        Assert.False(File.Exists(batch.FilePath));
    }

    [Fact]
    public async Task Remote_configuration_snapshot_is_atomic_and_immutable_by_version()
    {
        const string json = "{\"mode\":\"Normal\"}";
        using var document = JsonDocument.Parse(json);
        var hash = PayloadHasher.Compute(document.RootElement);
        await _store.SaveConfigSnapshotAsync(1, json, hash, "validated", CancellationToken.None);
        await _store.ActivateConfigSnapshotAsync(1, CancellationToken.None);
        var active = await _store.GetActiveConfigSnapshotAsync(CancellationToken.None);
        Assert.NotNull(active); Assert.Equal(1, active.Version); Assert.Equal(hash, active.Hash);
        await Assert.ThrowsAsync<InvalidDataException>(() => _store.SaveConfigSnapshotAsync(1, "{}", "changed", "validated", CancellationToken.None));
    }

    [Fact]
    public async Task Admit_stores_valid_command_as_executable_queued()
    {
        var command = MakeCommand(Guid.NewGuid(), "{\"amount\":10}");
        var validation = CommandValidator.Validate(command, ["create_customer_order"], 1024);
        Assert.True(validation.IsValid);
        Assert.Equal(StoreCommandOutcome.Stored, await _store.AdmitCommandAsync(command, DateTimeOffset.UtcNow, validation, CancellationToken.None));
        var ready = await _store.GetReadyCommandsAsync(10, DateTimeOffset.UtcNow.AddMinutes(1), CancellationToken.None);
        Assert.Single(ready);
        Assert.Equal(CommandStatus.Queued, ready[0].Status);
        Assert.Empty(await _store.GetPendingResultsAsync(10, DateTimeOffset.UtcNow, CancellationToken.None));
    }

    [Fact]
    public async Task Admit_rejects_invalid_hash_atomically_with_terminal_outbox_and_no_executable_row()
    {
        var command = MakeCommand(Guid.NewGuid(), "{\"amount\":10}", payloadHashOverride: "wrong-hash");
        var validation = CommandValidator.Validate(command, ["create_customer_order"], 1024);
        Assert.False(validation.IsValid);
        Assert.Equal("PAYLOAD_HASH_MISMATCH", validation.ErrorCode);
        Assert.Equal(StoreCommandOutcome.Rejected, await _store.AdmitCommandAsync(command, DateTimeOffset.UtcNow, validation, CancellationToken.None));
        Assert.Empty(await _store.GetReadyCommandsAsync(10, DateTimeOffset.UtcNow.AddMinutes(1), CancellationToken.None));
        var pending = Assert.Single(await _store.GetPendingResultsAsync(10, DateTimeOffset.UtcNow, CancellationToken.None));
        Assert.Contains("dead_letter", pending.PayloadJson, StringComparison.Ordinal);
        Assert.Contains("PAYLOAD_HASH_MISMATCH", pending.PayloadJson, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Admit_rejects_unknown_payload_version_without_executable_row()
    {
        var command = MakeCommand(Guid.NewGuid(), "{\"amount\":10}", payloadVersion: 2);
        var validation = CommandValidator.Validate(command, ["create_customer_order"], 1024);
        Assert.False(validation.IsValid);
        Assert.Equal("UNSUPPORTED_PAYLOAD_VERSION", validation.ErrorCode);
        Assert.Equal(StoreCommandOutcome.Rejected, await _store.AdmitCommandAsync(command, DateTimeOffset.UtcNow, validation, CancellationToken.None));
        Assert.Empty(await _store.GetReadyCommandsAsync(10, DateTimeOffset.UtcNow.AddMinutes(1), CancellationToken.None));
        var pending = Assert.Single(await _store.GetPendingResultsAsync(10, DateTimeOffset.UtcNow, CancellationToken.None));
        Assert.Contains("UNSUPPORTED_PAYLOAD_VERSION", pending.PayloadJson, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Admit_rejects_null_payload_without_executable_row()
    {
        using var document = JsonDocument.Parse("null");
        var payload = document.RootElement.Clone();
        var command = new CommandEnvelope(Guid.NewGuid(), "create_customer_order", 1, 100, null, null, DateTimeOffset.UtcNow, null, null, null, PayloadHasher.Compute(payload), payload);
        var validation = CommandValidator.Validate(command, ["create_customer_order"], 1024);
        Assert.False(validation.IsValid);
        Assert.Equal("INVALID_PAYLOAD", validation.ErrorCode);
        Assert.Equal(StoreCommandOutcome.Rejected, await _store.AdmitCommandAsync(command, DateTimeOffset.UtcNow, validation, CancellationToken.None));
        Assert.Empty(await _store.GetReadyCommandsAsync(10, DateTimeOffset.UtcNow.AddMinutes(1), CancellationToken.None));
        Assert.Single(await _store.GetPendingResultsAsync(10, DateTimeOffset.UtcNow, CancellationToken.None));
    }

    [Fact]
    public async Task Admit_of_invalid_command_is_idempotent_on_redelivery()
    {
        var command = MakeCommand(Guid.NewGuid(), "{\"amount\":10}", payloadHashOverride: "wrong-hash");
        var validation = CommandValidator.Validate(command, ["create_customer_order"], 1024);
        Assert.Equal(StoreCommandOutcome.Rejected, await _store.AdmitCommandAsync(command, DateTimeOffset.UtcNow, validation, CancellationToken.None));
        Assert.Equal(StoreCommandOutcome.Duplicate, await _store.AdmitCommandAsync(command, DateTimeOffset.UtcNow, validation, CancellationToken.None));
        Assert.Empty(await _store.GetReadyCommandsAsync(10, DateTimeOffset.UtcNow.AddMinutes(1), CancellationToken.None));
        Assert.Single(await _store.GetPendingResultsAsync(10, DateTimeOffset.UtcNow, CancellationToken.None));
    }

    private static void AssertCompletionUnchanged(CompletionSnapshot expected, CompletionSnapshot actual)
    {
        Assert.Equal(expected with { Attempts = [] }, actual with { Attempts = [] });
        Assert.Equal(expected.Attempts, actual.Attempts);
    }

    private async Task<CompletionSnapshot> CompletionSnapshotAsync(Guid commandId)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        CompletionSnapshot state;
        await using (var command = connection.CreateCommand())
        {
            command.CommandText = """
                SELECT c.status,c.result_status,c.result_json,c.external_ref,c.external_number,c.finished_at_utc,c.next_attempt_at_utc,c.last_error_code,c.last_error_message,c.erp_acknowledged_at_utc,c.row_version,c.exec_claim_owner_id,c.exec_claim_acquired_at_utc,
                       o.result_id,o.payload_json,o.payload_hash,o.status,o.attempt_count,o.next_attempt_at_utc,o.created_at_utc,o.sent_at_utc,o.acknowledged_at_utc,o.last_error
                FROM commands_inbox c
                LEFT JOIN results_outbox o ON o.command_id=c.command_id
                WHERE c.command_id=$id;
                """;
            command.Parameters.AddWithValue("$id", commandId.ToString("D"));
            await using var reader = await command.ExecuteReaderAsync(CancellationToken.None);
            Assert.True(await reader.ReadAsync(CancellationToken.None));
            state = new(
                reader.GetString(0), NullableString(reader, 1), NullableString(reader, 2), NullableString(reader, 3), NullableString(reader, 4),
                NullableString(reader, 5), NullableString(reader, 6), NullableString(reader, 7), NullableString(reader, 8), NullableString(reader, 9),
                reader.GetInt64(10), NullableString(reader, 11), NullableString(reader, 12), NullableString(reader, 13), NullableString(reader, 14),
                NullableString(reader, 15), NullableString(reader, 16), reader.GetInt64(17), NullableString(reader, 18), NullableString(reader, 19),
                NullableString(reader, 20), NullableString(reader, 21), NullableString(reader, 22), []);
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
        return state with { Attempts = attempts };
    }

    private async Task<(long Commands, long Outbox, long Attempts)> CompletionCountsAsync()
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT (SELECT COUNT(*) FROM commands_inbox),(SELECT COUNT(*) FROM results_outbox),(SELECT COUNT(*) FROM command_attempts);";
        await using var reader = await command.ExecuteReaderAsync(CancellationToken.None);
        Assert.True(await reader.ReadAsync(CancellationToken.None));
        return (reader.GetInt64(0), reader.GetInt64(1), reader.GetInt64(2));
    }

    private static string? NullableString(SqliteDataReader reader, int ordinal) => reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);
    private static int? NullableInt(SqliteDataReader reader, int ordinal) => reader.IsDBNull(ordinal) ? null : reader.GetInt32(ordinal);

    private sealed record CompletionSnapshot(
        string Status,
        string? ResultStatus,
        string? ResultJson,
        string? ExternalRef,
        string? ExternalNumber,
        string? FinishedAtUtc,
        string? NextAttemptAtUtc,
        string? LastErrorCode,
        string? LastErrorMessage,
        string? ErpAcknowledgedAtUtc,
        long RowVersion,
        string? ClaimOwner,
        string? ClaimAcquiredAtUtc,
        string? OutboxResultId,
        string? OutboxPayloadJson,
        string? OutboxPayloadHash,
        string? OutboxStatus,
        long OutboxAttemptCount,
        string? OutboxNextAttemptAtUtc,
        string? OutboxCreatedAtUtc,
        string? OutboxSentAtUtc,
        string? OutboxAcknowledgedAtUtc,
        string? OutboxLastError,
        IReadOnlyList<AttemptSnapshot> Attempts);

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

    private static CommandEnvelope MakeCommand(Guid id, string payloadJson, int payloadVersion = 1, string? payloadHashOverride = null)
    {
        using var document = JsonDocument.Parse(payloadJson); var payload = document.RootElement.Clone();
        return new(id, "create_customer_order", payloadVersion, 100, "order:42", null, DateTimeOffset.UtcNow, null, DateTimeOffset.UtcNow.AddHours(1), null, payloadHashOverride ?? PayloadHasher.Compute(payload), payload);
    }
}
