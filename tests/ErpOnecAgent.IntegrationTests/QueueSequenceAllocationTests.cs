using System.Text.Json;
using ErpOnecAgent.Application.Commands;
using ErpOnecAgent.Domain.Commands;
using ErpOnecAgent.Domain.Common;
using ErpOnecAgent.Infrastructure.Persistence.Sqlite;
using Microsoft.Data.Sqlite;
using Xunit;

namespace ErpOnecAgent.IntegrationTests;

public sealed class QueueSequenceAllocationTests : IAsyncLifetime
{
    private readonly SqliteTestDatabase _database = new();
    private SqliteConnectionFactory _factory = null!;
    private SqliteAgentStore _store = null!;

    public async Task InitializeAsync()
    {
        _factory = _database.CreateFactory(Path.Combine("data", "agent.db"));
        _store = CreateStore();
        await _store.InitializeAsync(CancellationToken.None);
    }

    public async Task DisposeAsync()
    {
        await _database.DisposeAsync();
    }

    [Fact]
    public async Task Interleaved_store_instances_preserve_one_successor_and_one_claim()
    {
        var receivedAt = DateTimeOffset.UtcNow;
        var storeA = CreateStore();
        var storeB = CreateStore();
        var first = MakeCommand("allocation:interleaved", receivedAt);
        var second = MakeCommand("allocation:interleaved", receivedAt);
        var third = MakeCommand("allocation:interleaved", receivedAt);

        await storeA.StoreCommandAsync(first, receivedAt, CancellationToken.None);
        await storeB.StoreCommandAsync(second, receivedAt, CancellationToken.None);
        await storeA.StoreCommandAsync(third, receivedAt, CancellationToken.None);
        await _store.CompleteLocallyAsync(first.CommandId, CommandStatus.SucceededLocal, "{\"status\":\"succeeded\"}", "ref-1", "1", null, CancellationToken.None);
        await _store.AcknowledgeResultAsync(first.CommandId, receivedAt.AddMinutes(1), CancellationToken.None);

        var successors = await _store.GetReadyCommandsAsync(10, receivedAt.AddMinutes(1), CancellationToken.None);
        var claims = await Task.WhenAll(successors.Select((command, index) => _store.TryAcquireCommandExecutionClaimAsync(
            command.Envelope.CommandId, $"owner-{index}", receivedAt.AddMinutes(1), DateTimeOffset.MinValue, CancellationToken.None)));
        var readyIds = successors.Select(static command => command.Envelope.CommandId).ToArray();
        var winningIds = successors.Where((_, index) => claims[index] is not null).Select(static command => command.Envelope.CommandId).ToArray();
        var actualRows = await SequenceRowsAsync();
        var expectedRows = new[] { (first.CommandId, 1L), (second.CommandId, 2L), (third.CommandId, 3L) };
        var expectedReadyIds = new[] { second.CommandId };
        var expectedWinningIds = new[] { second.CommandId };
        var details = $"rows=[{string.Join(", ", actualRows.Select(static row => $"{row.CommandId:D}:{row.QueueSequence}"))}], ready=[{string.Join(", ", readyIds)}], winners=[{string.Join(", ", winningIds)}]";

        Assert.True(
            actualRows.SequenceEqual(expectedRows) && readyIds.SequenceEqual(expectedReadyIds) && winningIds.SequenceEqual(expectedWinningIds),
            details);
        Assert.Equal(expectedRows, actualRows);
        Assert.Equal(expectedReadyIds, readyIds);
        Assert.Equal(expectedWinningIds, winningIds);
    }

    [Fact]
    public async Task Store_and_admit_paths_allocate_including_rejected_admission()
    {
        var receivedAt = DateTimeOffset.UtcNow;
        var storeA = CreateStore();
        var storeB = CreateStore();
        var stored = MakeCommand("allocation:mixed", receivedAt);
        var rejected = MakeCommand("allocation:mixed", receivedAt);
        var admitted = MakeCommand("allocation:mixed", receivedAt);
        var final = MakeCommand("allocation:mixed", receivedAt);
        var rejection = new ValidationResult(false, "TEST_REJECTION", "Rejected by test.");

        Assert.Equal(StoreCommandOutcome.Stored, await storeA.StoreCommandAsync(stored, receivedAt, CancellationToken.None));
        Assert.Equal(StoreCommandOutcome.Rejected, await storeB.AdmitCommandAsync(rejected, receivedAt, rejection, CancellationToken.None));
        Assert.Equal(StoreCommandOutcome.Stored, await storeA.AdmitCommandAsync(admitted, receivedAt, ValidationResult.Success, CancellationToken.None));
        Assert.Equal(StoreCommandOutcome.Stored, await storeB.StoreCommandAsync(final, receivedAt, CancellationToken.None));

        Assert.Equal(
            new[] { (stored.CommandId, 1L), (rejected.CommandId, 2L), (admitted.CommandId, 3L), (final.CommandId, 4L) },
            await SequenceRowsAsync());
    }

    [Fact]
    public async Task Failed_insert_does_not_change_existing_order_or_persist_failed_row()
    {
        var receivedAt = DateTimeOffset.UtcNow;
        var existing = MakeCommand("allocation:rollback", receivedAt);
        var invalid = MakeCommand("allocation:rollback", receivedAt) with { CommandType = null! };
        var good = MakeCommand("allocation:rollback", receivedAt);

        Assert.Equal(StoreCommandOutcome.Stored, await _store.StoreCommandAsync(existing, receivedAt, CancellationToken.None));
        await Assert.ThrowsAsync<SqliteException>(() => _store.StoreCommandAsync(invalid, receivedAt, CancellationToken.None));
        Assert.Equal(StoreCommandOutcome.Stored, await _store.StoreCommandAsync(good, receivedAt, CancellationToken.None));

        var retained = await SequenceRowsAsync();
        Assert.Equal(new[] { existing.CommandId, good.CommandId }, retained.Select(static row => row.CommandId).ToArray());
        Assert.Equal(1L, retained[0].QueueSequence);
        Assert.True(retained[1].QueueSequence > retained[0].QueueSequence);
        Assert.Equal(new[] { existing.CommandId }, await ReadyIdsAsync(receivedAt.AddMinutes(1)));

        await _store.CompleteLocallyAsync(existing.CommandId, CommandStatus.SucceededLocal, "{\"status\":\"succeeded\"}", "ref-1", "1", null, CancellationToken.None);
        await _store.AcknowledgeResultAsync(existing.CommandId, receivedAt.AddMinutes(1), CancellationToken.None);
        Assert.Equal(new[] { good.CommandId }, await ReadyIdsAsync(receivedAt.AddMinutes(1)));
    }

    [Fact]
    public async Task New_store_instance_preserves_retained_order_without_renumbering()
    {
        var receivedAt = DateTimeOffset.UtcNow;
        var first = MakeCommand("allocation:restart", receivedAt);
        var second = MakeCommand("allocation:restart", receivedAt);
        var third = MakeCommand("allocation:restart", receivedAt);
        var fourth = MakeCommand("allocation:restart", receivedAt);

        await _store.StoreCommandAsync(first, receivedAt, CancellationToken.None);
        await _store.StoreCommandAsync(second, receivedAt, CancellationToken.None);
        await CreateStore().AdmitCommandAsync(third, receivedAt, ValidationResult.Success, CancellationToken.None);
        var beforeRestart = await SequenceRowsAsync();
        await CreateStore().StoreCommandAsync(fourth, receivedAt, CancellationToken.None);
        var afterRestart = await SequenceRowsAsync();

        Assert.Equal(new[] { (first.CommandId, 1L), (second.CommandId, 2L), (third.CommandId, 3L) }, beforeRestart);
        Assert.Equal(
            new[] { (first.CommandId, 1L), (second.CommandId, 2L), (third.CommandId, 3L), (fourth.CommandId, 4L) },
            afterRestart);
        Assert.Equal(new[] { first.CommandId }, await ReadyIdsAsync(receivedAt.AddMinutes(1)));
    }

    [Fact]
    public async Task Concurrent_independent_store_instances_allocate_unique_sequences()
    {
        const int count = 8;
        var receivedAt = DateTimeOffset.UtcNow;
        var stores = Enumerable.Range(0, count).Select(_ => CreateStore()).ToArray();
        var commands = Enumerable.Range(0, count).Select(index => MakeCommand($"allocation:concurrent-{index}", receivedAt)).ToArray();
        var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var admissions = stores.Select(async (store, index) =>
        {
            await start.Task.ConfigureAwait(false);
            return await store.StoreCommandAsync(commands[index], receivedAt, CancellationToken.None).ConfigureAwait(false);
        }).ToArray();

        start.SetResult();
        var outcomes = await Task.WhenAll(admissions);
        var rows = await SequenceRowsAsync();

        Assert.All(outcomes, outcome => Assert.Equal(StoreCommandOutcome.Stored, outcome));
        Assert.Equal(Enumerable.Range(1, count).Select(static sequence => (long)sequence).ToArray(), rows.Select(static row => row.QueueSequence).ToArray());
        Assert.Equal(commands.Select(static command => command.CommandId).Order().ToArray(), rows.Select(static row => row.CommandId).Order().ToArray());
    }

    private SqliteAgentStore CreateStore() => new(_factory, new SqliteMigrator(_factory));

    private async Task<(Guid CommandId, long QueueSequence)[]> SequenceRowsAsync()
    {
        var rows = new List<(Guid CommandId, long QueueSequence)>();
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT command_id,queue_sequence FROM commands_inbox ORDER BY rowid;";
        await using var reader = await command.ExecuteReaderAsync(CancellationToken.None);
        while (await reader.ReadAsync(CancellationToken.None))
        {
            rows.Add((Guid.Parse(reader.GetString(0)), reader.GetInt64(1)));
        }

        return rows.ToArray();
    }

    private async Task<Guid[]> ReadyIdsAsync(DateTimeOffset nowUtc) =>
        (await _store.GetReadyCommandsAsync(100, nowUtc, CancellationToken.None)).Select(static command => command.Envelope.CommandId).ToArray();

    private static CommandEnvelope MakeCommand(string orderingKey, DateTimeOffset createdAtUtc)
    {
        using var document = JsonDocument.Parse("{\"amount\":10}");
        var payload = document.RootElement.Clone();
        return new(Guid.NewGuid(), "create_customer_order", 1, 100, orderingKey, null, createdAtUtc, null, null, null, PayloadHasher.Compute(payload), payload);
    }
}
