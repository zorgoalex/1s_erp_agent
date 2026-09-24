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

/// <summary>
/// A09 ordering/metrics slice: deterministic per-ordering-key head-of-line plus atomic claim guard.
/// Real SQLite, real SqliteAgentStore, faked 1C client only. No external ERP/1C is contacted.
/// </summary>
public sealed partial class CommandOrderingClaimGuardTests : IAsyncLifetime
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

    // ---- A09 head-of-line: equal received_at_utc, deterministic total order ----

    [Fact]
    public async Task Equal_received_time_same_ordering_key_yields_exactly_one_ready_head()
    {
        var receivedAt = DateTimeOffset.UtcNow;
        var first = MakeCommand("order:eq", receivedAt);
        var second = MakeCommand("order:eq", receivedAt);
        await _store.StoreCommandAsync(first, receivedAt, CancellationToken.None);
        await _store.StoreCommandAsync(second, receivedAt, CancellationToken.None);

        var ready = await _store.GetReadyCommandsAsync(10, DateTimeOffset.UtcNow.AddMinutes(1), CancellationToken.None);

        // One head only: the second command of the same key is blocked by the first even though the
        // receive timestamps are identical (the durable queue_sequence tie-break resolves it).
        var head = Assert.Single(ready);
        Assert.Equal(first.CommandId, head.Envelope.CommandId);
    }

    [Fact]
    public async Task Equal_received_time_head_is_released_only_after_the_predecessor_is_acknowledged()
    {
        var receivedAt = DateTimeOffset.UtcNow;
        var first = MakeCommand("order:eq-rel", receivedAt);
        var second = MakeCommand("order:eq-rel", receivedAt);
        await _store.StoreCommandAsync(first, receivedAt, CancellationToken.None);
        await _store.StoreCommandAsync(second, receivedAt, CancellationToken.None);

        // Locally completing the predecessor (result_pending) does NOT release its successor: the ERP
        // action for the ordering key is not yet acknowledged, so head-of-line stays blocked (preserved
        // baseline ordering-release semantics).
        await _store.CompleteLocallyAsync(first.CommandId, CommandStatus.SucceededLocal, "{\"status\":\"succeeded\"}", "ref", "1", null, CancellationToken.None);
        Assert.Empty(await _store.GetReadyCommandsAsync(10, DateTimeOffset.UtcNow.AddMinutes(1), CancellationToken.None));

        // Only the ERP result ACK (status -> completed) releases the successor.
        await _store.AcknowledgeResultAsync(first.CommandId, DateTimeOffset.UtcNow, CancellationToken.None);

        var ready = await _store.GetReadyCommandsAsync(10, DateTimeOffset.UtcNow.AddMinutes(1), CancellationToken.None);
        var head = Assert.Single(ready);
        Assert.Equal(second.CommandId, head.Envelope.CommandId);
    }

    [Fact]
    public async Task Ready_order_is_deterministic_and_stable_across_restarts_for_equal_timestamps()
    {
        var receivedAt = DateTimeOffset.UtcNow;
        var ids = new List<Guid>();
        for (var i = 0; i < 4; i++)
        {
            var command = MakeCommand("order:stable", receivedAt);
            ids.Add(command.CommandId);
            await _store.StoreCommandAsync(command, receivedAt, CancellationToken.None);
        }

        var firstPass = (await _store.GetReadyCommandsAsync(10, DateTimeOffset.UtcNow.AddMinutes(1), CancellationToken.None))
            .Select(static c => c.Envelope.CommandId).ToArray();
        // Only the head is visible while the predecessors block the key.
        Assert.Single(firstPass);
        Assert.Equal(ids[0], firstPass[0]);

        // Restart: the durable tie-break keeps the same total order, so the same head stays first.
        await SqliteTestDatabase.ClearPoolAsync(_factory);
        _store = new(_factory, new SqliteMigrator(_factory));
        await _store.InitializeAsync(CancellationToken.None);
        await _store.RecoverAsync(CancellationToken.None);

        var secondPass = (await _store.GetReadyCommandsAsync(10, DateTimeOffset.UtcNow.AddMinutes(1), CancellationToken.None))
            .Select(static c => c.Envelope.CommandId).ToArray();
        Assert.Equal(firstPass, secondPass);

        // Release the head (local result + ERP ACK) and the next command in admission order becomes
        // the new head. result_pending alone does not release; the ERP result ACK does.
        await _store.CompleteLocallyAsync(ids[0], CommandStatus.SucceededLocal, "{\"status\":\"succeeded\"}", "ref", "1", null, CancellationToken.None);
        Assert.Empty(await _store.GetReadyCommandsAsync(10, DateTimeOffset.UtcNow.AddMinutes(1), CancellationToken.None));
        await _store.AcknowledgeResultAsync(ids[0], DateTimeOffset.UtcNow, CancellationToken.None);
        var nextHead = Assert.Single(await _store.GetReadyCommandsAsync(10, DateTimeOffset.UtcNow.AddMinutes(1), CancellationToken.None));
        Assert.Equal(ids[1], nextHead.Envelope.CommandId);
    }

    [Fact]
    public async Task Different_ordering_keys_with_equal_received_time_are_both_ready_and_independent()
    {
        var receivedAt = DateTimeOffset.UtcNow;
        var a = MakeCommand("order:ka", receivedAt);
        var b = MakeCommand("order:kb", receivedAt);
        await _store.StoreCommandAsync(a, receivedAt, CancellationToken.None);
        await _store.StoreCommandAsync(b, receivedAt, CancellationToken.None);

        var ready = await _store.GetReadyCommandsAsync(10, DateTimeOffset.UtcNow.AddMinutes(1), CancellationToken.None);

        // Two different keys are independent: both are ready at the same time even though their
        // receive timestamps match, and neither blocks the other.
        Assert.Equal(2, ready.Count);
        Assert.Contains(ready, c => c.Envelope.CommandId == a.CommandId);
        Assert.Contains(ready, c => c.Envelope.CommandId == b.CommandId);
    }

    // ---- A09 atomic claim guard: concurrent claims, stale snapshots, owner-token release ----

    [Fact]
    public async Task Concurrent_claims_on_the_same_command_yield_exactly_one_winner()
    {
        var receivedAt = DateTimeOffset.UtcNow;
        var command = MakeCommand("order:claim", receivedAt);
        await _store.StoreCommandAsync(command, receivedAt, CancellationToken.None);

        var now = DateTimeOffset.UtcNow;
        var tasks = Enumerable.Range(0, 8).Select(i => _store.TryAcquireCommandExecutionClaimAsync(
            command.CommandId, $"owner-{i}", now, now.AddMinutes(-5), CancellationToken.None)).ToArray();
        var claims = await Task.WhenAll(tasks);

        // Exactly one atomic winner even under real concurrent SQLite writers; the losers get null
        // and must not call 1C.
        Assert.Equal(1, claims.Count(c => c is not null));
        Assert.Equal(7, claims.Count(c => c is null));
    }

    [Fact]
    public async Task Different_ordering_keys_are_claimed_independently_and_both_execute()
    {
        var receivedAt = DateTimeOffset.UtcNow;
        var a = MakeCommand("order:claim-a", receivedAt);
        var b = MakeCommand("order:claim-b", receivedAt);
        await _store.StoreCommandAsync(a, receivedAt, CancellationToken.None);
        await _store.StoreCommandAsync(b, receivedAt, CancellationToken.None);

        var now = DateTimeOffset.UtcNow;
        var tasks = new[]
        {
            _store.TryAcquireCommandExecutionClaimAsync(a.CommandId, "owner-a", now, now.AddMinutes(-5), CancellationToken.None),
            _store.TryAcquireCommandExecutionClaimAsync(b.CommandId, "owner-b", now, now.AddMinutes(-5), CancellationToken.None)
        };
        var claims = await Task.WhenAll(tasks);

        // Independent keys: no overlap and no mutual exclusion; both win their own claim.
        Assert.All(claims, c => Assert.NotNull(c));
    }

    [Fact]
    public async Task Stale_ready_snapshot_cannot_claim_release_or_complete_a_newer_claim()
    {
        var receivedAt = DateTimeOffset.UtcNow;
        var command = MakeCommand("order:stale", receivedAt);
        await _store.StoreCommandAsync(command, receivedAt, CancellationToken.None);

        var t0 = DateTimeOffset.UtcNow;
        // First pass takes the claim.
        Assert.NotNull(await _store.TryAcquireCommandExecutionClaimAsync(command.CommandId, "owner-live", t0, t0.AddMinutes(-5), CancellationToken.None));

        // A concurrent/stale claimant with the same non-stale boundary is refused while the live
        // claim stands (no double execution of the same command/key).
        Assert.Null(await _store.TryAcquireCommandExecutionClaimAsync(command.CommandId, "owner-stale", t0, t0.AddMinutes(-5), CancellationToken.None));

        // The stale snapshot tries to release a claim it does not own: the live claim survives.
        await _store.ReleaseCommandExecutionClaimAsync(command.CommandId, "owner-stale", CancellationToken.None);
        Assert.Equal("owner-live", await StringAsync("SELECT exec_claim_owner_id FROM commands_inbox WHERE command_id=$id", command.CommandId));
        // ... so a third claimant still cannot take it.
        Assert.Null(await _store.TryAcquireCommandExecutionClaimAsync(command.CommandId, "owner-third", t0, t0.AddMinutes(-5), CancellationToken.None));

        // Only the owner's own release frees the row for the next pass.
        await _store.ReleaseCommandExecutionClaimAsync(command.CommandId, "owner-live", CancellationToken.None);
        Assert.Null(await StringAsync("SELECT exec_claim_owner_id FROM commands_inbox WHERE command_id=$id", command.CommandId));
        Assert.NotNull(await _store.TryAcquireCommandExecutionClaimAsync(command.CommandId, "owner-next", t0, t0.AddMinutes(-5), CancellationToken.None));
    }

    [Fact]
    public async Task Stale_claim_beyond_the_documented_boundary_is_taken_over_by_a_later_pass()
    {
        var receivedAt = DateTimeOffset.UtcNow;
        var command = MakeCommand("order:takeover", receivedAt);
        await _store.StoreCommandAsync(command, receivedAt, CancellationToken.None);

        // The simulated crashed pass claims 10 minutes ago, so the row must be due for THAT
        // (past) scheduler tick: make the retry explicitly due instead of relying on a bypass
        // clock around the due guard the claim enforces.
        await ExecuteSqlAsync(
            "UPDATE commands_inbox SET next_attempt_at_utc=$due WHERE command_id=$id",
            ("$due", DateTimeOffset.UtcNow.AddMinutes(-20).ToString("O")),
            ("$id", command.CommandId.ToString("D")));

        var acquiredAt = DateTimeOffset.UtcNow.AddMinutes(-10);
        Assert.NotNull(await _store.TryAcquireCommandExecutionClaimAsync(command.CommandId, "owner-crashed", acquiredAt, acquiredAt.AddMinutes(-5), CancellationToken.None));

        // A later pass whose stale boundary has passed the crashed claim takes it over.
        var now = DateTimeOffset.UtcNow;
        var takeover = await _store.TryAcquireCommandExecutionClaimAsync(command.CommandId, "owner-takeover", now, now.AddMinutes(-5), CancellationToken.None);
        Assert.NotNull(takeover);
        Assert.Equal("owner-takeover", await StringAsync("SELECT exec_claim_owner_id FROM commands_inbox WHERE command_id=$id", command.CommandId));
    }

    [Fact]
    public async Task Recover_releases_claims_and_no_two_claims_overlap_at_the_same_key_after_restart()
    {
        var receivedAt = DateTimeOffset.UtcNow;
        var first = MakeCommand("order:recover", receivedAt);
        var second = MakeCommand("order:recover", receivedAt);
        await _store.StoreCommandAsync(first, receivedAt, CancellationToken.None);
        await _store.StoreCommandAsync(second, receivedAt, CancellationToken.None);

        // First pass claims and crashes mid-POST (status executing, claim held).
        Assert.NotNull(await _store.TryAcquireCommandExecutionClaimAsync(first.CommandId, "owner-crashed", DateTimeOffset.UtcNow, DateTimeOffset.UtcNow.AddMinutes(-5), CancellationToken.None));
        Assert.NotNull(await _store.ClaimPostAttemptAsync(first.CommandId, "owner-crashed", CancellationToken.None));

        // Restart: RecoverAsync releases the crashed claim and re-schedules the row as unknown_result.
        await SqliteTestDatabase.ClearPoolAsync(_factory);
        _store = new(_factory, new SqliteMigrator(_factory));
        await _store.InitializeAsync(CancellationToken.None);
        await _store.RecoverAsync(CancellationToken.None);

        Assert.Equal(CommandStatus.UnknownResult, await StatusAsync(first.CommandId));
        Assert.Null(await StringAsync("SELECT exec_claim_owner_id FROM commands_inbox WHERE command_id=$id", first.CommandId));

        // Nothing is lost and head-of-line selection exposes exactly one command for the key (the
        // successor is not dispatched while the head is still active).
        var ready = await _store.GetReadyCommandsAsync(10, DateTimeOffset.UtcNow.AddMinutes(1), CancellationToken.None);
        Assert.Equal(first.CommandId, Assert.Single(ready).Envelope.CommandId);

        // No overlap at the same key across a restart: two concurrent passes racing for the same
        // command yield exactly one winner (the other is refused and must not call 1C), and the
        // survivor holds the claim (never zero, never two).
        var now = DateTimeOffset.UtcNow;
        var racers = await Task.WhenAll(
            _store.TryAcquireCommandExecutionClaimAsync(first.CommandId, "owner-restarted-a", now, now.AddMinutes(-5), CancellationToken.None),
            _store.TryAcquireCommandExecutionClaimAsync(first.CommandId, "owner-restarted-b", now, now.AddMinutes(-5), CancellationToken.None));
        Assert.Equal(1, racers.Count(c => c is not null));
        Assert.NotNull(await StringAsync("SELECT exec_claim_owner_id FROM commands_inbox WHERE command_id=$id", first.CommandId));
    }

    // ---- A09 dead-letter metric accounting ----

    [Fact]
    public async Task Dead_letter_metric_counts_from_local_completion_and_once_across_ack()
    {
        var receivedAt = DateTimeOffset.UtcNow;
        var command = MakeCommand("order:dead", receivedAt);
        await _store.StoreCommandAsync(command, receivedAt, CancellationToken.None);

        Assert.Equal(0, (await _store.GetQueueMetricsAsync(CancellationToken.None)).CommandsDeadLetter);

        // Local dead-letter completion writes result_status='dead_letter' while status becomes
        // result_pending: counted immediately, before any ERP result ACK.
        await _store.CompleteLocallyAsync(command.CommandId, CommandStatus.DeadLetter, "{\"status\":\"dead_letter\"}", null, null, null, CancellationToken.None);
        var beforeAck = await _store.GetQueueMetricsAsync(CancellationToken.None);
        Assert.Equal(1, beforeAck.CommandsDeadLetter);
        Assert.Equal(1, beforeAck.DeadLetters);

        // The ERP result ACK (status -> completed) must NOT change the count: exactly one per command.
        await _store.AcknowledgeResultAsync(command.CommandId, DateTimeOffset.UtcNow, CancellationToken.None);
        var afterAck = await _store.GetQueueMetricsAsync(CancellationToken.None);
        Assert.Equal(1, afterAck.CommandsDeadLetter);
        Assert.Equal(beforeAck.DeadLetters, afterAck.DeadLetters);
    }

    [Fact]
    public async Task Success_result_is_not_counted_as_a_dead_letter_and_stays_counted_once_after_ack()
    {
        var receivedAt = DateTimeOffset.UtcNow;
        var dead = MakeCommand("order:mix-dead", receivedAt);
        var ok = MakeCommand("order:mix-ok", receivedAt);
        await _store.StoreCommandAsync(dead, receivedAt, CancellationToken.None);
        await _store.StoreCommandAsync(ok, receivedAt, CancellationToken.None);

        await _store.CompleteLocallyAsync(dead.CommandId, CommandStatus.DeadLetter, "{\"status\":\"dead_letter\"}", null, null, null, CancellationToken.None);
        await _store.CompleteLocallyAsync(ok.CommandId, CommandStatus.SucceededLocal, "{\"status\":\"succeeded\"}", "ref", "1", null, CancellationToken.None);

        var metrics = await _store.GetQueueMetricsAsync(CancellationToken.None);
        Assert.Equal(1, metrics.CommandsDeadLetter);

        await _store.AcknowledgeResultAsync(dead.CommandId, DateTimeOffset.UtcNow, CancellationToken.None);
        await _store.AcknowledgeResultAsync(ok.CommandId, DateTimeOffset.UtcNow, CancellationToken.None);

        var after = await _store.GetQueueMetricsAsync(CancellationToken.None);
        Assert.Equal(1, after.CommandsDeadLetter);
    }

    // ---- helpers ----

    private async Task ExecuteSqlAsync(string sql, params (string Name, object Value)[] parameters)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        foreach (var (name, value) in parameters) command.Parameters.AddWithValue(name, value);
        await command.ExecuteNonQueryAsync(CancellationToken.None);
    }

    private async Task<long> ScalarAsync(string sql, Guid commandId)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.Parameters.AddWithValue("$id", commandId.ToString("D"));
        return Convert.ToInt64(await command.ExecuteScalarAsync(CancellationToken.None), System.Globalization.CultureInfo.InvariantCulture);
    }

    private async Task<string?> StringAsync(string sql, Guid commandId)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.Parameters.AddWithValue("$id", commandId.ToString("D"));
        return await command.ExecuteScalarAsync(CancellationToken.None) as string;
    }

    private async Task<CommandStatus> StatusAsync(Guid commandId)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT status FROM commands_inbox WHERE command_id=$id";
        command.Parameters.AddWithValue("$id", commandId.ToString("D"));
        var status = (string?)await command.ExecuteScalarAsync(CancellationToken.None);
        return status is null ? throw new InvalidOperationException($"Command {commandId} not found.") : status switch
        {
            "unknown_result" => CommandStatus.UnknownResult,
            "result_pending" => CommandStatus.ResultPending,
            "completed" => CommandStatus.Completed,
            _ => Enum.Parse<CommandStatus>(status.Replace("_", string.Empty, StringComparison.Ordinal), true)
        };
    }

    private async Task<StoredCommand> ReadyForAsync(Guid commandId) =>
        (await _store.GetReadyCommandsAsync(10, DateTimeOffset.UtcNow.AddDays(1), CancellationToken.None)).Single(c => c.Envelope.CommandId == commandId);

    private static Task<bool> NeverAdministrative(Domain.Commands.CommandEnvelope _, string __, CancellationToken ___) => Task.FromResult(false);

    private static CommandEnvelope MakeCommand(string orderingKey, DateTimeOffset receivedAtUtc)
    {
        using var document = JsonDocument.Parse("{\"amount\":10}");
        var payload = document.RootElement.Clone();
        return new(Guid.NewGuid(), "create_customer_order", 1, 100, orderingKey, null, DateTimeOffset.UtcNow, null, null, null, PayloadHasher.Compute(payload), payload);
    }

    private static OnecExecutionResult SuccessResult(Guid id) =>
        new(OnecExecutionKind.Succeeded, new(id, "succeeded", JsonSerializer.SerializeToElement(new { @ref = "synthetic-ref" }), null, [], 1), 200, null, null);

    private sealed class FakeOnec : IOnecCommandClient
    {
        public int ExecuteCalls { get; private set; }
        public int StatusCalls { get; private set; }
        public Guid LastExecutedId { get; private set; }
        public OnecExecutionKind StatusKind { get; init; } = OnecExecutionKind.Succeeded;
        public OnecExecutionKind ExecuteKind { get; init; } = OnecExecutionKind.Succeeded;
        public Func<CommandEnvelope, CancellationToken, Task<OnecExecutionResult>>? OnExecute { get; init; }

        public Task<OnecExecutionResult> ExecuteAsync(CommandEnvelope command, CancellationToken cancellationToken)
        {
            ExecuteCalls++;
            LastExecutedId = command.CommandId;
            return OnExecute?.Invoke(command, cancellationToken) ?? Task.FromResult(ForKind(ExecuteKind, command.CommandId));
        }

        public Task<OnecExecutionResult> GetStatusAsync(Guid commandId, CancellationToken cancellationToken)
        {
            StatusCalls++;
            return Task.FromResult(ForKind(StatusKind, commandId));
        }

        private static OnecExecutionResult ForKind(OnecExecutionKind kind, Guid id) => kind switch
        {
            OnecExecutionKind.Succeeded => SuccessResult(id),
            OnecExecutionKind.BusinessError => new(OnecExecutionKind.BusinessError,
                new(id, "business_failed", null, new("ONEC_BUSINESS_ERROR", "rejected", false, null), [], 1), 200, "ONEC_BUSINESS_ERROR", "rejected"),
            _ => new(kind, null, 202, null, null)
        };
    }
}
