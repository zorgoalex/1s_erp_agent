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
/// A09 concurrency/claim-fencing regressions against the CURRENT API only (no imaginary overloads):
/// a held fresh-execution claim must fence concurrent status-lookup passes to exactly one network
/// call; a stale ready snapshot (older next_attempt_at_utc) must not re-POST; two claimants of the
/// same key must never both execute; the claim owner must be captured INSIDE the network call (not
/// after the finally-release); and cancellation must release the claim in finally while preserving
/// the potentially-sent evidence. Real SQLite + real SqliteAgentStore; 1C is a fake client gated by
/// TaskCompletionSource so a pass really is "in flight" while we assert. No external ERP/1C contact.
/// </summary>
public sealed class CommandOrderingClaimFencingTests : IAsyncLifetime
{
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

    // ---- regression 1: concurrent status lookups of a held-gate command: exactly ONE network call ----

    [Fact]
    public async Task Concurrent_lookup_passes_of_one_command_make_exactly_one_status_call()
    {
        // Potentially-sent seed via the EXISTING API only: MarkExecutingAsync (operational POST
        // attempt recorded) then RecoverAsync (restart recovery -> unknown_result, resolvable via
        // lookup; no fresh ready ever created for a status='executing' row).
        var command = MakeCommand("order:lookup-once");
        await _store.StoreCommandAsync(command, DateTimeOffset.UtcNow, CancellationToken.None);
        await _store.MarkExecutingAsync(command.CommandId, CancellationToken.None);
        await _store.RecoverAsync(CancellationToken.None);

        // The legacy MarkExecutingAsync setup POST is a REAL persisted attempt: the post counter and
        // exactly one unfinished 'post' audit row exist BEFORE any network of this test.
        Assert.Equal(1, await ScalarAsync("SELECT post_attempt_count FROM commands_inbox WHERE command_id=$id", command.CommandId));
        Assert.Equal(1, await ScalarAsync("SELECT COUNT(*) FROM command_attempts WHERE command_id=$id AND attempt_kind='post' AND finished_at_utc IS NULL", command.CommandId));

        // Held gate: the lookup network call blocks until both passes are in flight, so the second
        // stale snapshot really races the first (not sequential).
        var gate = new ReleaseGate();
        var onec = new FakeOnec { OnStatus = (id, _) => gate.GateAsync() };
        var service = new CommandExecutionService(_store, onec, static () => 12);

        var first = await ReadyForAsync(command.CommandId);
        var second = await ReadyForAsync(command.CommandId);
        var passA = Task.Run(() => service.ProcessAsync(first, NeverAdministrative, CancellationToken.None));
        var passB = Task.Run(() => service.ProcessAsync(second, NeverAdministrative, CancellationToken.None));
        await gate.Entered.Task.WaitAsync(GateTimeout);
        gate.Release.SetResult(true);
        await Task.WhenAll(passA, passB).WaitAsync(GateTimeout);

        // Defect reproduction target: exactly ONE lookup may reach the network; the loser must be
        // fenced by the durable lookup claim and touch nothing. ExecuteCalls stays 0: an
        // unknown_result row must never fall back to a fresh POST here.
        Assert.Equal(1, onec.StatusCalls);
        Assert.Equal(0, onec.ExecuteCalls);
        Assert.Equal(1, await ScalarAsync("SELECT lookup_attempt_count FROM commands_inbox WHERE command_id=$id", command.CommandId));
        // Legitimate count correction: the crashed setup POST stays as unknown-outcome evidence
        // (never sealed or rewritten); the fenced passes may add EXACTLY ONE lookup attempt row.
        Assert.Equal(1, await ScalarAsync("SELECT COUNT(*) FROM command_attempts WHERE command_id=$id AND attempt_kind='lookup'", command.CommandId));
        Assert.Equal(1, await ScalarAsync("SELECT COUNT(*) FROM command_attempts WHERE command_id=$id AND attempt_kind='post' AND finished_at_utc IS NULL", command.CommandId));
    }

    // ---- regression 2: stale future next_attempt_at snapshot must NOT re-POST ----

    [Fact]
    public async Task Stale_future_next_attempt_snapshot_never_posts()
    {
        // Potentially-sent seed via the existing API: one POST attempt happened (attempt_count=1,
        // first_sent_at_utc set), then the pass died inside the POST -> RecoverAsync marks
        // unknown_result with the re-resolve schedule.
        var command = MakeCommand("order:stale-next");
        await _store.StoreCommandAsync(command, DateTimeOffset.UtcNow, CancellationToken.None);
        await _store.MarkExecutingAsync(command.CommandId, CancellationToken.None);
        await _store.RecoverAsync(CancellationToken.None);

        // Stale ready snapshot taken now; a later pass then re-schedules the row into the future.
        var stale = await ReadyForAsync(command.CommandId);
        await ExecuteSqlAsync(
            "UPDATE commands_inbox SET next_attempt_at_utc=$future WHERE command_id=$id",
            ("$future", DateTimeOffset.UtcNow.AddHours(1).ToString("O")),
            ("$id", command.CommandId.ToString("D")));

        var onec = new FakeOnec { StatusKind = OnecExecutionKind.NotFound };
        var service = new CommandExecutionService(_store, onec, static () => 12);
        await service.ProcessAsync(stale, NeverAdministrative, CancellationToken.None).WaitAsync(GateTimeout);

        // Defect reproduction target: a stale snapshot holding only a past due-state must never
        // trigger a fresh POST (1C already may hold this command id). At most a status lookup may
        // run; ExecuteCalls must stay 0 and no new POST attempt may be recorded.
        Assert.Equal(0, onec.ExecuteCalls);
        Assert.Equal(1, await ScalarAsync("SELECT post_attempt_count FROM commands_inbox WHERE command_id=$id", command.CommandId));
    }

    // ---- regression 3: same key: atomic claim - a second claimant is rejected, first is in flight ----

    [Fact]
    public async Task Same_key_second_claimant_rejected_while_first_post_is_in_flight()
    {
        var receivedAt = DateTimeOffset.UtcNow;
        var command = MakeCommand("order:atomic-claim", receivedAt);
        await _store.StoreCommandAsync(command, receivedAt, CancellationToken.None);

        // FIRST pass holds its POST in flight (claim held for the whole gate).
        var gate = new ReleaseGate();
        var onec = new FakeOnec { OnExecute = (envelope, _) => gate.GateAsync() };
        var service = new CommandExecutionService(_store, onec, static () => 12);
        var snapshot = await ReadyForAsync(command.CommandId);
        var pass = Task.Run(() => service.ProcessAsync(snapshot, NeverAdministrative, CancellationToken.None));
        await gate.Entered.Task.WaitAsync(GateTimeout);

        // While the POST is in flight the durable fresh-execution claim is held by the first pass. A
        // SECOND claimant of the SAME key/command must lose the atomic claim and reach the network
        // zero times. The atomic claim is the fence (the row may still appear ready; the claim - not
        // the ready state - is what rejects the second claimant).
        var secondClaim = await _store.TryAcquireCommandExecutionClaimAsync(
            command.CommandId, "second-claimant-" + Guid.NewGuid().ToString("N"),
            DateTimeOffset.UtcNow, DateTimeOffset.UtcNow.AddMinutes(-5), CancellationToken.None);
        var loserService = new CommandExecutionService(_store, onec, static () => 12, null, null, executorId: "racer-" + Guid.NewGuid().ToString("N"));
        if (secondClaim is not null)
        {
            // Defect: the atomic claim let a second claimant through while the first POST is still in
            // flight. Process the leaked snapshot too, so the double-POST is observed on the network.
            var leaked = await ReadyForAsync(command.CommandId);
            await loserService.ProcessAsync(leaked, NeverAdministrative, CancellationToken.None).WaitAsync(GateTimeout);
        }

        // Defect reproduction target: the second claim must be rejected (null) and exactly ONE POST
        // must reach the network - one atomic claim winner, one durable attempt.
        Assert.Null(secondClaim);
        Assert.Equal(1, onec.ExecuteCalls);
        Assert.Equal(1, await ScalarAsync("SELECT COUNT(*) FROM command_attempts WHERE command_id=$id", command.CommandId));
        Assert.Equal(1, await ScalarAsync("SELECT post_attempt_count FROM commands_inbox WHERE command_id=$id", command.CommandId));

        gate.Release.SetResult(true);
        await pass.WaitAsync(GateTimeout);
    }

    // ---- regression 4: owner captured INSIDE the network call - a different owner must never complete ----

    [Fact]
    public async Task Owner_claim_captured_inside_network_call_is_the_pass_owner()
    {
        var receivedAt = DateTimeOffset.UtcNow;
        var command = MakeCommand("order:owner-inside", receivedAt);
        await _store.StoreCommandAsync(command, receivedAt, CancellationToken.None);

        // The pass's owner token must be captured INSIDE the network call (while its claim is held),
        // NOT after a finally-release. Capturing "after the finally release" (a defect) would observe
        // a cleared/foreign owner and mis-attribute the terminal transition to a different pass.
        var gate = new ReleaseGate();
        var ownerInsideCall = default(string?);
        string? unobservedOwner = null;
        var onec = new FakeOnec
        {
            OnExecute = async (envelope, _) =>
            {
                // Capture the owner DURING the in-flight call (claim is held here).
                ownerInsideCall = await StringAsync("SELECT exec_claim_owner_id FROM commands_inbox WHERE command_id=$id", envelope.CommandId);
                // DEFECT SHAPE under test: if the pass captured its owner only AFTER the finally
                // release (instead of inside this call), the captured value would be the post-release
                // row owner. Model that mis-capture here and require it to differ from the real
                // in-flight owner - proving the in-flight capture is the authoritative pass owner.
                await _store.MarkUnknownResultAsync(envelope.CommandId, ownerInsideCall!, "CANCELLED_MIDFLIGHT", "simulated release", null, CancellationToken.None);
                unobservedOwner = await StringAsync("SELECT exec_claim_owner_id FROM commands_inbox WHERE command_id=$id", envelope.CommandId);
                gate.Release.SetResult(true);
                return SucceededResult(envelope.CommandId);
            }
        };
        var service = new CommandExecutionService(_store, onec, static () => 12, null, null, executorId: "owner-inside-exec");
        await service.ProcessAsync(await ReadyForAsync(command.CommandId), NeverAdministrative, CancellationToken.None).WaitAsync(GateTimeout);
        await gate.Release.Task.WaitAsync(GateTimeout);

        // The owner observed INSIDE the network call must be the pass's own claim owner (minted for
        // this executor/pass), never null and never a different pass's token. This is the fence that
        // lets the pass complete its own row and reject any other owner.
        Assert.NotNull(ownerInsideCall);
        Assert.StartsWith("owner-inside-exec", ownerInsideCall, StringComparison.Ordinal);
        // The post-release (finally) owner capture must NOT be mistaken for the pass owner: it is
        // cleared and therefore different from the authoritative in-flight owner.
        Assert.NotEqual(ownerInsideCall, unobservedOwner);

        Assert.Equal(CommandStatus.UnknownResult, await StatusAsync(command.CommandId));
        Assert.Null(await StringAsync("SELECT result_status FROM commands_inbox WHERE command_id=$id", command.CommandId));
        Assert.Equal(1, await ScalarAsync("SELECT COUNT(*) FROM command_attempts WHERE command_id=$id", command.CommandId));
        Assert.Empty(await _store.GetPendingResultsAsync(10, DateTimeOffset.UtcNow, CancellationToken.None));
    }

    // ---- regression 5: cancellation releases ONLY its own claim in finally and preserves evidence ----

    [Fact]
    public async Task Cancellation_releases_owner_claim_in_finally_and_preserves_potentially_sent_evidence()
    {
        var receivedAt = DateTimeOffset.UtcNow;
        var command = MakeCommand("order:cancel", receivedAt);
        await _store.StoreCommandAsync(command, receivedAt, CancellationToken.None);

        // A blocked POST: the command may already have reached 1C when we cancel.
        var gate = new ReleaseGate();
        var onec = new FakeOnec { OnExecute = (envelope, token) => gate.GateAsync(exceptionOnRelease: new OperationCanceledException(token)) };
        var service = new CommandExecutionService(_store, onec, static () => 12);
        using var cts = new CancellationTokenSource();
        var pass = Task.Run(() => service.ProcessAsync(ReadyForAsync(command.CommandId).GetAwaiter().GetResult(), NeverAdministrative, cts.Token));
        await gate.Entered.Task.WaitAsync(GateTimeout);

        cts.Cancel();
        gate.Release.SetException(new OperationCanceledException(cts.Token));
        // Cancellation is observed as an OperationCanceledException out of the pass.
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pass).WaitAsync(GateTimeout);

        // The pass released ONLY its own claim in finally and left the row claimable again (no leak).
        Assert.Null(await StringAsync("SELECT exec_claim_owner_id FROM commands_inbox WHERE command_id=$id", command.CommandId));
        // Potentially-sent POST evidence is preserved (post attempt recorded, first_sent_at_utc set).
        Assert.Equal(1, await ScalarAsync("SELECT post_attempt_count FROM commands_inbox WHERE command_id=$id", command.CommandId));
        Assert.NotNull(await DateScalarAsync("SELECT first_sent_at_utc FROM commands_inbox WHERE command_id=$id", command.CommandId));
        // ClaimPostAttempt left it 'executing'; the finally-release must not rewrite that status, so a
        // potentially sent command is preserved and never silently retried as fresh.
        Assert.Equal("executing", await StringAsync("SELECT status FROM commands_inbox WHERE command_id=$id", command.CommandId));

        // A stale snapshot after that ambiguous POST must NOT re-POST: only a status lookup.
        await _store.RecoverAsync(CancellationToken.None);
        var recover = new CommandExecutionService(_store, onec, static () => 12);
        await recover.ProcessAsync(await ReadyForAsync(command.CommandId), NeverAdministrative, CancellationToken.None).WaitAsync(GateTimeout);
        Assert.Equal(1, onec.ExecuteCalls);
        Assert.True(onec.StatusCalls >= 1);
    }

    // ---- regression 5b (companion): different keys run in parallel - owned shared gate, not per-call gates ----

    [Fact]
    public async Task Different_ordering_keys_execute_in_parallel_against_one_shared_gate()
    {
        var receivedAt = DateTimeOffset.UtcNow;
        var a = MakeCommand("order:par-a", receivedAt);
        var b = MakeCommand("order:par-b", receivedAt);
        await _store.StoreCommandAsync(a, receivedAt, CancellationToken.None);
        await _store.StoreCommandAsync(b, receivedAt, CancellationToken.None);

        // ONE owned shared gate for both keys (a fresh untracked gate per call could never prove
        // overlap): both POSTs must be inside the gate concurrently before it is released.
        var gate = new ReleaseGate();
        var onec = new FakeOnec { OnExecute = (envelope, _) => gate.GateAsync() };
        var service = new CommandExecutionService(_store, onec, static () => 12);

        var passA = Task.Run(() => service.ProcessAsync(ReadyForAsync(a.CommandId).GetAwaiter().GetResult(), NeverAdministrative, CancellationToken.None));
        var passB = Task.Run(() => service.ProcessAsync(ReadyForAsync(b.CommandId).GetAwaiter().GetResult(), NeverAdministrative, CancellationToken.None));

        // Both POSTs must be in flight at the same time: keys are independent and do not serialize.
        await gate.EnteredAllAsync(2).WaitAsync(GateTimeout);
        gate.ReleaseAll();
        await Task.WhenAll(passA, passB).WaitAsync(GateTimeout);

        Assert.Equal(2, onec.ExecuteCalls);
        Assert.Equal(CommandStatus.ResultPending, await StatusAsync(a.CommandId));
        Assert.Equal(CommandStatus.ResultPending, await StatusAsync(b.CommandId));
    }

    // ---- regression 6: expiry is decided from LIVE send evidence inside the owned pass (A02) ----

    [Fact]
    public async Task Expired_stale_queued_snapshot_with_actual_prior_send_resolves_status_instead_of_expiring()
    {
        // Stale snapshot captured while the command was fresh/queued and its expiry was still ahead;
        // a prior pass then REALLY sent it (MarkExecutingAsync POST evidence -> unknown_result via
        // Recover) and the expiry only passed afterwards. Expiry decided from the stale "never sent"
        // snapshot (as before the claim) would SaveError expired before any lookup, violating A02:
        // a potentially-sent command must always resolve its status first.
        var command = MakeCommandWithExpiry("order:expired-stale-sent", DateTimeOffset.UtcNow.AddSeconds(2));
        await _store.StoreCommandAsync(command, DateTimeOffset.UtcNow, CancellationToken.None);
        var snapshot = await ReadyForAsync(command.CommandId);

        await _store.MarkExecutingAsync(command.CommandId, CancellationToken.None);
        await _store.RecoverAsync(CancellationToken.None);
        await Task.Delay(2600); // the snapshot's expiry passes while the row is genuinely potentially-sent

        var onec = new FakeOnec { StatusKind = OnecExecutionKind.Succeeded };
        var service = new CommandExecutionService(_store, onec, static () => 12);
        await service.ProcessAsync(snapshot, NeverAdministrative, CancellationToken.None).WaitAsync(GateTimeout);

        // The row really was sent: the pass must resolve via status lookup, never expire it, and the
        // persisted result must be the lookup's success (not an expired terminal overwrite).
        Assert.Equal(1, onec.StatusCalls);
        Assert.Equal(0, onec.ExecuteCalls);
        Assert.Equal("succeeded_local", await StringAsync("SELECT result_status FROM commands_inbox WHERE command_id=$id", command.CommandId));
        Assert.Equal("succeeded", (await ResultForAsync(command.CommandId)).GetProperty("status").GetString());
    }

    [Fact]
    public async Task Expired_stale_snapshot_for_terminal_result_never_mutates_or_calls_network()
    {
        // The row was driven TERMINAL by a real pass (fresh POST success -> result_pending
        // 'succeeded_local') before this queued snapshot's expiry lapsed. A pre-claim expiry decision
        // from the stale snapshot would CompleteLocally-overwrite the newer result (its only guard is
        // status <> 'completed') — a terminal row must never be re-opened by an unowned stale pass.
        var command = MakeCommandWithExpiry("order:expired-stale-terminal", DateTimeOffset.UtcNow.AddSeconds(2));
        await _store.StoreCommandAsync(command, DateTimeOffset.UtcNow, CancellationToken.None);
        var snapshot = await ReadyForAsync(command.CommandId);

        var postOnec = new FakeOnec();
        var postService = new CommandExecutionService(_store, postOnec, static () => 12);
        await postService.ProcessAsync(await ReadyForAsync(command.CommandId), NeverAdministrative, CancellationToken.None).WaitAsync(GateTimeout);
        Assert.Equal(1, postOnec.ExecuteCalls);
        await Task.Delay(2600); // the stale snapshot's expiry passes after the row went terminal

        var onec = new FakeOnec { StatusKind = OnecExecutionKind.Succeeded };
        var service = new CommandExecutionService(_store, onec, static () => 12);
        await service.ProcessAsync(snapshot, NeverAdministrative, CancellationToken.None).WaitAsync(GateTimeout);

        // Terminal rows are not claimable: zero network, zero mutation, newer result untouched, no claim.
        Assert.Equal(0, onec.StatusCalls);
        Assert.Equal(0, onec.ExecuteCalls);
        Assert.Equal("succeeded_local", await StringAsync("SELECT result_status FROM commands_inbox WHERE command_id=$id", command.CommandId));
        Assert.Equal("succeeded", (await ResultForAsync(command.CommandId)).GetProperty("status").GetString());
        Assert.Null(await StringAsync("SELECT exec_claim_owner_id FROM commands_inbox WHERE command_id=$id", command.CommandId));
    }

    // =============================== helpers ===============================

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

    private async Task<DateTimeOffset?> DateScalarAsync(string sql, Guid commandId)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.Parameters.AddWithValue("$id", commandId.ToString("D"));
        var value = await command.ExecuteScalarAsync(CancellationToken.None);
        return value is string text ? DateTimeOffset.Parse(text, System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.RoundtripKind).ToUniversalTime() : null;
    }

    private async Task ExecuteSqlAsync(string sql, params (string Name, object Value)[] parameters)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        foreach (var (name, value) in parameters) command.Parameters.AddWithValue(name, value);
        await command.ExecuteNonQueryAsync(CancellationToken.None);
    }

    private static Task<bool> NeverAdministrative(CommandEnvelope _, string __, CancellationToken ___) => Task.FromResult(false);

    private async Task<StoredCommand> ReadyForAsync(Guid commandId) =>
        (await _store.GetReadyCommandsAsync(10, DateTimeOffset.UtcNow.AddDays(1), CancellationToken.None)).Single(c => c.Envelope.CommandId == commandId);

    private async Task<JsonElement> ResultForAsync(Guid commandId)
    {
        var pending = await _store.GetPendingResultsAsync(10, DateTimeOffset.UtcNow, CancellationToken.None);
        var match = pending.Single(p => p.CommandId == commandId);
        using var document = JsonDocument.Parse(match.PayloadJson);
        return document.RootElement.Clone();
    }

    private async Task<CommandStatus> StatusAsync(Guid commandId)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT status FROM commands_inbox WHERE command_id=$id";
        command.Parameters.AddWithValue("$id", commandId.ToString("D"));
        var status = (string?)await command.ExecuteScalarAsync(CancellationToken.None);
        return status is null ? throw new InvalidOperationException($"Command {commandId} not found.") : ParseStatus(status);
    }

    private static CommandStatus ParseStatus(string status) => status switch
    {
        "queued" => CommandStatus.Queued,
        "retry_waiting" => CommandStatus.RetryWaiting,
        "unknown_result" => CommandStatus.UnknownResult,
        "dead_letter" => CommandStatus.DeadLetter,
        "result_pending" => CommandStatus.ResultPending,
        "succeeded_local" => CommandStatus.SucceededLocal,
        "business_failed_local" => CommandStatus.BusinessFailedLocal,
        _ => Enum.Parse<CommandStatus>(status.Replace("_", string.Empty, StringComparison.Ordinal), true)
    };

    /// <summary>Valid envelope: fresh Guid + real payload hash over a real JSON payload (CommandEnvelope stays valid for store guards).</summary>
    private static CommandEnvelope MakeCommand(string? orderingKey, DateTimeOffset? receivedAtUtc = null)
    {
        using var document = JsonDocument.Parse("{\"amount\":10}");
        var payload = document.RootElement.Clone();
        return new(Guid.NewGuid(), "create_customer_order", 1, 100, orderingKey ?? "order:42", null, receivedAtUtc ?? DateTimeOffset.UtcNow, null, null, null, PayloadHasher.Compute(payload), payload);
    }

    /// <summary>Same valid envelope as <see cref="MakeCommand"/> but with an explicit expiry (A02 expiry-stale-snapshot regressions).</summary>
    private static CommandEnvelope MakeCommandWithExpiry(string orderingKey, DateTimeOffset expiresAtUtc)
    {
        using var document = JsonDocument.Parse("{\"amount\":10}");
        var payload = document.RootElement.Clone();
        return new(Guid.NewGuid(), "create_customer_order", 1, 100, orderingKey, null, DateTimeOffset.UtcNow, null, expiresAtUtc, null, PayloadHasher.Compute(payload), payload);
    }

    private static OnecExecutionResult SucceededResult(Guid id) =>
        new(OnecExecutionKind.Succeeded, new(id, "succeeded", JsonSerializer.SerializeToElement(new { @ref = "synthetic-ref" }), null, [], 1), 200, null, null);

    /// <summary>
    /// ONE owned gate shared by every network call of a test. Callers enter (count up) and the test
    /// releases all at once, so overlap is proven against a single owned TaskCompletionSource rather
    /// than a fresh untracked gate per call (which could never observe concurrency).
    /// </summary>
    private sealed class ReleaseGate
    {
        private readonly TaskCompletionSource<bool> _entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource<bool> _release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _arrived;

        public TaskCompletionSource<bool> Entered => _entered;
        public TaskCompletionSource<bool> Release => _release;

        public async Task<OnecExecutionResult> GateAsync(Exception? exceptionOnRelease = null)
        {
            if (Interlocked.Increment(ref _arrived) == 1) _entered.TrySetResult(true);
            await _release.Task;
            if (exceptionOnRelease is not null) throw exceptionOnRelease;
            return SucceededResult(Guid.Empty);
        }

        public async Task EnteredAllAsync(int count)
        {
            while (Volatile.Read(ref _arrived) < count) await Task.Delay(10);
            _entered.TrySetResult(true);
        }

        public void ReleaseAll() => _release.TrySetResult(true);
    }

    private sealed class FakeOnec : IOnecCommandClient
    {
        private int _executeCalls;
        private int _statusCalls;

        public int ExecuteCalls => Volatile.Read(ref _executeCalls);
        public int StatusCalls => Volatile.Read(ref _statusCalls);
        public Guid LastExecutedId { get; private set; }
        public OnecExecutionKind StatusKind { get; init; } = OnecExecutionKind.Succeeded;
        public OnecExecutionKind ExecuteKind { get; init; } = OnecExecutionKind.Succeeded;
        public Func<CommandEnvelope, CancellationToken, Task<OnecExecutionResult>>? OnExecute { get; init; }
        public Func<Guid, CancellationToken, Task<OnecExecutionResult>>? OnStatus { get; init; }

        public Task<OnecExecutionResult> ExecuteAsync(CommandEnvelope command, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _executeCalls);
            LastExecutedId = command.CommandId;
            return OnExecute?.Invoke(command, cancellationToken) ?? Task.FromResult(ForKind(ExecuteKind, command.CommandId));
        }

        public Task<OnecExecutionResult> GetStatusAsync(Guid commandId, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _statusCalls);
            return OnStatus?.Invoke(commandId, cancellationToken) ?? Task.FromResult(ForKind(StatusKind, commandId));
        }

        private static OnecExecutionResult ForKind(OnecExecutionKind kind, Guid id) => kind switch
        {
            OnecExecutionKind.Succeeded => SucceededResult(id),
            OnecExecutionKind.BusinessError => new(OnecExecutionKind.BusinessError,
                new(id, "business_failed", null, new("ONEC_BUSINESS_ERROR", "rejected", false, null), [], 1), 200, "ONEC_BUSINESS_ERROR", "rejected"),
            _ => new(kind, null, 202, null, null)
        };
    }
}
