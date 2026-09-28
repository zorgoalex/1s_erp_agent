using System.Diagnostics;
using System.Text.Json;
using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Application.Configuration;
using ErpOnecAgent.Contracts.Erp;
using ErpOnecAgent.Contracts.OneC;
using ErpOnecAgent.Domain.Agent;
using ErpOnecAgent.Domain.Commands;
using ErpOnecAgent.Domain.Common;
using ErpOnecAgent.Domain.Etl;
using ErpOnecAgent.Infrastructure.Persistence.Sqlite;
using ErpOnecAgent.Service.Diagnostics;
using ErpOnecAgent.Service.Runtime;
using ErpOnecAgent.Service.Workers;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace ErpOnecAgent.IntegrationTests;

/// <summary>
/// L2: ERP hands out no command while the lease reports executing &gt;= capacity and holds such a
/// lease for the whole long poll (agent-bridge to-onec/0045). The agent therefore never leases at
/// full capacity (executing + accepted-but-not-started), reports that load, and asks again as soon
/// as a slot is released — a queued batch runs at execution speed, not one long poll per command.
/// </summary>
public sealed class L2CommandCapacityTests : IAsyncLifetime
{
    private static readonly TimeSpan BoundedWait = TimeSpan.FromSeconds(15);
    // Stands in for the 25 s long poll ERP holds a full-capacity lease for.
    private static readonly TimeSpan FullCapacityHold = TimeSpan.FromSeconds(3);
    private readonly SqliteTestDatabase _database = new();
    private SqliteConnectionFactory _factory = null!;
    private SqliteAgentStore _store = null!;

    public async Task InitializeAsync()
    {
        _factory = _database.CreateFactory(Path.Combine("data", "l2.db"));
        _store = new(_factory, new SqliteMigrator(_factory));
        await _store.InitializeAsync(CancellationToken.None);
    }

    public async Task DisposeAsync() => await _database.DisposeAsync();

    [Fact]
    public async Task A_queued_batch_runs_at_execution_speed_and_no_lease_claims_full_capacity()
    {
        var signals = new CommandWorkSignals();
        var state = ReadyState();
        var commands = Enumerable.Range(0, 4).Select(static _ => MakeEnvelope()).ToArray();
        var erp = new GatingErp(commands);
        using var sessions = new ErpSessionManager(erp, AgentOptions(), state);
        using var lease = CreateLeaseWorker(erp, sessions, state, signals, capacity: 1);
        using var execution = CreateExecutionWorker(state, signals, capacity: 1);
        using var delivery = new ResultDeliveryWorker(_store, erp, state, NullLogger<ResultDeliveryWorker>.Instance, signals);
        var done = commands.Select(command => erp.ExpectResult(command.CommandId)).ToArray();
        var watch = Stopwatch.StartNew();

        await execution.StartAsync(CancellationToken.None);
        await delivery.StartAsync(CancellationToken.None);
        await lease.StartAsync(CancellationToken.None);
        try
        {
            await Task.WhenAll(done).WaitAsync(BoundedWait);
        }
        finally
        {
            await StopAsync(lease);
            await StopAsync(delivery);
            await StopAsync(execution);
        }

        // Before L2 the lease right after intake reported 1/1, ERP held it for the full poll, and
        // every further command of the batch cost one hold (here >= 3 x 3 s).
        Assert.True(watch.Elapsed < FullCapacityHold, $"Batch of 4 took {watch.Elapsed.TotalMilliseconds} ms");
        Assert.DoesNotContain(erp.Loads, static load => load.Executing >= load.Capacity);
    }

    [Fact]
    public async Task At_full_capacity_no_lease_goes_out_until_a_slot_is_released()
    {
        var signals = new CommandWorkSignals();
        var state = ReadyState();
        // Accepted but not started: occupies the only slot.
        await _store.StoreCommandAsync(MakeEnvelope(), DateTimeOffset.UtcNow, CancellationToken.None);
        var erp = new GatingErp([]);
        using var sessions = new ErpSessionManager(erp, AgentOptions(), state);
        using var lease = CreateLeaseWorker(erp, sessions, state, signals, capacity: 1);
        using var execution = CreateExecutionWorker(state, signals, capacity: 1);

        await lease.StartAsync(CancellationToken.None);
        try
        {
            await Task.Delay(1500);
            Assert.Equal(0, erp.LeaseCalls);

            var released = Stopwatch.StartNew();
            await execution.StartAsync(CancellationToken.None);
            await erp.WaitForCallsAsync(1);
            Assert.True(erp.LeaseCalls >= 1, "No lease after the slot was released.");
            Assert.True(released.Elapsed < TimeSpan.FromSeconds(2), $"First lease {released.Elapsed.TotalMilliseconds} ms after the slot was released");
            Assert.Equal(new LeaseLoad(0, 1), erp.Loads[0]);
        }
        finally
        {
            await StopAsync(lease);
            await StopAsync(execution);
        }
    }

    [Fact]
    public async Task The_lease_reports_accepted_but_not_started_commands_as_load()
    {
        var signals = new CommandWorkSignals();
        var state = ReadyState();
        await _store.StoreCommandAsync(MakeEnvelope(), DateTimeOffset.UtcNow, CancellationToken.None);
        await _store.StoreCommandAsync(MakeEnvelope(), DateTimeOffset.UtcNow, CancellationToken.None);
        var erp = new GatingErp([]);
        using var sessions = new ErpSessionManager(erp, AgentOptions(), state);
        using var lease = CreateLeaseWorker(erp, sessions, state, signals, capacity: 4);

        await lease.StartAsync(CancellationToken.None);
        try
        {
            await erp.WaitForCallsAsync(1);
        }
        finally
        {
            await StopAsync(lease);
        }

        Assert.Equal(new LeaseLoad(2, 4), erp.Loads[0]);
    }

    [Fact]
    public async Task A_command_waiting_for_its_not_before_time_does_not_hold_leasing_back()
    {
        var signals = new CommandWorkSignals();
        var state = ReadyState();
        await _store.StoreCommandAsync(MakeEnvelope(notBefore: DateTimeOffset.UtcNow.AddHours(1)), DateTimeOffset.UtcNow, CancellationToken.None);
        var erp = new GatingErp([]);
        using var sessions = new ErpSessionManager(erp, AgentOptions(), state);
        using var lease = CreateLeaseWorker(erp, sessions, state, signals, capacity: 1);

        await lease.StartAsync(CancellationToken.None);
        try
        {
            await erp.WaitForCallsAsync(1);
        }
        finally
        {
            await StopAsync(lease);
        }

        Assert.True(erp.LeaseCalls >= 1);
        Assert.Equal(new LeaseLoad(0, 1), erp.Loads[0]);
    }

    [Fact]
    public async Task Only_unclaimed_ready_commands_are_counted()
    {
        var now = DateTimeOffset.UtcNow;
        await _store.StoreCommandAsync(MakeEnvelope(), now, CancellationToken.None);
        await _store.StoreCommandAsync(MakeEnvelope(notBefore: now.AddHours(1)), now, CancellationToken.None);
        var claimed = MakeEnvelope();
        await _store.StoreCommandAsync(claimed, now, CancellationToken.None);
        await using (var connection = await _factory.OpenAsync(CancellationToken.None))
        await using (var command = connection.CreateCommand())
        {
            command.CommandText = "UPDATE commands_inbox SET exec_claim_owner_id='other-executor', exec_claim_acquired_at_utc=$now WHERE command_id=$id;";
            command.Parameters.AddWithValue("$now", now.ToString("O"));
            command.Parameters.AddWithValue("$id", claimed.CommandId.ToString("D"));
            await command.ExecuteNonQueryAsync();
        }

        Assert.Equal(1, await _store.CountReadyUnclaimedCommandsAsync(DateTimeOffset.UtcNow, DateTimeOffset.UtcNow, CancellationToken.None));
    }

    // ---- helpers ----

    private static AgentRuntimeState ReadyState()
    {
        var state = new AgentRuntimeState();
        state.RestoreLocalEtlPause(false);
        state.SetRemoteMode(AgentMode.Normal);
        state.CompleteBootstrap();
        return state;
    }

    private CommandLeaseWorker CreateLeaseWorker(GatingErp erp, ErpSessionManager sessions, AgentRuntimeState state, CommandWorkSignals signals, int capacity) =>
        new(
            erp,
            _store,
            sessions,
            state,
            new DynamicConfigurationState(Options.Create(new CommandOptions { SupportedTypes = ["synthetic"] }), Options.Create(new EtlOptions { Entities = [], IntervalMinutes = 60 })),
            Options.Create(new ErpOptions { RequireClientCertificate = false, LongPollSeconds = 1 }),
            Options.Create(new CommandOptions { MaxConcurrency = capacity, SupportedTypes = ["synthetic"] }),
            NullLogger<CommandLeaseWorker>.Instance,
            signals);

    private CommandExecutionWorker CreateExecutionWorker(AgentRuntimeState state, CommandWorkSignals signals, int capacity)
    {
        var agent = AgentOptions();
        return new CommandExecutionWorker(
            _store,
            new FakeOnec(),
            new FakeOnecHealthClient(),
            new DynamicConfigurationState(Options.Create(new CommandOptions()), Options.Create(new EtlOptions())),
            state,
            new LocalEtlPauseController(_store, state),
            new DiagnosticsCollector(_store, agent, Options.Create(new ErpOptions { RequireClientCertificate = false }), Options.Create(new OnecOptions())),
            Options.Create(new CommandOptions { MaxConcurrency = capacity, MaxOperationalAttempts = 12, SupportedTypes = ["synthetic"] }),
            NullLogger<CommandExecutionWorker>.Instance,
            signals);
    }

    private static CommandEnvelope MakeEnvelope(DateTimeOffset? notBefore = null)
    {
        using var document = JsonDocument.Parse("{\"amount\":10}");
        var payload = document.RootElement.Clone();
        return new(Guid.NewGuid(), "synthetic", 1, 100, null, null, DateTimeOffset.UtcNow, notBefore, null, null, PayloadHasher.Compute(payload), payload);
    }

    private static IOptions<AgentOptions> AgentOptions() => Options.Create(new AgentOptions
    {
        AgentId = $"l2-{Guid.NewGuid():N}",
        SiteId = "l2-site",
        DataDirectory = Path.GetTempPath(),
        MaxClockDriftSeconds = 30
    });

    private static async Task StopAsync(BackgroundService worker)
    {
        using var stop = new CancellationTokenSource(BoundedWait);
        await worker.StopAsync(stop.Token);
    }

    /// <summary>ERP's lease rule (to-onec/0045): at executing &gt;= capacity no command, the lease is held for the full poll.</summary>
    private sealed class GatingErp(IEnumerable<CommandEnvelope> queue) : IErpClient
    {
        private readonly object _gate = new();
        private readonly Queue<CommandEnvelope> _queue = new(queue);
        private readonly List<LeaseLoad> _loads = [];
        private readonly Dictionary<Guid, TaskCompletionSource> _expected = [];

        public int LeaseCalls { get { lock (_gate) return _loads.Count; } }
        public IReadOnlyList<LeaseLoad> Loads { get { lock (_gate) return [.. _loads]; } }

        public Task ExpectResult(Guid commandId)
        {
            var completion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            lock (_gate) _expected[commandId] = completion;
            return completion.Task;
        }

        public async Task WaitForCallsAsync(int count)
        {
            var deadline = DateTime.UtcNow + BoundedWait;
            while (LeaseCalls < count && DateTime.UtcNow < deadline) await Task.Delay(20);
        }

        public async Task<LeaseResponse> LeaseCommandAsync(LeaseRequest request, CancellationToken cancellationToken)
        {
            CommandEnvelope? next = null;
            lock (_gate)
            {
                _loads.Add(request.CurrentLoad);
                if (request.CurrentLoad.Executing < request.CurrentLoad.Capacity && _queue.Count > 0) next = _queue.Dequeue();
            }
            if (next is not null)
                return new LeaseResponse(true, Guid.NewGuid(), DateTimeOffset.UtcNow.AddMinutes(5), JsonSerializer.SerializeToElement(next, JsonSerializerOptions.Web));
            await Task.Delay(FullCapacityHold, cancellationToken).ConfigureAwait(false);
            return new LeaseResponse(false, null, null, null);
        }

        public Task<SessionStartResponse> StartSessionAsync(SessionStartRequest request, CancellationToken cancellationToken) =>
            Task.FromResult(new SessionStartResponse(Guid.NewGuid(), DateTimeOffset.UtcNow, true, "1.0.0", 0, false));

        public Task AcknowledgeReceivedAsync(Guid commandId, CommandReceivedRequest request, CancellationToken cancellationToken) => Task.CompletedTask;

        public Task AcknowledgeResultAsync(Guid commandId, string resultJson, CancellationToken cancellationToken)
        {
            lock (_gate)
            {
                if (_expected.Remove(commandId, out var completion)) completion.TrySetResult();
            }
            return Task.CompletedTask;
        }

        public Task<BatchAcknowledgement> UploadBatchAsync(EtlBatch batch, Stream content, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task CompleteEtlRunAsync(Guid runId, object summary, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task SendHeartbeatAsync(HeartbeatRequest request, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task<RemoteConfigurationResponse?> GetConfigurationAsync(long currentVersion, CancellationToken cancellationToken) => throw new NotSupportedException();
    }

    private sealed class FakeOnec : IOnecCommandClient
    {
        public Task<OnecExecutionResult> ExecuteAsync(CommandEnvelope command, CancellationToken cancellationToken) =>
            Task.FromResult(new OnecExecutionResult(OnecExecutionKind.Succeeded, new(command.CommandId, "succeeded", JsonSerializer.SerializeToElement(new { @ref = "l2-ref" }), null, [], 1), 200, null, null));

        public Task<OnecExecutionResult> GetStatusAsync(Guid commandId, CancellationToken cancellationToken) =>
            Task.FromResult(new OnecExecutionResult(OnecExecutionKind.Succeeded, new(commandId, "succeeded", JsonSerializer.SerializeToElement(new { @ref = "l2-ref" }), null, [], 1), 200, null, null));
    }

    private sealed class FakeOnecHealthClient : IOnecHealthClient
    {
        public Task<OnecHealthResponse?> CheckAsync(CancellationToken cancellationToken) => Task.FromResult<OnecHealthResponse?>(null);
    }
}
