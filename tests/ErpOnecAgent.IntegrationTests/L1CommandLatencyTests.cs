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
/// L1: in-process wake-ups replace the command workers' polling pauses (lease → execution →
/// delivery), and an empty long poll is re-issued at once with a minimum cycle against a
/// non-long-polling endpoint.
/// </summary>
public sealed class L1CommandLatencyTests : IAsyncLifetime
{
    private static readonly TimeSpan BoundedWait = TimeSpan.FromSeconds(15);
    private readonly SqliteTestDatabase _database = new();
    private SqliteConnectionFactory _factory = null!;
    private SqliteAgentStore _store = null!;

    public async Task InitializeAsync()
    {
        _factory = _database.CreateFactory(Path.Combine("data", "l1.db"));
        _store = new(_factory, new SqliteMigrator(_factory));
        await _store.InitializeAsync(CancellationToken.None);
    }

    public async Task DisposeAsync() => await _database.DisposeAsync();

    // ---- WorkSignal ----

    [Fact]
    public async Task A_pulse_before_the_wait_is_kept_and_pulses_coalesce()
    {
        var signal = new WorkSignal();
        signal.Pulse();
        signal.Pulse();
        signal.Pulse();

        Assert.True(await signal.WaitAsync(BoundedWait, CancellationToken.None));
        Assert.False(await signal.WaitAsync(TimeSpan.FromMilliseconds(50), CancellationToken.None));
    }

    [Fact]
    public async Task A_pulse_wakes_a_waiter_long_before_its_timeout()
    {
        var signal = new WorkSignal();
        var watch = Stopwatch.StartNew();
        var wait = signal.WaitAsync(BoundedWait, CancellationToken.None);
        await Task.Delay(50);
        signal.Pulse();

        Assert.True(await wait);
        Assert.True(watch.Elapsed < TimeSpan.FromSeconds(5));
    }

    [Fact]
    public async Task Cancellation_ends_the_wait()
    {
        var signal = new WorkSignal();
        using var cancel = new CancellationTokenSource(50);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => signal.WaitAsync(BoundedWait, cancel.Token));
    }

    [Fact]
    public async Task Repeated_pulse_wins_complete_promptly()
    {
        var signal = new WorkSignal();
        var watch = Stopwatch.StartNew();
        for (var i = 0; i < 2000; i++)
        {
            signal.Pulse();
            Assert.True(await signal.WaitAsync(TimeSpan.FromSeconds(30), CancellationToken.None));
        }
        Assert.True(watch.Elapsed < TimeSpan.FromSeconds(10));
    }

    [Fact]
    public void The_host_gives_all_command_workers_one_shared_signal_instance()
    {
        var services = new Microsoft.Extensions.DependencyInjection.ServiceCollection();
        Microsoft.Extensions.DependencyInjection.ServiceCollectionServiceExtensions.AddSingleton<CommandWorkSignals>(services);
        Microsoft.Extensions.DependencyInjection.ServiceCollectionServiceExtensions.AddSingleton<Probe>(services);
        using var provider = Microsoft.Extensions.DependencyInjection.ServiceCollectionContainerBuilderExtensions.BuildServiceProvider(services);

        var probe = Microsoft.Extensions.DependencyInjection.ServiceProviderServiceExtensions.GetRequiredService<Probe>(provider);

        // Same resolution rule the workers rely on: a registered service wins over the default null.
        Assert.Same(Microsoft.Extensions.DependencyInjection.ServiceProviderServiceExtensions.GetRequiredService<CommandWorkSignals>(provider), probe.Signals);
    }

    [Fact]
    public void The_service_host_registers_the_shared_signals()
    {
        // Program.cs is top-level statements with no testable composition root; without this
        // registration every worker would silently fall back to a private instance (polling).
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory is not null && !File.Exists(Path.Combine(directory.FullName, "src", "ErpOnecAgent.Service", "Program.cs"))) directory = directory.Parent;
        Assert.NotNull(directory);
        var program = File.ReadAllText(Path.Combine(directory.FullName, "src", "ErpOnecAgent.Service", "Program.cs"));

        Assert.Contains("AddSingleton<CommandWorkSignals>()", program, StringComparison.Ordinal);
    }

    private sealed class Probe(CommandWorkSignals? signals = null)
    {
        public CommandWorkSignals? Signals { get; } = signals;
    }

    // ---- Lease worker ----

    [Fact]
    public async Task A_stored_command_wakes_execution_and_delivery()
    {
        var signals = new CommandWorkSignals();
        var state = ReadyState();
        var erp = new FakeErp { Command = MakeEnvelope() };
        using var sessions = new ErpSessionManager(erp, AgentOptions(), state);
        using var worker = CreateLeaseWorker(erp, sessions, state, signals);

        await worker.StartAsync(CancellationToken.None);
        try
        {
            await erp.ReceivedAck.Task.WaitAsync(BoundedWait);
            // The pulse follows the intake; without it both waits would run to the timeout.
            Assert.True(await signals.Commands.WaitAsync(TimeSpan.FromSeconds(2), CancellationToken.None));
            Assert.True(await signals.Results.WaitAsync(TimeSpan.FromSeconds(2), CancellationToken.None));
        }
        finally
        {
            await StopAsync(worker);
        }
    }

    [Fact]
    public async Task After_a_held_empty_poll_the_next_lease_goes_out_at_once()
    {
        var state = ReadyState();
        var erp = new FakeErp { EmptyHold = TimeSpan.FromMilliseconds(300) };
        using var sessions = new ErpSessionManager(erp, AgentOptions(), state);
        using var worker = CreateLeaseWorker(erp, sessions, state, new CommandWorkSignals());

        await worker.StartAsync(CancellationToken.None);
        try
        {
            await erp.WaitForCallsAsync(6);
        }
        finally
        {
            await StopAsync(worker);
        }

        var gaps = erp.GapsBetweenReturnAndNextCall();
        Assert.True(gaps.Count >= 4);
        // Old behaviour: a fixed 250 ms pause after every empty answer.
        Assert.True(Median(gaps) < TimeSpan.FromMilliseconds(100), $"Median gap {Median(gaps).TotalMilliseconds} ms");
    }

    [Fact]
    public async Task An_endpoint_answering_empty_at_once_is_not_polled_in_a_busy_loop()
    {
        var state = ReadyState();
        var erp = new FakeErp { EmptyHold = TimeSpan.Zero };
        using var sessions = new ErpSessionManager(erp, AgentOptions(), state);
        using var worker = CreateLeaseWorker(erp, sessions, state, new CommandWorkSignals());

        await worker.StartAsync(CancellationToken.None);
        await Task.Delay(1500);
        await StopAsync(worker);

        // Guards the floor only (the old fixed pause passes too). Minimum cycle 250 ms → about 6 calls in 1.5 s; allow scheduling slack, not a spin.
        Assert.InRange(erp.LeaseCalls, 2, 10);
    }

    // ---- Execution and delivery ----

    [Fact]
    public async Task A_finished_command_wakes_delivery()
    {
        var signals = new CommandWorkSignals();
        var state = ReadyState();
        await _store.StoreCommandAsync(MakeEnvelope(), DateTimeOffset.UtcNow, CancellationToken.None);
        using var worker = CreateExecutionWorker(state, signals);

        await worker.StartAsync(CancellationToken.None);
        try
        {
            Assert.True(await signals.Results.WaitAsync(BoundedWait, CancellationToken.None));
            await WaitUntilAsync(async () => (await _store.GetPendingResultsAsync(10, DateTimeOffset.UtcNow, CancellationToken.None)).Count == 1);
        }
        finally
        {
            await StopAsync(worker);
        }
    }

    [Fact]
    public async Task A_stored_command_is_executed_and_its_result_sent_without_polling_pauses()
    {
        var signals = new CommandWorkSignals();
        var state = ReadyState();
        var erp = new FakeErp();
        using var execution = CreateExecutionWorker(state, signals);
        using var delivery = new ResultDeliveryWorker(_store, erp, state, NullLogger<ResultDeliveryWorker>.Instance, signals);
        await execution.StartAsync(CancellationToken.None);
        await delivery.StartAsync(CancellationToken.None);
        var latencies = new List<TimeSpan>();
        try
        {
            for (var i = 0; i < 5; i++)
            {
                // Let both workers go idle (their first empty query is behind them).
                await Task.Delay(600);
                var command = MakeEnvelope();
                var acknowledged = erp.ExpectResult(command.CommandId);
                var watch = Stopwatch.StartNew();
                await _store.StoreCommandAsync(command, DateTimeOffset.UtcNow, CancellationToken.None);
                signals.Commands.Pulse(); // what the lease worker does after intake
                await acknowledged.WaitAsync(BoundedWait);
                latencies.Add(watch.Elapsed);
            }
        }
        finally
        {
            await StopAsync(delivery);
            await StopAsync(execution);
        }

        // Old behaviour: 0–250 ms execution poll + 0–500 ms delivery poll (median ≈ 290 ms).
        Assert.True(Median(latencies) < TimeSpan.FromMilliseconds(200), $"Median {Median(latencies).TotalMilliseconds} ms");
    }

    // ---- helpers ----

    private static TimeSpan Median(IReadOnlyList<TimeSpan> values)
    {
        var sorted = values.OrderBy(static v => v).ToArray();
        return sorted[sorted.Length / 2];
    }

    private static async Task WaitUntilAsync(Func<Task<bool>> condition)
    {
        var deadline = DateTime.UtcNow + BoundedWait;
        while (DateTime.UtcNow < deadline)
        {
            if (await condition()) return;
            await Task.Delay(25);
        }
        Assert.Fail("Condition not reached in time.");
    }

    private static AgentRuntimeState ReadyState()
    {
        var state = new AgentRuntimeState();
        state.RestoreLocalEtlPause(false);
        state.SetRemoteMode(AgentMode.Normal);
        state.CompleteBootstrap();
        return state;
    }

    private CommandLeaseWorker CreateLeaseWorker(FakeErp erp, ErpSessionManager sessions, AgentRuntimeState state, CommandWorkSignals signals) =>
        new(
            erp,
            _store,
            sessions,
            state,
            new DynamicConfigurationState(Options.Create(new CommandOptions { SupportedTypes = ["synthetic"] }), Options.Create(new EtlOptions { Entities = [], IntervalMinutes = 60 })),
            Options.Create(new ErpOptions { RequireClientCertificate = false, LongPollSeconds = 1 }),
            Options.Create(new CommandOptions { MaxConcurrency = 4, SupportedTypes = ["synthetic"] }),
            NullLogger<CommandLeaseWorker>.Instance,
            signals);

    private CommandExecutionWorker CreateExecutionWorker(AgentRuntimeState state, CommandWorkSignals signals)
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
            Options.Create(new CommandOptions { MaxConcurrency = 4, MaxOperationalAttempts = 12, SupportedTypes = ["synthetic"] }),
            NullLogger<CommandExecutionWorker>.Instance,
            signals);
    }

    private static CommandEnvelope MakeEnvelope()
    {
        using var document = JsonDocument.Parse("{\"amount\":10}");
        var payload = document.RootElement.Clone();
        return new(Guid.NewGuid(), "synthetic", 1, 100, $"order:{Guid.NewGuid():N}", null, DateTimeOffset.UtcNow, null, null, null, PayloadHasher.Compute(payload), payload);
    }

    private static IOptions<AgentOptions> AgentOptions() => Options.Create(new AgentOptions
    {
        AgentId = $"l1-{Guid.NewGuid():N}",
        SiteId = "l1-site",
        DataDirectory = Path.GetTempPath(),
        MaxClockDriftSeconds = 30
    });

    private static async Task StopAsync(BackgroundService worker)
    {
        using var stop = new CancellationTokenSource(BoundedWait);
        await worker.StopAsync(stop.Token);
    }

    private sealed class FakeErp : IErpClient
    {
        private readonly object _gate = new();
        private readonly List<(long Called, long Returned)> _calls = [];
        private readonly Dictionary<Guid, TaskCompletionSource> _expected = [];
        private int _leaseCalls;
        private int _commandHandedOut;

        public CommandEnvelope? Command { get; init; }
        public TimeSpan EmptyHold { get; init; } = TimeSpan.FromMilliseconds(300);
        public TaskCompletionSource ReceivedAck { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public int LeaseCalls => Volatile.Read(ref _leaseCalls);

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

        public List<TimeSpan> GapsBetweenReturnAndNextCall()
        {
            lock (_gate)
            {
                var gaps = new List<TimeSpan>();
                for (var i = 1; i < _calls.Count; i++)
                {
                    if (_calls[i - 1].Returned > 0) gaps.Add(Stopwatch.GetElapsedTime(_calls[i - 1].Returned, _calls[i].Called));
                }
                return gaps;
            }
        }

        public Task<SessionStartResponse> StartSessionAsync(SessionStartRequest request, CancellationToken cancellationToken) =>
            Task.FromResult(new SessionStartResponse(Guid.NewGuid(), DateTimeOffset.UtcNow, true, "1.0.0", 0, false));

        public async Task<LeaseResponse> LeaseCommandAsync(LeaseRequest request, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _leaseCalls);
            int index;
            lock (_gate)
            {
                _calls.Add((Stopwatch.GetTimestamp(), 0));
                index = _calls.Count - 1;
            }
            try
            {
                if (Command is not null && Interlocked.Exchange(ref _commandHandedOut, 1) == 0)
                {
                    return new LeaseResponse(true, Guid.NewGuid(), DateTimeOffset.UtcNow.AddMinutes(5), JsonSerializer.SerializeToElement(Command, JsonSerializerOptions.Web));
                }
                if (EmptyHold > TimeSpan.Zero) await Task.Delay(EmptyHold, cancellationToken).ConfigureAwait(false);
                return new LeaseResponse(false, null, null, null);
            }
            finally
            {
                lock (_gate) _calls[index] = (_calls[index].Called, Stopwatch.GetTimestamp());
            }
        }

        public Task AcknowledgeReceivedAsync(Guid commandId, CommandReceivedRequest request, CancellationToken cancellationToken)
        {
            ReceivedAck.TrySetResult();
            return Task.CompletedTask;
        }

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
            Task.FromResult(new OnecExecutionResult(OnecExecutionKind.Succeeded, new(command.CommandId, "succeeded", JsonSerializer.SerializeToElement(new { @ref = "l1-ref" }), null, [], 1), 200, null, null));

        public Task<OnecExecutionResult> GetStatusAsync(Guid commandId, CancellationToken cancellationToken) =>
            Task.FromResult(new OnecExecutionResult(OnecExecutionKind.Succeeded, new(commandId, "succeeded", JsonSerializer.SerializeToElement(new { @ref = "l1-ref" }), null, [], 1), 200, null, null));
    }

    private sealed class FakeOnecHealthClient : IOnecHealthClient
    {
        public Task<OnecHealthResponse?> CheckAsync(CancellationToken cancellationToken) => Task.FromResult<OnecHealthResponse?>(null);
    }
}
