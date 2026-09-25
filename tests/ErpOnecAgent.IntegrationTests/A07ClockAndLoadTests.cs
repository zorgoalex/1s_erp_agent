using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Application.Configuration;
using ErpOnecAgent.Contracts.Erp;
using ErpOnecAgent.Domain.Agent;
using ErpOnecAgent.Domain.Etl;
using ErpOnecAgent.Infrastructure.Persistence.Sqlite;
using ErpOnecAgent.Service.Runtime;
using ErpOnecAgent.Service.Workers;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace ErpOnecAgent.IntegrationTests;

/// <summary>
/// A07 time and load: the ERP clock offset is measured at the handshake and used for
/// ERP-defined expiry; drift is reported; the lease request carries the real executing count;
/// uptime is monotonic.
/// </summary>
public sealed class A07ClockAndLoadTests : IAsyncLifetime
{
    private static readonly TimeSpan BoundedWait = TimeSpan.FromSeconds(15);
    private readonly SqliteTestDatabase _database = new();
    private SqliteAgentStore _store = null!;

    public async Task InitializeAsync()
    {
        var factory = _database.CreateFactory(Path.Combine("data", "agent.db"));
        _store = new SqliteAgentStore(factory, new SqliteMigrator(factory));
        await _store.InitializeAsync(CancellationToken.None);
    }

    public async Task DisposeAsync() => await _database.DisposeAsync();

    [Fact]
    public void Expiry_uses_the_local_clock_until_the_erp_clock_is_known()
    {
        var state = new AgentRuntimeState();
        var now = DateTimeOffset.UtcNow;

        Assert.Null(state.ErpClockOffset);
        Assert.Equal(now, state.ExpiryNow(now));
    }

    [Fact]
    public void An_erp_clock_ahead_of_the_local_clock_moves_expiry_forward_by_offset_plus_uncertainty()
    {
        var state = new AgentRuntimeState();
        var sent = DateTimeOffset.UtcNow;
        // Round trip 400 ms: the server stamped its time about 200 ms after the send.
        state.RecordErpClock(sent + TimeSpan.FromMinutes(5) + TimeSpan.FromMilliseconds(200), sent, TimeSpan.FromMilliseconds(400));

        AssertNear(TimeSpan.FromMinutes(5), state.ErpClockOffset!.Value);
        Assert.Equal(TimeSpan.FromMilliseconds(200), state.ErpClockUncertainty);
        var now = DateTimeOffset.UtcNow;
        AssertNear(TimeSpan.FromMinutes(5) + TimeSpan.FromMilliseconds(200), state.ExpiryNow(now) - now);
    }

    [Fact]
    public void An_erp_clock_behind_the_local_clock_never_moves_expiry_back()
    {
        var state = new AgentRuntimeState();
        var sent = DateTimeOffset.UtcNow;
        state.RecordErpClock(sent - TimeSpan.FromMinutes(5), sent, TimeSpan.Zero);

        AssertNear(-TimeSpan.FromMinutes(5), state.ErpClockOffset!.Value);
        var now = DateTimeOffset.UtcNow;
        Assert.Equal(now, state.ExpiryNow(now));
    }

    [Fact]
    public void A_slow_or_implausible_sample_is_ignored_and_the_previous_estimate_kept()
    {
        var state = new AgentRuntimeState();
        var sent = DateTimeOffset.UtcNow;
        Assert.True(state.RecordErpClock(sent + TimeSpan.FromSeconds(3), sent, TimeSpan.FromMilliseconds(100)));

        Assert.False(state.RecordErpClock(sent + TimeSpan.FromMinutes(9), sent, TimeSpan.FromSeconds(5)));
        Assert.False(state.RecordErpClock(sent + TimeSpan.FromDays(400), sent, TimeSpan.Zero));

        AssertNear(TimeSpan.FromSeconds(3) - TimeSpan.FromMilliseconds(50), state.ErpClockOffset!.Value);
        Assert.Equal(TimeSpan.FromMilliseconds(50), state.ErpClockUncertainty);
    }

    [Fact]
    public void Drift_is_judged_net_of_the_uncertainty()
    {
        var state = new AgentRuntimeState();
        var sent = DateTimeOffset.UtcNow;
        // 31 s apart, but the sample is only accurate to 1 s: not provably beyond 30 s.
        state.RecordErpClock(sent + TimeSpan.FromSeconds(31) + TimeSpan.FromSeconds(1), sent, TimeSpan.FromSeconds(2));
        Assert.False(state.ClockDriftExceeds(TimeSpan.FromSeconds(30)));
        state.RecordErpClock(sent + TimeSpan.FromSeconds(40), sent, TimeSpan.Zero);
        Assert.True(state.ClockDriftExceeds(TimeSpan.FromSeconds(30)));
    }

    [Fact]
    public async Task A_handshake_without_server_time_records_nothing()
    {
        var state = ReadyState();
        var erp = new ClockErp { OmitServerTime = true };
        using var sessions = new ErpSessionManager(erp, AgentOptions(), state);

        await sessions.GetSessionAsync(CancellationToken.None);

        Assert.Null(state.ErpClockOffset);
    }

    [Fact]
    public async Task A_delayed_handshake_bounds_the_offset_by_half_the_round_trip()
    {
        var state = ReadyState();
        var erp = new ClockErp { ServerClockAhead = TimeSpan.FromMinutes(1), Delay = TimeSpan.FromMilliseconds(300) };
        using var sessions = new ErpSessionManager(erp, AgentOptions(), state);

        await sessions.GetSessionAsync(CancellationToken.None);

        Assert.InRange(state.ErpClockUncertainty, TimeSpan.FromMilliseconds(140), TimeSpan.FromSeconds(1));
        // The fake stamps its time before the delay: the true offset is 1 min, the estimate
        // lies within the uncertainty of it.
        Assert.InRange(state.ErpClockOffset!.Value, TimeSpan.FromMinutes(1) - state.ErpClockUncertainty - TimeSpan.FromMilliseconds(200), TimeSpan.FromMinutes(1) + TimeSpan.FromMilliseconds(200));
    }

    [Fact]
    public async Task The_handshake_records_the_erp_clock_and_reports_drift()
    {
        var state = ReadyState();
        var erp = new ClockErp { ServerClockAhead = TimeSpan.FromMinutes(10) };
        var logger = new CapturingLogger<ErpSessionManager>();
        using var sessions = new ErpSessionManager(erp, AgentOptions(), state, logger);

        await sessions.GetSessionAsync(CancellationToken.None);

        var offset = Assert.IsType<TimeSpan>(state.ErpClockOffset);
        Assert.InRange(offset, TimeSpan.FromMinutes(10) - TimeSpan.FromSeconds(5), TimeSpan.FromMinutes(10) + TimeSpan.FromSeconds(5));
        Assert.Contains(logger.Entries, entry => entry.Level == LogLevel.Warning && entry.Message.Contains("CLOCK_DRIFT", StringComparison.Ordinal));
    }

    [Fact]
    public async Task A_small_offset_is_recorded_without_a_drift_warning()
    {
        var state = ReadyState();
        var erp = new ClockErp { ServerClockAhead = TimeSpan.FromSeconds(2) };
        var logger = new CapturingLogger<ErpSessionManager>();
        using var sessions = new ErpSessionManager(erp, AgentOptions(), state, logger);

        await sessions.GetSessionAsync(CancellationToken.None);

        Assert.NotNull(state.ErpClockOffset);
        Assert.DoesNotContain(logger.Entries, entry => entry.Message.Contains("CLOCK_DRIFT", StringComparison.Ordinal));
    }

    [Fact]
    public void Execution_scopes_count_executing_commands()
    {
        var state = new AgentRuntimeState();

        var first = state.BeginCommandExecution();
        using (state.BeginCommandExecution())
        {
            Assert.Equal(2, state.ExecutingCommands);
        }
        Assert.Equal(1, state.ExecutingCommands);
        first.Dispose();
        first.Dispose();
        Assert.Equal(0, state.ExecutingCommands);
    }

    [Fact]
    public async Task The_lease_request_carries_the_executing_command_count()
    {
        var state = ReadyState();
        var erp = new ClockErp();
        using var sessions = new ErpSessionManager(erp, AgentOptions(), state);
        using var executing = state.BeginCommandExecution();
        using var worker = new CommandLeaseWorker(
            erp,
            _store,
            sessions,
            state,
            new DynamicConfigurationState(Options.Create(new CommandOptions { SupportedTypes = ["synthetic"] }), Options.Create(new EtlOptions { Entities = [], IntervalMinutes = 60 })),
            Options.Create(new ErpOptions { RequireClientCertificate = false, LongPollSeconds = 1 }),
            Options.Create(new CommandOptions { MaxConcurrency = 4, SupportedTypes = ["synthetic"] }),
            NullLogger<CommandLeaseWorker>.Instance);

        await worker.StartAsync(CancellationToken.None);
        try
        {
            var request = await erp.FirstLease.Task.WaitAsync(BoundedWait);
            Assert.Equal(1, request.CurrentLoad.Executing);
            Assert.Equal(4, request.CurrentLoad.Capacity);
        }
        finally
        {
            using var stop = new CancellationTokenSource(BoundedWait);
            await worker.StopAsync(stop.Token);
        }
    }

    [Fact]
    public void Clock_drift_beyond_the_threshold_degrades_health()
    {
        var state = ReadyState();
        state.OnecCommandApiAvailable = true;
        state.OnecODataAvailable = true;
        var metrics = new AgentMetrics(long.MaxValue, 0, 0, 0, 0);
        var storage = new StorageOptions();

        Assert.Equal("healthy", HeartbeatWorker.GetHealthState(state, metrics, storage, TimeSpan.FromSeconds(30)));
        var sent = DateTimeOffset.UtcNow;
        state.RecordErpClock(sent + TimeSpan.FromSeconds(10), sent, TimeSpan.Zero);
        Assert.Equal("healthy", HeartbeatWorker.GetHealthState(state, metrics, storage, TimeSpan.FromSeconds(30)));
        state.RecordErpClock(sent - TimeSpan.FromMinutes(2), sent, TimeSpan.Zero);
        Assert.Equal("degraded", HeartbeatWorker.GetHealthState(state, metrics, storage, TimeSpan.FromSeconds(30)));
    }

    [Fact]
    public void Uptime_is_monotonic()
    {
        var state = new AgentRuntimeState();
        var first = state.Uptime;
        Thread.Sleep(20);
        Assert.True(state.Uptime > first);
    }

    private static void AssertNear(TimeSpan expected, TimeSpan actual) =>
        Assert.InRange(actual, expected - TimeSpan.FromSeconds(1), expected + TimeSpan.FromSeconds(1));

    private static AgentRuntimeState ReadyState()
    {
        var state = new AgentRuntimeState();
        state.SetRemoteMode(AgentMode.Normal);
        state.CompleteBootstrap();
        return state;
    }

    private static IOptions<AgentOptions> AgentOptions() => Options.Create(new AgentOptions
    {
        AgentId = $"a07-clock-{Guid.NewGuid():N}",
        SiteId = "a07-site",
        DataDirectory = Path.GetTempPath(),
        MaxClockDriftSeconds = 30
    });

    private sealed class ClockErp : IErpClient
    {
        public TimeSpan ServerClockAhead { get; init; }
        public bool OmitServerTime { get; init; }
        public TimeSpan Delay { get; init; }
        public TaskCompletionSource<LeaseRequest> FirstLease { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public async Task<SessionStartResponse> StartSessionAsync(SessionStartRequest request, CancellationToken cancellationToken)
        {
            var stamp = OmitServerTime ? default : DateTimeOffset.UtcNow + ServerClockAhead;
            if (Delay > TimeSpan.Zero) await Task.Delay(Delay, cancellationToken);
            return new SessionStartResponse(Guid.NewGuid(), stamp, true, "1.0.0", 0, false);
        }

        public Task<LeaseResponse> LeaseCommandAsync(LeaseRequest request, CancellationToken cancellationToken)
        {
            FirstLease.TrySetResult(request);
            return Task.FromResult(new LeaseResponse(false, null, null, null));
        }

        public Task AcknowledgeReceivedAsync(Guid commandId, CommandReceivedRequest request, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task AcknowledgeResultAsync(Guid commandId, string resultJson, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task<BatchAcknowledgement> UploadBatchAsync(EtlBatch batch, Stream content, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task CompleteEtlRunAsync(Guid runId, object summary, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task SendHeartbeatAsync(HeartbeatRequest request, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task<RemoteConfigurationResponse?> GetConfigurationAsync(long currentVersion, CancellationToken cancellationToken) => Task.FromResult<RemoteConfigurationResponse?>(null);
    }

    private sealed class CapturingLogger<T> : ILogger<T>
    {
        private readonly List<(LogLevel Level, string Message)> _entries = [];
        public IReadOnlyList<(LogLevel Level, string Message)> Entries { get { lock (_entries) return [.. _entries]; } }
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            lock (_entries) _entries.Add((logLevel, formatter(state, exception)));
        }
    }
}
