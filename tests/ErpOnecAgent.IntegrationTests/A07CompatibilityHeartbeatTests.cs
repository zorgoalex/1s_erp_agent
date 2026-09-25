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
/// A07b B6 compatibility/heartbeat slice (GREEN after the fix): an explicit ERP version rejection
/// (accepted:false or a parsed minimumAgentVersion above the running binary) latches
/// <c>CompatibilityRejected</c> under the runtime mode lock; the latch denies new-work admission and
/// extraction but never gates already-sent resolution, result delivery, or upload/completion. The
/// heartbeat stays session-independent and reports <c>incompatible_version</c> while latched.
/// </summary>
public sealed class A07CompatibilityHeartbeatTests : IAsyncLifetime
{
    private static readonly TimeSpan BoundedWait = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan StopTimeout = TimeSpan.FromSeconds(5);
    private readonly SqliteTestDatabase _database = new();
    private SqliteConnectionFactory _factory = null!;
    private SqliteAgentStore _store = null!;

    public async Task InitializeAsync()
    {
        _factory = _database.CreateFactory(Path.Combine("data", "a07-compat.db"));
        _store = new(_factory, new SqliteMigrator(_factory));
        await _store.InitializeAsync(CancellationToken.None);
    }

    public async Task DisposeAsync() => await _database.DisposeAsync();

    // ---- Explicit rejection latches; admission denied; resolution/delivery/upload stay open ----

    [Fact]
    public async Task Explicit_rejection_latches_compatibility_and_denies_admission_only()
    {
        var state = ReadyState(AgentMode.Normal);
        var erp = new FakeErp { SessionResponses = [Rejected()] };
        using var sessions = new ErpSessionManager(erp, AgentOptions("latch"), state);

        await Assert.ThrowsAsync<InvalidOperationException>(() => sessions.GetSessionAsync(CancellationToken.None));

        var snapshot = state.Snapshot;
        Assert.True(snapshot.CompatibilityRejected);
        Assert.False(snapshot.CanLeaseCommands);
        Assert.False(snapshot.CanExecuteCommands);
        Assert.False(snapshot.CanExtract);
        // Already-sent resolution, result delivery, and upload/completion are NOT gated by the latch.
        Assert.True(snapshot.CanResolveCommandResults);
        Assert.True(snapshot.CanDeliverResults);
        Assert.True(snapshot.CanUploadBatches);
        Assert.True(snapshot.CanCompleteEtlRuns);
        Assert.True(snapshot.IsReady);
    }

    [Fact]
    public async Task Accepted_response_with_minimum_above_current_latches_rejection()
    {
        var state = ReadyState(AgentMode.Normal);
        var erp = new FakeErp { SessionResponses = [_ => Task.FromResult(new SessionStartResponse(Guid.NewGuid(), DateTimeOffset.UtcNow, Accepted: true, "99.0.0", 0, false))] };
        using var sessions = new ErpSessionManager(erp, AgentOptions("min-high"), state);

        await Assert.ThrowsAsync<InvalidOperationException>(() => sessions.GetSessionAsync(CancellationToken.None));

        var snapshot = state.Snapshot;
        Assert.True(snapshot.CompatibilityRejected);
        Assert.False(snapshot.CanLeaseCommands);
        Assert.False(snapshot.CanExecuteCommands);
        Assert.False(snapshot.CanExtract);
        Assert.True(snapshot.CanDeliverResults);
    }

    // ---- Mode-source non-interference ----

    [Fact]
    public async Task Rejection_preserves_local_pause_and_remote_mode()
    {
        var state = ReadyState(AgentMode.PauseCommands, localPaused: true);
        var erp = new FakeErp { SessionResponses = [Rejected()] };
        using var sessions = new ErpSessionManager(erp, AgentOptions("modes"), state);

        await Assert.ThrowsAsync<InvalidOperationException>(() => sessions.GetSessionAsync(CancellationToken.None));

        var snapshot = state.Snapshot;
        Assert.True(snapshot.CompatibilityRejected);
        Assert.Equal(AgentMode.PauseCommands, snapshot.RemoteMode);
        Assert.True(snapshot.LocalEtlPaused);
    }

    [Fact]
    public async Task Compatible_handshake_clears_only_rejection_and_applies_its_maintenance_flag()
    {
        var state = ReadyState(AgentMode.PauseCommands, localPaused: true);
        var erp = new FakeErp
        {
            SessionResponses =
            [
                Rejected(),
                _ => Task.FromResult(new SessionStartResponse(Guid.NewGuid(), DateTimeOffset.UtcNow, Accepted: true, "1.0.0", 0, MaintenanceMode: true)),
            ]
        };
        using var sessions = new ErpSessionManager(erp, AgentOptions("recover"), state);

        await Assert.ThrowsAsync<InvalidOperationException>(() => sessions.GetSessionAsync(CancellationToken.None));
        Assert.True(state.Snapshot.CompatibilityRejected);

        sessions.Invalidate();
        var sessionId = await sessions.GetSessionAsync(CancellationToken.None);
        Assert.NotEqual(Guid.Empty, sessionId);

        var snapshot = state.Snapshot;
        Assert.False(snapshot.CompatibilityRejected);
        // The response's maintenance flag is applied; the local pause and remote mode are untouched.
        Assert.True(snapshot.HandshakeMaintenance);
        Assert.Equal(AgentMode.PauseCommands, snapshot.RemoteMode);
        Assert.True(snapshot.LocalEtlPaused);
        Assert.False(snapshot.CanExecuteCommands);
        Assert.False(snapshot.CanExtract);
    }

    [Fact]
    public async Task Compatible_handshake_without_maintenance_clears_rejection_and_flag()
    {
        var state = ReadyState(AgentMode.Normal);
        state.SetHandshakeMaintenance(true);
        var erp = new FakeErp
        {
            SessionResponses =
            [
                Rejected(),
                _ => Task.FromResult(new SessionStartResponse(Guid.NewGuid(), DateTimeOffset.UtcNow, Accepted: true, "1.0.0", 0, MaintenanceMode: false)),
            ]
        };
        using var sessions = new ErpSessionManager(erp, AgentOptions("recover-normal"), state);

        await Assert.ThrowsAsync<InvalidOperationException>(() => sessions.GetSessionAsync(CancellationToken.None));
        sessions.Invalidate();
        await sessions.GetSessionAsync(CancellationToken.None);

        var snapshot = state.Snapshot;
        Assert.False(snapshot.CompatibilityRejected);
        Assert.False(snapshot.HandshakeMaintenance);
        Assert.True(snapshot.CanLeaseCommands);
        Assert.True(snapshot.CanExecuteCommands);
        Assert.True(snapshot.CanExtract);
    }

    // ---- Transient failures neither invent nor clear the latch; Invalidate clears cache only ----

    [Fact]
    public async Task Transient_handshake_failure_neither_latches_nor_clears_rejection()
    {
        var state = ReadyState(AgentMode.Normal);
        var erp = new FakeErp { SessionResponses = [_ => Task.FromException<SessionStartResponse>(new HttpRequestException("erp down"))] };
        using var sessions = new ErpSessionManager(erp, AgentOptions("transient"), state);

        await Assert.ThrowsAsync<HttpRequestException>(() => sessions.GetSessionAsync(CancellationToken.None));
        Assert.False(state.Snapshot.CompatibilityRejected);

        var erp2 = new FakeErp
        {
            SessionResponses =
            [
                Rejected(),
                _ => Task.FromException<SessionStartResponse>(new HttpRequestException("erp down")),
            ]
        };
        using var sessions2 = new ErpSessionManager(erp2, AgentOptions("transient-2"), state);
        await Assert.ThrowsAsync<InvalidOperationException>(() => sessions2.GetSessionAsync(CancellationToken.None));
        Assert.True(state.Snapshot.CompatibilityRejected);

        sessions2.Invalidate();
        await Assert.ThrowsAsync<HttpRequestException>(() => sessions2.GetSessionAsync(CancellationToken.None));
        Assert.True(state.Snapshot.CompatibilityRejected);
    }

    [Fact]
    public async Task Invalidate_clears_only_the_session_cache_not_the_rejection()
    {
        var state = ReadyState(AgentMode.Normal);
        var erp = new FakeErp
        {
            SessionResponses =
            [
                _ => Task.FromResult(new SessionStartResponse(Guid.NewGuid(), DateTimeOffset.UtcNow, Accepted: true, "1.0.0", 0, false)),
                Rejected(),
            ]
        };
        using var sessions = new ErpSessionManager(erp, AgentOptions("invalidate"), state);

        var first = await sessions.GetSessionAsync(CancellationToken.None);
        Assert.NotEqual(Guid.Empty, first);
        sessions.Invalidate();
        await Assert.ThrowsAsync<InvalidOperationException>(() => sessions.GetSessionAsync(CancellationToken.None));
        Assert.True(state.Snapshot.CompatibilityRejected);

        sessions.Invalidate();
        Assert.True(state.Snapshot.CompatibilityRejected);
        Assert.Equal(2, erp.StartSessionCalls);
    }

    [Fact]
    public async Task Empty_session_id_is_protocol_invalid_not_a_version_rejection()
    {
        var state = ReadyState(AgentMode.Normal);
        var validSession = Guid.NewGuid();
        var erp = new FakeErp
        {
            SessionResponses =
            [
                _ => Task.FromResult(new SessionStartResponse(Guid.Empty, DateTimeOffset.UtcNow, Accepted: true, "1.0.0", 0, false)),
                _ => Task.FromResult(new SessionStartResponse(validSession, DateTimeOffset.UtcNow, Accepted: true, "1.0.0", 0, false)),
            ]
        };
        using var sessions = new ErpSessionManager(erp, AgentOptions("empty-session"), state);

        await Assert.ThrowsAsync<InvalidDataException>(() => sessions.GetSessionAsync(CancellationToken.None));
        Assert.False(state.Snapshot.CompatibilityRejected);

        var sessionId = await sessions.GetSessionAsync(CancellationToken.None);
        Assert.Equal(validSession, sessionId);
        Assert.Equal(2, erp.StartSessionCalls);
    }

    // ---- Heartbeat: session-independent, incompatible_version priority over maintenance ----

    [Fact]
    public async Task Heartbeat_reports_incompatible_version_above_maintenance_state()
    {
        var state = ReadyState(AgentMode.Maintenance);
        var erp = new FakeErp { SessionResponses = [Rejected()] };
        using var sessions = new ErpSessionManager(erp, AgentOptions("hb-state"), state);
        await Assert.ThrowsAsync<InvalidOperationException>(() => sessions.GetSessionAsync(CancellationToken.None));
        using var worker = CreateHeartbeatWorker(erp, sessions, state);

        try
        {
            await worker.StartAsync(CancellationToken.None);
            var observed = await Task.WhenAny(erp.HeartbeatObserved.Task, Task.Delay(BoundedWait)) == erp.HeartbeatObserved.Task;

            Assert.True(observed, "Heartbeat was not delivered while compatibility rejection is latched.");
            Assert.NotNull(erp.LastHeartbeat);
            Assert.Equal("incompatible_version", erp.LastHeartbeat!.State);
        }
        finally
        {
            await StopWorkerAsync(worker);
        }
    }

    [Fact]
    public async Task Heartbeat_never_calls_or_waits_on_the_session_handshake()
    {
        var state = ReadyState(AgentMode.Normal);
        state.OnecODataAvailable = true;
        state.OnecCommandApiAvailable = true;
        var erp = new FakeErp { SessionResponses = [HangForever] };
        using var sessions = new ErpSessionManager(erp, AgentOptions("hb-hang"), state);
        using var worker = CreateHeartbeatWorker(erp, sessions, state);

        try
        {
            await worker.StartAsync(CancellationToken.None);
            var observed = await Task.WhenAny(erp.HeartbeatObserved.Task, Task.Delay(BoundedWait)) == erp.HeartbeatObserved.Task;

            Assert.True(observed, "Heartbeat was not delivered while the session endpoint hangs.");
            Assert.Equal(0, erp.StartSessionCalls);
            Assert.NotNull(erp.LastHeartbeat);
            Assert.Equal("healthy", erp.LastHeartbeat!.State);
        }
        finally
        {
            await StopWorkerAsync(worker);
        }
    }

    // ---- Recovery path: blocked lease never consumes the handshake; accepted response recovers ----

    [Fact]
    public async Task Blocked_lease_does_not_starve_handshake_retry_and_recovers_after_accept()
    {
        var state = ReadyState(AgentMode.Normal);
        var erp = new FakeErp
        {
            SessionResponses =
            [
                Rejected(),
                _ => Task.FromResult(new SessionStartResponse(Guid.NewGuid(), DateTimeOffset.UtcNow, Accepted: true, "1.0.0", 0, false)),
            ]
        };
        using var sessions = new ErpSessionManager(erp, AgentOptions("lease-recover"), state);
        await Assert.ThrowsAsync<InvalidOperationException>(() => sessions.GetSessionAsync(CancellationToken.None));
        using var worker = CreateLeaseWorker(erp, sessions, state);

        try
        {
            await worker.StartAsync(CancellationToken.None);
            // While latched, CanLeaseCommands is false: the worker loop cannot reach the handshake
            // at all, so the config path's retry is the recovery driver — no deadlock.
            Assert.False(state.Snapshot.CanLeaseCommands);

            sessions.Invalidate();
            var sessionId = await sessions.GetSessionAsync(CancellationToken.None);
            Assert.NotEqual(Guid.Empty, sessionId);
            Assert.False(state.Snapshot.CompatibilityRejected);

            var leased = await Task.WhenAny(erp.LeaseObserved.Task, Task.Delay(BoundedWait)) == erp.LeaseObserved.Task;
            Assert.True(leased, "Lease worker did not resume after the compatible handshake cleared the latch.");
            Assert.Equal(2, erp.StartSessionCalls);
        }
        finally
        {
            await StopWorkerAsync(worker);
        }
    }

    // ---- Real command worker under the latch: sent work resolves, fresh POST denied ----

    [Fact]
    public async Task Sent_business_command_resolves_under_latch_while_fresh_post_stays_denied()
    {
        var state = ReadyState(AgentMode.Normal);
        var sessionErp = new FakeErp { SessionResponses = [Rejected()] };
        using var sessions = new ErpSessionManager(sessionErp, AgentOptions("exec-latch"), state);
        await Assert.ThrowsAsync<InvalidOperationException>(() => sessions.GetSessionAsync(CancellationToken.None));
        var onec = new FakeOnec();
        var sent = MakeBusinessCommand();
        await SeedSentUnknownAsync(sent);
        var fresh = MakeBusinessCommand();
        await _store.StoreCommandAsync(fresh, DateTimeOffset.UtcNow, CancellationToken.None);
        var counter = new A07CommandBoundaryTests.QueryCounter();
        var store = A07CommandBoundaryTests.QueryCountingStoreProxy.Create(_store, counter);
        using var worker = CreateExecutionWorker(store, state, onec);

        try
        {
            await worker.StartAsync(CancellationToken.None);
            var observed = await Task.WhenAny(onec.LookupObserved.Task, Task.Delay(BoundedWait)) == onec.LookupObserved.Task;
            Assert.True(observed, "Sent business command was not status-resolved under the compatibility latch.");
            var polled = await Task.WhenAny(counter.SecondSentQuery, Task.Delay(BoundedWait)) == counter.SecondSentQuery;
            Assert.True(polled, "Worker did not keep polling the sent-only query under the latch.");

            Assert.Equal(0, counter.Full);
            Assert.Equal(0, onec.ExecuteCalls);
            Assert.Equal(1, onec.StatusCalls);
            Assert.Equal("queued", await StatusStringAsync(fresh.CommandId));
        }
        finally
        {
            await StopWorkerAsync(worker);
        }
    }

    // ---- Saved result delivery continues under the latch (real worker) ----

    [Fact]
    public async Task Saved_result_delivery_continues_under_rejection()
    {
        var state = ReadyState(AgentMode.Normal);
        var sessionErp = new FakeErp { SessionResponses = [Rejected()] };
        using var sessions = new ErpSessionManager(sessionErp, AgentOptions("deliver-latch"), state);
        await Assert.ThrowsAsync<InvalidOperationException>(() => sessions.GetSessionAsync(CancellationToken.None));
        var erp = new FakeErp();
        var command = MakeBusinessCommand();
        await _store.StoreCommandAsync(command, DateTimeOffset.UtcNow, CancellationToken.None);
        var resultJson = JsonSerializer.Serialize(new { commandId = command.CommandId, status = "succeeded", completedAtUtc = DateTimeOffset.UtcNow, data = new { }, warnings = Array.Empty<string>(), resultVersion = 1 });
        Assert.True(await _store.CompleteLocallyAsync(command.CommandId, CommandStatus.SucceededLocal, resultJson, null, null, null, CancellationToken.None));
        Assert.NotEmpty(await _store.GetPendingResultsAsync(10, DateTimeOffset.UtcNow, CancellationToken.None));
        using var worker = new ResultDeliveryWorker(_store, erp, state, NullLogger<ResultDeliveryWorker>.Instance);

        try
        {
            await worker.StartAsync(CancellationToken.None);
            var delivered = await Task.WhenAny(erp.ResultAcknowledged.Task, Task.Delay(BoundedWait)) == erp.ResultAcknowledged.Task;

            Assert.True(delivered, "Saved result was not delivered to ERP under the compatibility latch.");
            Assert.Equal(command.CommandId, erp.LastAcknowledgedCommandId);
            // The ERP ACK returns before the worker's store acknowledgement commits — poll for the
            // durable 'acknowledged' transition rather than racing a single read.
            var deadline = DateTimeOffset.UtcNow + BoundedWait;
            while (DateTimeOffset.UtcNow < deadline)
            {
                if (await PendingResultCountAsync(command.CommandId) == 0) return;
                await Task.Delay(25, CancellationToken.None);
            }
            Assert.Fail($"Result for command {command.CommandId} was not acknowledged in the store within {BoundedWait}.");
        }
        finally
        {
            await StopWorkerAsync(worker);
        }
    }

    // ---- helpers ----

    private static Func<CancellationToken, Task<SessionStartResponse>> Rejected() =>
        _ => Task.FromResult(new SessionStartResponse(Guid.NewGuid(), DateTimeOffset.UtcNow, Accepted: false, "99.0.0", 0, false));

    private static async Task<SessionStartResponse> HangForever(CancellationToken cancellationToken)
    {
        await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken).ConfigureAwait(false);
        throw new System.Diagnostics.UnreachableException();
    }

    private static AgentRuntimeState ReadyState(AgentMode mode, bool localPaused = false)
    {
        var state = new AgentRuntimeState();
        state.RestoreLocalEtlPause(localPaused);
        state.SetRemoteMode(mode);
        state.CompleteBootstrap();
        return state;
    }

    private HeartbeatWorker CreateHeartbeatWorker(FakeErp erp, ErpSessionManager sessions, AgentRuntimeState state)
    {
        var agent = AgentOptions("heartbeat");
        return new HeartbeatWorker(
            erp,
            _store,
            sessions,
            state,
            new AgentMetricsCollector(new FakeSpoolStore(), agent),
            agent,
            Options.Create(new ErpOptions { RequireClientCertificate = false }),
            Options.Create(new StorageOptions()),
            NullLogger<HeartbeatWorker>.Instance);
    }

    private CommandLeaseWorker CreateLeaseWorker(FakeErp erp, ErpSessionManager sessions, AgentRuntimeState state)
    {
        var configuration = new DynamicConfigurationState(
            Options.Create(new CommandOptions { SupportedTypes = ["synthetic"] }),
            Options.Create(new EtlOptions { Entities = [], IntervalMinutes = 60 }));
        return new CommandLeaseWorker(
            erp,
            _store,
            sessions,
            state,
            configuration,
            Options.Create(new ErpOptions { RequireClientCertificate = false, LongPollSeconds = 1 }),
            Options.Create(new CommandOptions { MaxConcurrency = 4, SupportedTypes = ["synthetic"] }),
            NullLogger<CommandLeaseWorker>.Instance);
    }

    private static CommandExecutionWorker CreateExecutionWorker(IAgentStore store, AgentRuntimeState state, FakeOnec onec)
    {
        var agent = AgentOptions("exec");
        var diagnostics = new DiagnosticsCollector(
            store,
            agent,
            Options.Create(new ErpOptions { RequireClientCertificate = false }),
            Options.Create(new OnecOptions()));
        return new CommandExecutionWorker(
            store,
            onec,
            new FakeOnecHealthClient(),
            new DynamicConfigurationState(Options.Create(new CommandOptions()), Options.Create(new EtlOptions())),
            state,
            new LocalEtlPauseController(store, state),
            diagnostics,
            Options.Create(new CommandOptions { MaxConcurrency = 4, MaxOperationalAttempts = 12, SupportedTypes = ["synthetic"] }),
            NullLogger<CommandExecutionWorker>.Instance);
    }

    private async Task SeedSentUnknownAsync(CommandEnvelope command)
    {
        await _store.StoreCommandAsync(command, DateTimeOffset.UtcNow, CancellationToken.None);
        var postAttempt = await _store.ClaimPostAttemptAsync(command.CommandId, CancellationToken.None);
        Assert.NotNull(postAttempt);
        Assert.True(await _store.MarkUnknownResultAsync(command.CommandId, postAttempt.AttemptId, "UNKNOWN_RESULT", "1C execution result is unknown.", DateTimeOffset.UtcNow.AddSeconds(-5), CancellationToken.None));
    }

    private async Task<long> PendingResultCountAsync(Guid commandId)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT COUNT(*) FROM results_outbox WHERE command_id=$id AND status <> 'acknowledged'";
        command.Parameters.AddWithValue("$id", commandId.ToString("D"));
        return Convert.ToInt64(await command.ExecuteScalarAsync(CancellationToken.None), System.Globalization.CultureInfo.InvariantCulture);
    }

    private async Task<string?> StatusStringAsync(Guid commandId)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT status FROM commands_inbox WHERE command_id=$id";
        command.Parameters.AddWithValue("$id", commandId.ToString("D"));
        return await command.ExecuteScalarAsync(CancellationToken.None) as string;
    }

    private static CommandEnvelope MakeBusinessCommand()
    {
        using var document = JsonDocument.Parse("{\"amount\":10}");
        var payload = document.RootElement.Clone();
        return new(Guid.NewGuid(), "create_customer_order", 1, 100, $"order:{Guid.NewGuid():N}", null, DateTimeOffset.UtcNow, null, null, null, PayloadHasher.Compute(payload), payload);
    }

    private static IOptions<AgentOptions> AgentOptions(string suffix) => Options.Create(new AgentOptions
    {
        AgentId = $"a07-compat-{suffix}-{Guid.NewGuid():N}",
        SiteId = "a07-site",
        DataDirectory = Path.GetTempPath(),
        HeartbeatIntervalSeconds = 10
    });

    private static async Task StopWorkerAsync(BackgroundService worker)
    {
        using var stop = new CancellationTokenSource(StopTimeout);
        await worker.StopAsync(stop.Token);
    }

    private sealed class FakeErp : IErpClient
    {
        private int _sessionIndex;
        private int _startSessionCalls;

        public Func<CancellationToken, Task<SessionStartResponse>>[] SessionResponses { get; init; } = [];
        public TaskCompletionSource<bool> HeartbeatObserved { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource<bool> LeaseObserved { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource<bool> ResultAcknowledged { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public HeartbeatRequest? LastHeartbeat { get; private set; }
        public Guid LastAcknowledgedCommandId { get; private set; }
        public int StartSessionCalls => Volatile.Read(ref _startSessionCalls);

        public Task<SessionStartResponse> StartSessionAsync(SessionStartRequest request, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _startSessionCalls);
            if (SessionResponses.Length == 0) return Task.FromResult(new SessionStartResponse(Guid.NewGuid(), DateTimeOffset.UtcNow, true, "1.0.0", 0, false));
            var responder = SessionResponses[Math.Min(_sessionIndex++, SessionResponses.Length - 1)];
            return responder(cancellationToken);
        }

        public Task<LeaseResponse> LeaseCommandAsync(LeaseRequest request, CancellationToken cancellationToken)
        {
            LeaseObserved.TrySetResult(true);
            return Task.FromResult(new LeaseResponse(false, null, null, null));
        }

        public Task SendHeartbeatAsync(HeartbeatRequest request, CancellationToken cancellationToken)
        {
            LastHeartbeat = request;
            HeartbeatObserved.TrySetResult(true);
            return Task.CompletedTask;
        }

        public Task AcknowledgeResultAsync(Guid commandId, string resultJson, CancellationToken cancellationToken)
        {
            LastAcknowledgedCommandId = commandId;
            ResultAcknowledged.TrySetResult(true);
            return Task.CompletedTask;
        }

        public Task AcknowledgeReceivedAsync(Guid commandId, CommandReceivedRequest request, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task<BatchAcknowledgement> UploadBatchAsync(EtlBatch batch, Stream content, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task CompleteEtlRunAsync(Guid runId, object summary, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task<RemoteConfigurationResponse?> GetConfigurationAsync(long currentVersion, CancellationToken cancellationToken) => Task.FromResult<RemoteConfigurationResponse?>(null);
    }

    private sealed class FakeOnec : IOnecCommandClient
    {
        private int _executeCalls;
        private int _statusCalls;

        public TaskCompletionSource<bool> LookupObserved { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public int ExecuteCalls => Volatile.Read(ref _executeCalls);
        public int StatusCalls => Volatile.Read(ref _statusCalls);

        public Task<OnecExecutionResult> ExecuteAsync(CommandEnvelope command, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _executeCalls);
            return Task.FromResult(new OnecExecutionResult(OnecExecutionKind.Succeeded, new(command.CommandId, "succeeded", JsonSerializer.SerializeToElement(new { @ref = "synthetic-ref" }), null, [], 1), 200, null, null));
        }

        public Task<OnecExecutionResult> GetStatusAsync(Guid commandId, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _statusCalls);
            LookupObserved.TrySetResult(true);
            return Task.FromResult(new OnecExecutionResult(OnecExecutionKind.Succeeded, new(commandId, "succeeded", JsonSerializer.SerializeToElement(new { @ref = "synthetic-ref" }), null, [], 1), 200, null, null));
        }
    }

    private sealed class FakeOnecHealthClient : IOnecHealthClient
    {
        public Task<OnecHealthResponse?> CheckAsync(CancellationToken cancellationToken) => Task.FromResult<OnecHealthResponse?>(null);
    }

    private sealed class FakeSpoolStore : ISpoolStore
    {
        public Task<EtlBatch> WriteBatchAsync(Guid runId, EtlEntityDefinition entity, IReadOnlyList<JsonElement> rows, EtlCursor? watermarkFrom, EtlCursor? watermarkTo, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task<Stream> OpenReadAsync(EtlBatch batch, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task<long> GetSizeAsync(CancellationToken cancellationToken) => Task.FromResult(0L);
        public Task QuarantineTemporaryFilesAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Task DeleteAcknowledgedAsync(EtlBatch batch, CancellationToken cancellationToken) => throw new NotSupportedException();
    }
}
