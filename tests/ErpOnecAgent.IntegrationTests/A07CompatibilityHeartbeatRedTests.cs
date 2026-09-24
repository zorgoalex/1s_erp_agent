using System.Diagnostics;
using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Application.Configuration;
using ErpOnecAgent.Contracts.Erp;
using ErpOnecAgent.Domain.Etl;
using ErpOnecAgent.Infrastructure.Persistence.Sqlite;
using ErpOnecAgent.Service.Runtime;
using ErpOnecAgent.Service.Workers;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace ErpOnecAgent.IntegrationTests;

/// <summary>
/// A07b B6 runtime reproductions of the missing compatibility/heartbeat behavior (baseline RED):
/// (1) an explicit ERP handshake rejection must not suppress the heartbeat — TZ §30.2 requires the
///     agent to keep sending heartbeat and to surface <c>incompatible_version</c>; the baseline
///     awaits <c>ErpSessionManager.GetSessionAsync</c> BEFORE sending, so the rejection throw makes
///     the heartbeat disappear entirely;
/// (2) the heartbeat request carries agentId and no sessionId, so a hung/failed handshake must
///     never be a heartbeat prerequisite — the baseline waits on the hung handshake forever.
/// Both tests must FAIL on the unchanged baseline and pass only after the bounded B6 slice lands.
/// </summary>
public sealed class A07CompatibilityHeartbeatRedTests : IAsyncLifetime
{
    private static readonly TimeSpan BoundedWait = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan StopTimeout = TimeSpan.FromSeconds(5);
    private readonly SqliteTestDatabase _database = new();
    private SqliteAgentStore _store = null!;

    public async Task InitializeAsync()
    {
        var factory = _database.CreateFactory(Path.Combine("data", "a07-compat-red.db"));
        _store = new(factory, new SqliteMigrator(factory));
        await _store.InitializeAsync(CancellationToken.None);
    }

    public async Task DisposeAsync() => await _database.DisposeAsync();

    [Fact]
    public async Task Explicit_handshake_rejection_still_delivers_heartbeat_with_incompatible_version()
    {
        var state = ReadyState();
        var erp = new FakeErp
        {
            SessionResponses = [_ => Task.FromResult(new SessionStartResponse(Guid.NewGuid(), DateTimeOffset.UtcNow, Accepted: false, "99.0.0", 0, false))]
        };
        using var sessions = new ErpSessionManager(erp, AgentOptions("red-reject"), state);
        // The rejection arrives through the session path (lease/config drive the handshake). On the
        // baseline this throw is also what suppresses the heartbeat below; after the fix it latches
        // CompatibilityRejected while the session-independent heartbeat keeps flowing.
        await Assert.ThrowsAsync<InvalidOperationException>(() => sessions.GetSessionAsync(CancellationToken.None));
        using var worker = CreateWorker(erp, sessions, state);

        try
        {
            await worker.StartAsync(CancellationToken.None);
            var observed = await Task.WhenAny(erp.HeartbeatObserved.Task, Task.Delay(BoundedWait)) == erp.HeartbeatObserved.Task;

            Assert.True(observed, "Explicit handshake rejection suppressed the heartbeat: the worker awaits GetSessionAsync before sending, so an incompatible agent never reports itself (TZ §30.2 requires heartbeat + incompatible_version).");
            Assert.NotNull(erp.LastHeartbeat);
            Assert.Equal("incompatible_version", erp.LastHeartbeat!.State);
        }
        finally
        {
            await StopWorkerAsync(worker);
        }
    }

    [Fact]
    public async Task Heartbeat_does_not_wait_on_a_hung_session_handshake()
    {
        var state = ReadyState();
        var erp = new FakeErp
        {
            SessionResponses = [ct => HangForever(ct)]
        };
        using var sessions = new ErpSessionManager(erp, AgentOptions("red-hang"), state);
        using var worker = CreateWorker(erp, sessions, state);

        try
        {
            await worker.StartAsync(CancellationToken.None);
            var observed = await Task.WhenAny(erp.HeartbeatObserved.Task, Task.Delay(BoundedWait)) == erp.HeartbeatObserved.Task;

            Assert.True(observed, "Heartbeat waited on a hung GetSessionAsync; the request carries only agentId and must stay session-independent.");
            Assert.NotNull(erp.LastHeartbeat);
        }
        finally
        {
            await StopWorkerAsync(worker);
        }
    }

    // ---- Root-review RED: malformed minimumAgentVersion on an accepted response must fail closed ----

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData("garbage")]
    public async Task Malformed_minimum_in_accepted_response_preserves_latch_and_never_caches_session(string? minimum)
    {
        // Review reproduction (fails on the pre-review implementation): after an explicit rejection
        // latches CompatibilityRejected, an ACCEPTED response whose required minimumAgentVersion is
        // null/empty/whitespace/garbage cannot establish compatibility — it must be a protocol error
        // that preserves the latch and caches no session. The pre-review code let TryParse=false
        // fall through to ApplyCompatibleHandshake, silently clearing the latch on an unverifiable
        // response and caching its session as valid.
        var state = ReadyState();
        var validSession = Guid.NewGuid();
        var erp = new FakeErp
        {
            SessionResponses =
            [
                _ => Task.FromResult(new SessionStartResponse(Guid.NewGuid(), DateTimeOffset.UtcNow, Accepted: false, "99.0.0", 0, false)),
                _ => Task.FromResult(new SessionStartResponse(Guid.NewGuid(), DateTimeOffset.UtcNow, Accepted: true, minimum!, 0, false)),
                _ => Task.FromResult(new SessionStartResponse(validSession, DateTimeOffset.UtcNow, Accepted: true, "1.0.0", 0, false)),
            ]
        };
        using var sessions = new ErpSessionManager(erp, AgentOptions("red-malformed"), state);

        await Assert.ThrowsAsync<InvalidOperationException>(() => sessions.GetSessionAsync(CancellationToken.None));
        Assert.True(state.Snapshot.CompatibilityRejected);

        sessions.Invalidate();
        await Assert.ThrowsAsync<InvalidDataException>(() => sessions.GetSessionAsync(CancellationToken.None));
        Assert.True(state.Snapshot.CompatibilityRejected);

        sessions.Invalidate();
        var session = await sessions.GetSessionAsync(CancellationToken.None);
        Assert.Equal(validSession, session);
        Assert.False(state.Snapshot.CompatibilityRejected);
        Assert.Equal(3, erp.StartSessionCalls);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("garbage")]
    public async Task Malformed_minimum_on_first_handshake_is_protocol_error_not_rejection(string? minimum)
    {
        // Unknown-before-first-handshake: a malformed required field is a protocol error — it must
        // fail closed WITHOUT inventing a compatibility rejection.
        var state = ReadyState();
        var erp = new FakeErp { SessionResponses = [_ => Task.FromResult(new SessionStartResponse(Guid.NewGuid(), DateTimeOffset.UtcNow, Accepted: true, minimum!, 0, false))] };
        using var sessions = new ErpSessionManager(erp, AgentOptions("red-malformed-first"), state);

        await Assert.ThrowsAsync<InvalidDataException>(() => sessions.GetSessionAsync(CancellationToken.None));
        Assert.False(state.Snapshot.CompatibilityRejected);
    }

    private static async Task<SessionStartResponse> HangForever(CancellationToken cancellationToken)
    {
        await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken).ConfigureAwait(false);
        throw new UnreachableException();
    }

    private static AgentRuntimeState ReadyState()
    {
        var state = new AgentRuntimeState();
        state.CompleteBootstrap();
        return state;
    }

    private HeartbeatWorker CreateWorker(FakeErp erp, ErpSessionManager sessions, AgentRuntimeState state)
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

    private static IOptions<AgentOptions> AgentOptions(string suffix) => Options.Create(new AgentOptions
    {
        AgentId = $"a07-compat-red-{suffix}-{Guid.NewGuid():N}",
        SiteId = "a07-site",
        DataDirectory = Path.GetTempPath(),
        HeartbeatIntervalSeconds = 10
    });

    private static async Task StopWorkerAsync(HeartbeatWorker worker)
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
        public HeartbeatRequest? LastHeartbeat { get; private set; }
        public int StartSessionCalls => Volatile.Read(ref _startSessionCalls);

        public Task<SessionStartResponse> StartSessionAsync(SessionStartRequest request, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _startSessionCalls);
            var responder = SessionResponses[Math.Min(_sessionIndex++, SessionResponses.Length - 1)];
            return responder(cancellationToken);
        }

        public Task SendHeartbeatAsync(HeartbeatRequest request, CancellationToken cancellationToken)
        {
            LastHeartbeat = request;
            HeartbeatObserved.TrySetResult(true);
            return Task.CompletedTask;
        }

        public Task<LeaseResponse> LeaseCommandAsync(LeaseRequest request, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task AcknowledgeReceivedAsync(Guid commandId, CommandReceivedRequest request, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task AcknowledgeResultAsync(Guid commandId, string resultJson, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task<BatchAcknowledgement> UploadBatchAsync(EtlBatch batch, Stream content, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task CompleteEtlRunAsync(Guid runId, object summary, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task<RemoteConfigurationResponse?> GetConfigurationAsync(long currentVersion, CancellationToken cancellationToken) => Task.FromResult<RemoteConfigurationResponse?>(null);
    }

    private sealed class FakeSpoolStore : ISpoolStore
    {
        public Task<EtlBatch> WriteBatchAsync(Guid runId, EtlEntityDefinition entity, IReadOnlyList<System.Text.Json.JsonElement> rows, EtlCursor? watermarkFrom, EtlCursor? watermarkTo, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task<Stream> OpenReadAsync(EtlBatch batch, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task<long> GetSizeAsync(CancellationToken cancellationToken) => Task.FromResult(0L);
        public Task QuarantineTemporaryFilesAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Task DeleteAcknowledgedAsync(EtlBatch batch, CancellationToken cancellationToken) => throw new NotSupportedException();
    }
}
