using System.Text.Json;
using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Application.Configuration;
using ErpOnecAgent.Contracts.Erp;
using ErpOnecAgent.Domain.Agent;
using ErpOnecAgent.Domain.Commands;
using ErpOnecAgent.Domain.Common;
using ErpOnecAgent.Domain.Etl;
using ErpOnecAgent.Infrastructure.Persistence.Sqlite;
using ErpOnecAgent.Service.Runtime;
using ErpOnecAgent.Service.Workers;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace ErpOnecAgent.IntegrationTests;

/// <summary>
/// A07b B1 admission boundary (pinned current behaviour, no production change): the lease
/// admission boundary is the moment the long poll is ISSUED. When the remote mode flips to
/// <c>PauseCommands</c>/<c>Drain</c>/<c>Maintenance</c>/<c>Disabled</c> — or handshake
/// maintenance begins — while a poll is held, a command the ERP returns on that poll is still
/// taken in: persisted durably in <c>commands_inbox</c> as <c>queued</c> and ACKed to the ERP
/// exactly once. It is never executed (no execution worker runs here), and the next loop
/// iteration is stopped by the <c>CanLeaseCommands</c> gate, so no further poll is issued while
/// the restriction holds. This conservative shape cannot lose work. Note that <c>Drain</c> also
/// denies new admission — it only permits already-accepted commands to finish executing.
/// </summary>
public sealed class A07LongPollAdmissionTests : IAsyncLifetime
{
    private static readonly TimeSpan BoundedWait = TimeSpan.FromSeconds(15);
    private static readonly TimeSpan StopTimeout = TimeSpan.FromSeconds(15);
    private static readonly TimeSpan ObservationWindow = TimeSpan.FromMilliseconds(1500);
    private const string SupportedType = "synthetic";
    private static readonly JsonSerializerOptions SerializerOptions = new(JsonSerializerDefaults.Web);

    private readonly SqliteTestDatabase _database = new();
    private SqliteConnectionFactory _factory = null!;
    private SqliteAgentStore _store = null!;

    public async Task InitializeAsync()
    {
        _factory = _database.CreateFactory(Path.Combine("data", "a07-longpoll.db"));
        _store = new(_factory, new SqliteMigrator(_factory));
        await _store.InitializeAsync(CancellationToken.None);
    }

    public async Task DisposeAsync() => await _database.DisposeAsync();

    [Theory]
    [InlineData(AgentMode.PauseCommands)]
    [InlineData(AgentMode.Drain)]
    [InlineData(AgentMode.Maintenance)]
    [InlineData(AgentMode.Disabled)]
    public async Task A_command_returned_by_a_poll_issued_before_the_mode_flip_is_still_admitted(AgentMode mode)
    {
        var state = ReadyState(AgentMode.Normal);
        var erp = new FakeErp();
        using var sessions = new ErpSessionManager(erp, AgentOptions(), state);
        var commandJson = MakeLeaseCommand(out var commandId);
        var leaseId = Guid.NewGuid();
        using var worker = CreateLeaseWorker(erp, sessions, state);

        try
        {
            await worker.StartAsync(CancellationToken.None);
            var request = await erp.FirstPollIssued.Task.WaitAsync(BoundedWait);
            Assert.Equal(1, erp.LeaseCalls);

            // The restriction lands while the poll is still held.
            state.SetRemoteMode(mode);
            Assert.False(state.Snapshot.CanLeaseCommands);

            // The ERP completes the held poll with a real command under that lease.
            erp.CompleteFirstPoll(new LeaseResponse(true, leaseId, DateTimeOffset.UtcNow.AddMinutes(5), commandJson));
            var ack = await erp.ReceivedAckObserved.Task.WaitAsync(BoundedWait);

            // Admission boundary = poll issuance: the command is durably stored and stays queued.
            Assert.Equal(commandId, ack.CommandId);
            Assert.Equal(leaseId, ack.Request.LeaseId);
            Assert.Equal("queued", await StatusAsync(commandId));

            // Under the restriction the CanLeaseCommands gate denies the next iteration: no new poll.
            await Task.Delay(ObservationWindow, CancellationToken.None);
            Assert.Equal(1, erp.LeaseCalls);
            Assert.Equal(1, erp.ReceivedAckCalls);
            Assert.Equal("queued", await StatusAsync(commandId));
        }
        finally
        {
            await StopWorkerAsync(worker);
        }
    }

    [Fact]
    public async Task Handshake_maintenance_during_a_held_poll_keeps_the_returned_command_admitted()
    {
        var state = ReadyState(AgentMode.Normal);
        var erp = new FakeErp();
        using var sessions = new ErpSessionManager(erp, AgentOptions(), state);
        var commandJson = MakeLeaseCommand(out var commandId);
        var leaseId = Guid.NewGuid();
        using var worker = CreateLeaseWorker(erp, sessions, state);

        try
        {
            await worker.StartAsync(CancellationToken.None);
            await erp.FirstPollIssued.Task.WaitAsync(BoundedWait);

            state.SetHandshakeMaintenance(true);
            Assert.False(state.Snapshot.CanLeaseCommands);

            erp.CompleteFirstPoll(new LeaseResponse(true, leaseId, DateTimeOffset.UtcNow.AddMinutes(5), commandJson));
            var ack = await erp.ReceivedAckObserved.Task.WaitAsync(BoundedWait);

            Assert.Equal(commandId, ack.CommandId);
            Assert.Equal(leaseId, ack.Request.LeaseId);
            Assert.Equal("queued", await StatusAsync(commandId));

            await Task.Delay(ObservationWindow, CancellationToken.None);
            Assert.Equal(1, erp.LeaseCalls);
            Assert.Equal(1, erp.ReceivedAckCalls);
            Assert.Equal("queued", await StatusAsync(commandId));
        }
        finally
        {
            await StopWorkerAsync(worker);
        }
    }

    [Fact]
    public async Task Normal_mode_admits_the_command_and_polling_continues()
    {
        var state = ReadyState(AgentMode.Normal);
        var erp = new FakeErp();
        using var sessions = new ErpSessionManager(erp, AgentOptions(), state);
        var commandJson = MakeLeaseCommand(out var commandId);
        var leaseId = Guid.NewGuid();
        using var worker = CreateLeaseWorker(erp, sessions, state);

        try
        {
            await worker.StartAsync(CancellationToken.None);
            await erp.FirstPollIssued.Task.WaitAsync(BoundedWait);
            Assert.True(state.Snapshot.CanLeaseCommands);

            erp.CompleteFirstPoll(new LeaseResponse(true, leaseId, DateTimeOffset.UtcNow.AddMinutes(5), commandJson));
            var ack = await erp.ReceivedAckObserved.Task.WaitAsync(BoundedWait);

            Assert.Equal(commandId, ack.CommandId);
            Assert.Equal("queued", await StatusAsync(commandId));

            // Control: with no restriction the next loop iteration issues another poll.
            await erp.SecondPollIssued.Task.WaitAsync(BoundedWait);
            Assert.True(erp.LeaseCalls >= 2);
        }
        finally
        {
            await StopWorkerAsync(worker);
        }
    }

    // ---- helpers ----

    private static AgentRuntimeState ReadyState(AgentMode mode)
    {
        var state = new AgentRuntimeState();
        state.RestoreLocalEtlPause(false);
        state.SetRemoteMode(mode);
        state.CompleteBootstrap();
        return state;
    }

    private CommandLeaseWorker CreateLeaseWorker(FakeErp erp, ErpSessionManager sessions, AgentRuntimeState state)
    {
        var configuration = new DynamicConfigurationState(
            Options.Create(new CommandOptions { SupportedTypes = [SupportedType] }),
            Options.Create(new EtlOptions { Entities = [], IntervalMinutes = 60 }));
        return new CommandLeaseWorker(
            erp,
            _store,
            sessions,
            state,
            configuration,
            Options.Create(new ErpOptions { RequireClientCertificate = false, LongPollSeconds = 1 }),
            Options.Create(new CommandOptions { MaxConcurrency = 4, SupportedTypes = [SupportedType] }),
            NullLogger<CommandLeaseWorker>.Instance);
    }

    private static JsonElement MakeLeaseCommand(out Guid commandId)
    {
        using var document = JsonDocument.Parse("{\"amount\":10}");
        var payload = document.RootElement.Clone();
        var envelope = new CommandEnvelope(
            Guid.NewGuid(), SupportedType, 1, 100, $"order:{Guid.NewGuid():N}", null,
            DateTimeOffset.UtcNow, null, null, null, PayloadHasher.Compute(payload), payload);
        commandId = envelope.CommandId;
        return JsonSerializer.SerializeToElement(envelope, SerializerOptions);
    }

    private async Task<string?> StatusAsync(Guid commandId)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT status FROM commands_inbox WHERE command_id=$id";
        command.Parameters.AddWithValue("$id", commandId.ToString("D"));
        return await command.ExecuteScalarAsync(CancellationToken.None) as string;
    }

    private static IOptions<AgentOptions> AgentOptions() => Options.Create(new AgentOptions
    {
        AgentId = $"a07-longpoll-{Guid.NewGuid():N}",
        SiteId = "a07-site",
        DataDirectory = Path.GetTempPath(),
        MaxClockDriftSeconds = 30
    });

    private static async Task StopWorkerAsync(BackgroundService worker)
    {
        using var stop = new CancellationTokenSource(StopTimeout);
        await worker.StopAsync(stop.Token);
    }

    private sealed record ReceivedAck(Guid CommandId, CommandReceivedRequest Request);

    private sealed class FakeErp : IErpClient
    {
        private int _leaseCalls;
        private int _receivedAckCalls;
        private readonly TaskCompletionSource<LeaseResponse> _firstPollResult = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource<LeaseRequest> FirstPollIssued { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource<LeaseRequest> SecondPollIssued { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource<ReceivedAck> ReceivedAckObserved { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public int LeaseCalls => Volatile.Read(ref _leaseCalls);
        public int ReceivedAckCalls => Volatile.Read(ref _receivedAckCalls);

        public void CompleteFirstPoll(LeaseResponse response) => _firstPollResult.TrySetResult(response);

        public Task<SessionStartResponse> StartSessionAsync(SessionStartRequest request, CancellationToken cancellationToken) =>
            Task.FromResult(new SessionStartResponse(Guid.NewGuid(), DateTimeOffset.UtcNow, true, "1.0.0", 0, false));

        public async Task<LeaseResponse> LeaseCommandAsync(LeaseRequest request, CancellationToken cancellationToken)
        {
            if (Interlocked.Increment(ref _leaseCalls) == 1)
            {
                FirstPollIssued.TrySetResult(request);
                return await _firstPollResult.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
            }
            SecondPollIssued.TrySetResult(request);
            return new LeaseResponse(false, null, null, null);
        }

        public Task AcknowledgeReceivedAsync(Guid commandId, CommandReceivedRequest request, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _receivedAckCalls);
            ReceivedAckObserved.TrySetResult(new(commandId, request));
            return Task.CompletedTask;
        }

        public Task AcknowledgeResultAsync(Guid commandId, string resultJson, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task<BatchAcknowledgement> UploadBatchAsync(EtlBatch batch, Stream content, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task CompleteEtlRunAsync(Guid runId, object summary, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task SendHeartbeatAsync(HeartbeatRequest request, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task<RemoteConfigurationResponse?> GetConfigurationAsync(long currentVersion, CancellationToken cancellationToken) => Task.FromResult<RemoteConfigurationResponse?>(null);
    }
}
