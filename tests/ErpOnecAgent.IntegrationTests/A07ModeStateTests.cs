using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.Json;
using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Application.Commands;
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
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace ErpOnecAgent.IntegrationTests;

public sealed class A07ModeStateTests : IAsyncLifetime
{
    private static readonly TimeSpan PersistenceTimeout = TimeSpan.FromSeconds(5);
    private readonly SqliteTestDatabase _database = new();
    private SqliteAgentStore _store = null!;

    public async Task InitializeAsync()
    {
        var factory = _database.CreateFactory(Path.Combine("data", "a07-mode.db"));
        _store = new(factory, new SqliteMigrator(factory));
        await _store.InitializeAsync(CancellationToken.None);
    }

    public async Task DisposeAsync() => await _database.DisposeAsync();

    [Fact]
    public async Task Remote_pause_survives_normal_handshake()
    {
        var state = new AgentRuntimeState();
        var configuration = NewConfiguration();
        configuration.Apply(1, Remote("PauseCommands"), state);
        state.CompleteBootstrap();
        using var sessions = new ErpSessionManager(new FakeErp(), AgentOptions("handshake"), state);

        await sessions.GetSessionAsync(CancellationToken.None);

        var snapshot = state.Snapshot;
        Assert.Equal(AgentMode.PauseCommands, snapshot.RemoteMode);
        Assert.Equal(AgentMode.PauseCommands, snapshot.EffectiveMode);
        Assert.False(snapshot.CanLeaseCommands);
        Assert.False(snapshot.CanExecuteCommands);
        Assert.True(snapshot.CanExtract);
    }

    [Fact]
    public async Task Local_pause_survives_handshake_maintenance_on_and_off()
    {
        var state = ReadyState(AgentMode.Normal, localPaused: true);
        var erp = new FakeErp { MaintenanceResponses = [true, false] };
        using var sessions = new ErpSessionManager(erp, AgentOptions("handshake-cycle"), state);

        await sessions.GetSessionAsync(CancellationToken.None);
        Assert.Equal(AgentMode.Maintenance, state.EffectiveMode);
        Assert.False(state.Snapshot.CanExecuteCommands);
        Assert.False(state.Snapshot.CanExtract);

        sessions.Invalidate();
        await sessions.GetSessionAsync(CancellationToken.None);

        var snapshot = state.Snapshot;
        Assert.True(snapshot.LocalEtlPaused);
        Assert.False(snapshot.HandshakeMaintenance);
        Assert.Equal(AgentMode.PauseEtl, snapshot.EffectiveMode);
        Assert.True(snapshot.CanExecuteCommands);
        Assert.False(snapshot.CanExtract);
    }

    [Theory]
    [InlineData(AgentMode.Disabled)]
    [InlineData(AgentMode.PauseEtl)]
    [InlineData(AgentMode.Maintenance)]
    public async Task Local_resume_cannot_lift_remote_restriction(AgentMode remoteMode)
    {
        var state = ReadyState(remoteMode, localPaused: true);
        using var controller = new LocalEtlPauseController(_store, state);

        await controller.SetAsync(false, CancellationToken.None);

        var snapshot = state.Snapshot;
        Assert.False(snapshot.LocalEtlPaused);
        Assert.Equal(remoteMode, snapshot.RemoteMode);
        Assert.Equal(remoteMode, snapshot.EffectiveMode);
        Assert.False(await PersistedPauseAsync());
    }

    [Fact]
    public async Task Local_resume_cannot_lift_handshake_maintenance()
    {
        var state = ReadyState(AgentMode.Normal, localPaused: true);
        var erp = new FakeErp { MaintenanceResponses = [true] };
        using var sessions = new ErpSessionManager(erp, AgentOptions("handshake-resume"), state);
        using var controller = new LocalEtlPauseController(_store, state);

        await sessions.GetSessionAsync(CancellationToken.None);
        await controller.SetAsync(false, CancellationToken.None);

        var snapshot = state.Snapshot;
        Assert.False(snapshot.LocalEtlPaused);
        Assert.True(snapshot.HandshakeMaintenance);
        Assert.Equal(AgentMode.Normal, snapshot.RemoteMode);
        Assert.Equal(AgentMode.Maintenance, snapshot.EffectiveMode);
        Assert.False(snapshot.CanLeaseCommands);
        Assert.False(snapshot.CanExecuteCommands);
        Assert.False(snapshot.CanExtract);
        Assert.False(await PersistedPauseAsync());
    }

    [Fact]
    public async Task Local_and_remote_command_pause_stop_both_command_processing_and_extraction()
    {
        var state = ReadyState(AgentMode.PauseCommands, localPaused: true);
        using var controller = new LocalEtlPauseController(_store, state);

        var blocked = state.Snapshot;
        Assert.False(blocked.CanLeaseCommands);
        Assert.False(blocked.CanExecuteCommands);
        Assert.False(blocked.CanExtract);

        await controller.SetAsync(false, CancellationToken.None);

        var remoteOnly = state.Snapshot;
        Assert.False(remoteOnly.LocalEtlPaused);
        Assert.False(remoteOnly.CanLeaseCommands);
        Assert.False(remoteOnly.CanExecuteCommands);
        Assert.True(remoteOnly.CanExtract);
    }

    [Fact]
    public void Drain_stops_admission_but_keeps_execution_and_delivery()
    {
        var snapshot = ReadyState(AgentMode.Drain).Snapshot;

        Assert.False(snapshot.CanLeaseCommands);
        Assert.True(snapshot.CanExecuteCommands);
        Assert.True(snapshot.CanExtract);
        Assert.True(snapshot.CanDeliverResults);
        Assert.True(snapshot.CanUploadBatches);
        Assert.True(snapshot.CanCompleteEtlRuns);
    }

    [Fact]
    public void Fresh_state_is_not_ready_and_denies_worker_activity()
    {
        var state = new AgentRuntimeState();

        Assert.False(state.IsReady);
        Assert.Equal(AgentMode.Disabled, state.Mode);
        Assert.False(state.Snapshot.CanLeaseCommands);
        Assert.False(state.Snapshot.CanExecuteCommands);
        Assert.False(state.Snapshot.CanExtract);
        Assert.False(state.Snapshot.CanDeliverResults);
        Assert.False(state.Snapshot.CanUploadBatches);
    }

    [Fact]
    public async Task Configuration_304_preserves_restored_local_and_remote_state()
    {
        var state = new AgentRuntimeState();
        var configuration = NewConfiguration();
        configuration.Apply(7, Remote("PauseCommands"), state);
        state.RestoreLocalEtlPause(true);
        state.CompleteBootstrap();
        var configurationJson = JsonSerializer.Serialize(Remote("PauseCommands"), JsonOptions);
        await _store.SaveConfigSnapshotAsync(7, configurationJson, Hash(configurationJson), "validated", CancellationToken.None);
        await _store.ActivateConfigSnapshotAsync(7, CancellationToken.None);
        var erp = new FakeErp();
        using var sessions = new ErpSessionManager(erp, AgentOptions("configuration-304"), state);
        using var worker = new ConfigurationWorker(erp, _store, sessions, configuration, state, NullLogger<ConfigurationWorker>.Instance);

        try
        {
            await worker.StartAsync(CancellationToken.None);
            await erp.ConfigurationRead.Task.WaitAsync(TimeSpan.FromSeconds(5));
        }
        finally
        {
            await worker.StopAsync(CancellationToken.None);
        }

        var snapshot = state.Snapshot;
        Assert.Equal(7, configuration.Version);
        Assert.Equal(AgentMode.PauseCommands, snapshot.RemoteMode);
        Assert.True(snapshot.LocalEtlPaused);
        Assert.Equal(AgentMode.PauseCommands, snapshot.EffectiveMode);
    }

    [Fact]
    public async Task Bootstrap_restores_local_and_remote_state_before_completion()
    {
        await _store.SetStateAsync(LocalEtlPauseController.StateKey, "{\"paused\":true}", CancellationToken.None);
        var remote = Remote("PauseCommands");
        var configurationJson = JsonSerializer.Serialize(remote, JsonOptions);
        await _store.SaveConfigSnapshotAsync(19, configurationJson, Hash(configurationJson), "validated", CancellationToken.None);
        await _store.ActivateConfigSnapshotAsync(19, CancellationToken.None);
        var state = new AgentRuntimeState();
        var configuration = NewConfiguration();
        using var controller = new LocalEtlPauseController(_store, state);
        var instanceLock = new SingleInstanceLock();
        var bootstrap = new BootstrapService(
            _store,
            new FakeSpoolStore(),
            new FakeSecretStore(),
            instanceLock,
            state,
            configuration,
            controller,
            AgentOptions("bootstrap"),
            Options.Create(new ErpOptions { RequireClientCertificate = false }),
            Options.Create(new OnecOptions { CredentialSecretName = "test-secret" }),
            NullLogger<BootstrapService>.Instance);

        Assert.False(state.IsReady);
        try
        {
            await bootstrap.StartAsync(CancellationToken.None);

            Assert.True(state.IsReady);
            Assert.Equal(19, configuration.Version);
            var snapshot = state.Snapshot;
            Assert.True(snapshot.LocalEtlPaused);
            Assert.Equal(AgentMode.PauseCommands, snapshot.RemoteMode);
            Assert.Equal(AgentMode.PauseCommands, snapshot.EffectiveMode);
            Assert.False(snapshot.CanExecuteCommands);
            Assert.False(snapshot.CanExtract);
        }
        finally
        {
            await bootstrap.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task Invalid_persisted_local_state_fails_closed_during_bootstrap()
    {
        await _store.SetStateAsync(LocalEtlPauseController.StateKey, "{\"paused\":\"yes\"}", CancellationToken.None);
        var state = new AgentRuntimeState();
        var configuration = NewConfiguration();
        using var controller = new LocalEtlPauseController(_store, state);
        var bootstrap = new BootstrapService(
            _store,
            new FakeSpoolStore(),
            new FakeSecretStore(),
            new SingleInstanceLock(),
            state,
            configuration,
            controller,
            AgentOptions("invalid-bootstrap"),
            Options.Create(new ErpOptions { RequireClientCertificate = false }),
            Options.Create(new OnecOptions { CredentialSecretName = "test-secret" }),
            NullLogger<BootstrapService>.Instance);

        try
        {
            await Assert.ThrowsAsync<InvalidDataException>(() => bootstrap.StartAsync(CancellationToken.None));
            Assert.False(state.IsReady);
            Assert.NotEqual(AgentMode.Normal, state.Mode);
        }
        finally
        {
            await bootstrap.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task Failed_local_persistence_keeps_previous_runtime_and_saved_intent()
    {
        var state = ReadyState(AgentMode.Normal, localPaused: false);
        using var successfulController = new LocalEtlPauseController(_store, state);
        await successfulController.SetAsync(true, CancellationToken.None);
        var previous = await _store.GetStateAsync(LocalEtlPauseController.StateKey, CancellationToken.None);
        using var failingController = new LocalEtlPauseController(FailingStoreProxy.Create(_store), state);

        await Assert.ThrowsAsync<InvalidOperationException>(() => failingController.SetAsync(false, CancellationToken.None));

        Assert.True(state.Snapshot.LocalEtlPaused);
        Assert.Equal(previous, await _store.GetStateAsync(LocalEtlPauseController.StateKey, CancellationToken.None));
    }

    [Fact]
    public async Task Concurrent_local_intent_updates_serialize_overlapped_sqlite_persistence()
    {
        var state = ReadyState(AgentMode.Normal);
        var control = new GatedStoreControl(_store);
        using var controller = new LocalEtlPauseController(GatedStoreProxy.Create(control), state);

        var first = controller.SetAsync(true, CancellationToken.None);
        await control.FirstPersisted.Task.WaitAsync(PersistenceTimeout);
        var durableTrueBeforePublish = await PersistedPauseAsync();
        var runtimePublishedBeforeRelease = state.Snapshot.LocalEtlPaused;
        var second = controller.SetAsync(false, CancellationToken.None);
        var secondEnteredBeforeRelease = control.SecondEntered.Task.IsCompleted;
        var secondCompletedBeforeRelease = second.IsCompleted;

        control.ReleaseFirst.TrySetResult(true);
        await Task.WhenAll(first, second).WaitAsync(PersistenceTimeout);

        Assert.True(durableTrueBeforePublish);
        Assert.False(runtimePublishedBeforeRelease);
        Assert.False(secondEnteredBeforeRelease);
        Assert.False(secondCompletedBeforeRelease);
        Assert.True(control.SecondEntered.Task.IsCompleted);
        Assert.Equal(2, control.SetStateCalls);
        Assert.False(await PersistedPauseAsync());
        Assert.False(state.Snapshot.LocalEtlPaused);
    }

    [Fact]
    public async Task Pause_admin_command_persists_before_successful_result()
    {
        var state = ReadyState(AgentMode.Normal);
        using var controller = new LocalEtlPauseController(_store, state);
        var onec = new FakeOnecCommandClient();
        using var worker = CreateWorker(controller, state, onec);
        var command = MakeCommand("pause_etl");
        await _store.StoreCommandAsync(command, DateTimeOffset.UtcNow, CancellationToken.None);
        var stored = await ReadyForAsync(command.CommandId);

        await ProcessAdminAsync(worker, stored);

        var result = await ResultForAsync(command.CommandId);
        Assert.Equal("succeeded", result.GetProperty("status").GetString());
        Assert.Equal("PauseEtl", result.GetProperty("data").GetProperty("mode").GetString());
        Assert.True(state.Snapshot.LocalEtlPaused);
        Assert.True(await PersistedPauseAsync());
        Assert.Equal(0, onec.ExecuteCalls);
        Assert.Equal(0, onec.StatusCalls);
    }

    [Fact]
    public async Task Pause_admin_result_waits_for_durable_persistence()
    {
        var state = ReadyState(AgentMode.Normal);
        var control = new GatedStoreControl(_store);
        using var controller = new LocalEtlPauseController(GatedStoreProxy.Create(control), state);
        var onec = new FakeOnecCommandClient();
        using var worker = CreateWorker(controller, state, onec);
        var command = MakeCommand("pause_etl");
        await _store.StoreCommandAsync(command, DateTimeOffset.UtcNow, CancellationToken.None);
        var stored = await ReadyForAsync(command.CommandId);
        var processing = ProcessAdminAsync(worker, stored);
        bool completedWhilePersistenceGated;
        IReadOnlyList<PendingResult> resultsWhilePersistenceGated;

        try
        {
            await control.FirstPersisted.Task.WaitAsync(PersistenceTimeout);
            completedWhilePersistenceGated = processing.IsCompleted;
            resultsWhilePersistenceGated = await _store.GetPendingResultsAsync(10, DateTimeOffset.UtcNow, CancellationToken.None);
        }
        finally
        {
            control.ReleaseFirst.TrySetResult(true);
        }

        await processing.WaitAsync(PersistenceTimeout);
        Assert.False(completedWhilePersistenceGated);
        Assert.Empty(resultsWhilePersistenceGated);
        var result = await ResultForAsync(command.CommandId);
        Assert.Equal("succeeded", result.GetProperty("status").GetString());
        Assert.True(state.Snapshot.LocalEtlPaused);
        Assert.True(await PersistedPauseAsync());
        Assert.Equal(0, onec.ExecuteCalls);
        Assert.Equal(0, onec.StatusCalls);
    }

    [Fact]
    public async Task Resume_admin_command_does_not_clear_remote_pause()
    {
        // A07b: a fresh administrative callback is new work, denied while CanExecuteCommands is
        // false — so this scenario uses a remote restriction that pauses ETL only: the admin
        // executes, clears ONLY the durable local pause, and the remote pause persists.
        var state = ReadyState(AgentMode.PauseEtl, localPaused: true);
        using var controller = new LocalEtlPauseController(_store, state);
        var onec = new FakeOnecCommandClient();
        using var worker = CreateWorker(controller, state, onec);
        var command = MakeCommand("resume_etl");
        await _store.StoreCommandAsync(command, DateTimeOffset.UtcNow, CancellationToken.None);
        var stored = await ReadyForAsync(command.CommandId);

        await ProcessAdminAsync(worker, stored);

        var result = await ResultForAsync(command.CommandId);
        Assert.Equal("succeeded", result.GetProperty("status").GetString());
        Assert.Equal("PauseEtl", result.GetProperty("data").GetProperty("mode").GetString());
        Assert.False(state.Snapshot.LocalEtlPaused);
        Assert.Equal(AgentMode.PauseEtl, state.EffectiveMode);
        Assert.False(await PersistedPauseAsync());
    }

    [Fact]
    public async Task Failed_local_persistence_uses_owner_aware_unknown_result()
    {
        var state = ReadyState(AgentMode.Normal);
        var onec = new FakeOnecCommandClient();
        using var controller = new LocalEtlPauseController(FailingStoreProxy.Create(_store), state);
        using var worker = CreateWorker(controller, state, onec);
        var command = MakeCommand("pause_etl");
        await _store.StoreCommandAsync(command, DateTimeOffset.UtcNow, CancellationToken.None);
        var stored = await ReadyForAsync(command.CommandId);

        await ProcessAdminAsync(worker, stored);

        var result = await ResultForAsync(command.CommandId);
        Assert.Equal("dead_letter", result.GetProperty("status").GetString());
        Assert.Equal("ADMINISTRATIVE_EXECUTION_UNKNOWN", result.GetProperty("error").GetProperty("code").GetString());
        Assert.True(result.GetProperty("error").GetProperty("details").GetProperty("outcomeUnknown").GetBoolean());
        Assert.False(state.Snapshot.LocalEtlPaused);
        Assert.Equal(0, onec.ExecuteCalls);
        Assert.Equal(0, onec.StatusCalls);
    }

    [Theory]
    [InlineData("0")]
    [InlineData("999")]
    public void Invalid_numeric_remote_mode_is_rejected_before_publication(string mode)
    {
        var state = new AgentRuntimeState();
        var configuration = NewConfiguration();

        Assert.Throws<InvalidDataException>(() => configuration.Apply(1, Remote(mode), state));
        Assert.Equal(AgentMode.Normal, state.Snapshot.RemoteMode);
    }

    private static AgentRuntimeState ReadyState(AgentMode mode, bool localPaused = false)
    {
        var state = new AgentRuntimeState();
        state.RestoreLocalEtlPause(localPaused);
        state.SetRemoteMode(mode);
        state.CompleteBootstrap();
        return state;
    }

    private static DynamicConfigurationState NewConfiguration() => new(
        Options.Create(new CommandOptions { SupportedTypes = ["synthetic"] }),
        Options.Create(new EtlOptions { Entities = [], IntervalMinutes = 60 }));

    private CommandExecutionWorker CreateWorker(LocalEtlPauseController controller, AgentRuntimeState state, FakeOnecCommandClient onec)
    {
        var agent = AgentOptions("worker");
        var diagnostics = new DiagnosticsCollector(
            _store,
            agent,
            Options.Create(new ErpOptions { RequireClientCertificate = false }),
            Options.Create(new OnecOptions()));
        return new CommandExecutionWorker(
            _store,
            onec,
            new FakeOnecHealthClient(),
            new DynamicConfigurationState(Options.Create(new CommandOptions()), Options.Create(new EtlOptions())),
            state,
            controller,
            diagnostics,
            Options.Create(new CommandOptions { MaxOperationalAttempts = 12, SupportedTypes = ["synthetic"] }),
            NullLogger<CommandExecutionWorker>.Instance);
    }

    private static async Task ProcessAdminAsync(CommandExecutionWorker worker, StoredCommand command)
    {
        var method = typeof(CommandExecutionWorker).GetMethod("ProcessAsync", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("Command execution method was not found.");
        await (Task)(method.Invoke(worker, [command, CancellationToken.None])
            ?? throw new InvalidOperationException("Command execution method returned no task."));
    }

    private async Task<StoredCommand> ReadyForAsync(Guid commandId) =>
        (await _store.GetReadyCommandsAsync(10, DateTimeOffset.UtcNow.AddDays(1), CancellationToken.None)).Single(command => command.Envelope.CommandId == commandId);

    private async Task<JsonElement> ResultForAsync(Guid commandId)
    {
        var result = (await _store.GetPendingResultsAsync(10, DateTimeOffset.UtcNow, CancellationToken.None)).Single(item => item.CommandId == commandId);
        using var document = JsonDocument.Parse(result.PayloadJson);
        return document.RootElement.Clone();
    }

    private async Task<bool> PersistedPauseAsync()
    {
        var value = await _store.GetStateAsync(LocalEtlPauseController.StateKey, CancellationToken.None);
        Assert.NotNull(value);
        using var document = JsonDocument.Parse(value);
        return document.RootElement.GetProperty("paused").GetBoolean();
    }

    private static IOptions<AgentOptions> AgentOptions(string suffix) => Options.Create(new AgentOptions
    {
        AgentId = $"a07-{suffix}-{Guid.NewGuid():N}",
        SiteId = "a07-site",
        DataDirectory = Path.GetTempPath()
    });

    private static RemoteAgentConfiguration Remote(string mode) => new()
    {
        Mode = mode,
        CommandTypes = ["synthetic"],
        EtlIntervalMinutes = 60,
        EtlEntities = [new EtlEntityDefinition("synthetic", "Synthetic", "Id", null, null, ["Id"], "incremental", 10, 3)]
    };

    private static CommandEnvelope MakeCommand(string commandType)
    {
        var payload = JsonSerializer.SerializeToElement(new { mode = "administrative" });
        return new(Guid.NewGuid(), commandType, 1, 100, $"a07:{Guid.NewGuid():N}", null, DateTimeOffset.UtcNow, null, null, null, PayloadHasher.Compute(payload), payload);
    }

    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    private static string Hash(string json)
    {
        using var document = JsonDocument.Parse(json);
        return PayloadHasher.Compute(document.RootElement);
    }

    private sealed class FakeErp : IErpClient
    {
        public bool[] MaintenanceResponses { get; init; } = [];
        public bool MaintenanceMode { get; init; }
        public TaskCompletionSource<bool> ConfigurationRead { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _maintenanceIndex;

        public Task<SessionStartResponse> StartSessionAsync(SessionStartRequest request, CancellationToken cancellationToken)
        {
            var maintenance = MaintenanceResponses.Length == 0 ? MaintenanceMode : MaintenanceResponses[_maintenanceIndex++];
            return Task.FromResult(new SessionStartResponse(Guid.NewGuid(), DateTimeOffset.UtcNow, true, "1.0.0", 0, maintenance));
        }

        public Task<LeaseResponse> LeaseCommandAsync(LeaseRequest request, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task AcknowledgeReceivedAsync(Guid commandId, CommandReceivedRequest request, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task AcknowledgeResultAsync(Guid commandId, string resultJson, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task<BatchAcknowledgement> UploadBatchAsync(EtlBatch batch, Stream content, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task CompleteEtlRunAsync(Guid runId, object summary, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task SendHeartbeatAsync(HeartbeatRequest request, CancellationToken cancellationToken) => throw new NotSupportedException();

        public Task<RemoteConfigurationResponse?> GetConfigurationAsync(long currentVersion, CancellationToken cancellationToken)
        {
            ConfigurationRead.TrySetResult(true);
            return Task.FromResult<RemoteConfigurationResponse?>(null);
        }
    }

    private sealed class FakeSpoolStore : ISpoolStore
    {
        public Task<EtlBatch> WriteBatchAsync(Guid runId, EtlEntityDefinition entity, IReadOnlyList<JsonElement> rows, EtlCursor? watermarkFrom, EtlCursor? watermarkTo, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task<Stream> OpenReadAsync(EtlBatch batch, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task<long> GetSizeAsync(CancellationToken cancellationToken) => Task.FromResult(0L);
        public Task QuarantineTemporaryFilesAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Task DeleteAcknowledgedAsync(EtlBatch batch, CancellationToken cancellationToken) => throw new NotSupportedException();
    }

    private sealed class FakeSecretStore : ISecretStore
    {
        public Task SaveAsync(string name, string secret, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task<string?> ReadAsync(string name, CancellationToken cancellationToken) => Task.FromResult<string?>("synthetic");
    }

    private sealed class FakeOnecCommandClient : IOnecCommandClient
    {
        public int ExecuteCalls { get; private set; }
        public int StatusCalls { get; private set; }

        public Task<OnecExecutionResult> ExecuteAsync(CommandEnvelope command, CancellationToken cancellationToken)
        {
            ExecuteCalls++;
            return Task.FromResult(new OnecExecutionResult(OnecExecutionKind.Succeeded, new(command.CommandId, "succeeded", JsonSerializer.SerializeToElement(new { @ref = "unexpected" }), null, [], 1), 200, null, null));
        }

        public Task<OnecExecutionResult> GetStatusAsync(Guid commandId, CancellationToken cancellationToken)
        {
            StatusCalls++;
            return Task.FromResult(new OnecExecutionResult(OnecExecutionKind.Succeeded, new(commandId, "succeeded", JsonSerializer.SerializeToElement(new { @ref = "unexpected" }), null, [], 1), 200, null, null));
        }
    }

    private sealed class FakeOnecHealthClient : IOnecHealthClient
    {
        public Task<OnecHealthResponse?> CheckAsync(CancellationToken cancellationToken) => Task.FromResult<OnecHealthResponse?>(null);
    }

    public sealed class GatedStoreControl(IAgentStore inner)
    {
        private int _setStateCalls;

        public IAgentStore Inner { get; } = inner;
        public TaskCompletionSource<bool> FirstPersisted { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource<bool> ReleaseFirst { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource<bool> SecondEntered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public int SetStateCalls => Volatile.Read(ref _setStateCalls);

        public int NextSetStateCall() => Interlocked.Increment(ref _setStateCalls);
    }

    public class GatedStoreProxy : DispatchProxy
    {
        private static readonly ConditionalWeakTable<DispatchProxy, GatedStoreControl> Controls = new();

        public static IAgentStore Create(GatedStoreControl control)
        {
            var proxy = DispatchProxy.Create<IAgentStore, GatedStoreProxy>();
            Controls.Add((DispatchProxy)(object)proxy, control);
            return proxy;
        }

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            if (!Controls.TryGetValue(this, out var control)) throw new InvalidOperationException("Proxy state is missing.");
            if (targetMethod?.Name == nameof(IAgentStore.SetStateAsync)) return SetStateAsync(control, args);
            return targetMethod!.Invoke(control.Inner, args);
        }

        private static async Task SetStateAsync(GatedStoreControl control, object?[]? args)
        {
            var key = (string)args![0]!;
            var value = (string)args[1]!;
            var cancellationToken = (CancellationToken)args[2]!;
            if (control.NextSetStateCall() == 1)
            {
                await control.Inner.SetStateAsync(key, value, cancellationToken).ConfigureAwait(false);
                control.FirstPersisted.TrySetResult(true);
                await control.ReleaseFirst.Task.ConfigureAwait(false);
                return;
            }

            control.SecondEntered.TrySetResult(true);
            await control.Inner.SetStateAsync(key, value, cancellationToken).ConfigureAwait(false);
        }
    }

    public class FailingStoreProxy : DispatchProxy
    {
        private static readonly ConditionalWeakTable<DispatchProxy, IAgentStore> Stores = new();

        public static IAgentStore Create(IAgentStore inner)
        {
            var proxy = DispatchProxy.Create<IAgentStore, FailingStoreProxy>();
            Stores.Add((DispatchProxy)(object)proxy, inner);
            return proxy;
        }

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            if (!Stores.TryGetValue(this, out var inner)) throw new InvalidOperationException("Proxy state is missing.");
            if (targetMethod?.Name == nameof(IAgentStore.SetStateAsync)) throw new InvalidOperationException("Injected local state persistence failure.");
            return targetMethod!.Invoke(inner, args);
        }
    }
}
