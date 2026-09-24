using System.Globalization;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.Json;
using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Application.Configuration;
using ErpOnecAgent.Contracts.Erp;
using ErpOnecAgent.Domain.Agent;
using ErpOnecAgent.Domain.Common;
using ErpOnecAgent.Domain.Etl;
using ErpOnecAgent.Infrastructure.Persistence.Sqlite;
using ErpOnecAgent.Service.Runtime;
using ErpOnecAgent.Service.Workers;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace ErpOnecAgent.IntegrationTests;

public sealed class ConfigurationActivationTests : IAsyncLifetime
{
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(5);
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web) { PropertyNameCaseInsensitive = true };
    private readonly SqliteTestDatabase _database = new();
    private SqliteConnectionFactory _factory = null!;
    private SqliteAgentStore _store = null!;

    public async Task InitializeAsync()
    {
        _factory = _database.CreateFactory(Path.Combine("data", "configuration-activation.db"));
        _store = new(_factory, new SqliteMigrator(_factory));
        await _store.InitializeAsync(CancellationToken.None);
    }

    public async Task DisposeAsync() => await _database.DisposeAsync();

    [Fact]
    public async Task Worker_activation_failure_keeps_previous_database_and_runtime_state()
    {
        var previous = Remote("PauseCommands", "old-command");
        var replacement = Remote("Normal", "new-command");
        await PersistActiveAsync(7, previous);
        var state = ReadyState(7, previous, localPaused: false, maintenance: false);
        var configuration = NewConfiguration();
        configuration.Apply(7, previous, state);
        var control = new ActivationFailureControl(_store);
        var erp = new FakeErp { Response = Response(8, replacement) };
        using var sessions = new ErpSessionManager(erp, AgentOptions("activation-failure"), state);
        using var worker = new ConfigurationWorker(erp, ActivationFailureStoreProxy.Create(_store, control), sessions, configuration, state, NullLogger<ConfigurationWorker>.Instance);

        try
        {
            await worker.StartAsync(CancellationToken.None);
            await control.FailureTriggered.Task.WaitAsync(WaitTimeout);
            await control.RejectionSaveCompleted.Task.WaitAsync(WaitTimeout);
        }
        finally
        {
            await worker.StopAsync(CancellationToken.None);
        }

        var active = await _store.GetActiveConfigSnapshotAsync(CancellationToken.None);
        Assert.NotNull(active);
        Assert.Equal(7, active.Version);
        Assert.Equal(7, configuration.Version);
        Assert.Equal(AgentMode.PauseCommands, state.Snapshot.RemoteMode);
        Assert.Equal("old-command", Assert.Single(configuration.CommandTypes));
    }

    [Fact]
    public async Task Rejected_replay_does_not_demote_active_snapshot()
    {
        var active = Remote("Normal", "active-command");
        var json = Serialize(active);
        var hash = Hash(json);
        await PersistActiveSnapshotAsync(4, json, hash);
        var control = new SqliteRowReader(_factory);

        await _store.SaveConfigSnapshotAsync(4, json, hash, "rejected", CancellationToken.None);

        var row = await control.ReadAsync(4);
        Assert.Equal("active", row.Status);
        Assert.Equal(hash, row.Hash);
        Assert.Equal(json, row.Json);
        Assert.Equal(4, (await _store.GetActiveConfigSnapshotAsync(CancellationToken.None))!.Version);
    }

    [Fact]
    public async Task Missing_activation_target_preserves_previous_active_snapshot()
    {
        var active = Remote("Normal", "active-command");
        await PersistActiveAsync(12, active);

        await Assert.ThrowsAsync<InvalidDataException>(() => _store.ActivateConfigSnapshotAsync(13, CancellationToken.None));

        var restored = await _store.GetActiveConfigSnapshotAsync(CancellationToken.None);
        Assert.NotNull(restored);
        Assert.Equal(12, restored.Version);
        Assert.Equal(Hash(Serialize(active)), restored.Hash);
    }

    [Fact]
    public async Task Rejected_activation_target_preserves_previous_active_snapshot()
    {
        var active = Remote("Normal", "active-command");
        var rejected = Remote("Disabled", "rejected-command");
        var rejectedJson = Serialize(rejected);
        await PersistActiveAsync(12, active);
        await _store.SaveConfigSnapshotAsync(13, rejectedJson, Hash(rejectedJson), "rejected", CancellationToken.None);

        await Assert.ThrowsAsync<InvalidDataException>(() => _store.ActivateConfigSnapshotAsync(13, CancellationToken.None));

        var restored = await _store.GetActiveConfigSnapshotAsync(CancellationToken.None);
        Assert.NotNull(restored);
        Assert.Equal(12, restored.Version);
        Assert.Equal(Hash(Serialize(active)), restored.Hash);
    }

    [Fact]
    public async Task Older_validated_snapshot_cannot_rollback_active_version()
    {
        var active = Remote("Normal", "active-command");
        var older = Remote("Disabled", "older-command");
        await PersistActiveAsync(12, active);
        await _store.SaveConfigSnapshotAsync(11, Serialize(older), Hash(Serialize(older)), "validated", CancellationToken.None);

        await Assert.ThrowsAsync<InvalidDataException>(() => _store.ActivateConfigSnapshotAsync(11, CancellationToken.None));

        var restored = await _store.GetActiveConfigSnapshotAsync(CancellationToken.None);
        Assert.NotNull(restored);
        Assert.Equal(12, restored.Version);
    }

    [Fact]
    public async Task Same_version_changed_hash_cannot_mutate_original_active_snapshot()
    {
        var active = Remote("Normal", "active-command");
        var originalJson = Serialize(active);
        var originalHash = Hash(originalJson);
        await PersistActiveSnapshotAsync(3, originalJson, originalHash);
        var changed = Remote("Disabled", "changed-command");

        await Assert.ThrowsAsync<InvalidDataException>(() => _store.SaveConfigSnapshotAsync(3, Serialize(changed), Hash(Serialize(changed)), "validated", CancellationToken.None));

        var row = await new SqliteRowReader(_factory).ReadAsync(3);
        Assert.Equal(originalJson, row.Json);
        Assert.Equal(originalHash, row.Hash);
        Assert.Equal("active", row.Status);
    }

    [Fact]
    public async Task Corrupted_persisted_hash_fails_closed_during_restart()
    {
        var active = Remote("PauseCommands", "active-command");
        await PersistActiveAsync(21, active);
        await ExecuteSqlAsync("UPDATE config_snapshots SET config_hash='corrupted-hash' WHERE config_version=21;");
        var state = new AgentRuntimeState();
        var configuration = NewConfiguration();
        using var controller = new LocalEtlPauseController(_store, state);
        var instanceLock = new SingleInstanceLock();
        var bootstrap = CreateBootstrap(state, configuration, controller, instanceLock, "corrupted-hash");

        try
        {
            await Assert.ThrowsAsync<InvalidDataException>(() => bootstrap.StartAsync(CancellationToken.None));
            Assert.False(state.IsReady);
        }
        finally
        {
            await bootstrap.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task Restart_restores_persisted_valid_activation_before_ready()
    {
        var active = Remote("PauseCommands", "active-command");
        await PersistActiveAsync(22, active);
        var state = new AgentRuntimeState();
        var configuration = NewConfiguration();
        using var controller = new LocalEtlPauseController(_store, state);
        var instanceLock = new SingleInstanceLock();
        var bootstrap = CreateBootstrap(state, configuration, controller, instanceLock, "valid-restart");

        try
        {
            await bootstrap.StartAsync(CancellationToken.None);

            Assert.True(state.IsReady);
            Assert.Equal(22, configuration.Version);
            Assert.Equal(AgentMode.PauseCommands, state.Snapshot.RemoteMode);
            Assert.Equal("active-command", Assert.Single(configuration.CommandTypes));
        }
        finally
        {
            await bootstrap.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task Valid_new_version_preserves_local_pause_and_handshake_maintenance()
    {
        var previous = Remote("Normal", "old-command");
        var replacement = Remote("PauseCommands", "new-command");
        await PersistActiveAsync(30, previous);
        var state = ReadyState(30, previous, localPaused: true, maintenance: true);
        var configuration = NewConfiguration();
        configuration.Apply(30, previous, state);
        var erp = new FakeErp { Response = Response(31, replacement), MaintenanceMode = true };
        using var sessions = new ErpSessionManager(erp, AgentOptions("preserved-sources"), state);
        using var worker = new ConfigurationWorker(erp, _store, sessions, configuration, state, NullLogger<ConfigurationWorker>.Instance);

        try
        {
            await worker.StartAsync(CancellationToken.None);
            await WaitUntilAsync(() => configuration.Version == 31);
        }
        finally
        {
            await worker.StopAsync(CancellationToken.None);
        }

        var snapshot = state.Snapshot;
        Assert.Equal(31, configuration.Version);
        Assert.Equal("new-command", Assert.Single(configuration.CommandTypes));
        Assert.Equal(AgentMode.PauseCommands, snapshot.RemoteMode);
        Assert.True(snapshot.LocalEtlPaused);
        Assert.True(snapshot.HandshakeMaintenance);
        Assert.Equal(AgentMode.Maintenance, snapshot.EffectiveMode);
        Assert.Equal(31, (await _store.GetActiveConfigSnapshotAsync(CancellationToken.None))!.Version);
    }

    [Fact]
    public async Task Configuration_304_is_a_noop_for_active_and_runtime_state()
    {
        var active = Remote("Drain", "active-command");
        await PersistActiveAsync(40, active);
        var state = ReadyState(40, active, localPaused: true, maintenance: true);
        var configuration = NewConfiguration();
        configuration.Apply(40, active, state);
        var erp = new FakeErp { Response = null, MaintenanceMode = true };
        using var sessions = new ErpSessionManager(erp, AgentOptions("configuration-304"), state);
        using var worker = new ConfigurationWorker(erp, _store, sessions, configuration, state, NullLogger<ConfigurationWorker>.Instance);

        try
        {
            await worker.StartAsync(CancellationToken.None);
            await erp.ConfigurationRead.Task.WaitAsync(WaitTimeout);
        }
        finally
        {
            await worker.StopAsync(CancellationToken.None);
        }

        var snapshot = state.Snapshot;
        Assert.Equal(40, configuration.Version);
        Assert.Equal(AgentMode.Drain, snapshot.RemoteMode);
        Assert.True(snapshot.LocalEtlPaused);
        Assert.True(snapshot.HandshakeMaintenance);
        Assert.Equal(AgentMode.Maintenance, snapshot.EffectiveMode);
        Assert.Equal(40, (await _store.GetActiveConfigSnapshotAsync(CancellationToken.None))!.Version);
    }

    [Fact]
    public async Task Worker_publishes_after_durable_commit_even_when_stopping_is_cancelled()
    {
        var previous = Remote("Normal", "old-command");
        var replacement = Remote("PauseCommands", "new-command");
        await PersistActiveAsync(50, previous);
        var state = ReadyState(50, previous, localPaused: false, maintenance: false);
        var configuration = NewConfiguration();
        configuration.Apply(50, previous, state);
        var control = new GatedStoreControl(_store) { GateAfterCommit = true };
        var erp = new FakeErp { Response = Response(51, replacement) };
        using var sessions = new ErpSessionManager(erp, AgentOptions("commit-cancel"), state);
        using var worker = new ConfigurationWorker(erp, GatedStoreProxy.Create(_store, control), sessions, configuration, state, NullLogger<ConfigurationWorker>.Instance);
        using var start = new CancellationTokenSource();

        try
        {
            await worker.StartAsync(start.Token);
            await control.CommitCompleted.Task.WaitAsync(WaitTimeout);
            var committed = await _store.GetActiveConfigSnapshotAsync(CancellationToken.None);
            Assert.NotNull(committed);
            Assert.Equal(51, committed.Version);
            Assert.Equal(50, configuration.Version);
            start.Cancel();
            control.ReleaseAfterCommit.TrySetResult(true);
            await WaitUntilAsync(() => configuration.Version == 51);
        }
        finally
        {
            control.ReleaseAfterCommit.TrySetResult(true);
            await worker.StopAsync(CancellationToken.None).WaitAsync(WaitTimeout);
        }

        Assert.Equal(AgentMode.PauseCommands, state.Snapshot.RemoteMode);
        Assert.Equal(51, (await _store.GetActiveConfigSnapshotAsync(CancellationToken.None))!.Version);
    }

    [Fact]
    public async Task Worker_same_version_replay_is_idempotent()
    {
        var active = Remote("Drain", "active-command");
        await PersistActiveAsync(60, active);
        var state = ReadyState(60, active, localPaused: false, maintenance: false);
        var configuration = NewConfiguration();
        configuration.Apply(60, active, state);
        var control = new GatedStoreControl(_store);
        var erp = new FakeErp { Response = Response(60, active) };
        using var sessions = new ErpSessionManager(erp, AgentOptions("same-version-replay"), state);
        using var worker = new ConfigurationWorker(erp, GatedStoreProxy.Create(_store, control), sessions, configuration, state, NullLogger<ConfigurationWorker>.Instance);

        try
        {
            await worker.StartAsync(CancellationToken.None);
            await control.AcceptCompleted.Task.WaitAsync(WaitTimeout);
        }
        finally
        {
            await worker.StopAsync(CancellationToken.None);
        }

        var snapshot = state.Snapshot;
        Assert.Equal(60, configuration.Version);
        Assert.Equal(AgentMode.Drain, snapshot.RemoteMode);
        Assert.Equal("active-command", Assert.Single(configuration.CommandTypes));
        var restored = await _store.GetActiveConfigSnapshotAsync(CancellationToken.None);
        Assert.NotNull(restored);
        Assert.Equal(60, restored.Version);
        Assert.Equal(Hash(Serialize(active)), restored.Hash);
    }

    [Fact]
    public async Task Store_same_version_replay_is_idempotent()
    {
        var remote = Remote("PauseCommands", "replay-command");
        var json = Serialize(remote);
        var hash = Hash(json);

        var first = await _store.AcceptAndActivateConfigSnapshotAsync(70, json, hash, CancellationToken.None);
        var second = await _store.AcceptAndActivateConfigSnapshotAsync(70, json, hash, CancellationToken.None);

        Assert.Equal(first.Version, second.Version);
        Assert.Equal(first.ConfigurationJson, second.ConfigurationJson);
        Assert.Equal(first.Hash, second.Hash);
        var active = await _store.GetActiveConfigSnapshotAsync(CancellationToken.None);
        Assert.NotNull(active);
        Assert.Equal(70, active.Version);
        var row = await new SqliteRowReader(_factory).ReadAsync(70);
        Assert.Equal("active", row.Status);
    }

    [Fact]
    public async Task Concurrent_accept_and_activate_converges_to_single_highest_active()
    {
        var older = Remote("Normal", "older-command");
        var newer = Remote("Drain", "newer-command");
        var olderJson = Serialize(older);
        var newerJson = Serialize(newer);
        var storeA = new SqliteAgentStore(_factory, new SqliteMigrator(_factory));
        var storeB = new SqliteAgentStore(_factory, new SqliteMigrator(_factory));
        var start = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        var acceptOlder = Task.Run(async () =>
        {
            await start.Task;
            await AcceptIgnoringRollbackAsync(storeA, 5, olderJson, Hash(olderJson));
        });
        var acceptNewer = Task.Run(async () =>
        {
            await start.Task;
            await AcceptIgnoringRollbackAsync(storeB, 7, newerJson, Hash(newerJson));
        });
        start.SetResult(true);
        await Task.WhenAll(acceptOlder, acceptNewer);

        var active = await _store.GetActiveConfigSnapshotAsync(CancellationToken.None);
        Assert.NotNull(active);
        Assert.Equal(7, active.Version);
        Assert.Equal(Hash(newerJson), active.Hash);
        Assert.Equal(1, await CountActiveAsync());
    }

    [Fact]
    public async Task Overlapping_publishes_converge_to_coherent_snapshot_and_remote_mode()
    {
        var state = new AgentRuntimeState();
        var configuration = NewConfiguration();
        for (var index = 1; index <= 64; index++)
        {
            var lowerVersion = index * 2;
            var higherVersion = lowerVersion + 1;
            var lower = configuration.Prepare(lowerVersion, Remote("Normal", $"command-{lowerVersion}"));
            var higher = configuration.Prepare(higherVersion, Remote("PauseCommands", $"command-{higherVersion}"));
            var publishLower = Task.Run(() => TryPublish(configuration, lower, state));
            var publishHigher = Task.Run(() => configuration.Publish(higher, state));
            await Task.WhenAll(publishLower, publishHigher);
            var snapshot = configuration.Snapshot;
            Assert.Equal(higherVersion, snapshot.Version);
            Assert.Equal(AgentMode.PauseCommands, snapshot.Mode);
            Assert.Equal(snapshot.Mode, state.Snapshot.RemoteMode);
            Assert.Equal($"command-{higherVersion}", Assert.Single(snapshot.CommandTypes));
        }

        static void TryPublish(DynamicConfigurationState configuration, DynamicConfigurationSnapshot snapshot, AgentRuntimeState state)
        {
            try { configuration.Publish(snapshot, state); }
            catch (InvalidDataException) { }
        }
    }

    [Fact]
    public void Prepared_snapshot_is_immutable_and_independent_of_source_lists()
    {
        var commandTypes = new List<string> { "command-a" };
        var select = new List<string> { "Id" };
        var keyFields = new List<string> { "Id" };
        var entities = new List<EtlEntityDefinition>
        {
            new("synthetic", "Synthetic", "Id", null, null, select, "incremental", 10, 3, 1, 3, true, keyFields)
        };
        var remote = new RemoteAgentConfiguration { Mode = "Normal", CommandTypes = commandTypes, EtlEntities = entities, EtlIntervalMinutes = 60 };
        var prepared = NewConfiguration().Prepare(5, remote);

        commandTypes.Add("mutated-command");
        entities.Add(new EtlEntityDefinition("other", "Other", "Id", null, null, ["Id"], "incremental", 10, 3));
        select.Add("mutated-field");
        keyFields.Add("mutated-key");

        Assert.Equal(["command-a"], prepared.CommandTypes);
        var entity = Assert.Single(prepared.Entities);
        Assert.Equal("synthetic", entity.EntityCode);
        Assert.Equal(["Id"], entity.Select);
        var keys = Assert.IsAssignableFrom<IReadOnlyList<string>>(entity.KeyFields);
        Assert.Equal(["Id"], keys);
    }

    [Fact]
    public async Task Worker_ambiguous_case_alias_replay_cannot_promote_rejected_snapshot()
    {
        var previous = Remote("PauseCommands", "old-command");
        await PersistActiveAsync(7, previous);
        var ambiguousHash = Hash(AmbiguousNormalFirst);
        await _store.SaveConfigSnapshotAsync(8, AmbiguousNormalFirst, ambiguousHash, "rejected", CancellationToken.None);
        var state = ReadyState(7, previous, localPaused: false, maintenance: false);
        var configuration = NewConfiguration();
        configuration.Apply(7, previous, state);
        var control = new GatedStoreControl(_store);
        var erp = new FakeErp { Response = ResponseRaw(8, Ambiguous999First) };
        using var sessions = new ErpSessionManager(erp, AgentOptions("ambiguous-replay"), state);
        using var worker = new ConfigurationWorker(erp, GatedStoreProxy.Create(_store, control), sessions, configuration, state, NullLogger<ConfigurationWorker>.Instance);

        try
        {
            await worker.StartAsync(CancellationToken.None);
            await control.SaveCompleted.Task.WaitAsync(WaitTimeout);
        }
        finally
        {
            await worker.StopAsync(CancellationToken.None).WaitAsync(WaitTimeout);
        }

        var active = await _store.GetActiveConfigSnapshotAsync(CancellationToken.None);
        Assert.NotNull(active);
        Assert.Equal(7, active.Version);
        Assert.Equal(Hash(Serialize(previous)), active.Hash);
        Assert.Equal(7, configuration.Version);
        Assert.Equal(AgentMode.PauseCommands, state.Snapshot.RemoteMode);
        var row = await new SqliteRowReader(_factory).ReadAsync(8);
        Assert.Equal("rejected", row.Status);
    }

    [Fact]
    public async Task Ambiguous_stored_validated_row_cannot_be_activated()
    {
        var active = Remote("Normal", "active-command");
        await PersistActiveAsync(7, active);
        await InsertConfigRowSqlAsync(8, AmbiguousNormalFirst, Hash(AmbiguousNormalFirst), "validated");

        await Assert.ThrowsAsync<InvalidDataException>(() => _store.ActivateConfigSnapshotAsync(8, CancellationToken.None));

        var restored = await _store.GetActiveConfigSnapshotAsync(CancellationToken.None);
        Assert.NotNull(restored);
        Assert.Equal(7, restored.Version);
        Assert.Equal(1, await CountActiveAsync());
    }

    [Fact]
    public async Task Ambiguous_persisted_body_fails_closed_during_restart()
    {
        await InsertConfigRowSqlAsync(9, Ambiguous999First, Hash(Ambiguous999First), "active");
        var state = new AgentRuntimeState();
        var configuration = NewConfiguration();
        using var controller = new LocalEtlPauseController(_store, state);
        var instanceLock = new SingleInstanceLock();
        var bootstrap = CreateBootstrap(state, configuration, controller, instanceLock, "ambiguous-body");

        try
        {
            await Assert.ThrowsAsync<InvalidDataException>(() => bootstrap.StartAsync(CancellationToken.None));
            Assert.False(state.IsReady);
        }
        finally
        {
            await bootstrap.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task Canonical_reorder_same_version_replay_is_accepted()
    {
        const string storedJson = "{\"mode\":\"Normal\",\"commandTypes\":[\"cmd-a\"],\"etlEntities\":[],\"etlIntervalMinutes\":60}";
        const string reorderedJson = "{\n  \"etlIntervalMinutes\": 60,\n  \"etlEntities\": [],\n  \"commandTypes\": [\"cmd-a\"],\n  \"mode\": \"Normal\"\n}";
        var hash = Hash(storedJson);
        Assert.Equal(hash, Hash(reorderedJson));
        await PersistActiveSnapshotAsync(80, storedJson, hash);

        var replayed = await _store.AcceptAndActivateConfigSnapshotAsync(80, reorderedJson, hash, CancellationToken.None);

        Assert.Equal(80, replayed.Version);
        Assert.Equal(storedJson, replayed.ConfigurationJson);
        Assert.Equal(hash, replayed.Hash);
        var active = await _store.GetActiveConfigSnapshotAsync(CancellationToken.None);
        Assert.NotNull(active);
        Assert.Equal(80, active.Version);
        Assert.Equal(1, await CountActiveAsync());
    }

    private const string AmbiguousNormalFirst =
        "{\"Mode\":\"Normal\",\"mode\":\"999\",\"commandTypes\":[\"x\"],\"etlEntities\":[],\"etlIntervalMinutes\":60}";

    private const string Ambiguous999First =
        "{\"mode\":\"999\",\"Mode\":\"Normal\",\"commandTypes\":[\"x\"],\"etlEntities\":[],\"etlIntervalMinutes\":60}";

    private static async Task AcceptIgnoringRollbackAsync(SqliteAgentStore store, long version, string json, string hash)
    {
        try { await store.AcceptAndActivateConfigSnapshotAsync(version, json, hash, CancellationToken.None); }
        catch (InvalidDataException) { }
    }

    private async Task<int> CountActiveAsync()
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT COUNT(*) FROM config_snapshots WHERE status='active';";
        return Convert.ToInt32(await command.ExecuteScalarAsync(CancellationToken.None), CultureInfo.InvariantCulture);
    }

    private async Task InsertConfigRowSqlAsync(long version, string json, string hash, string status)
    {
        var escaped = json.Replace("'", "''");
        await ExecuteSqlAsync($"INSERT INTO config_snapshots(config_version,config_json,config_hash,received_at_utc,status) VALUES({version},'{escaped}','{hash}','2026-09-24T00:00:00.0000000+00:00','{status}');");
    }

    private async Task PersistActiveAsync(long version, RemoteAgentConfiguration configuration)
    {
        var json = Serialize(configuration);
        await PersistActiveSnapshotAsync(version, json, Hash(json));
    }

    private async Task PersistActiveSnapshotAsync(long version, string json, string hash)
    {
        await _store.SaveConfigSnapshotAsync(version, json, hash, "validated", CancellationToken.None);
        await _store.ActivateConfigSnapshotAsync(version, CancellationToken.None);
    }

    private async Task ExecuteSqlAsync(string sql)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync(CancellationToken.None);
    }

    private BootstrapService CreateBootstrap(
        AgentRuntimeState state,
        DynamicConfigurationState configuration,
        LocalEtlPauseController controller,
        SingleInstanceLock instanceLock,
        string suffix) => new(
            _store,
            new FakeSpoolStore(),
            new FakeSecretStore(),
            instanceLock,
            state,
            configuration,
            controller,
            AgentOptions(suffix),
            Options.Create(new ErpOptions { RequireClientCertificate = false }),
            Options.Create(new OnecOptions { CredentialSecretName = "test-secret" }),
            NullLogger<BootstrapService>.Instance);

    private static AgentRuntimeState ReadyState(long version, RemoteAgentConfiguration configuration, bool localPaused, bool maintenance)
    {
        var state = new AgentRuntimeState();
        NewConfiguration().Apply(version, configuration, state);
        state.RestoreLocalEtlPause(localPaused);
        state.SetHandshakeMaintenance(maintenance);
        state.CompleteBootstrap();
        return state;
    }

    private static DynamicConfigurationState NewConfiguration() => new(
        Options.Create(new CommandOptions { SupportedTypes = ["default-command"] }),
        Options.Create(new EtlOptions { Entities = [], IntervalMinutes = 60 }));

    private static RemoteAgentConfiguration Remote(string mode, string commandType) => new()
    {
        Mode = mode,
        CommandTypes = [commandType],
        EtlIntervalMinutes = 60,
        EtlEntities = [new EtlEntityDefinition("synthetic", "Synthetic", "Id", null, null, ["Id"], "incremental", 10, 3)]
    };

    private static RemoteConfigurationResponse Response(long version, RemoteAgentConfiguration configuration)
    {
        var json = Serialize(configuration);
        using var document = JsonDocument.Parse(json);
        return new(version, Hash(json), document.RootElement.Clone());
    }

    private static RemoteConfigurationResponse ResponseRaw(long version, string json)
    {
        using var document = JsonDocument.Parse(json);
        return new(version, Hash(json), document.RootElement.Clone());
    }

    private static string Serialize(RemoteAgentConfiguration configuration) => JsonSerializer.Serialize(configuration, JsonOptions);

    private static string Hash(string json)
    {
        using var document = JsonDocument.Parse(json);
        return PayloadHasher.Compute(document.RootElement);
    }

    private static IOptions<AgentOptions> AgentOptions(string suffix) => Options.Create(new AgentOptions
    {
        AgentId = $"configuration-activation-{suffix}-{Guid.NewGuid():N}",
        SiteId = "configuration-activation-site",
        DataDirectory = Path.GetTempPath()
    });

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        var deadline = DateTimeOffset.UtcNow.Add(WaitTimeout);
        while (!condition())
        {
            if (DateTimeOffset.UtcNow >= deadline) throw new TimeoutException("Condition did not become true.");
            await Task.Delay(10);
        }
    }

    private sealed class FakeErp : IErpClient
    {
        public RemoteConfigurationResponse? Response { get; init; }
        public bool MaintenanceMode { get; init; }
        public TaskCompletionSource<bool> ConfigurationRead { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public int ConfigurationCalls { get; private set; }

        public Task<SessionStartResponse> StartSessionAsync(SessionStartRequest request, CancellationToken cancellationToken) =>
            Task.FromResult(new SessionStartResponse(Guid.NewGuid(), DateTimeOffset.UtcNow, true, "1.0.0", 0, MaintenanceMode));

        public Task<RemoteConfigurationResponse?> GetConfigurationAsync(long currentVersion, CancellationToken cancellationToken)
        {
            ConfigurationCalls++;
            ConfigurationRead.TrySetResult(true);
            return Task.FromResult(Response);
        }

        public Task<LeaseResponse> LeaseCommandAsync(LeaseRequest request, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task AcknowledgeReceivedAsync(Guid commandId, CommandReceivedRequest request, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task AcknowledgeResultAsync(Guid commandId, string resultJson, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task<BatchAcknowledgement> UploadBatchAsync(EtlBatch batch, Stream content, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task CompleteEtlRunAsync(Guid runId, object summary, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task SendHeartbeatAsync(HeartbeatRequest request, CancellationToken cancellationToken) => throw new NotSupportedException();
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

    public sealed class ActivationFailureControl(IAgentStore inner)
    {
        public IAgentStore Inner { get; } = inner;
        public TaskCompletionSource<bool> FailureTriggered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource<bool> RejectionSaveCompleted { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    public class ActivationFailureStoreProxy : DispatchProxy
    {
        private static readonly ConditionalWeakTable<DispatchProxy, ActivationFailureControl> Controls = new();

        public static IAgentStore Create(IAgentStore inner, ActivationFailureControl control)
        {
            var proxy = DispatchProxy.Create<IAgentStore, ActivationFailureStoreProxy>();
            Controls.Add((DispatchProxy)(object)proxy, control);
            return proxy;
        }

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            if (!Controls.TryGetValue(this, out var control)) throw new InvalidOperationException("Proxy state is missing.");
            if (targetMethod?.Name is "ActivateConfigSnapshotAsync" or "AcceptAndActivateConfigSnapshotAsync") return FailActivation(control);
            if (targetMethod?.Name == nameof(IAgentStore.SaveConfigSnapshotAsync)) return SaveAsync(control, targetMethod, args);
            return targetMethod!.Invoke(control.Inner, args);
        }

        private static Task FailActivation(ActivationFailureControl control)
        {
            control.FailureTriggered.TrySetResult(true);
            throw new InvalidOperationException("Injected configuration activation failure.");
        }

        private static async Task SaveAsync(ActivationFailureControl control, MethodInfo targetMethod, object?[]? args)
        {
            await (Task)targetMethod.Invoke(control.Inner, args)!;
            if (control.FailureTriggered.Task.IsCompleted) control.RejectionSaveCompleted.TrySetResult(true);
        }
    }

    public sealed class GatedStoreControl(IAgentStore inner)
    {
        public IAgentStore Inner { get; } = inner;
        public bool GateAfterCommit { get; init; }
        public TaskCompletionSource<bool> CommitCompleted { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource<bool> ReleaseAfterCommit { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource<bool> AcceptCompleted { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource<bool> SaveCompleted { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    public class GatedStoreProxy : DispatchProxy
    {
        private static readonly ConditionalWeakTable<DispatchProxy, GatedStoreControl> Controls = new();

        public static IAgentStore Create(IAgentStore inner, GatedStoreControl control)
        {
            var proxy = DispatchProxy.Create<IAgentStore, GatedStoreProxy>();
            Controls.Add((DispatchProxy)(object)proxy, control);
            return proxy;
        }

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            if (!Controls.TryGetValue(this, out var control)) throw new InvalidOperationException("Proxy state is missing.");
            if (targetMethod?.Name == nameof(IAgentStore.AcceptAndActivateConfigSnapshotAsync)) return AcceptAsync(control, targetMethod, args);
            if (targetMethod?.Name == nameof(IAgentStore.SaveConfigSnapshotAsync)) return SaveAsync(control, targetMethod, args);
            return targetMethod!.Invoke(control.Inner, args);
        }

        private static async Task SaveAsync(GatedStoreControl control, MethodInfo targetMethod, object?[]? args)
        {
            await (Task)targetMethod.Invoke(control.Inner, args)!;
            control.SaveCompleted.TrySetResult(true);
        }

        private static async Task<ConfigSnapshot> AcceptAsync(GatedStoreControl control, MethodInfo targetMethod, object?[]? args)
        {
            var accepted = await (Task<ConfigSnapshot>)targetMethod.Invoke(control.Inner, args)!;
            control.AcceptCompleted.TrySetResult(true);
            if (control.GateAfterCommit)
            {
                control.CommitCompleted.TrySetResult(true);
                await control.ReleaseAfterCommit.Task.WaitAsync(TimeSpan.FromSeconds(30)).ConfigureAwait(false);
            }
            return accepted;
        }
    }

    private sealed class SqliteRowReader(SqliteConnectionFactory factory)
    {
        public async Task<(string Json, string Hash, string Status)> ReadAsync(long version)
        {
            await using var connection = await factory.OpenAsync(CancellationToken.None);
            await using var command = connection.CreateCommand();
            command.CommandText = "SELECT config_json,config_hash,status FROM config_snapshots WHERE config_version=$version;";
            command.Parameters.AddWithValue("$version", version);
            await using var reader = await command.ExecuteReaderAsync(CancellationToken.None);
            Assert.True(await reader.ReadAsync(CancellationToken.None));
            return (reader.GetString(0), reader.GetString(1), reader.GetString(2));
        }
    }
}
