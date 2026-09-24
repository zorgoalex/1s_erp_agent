using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Application.Configuration;
using ErpOnecAgent.Contracts.Erp;
using ErpOnecAgent.Domain.Agent;
using ErpOnecAgent.Domain.Etl;
using ErpOnecAgent.Infrastructure.Persistence.Sqlite;
using ErpOnecAgent.Service.Runtime;
using Microsoft.Extensions.Options;
using Xunit;

namespace ErpOnecAgent.IntegrationTests;

public sealed class A07ModeStateRedTests : IAsyncLifetime
{
    private readonly SqliteTestDatabase _database = new();
    private SqliteAgentStore _store = null!;

    public async Task InitializeAsync()
    {
        var factory = _database.CreateFactory(Path.Combine("data", "red-agent.db"));
        _store = new(factory, new SqliteMigrator(factory));
        await _store.InitializeAsync(CancellationToken.None);
    }

    public async Task DisposeAsync() => await _database.DisposeAsync();

    [Fact]
    public async Task Remote_pause_survives_non_maintenance_handshake()
    {
        var state = new AgentRuntimeState();
        var configuration = new DynamicConfigurationState(Options.Create(new CommandOptions()), Options.Create(new EtlOptions()));
        configuration.Apply(1, Remote("PauseCommands"), state);
        state.CompleteBootstrap();
        using var sessions = new ErpSessionManager(new FakeErp { MaintenanceMode = false }, Options.Create(new AgentOptions { AgentId = "red-agent", SiteId = "red-site" }), state);

        await sessions.GetSessionAsync(CancellationToken.None);

        Assert.Equal(AgentMode.PauseCommands, state.Mode);
    }

    [Fact]
    public void Numeric_remote_mode_is_rejected_before_publication()
    {
        var state = new AgentRuntimeState();
        var configuration = new DynamicConfigurationState(Options.Create(new CommandOptions()), Options.Create(new EtlOptions()));

        Assert.Throws<InvalidDataException>(() => configuration.Apply(1, Remote("999"), state));
    }

    private static RemoteAgentConfiguration Remote(string mode) => new()
    {
        Mode = mode,
        CommandTypes = ["synthetic"],
        EtlIntervalMinutes = 60,
        EtlEntities = [new EtlEntityDefinition("synthetic", "Synthetic", "Id", null, null, ["Id"], "incremental", 10, 3)]
    };

    private sealed class FakeErp : IErpClient
    {
        public bool MaintenanceMode { get; init; }

        public Task<SessionStartResponse> StartSessionAsync(SessionStartRequest request, CancellationToken cancellationToken) =>
            Task.FromResult(new SessionStartResponse(Guid.NewGuid(), DateTimeOffset.UtcNow, true, "1.0.0", 0, MaintenanceMode));

        public Task<LeaseResponse> LeaseCommandAsync(LeaseRequest request, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task AcknowledgeReceivedAsync(Guid commandId, CommandReceivedRequest request, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task AcknowledgeResultAsync(Guid commandId, string resultJson, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task<BatchAcknowledgement> UploadBatchAsync(EtlBatch batch, Stream content, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task CompleteEtlRunAsync(Guid runId, object summary, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task SendHeartbeatAsync(HeartbeatRequest request, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task<RemoteConfigurationResponse?> GetConfigurationAsync(long currentVersion, CancellationToken cancellationToken) => Task.FromResult<RemoteConfigurationResponse?>(null);
    }
}
