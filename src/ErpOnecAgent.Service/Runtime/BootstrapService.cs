using System.Text.Json;
using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Application.Configuration;
using ErpOnecAgent.Domain.Agent;
using ErpOnecAgent.Domain.Common;
using ErpOnecAgent.Infrastructure.Persistence.Sqlite;
using ErpOnecAgent.Infrastructure.Security;
using Microsoft.Extensions.Options;

namespace ErpOnecAgent.Service.Runtime;

public sealed class BootstrapService(
    IAgentStore store,
    ISpoolStore spool,
    ISecretStore secrets,
    SingleInstanceLock instanceLock,
    AgentRuntimeState runtime,
    DynamicConfigurationState configuration,
    LocalEtlPauseController localPause,
    IOptions<AgentOptions> agentOptions,
    IOptions<ErpOptions> erpOptions,
    IOptions<OnecOptions> onecOptions,
    ILogger<BootstrapService> logger,
    SqliteConnectionFactory? database = null) : IHostedService
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web) { PropertyNameCaseInsensitive = true };

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        instanceLock.Acquire(agentOptions.Value.AgentId);
        if (erpOptions.Value.RequireClientCertificate)
        {
            using var certificate = CertificateLoader.LoadClientCertificate(erpOptions.Value.ClientCertificateThumbprint);
        }
        if (await secrets.ReadAsync(onecOptions.Value.CredentialSecretName, cancellationToken).ConfigureAwait(false) is null)
            throw new InvalidOperationException($"1C credential '{onecOptions.Value.CredentialSecretName}' is not configured. Run --store-onec-credential interactively before starting the service.");
        // The database-presence guard runs whenever the store is backed by a file database
        // (always in the service); in-process test hosts that inject a store without the
        // factory skip it.
        if (database is not null && DatabasePresenceGuard.CheckBeforeStart(database.DatabasePath, Path.Combine(agentOptions.Value.DataDirectory, "spool")) is { } missing)
            throw new InvalidOperationException(missing);
        await store.InitializeAsync(cancellationToken).ConfigureAwait(false);
        var integrity = await store.IntegrityCheckAsync(cancellationToken).ConfigureAwait(false);
        if (!string.Equals(integrity, "ok", StringComparison.OrdinalIgnoreCase)) throw new InvalidDataException($"SQLite integrity check failed: {integrity}");
        if (database is not null) DatabasePresenceGuard.MarkInitialized(database.DatabasePath);
        await store.RecoverAsync(cancellationToken).ConfigureAwait(false);
        await spool.QuarantineTemporaryFilesAsync(cancellationToken).ConfigureAwait(false);
        await localPause.RestoreAsync(cancellationToken).ConfigureAwait(false);
        var active = await store.GetActiveConfigSnapshotAsync(cancellationToken).ConfigureAwait(false);
        if (active is not null)
        {
            RemoteAgentConfiguration restored;
            try
            {
                using var document = JsonDocument.Parse(active.ConfigurationJson);
                JsonAmbiguityGuard.EnsureUnambiguous(document.RootElement);
                if (!PayloadHasher.Matches(document.RootElement, active.Hash)) throw new InvalidDataException("Active remote configuration hash mismatch.");
                restored = document.RootElement.Deserialize<RemoteAgentConfiguration>(JsonOptions)
                    ?? throw new InvalidDataException("Active remote configuration is invalid.");
            }
            catch (JsonException ex)
            {
                throw new InvalidDataException("Active remote configuration body is invalid.", ex);
            }
            var prepared = configuration.Prepare(active.Version, restored);
            configuration.Publish(prepared, runtime);
        }
        else
        {
            runtime.SetRemoteMode(AgentMode.Normal);
        }
        runtime.CompleteBootstrap();
        logger.LogInformation("AGENT_STARTED AgentId={AgentId} SiteId={SiteId} Version={Version}", agentOptions.Value.AgentId, agentOptions.Value.SiteId, ThisAssembly.Version);
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("AGENT_STOPPED AgentId={AgentId}", agentOptions.Value.AgentId);
        instanceLock.Dispose();
        return Task.CompletedTask;
    }
}
