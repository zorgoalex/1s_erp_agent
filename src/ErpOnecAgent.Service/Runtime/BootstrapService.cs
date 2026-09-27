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
        // E5: the separate command credential, when configured, must exist too.
        var commandSecret = onecOptions.Value.EffectiveCommandCredentialSecretName;
        if (!string.Equals(commandSecret, onecOptions.Value.CredentialSecretName, StringComparison.Ordinal)
            && await secrets.ReadAsync(commandSecret, cancellationToken).ConfigureAwait(false) is null)
            throw new InvalidOperationException($"1C command credential '{commandSecret}' is not configured. Run --store-onec-credential --purpose command interactively before starting the service.");
        if (string.Equals(commandSecret, onecOptions.Value.CredentialSecretName, StringComparison.Ordinal))
            logger.LogWarning("ONEC_SINGLE_CREDENTIAL — OData and commands use one 1C credential '{Secret}'; configure OneC:CommandCredentialSecretName so the OData user stays read-only", commandSecret);
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
        // C1 startup recovery of the durable ETL path (exclusive host, before any worker runs):
        // fence legacy runs first (so an interrupted legacy run is reported LEGACY_UNRESOLVED,
        // not INTERRUPTED), then orphan admitted sends, quarantine in-flight batches and block
        // interrupted runs, quarantine spool files no batch row references, and block runs
        // whose registered batch file is missing.
        var legacyBlocked = await store.BlockLegacyEtlRunsAsync(cancellationToken).ConfigureAwait(false);
        var recovery = await store.RecoverInterruptedEtlRunsAsync(cancellationToken).ConfigureAwait(false);
        await spool.QuarantineTemporaryFilesAsync(cancellationToken).ConfigureAwait(false);
        var orphans = await spool.QuarantineUnreferencedReadyFilesAsync(await store.GetReferencedBatchFilePathsAsync(cancellationToken).ConfigureAwait(false), cancellationToken).ConfigureAwait(false);
        var missingFiles = await store.BlockRunsWithMissingSpoolFilesAsync(File.Exists, cancellationToken).ConfigureAwait(false);
        if (recovery.RunsBlocked + recovery.BatchesFenced + recovery.AttemptsOrphaned + legacyBlocked + orphans + missingFiles > 0)
            logger.LogWarning("ETL_STARTUP_RECOVERY RunsBlocked={Runs} BatchesFenced={Batches} AttemptsOrphaned={Attempts} LegacyRunsBlocked={Legacy} OrphanSpoolFiles={Orphans} MissingSpoolFiles={Missing}",
                recovery.RunsBlocked, recovery.BatchesFenced, recovery.AttemptsOrphaned, legacyBlocked, orphans, missingFiles);
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
            // E3: a config activated by a pre-E3 agent may carry a token the old agent ignored.
            // Restoring it must not stop the service: an unusable token is dropped (runs then
            // go without one) until ERP sends a new configuration.
            if (restored.SourceGeneration is not null && !ErpOnecAgent.Application.Etl.SourceGenerationToken.IsValid(restored.SourceGeneration))
            {
                logger.LogWarning("CONFIG_SOURCE_GENERATION_IGNORED Version={ConfigVersion} — the restored token is not 1..128 printable ASCII; runs are created without a generation token", active.Version);
                restored = new RemoteAgentConfiguration { Mode = restored.Mode, CommandTypes = restored.CommandTypes, EtlEntities = restored.EtlEntities, EtlIntervalMinutes = restored.EtlIntervalMinutes };
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
