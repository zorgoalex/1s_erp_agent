using System.Text.Json;
using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Application.Configuration;
using ErpOnecAgent.Domain.Common;
using ErpOnecAgent.Service.Runtime;

namespace ErpOnecAgent.Service.Workers;

public sealed class ConfigurationWorker(IErpClient erp, IAgentStore store, ErpSessionManager sessions, DynamicConfigurationState configuration, AgentRuntimeState runtime, ILogger<ConfigurationWorker> logger) : BackgroundService
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web) { PropertyNameCaseInsensitive = true };

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            if (!runtime.IsReady)
            {
                await Task.Delay(250, stoppingToken).ConfigureAwait(false);
                continue;
            }
            RemoteConfigurationResponseHolder? pending = null;
            try
            {
                await sessions.GetSessionAsync(stoppingToken).ConfigureAwait(false);
                var current = configuration.Snapshot;
                var response = await erp.GetConfigurationAsync(current.Version, stoppingToken).ConfigureAwait(false);
                if (response is not null)
                {
                    var json = response.Configuration.GetRawText();
                    pending = new(response.ConfigVersion, response.ConfigHash, json);
                    if (!PayloadHasher.Matches(response.Configuration, response.ConfigHash)) throw new InvalidDataException("Remote configuration hash mismatch.");
                    JsonAmbiguityGuard.EnsureUnambiguous(response.Configuration);
                    var parsed = response.Configuration.Deserialize<RemoteAgentConfiguration>(JsonOptions) ?? throw new InvalidDataException("Remote configuration body is empty.");
                    var prepared = configuration.Prepare(response.ConfigVersion, parsed);
                    var accepted = await store.AcceptAndActivateConfigSnapshotAsync(prepared.Version, json, response.ConfigHash, CancellationToken.None).ConfigureAwait(false);
                    var persistedJson = accepted.ConfigurationJson;
                    using var persistedDocument = JsonDocument.Parse(persistedJson);
                    if (!PayloadHasher.Matches(persistedDocument.RootElement, accepted.Hash)) throw new InvalidDataException("Persisted remote configuration hash mismatch.");
                    JsonAmbiguityGuard.EnsureUnambiguous(persistedDocument.RootElement);
                    var persisted = persistedDocument.RootElement.Deserialize<RemoteAgentConfiguration>(JsonOptions) ?? throw new InvalidDataException("Persisted remote configuration body is empty.");
                    var persistedPrepared = configuration.Prepare(accepted.Version, persisted);
                    configuration.Publish(persistedPrepared, runtime);
                    logger.LogInformation("CONFIG_ACTIVATED Version={ConfigVersion}", accepted.Version);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { return; }
            catch (Exception ex)
            {
                if (pending is not null)
                {
                    try { await store.SaveConfigSnapshotAsync(pending.Version, pending.Json, pending.Hash, "rejected", CancellationToken.None).ConfigureAwait(false); }
                    catch (Exception snapshotError) { logger.LogError(snapshotError, "Failed to persist rejected remote configuration Version={ConfigVersion}", pending.Version); }
                }
                if (pending is not null) configuration.RecordRejection(pending.Version, RejectionReason(ex));
                // Nothing was received (transport or HTTP failure): not a rejection, nothing is recorded.
                if (pending is null) logger.LogWarning(ex, "CONFIG_FETCH_FAILED");
                else logger.LogWarning(ex, "CONFIG_REJECTED Version={ConfigVersion} Reason={Reason}", pending.Version, RejectionReason(ex));
            }
            await Task.Delay(TimeSpan.FromMinutes(1), stoppingToken).ConfigureAwait(false);
        }
    }

    private sealed record RemoteConfigurationResponseHolder(long Version, string Hash, string Json);

    // E4: a stable code for ERP; exception text can carry configuration details and is never sent.
    internal static string RejectionReason(Exception ex) => ex switch
    {
        JsonException => "CONFIG_JSON_INVALID",
        InvalidDataException when ex.Message.Contains("hash mismatch", StringComparison.OrdinalIgnoreCase) => "CONFIG_HASH_MISMATCH",
        InvalidDataException when ex.Message.Contains("rollback", StringComparison.OrdinalIgnoreCase) => "CONFIG_VERSION_ROLLBACK",
        InvalidDataException => "CONFIG_INVALID",
        _ => "CONFIG_APPLY_FAILED"
    };
}
