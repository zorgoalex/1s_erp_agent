using System.Globalization;
using System.Text.Json;
using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Application.Commands;
using ErpOnecAgent.Application.Configuration;
using ErpOnecAgent.Domain.Commands;
using ErpOnecAgent.Domain.Etl;
using ErpOnecAgent.Service.Diagnostics;
using ErpOnecAgent.Service.Runtime;
using Microsoft.Extensions.Options;

namespace ErpOnecAgent.Service.Workers;

public sealed class CommandExecutionWorker(
    IAgentStore store,
    IOnecCommandClient onec,
    IOnecHealthClient health,
    DynamicConfigurationState configuration,
    AgentRuntimeState state,
    LocalEtlPauseController localPause,
    DiagnosticsCollector diagnostics,
    IOptions<CommandOptions> options,
    ILogger<CommandExecutionWorker> logger) : BackgroundService
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);
    private readonly CommandExecutionService execution = new(
        store,
        onec,
        () => options.Value.MaxOperationalAttempts,
        new CommandExecutionHooks(
            OnExecutionStarted: (commandId, commandType, attempt) => logger.LogInformation("COMMAND_EXECUTION_STARTED CommandId={CommandId} Type={CommandType} Attempt={Attempt}", commandId, commandType, attempt),
            OnExecutionSucceeded: (commandId, externalRef) =>
            {
                state.LastOnecSuccessAtUtc = DateTimeOffset.UtcNow;
                logger.LogInformation("COMMAND_EXECUTION_SUCCEEDED CommandId={CommandId} ExternalRef={ExternalRef}", commandId, externalRef);
                return Task.CompletedTask;
            },
            OnBusinessFailed: (commandId, code) => logger.LogWarning("COMMAND_BUSINESS_FAILED CommandId={CommandId} Code={ErrorCode}", commandId, code)),
        () =>
        {
            var commandOptions = options.Value;
            return new CommandExecutionOptions
            {
                MaxLookupAttempts = commandOptions.MaxLookupAttempts,
                MaxPostAttempts = commandOptions.MaxPostAttempts,
                MaxResolutionAgeHours = commandOptions.MaxResolutionAgeHours,
                RetryBaseDelaySeconds = commandOptions.RetryBaseDelaySeconds,
                RetryMaxDelaySeconds = commandOptions.RetryMaxDelaySeconds
            };
        },
        executorId: CommandExecutionIdentity.NewOwnerId(),
        expiryNow: state.ExpiryNow);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            var snapshot = state.Snapshot;
            if (!snapshot.IsReady)
            {
                await Task.Delay(1000, stoppingToken).ConfigureAwait(false); continue;
            }
            // A07b resolution/admission split: under a command restriction the worker selects only
            // rows with durable send evidence (sent-only query, filtered in SQL before LIMIT) so
            // already-sent work keeps resolving through status lookup while fresh work is never
            // fetched — no post-fetch filtering and no empty-batch spin. New-work admission is
            // decided per pass via mayStartNewWork (a documented decision point, not an atomic
            // mode+network gate; in-flight calls are never cancelled on a mode change).
            var commands = snapshot.CanExecuteCommands
                ? await store.GetReadyCommandsAsync(options.Value.MaxConcurrency, DateTimeOffset.UtcNow, stoppingToken).ConfigureAwait(false)
                : snapshot.CanResolveCommandResults
                    ? await store.GetDueSentCommandsAsync(options.Value.MaxConcurrency, DateTimeOffset.UtcNow, stoppingToken).ConfigureAwait(false)
                    : [];
            if (commands.Count == 0) { await Task.Delay(250, stoppingToken).ConfigureAwait(false); continue; }
            await Task.WhenAll(commands.Select(command => ProcessAsync(command, stoppingToken))).ConfigureAwait(false);
        }
    }

    private async Task ProcessAsync(StoredCommand stored, CancellationToken cancellationToken)
    {
        using var executing = state.BeginCommandExecution();
        try
        {
            await execution.ProcessAsync(stored, TryExecuteAdministrativeAsync, cancellationToken, () => state.Snapshot.CanExecuteCommands).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { }
        catch (Exception ex)
        {
            logger.LogError(ex, "COMMAND_TECHNICAL_FAILED CommandId={CommandId}", stored.Envelope.CommandId);
            await store.MarkUnknownResultAsync(stored.Envelope.CommandId, "UNHANDLED_EXECUTION_ERROR", ex.Message, DateTimeOffset.UtcNow.AddSeconds(2), CancellationToken.None).ConfigureAwait(false);
        }
    }

    private async Task<bool> TryExecuteAdministrativeAsync(CommandEnvelope command, string claimOwner, CancellationToken cancellationToken)
    {
        object? data = null;
        switch (command.CommandType)
        {
            case "start_full_sync":
            {
                var requested = command.Payload.TryGetProperty("entities", out var list) && list.ValueKind == JsonValueKind.Array
                    ? list.EnumerateArray().Select(static item => item.GetString()).Where(static item => item is not null).Cast<string>().ToArray()
                    : [];
                return await AcceptEtlJobAsync(command, claimOwner, "bootstrap_full", requested, cancellationToken).ConfigureAwait(false);
            }
            case "reload_entity":
                if (!command.Payload.TryGetProperty("entity", out var entityValue) || string.IsNullOrWhiteSpace(entityValue.GetString())) { await SaveErrorAsync(command, claimOwner, CommandStatus.BusinessFailedLocal, "ENTITY_REQUIRED", "payload.entity is required.", false, cancellationToken).ConfigureAwait(false); return true; }
                return await AcceptEtlJobAsync(command, claimOwner, "entity_reload", [entityValue.GetString()!], cancellationToken).ConfigureAwait(false);
            case "reconcile_keys":
            case "reconcile_totals":
                // Reconciliation modes are not implemented (A10); they are refused explicitly
                // instead of silently running a full read.
                await SaveErrorAsync(command, claimOwner, CommandStatus.BusinessFailedLocal, "ETL_MODE_UNSUPPORTED", $"'{command.CommandType}' is not supported by this agent version.", false, cancellationToken).ConfigureAwait(false);
                return true;
            case "pause_etl":
                await localPause.SetAsync(true, cancellationToken).ConfigureAwait(false);
                data = new { mode = state.EffectiveMode.ToString() };
                break;
            case "resume_etl":
                await localPause.SetAsync(false, cancellationToken).ConfigureAwait(false);
                data = new { mode = state.EffectiveMode.ToString() };
                break;
            case "collect_diagnostics": data = new { archivePath = await diagnostics.CollectAsync(cancellationToken).ConfigureAwait(false), containsPayload = false }; break;
            case "rotate_certificate_hint": data = new { accepted = true, administratorActionRequired = true }; break;
            case "run_connectivity_test":
                var status = await health.CheckAsync(cancellationToken).ConfigureAwait(false); data = new { onecAvailable = status is not null, onec = status }; break;
            default: return false;
        }
        var result = JsonSerializer.Serialize(new { commandId = command.CommandId, status = "succeeded", completedAtUtc = DateTimeOffset.UtcNow, data, warnings = Array.Empty<string>(), resultVersion = 1 }, JsonOptions);
        await store.CompleteLocallyAsync(command.CommandId, claimOwner, CommandStatus.SucceededLocal, result, null, null, null, cancellationToken).ConfigureAwait(false);
        return true;
    }

    // C1: a durable ETL job + pending run + the command's accepted result + outbox commit in ONE
    // transaction (A03a). The extraction worker dispatches the job from SQLite; nothing lives
    // only in RAM. An explicit entity list must name enabled configured entities; an empty
    // start_full_sync selects every enabled entity.
    private async Task<bool> AcceptEtlJobAsync(CommandEnvelope command, string claimOwner, string mode, string[] requested, CancellationToken cancellationToken)
    {
        var enabled = configuration.Entities.Where(static entity => entity.Enabled).ToArray();
        EtlEntityDefinition[] selected;
        if (requested.Length == 0) selected = enabled;
        else
        {
            var unknown = requested.Where(code => !enabled.Any(entity => entity.EntityCode == code)).ToArray();
            if (unknown.Length > 0)
            {
                await SaveErrorAsync(command, claimOwner, CommandStatus.BusinessFailedLocal, "ENTITY_UNKNOWN", $"Not an enabled configured entity: {string.Join(", ", unknown)}.", false, cancellationToken).ConfigureAwait(false);
                return true;
            }
            selected = requested.Distinct(StringComparer.Ordinal).Select(code => enabled.First(entity => entity.EntityCode == code)).ToArray();
        }
        if (selected.Length == 0)
        {
            await SaveErrorAsync(command, claimOwner, CommandStatus.BusinessFailedLocal, "NO_ENTITIES", "No enabled ETL entities are configured.", false, cancellationToken).ConfigureAwait(false);
            return true;
        }

        var outcome = await store.AcceptEtlJobAndCompleteCommandAsync(command.CommandId, claimOwner, new EtlJobAcceptanceRequest(mode, selected, configuration.Version), cancellationToken).ConfigureAwait(false);
        switch (outcome)
        {
            case EtlJobAcceptanceOutcome.Applied applied:
                logger.LogInformation("ETL_JOB_ACCEPTED CommandId={CommandId} JobId={JobId} RunId={RunId} Mode={Mode}", command.CommandId, applied.Job.JobId, applied.Job.RunId, mode);
                return true;
            case EtlJobAcceptanceOutcome.AlreadyAccepted:
                return true;
            case EtlJobAcceptanceOutcome.NotApplied { Reason: EtlJobAcceptanceRejection.TypeModeMismatch }:
                await SaveErrorAsync(command, claimOwner, CommandStatus.BusinessFailedLocal, "ENTITY_SELECTION_INVALID", "The resolved entity selection does not correspond to the command payload.", false, cancellationToken).ConfigureAwait(false);
                return true;
            case EtlJobAcceptanceOutcome.NotApplied notApplied:
                // Claim lost, not in the never-sent state, expired or conflicting: nothing was
                // written; the administrative fail-closed path records the uncertainty.
                throw new InvalidOperationException($"ETL job acceptance not applied: {notApplied.Reason}.");
            default:
                throw new InvalidOperationException("Unknown ETL job acceptance outcome.");
        }
    }

    private async Task SaveErrorAsync(CommandEnvelope command, string claimOwner, CommandStatus status, string code, string message, bool retryable, CancellationToken cancellationToken)
    {
        var result = JsonSerializer.Serialize(new { commandId = command.CommandId, status = status == CommandStatus.Expired ? "expired" : status == CommandStatus.DeadLetter ? "dead_letter" : "business_error", completedAtUtc = DateTimeOffset.UtcNow, error = new { code, message, retryable, details = new { } }, resultVersion = 1 }, JsonOptions);
        var changed = await store.CompleteLocallyAsync(command.CommandId, claimOwner, status, result, null, null, null, cancellationToken).ConfigureAwait(false);
        if (changed) logger.LogWarning("COMMAND_BUSINESS_FAILED CommandId={CommandId} Code={ErrorCode}", command.CommandId, code);
    }
}
