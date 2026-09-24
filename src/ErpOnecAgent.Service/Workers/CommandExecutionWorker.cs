using System.Globalization;
using System.Text.Json;
using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Application.Commands;
using ErpOnecAgent.Application.Configuration;
using ErpOnecAgent.Domain.Commands;
using ErpOnecAgent.Service.Diagnostics;
using ErpOnecAgent.Service.Runtime;
using Microsoft.Extensions.Options;

namespace ErpOnecAgent.Service.Workers;

public sealed class CommandExecutionWorker(
    IAgentStore store,
    IOnecCommandClient onec,
    IOnecHealthClient health,
    EtlTrigger etlTrigger,
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
        executorId: CommandExecutionIdentity.NewOwnerId());

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
                var entities = command.Payload.TryGetProperty("entities", out var list) && list.ValueKind == JsonValueKind.Array ? list.EnumerateArray().Select(static item => item.GetString()).Where(static item => item is not null).Cast<string>().ToArray() : null;
                if (!etlTrigger.TryWrite(new("bootstrap_full", entities))) { await SaveErrorAsync(command, claimOwner, CommandStatus.BusinessFailedLocal, "ETL_TRIGGER_QUEUE_FULL", "ETL trigger queue is full.", true, cancellationToken).ConfigureAwait(false); return true; }
                data = new { accepted = true, mode = "bootstrap_full" }; break;
            case "reload_entity":
                if (!command.Payload.TryGetProperty("entity", out var entityValue) || string.IsNullOrWhiteSpace(entityValue.GetString())) { await SaveErrorAsync(command, claimOwner, CommandStatus.BusinessFailedLocal, "ENTITY_REQUIRED", "payload.entity is required.", false, cancellationToken).ConfigureAwait(false); return true; }
                var entity = entityValue.GetString()!;
                if (!etlTrigger.TryWrite(new("entity_reload", [entity]))) { await SaveErrorAsync(command, claimOwner, CommandStatus.BusinessFailedLocal, "ETL_TRIGGER_QUEUE_FULL", "ETL trigger queue is full.", true, cancellationToken).ConfigureAwait(false); return true; }
                data = new { accepted = true, mode = "entity_reload", entity }; break;
            case "reconcile_keys":
            case "reconcile_totals":
                if (!etlTrigger.TryWrite(new(command.CommandType, null))) { await SaveErrorAsync(command, claimOwner, CommandStatus.BusinessFailedLocal, "ETL_TRIGGER_QUEUE_FULL", "ETL trigger queue is full.", true, cancellationToken).ConfigureAwait(false); return true; }
                data = new { accepted = true, mode = command.CommandType }; break;
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

    private async Task SaveErrorAsync(CommandEnvelope command, string claimOwner, CommandStatus status, string code, string message, bool retryable, CancellationToken cancellationToken)
    {
        var result = JsonSerializer.Serialize(new { commandId = command.CommandId, status = status == CommandStatus.Expired ? "expired" : status == CommandStatus.DeadLetter ? "dead_letter" : "business_error", completedAtUtc = DateTimeOffset.UtcNow, error = new { code, message, retryable, details = new { } }, resultVersion = 1 }, JsonOptions);
        var changed = await store.CompleteLocallyAsync(command.CommandId, claimOwner, status, result, null, null, null, cancellationToken).ConfigureAwait(false);
        if (changed) logger.LogWarning("COMMAND_BUSINESS_FAILED CommandId={CommandId} Code={ErrorCode}", command.CommandId, code);
    }
}
