using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Application.Commands;
using ErpOnecAgent.Application.Configuration;
using ErpOnecAgent.Service.Runtime;
using Microsoft.Extensions.Options;

namespace ErpOnecAgent.Service.Workers;

public sealed class ResultDeliveryWorker(IAgentStore store, IErpClient erp, AgentRuntimeState state, ILogger<ResultDeliveryWorker> logger, CommandWorkSignals? signals = null,
    IOptions<CommandOptions>? commandOptions = null, IOptions<OnecOptions>? onecOptions = null) : BackgroundService
{
    // Fallback re-check: retry times and rows written without a signal.
    private static readonly TimeSpan IdleRecheck = TimeSpan.FromMilliseconds(500);
    private readonly CommandWorkSignals _signals = signals ?? new CommandWorkSignals();
    private readonly TimeSpan? _probeHold = ProbeHold(commandOptions?.Value, onecOptions?.Value);

    internal const string ProbeCommandType = "integration_probe";
    internal const int MaxProbeHoldSeconds = 600;

    // The stage-only redelivery test switch: honoured only against a test source binding.
    internal static TimeSpan? ProbeHold(CommandOptions? commands, OnecOptions? onec) =>
        commands?.TestHoldProbeResultSeconds is > 0 and var seconds
        && string.Equals(onec?.SourceBinding?.Environment, "test", StringComparison.Ordinal)
            ? TimeSpan.FromSeconds(Math.Min(seconds, MaxProbeHoldSeconds))
            : null;

    public override Task StartAsync(CancellationToken cancellationToken)
    {
        if (commandOptions?.Value.TestHoldProbeResultSeconds > 0 && _probeHold is null)
            logger.LogError("TEST_RESULT_HOLD_IGNORED — Commands:TestHoldProbeResultSeconds is set but the source binding is not 'test'; results are delivered normally");
        else if (_probeHold is { } hold)
            logger.LogWarning("TEST_RESULT_HOLD_ENABLED Seconds={Seconds} — the first delivery of every {CommandType} result is postponed (stage redelivery test)", hold.TotalSeconds, ProbeCommandType);
        return base.StartAsync(cancellationToken);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            if (!state.Snapshot.CanDeliverResults)
            {
                await Task.Delay(250, stoppingToken).ConfigureAwait(false);
                continue;
            }
            var results = await store.GetPendingResultsAsync(25, DateTimeOffset.UtcNow, stoppingToken).ConfigureAwait(false);
            if (results.Count == 0) { await _signals.Results.WaitAsync(IdleRecheck, stoppingToken).ConfigureAwait(false); continue; }
            foreach (var result in results)
            {
                if (_probeHold is { } hold && result.AttemptCount == 0 && string.Equals(result.CommandType, ProbeCommandType, StringComparison.Ordinal))
                {
                    // Durable, once: the retry schedule carries the hold and counts as the first attempt.
                    if (await RecordAsync(() => store.MarkResultRetryAsync(result.CommandId, "TEST_RESULT_HOLD", DateTimeOffset.UtcNow + hold, stoppingToken), result.CommandId).ConfigureAwait(false))
                        logger.LogWarning("COMMAND_RESULT_HELD_FOR_TEST CommandId={CommandId} Seconds={Seconds}", result.CommandId, hold.TotalSeconds);
                    continue;
                }
                try
                {
                    await erp.AcknowledgeResultAsync(result.CommandId, result.PayloadJson, stoppingToken).ConfigureAwait(false);
                    if (await store.AcknowledgeResultAsync(result.CommandId, DateTimeOffset.UtcNow, stoppingToken).ConfigureAwait(false))
                    {
                        logger.LogInformation("COMMAND_RESULT_ACKNOWLEDGED CommandId={CommandId}", result.CommandId);
                    }
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { return; }
                catch (ErpApiException ex) when (ex.Is(System.Net.HttpStatusCode.Conflict, ErpApiException.ResultConflict))
                {
                    // ERP holds a DIFFERENT result for this command: re-sending the same bytes can
                    // never succeed. Stop delivery, keep the evidence, and raise it loudly.
                    logger.LogCritical("RESULT_CONFLICT CommandId={CommandId} — ERP already holds a different result; delivery stopped, operator action required", result.CommandId);
                    await RecordAsync(() => store.MarkResultConflictAsync(result.CommandId, ErpApiException.ResultConflict, stoppingToken), result.CommandId).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    var retryAt = DateTimeOffset.UtcNow + CommandPolicy.RetryDelay(result.AttemptCount + 1);
                    if (await RecordAsync(() => store.MarkResultRetryAsync(result.CommandId, ex.Message, retryAt, stoppingToken), result.CommandId).ConfigureAwait(false))
                    {
                        logger.LogWarning(ex, "Result delivery failed CommandId={CommandId} RetryAt={RetryAt}", result.CommandId, retryAt);
                    }
                }
            }
        }
    }

    // A failed store write (e.g. a busy database) must not stop the worker: the row keeps its
    // state and the next pass handles it again.
    private async Task<bool> RecordAsync(Func<Task<bool>> write, Guid commandId)
    {
        try
        {
            return await write().ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogError(ex, "RESULT_DELIVERY_STATE_WRITE_FAILED CommandId={CommandId}", commandId);
            return false;
        }
    }
}
