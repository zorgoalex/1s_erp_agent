using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Application.Commands;
using ErpOnecAgent.Service.Runtime;

namespace ErpOnecAgent.Service.Workers;

public sealed class ResultDeliveryWorker(IAgentStore store, IErpClient erp, AgentRuntimeState state, ILogger<ResultDeliveryWorker> logger) : BackgroundService
{
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
            if (results.Count == 0) { await Task.Delay(500, stoppingToken).ConfigureAwait(false); continue; }
            foreach (var result in results)
            {
                try
                {
                    await erp.AcknowledgeResultAsync(result.CommandId, result.PayloadJson, stoppingToken).ConfigureAwait(false);
                    if (await store.AcknowledgeResultAsync(result.CommandId, DateTimeOffset.UtcNow, stoppingToken).ConfigureAwait(false))
                    {
                        logger.LogInformation("COMMAND_RESULT_ACKNOWLEDGED CommandId={CommandId}", result.CommandId);
                    }
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { return; }
                catch (Exception ex)
                {
                    var retryAt = DateTimeOffset.UtcNow + CommandPolicy.RetryDelay(result.AttemptCount + 1);
                    if (await store.MarkResultRetryAsync(result.CommandId, ex.Message, retryAt, stoppingToken).ConfigureAwait(false))
                    {
                        logger.LogWarning(ex, "Result delivery failed CommandId={CommandId} RetryAt={RetryAt}", result.CommandId, retryAt);
                    }
                }
            }
        }
    }
}
