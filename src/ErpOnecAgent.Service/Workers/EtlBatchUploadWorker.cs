using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Application.Commands;
using ErpOnecAgent.Application.Configuration;
using ErpOnecAgent.Domain.Etl;
using ErpOnecAgent.Service.Runtime;
using Microsoft.Extensions.Options;

namespace ErpOnecAgent.Service.Workers;

public sealed class EtlBatchUploadWorker(IAgentStore store, ISpoolStore spool, IErpClient erp, AgentRuntimeState state, IOptions<EtlOptions> options, ILogger<EtlBatchUploadWorker> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            if (!state.Snapshot.CanUploadBatches)
            {
                await Task.Delay(250, stoppingToken).ConfigureAwait(false);
                continue;
            }
            var batches = await store.GetPendingBatchesAsync(options.Value.MaxConcurrentBatchUploads, DateTimeOffset.UtcNow, stoppingToken).ConfigureAwait(false);
            await Task.WhenAll(batches.Select(batch => UploadBatchAsync(batch, stoppingToken))).ConfigureAwait(false);

            foreach (var run in await store.GetRunsReadyToCompleteAsync(stoppingToken).ConfigureAwait(false))
            {
                try
                {
                    await erp.CompleteEtlRunAsync(run.RunId, new { runId = run.RunId, status = "succeeded", rowsRead = run.RowsRead, batchesCreated = run.BatchCount, batchesAcknowledged = run.BatchCount, completedAtUtc = DateTimeOffset.UtcNow }, stoppingToken).ConfigureAwait(false);
                    foreach (var watermark in run.Watermarks) await store.CommitWatermarkAsync(watermark.Key, watermark.Value, run.RunId, stoppingToken).ConfigureAwait(false);
                    await store.CompleteEtlRunAsync(run.RunId, EtlRunStatus.Succeeded, null, stoppingToken).ConfigureAwait(false);
                    state.LastEtlSuccessAtUtc = DateTimeOffset.UtcNow;
                    logger.LogInformation("ETL_RUN_SUCCEEDED RunId={RunId} Rows={Rows}", run.RunId, run.RowsRead);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { return; }
                catch (Exception ex) { logger.LogWarning(ex, "ETL run completion delivery failed RunId={RunId}", run.RunId); }
            }
            if (batches.Count == 0) await Task.Delay(750, stoppingToken).ConfigureAwait(false);
        }
    }

    private async Task UploadBatchAsync(EtlBatch batch, CancellationToken cancellationToken)
    {
        try
        {
            await using var content = await spool.OpenReadAsync(batch, cancellationToken).ConfigureAwait(false);
            var acknowledgement = await erp.UploadBatchAsync(batch, content, cancellationToken).ConfigureAwait(false);
            if (acknowledgement.BatchId != batch.BatchId || !acknowledgement.ChecksumValid || acknowledgement.RowsAccepted != batch.RowCount)
                throw new InvalidDataException("ERP ETL acknowledgement does not match the uploaded batch.");
            await store.AcknowledgeBatchAsync(batch.BatchId, acknowledgement.AcknowledgedAtUtc, cancellationToken).ConfigureAwait(false);
            logger.LogInformation("ETL_BATCH_ACKNOWLEDGED BatchId={BatchId} Rows={Rows}", batch.BatchId, batch.RowCount);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { }
        catch (Exception ex)
        {
            var retryAt = DateTimeOffset.UtcNow + CommandPolicy.RetryDelay(batch.AttemptCount + 1);
            await store.MarkBatchRetryAsync(batch.BatchId, ex.Message, retryAt, CancellationToken.None).ConfigureAwait(false);
            logger.LogWarning(ex, "ETL batch upload failed BatchId={BatchId} RetryAt={RetryAt}", batch.BatchId, retryAt);
        }
    }
}
