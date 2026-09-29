using System.Net;
using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Service.Runtime;

namespace ErpOnecAgent.Service.Workers.Etl;

/// <summary>
/// A04b (agreed with ERP, agent-bridge to-onec/0049): sends the queued closing complete of an
/// interrupted run (every entity failed RUN_INTERRUPTED) so ERP closes it at once. The exact
/// stored bytes are sent. 2xx → sent. A coded 409/422 is final (refused, never retried): ERP
/// then abandons the run after 24 h, the safe fallback. Anything else (503 RUN_NOT_READY,
/// transport) is retried with backoff, at most <see cref="MaxAttempts"/> times.
/// </summary>
public sealed class EtlInterruptionNoticeWorker(
    IAgentStore store,
    IErpClient erp,
    AgentRuntimeState state,
    ILogger<EtlInterruptionNoticeWorker> logger) : BackgroundService
{
    internal static TimeSpan IdleRecheck { get; set; } = TimeSpan.FromSeconds(30);
    internal const int MaxAttempts = 20;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            if (state.IsReady && state.Snapshot.CanCompleteEtlRuns)
            {
                try
                {
                    await RunOnceAsync(stoppingToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { return; }
                catch (Exception ex) { logger.LogError(ex, "ETL_INTERRUPTION_NOTICE_PASS_FAILED"); }
            }
            await Task.Delay(IdleRecheck, stoppingToken).ConfigureAwait(false);
        }
    }

    internal async Task<int> RunOnceAsync(CancellationToken cancellationToken)
    {
        var due = await store.GetDueInterruptionNoticesAsync(10, DateTimeOffset.UtcNow, cancellationToken).ConfigureAwait(false);
        foreach (var notice in due)
        {
            try
            {
                await erp.CompleteEtlRunRawAsync(notice.RunId, notice.PayloadJson, cancellationToken).ConfigureAwait(false);
                await store.RecordInterruptionNoticeAsync(notice.RunId, EtlInterruptionNoticeStatus.Sent, null, null, CancellationToken.None).ConfigureAwait(false);
                logger.LogInformation("ETL_INTERRUPTED_RUN_CLOSED RunId={RunId}", notice.RunId);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { throw; }
            catch (ErpApiException ex) when (ex.ApiErrorCode is not null && ex.StatusCode is HttpStatusCode.Conflict or HttpStatusCode.UnprocessableEntity)
            {
                await store.RecordInterruptionNoticeAsync(notice.RunId, EtlInterruptionNoticeStatus.Refused, $"{(int)ex.StatusCode} {ex.ApiErrorCode}", null, CancellationToken.None).ConfigureAwait(false);
                logger.LogWarning("ETL_INTERRUPTED_RUN_CLOSE_REFUSED RunId={RunId} Status={Status} Code={Code} RequestId={RequestId} — not retried; ERP abandons the run itself", notice.RunId, (int)ex.StatusCode, ex.ApiErrorCode, ex.RequestId);
            }
            catch (Exception ex)
            {
                var attempt = notice.AttemptCount + 1;
                if (attempt >= MaxAttempts)
                {
                    await store.RecordInterruptionNoticeAsync(notice.RunId, EtlInterruptionNoticeStatus.Exhausted, ex.GetType().Name, null, CancellationToken.None).ConfigureAwait(false);
                    logger.LogWarning(ex, "ETL_INTERRUPTED_RUN_CLOSE_EXHAUSTED RunId={RunId} Attempts={Attempts}", notice.RunId, attempt);
                    continue;
                }
                var delay = TimeSpan.FromSeconds(Math.Min(600, 10 * Math.Pow(2, Math.Min(attempt - 1, 6))));
                await store.RecordInterruptionNoticeAsync(notice.RunId, EtlInterruptionNoticeStatus.Pending, ex.GetType().Name, DateTimeOffset.UtcNow + delay, CancellationToken.None).ConfigureAwait(false);
                logger.LogWarning(ex, "ETL_INTERRUPTED_RUN_CLOSE_RETRY RunId={RunId} Attempt={Attempt} RetryIn={RetryIn}", notice.RunId, attempt, delay);
            }
        }
        return due.Count;
    }
}
