using System.Security.Cryptography;
using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Application.Commands;
using ErpOnecAgent.Application.Configuration;
using ErpOnecAgent.Domain.Etl;
using ErpOnecAgent.Service.Runtime;
using Microsoft.Extensions.Options;

namespace ErpOnecAgent.Service.Workers.Etl;

/// <summary>
/// C1: owner-fenced batch upload over the O2 send ledger. Any failure before the ERP call
/// is invoked (spool open, checksum mismatch) is a trusted precheck failure and gets a
/// bounded retry. Once the call is invoked, any exception or non-2xx is an UNKNOWN outcome:
/// the batch is quarantined and the run blocked, never re-sent. A returned ACK is validated
/// by the store against the claimed attempt.
/// </summary>
public sealed class EtlUploadWorker(
    IAgentStore store,
    ISpoolStore spool,
    IErpClient erp,
    AgentRuntimeState state,
    IOptions<EtlOptions> options,
    IOptions<AgentOptions> agentOptions,
    ILogger<EtlUploadWorker> logger) : BackgroundService
{
    private readonly string _owner = $"{agentOptions.Value.AgentId}:upload:{Environment.ProcessId}";

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var worked = state.Snapshot.CanUploadBatches && await RunOnceAsync(stoppingToken).ConfigureAwait(false) > 0;
                if (!worked) await Task.Delay(TimeSpan.FromMilliseconds(750), stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { break; }
            catch (Exception ex)
            {
                logger.LogError(ex, "ETL_UPLOAD_PASS_FAILED");
                await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken).ConfigureAwait(false);
            }
        }
    }

    internal async Task<int> RunOnceAsync(CancellationToken cancellationToken)
    {
        // Returns the number of batches actually claimed: due rows that could not be claimed
        // (quarantined, claimed elsewhere) must not keep the loop spinning without a delay.
        var due = await store.GetDueBatchUploadsAsync(options.Value.MaxConcurrentBatchUploads, DateTimeOffset.UtcNow, cancellationToken).ConfigureAwait(false);
        var claimed = await Task.WhenAll(due.Select(batch => UploadAsync(batch, cancellationToken))).ConfigureAwait(false);
        return claimed.Count(static value => value);
    }

    private async Task<bool> UploadAsync(EtlDueBatchUpload due, CancellationToken cancellationToken)
    {
        var claimed = await store.TryClaimBatchUploadAsync(due.BatchId, _owner, DateTimeOffset.UtcNow, options.Value.MaxBatchUploadAttempts, cancellationToken).ConfigureAwait(false);
        if (claimed is EtlBatchUploadClaimOutcome.Blocked blocked)
        {
            logger.LogWarning("ETL_BATCH_QUARANTINED BatchId={BatchId} Code={Code}", due.BatchId, blocked.Code);
            return false;
        }
        if (claimed is not EtlBatchUploadClaimOutcome.Claimed { Claim: var claim }) return false;

        var batch = new EtlBatch(due.BatchId, due.RunId, due.EntityName, due.SchemaVersion, due.FilePath, EtlBatchStatus.Uploading, due.RowCount,
            null, null, due.Sha256, 0, 0, claim.AttemptNo, DateTimeOffset.UtcNow);

        // Precheck: the network call has NOT been invoked on any path through this block. The
        // checksum is computed by streaming; the file is reopened for the send (the spool is
        // private to the agent, and the registered sha256 travels in the request header).
        Stream content;
        try
        {
            await using (var verify = await spool.OpenReadAsync(batch, cancellationToken).ConfigureAwait(false))
            {
                var actual = Convert.ToBase64String(await SHA256.HashDataAsync(verify, cancellationToken).ConfigureAwait(false));
                if (!string.Equals(actual, due.Sha256, StringComparison.Ordinal)) throw new InvalidDataException("Spool file checksum does not match the registered batch.");
            }
            content = await spool.OpenReadAsync(batch, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Shutdown during the precheck: nothing was sent, so the attempt is recorded as a
            // precheck failure (due immediately) instead of staying admitted and later being
            // treated as an unknown outcome.
            await WriteWithRetryAsync(() => store.RetryClaimedBatchSendAsync(due.BatchId, claim.AttemptId, "PRECHECK: cancelled by shutdown", DateTimeOffset.UtcNow, CancellationToken.None)).ConfigureAwait(false);
            throw;
        }
        catch (Exception ex)
        {
            // A missing file never heals by retrying; it is recorded distinctly so the
            // eventual UPLOAD_ATTEMPTS_EXHAUSTED block shows the cause in the send ledger.
            var missing = ex is FileNotFoundException or DirectoryNotFoundException;
            if (missing) logger.LogError(ex, "ETL_SPOOL_FILE_MISSING BatchId={BatchId} Path={Path}", due.BatchId, due.FilePath);
            var next = DateTimeOffset.UtcNow + CommandPolicy.BackoffDelay(claim.AttemptNo, TimeSpan.FromSeconds(5), TimeSpan.FromMinutes(5));
            var retry = await WriteWithRetryAsync(() => store.RetryClaimedBatchSendAsync(due.BatchId, claim.AttemptId,
                (missing ? "SPOOL_FILE_MISSING: " : "PRECHECK: ") + ex.Message, next, CancellationToken.None)).ConfigureAwait(false);
            logger.LogWarning(ex, "ETL_BATCH_PRECHECK_FAILED BatchId={BatchId} Outcome={Outcome}", due.BatchId, retry);
            return true;
        }

        BatchAcknowledgementResult result;
        try
        {
            var response = await erp.UploadBatchWithEvidenceAsync(batch, content, cancellationToken).ConfigureAwait(false);
            result = new BatchAcknowledgementResult(response, null, null);
        }
        catch (Exception ex)
        {
            // Invoked: absence of a response or a non-2xx never proves no remote effect.
            result = new BatchAcknowledgementResult(null, ex, (ex as HttpRequestException)?.StatusCode is { } code ? (int)code : null);
        }
        finally
        {
            await content.DisposeAsync().ConfigureAwait(false);
        }

        // After the ERP call the outcome must reach the ledger: the write is retried a few
        // times (a busy database), and if it still fails the attempt stays sent-but-unrecorded,
        // which startup recovery orphans and quarantines — never a resend.
        if (result.Response is null)
        {
            var failed = await RecordOutcomeAsync(due.BatchId, () => store.FailClaimedBatchSendAsync(due.BatchId, claim.AttemptId, $"{result.Error!.GetType().Name}: {result.Error.Message}", result.HttpStatus, CancellationToken.None)).ConfigureAwait(false);
            logger.LogWarning("ETL_BATCH_OUTCOME_UNKNOWN BatchId={BatchId} Outcome={Outcome}", due.BatchId, failed);
            return true;
        }

        var ack = result.Response.Ack;
        var evidence = new EtlBatchAckEvidence(ack.Status, ack.BatchId, ack.RowsAccepted, ack.ChecksumValid, ack.AcknowledgedAtUtc);
        var acknowledged = await RecordOutcomeAsync(due.BatchId, () => store.AcknowledgeClaimedBatchAsync(due.BatchId, claim.AttemptId, evidence,
            Convert.ToHexString(SHA256.HashData(result.Response.Body)), result.Response.HttpStatus, CancellationToken.None)).ConfigureAwait(false);
        if (acknowledged is EtlBatchAckOutcome.Acknowledged) logger.LogInformation("ETL_BATCH_ACKNOWLEDGED BatchId={BatchId}", due.BatchId);
        else logger.LogWarning("ETL_BATCH_ACK_NOT_APPLIED BatchId={BatchId} Outcome={Outcome}", due.BatchId, acknowledged);
        return true;
    }

    // After the ERP call: if the outcome still cannot be recorded, the attempt stays admitted
    // and the batch uploading — never re-sent, but stalled until the next start orphans it.
    // That is loud: an operator must know the run is stuck.
    private async Task<T> RecordOutcomeAsync<T>(Guid batchId, Func<Task<T>> write)
    {
        try
        {
            return await WriteWithRetryAsync(write).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogCritical(ex, "ETL_BATCH_OUTCOME_UNRECORDED BatchId={BatchId} — the send outcome could not be written; the batch stays uploading until the next service start quarantines it", batchId);
            throw;
        }
    }

    internal static TimeSpan StoreWriteRetryDelay = TimeSpan.FromMilliseconds(500);
    internal const int StoreWriteAttempts = 3;

    private static async Task<T> WriteWithRetryAsync<T>(Func<Task<T>> write)
    {
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                return await write().ConfigureAwait(false);
            }
            catch (Exception ex) when (attempt < StoreWriteAttempts && ex is not OperationCanceledException)
            {
                await Task.Delay(StoreWriteRetryDelay, CancellationToken.None).ConfigureAwait(false);
            }
        }
    }

    private sealed record BatchAcknowledgementResult(BatchUploadResponse? Response, Exception? Error, int? HttpStatus);
}
