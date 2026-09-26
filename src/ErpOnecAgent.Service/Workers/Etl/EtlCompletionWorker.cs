using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Application.Commands;
using ErpOnecAgent.Application.Configuration;
using ErpOnecAgent.Service.Runtime;
using Microsoft.Extensions.Options;

namespace ErpOnecAgent.Service.Workers.Etl;

/// <summary>
/// C1: run completion under the F1 claim. The stored complete payload is sent
/// byte-identically (H1). On ERP success, the atomic finalize commits all watermarks, the
/// run, the job and the ownership release together. A failed send schedules a bounded
/// fenced retry of the SAME payload. Exhaustion blocks the run with evidence preserved.
/// </summary>
public sealed class EtlCompletionWorker(
    IAgentStore store,
    IErpClient erp,
    AgentRuntimeState state,
    IOptions<EtlOptions> options,
    IOptions<AgentOptions> agentOptions,
    ILogger<EtlCompletionWorker> logger,
    IOptions<ErpOptions>? erpOptions = null) : BackgroundService
{
    /// <summary>
    /// A crashed sender's claim is reclaimable only after this period (startup recovery releases
    /// it sooner). It always outlasts one completion call on the transfer channel, so a claim can
    /// never expire — and be re-claimed and re-sent — while its own call is still in flight.
    /// </summary>
    internal static TimeSpan ClaimHoldFor(ErpOptions? erp)
    {
        var minimum = TimeSpan.FromMinutes(5);
        if (erp is null) return minimum;
        var call = ErpOnecAgent.Infrastructure.ErpApi.ErpClientRegistration.TransferHttpClientTimeout(erp) + TimeSpan.FromMinutes(1);
        return call > minimum ? call : minimum;
    }

    private readonly TimeSpan _claimHold = ClaimHoldFor(erpOptions?.Value);
    private readonly string _owner = $"{agentOptions.Value.AgentId}:complete:{Environment.ProcessId}";

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var worked = state.Snapshot.CanCompleteEtlRuns && await RunOnceAsync(stoppingToken).ConfigureAwait(false) > 0;
                if (!worked) await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { break; }
            catch (Exception ex)
            {
                logger.LogError(ex, "ETL_COMPLETION_PASS_FAILED");
                await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken).ConfigureAwait(false);
            }
        }
    }

    internal async Task<int> RunOnceAsync(CancellationToken cancellationToken)
    {
        var handled = 0;
        foreach (var candidate in await store.GetDueRunCompletionsAsync(4, DateTimeOffset.UtcNow, cancellationToken).ConfigureAwait(false))
        {
            var claimed = await store.TryClaimRunCompletionAsync(candidate.RunId, _owner, DateTimeOffset.UtcNow + _claimHold, options.Value.MaxRunCompletionAttempts, cancellationToken).ConfigureAwait(false);
            if (claimed is EtlRunClaimOutcome.Blocked blocked)
            {
                logger.LogWarning("ETL_RUN_BLOCKED_AT_COMPLETION RunId={RunId} Code={Code}", candidate.RunId, blocked.Code);
                continue;
            }
            if (claimed is not EtlRunClaimOutcome.Claimed { Claim: var claim }) continue;
            handled++;
            try
            {
                await erp.CompleteEtlRunRawAsync(claim.RunId, claim.CompletePayloadJson, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
            {
                var next = DateTimeOffset.UtcNow + CommandPolicy.BackoffDelay(claim.Attempt, TimeSpan.FromSeconds(10), TimeSpan.FromMinutes(10));
                var retry = await store.MarkRunCompletionRetryAsync(claim.RunId, claim.ClaimId, ex.GetType().Name + ": " + ex.Message, next, options.Value.MaxRunCompletionAttempts, CancellationToken.None).ConfigureAwait(false);
                logger.LogWarning(ex, "ETL_RUN_COMPLETION_RETRY RunId={RunId} Outcome={Outcome}", claim.RunId, retry);
                continue;
            }
            var finalized = await store.FinalizeEtlRunAsync(claim.RunId, claim.ClaimId, CancellationToken.None).ConfigureAwait(false);
            if (finalized is EtlRunFinalizeOutcome.Finalized { EntitiesFailed: var entitiesFailed, FailedEntities: var failedEntities })
            {
                state.LastEtlSuccessAtUtc = DateTimeOffset.UtcNow;
                if (entitiesFailed > 0)
                    logger.LogWarning("ETL_RUN_PARTIAL RunId={RunId} EntitiesFailed={EntitiesFailed} FailedEntities={FailedEntities} — the other entities were committed; failed entities keep their watermark and are retried by the next run (details: ETL_ENTITY_FAILED)", claim.RunId, entitiesFailed, string.Join(", ", failedEntities ?? []));
                else
                    logger.LogInformation("ETL_RUN_SUCCEEDED RunId={RunId}", claim.RunId);
            }
            else logger.LogWarning("ETL_RUN_FINALIZE_NOT_APPLIED RunId={RunId} Outcome={Outcome}", claim.RunId, finalized);
        }
        return handled;
    }
}
