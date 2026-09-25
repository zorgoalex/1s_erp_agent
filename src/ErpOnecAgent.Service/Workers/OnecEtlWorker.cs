using System.Text.Json;
using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Application.Configuration;
using ErpOnecAgent.Application.Etl;
using ErpOnecAgent.Domain.Etl;
using ErpOnecAgent.Service.Runtime;
using Microsoft.Extensions.Options;

namespace ErpOnecAgent.Service.Workers;

public sealed class OnecEtlWorker(
    IAgentStore store,
    ISpoolStore spool,
    IOnecODataClient odata,
    EtlTrigger trigger,
    AgentRuntimeState state,
    DynamicConfigurationState dynamicConfiguration,
    IOptions<EtlOptions> etlOptions,
    IOptions<StorageOptions> storageOptions,
    IOptions<AgentOptions> agentOptions,
    IDiskSpaceProbe diskProbe,
    ILogger<OnecEtlWorker> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var nextScheduled = etlOptions.Value.RunOnStartup ? DateTimeOffset.UtcNow : DateTimeOffset.UtcNow.AddMinutes(dynamicConfiguration.Snapshot.IntervalMinutes);
        while (!stoppingToken.IsCancellationRequested)
        {
            if (!state.IsReady)
            {
                await Task.Delay(250, stoppingToken).ConfigureAwait(false);
                continue;
            }
            EtlTriggerRequest request;
            var delay = Task.Delay(nextScheduled - DateTimeOffset.UtcNow > TimeSpan.Zero ? nextScheduled - DateTimeOffset.UtcNow : TimeSpan.Zero, stoppingToken);
            var triggerTask = trigger.ReadAsync(stoppingToken).AsTask();
            if (await Task.WhenAny(delay, triggerTask).ConfigureAwait(false) == triggerTask) request = await triggerTask.ConfigureAwait(false);
            else { request = new("incremental", null); nextScheduled = DateTimeOffset.UtcNow.AddMinutes(dynamicConfiguration.Snapshot.IntervalMinutes); }

            if (!etlOptions.Value.Enabled || !state.Snapshot.CanExtract) continue;
            if (await spool.GetSizeAsync(stoppingToken).ConfigureAwait(false) >= storageOptions.Value.MaxSpoolBytes) { logger.LogWarning("DISK_WARNING ETL skipped because spool limit is reached"); continue; }
            // A08: extraction starts only while the data volume keeps the command reserve plus
            // one batch of headroom free; otherwise the run is not created at all.
            if (!DiskAllowsExtraction()) { logger.LogWarning("DISK_WARNING ETL skipped because free disk space would not keep the command reserve"); continue; }
            await RunAsync(request, stoppingToken).ConfigureAwait(false);
        }
    }

    private bool DiskAllowsExtraction()
    {
        var storage = storageOptions.Value;
        try
        {
            var free = diskProbe.GetAvailableFreeBytes(agentOptions.Value.DataDirectory);
            return EtlDiskAdmission.Allows(free, storage.MinimumReservedBytesForCommands, Math.Min(storage.MaxBatchCompressedBytes, EtlDiskAdmission.DefaultBatchHeadroomBytes));
        }
        catch (Exception ex) when (ex is IOException or ArgumentException or UnauthorizedAccessException or InvalidOperationException)
        {
            // Unknown free space fails closed without stopping the host.
            logger.LogWarning(ex, "DISK_WARNING free disk space could not be determined; ETL extraction deferred");
            return false;
        }
    }

    private async Task RunAsync(EtlTriggerRequest request, CancellationToken cancellationToken)
    {
        var options = etlOptions.Value;
        var entities = dynamicConfiguration.Snapshot.Entities;
        var selectedByRequest = request.Entities is { Count: > 0 }
            ? entities.Where(entity => entity.Enabled && request.Entities.Contains(entity.EntityCode, StringComparer.Ordinal)).ToArray()
            : entities.Where(static entity => entity.Enabled).ToArray();
        var selected = request.Mode == "incremental" && request.Entities is not { Count: > 0 }
            ? selectedByRequest.Where(static entity => entity.RunsOnSchedule()).ToArray()
            : selectedByRequest;
        var runId = Guid.NewGuid();
        var run = new EtlRun(runId, request.Mode, selected.Select(static entity => entity.EntityCode).ToArray(), EtlRunStatus.Running);
        await store.CreateEtlRunAsync(run, cancellationToken).ConfigureAwait(false);
        state.CurrentEtlRunId = runId;
        logger.LogInformation("ETL_RUN_STARTED RunId={RunId} Mode={Mode}", runId, request.Mode);
        try
        {
            foreach (var entity in selected)
            {
                var committed = await store.GetCommittedWatermarkAsync(entity.EntityCode, cancellationToken).ConfigureAwait(false);
                var upper = new EtlCursor(DateTimeOffset.UtcNow.AddSeconds(-Math.Max(0, options.SafetyLagSeconds)), null);
                var rows = new List<JsonElement>();
                long approximateBytes = 0;
                var from = committed;
                EtlCursor? last = null;
                var full = request.Mode is "bootstrap_full" or "entity_reload" or "reconcile_keys" or "reconcile_totals";
                await foreach (var row in odata.ReadEntityAsync(entity, committed, upper, full, cancellationToken).ConfigureAwait(false))
                {
                    rows.Add(row); approximateBytes += row.GetRawText().Length + 128;
                    last = CursorFrom(row, entity);
                    if (approximateBytes >= options.TargetBatchUncompressedBytes)
                    {
                        await FlushAsync(runId, entity, rows, from, last, cancellationToken).ConfigureAwait(false);
                        from = last; rows = []; approximateBytes = 0;
                    }
                }
                if (rows.Count > 0) await FlushAsync(runId, entity, rows, from, last, cancellationToken).ConfigureAwait(false);
                else if (last is null) await FlushAsync(runId, entity, [], from, upper, cancellationToken).ConfigureAwait(false);
            }
            await store.MarkEtlRunExtractedAsync(runId, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            await store.CompleteEtlRunAsync(runId, EtlRunStatus.Failed, ex.Message, cancellationToken).ConfigureAwait(false);
            logger.LogError(ex, "ETL_RUN_FAILED RunId={RunId}", runId);
        }
        finally { state.CurrentEtlRunId = null; }
    }

    private async Task FlushAsync(Guid runId, EtlEntityDefinition entity, List<JsonElement> rows, EtlCursor? from, EtlCursor? to, CancellationToken cancellationToken)
    {
        var batch = await spool.WriteBatchAsync(runId, entity, rows, from, to, cancellationToken).ConfigureAwait(false);
        await store.RegisterBatchAsync(batch, cancellationToken).ConfigureAwait(false);
        logger.LogInformation("ETL_BATCH_CREATED RunId={RunId} BatchId={BatchId} Entity={Entity} Rows={Rows}", runId, batch.BatchId, entity.EntityCode, rows.Count);
    }

    private static EtlCursor CursorFrom(JsonElement row, EtlEntityDefinition entity)
    {
        var id = entity.SourceIdFrom(row);
        DateTimeOffset? updated = null;
        if (entity.UpdatedAtField is not null && row.TryGetProperty(entity.UpdatedAtField, out var value) && DateTimeOffset.TryParse(value.ToString(), System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.AssumeUniversal, out var parsed)) updated = parsed.ToUniversalTime();
        return new(updated, id);
    }
}
