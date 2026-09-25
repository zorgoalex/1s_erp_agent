using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Application.Commands;
using ErpOnecAgent.Application.Configuration;
using ErpOnecAgent.Contracts.Erp;
using ErpOnecAgent.Domain.Commands;
using ErpOnecAgent.Service.Runtime;
using Microsoft.Extensions.Options;

namespace ErpOnecAgent.Service.Workers;

public sealed class CommandLeaseWorker(
    IErpClient erp,
    IAgentStore store,
    ErpSessionManager sessions,
    AgentRuntimeState state,
    DynamicConfigurationState dynamicConfiguration,
    IOptions<ErpOptions> erpOptions,
    IOptions<CommandOptions> commandOptions,
    ILogger<CommandLeaseWorker> logger) : BackgroundService
{
    private readonly CommandIntakeService intake = new(erp, store);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var failureCount = 0;
        while (!stoppingToken.IsCancellationRequested)
        {
            if (!state.Snapshot.CanLeaseCommands)
            {
                await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken).ConfigureAwait(false); continue;
            }
            try
            {
                var sessionId = await sessions.GetSessionAsync(stoppingToken).ConfigureAwait(false);
                var options = commandOptions.Value;
                var supportedTypes = dynamicConfiguration.Snapshot.CommandTypes;
                var lease = await erp.LeaseCommandAsync(new LeaseRequest(sessionId, supportedTypes, erpOptions.Value.LongPollSeconds, new LeaseLoad(state.ExecutingCommands, options.MaxConcurrency)), stoppingToken).ConfigureAwait(false);
                state.LastErpSuccessAtUtc = DateTimeOffset.UtcNow; failureCount = 0;
                // A conforming ERP holds this request for LongPollSeconds. The
                // small floor also prevents a faulty/non-long-polling endpoint
                // from turning an empty queue into a CPU/network busy loop.
                if (!lease.HasCommand)
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(250), stoppingToken).ConfigureAwait(false);
                    continue;
                }
                if (lease.LeaseId is null || lease.Command is null) throw new InvalidDataException("ERP lease response is missing leaseId or command.");
                var result = await intake.IntakeAsync(lease.LeaseId.Value, lease.Command, supportedTypes, options.MaxPayloadBytes, DateTimeOffset.UtcNow, stoppingToken).ConfigureAwait(false);
                if (result.Outcome == StoreCommandOutcome.PayloadConflict)
                {
                    logger.LogCritical("COMMAND_PAYLOAD_CONFLICT CommandId={CommandId}", result.CommandId);
                }
                else if (result.Outcome == StoreCommandOutcome.Duplicate) logger.LogInformation("COMMAND_DUPLICATE CommandId={CommandId}", result.CommandId);
                else if (result.Outcome == StoreCommandOutcome.Rejected) logger.LogWarning("COMMAND_REJECTED CommandId={CommandId} Code={ErrorCode}", result.CommandId, result.Validation.ErrorCode);
                else logger.LogInformation("COMMAND_RECEIVED CommandId={CommandId}", result.CommandId);
                if (result.AckError is not null) throw result.AckError;
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { break; }
            catch (Exception ex)
            {
                sessions.Invalidate(); failureCount++;
                var delay = TimeSpan.FromSeconds(Math.Min(60, Math.Pow(2, Math.Min(failureCount, 6)))) + TimeSpan.FromMilliseconds(Random.Shared.Next(50, 750));
                logger.LogWarning(ex, "ERP_DISCONNECTED RetryIn={RetryIn}", delay);
                await Task.Delay(delay, stoppingToken).ConfigureAwait(false);
            }
        }
    }
}
