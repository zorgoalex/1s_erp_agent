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
    ILogger<CommandLeaseWorker> logger,
    CommandWorkSignals? signals = null) : BackgroundService
{
    // Minimum cycle for an EMPTY lease answer. A conforming ERP holds the request for
    // LongPollSeconds, so the next lease goes out at once; an endpoint that answers empty
    // early is capped at a few requests per second instead of a busy loop.
    internal static readonly TimeSpan EmptyLeaseMinimumCycle = TimeSpan.FromMilliseconds(250);
    private readonly CommandWorkSignals _signals = signals ?? new CommandWorkSignals();

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
                var leaseStarted = System.Diagnostics.Stopwatch.GetTimestamp();
                var lease = await erp.LeaseCommandAsync(new LeaseRequest(sessionId, supportedTypes, erpOptions.Value.LongPollSeconds, new LeaseLoad(state.ExecutingCommands, options.MaxConcurrency)), stoppingToken).ConfigureAwait(false);
                state.LastErpSuccessAtUtc = DateTimeOffset.UtcNow; failureCount = 0;
                if (!lease.HasCommand)
                {
                    var rest = EmptyLeaseMinimumCycle - System.Diagnostics.Stopwatch.GetElapsedTime(leaseStarted);
                    if (rest > TimeSpan.Zero) await Task.Delay(rest, stoppingToken).ConfigureAwait(false);
                    continue;
                }
                if (lease.LeaseId is null || lease.Command is null) throw new InvalidDataException("ERP lease response is missing leaseId or command.");
                var result = await intake.IntakeAsync(lease.LeaseId.Value, lease.Command, supportedTypes, options.MaxPayloadBytes, DateTimeOffset.UtcNow, stoppingToken).ConfigureAwait(false);
                // Stored (or rejected with a stored result): wake execution and delivery now
                // instead of at their next timed re-check.
                _signals.Commands.Pulse();
                _signals.Results.Pulse();
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
