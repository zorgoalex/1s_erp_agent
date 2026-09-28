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
    internal static TimeSpan IdleRecheck { get; set; } = TimeSpan.FromSeconds(15);
    // L2: fallback re-check while at full capacity (a released slot normally wakes the worker via
    // CommandWorkSignals.Capacity; this covers rows that become due or a missed pulse).
    internal static TimeSpan CapacityRecheck { get; set; } = TimeSpan.FromSeconds(1);
    private bool _idleWithoutCommandTypes;
    private bool _atCapacity;

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
            // No command type is allowed (ERP configuration commandTypes: [] or an empty local
            // allowlist): there is nothing to lease, so no long poll is issued (agreed with ERP,
            // to-onec/0015 — before E2 the lease endpoint does not exist).
            if (dynamicConfiguration.Snapshot.CommandTypes.Count == 0)
            {
                if (!_idleWithoutCommandTypes) logger.LogInformation("COMMAND_LEASE_IDLE — no command types are allowed; leasing paused until the configuration allows some");
                _idleWithoutCommandTypes = true;
                await Task.Delay(IdleRecheck, stoppingToken).ConfigureAwait(false);
                continue;
            }
            _idleWithoutCommandTypes = false;
            try
            {
                var options = commandOptions.Value;
                // L2 (agreed with ERP, to-onec/0045): ERP hands out no command while
                // executing >= capacity and holds such a lease for the whole long poll, so leasing
                // at full capacity cost a long poll per command of a queued batch. The load counts
                // commands being executed plus accepted-but-not-started ones; at capacity no lease
                // goes out until a slot is released.
                var now = DateTimeOffset.UtcNow;
                var occupied = state.ExecutingCommands + await store.CountReadyUnclaimedCommandsAsync(now, state.NotBeforeNow(now), stoppingToken).ConfigureAwait(false);
                if (occupied >= options.MaxConcurrency)
                {
                    if (!_atCapacity) logger.LogDebug("COMMAND_LEASE_AT_CAPACITY Occupied={Occupied} Capacity={Capacity}", occupied, options.MaxConcurrency);
                    _atCapacity = true;
                    await _signals.Capacity.WaitAsync(CapacityRecheck, stoppingToken).ConfigureAwait(false);
                    continue;
                }
                _atCapacity = false;
                var sessionId = await sessions.GetSessionAsync(stoppingToken).ConfigureAwait(false);
                var supportedTypes = dynamicConfiguration.Snapshot.CommandTypes;
                var leaseStarted = System.Diagnostics.Stopwatch.GetTimestamp();
                var lease = await erp.LeaseCommandAsync(new LeaseRequest(sessionId, supportedTypes, erpOptions.Value.LongPollSeconds, new LeaseLoad(occupied, options.MaxConcurrency)), stoppingToken).ConfigureAwait(false);
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
