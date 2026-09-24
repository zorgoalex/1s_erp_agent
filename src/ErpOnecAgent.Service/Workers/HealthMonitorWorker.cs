using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Application.Configuration;
using ErpOnecAgent.Service.Runtime;
using Microsoft.Extensions.Options;

namespace ErpOnecAgent.Service.Workers;

public sealed class HealthMonitorWorker(
    IOnecHealthClient commandHealth,
    IOnecODataClient odata,
    AgentRuntimeState state,
    IOptions<AgentOptions> options,
    IOptions<OnecOptions> onecOptions,
    ILogger<HealthMonitorWorker> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            if (!state.IsReady)
            {
                await Task.Delay(250, stoppingToken).ConfigureAwait(false);
                continue;
            }
            var previousCommand = state.OnecCommandApiAvailable;
            var previousOdata = state.OnecODataAvailable;
            try
            {
                using var healthTimeout = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
                healthTimeout.CancelAfter(TimeSpan.FromSeconds(Math.Max(1, onecOptions.Value.HealthTimeoutSeconds)));
                var command = await commandHealth.CheckAsync(healthTimeout.Token).ConfigureAwait(false);
                var odataAvailable = await odata.CheckAsync(healthTimeout.Token).ConfigureAwait(false);
                state.OnecCommandApiAvailable = command is not null;
                state.OnecODataAvailable = odataAvailable;
                state.LastOnecError = command is null || !odataAvailable ? "One or more 1C endpoints are unavailable." : null;
                if (command is not null || odataAvailable) state.LastOnecSuccessAtUtc = DateTimeOffset.UtcNow;
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { return; }
            catch (Exception ex)
            {
                state.OnecCommandApiAvailable = false;
                state.OnecODataAvailable = false;
                state.LastOnecError = ex.Message.Length > 512 ? ex.Message[..512] : ex.Message;
                logger.LogWarning(ex, "1C health check failed");
            }
            if (previousCommand != state.OnecCommandApiAvailable || previousOdata != state.OnecODataAvailable)
                logger.LogInformation("1C health changed OData={ODataAvailable} CommandApi={CommandApiAvailable}", state.OnecODataAvailable, state.OnecCommandApiAvailable);
            await Task.Delay(TimeSpan.FromSeconds(Math.Max(5, options.Value.HealthCheckIntervalSeconds)), stoppingToken).ConfigureAwait(false);
        }
    }
}
