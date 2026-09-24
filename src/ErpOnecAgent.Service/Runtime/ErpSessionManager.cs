using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Application.Configuration;
using ErpOnecAgent.Contracts.Erp;
using ErpOnecAgent.Infrastructure.Persistence.Sqlite;
using Microsoft.Extensions.Options;

namespace ErpOnecAgent.Service.Runtime;

public sealed class ErpSessionManager(IErpClient erp, IOptions<AgentOptions> agentOptions, AgentRuntimeState state) : IDisposable
{
    private static readonly string[] Capabilities = ["commands.long-poll.v1", "commands.result.v1", "etl.ndjson-gzip.v1", "onec.odata.v1"];
    private readonly SemaphoreSlim _lock = new(1, 1);
    private Guid? _sessionId;

    public async Task<Guid> GetSessionAsync(CancellationToken cancellationToken)
    {
        if (_sessionId is { } existing) return existing;
        await _lock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_sessionId is { } current) return current;
            var options = agentOptions.Value;
            var response = await erp.StartSessionAsync(new(options.AgentId, options.SiteId, ThisAssembly.Version, SqliteMigrator.CurrentSchemaVersion, Capabilities, DateTimeOffset.UtcNow), cancellationToken).ConfigureAwait(false);
            if (!response.Accepted) throw new InvalidOperationException($"ERP rejected agent version {ThisAssembly.Version}; minimum is {response.MinimumAgentVersion}.");
            if (Version.TryParse(response.MinimumAgentVersion, out var minimum) && Version.TryParse(ThisAssembly.Version, out var currentVersion) && currentVersion < minimum)
                throw new InvalidOperationException($"Agent {currentVersion} is older than required {minimum}.");
            state.SetHandshakeMaintenance(response.MaintenanceMode);
            state.LastErpSuccessAtUtc = DateTimeOffset.UtcNow;
            _sessionId = response.SessionId;
            return response.SessionId;
        }
        finally { _lock.Release(); }
    }

    public void Invalidate() => _sessionId = null;
    public void Dispose() => _lock.Dispose();
}

internal static class ThisAssembly
{
    public static string Version { get; } = typeof(ThisAssembly).Assembly.GetName().Version?.ToString(3) ?? "1.0.0";
}
