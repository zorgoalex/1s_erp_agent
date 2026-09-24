using System.Diagnostics;
using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Application.Configuration;
using Microsoft.Extensions.Options;

namespace ErpOnecAgent.Service.Runtime;

public sealed record AgentMetrics(long DiskFreeBytes, long WorkingSetBytes, double CpuPercent, long SqliteSizeBytes, long SpoolSizeBytes);

public sealed class AgentMetricsCollector(ISpoolStore spool, IOptions<AgentOptions> agentOptions)
{
    private readonly object _gate = new();
    private TimeSpan _lastProcessorTime;
    private DateTimeOffset _lastSampleAtUtc = DateTimeOffset.UtcNow;
    private bool _hasCpuSample;

    public async Task<AgentMetrics> CaptureAsync(CancellationToken cancellationToken)
    {
        var dataDirectory = Path.GetFullPath(agentOptions.Value.DataDirectory);
        var dataRoot = Path.GetPathRoot(dataDirectory) ?? throw new InvalidOperationException("Unable to determine the agent data drive.");
        using var process = Process.GetCurrentProcess();
        process.Refresh();
        var sqlitePath = Path.Combine(dataDirectory, "data", "agent.db");
        var sqliteSize = GetFileSize(sqlitePath) + GetFileSize(sqlitePath + "-wal") + GetFileSize(sqlitePath + "-shm");
        return new AgentMetrics(
            new DriveInfo(dataRoot).AvailableFreeSpace,
            process.WorkingSet64,
            CalculateCpuPercent(process.TotalProcessorTime),
            sqliteSize,
            await spool.GetSizeAsync(cancellationToken).ConfigureAwait(false));
    }

    private double CalculateCpuPercent(TimeSpan currentProcessorTime)
    {
        lock (_gate)
        {
            var now = DateTimeOffset.UtcNow;
            var elapsed = now - _lastSampleAtUtc;
            var processor = currentProcessorTime - _lastProcessorTime;
            _lastSampleAtUtc = now;
            _lastProcessorTime = currentProcessorTime;
            if (!_hasCpuSample) { _hasCpuSample = true; return 0; }
            if (elapsed <= TimeSpan.Zero) return 0;
            return Math.Clamp(processor.TotalMilliseconds / (elapsed.TotalMilliseconds * Environment.ProcessorCount) * 100, 0, 100);
        }
    }

    private static long GetFileSize(string path) => File.Exists(path) ? new FileInfo(path).Length : 0;
}
