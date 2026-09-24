using System.IO.Compression;
using System.Text.Json;
using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Application.Configuration;
using ErpOnecAgent.Service.Runtime;
using Microsoft.Extensions.Options;

namespace ErpOnecAgent.Service.Diagnostics;

public sealed class DiagnosticsCollector(IAgentStore store, IOptions<AgentOptions> agent, IOptions<ErpOptions> erp, IOptions<OnecOptions> onec)
{
    public async Task<string> CollectAsync(CancellationToken cancellationToken)
    {
        var directory = Path.Combine(agent.Value.DataDirectory, "diagnostics"); Directory.CreateDirectory(directory);
        var path = Path.Combine(directory, $"diagnostics-{DateTimeOffset.UtcNow:yyyyMMdd-HHmmss}.zip");
        using var archive = ZipFile.Open(path, ZipArchiveMode.Create);
        await WriteAsync(archive, "version.json", JsonSerializer.Serialize(new { version = ThisAssembly.Version, generatedAtUtc = DateTimeOffset.UtcNow }, Pretty), cancellationToken).ConfigureAwait(false);
        await WriteAsync(archive, "configuration-redacted.json", JsonSerializer.Serialize(new
        {
            agent = new { agent.Value.AgentId, agent.Value.SiteId, agent.Value.Environment, agent.Value.DataDirectory },
            erp = new { erp.Value.BaseUrl, erp.Value.ApiVersion, certificateConfigured = !string.IsNullOrWhiteSpace(erp.Value.ClientCertificateThumbprint) },
            oneC = new { onec.Value.ODataBaseUrl, onec.Value.CommandApiBaseUrl, credentialConfigured = !string.IsNullOrWhiteSpace(onec.Value.CredentialSecretName) }
        }, Pretty), cancellationToken).ConfigureAwait(false);
        await WriteAsync(archive, "sqlite-integrity.txt", await store.IntegrityCheckAsync(cancellationToken).ConfigureAwait(false), cancellationToken).ConfigureAwait(false);
        await WriteAsync(archive, "queue-summary.json", JsonSerializer.Serialize(await store.GetQueueMetricsAsync(cancellationToken).ConfigureAwait(false), Pretty), cancellationToken).ConfigureAwait(false);
        return path;
    }

    private static readonly JsonSerializerOptions Pretty = new(JsonSerializerDefaults.Web) { WriteIndented = true };
    private static async Task WriteAsync(ZipArchive archive, string name, string content, CancellationToken cancellationToken)
    {
        var entry = archive.CreateEntry(name, CompressionLevel.SmallestSize);
        await using var stream = entry.Open(); await using var writer = new StreamWriter(stream);
        await writer.WriteAsync(content.AsMemory(), cancellationToken).ConfigureAwait(false);
    }
}
