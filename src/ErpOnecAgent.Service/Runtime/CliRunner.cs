using System.Text.Json;
using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Application.Configuration;
using ErpOnecAgent.Infrastructure.Security;
using ErpOnecAgent.Service.Diagnostics;
using Microsoft.Extensions.Options;

namespace ErpOnecAgent.Service.Runtime;

public static class CliRunner
{
    public static bool HasCommand(string[] args) => args.Any(static arg => arg is "--validate-config" or "--test-erp" or "--test-onec" or "--migrate" or "--integrity-check" or "--collect-diagnostics" or "--store-onec-credential" or "--version"
        or "--etl-resolve-run" or "--etl-reset-domain");

    public static async Task<int> RunAsync(IServiceProvider services, string[] args, CancellationToken cancellationToken)
    {
        if (args.Contains("--version", StringComparer.Ordinal)) { Console.WriteLine(ThisAssembly.Version); return 0; }
        if (args.Contains("--store-onec-credential", StringComparer.Ordinal))
        {
            Console.Write("1C username: "); var username = Console.ReadLine();
            Console.Write("1C password: "); var password = ReadPassword(); Console.WriteLine();
            if (string.IsNullOrWhiteSpace(username) || string.IsNullOrEmpty(password)) throw new InvalidOperationException("Username and password are required.");
            var name = services.GetRequiredService<IOptions<OnecOptions>>().Value.CredentialSecretName;
            await services.GetRequiredService<ISecretStore>().SaveAsync(name, JsonSerializer.Serialize(new { username, password }), cancellationToken).ConfigureAwait(false);
            Console.WriteLine($"Stored DPAPI-protected credential '{name}'."); return 0;
        }

        var store = services.GetRequiredService<IAgentStore>();
        // Every command below opens SQLite in create mode. Without this check a lost database
        // would be silently re-created empty by e.g. --integrity-check, and the next service
        // start would then pass the presence guard. Only --migrate may create it deliberately.
        if (!args.Contains("--migrate", StringComparer.Ordinal) && MissingDatabase(services) is { } missing)
        {
            Console.Error.WriteLine(missing);
            return 4;
        }
        // Maintenance commands take the instance lock before anything touches the database.
        if (args.Contains("--etl-resolve-run", StringComparer.Ordinal) || args.Contains("--etl-reset-domain", StringComparer.Ordinal))
            return await RunEtlMaintenanceAsync(services, store, args, cancellationToken).ConfigureAwait(false);
        if (args.Contains("--validate-config", StringComparer.Ordinal))
        {
            _ = services.GetRequiredService<IOptions<AgentOptions>>().Value;
            var erp = services.GetRequiredService<IOptions<ErpOptions>>().Value;
            var onec = services.GetRequiredService<IOptions<OnecOptions>>().Value;
            _ = services.GetRequiredService<IOptions<CommandOptions>>().Value;
            _ = services.GetRequiredService<IOptions<EtlOptions>>().Value;
            _ = services.GetRequiredService<IOptions<StorageOptions>>().Value;
            if (erp.RequireClientCertificate) using (CertificateLoader.LoadClientCertificate(erp.ClientCertificateThumbprint)) { }
            if (await services.GetRequiredService<ISecretStore>().ReadAsync(onec.CredentialSecretName, cancellationToken).ConfigureAwait(false) is null) throw new InvalidOperationException("1C credential is not configured.");
            await store.InitializeAsync(cancellationToken).ConfigureAwait(false);
            Console.WriteLine("Configuration is valid."); return 0;
        }
        await store.InitializeAsync(cancellationToken).ConfigureAwait(false);
        if (args.Contains("--migrate", StringComparer.Ordinal))
        {
            // An explicit --migrate is the operator's decision to (re)create or upgrade the
            // database; it records the initialization marker the service start checks.
            DatabasePresenceGuard.MarkInitialized(services.GetRequiredService<ErpOnecAgent.Infrastructure.Persistence.Sqlite.SqliteConnectionFactory>().DatabasePath);
            Console.WriteLine("SQLite migrations applied."); return 0;
        }
        if (args.Contains("--integrity-check", StringComparer.Ordinal)) { var result = await store.IntegrityCheckAsync(cancellationToken).ConfigureAwait(false); Console.WriteLine(result); return string.Equals(result, "ok", StringComparison.OrdinalIgnoreCase) ? 0 : 2; }
        if (args.Contains("--test-erp", StringComparer.Ordinal)) { var session = await services.GetRequiredService<ErpSessionManager>().GetSessionAsync(cancellationToken).ConfigureAwait(false); Console.WriteLine($"ERP handshake succeeded. Session: {session:D}"); return 0; }
        if (args.Contains("--test-onec", StringComparer.Ordinal)) { var result = await services.GetRequiredService<IOnecHealthClient>().CheckAsync(cancellationToken).ConfigureAwait(false); Console.WriteLine(result is null ? "1C health check failed." : "1C health check succeeded."); return result is null ? 2 : 0; }
        if (args.Contains("--collect-diagnostics", StringComparer.Ordinal)) { Console.WriteLine(await services.GetRequiredService<DiagnosticsCollector>().CollectAsync(cancellationToken).ConfigureAwait(false)); return 0; }
        return 1;
    }

    private static string? MissingDatabase(IServiceProvider services) =>
        services.GetService<ErpOnecAgent.Infrastructure.Persistence.Sqlite.SqliteConnectionFactory>() is { } database
            ? DatabasePresenceGuard.CheckBeforeStart(database.DatabasePath, Path.Combine(services.GetRequiredService<IOptions<AgentOptions>>().Value.DataDirectory, "spool"))
            : null;

    // R1/D1 operator commands. They run only while the service is stopped: the command takes the
    // same single-instance lock the service holds, so a running service (whose dispatchers could
    // still act) makes the command fail instead of racing it. That is the durable half of the
    // "workers stopped and drained" boundary; the operator attests the rest (--workers-stopped).
    internal static async Task<int> RunEtlMaintenanceAsync(IServiceProvider services, IAgentStore store, string[] args, CancellationToken cancellationToken)
    {
        using var instanceLock = new SingleInstanceLock();
        instanceLock.Acquire(services.GetRequiredService<IOptions<AgentOptions>>().Value.AgentId);
        if (args.Contains("--etl-resolve-run", StringComparer.Ordinal) && args.Contains("--etl-reset-domain", StringComparer.Ordinal))
            throw new ArgumentException("--etl-resolve-run and --etl-reset-domain cannot be combined.");
        var operatorId = Value(args, "--operator") ?? throw new ArgumentException("--operator <id> is required.");
        await store.InitializeAsync(cancellationToken).ConfigureAwait(false);

        if (args.Contains("--etl-resolve-run", StringComparer.Ordinal))
        {
            var runText = Value(args, "--etl-resolve-run") ?? throw new ArgumentException("--etl-resolve-run <runId> is required.");
            if (!Guid.TryParse(runText, out var runId)) throw new ArgumentException("The run id is not a GUID.");
            var decision = (Value(args, "--decision") ?? string.Empty) switch
            {
                "abandon" => EtlRunResolutionDecision.Abandon,
                "retry" => EtlRunResolutionDecision.Retry,
                "rebaseline" => EtlRunResolutionDecision.Rebaseline,
                _ => throw new ArgumentException("--decision abandon|retry|rebaseline is required.")
            };
            var verification = Value(args, "--verification") ?? throw new ArgumentException("--verification \"<what was verified at ERP>\" is required.");
            var outcome = await store.ResolveEtlRunAsync(new EtlRunResolutionRequest(runId, operatorId, decision, verification,
                WorkersQuiesced: args.Contains("--workers-stopped", StringComparer.Ordinal)), DateTimeOffset.UtcNow, cancellationToken).ConfigureAwait(false);
            switch (outcome)
            {
                case EtlRunResolutionOutcome.Resolved resolved:
                    Console.WriteLine($"Run {runId:D} resolved ({resolved.Record.Decision}). Ownership released: {resolved.Record.OwnershipReleased}. Batches fenced: {resolved.Record.BatchesFenced}. Resolution id: {resolved.Record.ResolutionId:D}.");
                    return 0;
                case EtlRunResolutionOutcome.AlreadyResolved already:
                    Console.WriteLine($"Run {runId:D} was already resolved at {already.Record.ResolvedAtUtc:O} by {already.Record.OperatorId}{(already.SameRequest ? string.Empty : " (with a different request)")}.");
                    return already.SameRequest ? 0 : 3;
                case EtlRunResolutionOutcome.Refused refused:
                    Console.WriteLine($"Run {runId:D} was NOT resolved: {refused.Reason}.");
                    return 2;
                default:
                    return 1;
            }
        }

        var entity = Value(args, "--etl-reset-domain") ?? throw new ArgumentException("--etl-reset-domain <entity> is required.");
        if (!long.TryParse(Value(args, "--generation"), System.Globalization.NumberStyles.None, System.Globalization.CultureInfo.InvariantCulture, out var generation))
            throw new ArgumentException("--generation <observed watermark generation> is required.");
        var reason = Value(args, "--reason") ?? throw new ArgumentException("--reason \"<why the domain is reset>\" is required.");
        var reset = await store.ResetEtlWatermarkDomainAsync(new EtlWatermarkDomainResetRequest(entity, generation, operatorId, reason), DateTimeOffset.UtcNow, cancellationToken).ConfigureAwait(false);
        switch (reset)
        {
            case EtlWatermarkDomainResetOutcome.Reset done:
                Console.WriteLine($"Watermark domain of '{entity}' reset (archived generation {done.Record.PriorGeneration}, reset id {done.Record.ResetId:D}). The next extraction of '{entity}' must be a full baseline (start_full_sync or reload_entity).");
                return 0;
            case EtlWatermarkDomainResetOutcome.Refused refused:
                Console.WriteLine($"Watermark domain of '{entity}' was NOT reset: {refused.Reason}.");
                return 2;
            default:
                return 1;
        }
    }

    private static string? Value(string[] args, string name)
    {
        var index = Array.IndexOf(args, name);
        return index >= 0 && index + 1 < args.Length && !args[index + 1].StartsWith("--", StringComparison.Ordinal) ? args[index + 1] : null;
    }

    private static string ReadPassword()
    {
        var chars = new List<char>();
        while (true)
        {
            var key = Console.ReadKey(intercept: true);
            if (key.Key == ConsoleKey.Enter) break;
            if (key.Key == ConsoleKey.Backspace) { if (chars.Count > 0) chars.RemoveAt(chars.Count - 1); continue; }
            if (!char.IsControl(key.KeyChar)) chars.Add(key.KeyChar);
        }
        return new string(chars.ToArray());
    }
}

