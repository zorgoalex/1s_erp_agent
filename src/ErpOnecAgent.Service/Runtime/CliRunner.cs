using System.Text.Json;
using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Application.Configuration;
using ErpOnecAgent.Infrastructure.Security;
using ErpOnecAgent.Service.Diagnostics;
using Microsoft.Extensions.Options;

namespace ErpOnecAgent.Service.Runtime;

public static class CliRunner
{
    public static bool HasCommand(string[] args) => args.Any(static arg => arg is "--validate-config" or "--test-erp" or "--test-onec" or "--migrate" or "--integrity-check" or "--collect-diagnostics" or "--store-onec-credential" or "--version");

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
        if (args.Contains("--migrate", StringComparer.Ordinal)) { Console.WriteLine("SQLite migrations applied."); return 0; }
        if (args.Contains("--integrity-check", StringComparer.Ordinal)) { var result = await store.IntegrityCheckAsync(cancellationToken).ConfigureAwait(false); Console.WriteLine(result); return string.Equals(result, "ok", StringComparison.OrdinalIgnoreCase) ? 0 : 2; }
        if (args.Contains("--test-erp", StringComparer.Ordinal)) { var session = await services.GetRequiredService<ErpSessionManager>().GetSessionAsync(cancellationToken).ConfigureAwait(false); Console.WriteLine($"ERP handshake succeeded. Session: {session:D}"); return 0; }
        if (args.Contains("--test-onec", StringComparer.Ordinal)) { var result = await services.GetRequiredService<IOnecHealthClient>().CheckAsync(cancellationToken).ConfigureAwait(false); Console.WriteLine(result is null ? "1C health check failed." : "1C health check succeeded."); return result is null ? 2 : 0; }
        if (args.Contains("--collect-diagnostics", StringComparer.Ordinal)) { Console.WriteLine(await services.GetRequiredService<DiagnosticsCollector>().CollectAsync(cancellationToken).ConfigureAwait(false)); return 0; }
        return 1;
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

