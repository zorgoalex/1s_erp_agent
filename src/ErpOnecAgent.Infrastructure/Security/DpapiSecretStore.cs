using System.Security.Cryptography;
using System.Text;
using ErpOnecAgent.Application.Abstractions;

namespace ErpOnecAgent.Infrastructure.Security;

public sealed class DpapiSecretStore(string secretDirectory) : ISecretStore
{
    private readonly string _directory = Path.GetFullPath(secretDirectory);

    public async Task SaveAsync(string name, string secret, CancellationToken cancellationToken)
    {
        if (!OperatingSystem.IsWindows()) throw new PlatformNotSupportedException("DPAPI secret storage is available only on Windows.");
        Directory.CreateDirectory(_directory);
        var bytes = ProtectedData.Protect(Encoding.UTF8.GetBytes(secret), Encoding.UTF8.GetBytes(name), DataProtectionScope.LocalMachine);
        await File.WriteAllBytesAsync(GetPath(name), bytes, cancellationToken).ConfigureAwait(false);
    }

    public async Task<string?> ReadAsync(string name, CancellationToken cancellationToken)
    {
        if (!OperatingSystem.IsWindows()) throw new PlatformNotSupportedException("DPAPI secret storage is available only on Windows.");
        var path = GetPath(name); if (!File.Exists(path)) return null;
        var bytes = await File.ReadAllBytesAsync(path, cancellationToken).ConfigureAwait(false);
        return Encoding.UTF8.GetString(ProtectedData.Unprotect(bytes, Encoding.UTF8.GetBytes(name), DataProtectionScope.LocalMachine));
    }

    private string GetPath(string name)
    {
        if (string.IsNullOrWhiteSpace(name) || name.Any(c => !char.IsLetterOrDigit(c) && c is not '-' and not '_')) throw new ArgumentException("Secret name contains unsupported characters.", nameof(name));
        return Path.Combine(_directory, name + ".dpapi");
    }
}
