using System.Security.Cryptography.X509Certificates;

namespace ErpOnecAgent.Infrastructure.Security;

public static class CertificateLoader
{
    public static X509Certificate2 LoadClientCertificate(string thumbprint)
    {
        var normalized = thumbprint.Replace(" ", string.Empty, StringComparison.Ordinal).ToUpperInvariant();
        using var store = new X509Store(StoreName.My, StoreLocation.LocalMachine);
        store.Open(OpenFlags.ReadOnly | OpenFlags.OpenExistingOnly);
        var certificate = store.Certificates.Find(X509FindType.FindByThumbprint, normalized, validOnly: false).OfType<X509Certificate2>().SingleOrDefault()
            ?? throw new InvalidOperationException($"Client certificate '{normalized}' was not found in LocalMachine\\My.");
        if (!certificate.HasPrivateKey) { certificate.Dispose(); throw new InvalidOperationException("Client certificate has no accessible private key."); }
        var now = DateTimeOffset.UtcNow;
        if (now < certificate.NotBefore || now > certificate.NotAfter) { certificate.Dispose(); throw new InvalidOperationException("Client certificate is outside its validity period."); }
        return certificate;
    }

    /// <summary>The certificate's NotAfter WITHOUT the validity check (expiry monitoring must
    /// see an already expired certificate); null when it is not found.</summary>
    public static DateTimeOffset? TryReadNotAfter(string thumbprint)
    {
        var normalized = thumbprint.Replace(" ", string.Empty, StringComparison.Ordinal).ToUpperInvariant();
        using var store = new X509Store(StoreName.My, StoreLocation.LocalMachine);
        store.Open(OpenFlags.ReadOnly | OpenFlags.OpenExistingOnly);
        using var certificate = store.Certificates.Find(X509FindType.FindByThumbprint, normalized, validOnly: false).OfType<X509Certificate2>().SingleOrDefault();
        return certificate is null ? null : new DateTimeOffset(certificate.NotAfter.ToUniversalTime(), TimeSpan.Zero);
    }
}
