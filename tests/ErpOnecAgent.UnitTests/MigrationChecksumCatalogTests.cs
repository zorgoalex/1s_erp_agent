using System.Security.Cryptography;
using System.Text;
using ErpOnecAgent.Infrastructure.Persistence.Sqlite;
using Xunit;

namespace ErpOnecAgent.UnitTests;

/// <summary>
/// Pinned checksum compatibility catalog for published migrations 001-007:
/// exact (version, file name) keys, canonical published (LF) checksums recorded
/// for new applications, historical CRLF variants admitted for existing
/// ledgers, and fail-closed rejection of any drifted resource bytes, foreign
/// checksums, or renamed files. Unlisted versions keep the strict raw-hash
/// contract.
/// </summary>
public sealed class MigrationChecksumCatalogTests
{
    private const string V1Published = "6BB8EC130CA7FDD303C08BEF28C117452613D768BA9A995D627F9D4E31F7959A";
    private const string V2Published = "95DA7777F172B465E11A8E7439F8A2D5FC7694B775A369D23A55A09610192F04";
    private const string V2Crlf = "EB4D22E6A0B747B89FC09B8B9B2857C3AD893F1F15B9010BA38CDF6447CA3590";
    private const string V3Crlf = "0BF8E5122B6D094C6E65E0781A72B967FCE361130BEBE63F9D9CA47F3AAC44CB";
    private const string V5Crlf = "38BB53A4732A258A5B528BB1B2FEEB6E6E8A5ACB4743813F954BB09117911A34";
    private const string V6Crlf = "1634CFEB915BB7530F911104267575AF158F3446D828297D775FAA8693D3166A";
    private const string V7Published = "8F4DBACBEE7C66603AD296C53182E8E108F680B7DA37FF5612345204D58B8658";
    private const string V9Published = "C9A69671C4DE9471C03E073E6796D4F9C5C3BB18F03583FF3AE1B30964A9ED07";
    private const string V10Published = "AED834B63AC9051690F9DEFDD253A0BD003B026416BDF93934C536AC464DFE20";

    [Fact]
    public void Cataloged_resource_variants_resolve_to_canonical_published_checksum()
    {
        Assert.Equal(V2Published, MigrationChecksumCatalog.ResolveRecordedChecksum(2, "002_retry_budgets.sql", V2Published));
        Assert.Equal(V2Published, MigrationChecksumCatalog.ResolveRecordedChecksum(2, "002_retry_budgets.sql", V2Crlf));
        Assert.Equal(V1Published, MigrationChecksumCatalog.ResolveRecordedChecksum(1, "001_initial.sql", V1Published));
        Assert.Equal(V7Published, MigrationChecksumCatalog.ResolveRecordedChecksum(7, "007_etl_ownership.sql", V7Published));
        Assert.Equal(V9Published, MigrationChecksumCatalog.ResolveRecordedChecksum(9, "009_etl_scheduled_runs.sql", V9Published));
        Assert.Equal(V10Published, MigrationChecksumCatalog.ResolveRecordedChecksum(10, "010_etl_run_resolutions.sql", V10Published));
    }

    [Fact]
    public void Drifted_cataloged_resource_checksum_fails_closed()
    {
        var drifted = Hash("drifted");
        Assert.Throws<InvalidOperationException>(() => MigrationChecksumCatalog.ResolveRecordedChecksum(2, "002_retry_budgets.sql", drifted));
        Assert.Throws<InvalidOperationException>(() => MigrationChecksumCatalog.ResolveRecordedChecksum(7, "007_etl_ownership.sql", V2Crlf));
    }

    [Fact]
    public void Renamed_cataloged_resource_fails_closed()
    {
        Assert.Throws<InvalidOperationException>(() => MigrationChecksumCatalog.ResolveRecordedChecksum(2, "002_renamed.sql", V2Published));
    }

    [Fact]
    public void Unlisted_version_uses_raw_resource_checksum()
    {
        var raw = Hash("future-migration");
        Assert.Equal(raw, MigrationChecksumCatalog.ResolveRecordedChecksum(11, "011_future.sql", raw));
    }

    [Fact]
    public void Stored_checksum_accepts_only_approved_variants_for_cataloged_versions()
    {
        Assert.True(MigrationChecksumCatalog.IsAcceptedStoredChecksum(2, "002_retry_budgets.sql", V2Crlf, V2Published));
        Assert.True(MigrationChecksumCatalog.IsAcceptedStoredChecksum(2, "002_retry_budgets.sql", V2Published, V2Crlf));
        Assert.True(MigrationChecksumCatalog.IsAcceptedStoredChecksum(3, "003_ordering_claims.sql", V3Crlf, V3Crlf));
        Assert.True(MigrationChecksumCatalog.IsAcceptedStoredChecksum(5, "005_durable_etl_jobs.sql", V5Crlf, V5Crlf));
        Assert.True(MigrationChecksumCatalog.IsAcceptedStoredChecksum(6, "006_etl_finalize.sql", V6Crlf, V6Crlf));

        Assert.False(MigrationChecksumCatalog.IsAcceptedStoredChecksum(2, "002_retry_budgets.sql", V2Crlf, V1Published));
        Assert.False(MigrationChecksumCatalog.IsAcceptedStoredChecksum(2, "002_retry_budgets.sql", V2Crlf, V3Crlf));
        Assert.False(MigrationChecksumCatalog.IsAcceptedStoredChecksum(2, "002_retry_budgets.sql", V2Crlf, Hash("drifted")));
        Assert.False(MigrationChecksumCatalog.IsAcceptedStoredChecksum(2, "002_other.sql", V2Published, V2Published));
    }

    [Fact]
    public void Stored_checksum_for_cataloged_version_rejects_unapproved_resource_checksum()
    {
        // The helper itself enforces the resource pin — it must not accept a
        // ledger row merely because the stored checksum is approved when the
        // embedded resource has drifted to unapproved bytes.
        Assert.False(MigrationChecksumCatalog.IsAcceptedStoredChecksum(2, "002_retry_budgets.sql", Hash("drifted"), V2Published));
        Assert.False(MigrationChecksumCatalog.IsAcceptedStoredChecksum(2, "002_retry_budgets.sql", Hash("drifted"), V2Crlf));
        Assert.False(MigrationChecksumCatalog.IsAcceptedStoredChecksum(2, "002_retry_budgets.sql", V3Crlf, V2Published));
    }

    [Fact]
    public void Stored_checksum_for_unlisted_version_must_equal_raw_resource_checksum()
    {
        var raw = Hash("future-migration");
        Assert.True(MigrationChecksumCatalog.IsAcceptedStoredChecksum(11, "011_future.sql", raw, raw));
        Assert.False(MigrationChecksumCatalog.IsAcceptedStoredChecksum(11, "011_future.sql", raw, V2Published));
    }

    [Fact]
    public void Catalog_checksums_match_shipped_embedded_migration_resources()
    {
        // The cataloged approved hashes must be exactly the hashes this build's
        // embedded resources produce — and vice versa, every embedded migration
        // 001-010 must be cataloged and approved.
        var assembly = typeof(SqliteMigrator).Assembly;
        var resources = assembly.GetManifestResourceNames()
            .Where(static name => name.Contains(".Persistence.Migrations.", StringComparison.Ordinal) && name.EndsWith(".sql", StringComparison.Ordinal))
            .OrderBy(static name => name, StringComparer.Ordinal)
            .ToArray();
        Assert.Equal(10, resources.Length);

        foreach (var resource in resources)
        {
            var marker = resource.LastIndexOf(".Migrations.", StringComparison.Ordinal) + ".Migrations.".Length;
            var fileName = resource[marker..];
            var version = int.Parse(fileName.AsSpan(0, fileName.IndexOf('_', StringComparison.Ordinal)), System.Globalization.CultureInfo.InvariantCulture);
            using var stream = assembly.GetManifestResourceStream(resource)!;
            using var reader = new StreamReader(stream);
            var checksum = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(reader.ReadToEnd())));
            var recorded = MigrationChecksumCatalog.ResolveRecordedChecksum(version, fileName, checksum);
            Assert.True(MigrationChecksumCatalog.IsAcceptedStoredChecksum(version, fileName, checksum, recorded));
        }
    }

    private static string Hash(string text) => Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(text)));
}
