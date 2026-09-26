using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using ErpOnecAgent.Infrastructure.Persistence.Sqlite;
using Microsoft.Data.Sqlite;
using Xunit;

namespace ErpOnecAgent.IntegrationTests;

/// <summary>
/// Migration checksum portability (pinned compatibility catalog for published
/// migrations 001-007): a durable ledger written by a build whose embedded
/// migration resources had LF bytes (the published git blob form) must be
/// accepted by a build whose resources have the historical CRLF bytes — and the
/// reverse — without rewriting any existing ledger row. Acceptance is bounded:
/// the embedded resource itself must hash to one of the pinned approved
/// variants, the stored ledger name and checksum must match the exact published
/// migration for that version, and unknown/other-migration/drifted hashes and
/// wrong names remain rejected with the durable database left unchanged.
/// New applications always record the canonical published (LF) checksum.
/// </summary>
public sealed class MigrationChecksumPortabilityTests : IAsyncLifetime
{
    private const string LedgerAppliedAt = "2026-09-20T00:00:00.0000000+00:00";

    private static readonly string[] Names =
    [
        "001_initial.sql", "002_retry_budgets.sql", "003_ordering_claims.sql",
        "004_command_payload_conflicts.sql", "005_durable_etl_jobs.sql",
        "006_etl_finalize.sql", "007_etl_ownership.sql", "008_etl_send_attempts.sql",
        "009_etl_scheduled_runs.sql", "010_etl_run_resolutions.sql",
        "011_watermark_domain_resets.sql",
        "012_etl_partial_runs.sql"
    ];

    // SHA-256 of the published git-blob (LF) bytes for each migration, from
    // repo_1c-agent `git show HEAD:...` at base 62e559d.
    private static readonly string[] PublishedLfChecksums =
    [
        "6BB8EC130CA7FDD303C08BEF28C117452613D768BA9A995D627F9D4E31F7959A",
        "95DA7777F172B465E11A8E7439F8A2D5FC7694B775A369D23A55A09610192F04",
        "C9F6EF47483F8ABEFE6266AE9044D1FF4F20F00EA09B81B882EF1BC8E238981B",
        "E75CAF3EF711BE74A91E15BE8FC467330B83C8F943C7C1626DDB38F91C23A6AE",
        "2D30F71101B88E26721EED688668C804FDF344E567363C5D7961055D41212657",
        "D1B20CE29BC87A9F962BD9842E5C2D613704540326D5F10AA2796CC85EBA6FB3",
        "8F4DBACBEE7C66603AD296C53182E8E108F680B7DA37FF5612345204D58B8658",
        "2984DC879A62064E03F76CA1F2369DEF62DDBD21C2231F382F5583EFB1C2F373",
        "C9A69671C4DE9471C03E073E6796D4F9C5C3BB18F03583FF3AE1B30964A9ED07",
        "AED834B63AC9051690F9DEFDD253A0BD003B026416BDF93934C536AC464DFE20",
        "6ABF179B5A13EE6928B72116099BCC9402584BE82062D107DE10B28B77BE2BEA",
        "7A597CDA38537AB2AC178962B5BBA427F413499F1036678624D5A40FE83D4B99"
    ];

    // SHA-256 of the historical CRLF checkout bytes for the four migrations that
    // differ from their git blobs in this worktree.
    private static readonly Dictionary<int, string> HistoricalCrlfChecksums = new()
    {
        [2] = "EB4D22E6A0B747B89FC09B8B9B2857C3AD893F1F15B9010BA38CDF6447CA3590",
        [3] = "0BF8E5122B6D094C6E65E0781A72B967FCE361130BEBE63F9D9CA47F3AAC44CB",
        [5] = "38BB53A4732A258A5B528BB1B2FEEB6E6E8A5ACB4743813F954BB09117911A34",
        [6] = "1634CFEB915BB7530F911104267575AF158F3446D828297D775FAA8693D3166A"
    };

    private readonly SqliteTestDatabase _database = new();
    private SqliteConnectionFactory _factory = null!;
    private SqliteMigrator _migrator = null!;
    private string[] _migrationSql = null!;
    private string[] _resourceChecksums = null!;

    public async Task InitializeAsync()
    {
        _migrationSql = new string[Names.Length];
        _resourceChecksums = new string[Names.Length];
        for (var index = 0; index < Names.Length; index++)
        {
            _migrationSql[index] = await File.ReadAllTextAsync(Path.Combine(AppContext.BaseDirectory, "Migrations", Names[index]));
            _resourceChecksums[index] = Checksum(_migrationSql[index]);
        }

        _factory = _database.CreateFactory(Path.Combine("data", "agent.db"));
        _migrator = new SqliteMigrator(_factory);
    }

    public async Task DisposeAsync()
    {
        await _database.DisposeAsync();
    }

    [Fact]
    public async Task Published_LF_ledger_is_accepted_by_this_build_and_preserved_byte_for_byte()
    {
        // Ledger written by a published/LF build: canonical checksums for v1-v6
        // plus real populated v6 schema and unfinished work.
        await SeedLedgerAsync(6, version => PublishedLfChecksums[version - 1]);

        var commandsBefore = await SnapshotRowsAsync("SELECT * FROM commands_inbox ORDER BY command_id;");
        var jobsBefore = await SnapshotRowsAsync("SELECT job_id,status FROM etl_jobs ORDER BY job_id;");
        var ledgerBefore = await SnapshotRowsAsync("SELECT version,name,checksum,applied_at_utc FROM schema_migrations ORDER BY version;");

        await _migrator.ApplyAsync(CancellationToken.None);

        Assert.Equal(12, await CountAsync("SELECT COUNT(*) FROM schema_migrations"));
        // Existing LF ledger rows retained unchanged; only the v7-v12 rows were appended.
        Assert.Equal(ledgerBefore, await SnapshotRowsAsync("SELECT version,name,checksum,applied_at_utc FROM schema_migrations WHERE version<=6 ORDER BY version;"));
        Assert.Equal(ledgerBefore.Count + 6, await CountAsync("SELECT COUNT(*) FROM schema_migrations"));
        Assert.Equal(PublishedLfChecksums[6], await ChecksumForVersionAsync(7));
        Assert.Equal(PublishedLfChecksums[7], await ChecksumForVersionAsync(8));
        Assert.Equal(PublishedLfChecksums[8], await ChecksumForVersionAsync(9));
        Assert.Equal(PublishedLfChecksums[9], await ChecksumForVersionAsync(10));
        Assert.Equal(PublishedLfChecksums[10], await ChecksumForVersionAsync(11));
        Assert.Equal("007_etl_ownership.sql", await NameForVersionAsync(7));
        Assert.Equal("008_etl_send_attempts.sql", await NameForVersionAsync(8));
        Assert.Equal("009_etl_scheduled_runs.sql", await NameForVersionAsync(9));
        Assert.Equal("010_etl_run_resolutions.sql", await NameForVersionAsync(10));
        Assert.Equal("011_watermark_domain_resets.sql", await NameForVersionAsync(11));
        Assert.Equal(PublishedLfChecksums[11], await ChecksumForVersionAsync(12));
        Assert.Equal("012_etl_partial_runs.sql", await NameForVersionAsync(12));
        Assert.Equal(commandsBefore, await SnapshotRowsAsync("SELECT * FROM commands_inbox ORDER BY command_id;"));
        Assert.Equal(jobsBefore, await SnapshotRowsAsync("SELECT job_id,status FROM etl_jobs ORDER BY job_id;"));
        Assert.Equal(1, await CountAsync("SELECT COUNT(*) FROM etl_jobs WHERE status='pending'"));

        var ledgerAfter = await SnapshotRowsAsync("SELECT version,name,checksum,applied_at_utc FROM schema_migrations ORDER BY version;");
        await _migrator.ApplyAsync(CancellationToken.None);
        Assert.Equal(ledgerAfter, await SnapshotRowsAsync("SELECT version,name,checksum,applied_at_utc FROM schema_migrations ORDER BY version;"));
        Assert.Equal(commandsBefore, await SnapshotRowsAsync("SELECT * FROM commands_inbox ORDER BY command_id;"));
    }

    [Fact]
    public async Task Historical_CRLF_ledger_is_accepted_and_preserved_byte_for_byte()
    {
        // Ledger written by a historical CRLF checkout build: the pinned
        // historical CRLF checksums for 002/003/005/006 and the published (LF)
        // checksums for 001/004 — explicit pinned values, independent of which
        // resource bytes this build embeds — plus real populated v6 data.
        await SeedLedgerAsync(6, version =>
            HistoricalCrlfChecksums.TryGetValue(version, out var historical) ? historical : PublishedLfChecksums[version - 1]);

        var commandsBefore = await SnapshotRowsAsync("SELECT * FROM commands_inbox ORDER BY command_id;");
        var ledgerBefore = await SnapshotRowsAsync("SELECT version,name,checksum,applied_at_utc FROM schema_migrations ORDER BY version;");

        await _migrator.ApplyAsync(CancellationToken.None);

        Assert.Equal(ledgerBefore, await SnapshotRowsAsync("SELECT version,name,checksum,applied_at_utc FROM schema_migrations WHERE version<=6 ORDER BY version;"));
        foreach (var (version, expected) in HistoricalCrlfChecksums)
        {
            Assert.Equal(expected, await ChecksumForVersionAsync(version));
        }
        Assert.Equal(PublishedLfChecksums[6], await ChecksumForVersionAsync(7));
        Assert.Equal(PublishedLfChecksums[7], await ChecksumForVersionAsync(8));
        Assert.Equal(PublishedLfChecksums[8], await ChecksumForVersionAsync(9));
        Assert.Equal(PublishedLfChecksums[9], await ChecksumForVersionAsync(10));
        Assert.Equal(PublishedLfChecksums[10], await ChecksumForVersionAsync(11));
        Assert.Equal(commandsBefore, await SnapshotRowsAsync("SELECT * FROM commands_inbox ORDER BY command_id;"));
    }

    [Fact]
    public async Task Fresh_apply_records_canonical_published_checksums_for_001_011()
    {
        await _migrator.ApplyAsync(CancellationToken.None);

        Assert.Equal(12, await CountAsync("SELECT COUNT(*) FROM schema_migrations"));
        for (var version = 1; version <= 12; version++)
        {
            Assert.Equal(PublishedLfChecksums[version - 1], await ChecksumForVersionAsync(version));
            Assert.Equal(Names[version - 1], await NameForVersionAsync(version));
        }
    }

    [Fact]
    public async Task Unknown_checksum_for_applied_version_is_rejected_and_leaves_database_unchanged()
    {
        await SeedLedgerAsync(6, version => _resourceChecksums[version - 1]);
        await OverwriteChecksumAsync(3, Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes("not-a-migration"))));

        var ledgerBefore = await SnapshotRowsAsync("SELECT version,name,checksum,applied_at_utc FROM schema_migrations ORDER BY version;");
        var commandsBefore = await SnapshotRowsAsync("SELECT * FROM commands_inbox ORDER BY command_id;");

        await Assert.ThrowsAsync<InvalidOperationException>(() => _migrator.ApplyAsync(CancellationToken.None));

        Assert.Equal(ledgerBefore, await SnapshotRowsAsync("SELECT version,name,checksum,applied_at_utc FROM schema_migrations ORDER BY version;"));
        Assert.Equal(commandsBefore, await SnapshotRowsAsync("SELECT * FROM commands_inbox ORDER BY command_id;"));
        Assert.Equal(0, await CountAsync("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='etl_entity_ownership'"));
    }

    [Fact]
    public async Task Checksum_from_another_migration_is_rejected_and_leaves_database_unchanged()
    {
        await SeedLedgerAsync(6, version => _resourceChecksums[version - 1]);
        await OverwriteChecksumAsync(4, _resourceChecksums[0]);

        var ledgerBefore = await SnapshotRowsAsync("SELECT version,name,checksum,applied_at_utc FROM schema_migrations ORDER BY version;");

        await Assert.ThrowsAsync<InvalidOperationException>(() => _migrator.ApplyAsync(CancellationToken.None));

        Assert.Equal(ledgerBefore, await SnapshotRowsAsync("SELECT version,name,checksum,applied_at_utc FROM schema_migrations ORDER BY version;"));
        Assert.Equal(0, await CountAsync("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='etl_entity_ownership'"));
    }

    [Fact]
    public async Task Meaningful_sql_drift_checksum_is_rejected_and_leaves_database_unchanged()
    {
        await SeedLedgerAsync(6, version => _resourceChecksums[version - 1]);
        var drifted = Checksum(_migrationSql[1] + "\nALTER TABLE commands_inbox ADD COLUMN injected TEXT;\n");
        await OverwriteChecksumAsync(2, drifted);

        var ledgerBefore = await SnapshotRowsAsync("SELECT version,name,checksum,applied_at_utc FROM schema_migrations ORDER BY version;");

        await Assert.ThrowsAsync<InvalidOperationException>(() => _migrator.ApplyAsync(CancellationToken.None));

        Assert.Equal(ledgerBefore, await SnapshotRowsAsync("SELECT version,name,checksum,applied_at_utc FROM schema_migrations ORDER BY version;"));
        Assert.Equal(0, await CountAsync("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='etl_entity_ownership'"));
    }

    [Fact]
    public async Task Wrong_ledger_name_for_applied_version_is_rejected_and_leaves_database_unchanged()
    {
        await SeedLedgerAsync(6, version => _resourceChecksums[version - 1]);
        await OverwriteNameAsync(2, "002_renamed.sql");

        var ledgerBefore = await SnapshotRowsAsync("SELECT version,name,checksum,applied_at_utc FROM schema_migrations ORDER BY version;");

        await Assert.ThrowsAsync<InvalidOperationException>(() => _migrator.ApplyAsync(CancellationToken.None));

        Assert.Equal(ledgerBefore, await SnapshotRowsAsync("SELECT version,name,checksum,applied_at_utc FROM schema_migrations ORDER BY version;"));
        Assert.Equal(0, await CountAsync("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='etl_entity_ownership'"));
    }

    [Fact]
    public async Task Populated_v7_with_unfinished_work_survives_idempotent_rerun()
    {
        await _migrator.ApplyAsync(CancellationToken.None);
        await using (var connection = await _factory.OpenAsync(CancellationToken.None))
        {
            await ExecuteAsync(connection, "INSERT INTO commands_inbox(command_id,command_type,payload_version,priority,payload_json,payload_hash,status,created_at_utc,received_at_utc) VALUES('cmd-p7','noop',1,0,'{}','hash-p7','pending','2026-09-25T00:00:00.0000000+00:00','2026-09-25T00:00:00.0000000+00:00');");
        }

        var commandsBefore = await SnapshotRowsAsync("SELECT * FROM commands_inbox ORDER BY command_id;");
        var ledgerBefore = await SnapshotRowsAsync("SELECT version,name,checksum,applied_at_utc FROM schema_migrations ORDER BY version;");

        await _migrator.ApplyAsync(CancellationToken.None);

        Assert.Equal(ledgerBefore, await SnapshotRowsAsync("SELECT version,name,checksum,applied_at_utc FROM schema_migrations ORDER BY version;"));
        Assert.Equal(commandsBefore, await SnapshotRowsAsync("SELECT * FROM commands_inbox ORDER BY command_id;"));
        Assert.Equal(1, await CountAsync("SELECT COUNT(*) FROM commands_inbox WHERE command_id='cmd-p7' AND status='pending'"));
    }

    [Fact]
    public void Migration_resource_bytes_001_through_011_are_sha256_pinned_to_the_published_catalog()
    {
        // Every shipped migration file is immutable — its raw bytes hash to the published
        // canonical LF checksum, or (only for 002/003/005/006) to the known historical
        // CRLF variant of a pre-.gitattributes checkout. 008, 009, 010 and 011 have no
        // CRLF variant and must be byte-exact LF in every checkout.
        for (var index = 0; index < Names.Length; index++)
        {
            var version = index + 1;
            var accepted = _resourceChecksums[index] == PublishedLfChecksums[index]
                || (HistoricalCrlfChecksums.TryGetValue(version, out var crlf) && _resourceChecksums[index] == crlf);
            Assert.True(accepted, $"{Names[index]} resource checksum {_resourceChecksums[index]} is neither the published LF hash nor a known historical CRLF variant.");
        }
        Assert.Equal(PublishedLfChecksums[7], _resourceChecksums[7]);
        Assert.Equal(PublishedLfChecksums[8], _resourceChecksums[8]);
        Assert.Equal(PublishedLfChecksums[9], _resourceChecksums[9]);
        Assert.Equal(PublishedLfChecksums[10], _resourceChecksums[10]);
        Assert.Equal(PublishedLfChecksums[11], _resourceChecksums[11]);
    }

    private async Task SeedLedgerAsync(int throughVersion, Func<int, string> checksumFor)
    {
        await using (var connection = await _factory.OpenAsync(CancellationToken.None))
        {
            await ExecuteAsync(connection, "CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY, name TEXT NOT NULL, checksum TEXT NOT NULL, applied_at_utc TEXT NOT NULL);");
            for (var index = 0; index < throughVersion; index++)
            {
                await ExecuteAsync(connection, _migrationSql[index]);
                await InsertLedgerAsync(connection, index + 1, Names[index], checksumFor(index + 1));
            }
            await ExecuteAsync(connection, await File.ReadAllTextAsync(Path.Combine(AppContext.BaseDirectory, "Fixtures", $"v{throughVersion}-populated", $"populated-v{throughVersion}.sql")));
        }
        await SqliteTestDatabase.ClearPoolAsync(_factory);
    }

    private static string Checksum(string sql) => Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(sql)));

    private static async Task InsertLedgerAsync(SqliteConnection connection, int version, string name, string checksum)
    {
        await using var record = connection.CreateCommand();
        record.CommandText = "INSERT INTO schema_migrations(version,name,checksum,applied_at_utc) VALUES($version,$name,$checksum,$appliedAt);";
        record.Parameters.AddWithValue("$version", version);
        record.Parameters.AddWithValue("$name", name);
        record.Parameters.AddWithValue("$checksum", checksum);
        record.Parameters.AddWithValue("$appliedAt", LedgerAppliedAt);
        await record.ExecuteNonQueryAsync(CancellationToken.None);
    }

    private async Task OverwriteChecksumAsync(int version, string checksum)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = "UPDATE schema_migrations SET checksum=$checksum WHERE version=$version;";
        command.Parameters.AddWithValue("$checksum", checksum);
        command.Parameters.AddWithValue("$version", version);
        Assert.Equal(1, await command.ExecuteNonQueryAsync(CancellationToken.None));
    }

    private async Task OverwriteNameAsync(int version, string name)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = "UPDATE schema_migrations SET name=$name WHERE version=$version;";
        command.Parameters.AddWithValue("$name", name);
        command.Parameters.AddWithValue("$version", version);
        Assert.Equal(1, await command.ExecuteNonQueryAsync(CancellationToken.None));
    }

    private async Task<IReadOnlyList<string>> SnapshotRowsAsync(string sql)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        var rows = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(CancellationToken.None);
        while (await reader.ReadAsync(CancellationToken.None))
        {
            var fields = new string[reader.FieldCount];
            for (var index = 0; index < reader.FieldCount; index++)
            {
                fields[index] = reader.IsDBNull(index) ? "<null>" : Convert.ToString(reader.GetValue(index), CultureInfo.InvariantCulture) ?? string.Empty;
            }
            rows.Add(string.Join("\u001F", fields));
        }
        return rows;
    }

    private async Task<long> CountAsync(string sql)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        return Convert.ToInt64(await command.ExecuteScalarAsync(CancellationToken.None), CultureInfo.InvariantCulture);
    }

    private async Task<string?> ChecksumForVersionAsync(int version)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT checksum FROM schema_migrations WHERE version=$version;";
        command.Parameters.AddWithValue("$version", version);
        return await command.ExecuteScalarAsync(CancellationToken.None) as string;
    }

    private async Task<string?> NameForVersionAsync(int version)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT name FROM schema_migrations WHERE version=$version;";
        command.Parameters.AddWithValue("$version", version);
        return await command.ExecuteScalarAsync(CancellationToken.None) as string;
    }

    private static async Task ExecuteAsync(SqliteConnection connection, string sql)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync(CancellationToken.None);
    }
}
