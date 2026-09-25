using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using ErpOnecAgent.Infrastructure.Persistence.Sqlite;
using Microsoft.Data.Sqlite;
using Xunit;

namespace ErpOnecAgent.IntegrationTests;

/// <summary>
/// Migration review for 005_durable_etl_jobs: seeds a real v4 database (immutable 001-004 schema +
/// correct v1-v4 schema_migrations ledger rows + Fixtures/v4-populated/populated-v4.sql) and asserts
/// the additive upgrade to schema version 5: every unfinished command, attempt, outbox row, ETL
/// run/batch, watermark, state, snapshot and conflict row survives byte-for-byte; the new etl_runs
/// columns backfill deterministically; etl_jobs is created empty; and the migrator is idempotent on
/// rerun with stable SHA-256 checksums for all five migrations.
/// </summary>
public sealed class EtlJobMigrationTests : IAsyncLifetime
{
    private const string LedgerAppliedAt = "2026-09-20T00:00:00.0000000+00:00";

    private readonly SqliteTestDatabase _database = new();
    private SqliteConnectionFactory _factory = null!;
    private string _migration001Sql = null!;
    private string _migration002Sql = null!;
    private string _migration003Sql = null!;
    private string _migration004Sql = null!;
    private string _migration005Sql = null!;
    private string _migration006Sql = null!;
    private string _migration007Sql = null!;

    public async Task InitializeAsync()
    {
        _migration001Sql = await ReadMigrationAsync("001_initial.sql");
        _migration002Sql = await ReadMigrationAsync("002_retry_budgets.sql");
        _migration003Sql = await ReadMigrationAsync("003_ordering_claims.sql");
        _migration004Sql = await ReadMigrationAsync("004_command_payload_conflicts.sql");
        _migration005Sql = await ReadMigrationAsync("005_durable_etl_jobs.sql");
        _migration006Sql = await ReadMigrationAsync("006_etl_finalize.sql");
        _migration007Sql = await ReadMigrationAsync("007_etl_ownership.sql");
        _factory = _database.CreateFactory(Path.Combine("data", "agent.db"));

        // Seed an actual v4 database: 001-004 schema + correct v1-v4 ledger rows + populated v4 data.
        await using (var connection = await _factory.OpenAsync(CancellationToken.None))
        {
            await ExecuteAsync(connection, "CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY, name TEXT NOT NULL, checksum TEXT NOT NULL, applied_at_utc TEXT NOT NULL);");
            await ExecuteAsync(connection, _migration001Sql);
            await ExecuteAsync(connection, _migration002Sql);
            await InsertLedgerAsync(connection, 1, "001_initial.sql", _migration001Sql);
            await InsertLedgerAsync(connection, 2, "002_retry_budgets.sql", _migration002Sql);
            await ExecuteAsync(connection, _migration003Sql);
            await InsertLedgerAsync(connection, 3, "003_ordering_claims.sql", _migration003Sql);
            await ExecuteAsync(connection, _migration004Sql);
            await InsertLedgerAsync(connection, 4, "004_command_payload_conflicts.sql", _migration004Sql);
            await ExecuteAsync(connection, await File.ReadAllTextAsync(Path.Combine(AppContext.BaseDirectory, "Fixtures", "v4-populated", "populated-v4.sql")));
        }
        await SqliteTestDatabase.ClearPoolAsync(_factory);
    }

    public async Task DisposeAsync()
    {
        await _database.DisposeAsync();
    }

    [Fact]
    public async Task Populated_v4_upgrade_preserves_all_unfinished_work_and_is_idempotent()
    {
        var commandsBefore = await SnapshotRowsAsync("SELECT * FROM commands_inbox ORDER BY command_id;");
        var attemptsBefore = await SnapshotRowsAsync("SELECT * FROM command_attempts ORDER BY attempt_id;");
        var outboxBefore = await SnapshotRowsAsync("SELECT * FROM results_outbox ORDER BY result_id;");
        var runsBefore = await SnapshotRowsAsync("SELECT run_id,mode,requested_entities_json,status,started_at_utc,finished_at_utc,rows_read,batches_created,batches_acknowledged,error_count,last_error FROM etl_runs ORDER BY run_id;");
        var batchesBefore = await SnapshotRowsAsync("SELECT * FROM etl_batches ORDER BY batch_id;");
        var watermarksBefore = await SnapshotRowsAsync("SELECT entity_name,committed_cursor_json,extracting_cursor_json,last_run_id,updated_at_utc FROM watermarks ORDER BY entity_name;");
        var stateBefore = await SnapshotRowsAsync("SELECT * FROM agent_state ORDER BY key;");
        var snapshotsBefore = await SnapshotRowsAsync("SELECT * FROM config_snapshots ORDER BY config_version;");
        var conflictsBefore = await SnapshotRowsAsync("SELECT * FROM command_payload_conflicts ORDER BY event_id;");
        var ledgerBefore = await SnapshotRowsAsync("SELECT version,name,checksum,applied_at_utc FROM schema_migrations WHERE version<=4 ORDER BY version;");
        var migrator = new SqliteMigrator(_factory);

        await migrator.ApplyAsync(CancellationToken.None);

        Assert.Equal(7, SqliteMigrator.CurrentSchemaVersion);
        Assert.Equal(7, await CountAsync("SELECT COUNT(*) FROM schema_migrations"));
        Assert.Equal(commandsBefore, await SnapshotRowsAsync("SELECT * FROM commands_inbox ORDER BY command_id;"));
        Assert.Equal(attemptsBefore, await SnapshotRowsAsync("SELECT * FROM command_attempts ORDER BY attempt_id;"));
        Assert.Equal(outboxBefore, await SnapshotRowsAsync("SELECT * FROM results_outbox ORDER BY result_id;"));
        Assert.Equal(runsBefore, await SnapshotRowsAsync("SELECT run_id,mode,requested_entities_json,status,started_at_utc,finished_at_utc,rows_read,batches_created,batches_acknowledged,error_count,last_error FROM etl_runs ORDER BY run_id;"));
        Assert.Equal(batchesBefore, await SnapshotRowsAsync("SELECT * FROM etl_batches ORDER BY batch_id;"));
        Assert.Equal(watermarksBefore, await SnapshotRowsAsync("SELECT entity_name,committed_cursor_json,extracting_cursor_json,last_run_id,updated_at_utc FROM watermarks ORDER BY entity_name;"));
        // Additive v6 defaults on the pre-existing watermark row.
        Assert.Equal(1, await CountAsync("SELECT COUNT(*) FROM watermarks WHERE generation=1 AND domain_fingerprint IS NULL"));
        Assert.Equal(stateBefore, await SnapshotRowsAsync("SELECT * FROM agent_state ORDER BY key;"));
        Assert.Equal(snapshotsBefore, await SnapshotRowsAsync("SELECT * FROM config_snapshots ORDER BY config_version;"));
        Assert.Equal(conflictsBefore, await SnapshotRowsAsync("SELECT * FROM command_payload_conflicts ORDER BY event_id;"));
        Assert.Equal(ledgerBefore, await SnapshotRowsAsync("SELECT version,name,checksum,applied_at_utc FROM schema_migrations WHERE version<=4 ORDER BY version;"));
        Assert.Equal(Checksum(_migration001Sql), await ChecksumForVersionAsync(1));
        Assert.Equal(Checksum(_migration002Sql), await ChecksumForVersionAsync(2));
        Assert.Equal(Checksum(_migration003Sql), await ChecksumForVersionAsync(3));
        Assert.Equal(Checksum(_migration004Sql), await ChecksumForVersionAsync(4));
        Assert.Equal(PublishedChecksum(_migration005Sql), await ChecksumForVersionAsync(5));
        Assert.Equal(PublishedChecksum(_migration006Sql), await ChecksumForVersionAsync(6));
        Assert.Equal(PublishedChecksum(_migration007Sql), await ChecksumForVersionAsync(7));

        // New durable storage exists and is empty; new pending-run columns backfill deterministically.
        Assert.Equal(1, await CountAsync("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='etl_jobs'"));
        Assert.Equal(0, await CountAsync("SELECT COUNT(*) FROM etl_jobs"));
        Assert.Equal(0, await CountAsync("SELECT COUNT(*) FROM pragma_foreign_key_list('etl_jobs') WHERE \"table\"<>'etl_runs'"));
        Assert.Equal(3, await CountAsync("SELECT COUNT(*) FROM etl_runs"));
        Assert.Equal(3, await CountAsync("SELECT COUNT(*) FROM etl_runs WHERE created_at_utc=started_at_utc"));
        Assert.Equal(3, await CountAsync("SELECT COUNT(*) FROM etl_runs WHERE updated_at_utc=COALESCE(finished_at_utc,started_at_utc)"));
        Assert.Equal(3, await CountAsync("SELECT COUNT(*) FROM etl_runs WHERE row_version=1 AND configuration_version IS NULL"));
        Assert.Equal(7, await CountAsync("SELECT COUNT(*) FROM commands_inbox WHERE status IN ('queued','retry_waiting','unknown_result','executing','result_pending','completed')"));
        Assert.Equal(1, await CountAsync("SELECT COUNT(*) FROM results_outbox WHERE status='pending' AND command_id='00000000-0000-0000-0000-000000000505'"));

        var afterFirstRun = await SnapshotRowsAsync("SELECT version,name,checksum,applied_at_utc FROM schema_migrations ORDER BY version;");
        var jobsAfterFirstRun = await SnapshotRowsAsync("SELECT * FROM etl_jobs ORDER BY job_id;");

        await migrator.ApplyAsync(CancellationToken.None);

        Assert.Equal(afterFirstRun, await SnapshotRowsAsync("SELECT version,name,checksum,applied_at_utc FROM schema_migrations ORDER BY version;"));
        Assert.Equal(jobsAfterFirstRun, await SnapshotRowsAsync("SELECT * FROM etl_jobs ORDER BY job_id;"));
        Assert.Equal(commandsBefore, await SnapshotRowsAsync("SELECT * FROM commands_inbox ORDER BY command_id;"));
        Assert.Equal(7, await CountAsync("SELECT COUNT(*) FROM schema_migrations"));
    }

    private static async Task<string> ReadMigrationAsync(string fileName) =>
        await File.ReadAllTextAsync(Path.Combine(AppContext.BaseDirectory, "Migrations", fileName));

    private static string Checksum(string sql) => Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(sql)));

    // Migrations applied now record the canonical published (git-blob LF)
    // checksum; seeded ledger rows keep the seeded checksum byte-for-byte.
    private static string PublishedChecksum(string sql) => Checksum(sql.Replace("\r\n", "\n", StringComparison.Ordinal));

    private static async Task InsertLedgerAsync(SqliteConnection connection, int version, string name, string sql)
    {
        await using var record = connection.CreateCommand();
        record.CommandText = "INSERT INTO schema_migrations(version,name,checksum,applied_at_utc) VALUES($version,$name,$checksum,$appliedAt);";
        record.Parameters.AddWithValue("$version", version);
        record.Parameters.AddWithValue("$name", name);
        record.Parameters.AddWithValue("$checksum", Checksum(sql));
        record.Parameters.AddWithValue("$appliedAt", LedgerAppliedAt);
        await record.ExecuteNonQueryAsync(CancellationToken.None);
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

    private static async Task ExecuteAsync(SqliteConnection connection, string sql)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync(CancellationToken.None);
    }
}
