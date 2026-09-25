using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using ErpOnecAgent.Infrastructure.Persistence.Sqlite;
using Microsoft.Data.Sqlite;
using Xunit;

namespace ErpOnecAgent.IntegrationTests;

/// <summary>
/// Migration review for 006_etl_finalize (A05b F1 dark storage): seeds a real v5 database
/// (immutable 001-005 schema + correct v1-v5 schema_migrations ledger rows +
/// Fixtures/v5-populated/populated-v5.sql) and asserts the additive upgrade to schema
/// version 6: every command, attempt, outbox row, ETL job, run/batch, watermark, state,
/// snapshot and conflict row survives byte-for-byte; the new watermarks columns get
/// generation=1 and NULL domain_fingerprint; etl_run_entities is created empty; the new
/// etl_runs seal/claim/finalize columns are NULL/defaults; and the migrator is idempotent
/// on rerun with stable SHA-256 checksums for all six migrations.
/// </summary>
public sealed class EtlFinalizeMigrationTests : IAsyncLifetime
{
    private const string LedgerAppliedAt = "2026-09-20T00:00:00.0000000+00:00";
    private const string RunColumnsV5 = "run_id,mode,requested_entities_json,status,started_at_utc,finished_at_utc,rows_read,batches_created,batches_acknowledged,error_count,last_error,configuration_version,created_at_utc,updated_at_utc,row_version";
    private const string BatchColumnsV7 = "batch_id,run_id,entity_name,schema_version,file_path,status,row_count,watermark_from_json,watermark_to_json,sha256,compressed_size,uncompressed_size,attempt_count,next_attempt_at_utc,created_at_utc,acknowledged_at_utc,last_error";
    private const string WatermarkColumnsV5 = "entity_name,committed_cursor_json,extracting_cursor_json,last_run_id,updated_at_utc";
    private const string JobColumnsV5 = "job_id,command_id,run_id,mode,entities_json,configuration_version,status,command_payload_hash,acceptance_result_json,created_at_utc,updated_at_utc,row_version";

    private readonly SqliteTestDatabase _database = new();
    private SqliteConnectionFactory _factory = null!;
    private SqliteMigrator _migrator = null!;
    private string[] _migrationSql = null!;

    public async Task InitializeAsync()
    {
        _migrationSql = new string[8];
        string[] names = ["001_initial.sql", "002_retry_budgets.sql", "003_ordering_claims.sql", "004_command_payload_conflicts.sql", "005_durable_etl_jobs.sql", "006_etl_finalize.sql", "007_etl_ownership.sql", "008_etl_send_attempts.sql"];
        for (var index = 0; index < names.Length; index++)
        {
            _migrationSql[index] = await File.ReadAllTextAsync(Path.Combine(AppContext.BaseDirectory, "Migrations", names[index]));
        }

        _factory = _database.CreateFactory(Path.Combine("data", "agent.db"));
        _migrator = new SqliteMigrator(_factory);

        // Seed an actual v5 database: 001-005 schema + correct v1-v5 ledger rows + populated v5 data.
        await using (var connection = await _factory.OpenAsync(CancellationToken.None))
        {
            await ExecuteAsync(connection, "CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY, name TEXT NOT NULL, checksum TEXT NOT NULL, applied_at_utc TEXT NOT NULL);");
            for (var index = 0; index < 5; index++)
            {
                await ExecuteAsync(connection, _migrationSql[index]);
                await InsertLedgerAsync(connection, index + 1, names[index], _migrationSql[index]);
            }
            await ExecuteAsync(connection, await File.ReadAllTextAsync(Path.Combine(AppContext.BaseDirectory, "Fixtures", "v5-populated", "populated-v5.sql")));
        }
        await SqliteTestDatabase.ClearPoolAsync(_factory);
    }

    public async Task DisposeAsync()
    {
        await _database.DisposeAsync();
    }

    [Fact]
    public async Task Populated_v5_upgrade_preserves_all_unfinished_work_and_is_idempotent()
    {
        var commandsBefore = await SnapshotRowsAsync("SELECT * FROM commands_inbox ORDER BY command_id;");
        var attemptsBefore = await SnapshotRowsAsync("SELECT * FROM command_attempts ORDER BY attempt_id;");
        var outboxBefore = await SnapshotRowsAsync("SELECT * FROM results_outbox ORDER BY result_id;");
        var runsBefore = await SnapshotRowsAsync($"SELECT {RunColumnsV5} FROM etl_runs ORDER BY run_id;");
        var batchesBefore = await SnapshotRowsAsync($"SELECT {BatchColumnsV7} FROM etl_batches ORDER BY batch_id;");
        var watermarksBefore = await SnapshotRowsAsync($"SELECT {WatermarkColumnsV5} FROM watermarks ORDER BY entity_name;");
        var jobsBefore = await SnapshotRowsAsync($"SELECT {JobColumnsV5} FROM etl_jobs ORDER BY job_id;");
        var stateBefore = await SnapshotRowsAsync("SELECT * FROM agent_state ORDER BY key;");
        var snapshotsBefore = await SnapshotRowsAsync("SELECT * FROM config_snapshots ORDER BY config_version;");
        var conflictsBefore = await SnapshotRowsAsync("SELECT * FROM command_payload_conflicts ORDER BY event_id;");
        var ledgerBefore = await SnapshotRowsAsync("SELECT version,name,checksum,applied_at_utc FROM schema_migrations WHERE version<=5 ORDER BY version;");

        await _migrator.ApplyAsync(CancellationToken.None);

        Assert.Equal(8, SqliteMigrator.CurrentSchemaVersion);
        Assert.Equal(8, await CountAsync("SELECT COUNT(*) FROM schema_migrations"));
        Assert.Equal(commandsBefore, await SnapshotRowsAsync("SELECT * FROM commands_inbox ORDER BY command_id;"));
        Assert.Equal(attemptsBefore, await SnapshotRowsAsync("SELECT * FROM command_attempts ORDER BY attempt_id;"));
        Assert.Equal(outboxBefore, await SnapshotRowsAsync("SELECT * FROM results_outbox ORDER BY result_id;"));
        Assert.Equal(runsBefore, await SnapshotRowsAsync($"SELECT {RunColumnsV5} FROM etl_runs ORDER BY run_id;"));
        Assert.Equal(batchesBefore, await SnapshotRowsAsync($"SELECT {BatchColumnsV7} FROM etl_batches ORDER BY batch_id;"));
        Assert.Equal(watermarksBefore, await SnapshotRowsAsync($"SELECT {WatermarkColumnsV5} FROM watermarks ORDER BY entity_name;"));
        Assert.Equal(jobsBefore, await SnapshotRowsAsync($"SELECT {JobColumnsV5} FROM etl_jobs ORDER BY job_id;"));
        Assert.Equal(stateBefore, await SnapshotRowsAsync("SELECT * FROM agent_state ORDER BY key;"));
        Assert.Equal(snapshotsBefore, await SnapshotRowsAsync("SELECT * FROM config_snapshots ORDER BY config_version;"));
        Assert.Equal(conflictsBefore, await SnapshotRowsAsync("SELECT * FROM command_payload_conflicts ORDER BY event_id;"));
        Assert.Equal(ledgerBefore, await SnapshotRowsAsync("SELECT version,name,checksum,applied_at_utc FROM schema_migrations WHERE version<=5 ORDER BY version;"));
        for (var version = 1; version <= 8; version++)
        {
            var expected = version <= 5 ? Checksum(_migrationSql[version - 1]) : PublishedChecksum(_migrationSql[version - 1]);
            Assert.Equal(expected, await ChecksumForVersionAsync(version));
        }

        // New durable storage exists; existing watermark rows keep their cursor bytes with
        // generation=1 and NULL fingerprint (unknown domain — fail closed, never adopted).
        Assert.Equal(1, await CountAsync("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='etl_run_entities'"));
        Assert.Equal(0, await CountAsync("SELECT COUNT(*) FROM etl_run_entities"));
        Assert.Equal(1, await CountAsync("SELECT COUNT(*) FROM watermarks"));
        Assert.Equal(1, await CountAsync("SELECT COUNT(*) FROM watermarks WHERE generation=1 AND domain_fingerprint IS NULL"));
        Assert.Equal(4, await CountAsync("SELECT COUNT(*) FROM etl_runs WHERE sealed_at_utc IS NULL AND sealed_entity_count IS NULL AND sealed_expected_batch_count IS NULL"));
        Assert.Equal(4, await CountAsync("SELECT COUNT(*) FROM etl_runs WHERE completion_claim_id IS NULL AND completion_claim_owner_id IS NULL AND completion_claim_acquired_at_utc IS NULL AND completion_attempt_count=0 AND completion_max_attempts IS NULL AND next_completion_attempt_at_utc IS NULL"));
        Assert.Equal(4, await CountAsync("SELECT COUNT(*) FROM etl_runs WHERE complete_payload_json IS NULL AND completion_acknowledged_at_utc IS NULL AND finalize_conflict_code IS NULL AND finalize_conflict_message IS NULL AND resolved_at_utc IS NULL"));

        var afterFirstRun = await SnapshotRowsAsync("SELECT version,name,checksum,applied_at_utc FROM schema_migrations ORDER BY version;");
        var entitiesAfterFirstRun = await SnapshotRowsAsync("SELECT * FROM etl_run_entities ORDER BY run_id,entity_name;");
        var runsAfterFirstRun = await SnapshotRowsAsync("SELECT * FROM etl_runs ORDER BY run_id;");

        await _migrator.ApplyAsync(CancellationToken.None);

        Assert.Equal(afterFirstRun, await SnapshotRowsAsync("SELECT version,name,checksum,applied_at_utc FROM schema_migrations ORDER BY version;"));
        Assert.Equal(entitiesAfterFirstRun, await SnapshotRowsAsync("SELECT * FROM etl_run_entities ORDER BY run_id,entity_name;"));
        Assert.Equal(runsAfterFirstRun, await SnapshotRowsAsync("SELECT * FROM etl_runs ORDER BY run_id;"));
        Assert.Equal(commandsBefore, await SnapshotRowsAsync("SELECT * FROM commands_inbox ORDER BY command_id;"));
        Assert.Equal(8, await CountAsync("SELECT COUNT(*) FROM schema_migrations"));
    }

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
            rows.Add(string.Join("", fields));
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
