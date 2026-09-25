using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using ErpOnecAgent.Infrastructure.Persistence.Sqlite;
using Microsoft.Data.Sqlite;
using Xunit;

namespace ErpOnecAgent.IntegrationTests;

/// <summary>
/// Migration review for 007_etl_ownership (O1 dark storage): seeds a real v6 database
/// (immutable 001-006 schema + correct v1-v6 schema_migrations ledger rows +
/// Fixtures/v6-populated/populated-v6.sql) and asserts the additive upgrade to schema
/// version 7: every command, attempt, outbox, ETL job, run/entity/batch, watermark,
/// state, snapshot and conflict row survives byte-for-byte; etl_entity_ownership and
/// etl_run_ownership_bindings are created empty; the new etl_jobs dispatch/deferral and
/// etl_runs extraction-claim columns get their documented NULL/zero defaults; and the
/// migrator is idempotent on rerun with stable SHA-256 checksums for all seven
/// migrations.
/// </summary>
public sealed class EtlOwnershipMigrationTests : IAsyncLifetime
{
    private const string LedgerAppliedAt = "2026-09-20T00:00:00.0000000+00:00";
    private const string RunColumnsV6 = "run_id,mode,requested_entities_json,status,started_at_utc,finished_at_utc,rows_read,batches_created,batches_acknowledged,error_count,last_error,configuration_version,created_at_utc,updated_at_utc,row_version,sealed_at_utc,sealed_entity_count,sealed_expected_batch_count,completion_claim_id,completion_claim_owner_id,completion_claim_acquired_at_utc,completion_attempt_count,completion_max_attempts,next_completion_attempt_at_utc,complete_payload_json,completion_acknowledged_at_utc,finalize_conflict_code,finalize_conflict_message,resolved_at_utc";
    private const string BatchColumnsV7 = "batch_id,run_id,entity_name,schema_version,file_path,status,row_count,watermark_from_json,watermark_to_json,sha256,compressed_size,uncompressed_size,attempt_count,next_attempt_at_utc,created_at_utc,acknowledged_at_utc,last_error";
    private const string JobColumnsV5 = "job_id,command_id,run_id,mode,entities_json,configuration_version,status,command_payload_hash,acceptance_result_json,created_at_utc,updated_at_utc,row_version";

    private readonly SqliteTestDatabase _database = new();
    private SqliteConnectionFactory _factory = null!;
    private SqliteMigrator _migrator = null!;
    private string[] _migrationSql = null!;

    public async Task InitializeAsync()
    {
        string[] names = ["001_initial.sql", "002_retry_budgets.sql", "003_ordering_claims.sql", "004_command_payload_conflicts.sql", "005_durable_etl_jobs.sql", "006_etl_finalize.sql", "007_etl_ownership.sql", "008_etl_send_attempts.sql"];
        _migrationSql = new string[names.Length];
        for (var index = 0; index < names.Length; index++)
        {
            _migrationSql[index] = await File.ReadAllTextAsync(Path.Combine(AppContext.BaseDirectory, "Migrations", names[index]));
        }

        _factory = _database.CreateFactory(Path.Combine("data", "agent.db"));
        _migrator = new SqliteMigrator(_factory);

        // Seed an actual v6 database: 001-006 schema + correct v1-v6 ledger rows + populated v6 data.
        await using (var connection = await _factory.OpenAsync(CancellationToken.None))
        {
            await ExecuteAsync(connection, "CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY, name TEXT NOT NULL, checksum TEXT NOT NULL, applied_at_utc TEXT NOT NULL);");
            for (var index = 0; index < 6; index++)
            {
                await ExecuteAsync(connection, _migrationSql[index]);
                await InsertLedgerAsync(connection, index + 1, names[index], _migrationSql[index]);
            }
            await ExecuteAsync(connection, await File.ReadAllTextAsync(Path.Combine(AppContext.BaseDirectory, "Fixtures", "v6-populated", "populated-v6.sql")));
        }
        await SqliteTestDatabase.ClearPoolAsync(_factory);
    }

    public async Task DisposeAsync()
    {
        await _database.DisposeAsync();
    }

    [Fact]
    public async Task Populated_v6_upgrade_preserves_all_work_and_is_idempotent()
    {
        var commandsBefore = await SnapshotRowsAsync("SELECT * FROM commands_inbox ORDER BY command_id;");
        var attemptsBefore = await SnapshotRowsAsync("SELECT * FROM command_attempts ORDER BY attempt_id;");
        var outboxBefore = await SnapshotRowsAsync("SELECT * FROM results_outbox ORDER BY result_id;");
        var runsBefore = await SnapshotRowsAsync($"SELECT {RunColumnsV6} FROM etl_runs ORDER BY run_id;");
        var entitiesBefore = await SnapshotRowsAsync("SELECT * FROM etl_run_entities ORDER BY run_id,entity_name;");
        var batchesBefore = await SnapshotRowsAsync($"SELECT {BatchColumnsV7} FROM etl_batches ORDER BY batch_id;");
        var watermarksBefore = await SnapshotRowsAsync("SELECT * FROM watermarks ORDER BY entity_name;");
        var jobsBefore = await SnapshotRowsAsync($"SELECT {JobColumnsV5} FROM etl_jobs ORDER BY job_id;");
        var stateBefore = await SnapshotRowsAsync("SELECT * FROM agent_state ORDER BY key;");
        var snapshotsBefore = await SnapshotRowsAsync("SELECT * FROM config_snapshots ORDER BY config_version;");
        var conflictsBefore = await SnapshotRowsAsync("SELECT * FROM command_payload_conflicts ORDER BY event_id;");
        var ledgerBefore = await SnapshotRowsAsync("SELECT version,name,checksum,applied_at_utc FROM schema_migrations WHERE version<=6 ORDER BY version;");

        await _migrator.ApplyAsync(CancellationToken.None);

        Assert.Equal(8, SqliteMigrator.CurrentSchemaVersion);
        Assert.Equal(8, await CountAsync("SELECT COUNT(*) FROM schema_migrations"));
        Assert.Equal(commandsBefore, await SnapshotRowsAsync("SELECT * FROM commands_inbox ORDER BY command_id;"));
        Assert.Equal(attemptsBefore, await SnapshotRowsAsync("SELECT * FROM command_attempts ORDER BY attempt_id;"));
        Assert.Equal(outboxBefore, await SnapshotRowsAsync("SELECT * FROM results_outbox ORDER BY result_id;"));
        Assert.Equal(runsBefore, await SnapshotRowsAsync($"SELECT {RunColumnsV6} FROM etl_runs ORDER BY run_id;"));
        Assert.Equal(entitiesBefore, await SnapshotRowsAsync("SELECT * FROM etl_run_entities ORDER BY run_id,entity_name;"));
        Assert.Equal(batchesBefore, await SnapshotRowsAsync($"SELECT {BatchColumnsV7} FROM etl_batches ORDER BY batch_id;"));
        Assert.Equal(watermarksBefore, await SnapshotRowsAsync("SELECT * FROM watermarks ORDER BY entity_name;"));
        Assert.Equal(jobsBefore, await SnapshotRowsAsync($"SELECT {JobColumnsV5} FROM etl_jobs ORDER BY job_id;"));
        Assert.Equal(stateBefore, await SnapshotRowsAsync("SELECT * FROM agent_state ORDER BY key;"));
        Assert.Equal(snapshotsBefore, await SnapshotRowsAsync("SELECT * FROM config_snapshots ORDER BY config_version;"));
        Assert.Equal(conflictsBefore, await SnapshotRowsAsync("SELECT * FROM command_payload_conflicts ORDER BY event_id;"));
        Assert.Equal(ledgerBefore, await SnapshotRowsAsync("SELECT version,name,checksum,applied_at_utc FROM schema_migrations WHERE version<=6 ORDER BY version;"));
        for (var version = 1; version <= 8; version++)
        {
            Assert.Equal(Checksum(_migrationSql[version - 1]), await ChecksumForVersionAsync(version));
        }

        // Ownership storage exists and is empty; the completing run's claim and payload
        // are preserved, and no ownership was ever fabricated for pre-existing rows.
        Assert.Equal(1, await CountAsync("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='etl_entity_ownership'"));
        Assert.Equal(1, await CountAsync("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='etl_run_ownership_bindings'"));
        Assert.Equal(0, await CountAsync("SELECT COUNT(*) FROM etl_entity_ownership"));
        Assert.Equal(0, await CountAsync("SELECT COUNT(*) FROM etl_run_ownership_bindings"));
        Assert.Equal(5, await CountAsync("SELECT COUNT(*) FROM etl_runs WHERE extraction_claim_id IS NULL AND extraction_claim_owner_id IS NULL AND extraction_claim_acquired_at_utc IS NULL"));
        Assert.Equal(2, await CountAsync("SELECT COUNT(*) FROM etl_jobs WHERE dispatch_owner_id IS NULL AND dispatch_claimed_at_utc IS NULL AND claim_attempt_count=0 AND deferral_code IS NULL AND deferral_message IS NULL AND available_at_utc IS NULL"));

        var ledgerAfterFirstRun = await SnapshotRowsAsync("SELECT version,name,checksum,applied_at_utc FROM schema_migrations ORDER BY version;");
        var ownershipAfterFirstRun = await SnapshotRowsAsync("SELECT * FROM etl_entity_ownership ORDER BY entity_name;");
        var bindingsAfterFirstRun = await SnapshotRowsAsync("SELECT * FROM etl_run_ownership_bindings ORDER BY run_id,entity_name;");
        var runsAfterFirstRun = await SnapshotRowsAsync("SELECT * FROM etl_runs ORDER BY run_id;");

        await _migrator.ApplyAsync(CancellationToken.None);

        Assert.Equal(ledgerAfterFirstRun, await SnapshotRowsAsync("SELECT version,name,checksum,applied_at_utc FROM schema_migrations ORDER BY version;"));
        Assert.Equal(ownershipAfterFirstRun, await SnapshotRowsAsync("SELECT * FROM etl_entity_ownership ORDER BY entity_name;"));
        Assert.Equal(bindingsAfterFirstRun, await SnapshotRowsAsync("SELECT * FROM etl_run_ownership_bindings ORDER BY run_id,entity_name;"));
        Assert.Equal(runsAfterFirstRun, await SnapshotRowsAsync("SELECT * FROM etl_runs ORDER BY run_id;"));
        Assert.Equal(commandsBefore, await SnapshotRowsAsync("SELECT * FROM commands_inbox ORDER BY command_id;"));
        Assert.Equal(8, await CountAsync("SELECT COUNT(*) FROM schema_migrations"));
    }

    private static string Checksum(string sql) => Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(sql)));

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
