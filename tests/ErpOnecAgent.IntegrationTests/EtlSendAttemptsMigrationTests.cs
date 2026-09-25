using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using ErpOnecAgent.Infrastructure.Persistence.Sqlite;
using Microsoft.Data.Sqlite;
using Xunit;

namespace ErpOnecAgent.IntegrationTests;

/// <summary>
/// Migration 008 (etl_batch_send_attempts + etl_batches send-fence/bound/quarantine
/// columns) on a populated REAL v7 database: every pre-existing row survives
/// byte-identical — including in-flight legacy 'uploading' batches, a blocked run
/// with retained ownership evidence, and released/finalized ownership — while the
/// new columns take their declared defaults and the new table arrives empty.
/// Checksums 1-8 are recorded canonically and an idempotent rerun is a no-op.
/// </summary>
public sealed class EtlSendAttemptsMigrationTests : IAsyncLifetime
{
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

        // Seed an actual v7 database: 001-007 schema + correct v1-v7 ledger rows + the
        // populated v7 fixture (in-flight uploads, blocked runs, retained ownership).
        await using (var connection = await _factory.OpenAsync(CancellationToken.None))
        {
            await ExecuteAsync(connection, "CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY, name TEXT NOT NULL, checksum TEXT NOT NULL, applied_at_utc TEXT NOT NULL);");
            for (var index = 0; index < 7; index++)
            {
                await ExecuteAsync(connection, _migrationSql[index]);
                await InsertLedgerAsync(connection, index + 1, names[index], _migrationSql[index]);
            }
            await ExecuteAsync(connection, await File.ReadAllTextAsync(Path.Combine(AppContext.BaseDirectory, "Fixtures", "v7-populated", "populated-v7.sql")));
        }
        await SqliteTestDatabase.ClearPoolAsync(_factory);
    }

    public async Task DisposeAsync()
    {
        await _database.DisposeAsync();
    }

    [Fact]
    public async Task Populated_v7_database_upgrades_additively_preserving_every_row_and_evidence()
    {
        var commandsBefore = await SnapshotRowsAsync("SELECT * FROM commands_inbox ORDER BY command_id;");
        var attemptsBefore = await SnapshotRowsAsync("SELECT * FROM command_attempts ORDER BY attempt_id;");
        var outboxBefore = await SnapshotRowsAsync("SELECT * FROM results_outbox ORDER BY result_id;");
        var jobsBefore = await SnapshotRowsAsync("SELECT * FROM etl_jobs ORDER BY job_id;");
        var runsBefore = await SnapshotRowsAsync("SELECT * FROM etl_runs ORDER BY run_id;");
        var entitiesBefore = await SnapshotRowsAsync("SELECT * FROM etl_run_entities ORDER BY run_id, entity_name;");
        var batchesBefore = await SnapshotRowsAsync("SELECT batch_id,run_id,entity_name,schema_version,file_path,status,row_count,sha256,compressed_size,uncompressed_size,attempt_count,next_attempt_at_utc,created_at_utc,acknowledged_at_utc,last_error FROM etl_batches ORDER BY batch_id;");
        var ownershipBefore = await SnapshotRowsAsync("SELECT * FROM etl_entity_ownership ORDER BY entity_name;");
        var bindingsBefore = await SnapshotRowsAsync("SELECT * FROM etl_run_ownership_bindings ORDER BY run_id, entity_name;");
        var watermarksBefore = await SnapshotRowsAsync("SELECT * FROM watermarks ORDER BY entity_name;");
        var stateBefore = await SnapshotRowsAsync("SELECT * FROM agent_state ORDER BY key;");
        var snapshotsBefore = await SnapshotRowsAsync("SELECT * FROM config_snapshots ORDER BY config_version;");
        var ledgerBefore = await SnapshotRowsAsync("SELECT version,name,checksum,applied_at_utc FROM schema_migrations WHERE version<=7 ORDER BY version;");

        await _migrator.ApplyAsync(CancellationToken.None);

        Assert.Equal(8, SqliteMigrator.CurrentSchemaVersion);
        Assert.Equal(8, await CountAsync("SELECT COUNT(*) FROM schema_migrations"));
        // Every pre-existing row in every table survives byte-identical — the in-flight
        // 'uploading' batch, the blocked run, retained + released ownership, bindings.
        Assert.Equal(commandsBefore, await SnapshotRowsAsync("SELECT * FROM commands_inbox ORDER BY command_id;"));
        Assert.Equal(attemptsBefore, await SnapshotRowsAsync("SELECT * FROM command_attempts ORDER BY attempt_id;"));
        Assert.Equal(outboxBefore, await SnapshotRowsAsync("SELECT * FROM results_outbox ORDER BY result_id;"));
        Assert.Equal(jobsBefore, await SnapshotRowsAsync("SELECT * FROM etl_jobs ORDER BY job_id;"));
        Assert.Equal(runsBefore, await SnapshotRowsAsync("SELECT * FROM etl_runs ORDER BY run_id;"));
        Assert.Equal(entitiesBefore, await SnapshotRowsAsync("SELECT * FROM etl_run_entities ORDER BY run_id, entity_name;"));
        Assert.Equal(batchesBefore, await SnapshotRowsAsync("SELECT batch_id,run_id,entity_name,schema_version,file_path,status,row_count,sha256,compressed_size,uncompressed_size,attempt_count,next_attempt_at_utc,created_at_utc,acknowledged_at_utc,last_error FROM etl_batches ORDER BY batch_id;"));
        Assert.Equal(ownershipBefore, await SnapshotRowsAsync("SELECT * FROM etl_entity_ownership ORDER BY entity_name;"));
        Assert.Equal(bindingsBefore, await SnapshotRowsAsync("SELECT * FROM etl_run_ownership_bindings ORDER BY run_id, entity_name;"));
        Assert.Equal(watermarksBefore, await SnapshotRowsAsync("SELECT * FROM watermarks ORDER BY entity_name;"));
        Assert.Equal(stateBefore, await SnapshotRowsAsync("SELECT * FROM agent_state ORDER BY key;"));
        Assert.Equal(snapshotsBefore, await SnapshotRowsAsync("SELECT * FROM config_snapshots ORDER BY config_version;"));
        Assert.Equal(ledgerBefore, await SnapshotRowsAsync("SELECT version,name,checksum,applied_at_utc FROM schema_migrations WHERE version<=7 ORDER BY version;"));

        // New schema surface: the send-attempt ledger exists and is EMPTY — no pre-008
        // send is ever retroactively ledgered.
        Assert.Equal(0, await CountAsync("SELECT COUNT(*) FROM etl_batch_send_attempts"));
        // Additive batch columns take their declared defaults on every pre-008 row —
        // no fence, no bound, no quarantine code, row_version 1. NULL = legacy/unknown.
        Assert.Equal(0, await CountAsync("SELECT COUNT(*) FROM etl_batches WHERE send_attempt_id IS NOT NULL OR upload_max_attempts IS NOT NULL OR quarantine_code IS NOT NULL OR row_version <> 1"));
        Assert.Equal(6, await CountAsync("SELECT COUNT(*) FROM etl_batches"));
        // The CHECK vocabulary exists: only the four stable codes or NULL.
        Assert.Equal(0, await CountAsync("SELECT COUNT(*) FROM etl_batches WHERE quarantine_code IS NOT NULL AND quarantine_code NOT IN ('UPLOAD_OUTCOME_UNKNOWN','ACK_INVALID','UPLOAD_ATTEMPTS_EXHAUSTED','RUN_BLOCKED','RUN_FAILED','SEND_LEDGER_LOST')"));
        // The single-live-admission partial unique index exists.
        Assert.Equal(1, await CountAsync("SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='ux_etl_batch_send_attempts_admitted'"));
        // Checksums 1-8 recorded canonically.
        for (var version = 1; version <= 8; version++)
        {
            // The v1-v7 ledger was seeded with the canonical LF checksums; 008 is appended
            // canonically too — independent of the checkout's line endings.
            var expected = PublishedChecksum(_migrationSql[version - 1]);
            Assert.Equal(expected, await ChecksumForVersionAsync(version));
        }
        Assert.Equal("008_etl_send_attempts.sql", await NameForVersionAsync(8));
    }

    [Fact]
    public async Task Apply_is_idempotent_on_a_populated_v8_database()
    {
        await _migrator.ApplyAsync(CancellationToken.None);
        var batchesAfterFirst = await SnapshotRowsAsync("SELECT * FROM etl_batches ORDER BY batch_id;");
        var ledgerAfterFirst = await SnapshotRowsAsync("SELECT version,name,checksum,applied_at_utc FROM schema_migrations ORDER BY version;");

        await _migrator.ApplyAsync(CancellationToken.None);

        Assert.Equal(batchesAfterFirst, await SnapshotRowsAsync("SELECT * FROM etl_batches ORDER BY batch_id;"));
        Assert.Equal(ledgerAfterFirst, await SnapshotRowsAsync("SELECT version,name,checksum,applied_at_utc FROM schema_migrations ORDER BY version;"));
        Assert.Equal(8, await CountAsync("SELECT COUNT(*) FROM schema_migrations"));
    }

    [Fact]
    public async Task Send_attempt_foreign_key_blocks_orphan_attempt_rows()
    {
        await _migrator.ApplyAsync(CancellationToken.None);
        // The batch FK is real: an attempt without a batch fails; deleting a batch
        // with attempts fails (evidence can never be orphaned away).
        await Assert.ThrowsAsync<SqliteException>(async () =>
        {
            await using var connection = await _factory.OpenAsync(CancellationToken.None);
            await ExecuteAsync(connection, "INSERT INTO etl_batch_send_attempts(attempt_id,batch_id,attempt_no,owner_id,admitted_at_utc,outcome) VALUES('a1','missing-batch',1,'o','2026-09-25T00:00:00Z','admitted');");
        });
        await Assert.ThrowsAsync<SqliteException>(async () =>
        {
            await using var connection = await _factory.OpenAsync(CancellationToken.None);
            await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(CancellationToken.None);
            await ExecuteAsync(connection, transaction, "INSERT INTO etl_batch_send_attempts(attempt_id,batch_id,attempt_no,owner_id,admitted_at_utc,outcome) VALUES('a2','e2000000-0000-0000-0000-000000000003',1,'o','2026-09-25T00:00:00Z','admitted');");
            await ExecuteAsync(connection, transaction, "DELETE FROM etl_batches WHERE batch_id='e2000000-0000-0000-0000-000000000003';");
            await transaction.CommitAsync(CancellationToken.None);
        });
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
        command.CommandText = "SELECT checksum FROM schema_migrations WHERE version=$v;";
        command.Parameters.AddWithValue("$v", version);
        return await command.ExecuteScalarAsync(CancellationToken.None) as string;
    }

    private async Task<string?> NameForVersionAsync(int version)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT name FROM schema_migrations WHERE version=$v;";
        command.Parameters.AddWithValue("$v", version);
        return await command.ExecuteScalarAsync(CancellationToken.None) as string;
    }

    private async Task<List<string>> SnapshotRowsAsync(string sql)
    {
        var rows = new List<string>();
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        await using var reader = await command.ExecuteReaderAsync(CancellationToken.None);
        while (await reader.ReadAsync(CancellationToken.None))
        {
            var values = new string[reader.FieldCount];
            for (var i = 0; i < reader.FieldCount; i++) values[i] = reader.IsDBNull(i) ? "<null>" : reader.GetValue(i).ToString() ?? "";
            rows.Add(string.Join("|", values));
        }
        return rows;
    }

    private static string Checksum(string sql) => Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(sql)));

    // A published checksum is the LF-canonicalized hash — the catalog's recorded value.
    private static string PublishedChecksum(string sql) => Checksum(sql.Replace("\r\n", "\n", StringComparison.Ordinal));

    private static async Task ExecuteAsync(SqliteConnection connection, string sql)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync(CancellationToken.None);
    }

    private static async Task ExecuteAsync(SqliteConnection connection, SqliteTransaction transaction, string sql)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync(CancellationToken.None);
    }

    private static async Task InsertLedgerAsync(SqliteConnection connection, int version, string name, string sql)
    {
        await using var record = connection.CreateCommand();
        // The recorded checksum is the canonical published hash of the raw file bytes.
        record.CommandText = "INSERT INTO schema_migrations(version,name,checksum,applied_at_utc) VALUES($version,$name,$checksum,$now);";
        record.Parameters.AddWithValue("$version", version);
        record.Parameters.AddWithValue("$name", name);
        record.Parameters.AddWithValue("$checksum", PublishedChecksum(sql));
        record.Parameters.AddWithValue("$now", DateTimeOffset.UtcNow.ToString("O"));
        await record.ExecuteNonQueryAsync(CancellationToken.None);
    }
}
