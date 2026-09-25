using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using ErpOnecAgent.Infrastructure.Persistence.Sqlite;
using Microsoft.Data.Sqlite;
using Xunit;

namespace ErpOnecAgent.IntegrationTests;

/// <summary>
/// Migration 009 (etl_runs.schedule_key + etl_runs.resolved_entities_json +
/// ux_etl_runs_schedule_active) on a populated REAL v8 database: every pre-existing
/// row survives byte-identical — including the live 'admitted' send attempt fencing
/// an in-flight 'uploading' batch, the blocked run with retained ownership evidence,
/// and released/finalized ownership — while the new run columns take NULL on every
/// existing row and the partial unique index arrives enforcing exactly one active or
/// unresolved run per schedule key. Checksums 1-9 are recorded canonically and an
/// idempotent rerun is a no-op.
/// </summary>
public sealed class EtlScheduledRunsMigrationTests : IAsyncLifetime
{
    private const string RunColumnsV8 = "run_id,mode,requested_entities_json,status,started_at_utc,finished_at_utc,rows_read,batches_created,batches_acknowledged,error_count,last_error,configuration_version,created_at_utc,updated_at_utc,row_version,sealed_at_utc,sealed_entity_count,sealed_expected_batch_count,completion_claim_id,completion_claim_owner_id,completion_claim_acquired_at_utc,completion_attempt_count,completion_max_attempts,next_completion_attempt_at_utc,complete_payload_json,completion_acknowledged_at_utc,finalize_conflict_code,finalize_conflict_message,resolved_at_utc,extraction_claim_id,extraction_claim_owner_id,extraction_claim_acquired_at_utc";
    private const string BatchColumnsV8 = "batch_id,run_id,entity_name,schema_version,file_path,status,row_count,watermark_from_json,watermark_to_json,sha256,compressed_size,uncompressed_size,attempt_count,next_attempt_at_utc,created_at_utc,acknowledged_at_utc,last_error,send_attempt_id,upload_max_attempts,quarantine_code,row_version";

    private readonly SqliteTestDatabase _database = new();
    private SqliteConnectionFactory _factory = null!;
    private SqliteMigrator _migrator = null!;
    private string[] _migrationSql = null!;

    public async Task InitializeAsync()
    {
        _migrationSql = new string[11];
        string[] names = ["001_initial.sql", "002_retry_budgets.sql", "003_ordering_claims.sql", "004_command_payload_conflicts.sql", "005_durable_etl_jobs.sql", "006_etl_finalize.sql", "007_etl_ownership.sql", "008_etl_send_attempts.sql", "009_etl_scheduled_runs.sql", "010_etl_run_resolutions.sql", "011_watermark_domain_resets.sql"];
        for (var index = 0; index < names.Length; index++)
        {
            _migrationSql[index] = await File.ReadAllTextAsync(Path.Combine(AppContext.BaseDirectory, "Migrations", names[index]));
        }

        _factory = _database.CreateFactory(Path.Combine("data", "agent.db"));
        _migrator = new SqliteMigrator(_factory);

        // Seed an actual v8 database: 001-008 schema + correct v1-v8 ledger rows + the
        // populated v8 fixture (ledgered sends, in-flight admitted attempt, retained
        // ownership, quarantine codes).
        await using (var connection = await _factory.OpenAsync(CancellationToken.None))
        {
            await ExecuteAsync(connection, "CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY, name TEXT NOT NULL, checksum TEXT NOT NULL, applied_at_utc TEXT NOT NULL);");
            for (var index = 0; index < 8; index++)
            {
                await ExecuteAsync(connection, _migrationSql[index]);
                await InsertLedgerAsync(connection, index + 1, names[index], _migrationSql[index]);
            }
            await ExecuteAsync(connection, await File.ReadAllTextAsync(Path.Combine(AppContext.BaseDirectory, "Fixtures", "v8-populated", "populated-v8.sql")));
        }
        await SqliteTestDatabase.ClearPoolAsync(_factory);
    }

    public async Task DisposeAsync()
    {
        await _database.DisposeAsync();
    }

    [Fact]
    public async Task Populated_v8_database_upgrades_additively_preserving_every_row_and_evidence()
    {
        var commandsBefore = await SnapshotRowsAsync("SELECT * FROM commands_inbox ORDER BY command_id;");
        var attemptsBefore = await SnapshotRowsAsync("SELECT * FROM command_attempts ORDER BY attempt_id;");
        var outboxBefore = await SnapshotRowsAsync("SELECT * FROM results_outbox ORDER BY result_id;");
        var jobsBefore = await SnapshotRowsAsync("SELECT * FROM etl_jobs ORDER BY job_id;");
        var runsBefore = await SnapshotRowsAsync($"SELECT {RunColumnsV8} FROM etl_runs ORDER BY run_id;");
        var entitiesBefore = await SnapshotRowsAsync("SELECT * FROM etl_run_entities ORDER BY run_id, entity_name;");
        var batchesBefore = await SnapshotRowsAsync($"SELECT {BatchColumnsV8} FROM etl_batches ORDER BY batch_id;");
        var sendAttemptsBefore = await SnapshotRowsAsync("SELECT * FROM etl_batch_send_attempts ORDER BY attempt_id;");
        var ownershipBefore = await SnapshotRowsAsync("SELECT * FROM etl_entity_ownership ORDER BY entity_name;");
        var bindingsBefore = await SnapshotRowsAsync("SELECT * FROM etl_run_ownership_bindings ORDER BY run_id, entity_name;");
        var watermarksBefore = await SnapshotRowsAsync("SELECT * FROM watermarks ORDER BY entity_name;");
        var stateBefore = await SnapshotRowsAsync("SELECT * FROM agent_state ORDER BY key;");
        var snapshotsBefore = await SnapshotRowsAsync("SELECT * FROM config_snapshots ORDER BY config_version;");
        var conflictsBefore = await SnapshotRowsAsync("SELECT * FROM command_payload_conflicts ORDER BY event_id;");
        var ledgerBefore = await SnapshotRowsAsync("SELECT version,name,checksum,applied_at_utc FROM schema_migrations WHERE version<=8 ORDER BY version;");

        await _migrator.ApplyAsync(CancellationToken.None);

        Assert.Equal(11, SqliteMigrator.CurrentSchemaVersion);
        Assert.Equal(11, await CountAsync("SELECT COUNT(*) FROM schema_migrations"));
        // Every pre-existing row in every table survives byte-identical — the live
        // 'admitted' attempt row, the in-flight 'uploading' batch, retained ownership.
        Assert.Equal(commandsBefore, await SnapshotRowsAsync("SELECT * FROM commands_inbox ORDER BY command_id;"));
        Assert.Equal(attemptsBefore, await SnapshotRowsAsync("SELECT * FROM command_attempts ORDER BY attempt_id;"));
        Assert.Equal(outboxBefore, await SnapshotRowsAsync("SELECT * FROM results_outbox ORDER BY result_id;"));
        Assert.Equal(jobsBefore, await SnapshotRowsAsync("SELECT * FROM etl_jobs ORDER BY job_id;"));
        Assert.Equal(runsBefore, await SnapshotRowsAsync($"SELECT {RunColumnsV8} FROM etl_runs ORDER BY run_id;"));
        Assert.Equal(entitiesBefore, await SnapshotRowsAsync("SELECT * FROM etl_run_entities ORDER BY run_id, entity_name;"));
        Assert.Equal(batchesBefore, await SnapshotRowsAsync($"SELECT {BatchColumnsV8} FROM etl_batches ORDER BY batch_id;"));
        Assert.Equal(sendAttemptsBefore, await SnapshotRowsAsync("SELECT * FROM etl_batch_send_attempts ORDER BY attempt_id;"));
        Assert.Equal(ownershipBefore, await SnapshotRowsAsync("SELECT * FROM etl_entity_ownership ORDER BY entity_name;"));
        Assert.Equal(bindingsBefore, await SnapshotRowsAsync("SELECT * FROM etl_run_ownership_bindings ORDER BY run_id, entity_name;"));
        Assert.Equal(watermarksBefore, await SnapshotRowsAsync("SELECT * FROM watermarks ORDER BY entity_name;"));
        Assert.Equal(stateBefore, await SnapshotRowsAsync("SELECT * FROM agent_state ORDER BY key;"));
        Assert.Equal(snapshotsBefore, await SnapshotRowsAsync("SELECT * FROM config_snapshots ORDER BY config_version;"));
        Assert.Equal(conflictsBefore, await SnapshotRowsAsync("SELECT * FROM command_payload_conflicts ORDER BY event_id;"));
        Assert.Equal(ledgerBefore, await SnapshotRowsAsync("SELECT version,name,checksum,applied_at_utc FROM schema_migrations WHERE version<=8 ORDER BY version;"));

        // Additive run columns take NULL on every pre-existing row — no row is
        // retroactively attributed to a schedule or a frozen resolved definition set.
        Assert.Equal(6, await CountAsync("SELECT COUNT(*) FROM etl_runs"));
        Assert.Equal(0, await CountAsync("SELECT COUNT(*) FROM etl_runs WHERE schedule_key IS NOT NULL OR resolved_entities_json IS NOT NULL"));

        // The partial unique index exists — UNIQUE and partial, exactly as declared.
        Assert.Equal(1, await CountAsync("SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='ux_etl_runs_schedule_active'"));
        Assert.Equal(1, await CountAsync("SELECT COUNT(*) FROM pragma_index_list('etl_runs') WHERE name='ux_etl_runs_schedule_active' AND \"unique\"=1 AND partial=1"));

        // Checksums 1-11 recorded canonically — independent of the checkout's line endings.
        for (var version = 1; version <= 11; version++)
        {
            var expected = PublishedChecksum(_migrationSql[version - 1]);
            Assert.Equal(expected, await ChecksumForVersionAsync(version));
        }
        Assert.Equal("009_etl_scheduled_runs.sql", await NameForVersionAsync(9));
        Assert.Equal("010_etl_run_resolutions.sql", await NameForVersionAsync(10));
        Assert.Equal("011_watermark_domain_resets.sql", await NameForVersionAsync(11));
    }

    [Fact]
    public async Task Apply_is_idempotent_on_a_populated_v9_database()
    {
        await _migrator.ApplyAsync(CancellationToken.None);
        var runsAfterFirst = await SnapshotRowsAsync("SELECT * FROM etl_runs ORDER BY run_id;");
        var ledgerAfterFirst = await SnapshotRowsAsync("SELECT version,name,checksum,applied_at_utc FROM schema_migrations ORDER BY version;");

        await _migrator.ApplyAsync(CancellationToken.None);

        Assert.Equal(runsAfterFirst, await SnapshotRowsAsync("SELECT * FROM etl_runs ORDER BY run_id;"));
        Assert.Equal(ledgerAfterFirst, await SnapshotRowsAsync("SELECT version,name,checksum,applied_at_utc FROM schema_migrations ORDER BY version;"));
        Assert.Equal(11, await CountAsync("SELECT COUNT(*) FROM schema_migrations"));
    }

    [Fact]
    public async Task Schedule_active_index_holds_exactly_one_active_or_unresolved_run_per_key()
    {
        await _migrator.ApplyAsync(CancellationToken.None);
        var now = DateTimeOffset.UtcNow.ToString("O");

        // Two pending runs with the same schedule key: the second insert is rejected.
        await InsertRunAsync("00000000-0000-0000-0000-0000000000f1", "sched-a", "pending");
        await Assert.ThrowsAsync<SqliteException>(() => InsertRunAsync("00000000-0000-0000-0000-0000000000f2", "sched-a", "pending"));

        // A succeeded run releases the key: pending over the same key is admitted.
        await InsertRunAsync("00000000-0000-0000-0000-0000000000f3", "sched-b", "succeeded");
        await InsertRunAsync("00000000-0000-0000-0000-0000000000f4", "sched-b", "pending");

        // An unresolved failed run still holds the key.
        await InsertRunAsync("00000000-0000-0000-0000-0000000000f5", "sched-c", "failed");
        await Assert.ThrowsAsync<SqliteException>(() => InsertRunAsync("00000000-0000-0000-0000-0000000000f6", "sched-c", "pending"));

        // Resolving the failed run releases the key.
        await ExecuteSqlAsync("UPDATE etl_runs SET resolved_at_utc=$now WHERE run_id='00000000-0000-0000-0000-0000000000f5';", ("$now", now));
        await InsertRunAsync("00000000-0000-0000-0000-0000000000f7", "sched-c", "pending");

        // An unresolved blocked run still holds the key; a NULL key is never indexed.
        await InsertRunAsync("00000000-0000-0000-0000-0000000000f8", "sched-d", "blocked");
        await Assert.ThrowsAsync<SqliteException>(() => InsertRunAsync("00000000-0000-0000-0000-0000000000f9", "sched-d", "running"));
        await InsertRunAsync("00000000-0000-0000-0000-0000000000fa", null, "pending");
        await InsertRunAsync("00000000-0000-0000-0000-0000000000fb", null, "pending");
    }

    private async Task InsertRunAsync(string runId, string? scheduleKey, string status)
    {
        await ExecuteSqlAsync(
            "INSERT INTO etl_runs(run_id,mode,requested_entities_json,status,created_at_utc,updated_at_utc,schedule_key) VALUES($run,'incremental','[\"clients\"]',$status,$now,$now,$key);",
            ("$run", runId), ("$status", status), ("$key", scheduleKey), ("$now", DateTimeOffset.UtcNow.ToString("O")));
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

    private async Task ExecuteSqlAsync(string sql, params (string Name, object? Value)[] parameters)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        foreach (var (name, value) in parameters) command.Parameters.AddWithValue(name, value ?? DBNull.Value);
        await command.ExecuteNonQueryAsync(CancellationToken.None);
    }

    private static async Task ExecuteAsync(SqliteConnection connection, string sql)
    {
        await using var command = connection.CreateCommand();
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
