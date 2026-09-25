using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using ErpOnecAgent.Infrastructure.Persistence.Sqlite;
using Microsoft.Data.Sqlite;
using Xunit;

namespace ErpOnecAgent.IntegrationTests;

/// <summary>
/// Migration 011 (watermark_domain_resets — the immutable attested domain-reset
/// archive, slice D1) on a populated REAL v10 database: every pre-existing row
/// survives byte-identical — the scheduled runs holding their keys and frozen
/// resolved definitions, the unresolved failed/blocked runs, the RESOLVED blocked
/// run with its etl_run_resolutions record and 'manual_release' ownership, the
/// watermarks (a fingerprinted row, a legacy NULL-fingerprint row and a
/// high-generation row) — while watermark_domain_resets is created EMPTY with its
/// index and full CHECK domain (non-blank operator_id/reason, prior_generation>0).
/// Checksums 1-11 are recorded canonically and an idempotent rerun is a no-op.
/// </summary>
public sealed class EtlDomainResetsMigrationTests : IAsyncLifetime
{
    private const string RunColumnsV10 = "run_id,mode,requested_entities_json,status,started_at_utc,finished_at_utc,rows_read,batches_created,batches_acknowledged,error_count,last_error,configuration_version,created_at_utc,updated_at_utc,row_version,sealed_at_utc,sealed_entity_count,sealed_expected_batch_count,completion_claim_id,completion_claim_owner_id,completion_claim_acquired_at_utc,completion_attempt_count,completion_max_attempts,next_completion_attempt_at_utc,complete_payload_json,completion_acknowledged_at_utc,finalize_conflict_code,finalize_conflict_message,resolved_at_utc,extraction_claim_id,extraction_claim_owner_id,extraction_claim_acquired_at_utc,schedule_key,resolved_entities_json";

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

        // Seed an actual v10 database: 001-010 schema + correct v1-v10 ledger rows + the
        // populated v10 fixture (scheduled runs in every key-holding position, a resolved
        // blocked run with its attested resolution, watermarks in every domain shape).
        await using (var connection = await _factory.OpenAsync(CancellationToken.None))
        {
            await ExecuteAsync(connection, "CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY, name TEXT NOT NULL, checksum TEXT NOT NULL, applied_at_utc TEXT NOT NULL);");
            for (var index = 0; index < 10; index++)
            {
                await ExecuteAsync(connection, _migrationSql[index]);
                await InsertLedgerAsync(connection, index + 1, names[index], _migrationSql[index]);
            }
            await ExecuteAsync(connection, await File.ReadAllTextAsync(Path.Combine(AppContext.BaseDirectory, "Fixtures", "v10-populated", "populated-v10.sql")));
        }
        await SqliteTestDatabase.ClearPoolAsync(_factory);
    }

    public async Task DisposeAsync()
    {
        await _database.DisposeAsync();
    }

    [Fact]
    public async Task Populated_v10_database_upgrades_additively_preserving_every_row_and_evidence()
    {
        var commandsBefore = await SnapshotRowsAsync("SELECT * FROM commands_inbox ORDER BY command_id;");
        var attemptsBefore = await SnapshotRowsAsync("SELECT * FROM command_attempts ORDER BY attempt_id;");
        var outboxBefore = await SnapshotRowsAsync("SELECT * FROM results_outbox ORDER BY result_id;");
        var jobsBefore = await SnapshotRowsAsync("SELECT * FROM etl_jobs ORDER BY job_id;");
        var runsBefore = await SnapshotRowsAsync($"SELECT {RunColumnsV10} FROM etl_runs ORDER BY run_id;");
        var entitiesBefore = await SnapshotRowsAsync("SELECT * FROM etl_run_entities ORDER BY run_id, entity_name;");
        var batchesBefore = await SnapshotRowsAsync("SELECT * FROM etl_batches ORDER BY batch_id;");
        var sendAttemptsBefore = await SnapshotRowsAsync("SELECT * FROM etl_batch_send_attempts ORDER BY attempt_id;");
        var ownershipBefore = await SnapshotRowsAsync("SELECT * FROM etl_entity_ownership ORDER BY entity_name;");
        var bindingsBefore = await SnapshotRowsAsync("SELECT * FROM etl_run_ownership_bindings ORDER BY run_id, entity_name;");
        var watermarksBefore = await SnapshotRowsAsync("SELECT * FROM watermarks ORDER BY entity_name;");
        var stateBefore = await SnapshotRowsAsync("SELECT * FROM agent_state ORDER BY key;");
        var snapshotsBefore = await SnapshotRowsAsync("SELECT * FROM config_snapshots ORDER BY config_version;");
        var conflictsBefore = await SnapshotRowsAsync("SELECT * FROM command_payload_conflicts ORDER BY event_id;");
        var resolutionsBefore = await SnapshotRowsAsync("SELECT * FROM etl_run_resolutions ORDER BY run_id;");
        var ledgerBefore = await SnapshotRowsAsync("SELECT version,name,checksum,applied_at_utc FROM schema_migrations WHERE version<=10 ORDER BY version;");

        await _migrator.ApplyAsync(CancellationToken.None);

        Assert.Equal(11, SqliteMigrator.CurrentSchemaVersion);
        Assert.Equal(11, await CountAsync("SELECT COUNT(*) FROM schema_migrations"));
        // Every pre-existing row in every table survives byte-identical — the scheduled
        // runs with their keys and frozen identity, the resolved/unresolved terminal
        // runs, the live 'admitted' attempt row, retained + released ownership, the
        // fingerprinted/NULL-fingerprint/high-generation watermark rows.
        Assert.Equal(commandsBefore, await SnapshotRowsAsync("SELECT * FROM commands_inbox ORDER BY command_id;"));
        Assert.Equal(attemptsBefore, await SnapshotRowsAsync("SELECT * FROM command_attempts ORDER BY attempt_id;"));
        Assert.Equal(outboxBefore, await SnapshotRowsAsync("SELECT * FROM results_outbox ORDER BY result_id;"));
        Assert.Equal(jobsBefore, await SnapshotRowsAsync("SELECT * FROM etl_jobs ORDER BY job_id;"));
        Assert.Equal(runsBefore, await SnapshotRowsAsync($"SELECT {RunColumnsV10} FROM etl_runs ORDER BY run_id;"));
        Assert.Equal(entitiesBefore, await SnapshotRowsAsync("SELECT * FROM etl_run_entities ORDER BY run_id, entity_name;"));
        Assert.Equal(batchesBefore, await SnapshotRowsAsync("SELECT * FROM etl_batches ORDER BY batch_id;"));
        Assert.Equal(sendAttemptsBefore, await SnapshotRowsAsync("SELECT * FROM etl_batch_send_attempts ORDER BY attempt_id;"));
        Assert.Equal(ownershipBefore, await SnapshotRowsAsync("SELECT * FROM etl_entity_ownership ORDER BY entity_name;"));
        Assert.Equal(bindingsBefore, await SnapshotRowsAsync("SELECT * FROM etl_run_ownership_bindings ORDER BY run_id, entity_name;"));
        Assert.Equal(watermarksBefore, await SnapshotRowsAsync("SELECT * FROM watermarks ORDER BY entity_name;"));
        Assert.Equal(stateBefore, await SnapshotRowsAsync("SELECT * FROM agent_state ORDER BY key;"));
        Assert.Equal(snapshotsBefore, await SnapshotRowsAsync("SELECT * FROM config_snapshots ORDER BY config_version;"));
        Assert.Equal(conflictsBefore, await SnapshotRowsAsync("SELECT * FROM command_payload_conflicts ORDER BY event_id;"));
        Assert.Equal(resolutionsBefore, await SnapshotRowsAsync("SELECT * FROM etl_run_resolutions ORDER BY run_id;"));
        Assert.Equal(ledgerBefore, await SnapshotRowsAsync("SELECT version,name,checksum,applied_at_utc FROM schema_migrations WHERE version<=10 ORDER BY version;"));

        // The resets archive exists and is EMPTY — no pre-011 watermark change is ever
        // retroactively attributed a reset.
        Assert.Equal(1, await CountAsync("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='watermark_domain_resets'"));
        Assert.Equal(1, await CountAsync("SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='ix_watermark_domain_resets_entity'"));
        Assert.Equal(0, await CountAsync("SELECT COUNT(*) FROM watermark_domain_resets"));
        // The fixture surface: the resolution record of f6 and the three domain shapes.
        Assert.Equal(1, await CountAsync("SELECT COUNT(*) FROM etl_run_resolutions"));
        Assert.Equal(3, await CountAsync("SELECT COUNT(*) FROM watermarks"));
        Assert.Equal(1, await CountAsync("SELECT COUNT(*) FROM watermarks WHERE domain_fingerprint IS NULL"));
        Assert.Equal(1, await CountAsync("SELECT COUNT(*) FROM watermarks WHERE generation > 1000"));

        // Checksums 1-11 recorded canonically — independent of the checkout's line endings.
        for (var version = 1; version <= 11; version++)
        {
            var expected = PublishedChecksum(_migrationSql[version - 1]);
            Assert.Equal(expected, await ChecksumForVersionAsync(version));
        }
        Assert.Equal("010_etl_run_resolutions.sql", await NameForVersionAsync(10));
        Assert.Equal("011_watermark_domain_resets.sql", await NameForVersionAsync(11));
    }

    [Fact]
    public async Task Apply_is_idempotent_on_a_populated_v11_database()
    {
        await _migrator.ApplyAsync(CancellationToken.None);
        var runsAfterFirst = await SnapshotRowsAsync("SELECT * FROM etl_runs ORDER BY run_id;");
        var watermarksAfterFirst = await SnapshotRowsAsync("SELECT * FROM watermarks ORDER BY entity_name;");
        var resetsAfterFirst = await SnapshotRowsAsync("SELECT * FROM watermark_domain_resets ORDER BY reset_id;");
        var ledgerAfterFirst = await SnapshotRowsAsync("SELECT version,name,checksum,applied_at_utc FROM schema_migrations ORDER BY version;");

        await _migrator.ApplyAsync(CancellationToken.None);

        Assert.Equal(runsAfterFirst, await SnapshotRowsAsync("SELECT * FROM etl_runs ORDER BY run_id;"));
        Assert.Equal(watermarksAfterFirst, await SnapshotRowsAsync("SELECT * FROM watermarks ORDER BY entity_name;"));
        Assert.Equal(resetsAfterFirst, await SnapshotRowsAsync("SELECT * FROM watermark_domain_resets ORDER BY reset_id;"));
        Assert.Equal(ledgerAfterFirst, await SnapshotRowsAsync("SELECT version,name,checksum,applied_at_utc FROM schema_migrations ORDER BY version;"));
        Assert.Equal(11, await CountAsync("SELECT COUNT(*) FROM schema_migrations"));
    }

    [Fact]
    public async Task Resets_table_enforces_its_check_domain()
    {
        await _migrator.ApplyAsync(CancellationToken.None);
        var now = "2026-09-26T00:00:00.0000000+00:00";

        // Blank attestations are rejected: operator_id and reason are never empty.
        await Assert.ThrowsAsync<SqliteException>(() => InsertResetAsync("r-blank-op", "clients", now, "   ", "epoch change", 3));
        await Assert.ThrowsAsync<SqliteException>(() => InsertResetAsync("r-blank-reason", "clients", now, "operator-1", "", 3));
        // A reset must archive a real generation — 0 is rejected.
        await Assert.ThrowsAsync<SqliteException>(() => InsertResetAsync("r-zero-gen", "clients", now, "operator-1", "epoch change", 0));

        // A valid archive row — including NULL prior cursor/fingerprint — is accepted.
        await InsertResetAsync("r-valid", "clients", now, "operator-1", "epoch change", 3);
        Assert.Equal(1, await CountAsync("SELECT COUNT(*) FROM watermark_domain_resets"));
        await Assert.ThrowsAsync<SqliteException>(() => InsertResetAsync("r-valid", "orders", now, "operator-1", "epoch change", 1));
        Assert.Equal(1, await CountAsync("SELECT COUNT(*) FROM watermark_domain_resets"));
    }

    private async Task InsertResetAsync(string resetId, string entity, string resetAtUtc, string operatorId, string reason, long priorGeneration)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = "INSERT INTO watermark_domain_resets(reset_id,entity_name,reset_at_utc,operator_id,reason,prior_committed_cursor_json,prior_generation,prior_domain_fingerprint,prior_last_run_id,prior_updated_at_utc) VALUES($id,$entity,$at,$op,$reason,$cursor,$gen,$fp,$lastRun,$updated);";
        command.Parameters.AddWithValue("$id", resetId);
        command.Parameters.AddWithValue("$entity", entity);
        command.Parameters.AddWithValue("$at", resetAtUtc);
        command.Parameters.AddWithValue("$op", operatorId);
        command.Parameters.AddWithValue("$reason", reason);
        command.Parameters.AddWithValue("$cursor", DBNull.Value);
        command.Parameters.AddWithValue("$gen", priorGeneration);
        command.Parameters.AddWithValue("$fp", DBNull.Value);
        command.Parameters.AddWithValue("$lastRun", DBNull.Value);
        command.Parameters.AddWithValue("$updated", DBNull.Value);
        await command.ExecuteNonQueryAsync(CancellationToken.None);
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
