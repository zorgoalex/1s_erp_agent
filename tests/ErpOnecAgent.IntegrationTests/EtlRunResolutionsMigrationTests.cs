using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using ErpOnecAgent.Infrastructure.Persistence.Sqlite;
using Microsoft.Data.Sqlite;
using Xunit;

namespace ErpOnecAgent.IntegrationTests;

/// <summary>
/// Migration 010 (etl_run_resolutions — the immutable attested-resolution record
/// table, design §8 slice R1) on a populated REAL v9 database: every pre-existing
/// row survives byte-identical — including the scheduled runs (pending / running /
/// blocked-unresolved / succeeded) holding their schedule keys and frozen resolved
/// definitions, the failed and blocked manual runs with retained ownership and
/// bindings, the live 'admitted' send attempt fencing an in-flight 'uploading'
/// batch, and released/finalized ownership — while etl_run_resolutions is created
/// EMPTY with its full CHECK domain (decision set, workers_quiesced=1, prior_status
/// failed|blocked, non-blank attestations, non-negative counts) and the FK to
/// etl_runs. Checksums 1-10 are recorded canonically and an idempotent rerun is a
/// no-op.
/// </summary>
public sealed class EtlRunResolutionsMigrationTests : IAsyncLifetime
{
    private const string RunColumnsV9 = "run_id,mode,requested_entities_json,status,started_at_utc,finished_at_utc,rows_read,batches_created,batches_acknowledged,error_count,last_error,configuration_version,created_at_utc,updated_at_utc,row_version,sealed_at_utc,sealed_entity_count,sealed_expected_batch_count,completion_claim_id,completion_claim_owner_id,completion_claim_acquired_at_utc,completion_attempt_count,completion_max_attempts,next_completion_attempt_at_utc,complete_payload_json,completion_acknowledged_at_utc,finalize_conflict_code,finalize_conflict_message,resolved_at_utc,extraction_claim_id,extraction_claim_owner_id,extraction_claim_acquired_at_utc,schedule_key,resolved_entities_json";

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

        // Seed an actual v9 database: 001-009 schema + correct v1-v9 ledger rows + the
        // populated v9 fixture (scheduled runs in every key-holding position, failed
        // and blocked runs with retained ownership, ledgered send evidence).
        await using (var connection = await _factory.OpenAsync(CancellationToken.None))
        {
            await ExecuteAsync(connection, "CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY, name TEXT NOT NULL, checksum TEXT NOT NULL, applied_at_utc TEXT NOT NULL);");
            for (var index = 0; index < 9; index++)
            {
                await ExecuteAsync(connection, _migrationSql[index]);
                await InsertLedgerAsync(connection, index + 1, names[index], _migrationSql[index]);
            }
            await ExecuteAsync(connection, await File.ReadAllTextAsync(Path.Combine(AppContext.BaseDirectory, "Fixtures", "v9-populated", "populated-v9.sql")));
        }
        await SqliteTestDatabase.ClearPoolAsync(_factory);
    }

    public async Task DisposeAsync()
    {
        await _database.DisposeAsync();
    }

    [Fact]
    public async Task Populated_v9_database_upgrades_additively_preserving_every_row_and_evidence()
    {
        var commandsBefore = await SnapshotRowsAsync("SELECT * FROM commands_inbox ORDER BY command_id;");
        var attemptsBefore = await SnapshotRowsAsync("SELECT * FROM command_attempts ORDER BY attempt_id;");
        var outboxBefore = await SnapshotRowsAsync("SELECT * FROM results_outbox ORDER BY result_id;");
        var jobsBefore = await SnapshotRowsAsync("SELECT * FROM etl_jobs ORDER BY job_id;");
        var runsBefore = await SnapshotRowsAsync($"SELECT {RunColumnsV9} FROM etl_runs ORDER BY run_id;");
        var entitiesBefore = await SnapshotRowsAsync("SELECT run_id,entity_name,entity_definition_json,domain_fingerprint,status,base_row_present,expected_base_generation,expected_base_cursor_json,expected_base_domain_fingerprint,domain_status,watermark_from_json,snapshot_upper_bound_json,final_watermark_json,expected_batch_count,rows_read,batches_created,last_error,created_at_utc,updated_at_utc,row_version FROM etl_run_entities ORDER BY run_id, entity_name;");
        var batchesBefore = await SnapshotRowsAsync("SELECT * FROM etl_batches ORDER BY batch_id;");
        var sendAttemptsBefore = await SnapshotRowsAsync("SELECT * FROM etl_batch_send_attempts ORDER BY attempt_id;");
        var ownershipBefore = await SnapshotRowsAsync("SELECT * FROM etl_entity_ownership ORDER BY entity_name;");
        var bindingsBefore = await SnapshotRowsAsync("SELECT * FROM etl_run_ownership_bindings ORDER BY run_id, entity_name;");
        var watermarksBefore = await SnapshotRowsAsync("SELECT * FROM watermarks ORDER BY entity_name;");
        var stateBefore = await SnapshotRowsAsync("SELECT * FROM agent_state ORDER BY key;");
        var snapshotsBefore = await SnapshotRowsAsync("SELECT * FROM config_snapshots ORDER BY config_version;");
        var conflictsBefore = await SnapshotRowsAsync("SELECT * FROM command_payload_conflicts ORDER BY event_id;");
        var ledgerBefore = await SnapshotRowsAsync("SELECT version,name,checksum,applied_at_utc FROM schema_migrations WHERE version<=9 ORDER BY version;");

        await _migrator.ApplyAsync(CancellationToken.None);

        Assert.Equal(12, SqliteMigrator.CurrentSchemaVersion);
        Assert.Equal(12, await CountAsync("SELECT COUNT(*) FROM schema_migrations"));
        // Every pre-existing row in every table survives byte-identical — the scheduled
        // runs with their keys and frozen identity, the unresolved failed/blocked runs,
        // the live 'admitted' attempt row, retained + released ownership, bindings.
        Assert.Equal(commandsBefore, await SnapshotRowsAsync("SELECT * FROM commands_inbox ORDER BY command_id;"));
        Assert.Equal(attemptsBefore, await SnapshotRowsAsync("SELECT * FROM command_attempts ORDER BY attempt_id;"));
        Assert.Equal(outboxBefore, await SnapshotRowsAsync("SELECT * FROM results_outbox ORDER BY result_id;"));
        Assert.Equal(jobsBefore, await SnapshotRowsAsync("SELECT * FROM etl_jobs ORDER BY job_id;"));
        Assert.Equal(runsBefore, await SnapshotRowsAsync($"SELECT {RunColumnsV9} FROM etl_runs ORDER BY run_id;"));
        Assert.Equal(entitiesBefore, await SnapshotRowsAsync("SELECT run_id,entity_name,entity_definition_json,domain_fingerprint,status,base_row_present,expected_base_generation,expected_base_cursor_json,expected_base_domain_fingerprint,domain_status,watermark_from_json,snapshot_upper_bound_json,final_watermark_json,expected_batch_count,rows_read,batches_created,last_error,created_at_utc,updated_at_utc,row_version FROM etl_run_entities ORDER BY run_id, entity_name;"));
        Assert.Equal(batchesBefore, await SnapshotRowsAsync("SELECT * FROM etl_batches ORDER BY batch_id;"));
        Assert.Equal(sendAttemptsBefore, await SnapshotRowsAsync("SELECT * FROM etl_batch_send_attempts ORDER BY attempt_id;"));
        Assert.Equal(ownershipBefore, await SnapshotRowsAsync("SELECT * FROM etl_entity_ownership ORDER BY entity_name;"));
        Assert.Equal(bindingsBefore, await SnapshotRowsAsync("SELECT * FROM etl_run_ownership_bindings ORDER BY run_id, entity_name;"));
        Assert.Equal(watermarksBefore, await SnapshotRowsAsync("SELECT * FROM watermarks ORDER BY entity_name;"));
        Assert.Equal(stateBefore, await SnapshotRowsAsync("SELECT * FROM agent_state ORDER BY key;"));
        Assert.Equal(snapshotsBefore, await SnapshotRowsAsync("SELECT * FROM config_snapshots ORDER BY config_version;"));
        Assert.Equal(conflictsBefore, await SnapshotRowsAsync("SELECT * FROM command_payload_conflicts ORDER BY event_id;"));
        Assert.Equal(ledgerBefore, await SnapshotRowsAsync("SELECT version,name,checksum,applied_at_utc FROM schema_migrations WHERE version<=9 ORDER BY version;"));

        // The resolutions table exists and is EMPTY — no pre-010 run is ever
        // retroactively attributed a resolution, even the failed/blocked ones.
        Assert.Equal(1, await CountAsync("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='etl_run_resolutions'"));
        Assert.Equal(0, await CountAsync("SELECT COUNT(*) FROM etl_run_resolutions"));
        Assert.Equal(12, await CountAsync("SELECT COUNT(*) FROM etl_runs"));
        Assert.Equal(4, await CountAsync("SELECT COUNT(*) FROM etl_runs WHERE schedule_key IS NOT NULL"));
        Assert.Equal(4, await CountAsync("SELECT COUNT(*) FROM etl_runs WHERE resolved_entities_json IS NOT NULL"));
        // The unresolved failed/blocked runs still hold no resolution marker.
        Assert.Equal(0, await CountAsync("SELECT COUNT(*) FROM etl_runs WHERE status IN ('failed','blocked') AND resolved_at_utc IS NOT NULL"));
        Assert.Equal(4, await CountAsync("SELECT COUNT(*) FROM etl_runs WHERE status IN ('failed','blocked')"));

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
    public async Task Apply_is_idempotent_on_a_populated_v10_database()
    {
        await _migrator.ApplyAsync(CancellationToken.None);
        var runsAfterFirst = await SnapshotRowsAsync("SELECT * FROM etl_runs ORDER BY run_id;");
        var resolutionsAfterFirst = await SnapshotRowsAsync("SELECT * FROM etl_run_resolutions ORDER BY run_id;");
        var ledgerAfterFirst = await SnapshotRowsAsync("SELECT version,name,checksum,applied_at_utc FROM schema_migrations ORDER BY version;");

        await _migrator.ApplyAsync(CancellationToken.None);

        Assert.Equal(runsAfterFirst, await SnapshotRowsAsync("SELECT * FROM etl_runs ORDER BY run_id;"));
        Assert.Equal(resolutionsAfterFirst, await SnapshotRowsAsync("SELECT * FROM etl_run_resolutions ORDER BY run_id;"));
        Assert.Equal(ledgerAfterFirst, await SnapshotRowsAsync("SELECT version,name,checksum,applied_at_utc FROM schema_migrations ORDER BY version;"));
        Assert.Equal(12, await CountAsync("SELECT COUNT(*) FROM schema_migrations"));
    }

    [Fact]
    public async Task Resolutions_table_enforces_its_check_domain_and_run_fk()
    {
        await _migrator.ApplyAsync(CancellationToken.None);
        var run = "00000000-0000-0000-0000-0000000000f5"; // the unresolved 'failed' run
        var now = "2026-09-26T00:00:00.0000000+00:00";

        // decision outside the bounded set.
        await Assert.ThrowsAsync<SqliteException>(() => InsertResolutionAsync("r-bad-decision", run, now, "op", "pause", "verified at ERP", 1, "failed", 0, 0));
        // the operator attestation of quiescence is mandatory.
        await Assert.ThrowsAsync<SqliteException>(() => InsertResolutionAsync("r-not-quiesced", run, now, "op", "abandon", "verified at ERP", 0, "failed", 0, 0));
        // only failed/blocked runs can ever carry a resolution.
        await Assert.ThrowsAsync<SqliteException>(() => InsertResolutionAsync("r-bad-prior", run, now, "op", "abandon", "verified at ERP", 1, "running", 0, 0));
        // blank attestations are rejected.
        await Assert.ThrowsAsync<SqliteException>(() => InsertResolutionAsync("r-blank-op", run, now, "   ", "abandon", "verified at ERP", 1, "failed", 0, 0));
        await Assert.ThrowsAsync<SqliteException>(() => InsertResolutionAsync("r-blank-remote", run, now, "op", "abandon", "", 1, "failed", 0, 0));
        // negative counts are rejected.
        await Assert.ThrowsAsync<SqliteException>(() => InsertResolutionAsync("r-neg-ownership", run, now, "op", "abandon", "verified at ERP", 1, "failed", -1, 0));
        await Assert.ThrowsAsync<SqliteException>(() => InsertResolutionAsync("r-neg-batches", run, now, "op", "abandon", "verified at ERP", 1, "failed", 0, -1));
        // A resolution for a run that does not exist violates the FK.
        await Assert.ThrowsAsync<SqliteException>(() => InsertResolutionAsync("r-missing-run", "00000000-0000-0000-0000-0000000000ff", now, "op", "abandon", "verified at ERP", 1, "failed", 0, 0));

        // A valid row is accepted.
        await InsertResolutionAsync("r-valid", run, now, "operator-1", "abandon", "verified at ERP", 1, "failed", 0, 0);
        Assert.Equal(1, await CountAsync("SELECT COUNT(*) FROM etl_run_resolutions"));

        // One immutable record per resolved run (PK) and per resolution id (UNIQUE).
        await Assert.ThrowsAsync<SqliteException>(() => InsertResolutionAsync("r-other", run, now, "op", "retry", "verified at ERP", 1, "failed", 0, 0));
        await Assert.ThrowsAsync<SqliteException>(() => InsertResolutionAsync("r-valid", "00000000-0000-0000-0000-0000000000f6", now, "op", "retry", "verified at ERP", 1, "blocked", 0, 0));
        Assert.Equal(1, await CountAsync("SELECT COUNT(*) FROM etl_run_resolutions"));
    }

    private async Task InsertResolutionAsync(string resolutionId, string runId, string resolvedAtUtc, string operatorId, string decision, string remoteVerification, int workersQuiesced, string priorStatus, int ownershipReleased, int batchesFenced)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = "INSERT INTO etl_run_resolutions(run_id,resolution_id,resolved_at_utc,operator_id,decision,remote_verification,workers_quiesced,prior_status,ownership_released,batches_fenced) VALUES($run,$res,$at,$op,$decision,$remote,$quiesced,$prior,$released,$fenced);";
        command.Parameters.AddWithValue("$run", runId);
        command.Parameters.AddWithValue("$res", resolutionId);
        command.Parameters.AddWithValue("$at", resolvedAtUtc);
        command.Parameters.AddWithValue("$op", operatorId);
        command.Parameters.AddWithValue("$decision", decision);
        command.Parameters.AddWithValue("$remote", remoteVerification);
        command.Parameters.AddWithValue("$quiesced", workersQuiesced);
        command.Parameters.AddWithValue("$prior", priorStatus);
        command.Parameters.AddWithValue("$released", ownershipReleased);
        command.Parameters.AddWithValue("$fenced", batchesFenced);
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
