using System.Globalization;
using System.Reflection;
using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Application.Configuration;
using ErpOnecAgent.Infrastructure.Persistence.Sqlite;
using Microsoft.Data.Sqlite;
using Xunit;

namespace ErpOnecAgent.IntegrationTests;

/// <summary>
/// O2 runtime-RED suite: every test exercises a guarantee that is missing on the
/// pre-008 baseline and fails at RUNTIME there (SQLite errors, absent members via
/// reflection, or behavioral assertions) — no compile-only assertions. The suite
/// never calls the new send-ledger APIs directly so it compiles unchanged against
/// the baseline; after the O2 slice lands the same tests are GREEN.
/// Guarantees proven missing on baseline:
///   * migration 008 (etl_batch_send_attempts + etl_batches send columns) does not exist;
///   * the fenced send/ACK/outcome APIs are absent from IAgentStore;
///   * EtlOptions carries no upload-attempt bound;
///   * new-path recovery leaves an in-flight 'uploading' batch dispatchable instead of
///     quarantining it UPLOAD_OUTCOME_UNKNOWN (the fail-closed unknown-outcome rule);
///   * attempt rows cannot exist, so terminal-batch retention cannot reconcile them.
/// </summary>
public sealed class EtlSendLedgerRedTests : IAsyncLifetime
{
    private readonly SqliteTestDatabase _database = new();
    private SqliteConnectionFactory _factory = null!;
    private SqliteAgentStore _store = null!;

    public async Task InitializeAsync()
    {
        _factory = _database.CreateFactory(Path.Combine("data", "agent.db"));
        _store = new(_factory, new SqliteMigrator(_factory));
        await _store.InitializeAsync(CancellationToken.None);
    }

    public async Task DisposeAsync()
    {
        await _database.DisposeAsync();
    }

    [Fact]
    public async Task Migration_008_creates_the_send_attempt_ledger_and_batch_send_columns()
    {
        Assert.Equal(12, SqliteMigrator.CurrentSchemaVersion);
        Assert.Equal(12, await ScalarAsync("SELECT COUNT(*) FROM schema_migrations"));
        Assert.Equal("008_etl_send_attempts.sql", await ScalarStringAsync("SELECT name FROM schema_migrations WHERE version=8"));
        Assert.Equal("009_etl_scheduled_runs.sql", await ScalarStringAsync("SELECT name FROM schema_migrations WHERE version=9"));
        Assert.Equal("010_etl_run_resolutions.sql", await ScalarStringAsync("SELECT name FROM schema_migrations WHERE version=10"));
        Assert.Equal("011_watermark_domain_resets.sql", await ScalarStringAsync("SELECT name FROM schema_migrations WHERE version=11"));
        Assert.Equal(1, await ScalarAsync("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='etl_batch_send_attempts'"));
        // Full ledger shape per design §3.2, including the single-live-admission index.
        Assert.Equal(1, await ScalarAsync("SELECT COUNT(*) FROM pragma_table_info('etl_batch_send_attempts') WHERE name='attempt_id' AND pk=1"));
        Assert.Equal(1, await ScalarAsync("SELECT COUNT(*) FROM pragma_table_info('etl_batch_send_attempts') WHERE name='ack_payload_hash'"));
        Assert.Equal(1, await ScalarAsync("SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='ux_etl_batch_send_attempts_admitted' AND sql LIKE '%outcome=''admitted''%'"));
        // etl_batches send-fence/bound/quarantine columns.
        Assert.Equal(1, await ScalarAsync("SELECT COUNT(*) FROM pragma_table_info('etl_batches') WHERE name='send_attempt_id'"));
        Assert.Equal(1, await ScalarAsync("SELECT COUNT(*) FROM pragma_table_info('etl_batches') WHERE name='upload_max_attempts'"));
        Assert.Equal(1, await ScalarAsync("SELECT COUNT(*) FROM pragma_table_info('etl_batches') WHERE name='quarantine_code'"));
        Assert.Equal(1, await ScalarAsync("SELECT COUNT(*) FROM pragma_table_info('etl_batches') WHERE name='row_version' AND dflt_value='1'"));
    }

    [Fact]
    public void Send_ledger_store_APIs_are_declared_on_IAgentStore()
    {
        // String literals (not nameof): the members do not exist on the baseline, so
        // nameof would be a compile-time failure instead of a runtime RED.
        var names = new[]
        {
            "GetDueBatchUploadsAsync",
            "TryClaimBatchUploadAsync",
            "AcknowledgeClaimedBatchAsync",
            "FailClaimedBatchSendAsync",
            "RetryClaimedBatchSendAsync"
        };
        var declared = typeof(IAgentStore).GetMethods(BindingFlags.Public | BindingFlags.Instance).Select(m => m.Name).ToHashSet(StringComparer.Ordinal);
        foreach (var name in names)
        {
            Assert.True(declared.Contains(name), $"IAgentStore is missing {name} — the O2 send-ledger surface does not exist on the baseline.");
        }
    }

    [Fact]
    public void EtlOptions_declares_a_bounded_MaxBatchUploadAttempts_defaulting_to_5()
    {
        var property = typeof(EtlOptions).GetProperty("MaxBatchUploadAttempts", BindingFlags.Public | BindingFlags.Instance);
        Assert.NotNull(property);
        Assert.Equal(typeof(int), property!.PropertyType);
        Assert.Equal(5, Assert.IsType<int>(property.GetValue(new EtlOptions())));
    }

    [Fact]
    public async Task Recovery_orphans_an_admitted_attempt_and_quarantines_the_in_flight_upload()
    {
        // A sealed run in 'uploading' with a batch mid-send (admitted attempt, never
        // finished). On baseline the seed INSERT into the ledger fails (no table);
        // even the batch/run quarantine assertions would fail: new-path recovery never
        // touches 'uploading' runs or their batches.
        var runId = Guid.NewGuid();
        var batchId = Guid.NewGuid();
        var attemptId = Guid.NewGuid();
        var now = DateTimeOffset.UtcNow.ToString("O");
        await ExecuteSqlAsync(
            "INSERT INTO etl_runs(run_id,mode,requested_entities_json,status,started_at_utc,rows_read,batches_created) VALUES($run,'incremental','[\"clients\"]','uploading',$now,5,1);",
            ("$run", runId.ToString("D")), ("$now", now));
        await ExecuteSqlAsync(
            "INSERT INTO etl_batches(batch_id,run_id,entity_name,schema_version,file_path,status,row_count,sha256,compressed_size,uncompressed_size,created_at_utc,send_attempt_id) VALUES($b,$run,'clients',1,'spool/o2-red.gz','uploading',5,'h',10,10,$now,$attempt);",
            ("$b", batchId.ToString("D")), ("$run", runId.ToString("D")), ("$now", now), ("$attempt", attemptId.ToString("D")));
        await ExecuteSqlAsync(
            "INSERT INTO etl_batch_send_attempts(attempt_id,batch_id,attempt_no,owner_id,admitted_at_utc,outcome) VALUES($attempt,$b,1,'owner-1',$now,'admitted');",
            ("$attempt", attemptId.ToString("D")), ("$b", batchId.ToString("D")), ("$now", now));

        var result = await _store.RecoverInterruptedEtlRunsAsync(CancellationToken.None);

        Assert.Equal("orphaned", await ScalarStringAsync($"SELECT outcome FROM etl_batch_send_attempts WHERE attempt_id='{attemptId:D}'"));
        Assert.Equal("dead_letter", await ScalarStringAsync($"SELECT status FROM etl_batches WHERE batch_id='{batchId:D}'"));
        Assert.Equal("UPLOAD_OUTCOME_UNKNOWN", await ScalarStringAsync($"SELECT quarantine_code FROM etl_batches WHERE batch_id='{batchId:D}'"));
        Assert.Equal("blocked", await ScalarStringAsync($"SELECT status FROM etl_runs WHERE run_id='{runId:D}'"));
        Assert.Equal("UPLOAD_OUTCOME_UNKNOWN", await ScalarStringAsync($"SELECT finalize_conflict_code FROM etl_runs WHERE run_id='{runId:D}'"));
        // The recovery result must report orphaned attempts — reflected so the test
        // compiles on the baseline where the member does not exist.
        var orphaned = typeof(EtlRecoveryResult).GetProperty("AttemptsOrphaned", BindingFlags.Public | BindingFlags.Instance);
        Assert.NotNull(orphaned);
        Assert.True(Assert.IsType<int>(orphaned!.GetValue(result)) >= 1);
    }

    [Fact]
    public async Task Succeeded_run_cleanup_deletes_batch_attempt_rows_in_the_same_transaction()
    {
        // A succeeded run's 'deleted' batch still referenced by attempt rows: cleanup
        // must remove the attempts in the same tx — no FK failure, no orphan rows.
        // On baseline the ledger does not exist, so the seed fails at runtime.
        var runId = Guid.NewGuid();
        var batchId = Guid.NewGuid();
        var now = DateTimeOffset.UtcNow.ToString("O");
        await ExecuteSqlAsync(
            "INSERT INTO etl_runs(run_id,mode,requested_entities_json,status,started_at_utc,finished_at_utc) VALUES($run,'incremental','[\"clients\"]','succeeded',$now,$now);",
            ("$run", runId.ToString("D")), ("$now", now));
        await ExecuteSqlAsync(
            "INSERT INTO etl_batches(batch_id,run_id,entity_name,schema_version,file_path,status,row_count,sha256,compressed_size,uncompressed_size,created_at_utc,acknowledged_at_utc,send_attempt_id) VALUES($b,$run,'clients',1,'spool/o2-red-del.gz','deleted',5,'h',10,10,$now,$now,$attempt);",
            ("$b", batchId.ToString("D")), ("$run", runId.ToString("D")), ("$now", now), ("$attempt", Guid.NewGuid().ToString("D")));
        var attemptId = Guid.NewGuid();
        await ExecuteSqlAsync(
            "INSERT INTO etl_batch_send_attempts(attempt_id,batch_id,attempt_no,owner_id,admitted_at_utc,finished_at_utc,outcome) VALUES($attempt,$b,1,'owner-1',$now,$now,'acknowledged');",
            ("$attempt", attemptId.ToString("D")), ("$b", batchId.ToString("D")), ("$now", now));

        await _store.CleanupAsync(DateTimeOffset.UtcNow.AddDays(1), DateTimeOffset.UtcNow.AddDays(1), CancellationToken.None);

        Assert.Equal(0, await ScalarAsync($"SELECT COUNT(*) FROM etl_batch_send_attempts WHERE batch_id='{batchId:D}'"));
        Assert.Equal(0, await ScalarAsync($"SELECT COUNT(*) FROM etl_batches WHERE batch_id='{batchId:D}'"));
    }

    [Fact]
    public async Task Attempt_rows_of_an_unresolved_run_are_never_deleted()
    {
        var runId = Guid.NewGuid();
        var batchId = Guid.NewGuid();
        var attemptId = Guid.NewGuid();
        var now = DateTimeOffset.UtcNow.ToString("O");
        await ExecuteSqlAsync(
            "INSERT INTO etl_runs(run_id,mode,requested_entities_json,status,started_at_utc,finished_at_utc,finalize_conflict_code) VALUES($run,'incremental','[\"clients\"]','blocked',$now,$now,'UPLOAD_OUTCOME_UNKNOWN');",
            ("$run", runId.ToString("D")), ("$now", now));
        await ExecuteSqlAsync(
            "INSERT INTO etl_batches(batch_id,run_id,entity_name,schema_version,file_path,status,row_count,sha256,compressed_size,uncompressed_size,created_at_utc,send_attempt_id) VALUES($b,$run,'clients',1,'spool/o2-red-kept.gz','dead_letter',5,'h',10,10,$now,$attempt);",
            ("$b", batchId.ToString("D")), ("$run", runId.ToString("D")), ("$now", now), ("$attempt", attemptId.ToString("D")));
        await ExecuteSqlAsync(
            "INSERT INTO etl_batch_send_attempts(attempt_id,batch_id,attempt_no,owner_id,admitted_at_utc,finished_at_utc,outcome) VALUES($attempt,$b,1,'owner-1',$now,$now,'unknown');",
            ("$attempt", attemptId.ToString("D")), ("$b", batchId.ToString("D")), ("$now", now));

        await _store.CleanupAsync(DateTimeOffset.UtcNow.AddDays(1), DateTimeOffset.UtcNow.AddDays(1), CancellationToken.None);

        Assert.Equal(1, await ScalarAsync($"SELECT COUNT(*) FROM etl_batch_send_attempts WHERE attempt_id='{attemptId:D}'"));
        Assert.Equal(1, await ScalarAsync($"SELECT COUNT(*) FROM etl_batches WHERE batch_id='{batchId:D}'"));
        Assert.Equal("dead_letter", await ScalarStringAsync($"SELECT status FROM etl_batches WHERE batch_id='{batchId:D}'"));
    }

    private async Task<long> ScalarAsync(string sql)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        return Convert.ToInt64(await command.ExecuteScalarAsync(CancellationToken.None), CultureInfo.InvariantCulture);
    }

    private async Task<string?> ScalarStringAsync(string sql)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        var value = await command.ExecuteScalarAsync(CancellationToken.None);
        return value is null or DBNull ? null : Convert.ToString(value, CultureInfo.InvariantCulture);
    }

    private async Task ExecuteSqlAsync(string sql, params (string Name, object? Value)[] parameters)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        foreach (var (name, value) in parameters) command.Parameters.AddWithValue(name, value ?? DBNull.Value);
        await command.ExecuteNonQueryAsync(CancellationToken.None);
    }
}
