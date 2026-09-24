using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using ErpOnecAgent.Infrastructure.Persistence.Sqlite;
using Microsoft.Data.Sqlite;
using Xunit;

namespace ErpOnecAgent.IntegrationTests;

/// <summary>
/// A09 migration review: seeds a real v1 database (immutable 001_initial.sql + correct v1
/// schema_migrations ledger row + Fixtures/v1-populated/populated-v1.sql) and asserts the
/// 002_retry_budgets backfill semantics: evidence of prior send (attempt records / attempt_count)
/// drives first_sent_at_utc, never a status-based exclusion; earliest actual attempt timestamp
/// wins; conservative started_at_utc fallback only for potentially-sent rows without attempt rows;
/// never-sent queued/retry_waiting stay NULL. Idempotency is asserted by running the migrator twice.
/// </summary>
public sealed class RetryBudgetMigrationTests : IAsyncLifetime
{
    private const string NeverSentQueued = "00000000-0000-0000-0000-000000000101";
    private const string NeverSentRetryWaiting = "00000000-0000-0000-0000-000000000202";
    private const string SentRetryWaiting = "00000000-0000-0000-0000-000000000303";
    private const string SentUnknownResult = "00000000-0000-0000-0000-000000000404";
    private const string ExecutingFallback = "00000000-0000-0000-0000-000000000505";
    private const string UnknownResultFallback = "00000000-0000-0000-0000-000000000606";
    private const string TerminalResultPending = "00000000-0000-0000-0000-000000000707";
    private const string TerminalCompleted = "00000000-0000-0000-0000-000000000808";

    private const string EarliestAttemptSentRetryWaiting = "2026-09-10T08:00:00.0000000+00:00";
    private const string EarliestAttemptSentUnknownResult = "2026-09-13T09:00:00.0000000+00:00";
    private const string EarliestAttemptTerminalResultPending = "2026-09-10T09:00:00.0000000+00:00";
    private const string StartedAtExecutingFallback = "2026-09-15T10:00:00.0000000+00:00";
    private const string StartedAtUnknownResultFallback = "2026-09-14T10:00:00.0000000+00:00";
    private const string StartedAtTerminalCompleted = "2026-09-11T12:00:00.0000000+00:00";
    private const string V1LedgerAppliedAt = "2026-09-20T00:00:00.0000000+00:00";

    private readonly SqliteTestDatabase _database = new();
    private SqliteConnectionFactory _factory = null!;
    private string _migration001Checksum = null!;
    private string _migration002Checksum = null!;

    public async Task InitializeAsync()
    {
        var migration001Sql = await File.ReadAllTextAsync(Path.Combine(AppContext.BaseDirectory, "Migrations", "001_initial.sql"));
        var migration002Sql = await File.ReadAllTextAsync(Path.Combine(AppContext.BaseDirectory, "Migrations", "002_retry_budgets.sql"));
        _migration001Checksum = Checksum(migration001Sql);
        _migration002Checksum = Checksum(migration002Sql);
        _factory = _database.CreateFactory(Path.Combine("data", "agent.db"));

        // Seed an actual v1 database: immutable 001 schema + the correct v1 ledger row
        // (checksum 1 = SHA-256 of 001_initial.sql text, exactly as the migrator computes it).
        await using (var connection = await _factory.OpenAsync(CancellationToken.None))
        {
            await ExecuteAsync(connection, "CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY, name TEXT NOT NULL, checksum TEXT NOT NULL, applied_at_utc TEXT NOT NULL);");
            await ExecuteAsync(connection, migration001Sql);
            await using var record = connection.CreateCommand();
            record.CommandText = "INSERT INTO schema_migrations(version,name,checksum,applied_at_utc) VALUES(1,'001_initial.sql',$checksum,$now);";
            record.Parameters.AddWithValue("$checksum", _migration001Checksum);
            record.Parameters.AddWithValue("$now", V1LedgerAppliedAt);
            await record.ExecuteNonQueryAsync(CancellationToken.None);
            await ExecuteAsync(connection, await File.ReadAllTextAsync(Path.Combine(AppContext.BaseDirectory, "Fixtures", "v1-populated", "populated-v1.sql")));
        }
        await SqliteTestDatabase.ClearPoolAsync(_factory);
    }

    public async Task DisposeAsync()
    {
        await _database.DisposeAsync();
    }

    [Fact]
    public async Task Sent_retry_waiting_keeps_earliest_send_age_from_attempts()
    {
        await MigrateAsync();

        // Evidence of prior send (3 command_attempts) must survive the retry_waiting state:
        // first_sent_at_utc = earliest actual attempt, NOT the overwritten commands.started_at_utc.
        Assert.Equal(EarliestAttemptSentRetryWaiting, await StringAsync("SELECT first_sent_at_utc FROM commands_inbox WHERE command_id=$id", SentRetryWaiting));
        Assert.Equal(3, await CountAsync("SELECT COUNT(*) FROM command_attempts WHERE command_id=$id", SentRetryWaiting));
    }

    [Fact]
    public async Task Never_sent_queued_and_retry_waiting_stay_null_without_fallback()
    {
        await MigrateAsync();

        // Never-sent rows (no attempts, attempt_count = 0) keep NULL even though started_at_utc is
        // NULL: the age budget starts at the first real POST claim, never at admission time.
        Assert.Null(await StringAsync("SELECT first_sent_at_utc FROM commands_inbox WHERE command_id=$id", NeverSentQueued));
        Assert.Null(await StringAsync("SELECT first_sent_at_utc FROM commands_inbox WHERE command_id=$id", NeverSentRetryWaiting));
    }

    [Fact]
    public async Task Sent_unknown_result_keeps_earliest_attempt_older_than_started_at()
    {
        await MigrateAsync();

        Assert.Equal(EarliestAttemptSentUnknownResult, await StringAsync("SELECT first_sent_at_utc FROM commands_inbox WHERE command_id=$id", SentUnknownResult));
    }

    [Fact]
    public async Task Executing_and_unknown_result_without_attempts_use_conservative_started_at_fallback()
    {
        await MigrateAsync();

        // Potentially-sent rows without attempt rows (ambiguous send evidence): conservative
        // fallback to the overwritten commands.started_at_utc rather than NULL (the age budget
        // must not restart) and rather than a fabricated attempt timestamp.
        Assert.Equal(StartedAtExecutingFallback, await StringAsync("SELECT first_sent_at_utc FROM commands_inbox WHERE command_id=$id", ExecutingFallback));
        Assert.Equal(StartedAtUnknownResultFallback, await StringAsync("SELECT first_sent_at_utc FROM commands_inbox WHERE command_id=$id", UnknownResultFallback));
    }

    [Fact]
    public async Task Terminal_rows_keep_first_send_evidence_and_results()
    {
        await MigrateAsync();

        // result_pending (sent, 1 attempt): terminal-for-execution rows still carry send evidence
        // and must retain the earliest attempt age (previously lost to the broad status exclusion).
        Assert.Equal(EarliestAttemptTerminalResultPending, await StringAsync("SELECT first_sent_at_utc FROM commands_inbox WHERE command_id=$id", TerminalResultPending));
        // completed (A01/A02): rows and results are preserved untouched.
        Assert.Equal(StartedAtTerminalCompleted, await StringAsync("SELECT first_sent_at_utc FROM commands_inbox WHERE command_id=$id", TerminalCompleted));
        Assert.Equal(2, await CountAsync("SELECT COUNT(*) FROM results_outbox"));
        Assert.Equal("acknowledged", await StringAsync("SELECT status FROM results_outbox WHERE command_id=$id", TerminalCompleted));
    }

    [Fact]
    public async Task Retry_budget_counters_and_attempt_kinds_are_backfilled()
    {
        await MigrateAsync();

        // post_attempt_count backfills from legacy attempt_count (counters preserved).
        Assert.Equal(3, await CountAsync("SELECT post_attempt_count FROM commands_inbox WHERE command_id=$id", SentRetryWaiting));
        Assert.Equal(2, await CountAsync("SELECT post_attempt_count FROM commands_inbox WHERE command_id=$id", SentUnknownResult));
        Assert.Equal(0, await CountAsync("SELECT post_attempt_count FROM commands_inbox WHERE command_id=$id", NeverSentRetryWaiting));
        // lookup_attempt_count stays 0 for all v1 rows (never persisted before 002).
        Assert.Equal(0, await CountAsync("SELECT lookup_attempt_count FROM commands_inbox WHERE command_id=$id", SentRetryWaiting));
        // legacy attempt rows normalize to attempt_kind='post'.
        Assert.Equal(6, await CountAsync("SELECT COUNT(*) FROM command_attempts WHERE attempt_kind='post'"));
    }

    [Fact]
    public async Task Migrator_is_idempotent_and_checksums_stay_stable_across_second_run()
    {
        await MigrateAsync();
        var firstPass = await SnapshotAsync();

        await MigrateAsync();

        var secondPass = await SnapshotAsync();
        Assert.Equal(firstPass, secondPass);
        Assert.Equal(2, await CountAsync("SELECT COUNT(*) FROM schema_migrations"));
        Assert.Equal(_migration001Checksum, await StringAsync("SELECT checksum FROM schema_migrations WHERE version=1"));
        Assert.Equal(_migration002Checksum, await StringAsync("SELECT checksum FROM schema_migrations WHERE version=2"));
        Assert.Equal(V1LedgerAppliedAt, await StringAsync("SELECT applied_at_utc FROM schema_migrations WHERE version=1"));
    }

    // Applies 002 exactly like SqliteMigrator: the full script text on one connection inside a
    // transaction, recording the SHA-256 checksum in schema_migrations. Re-running with an
    // applied version is a no-op (checksum verified), matching migrator behavior.
    private async Task MigrateAsync()
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        var sql = await File.ReadAllTextAsync(Path.Combine(AppContext.BaseDirectory, "Migrations", "002_retry_budgets.sql"));
        var checksum = Checksum(sql);
        await using (var check = connection.CreateCommand())
        {
            check.CommandText = "SELECT checksum FROM schema_migrations WHERE version=$version;";
            check.Parameters.AddWithValue("$version", 2);
            if (await check.ExecuteScalarAsync(CancellationToken.None) is string existing)
            {
                Assert.Equal(checksum, existing);
                return;
            }
        }

        await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(CancellationToken.None);
        await using (var apply = connection.CreateCommand())
        {
            apply.Transaction = transaction;
            apply.CommandText = sql;
            await apply.ExecuteNonQueryAsync(CancellationToken.None);
        }
        await using (var record = connection.CreateCommand())
        {
            record.Transaction = transaction;
            record.CommandText = "INSERT INTO schema_migrations(version,name,checksum,applied_at_utc) VALUES($version,$name,$checksum,$now);";
            record.Parameters.AddWithValue("$version", 2);
            record.Parameters.AddWithValue("$name", "002_retry_budgets.sql");
            record.Parameters.AddWithValue("$checksum", checksum);
            record.Parameters.AddWithValue("$now", V1LedgerAppliedAt);
            await record.ExecuteNonQueryAsync(CancellationToken.None);
        }
        await transaction.CommitAsync(CancellationToken.None);
    }

    private static string Checksum(string sql) => Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(sql)));

    private async Task<IReadOnlyList<string?>> SnapshotAsync()
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT command_id,status,first_sent_at_utc,post_attempt_count,lookup_attempt_count FROM commands_inbox ORDER BY command_id; SELECT attempt_id,attempt_kind,started_at_utc FROM command_attempts ORDER BY attempt_id; SELECT result_id,status FROM results_outbox ORDER BY result_id; SELECT version,name,checksum,applied_at_utc FROM schema_migrations ORDER BY version;";
        var rows = new List<string?>();
        await using var reader = await command.ExecuteReaderAsync(CancellationToken.None);
        do
        {
            while (await reader.ReadAsync(CancellationToken.None))
            {
                var fields = new string[reader.FieldCount];
                for (var i = 0; i < reader.FieldCount; i++) fields[i] = reader.IsDBNull(i) ? "<null>" : reader.GetString(i);
                rows.Add(string.Join("|", fields));
            }
        } while (await reader.NextResultAsync(CancellationToken.None));
        return rows;
    }

    private async Task<long> CountAsync(string sql, string? commandId = null)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        if (commandId is not null) command.Parameters.AddWithValue("$id", commandId);
        return Convert.ToInt64(await command.ExecuteScalarAsync(CancellationToken.None), CultureInfo.InvariantCulture);
    }

    private async Task<string?> StringAsync(string sql, string? commandId = null)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        if (commandId is not null) command.Parameters.AddWithValue("$id", commandId);
        return await command.ExecuteScalarAsync(CancellationToken.None) as string;
    }

    private static async Task ExecuteAsync(SqliteConnection connection, string sql)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync(CancellationToken.None);
    }
}

