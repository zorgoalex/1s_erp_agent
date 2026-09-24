using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using ErpOnecAgent.Infrastructure.Persistence.Sqlite;
using Microsoft.Data.Sqlite;
using Xunit;

namespace ErpOnecAgent.IntegrationTests;

/// <summary>
/// A09 migration review for 003_ordering_claims: seeds a real v2 database (immutable 001_initial.sql
/// + 002_retry_budgets.sql + correct v2 schema_migrations ledger rows + Fixtures/v2-populated/
/// populated-v2.sql) and asserts the 003 backfill semantics: queue_sequence is assigned deterministically
/// from (received_at_utc, command_id) so equal receive times get a stable total order, exec_claim_* stays
/// NULL (claims are runtime-only and never fabricated at migration), and existing rows/results are never
/// rewritten. Idempotency is asserted by running the migrator twice.
/// </summary>
public sealed class OrderingClaimsMigrationTests : IAsyncLifetime
{
    private const string Group1Head = "00000000-0000-0000-0000-000000000a01";
    private const string Group1Next = "00000000-0000-0000-0000-000000000a02";
    private const string Group2Head = "00000000-0000-0000-0000-000000000b01";
    private const string Group2Next = "00000000-0000-0000-0000-000000000b02";
    private const string TerminalCompleted = "00000000-0000-0000-0000-000000000808";

    private const string LedgerAppliedAt = "2026-09-20T00:00:00.0000000+00:00";

    private readonly SqliteTestDatabase _database = new();
    private SqliteConnectionFactory _factory = null!;
    private string _migration001Checksum = null!;
    private string _migration002Checksum = null!;
    private string _migration003Checksum = null!;

    public async Task InitializeAsync()
    {
        var migration001Sql = await File.ReadAllTextAsync(Path.Combine(AppContext.BaseDirectory, "Migrations", "001_initial.sql"));
        var migration002Sql = await File.ReadAllTextAsync(Path.Combine(AppContext.BaseDirectory, "Migrations", "002_retry_budgets.sql"));
        var migration003Sql = await File.ReadAllTextAsync(Path.Combine(AppContext.BaseDirectory, "Migrations", "003_ordering_claims.sql"));
        _migration001Checksum = Checksum(migration001Sql);
        _migration002Checksum = Checksum(migration002Sql);
        _migration003Checksum = Checksum(migration003Sql);
        _factory = _database.CreateFactory(Path.Combine("data", "agent.db"));

        // Seed an actual v2 database: 001 + 002 schema + correct v1/v2 ledger rows + populated v2 data.
        await using (var connection = await _factory.OpenAsync(CancellationToken.None))
        {
            await ExecuteAsync(connection, "CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY, name TEXT NOT NULL, checksum TEXT NOT NULL, applied_at_utc TEXT NOT NULL);");
            await ExecuteAsync(connection, migration001Sql);
            await ExecuteAsync(connection, migration002Sql);
            await InsertLedgerAsync(connection, 1, "001_initial.sql", _migration001Checksum);
            await InsertLedgerAsync(connection, 2, "002_retry_budgets.sql", _migration002Checksum);
            await ExecuteAsync(connection, await File.ReadAllTextAsync(Path.Combine(AppContext.BaseDirectory, "Fixtures", "v2-populated", "populated-v2.sql")));
        }
        await SqliteTestDatabase.ClearPoolAsync(_factory);
    }

    public async Task DisposeAsync()
    {
        await _database.DisposeAsync();
    }

    [Fact]
    public async Task Equal_received_time_rows_get_deterministic_queue_sequence_from_command_id()
    {
        await MigrateAsync();

        // With distinct receive times the backfill is the pure total order (received_at_utc, command_id):
        // 808 @06:00 -> 1, then the 07:00 pair a01 < a02 -> 2,3, then the 08:00 pair b01 < b02 -> 4,5.
        Assert.Equal(1, await CountAsync("SELECT queue_sequence FROM commands_inbox WHERE command_id=$id", TerminalCompleted));
        Assert.Equal(2, await CountAsync("SELECT queue_sequence FROM commands_inbox WHERE command_id=$id", Group1Head));
        Assert.Equal(3, await CountAsync("SELECT queue_sequence FROM commands_inbox WHERE command_id=$id", Group1Next));
        Assert.Equal(4, await CountAsync("SELECT queue_sequence FROM commands_inbox WHERE command_id=$id", Group2Head));
        Assert.Equal(5, await CountAsync("SELECT queue_sequence FROM commands_inbox WHERE command_id=$id", Group2Next));
    }

    [Fact]
    public async Task Equal_received_time_pair_is_ordered_by_command_id_and_stable_across_a_second_run()
    {
        await MigrateAsync();

        // The exactly-equal receive timestamps (a01 = a02 at 07:00; b01 = b02 at 08:00) are resolved by
        // the command_id tie-break inside the deterministic total order: a01 before a02, b01 before b02.
        Assert.True(await CountAsync("SELECT queue_sequence FROM commands_inbox WHERE command_id=$id", Group1Head) <
                    await CountAsync("SELECT queue_sequence FROM commands_inbox WHERE command_id=$id", Group1Next));
        Assert.True(await CountAsync("SELECT queue_sequence FROM commands_inbox WHERE command_id=$id", Group2Head) <
                    await CountAsync("SELECT queue_sequence FROM commands_inbox WHERE command_id=$id", Group2Next));
    }

    [Fact]
    public async Task Terminal_row_keeps_its_state_and_result_and_is_never_rewritten()
    {
        await MigrateAsync();

        // The completed row keeps its terminal state, result and ACK (only additive columns are filled).
        Assert.Equal(1, await CountAsync("SELECT queue_sequence FROM commands_inbox WHERE command_id=$id", TerminalCompleted));
        Assert.Equal("completed", await StringAsync("SELECT status FROM commands_inbox WHERE command_id=$id", TerminalCompleted));
        Assert.Equal("succeeded_local", await StringAsync("SELECT result_status FROM commands_inbox WHERE command_id=$id", TerminalCompleted));
        Assert.Equal(1, await CountAsync("SELECT COUNT(*) FROM results_outbox"));
    }

    [Fact]
    public async Task Execution_claims_are_null_after_migration()
    {
        await MigrateAsync();

        // exec_claim_* is runtime-only: migration backfills it to NULL and never fabricates a claim.
        Assert.Equal(0, await CountAsync("SELECT COUNT(*) FROM commands_inbox WHERE exec_claim_owner_id IS NOT NULL OR exec_claim_acquired_at_utc IS NOT NULL"));
        Assert.Equal(5, await CountAsync("SELECT COUNT(*) FROM commands_inbox"));
    }

    [Fact]
    public async Task Migrator_is_idempotent_and_checksums_stay_stable_across_second_run()
    {
        await MigrateAsync();
        var firstPass = await SnapshotAsync();

        await MigrateAsync();

        var secondPass = await SnapshotAsync();
        Assert.Equal(firstPass, secondPass);
        Assert.Equal(3, await CountAsync("SELECT COUNT(*) FROM schema_migrations"));
        Assert.Equal(_migration001Checksum, await StringAsync("SELECT checksum FROM schema_migrations WHERE version=1"));
        Assert.Equal(_migration002Checksum, await StringAsync("SELECT checksum FROM schema_migrations WHERE version=2"));
        Assert.Equal(_migration003Checksum, await StringAsync("SELECT checksum FROM schema_migrations WHERE version=3"));
        Assert.Equal(LedgerAppliedAt, await StringAsync("SELECT applied_at_utc FROM schema_migrations WHERE version=1"));
    }

    // Applies 003 exactly like SqliteMigrator: the full script text on one connection inside a
    // transaction, recording the SHA-256 checksum in schema_migrations. Re-running with an applied
    // version is a no-op (checksum verified), matching migrator behavior.
    private async Task MigrateAsync()
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        var sql = await File.ReadAllTextAsync(Path.Combine(AppContext.BaseDirectory, "Migrations", "003_ordering_claims.sql"));
        var checksum = Checksum(sql);
        await using (var check = connection.CreateCommand())
        {
            check.CommandText = "SELECT checksum FROM schema_migrations WHERE version=$version;";
            check.Parameters.AddWithValue("$version", 3);
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
            record.Parameters.AddWithValue("$version", 3);
            record.Parameters.AddWithValue("$name", "003_ordering_claims.sql");
            record.Parameters.AddWithValue("$checksum", checksum);
            record.Parameters.AddWithValue("$now", LedgerAppliedAt);
            await record.ExecuteNonQueryAsync(CancellationToken.None);
        }
        await transaction.CommitAsync(CancellationToken.None);
    }

    private static async Task InsertLedgerAsync(SqliteConnection connection, int version, string name, string checksum)
    {
        await using var record = connection.CreateCommand();
        record.CommandText = "INSERT INTO schema_migrations(version,name,checksum,applied_at_utc) VALUES($version,$name,$checksum,$now);";
        record.Parameters.AddWithValue("$version", version);
        record.Parameters.AddWithValue("$name", name);
        record.Parameters.AddWithValue("$checksum", checksum);
        record.Parameters.AddWithValue("$now", LedgerAppliedAt);
        await record.ExecuteNonQueryAsync(CancellationToken.None);
    }

    private static string Checksum(string sql) => Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(sql)));

    private async Task<IReadOnlyList<string?>> SnapshotAsync()
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT command_id,status,queue_sequence,exec_claim_owner_id,exec_claim_acquired_at_utc FROM commands_inbox ORDER BY command_id; SELECT result_id,status FROM results_outbox ORDER BY result_id; SELECT version,name,checksum,applied_at_utc FROM schema_migrations ORDER BY version;";
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
