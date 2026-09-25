using System.Security.Cryptography;
using Microsoft.Data.Sqlite;

namespace ErpOnecAgent.Infrastructure.Persistence.Sqlite;

public sealed class SqliteMigrator(SqliteConnectionFactory factory)
{
    public const int CurrentSchemaVersion = 9;

    public async Task ApplyAsync(CancellationToken cancellationToken)
    {
        await using var connection = await factory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await ExecuteAsync(connection, "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA temp_store=MEMORY; PRAGMA wal_autocheckpoint=1000;", cancellationToken).ConfigureAwait(false);
        await ExecuteAsync(connection, "CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY, name TEXT NOT NULL, checksum TEXT NOT NULL, applied_at_utc TEXT NOT NULL);", cancellationToken).ConfigureAwait(false);

        var assembly = typeof(SqliteMigrator).Assembly;
        var resources = assembly.GetManifestResourceNames()
            .Where(static name => name.Contains(".Persistence.Migrations.", StringComparison.Ordinal) && name.EndsWith(".sql", StringComparison.Ordinal))
            .OrderBy(static name => name, StringComparer.Ordinal)
            .ToArray();

        foreach (var resource in resources)
        {
            var marker = resource.LastIndexOf(".Migrations.", StringComparison.Ordinal) + ".Migrations.".Length;
            var fileName = resource[marker..];
            var version = int.Parse(fileName.AsSpan(0, fileName.IndexOf('_', StringComparison.Ordinal)), System.Globalization.CultureInfo.InvariantCulture);
            await using var stream = assembly.GetManifestResourceStream(resource) ?? throw new InvalidOperationException($"Missing migration resource {resource}.");
            using var reader = new StreamReader(stream);
            var sql = await reader.ReadToEndAsync(cancellationToken).ConfigureAwait(false);
            var resourceChecksum = Convert.ToHexString(SHA256.HashData(System.Text.Encoding.UTF8.GetBytes(sql)));
            var checksum = MigrationChecksumCatalog.ResolveRecordedChecksum(version, fileName, resourceChecksum);

            await using var check = connection.CreateCommand();
            check.CommandText = "SELECT name, checksum FROM schema_migrations WHERE version=$version;";
            check.Parameters.AddWithValue("$version", version);
            string? existingName = null;
            string? existingChecksum = null;
            await using (var ledger = await check.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
            {
                if (await ledger.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    existingName = ledger.GetString(0);
                    existingChecksum = ledger.GetString(1);
                }
            }
            if (existingChecksum is not null)
            {
                if (!string.Equals(existingName, fileName, StringComparison.Ordinal)) throw new InvalidOperationException($"Name mismatch for applied migration version {version}: ledger has {existingName}, resource is {fileName}.");
                if (!MigrationChecksumCatalog.IsAcceptedStoredChecksum(version, fileName, resourceChecksum, existingChecksum)) throw new InvalidOperationException($"Checksum mismatch for applied migration {fileName}.");
                continue;
            }

            await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
            await using var apply = connection.CreateCommand();
            apply.Transaction = transaction;
            apply.CommandText = sql;
            await apply.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            await using var record = connection.CreateCommand();
            record.Transaction = transaction;
            record.CommandText = "INSERT INTO schema_migrations(version,name,checksum,applied_at_utc) VALUES($version,$name,$checksum,$now);";
            record.Parameters.AddWithValue("$version", version);
            record.Parameters.AddWithValue("$name", fileName);
            record.Parameters.AddWithValue("$checksum", checksum);
            record.Parameters.AddWithValue("$now", DateTimeOffset.UtcNow.ToString("O"));
            await record.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    private static async Task ExecuteAsync(SqliteConnection connection, string sql, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }
}
