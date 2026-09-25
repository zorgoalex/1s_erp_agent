using System.Globalization;
using Microsoft.Data.Sqlite;

namespace ErpOnecAgent.Infrastructure.Persistence.Sqlite;

/// <summary>
/// Stage 6: verifies a backup file independently of the live database — read-only, unpooled
/// (the file must not stay locked), full integrity check, and a migrated agent schema.
/// Returns "ok" or the reason the file is not a usable backup.
/// </summary>
public static class SqliteBackupVerifier
{
    public static async Task<string> VerifyAsync(string path, CancellationToken cancellationToken)
    {
        if (!File.Exists(path)) return "missing";
        var builder = new SqliteConnectionStringBuilder { DataSource = path, Mode = SqliteOpenMode.ReadOnly, Pooling = false };
        try
        {
            await using var connection = new SqliteConnection(builder.ToString());
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            await using var integrity = connection.CreateCommand();
            integrity.CommandText = "PRAGMA integrity_check;";
            var result = Convert.ToString(await integrity.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false), CultureInfo.InvariantCulture) ?? "unknown";
            if (!string.Equals(result, "ok", StringComparison.OrdinalIgnoreCase)) return result;
            await using var schema = connection.CreateCommand();
            schema.CommandText = "SELECT COUNT(*) FROM schema_migrations;";
            var migrations = Convert.ToInt64(await schema.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false), CultureInfo.InvariantCulture);
            return migrations > 0 ? "ok" : "no applied migrations";
        }
        catch (SqliteException ex)
        {
            return $"not a usable SQLite backup: {ex.Message}";
        }
    }
}
