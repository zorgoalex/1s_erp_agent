using System.Globalization;
using System.Text.Json;
using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Domain.Commands;
using ErpOnecAgent.Domain.Common;
using ErpOnecAgent.Domain.Etl;
using Microsoft.Data.Sqlite;

namespace ErpOnecAgent.Infrastructure.Persistence.Sqlite;

public sealed partial class SqliteAgentStore(SqliteConnectionFactory factory, SqliteMigrator migrator) : IAgentStore
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);
    // Process-lifetime write gate: serializes the read-check-write config transactions for every
    // store in this process so a concurrent caller cannot interleave between read and write.
    private static readonly SemaphoreSlim ConfigurationGate = new(1, 1);

    public Task InitializeAsync(CancellationToken cancellationToken) => migrator.ApplyAsync(cancellationToken);

    public async Task RecoverAsync(CancellationToken cancellationToken)
    {
        await using var connection = await factory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        await ExecuteAsync(connection, transaction,
            "UPDATE commands_inbox SET status='unknown_result', next_attempt_at_utc=$now, exec_claim_owner_id=NULL, exec_claim_acquired_at_utc=NULL, last_error_code='PROCESS_RESTART', last_error_message='Agent restarted during command execution', row_version=row_version+1 WHERE status='executing';",
            cancellationToken, ("$now", UtcNow())).ConfigureAwait(false);
        await ExecuteAsync(connection, transaction,
            "UPDATE commands_inbox SET exec_claim_owner_id=NULL, exec_claim_acquired_at_utc=NULL, row_version=row_version+1 WHERE status IN ('queued','retry_waiting','unknown_result') AND (exec_claim_owner_id IS NOT NULL OR exec_claim_acquired_at_utc IS NOT NULL);",
            cancellationToken).ConfigureAwait(false);
        await ExecuteAsync(connection, transaction,
            "UPDATE results_outbox SET status='pending', next_attempt_at_utc=$now WHERE status='sending'; UPDATE etl_batches SET status='ready', next_attempt_at_utc=$now WHERE status='uploading';",
            cancellationToken, ("$now", UtcNow())).ConfigureAwait(false);
        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<string> IntegrityCheckAsync(CancellationToken cancellationToken)
    {
        await using var connection = await factory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = "PRAGMA integrity_check;";
        return Convert.ToString(await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false), CultureInfo.InvariantCulture) ?? "unknown";
    }

    public async Task BackupAsync(string destinationPath, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        Directory.CreateDirectory(Path.GetDirectoryName(destinationPath) ?? throw new InvalidOperationException("Backup path has no directory."));
        await using var source = await factory.OpenAsync(cancellationToken).ConfigureAwait(false);
        var builder = new SqliteConnectionStringBuilder { DataSource = destinationPath, Mode = SqliteOpenMode.ReadWriteCreate };
        await using var destination = new SqliteConnection(builder.ToString());
        await destination.OpenAsync(cancellationToken).ConfigureAwait(false);
        source.BackupDatabase(destination);
    }

    public async Task RunMaintenanceAsync(CancellationToken cancellationToken)
    {
        // Keep this outside a transaction: WAL checkpoint must not run while a
        // write transaction is active on the same database.
        await using var connection = await factory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = "PRAGMA wal_checkpoint(TRUNCATE); PRAGMA optimize;";
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task SetStateAsync(string key, string valueJson, CancellationToken cancellationToken)
    {
        await using var connection = await factory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = "INSERT INTO agent_state(key,value_json,updated_at_utc) VALUES($key,$value,$now) ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json,updated_at_utc=excluded.updated_at_utc;";
        Add(command, "$key", key); Add(command, "$value", valueJson); Add(command, "$now", UtcNow());
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<string?> GetStateAsync(string key, CancellationToken cancellationToken)
    {
        await using var connection = await factory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT value_json FROM agent_state WHERE key=$key;";
        Add(command, "$key", key);
        return await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) as string;
    }

    public async Task SaveConfigSnapshotAsync(long version, string configurationJson, string hash, string status, CancellationToken cancellationToken)
    {
        var normalizedStatus = NormalizeConfigStatus(status);
        if (normalizedStatus == "validated") ValidateSnapshot(configurationJson, hash);
        cancellationToken.ThrowIfCancellationRequested();
        await ConfigurationGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await using var connection = await factory.OpenAsync(cancellationToken).ConfigureAwait(false);
            await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
            var existing = await ReadConfigRowAsync(connection, transaction, version, cancellationToken).ConfigureAwait(false);
            if (existing is null)
            {
                await InsertConfigRowAsync(connection, transaction, version, configurationJson, hash, normalizedStatus, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                EnsureCompatibleSnapshot(existing, configurationJson, hash, normalizedStatus == "validated");
                var nextStatus = existing.Status;
                if (existing.Status is "active" or "superseded")
                {
                    nextStatus = existing.Status;
                }
                else if (existing.Status == "rejected" && normalizedStatus == "validated")
                {
                    var latestAccepted = await ReadLatestAcceptedVersionAsync(connection, transaction, cancellationToken).ConfigureAwait(false);
                    if (latestAccepted > version) throw new InvalidDataException($"Remote configuration version {version} is older than the accepted configuration version {latestAccepted}.");
                    nextStatus = "validated";
                }
                else if (existing.Status == "validated" && normalizedStatus == "rejected")
                {
                    nextStatus = "rejected";
                }

                if (!string.Equals(nextStatus, existing.Status, StringComparison.Ordinal))
                {
                    await ExecuteAsync(connection, transaction, "UPDATE config_snapshots SET status=$status WHERE config_version=$version AND status=$current;", cancellationToken, ("$status", nextStatus), ("$version", version), ("$current", existing.Status)).ConfigureAwait(false);
                }
            }

            await transaction.CommitAsync(CancellationToken.None).ConfigureAwait(false);
        }
        finally
        {
            ConfigurationGate.Release();
        }
    }

    public async Task<ConfigSnapshot> AcceptAndActivateConfigSnapshotAsync(long version, string configurationJson, string hash, CancellationToken cancellationToken)
    {
        if (version <= 0) throw new InvalidDataException("Remote config version must be positive.");
        ValidateSnapshot(configurationJson, hash);
        cancellationToken.ThrowIfCancellationRequested();
        await ConfigurationGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await using var connection = await factory.OpenAsync(cancellationToken).ConfigureAwait(false);
            await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
            var target = await ReadConfigRowAsync(connection, transaction, version, cancellationToken).ConfigureAwait(false);
            var latestAccepted = await ReadLatestAcceptedVersionAsync(connection, transaction, cancellationToken).ConfigureAwait(false);
            if (latestAccepted > version) throw new InvalidDataException($"Remote configuration version {version} is older than the accepted configuration version {latestAccepted}.");

            if (target is null)
            {
                await InsertConfigRowAsync(connection, transaction, version, configurationJson, hash, "validated", cancellationToken).ConfigureAwait(false);
                target = new(version, configurationJson, hash, "validated");
            }
            else
            {
                ValidateSnapshot(target.Json, target.Hash);
                EnsureCompatibleSnapshot(target, configurationJson, hash, true);
                if (target.Status != "active")
                {
                    await ExecuteAsync(connection, transaction, "UPDATE config_snapshots SET status='validated' WHERE config_version=$version;", cancellationToken, ("$version", version)).ConfigureAwait(false);
                    target = target with { Status = "validated" };
                }
            }

            var active = await ReadActiveConfigRowAsync(connection, transaction, cancellationToken).ConfigureAwait(false);
            if (active is not null && active.Version > version) throw new InvalidDataException($"Remote configuration version {version} is older than the active configuration version {active.Version}.");
            if (active is not null && active.Version == version && !SameSnapshot(active, target)) throw new InvalidDataException($"Remote configuration version {version} conflicts with the active snapshot.");

            if (target.Status != "active")
            {
                if (active is not null && active.Version != version)
                {
                    await ExecuteAsync(connection, transaction, "UPDATE config_snapshots SET status='superseded' WHERE status='active' AND config_version <> $version;", cancellationToken, ("$version", version)).ConfigureAwait(false);
                }
                var updated = await ExecuteAsync(connection, transaction, "UPDATE config_snapshots SET status='active',activated_at_utc=$now WHERE config_version=$version AND status='validated';", cancellationToken, ("$now", UtcNow()), ("$version", version)).ConfigureAwait(false);
                if (updated != 1) throw new InvalidDataException($"Remote configuration version {version} could not be activated.");
                target = target with { Status = "active" };
            }

            await transaction.CommitAsync(CancellationToken.None).ConfigureAwait(false);
            return new(target.Version, target.Json, target.Hash);
        }
        finally
        {
            ConfigurationGate.Release();
        }
    }

    public async Task ActivateConfigSnapshotAsync(long version, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        await ConfigurationGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await using var connection = await factory.OpenAsync(cancellationToken).ConfigureAwait(false);
            await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
            var target = await ReadConfigRowAsync(connection, transaction, version, cancellationToken).ConfigureAwait(false);
            if (target is null) throw new InvalidDataException($"Remote configuration version {version} does not exist.");
            ValidateSnapshot(target.Json, target.Hash);
            if (target.Status == "active")
            {
                await transaction.CommitAsync(CancellationToken.None).ConfigureAwait(false);
                return;
            }
            if (target.Status != "validated") throw new InvalidDataException($"Remote configuration version {version} is not validated.");

            var active = await ReadActiveConfigRowAsync(connection, transaction, cancellationToken).ConfigureAwait(false);
            var latestAccepted = await ReadLatestAcceptedVersionAsync(connection, transaction, cancellationToken).ConfigureAwait(false);
            if (latestAccepted > version || active is not null && active.Version > version) throw new InvalidDataException($"Remote configuration version {version} is older than the accepted configuration version {latestAccepted}.");
            if (active is not null && active.Version == version && !SameSnapshot(active, target)) throw new InvalidDataException($"Remote configuration version {version} conflicts with the active snapshot.");

            if (active is not null && active.Version != version)
            {
                await ExecuteAsync(connection, transaction, "UPDATE config_snapshots SET status='superseded' WHERE status='active' AND config_version <> $version;", cancellationToken, ("$version", version)).ConfigureAwait(false);
            }
            var updated = await ExecuteAsync(connection, transaction, "UPDATE config_snapshots SET status='active',activated_at_utc=$now WHERE config_version=$version AND status='validated';", cancellationToken, ("$now", UtcNow()), ("$version", version)).ConfigureAwait(false);
            if (updated != 1) throw new InvalidDataException($"Remote configuration version {version} could not be activated.");
            await transaction.CommitAsync(CancellationToken.None).ConfigureAwait(false);
        }
        finally
        {
            ConfigurationGate.Release();
        }
    }

    public async Task<ConfigSnapshot?> GetActiveConfigSnapshotAsync(CancellationToken cancellationToken)
    {
        await using var connection = await factory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT config_version,config_json,config_hash FROM config_snapshots WHERE status='active' ORDER BY config_version DESC LIMIT 1;";
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        return await reader.ReadAsync(cancellationToken).ConfigureAwait(false) ? new(reader.GetInt64(0), reader.GetString(1), reader.GetString(2)) : null;
    }

    private static async Task<StoredConfigRow?> ReadConfigRowAsync(SqliteConnection connection, SqliteTransaction transaction, long version, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "SELECT config_version,config_json,config_hash,status FROM config_snapshots WHERE config_version=$version;";
        Add(command, "$version", version);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        return await reader.ReadAsync(cancellationToken).ConfigureAwait(false)
            ? new(reader.GetInt64(0), reader.GetString(1), reader.GetString(2), reader.GetString(3))
            : null;
    }

    private static async Task<StoredConfigRow?> ReadActiveConfigRowAsync(SqliteConnection connection, SqliteTransaction transaction, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "SELECT config_version,config_json,config_hash,status FROM config_snapshots WHERE status='active' ORDER BY config_version DESC LIMIT 1;";
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        return await reader.ReadAsync(cancellationToken).ConfigureAwait(false)
            ? new(reader.GetInt64(0), reader.GetString(1), reader.GetString(2), reader.GetString(3))
            : null;
    }

    private static async Task<long> ReadLatestAcceptedVersionAsync(SqliteConnection connection, SqliteTransaction transaction, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "SELECT COALESCE(MAX(config_version),0) FROM config_snapshots WHERE status IN ('active','superseded');";
        return Convert.ToInt64(await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false), CultureInfo.InvariantCulture);
    }

    private static async Task InsertConfigRowAsync(SqliteConnection connection, SqliteTransaction transaction, long version, string json, string hash, string status, CancellationToken cancellationToken)
    {
        await ExecuteAsync(connection, transaction,
            "INSERT INTO config_snapshots(config_version,config_json,config_hash,received_at_utc,status) VALUES($version,$json,$hash,$now,$status);",
            cancellationToken, ("$version", version), ("$json", json), ("$hash", hash), ("$now", UtcNow()), ("$status", status)).ConfigureAwait(false);
    }

    private static void EnsureCompatibleSnapshot(StoredConfigRow existing, string json, string hash, bool requireEquivalentBody)
    {
        if (!string.Equals(existing.Hash, hash, StringComparison.Ordinal)) throw new InvalidDataException($"Remote configuration version {existing.Version} changed its hash.");
        if (requireEquivalentBody && !SameSnapshotBody(existing.Json, json)) throw new InvalidDataException($"Remote configuration version {existing.Version} changed its body.");
    }

    private static bool SameSnapshot(StoredConfigRow left, StoredConfigRow right) =>
        string.Equals(left.Hash, right.Hash, StringComparison.Ordinal) && SameSnapshotBody(left.Json, right.Json);

    private static bool SameSnapshotBody(string left, string right)
    {
        if (string.Equals(left, right, StringComparison.Ordinal)) return true;
        try
        {
            using var leftDocument = JsonDocument.Parse(left);
            using var rightDocument = JsonDocument.Parse(right);
            return string.Equals(PayloadHasher.Compute(leftDocument.RootElement), PayloadHasher.Compute(rightDocument.RootElement), StringComparison.Ordinal);
        }
        catch (JsonException)
        {
            return false;
        }
    }

    private static void ValidateSnapshot(string json, string hash)
    {
        if (string.IsNullOrWhiteSpace(json) || string.IsNullOrWhiteSpace(hash)) throw new InvalidDataException("Remote configuration body and hash are required.");
        try
        {
            using var document = JsonDocument.Parse(json);
            JsonAmbiguityGuard.EnsureUnambiguous(document.RootElement);
            if (!PayloadHasher.Matches(document.RootElement, hash)) throw new InvalidDataException("Remote configuration hash mismatch.");
        }
        catch (JsonException ex)
        {
            throw new InvalidDataException("Remote configuration body is not valid JSON.", ex);
        }
    }

    private static string NormalizeConfigStatus(string status) => status switch
    {
        "validated" => status,
        "rejected" => status,
        _ => throw new ArgumentException("Remote configuration status is invalid.", nameof(status))
    };

    private sealed record StoredConfigRow(long Version, string Json, string Hash, string Status);

    public async Task CleanupAsync(DateTimeOffset completedBeforeUtc, DateTimeOffset batchesBeforeUtc, CancellationToken cancellationToken)
    {
        await using var connection = await factory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        // A command referenced by a non-terminal etl_jobs row keeps its inbox/attempts/result/outbox
        // rows regardless of age, so a re-delivered commandId always has the original hash and result
        // to check against. The job's own frozen evidence is independent of this guard.
        await ExecuteAsync(connection, transaction,
            "DELETE FROM command_attempts WHERE command_id IN (SELECT c.command_id FROM commands_inbox c WHERE c.status='completed' AND c.erp_acknowledged_at_utc < $before AND (c.result_status IS NULL OR c.result_status <> 'dead_letter') AND NOT EXISTS (SELECT 1 FROM results_outbox o WHERE o.command_id=c.command_id AND o.status <> 'acknowledged') AND NOT EXISTS (SELECT 1 FROM etl_jobs j WHERE j.command_id=c.command_id AND j.status NOT IN ('finished','cancelled'))); DELETE FROM results_outbox WHERE status='acknowledged' AND acknowledged_at_utc < $before AND NOT EXISTS (SELECT 1 FROM commands_inbox c WHERE c.command_id=results_outbox.command_id AND c.result_status='dead_letter') AND NOT EXISTS (SELECT 1 FROM etl_jobs j WHERE j.command_id=results_outbox.command_id AND j.status NOT IN ('finished','cancelled')); DELETE FROM commands_inbox WHERE status='completed' AND erp_acknowledged_at_utc < $before AND (result_status IS NULL OR result_status <> 'dead_letter') AND NOT EXISTS (SELECT 1 FROM results_outbox o WHERE o.command_id=commands_inbox.command_id AND o.status <> 'acknowledged') AND NOT EXISTS (SELECT 1 FROM etl_jobs j WHERE j.command_id=commands_inbox.command_id AND j.status NOT IN ('finished','cancelled'));",
            cancellationToken, ("$before", completedBeforeUtc.ToUniversalTime().ToString("O"))).ConfigureAwait(false);
        // Independent parent guard on the purge itself: 'deleted' batch rows (including legacy or
        // manually marked ones) of an unresolved run are evidence and survive cleanup; only rows
        // whose parent run succeeded are purged.
        await ExecuteAsync(connection, transaction, "DELETE FROM etl_batches WHERE status='deleted' AND acknowledged_at_utc < $before AND EXISTS (SELECT 1 FROM etl_runs r WHERE r.run_id=etl_batches.run_id AND r.status='succeeded');", cancellationToken, ("$before", batchesBeforeUtc.ToUniversalTime().ToString("O"))).ConfigureAwait(false);
        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
    }

    private static async Task<int> ExecuteAsync(SqliteConnection connection, SqliteTransaction transaction, string sql, CancellationToken cancellationToken, params (string Name, object? Value)[] parameters)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = sql;
        foreach (var parameter in parameters) Add(command, parameter.Name, parameter.Value);
        return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private static void Add(SqliteCommand command, string name, object? value) => command.Parameters.AddWithValue(name, value ?? DBNull.Value);
    private static string UtcNow() => DateTimeOffset.UtcNow.ToString("O");
    private static string? NullableString(SqliteDataReader reader, int ordinal) => reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);
    private static Guid? ParseGuid(string? value) => Guid.TryParse(value, out var parsed) ? parsed : null;
    private static DateTimeOffset ParseDate(string value) => DateTimeOffset.Parse(value, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind).ToUniversalTime();
    private static DateTimeOffset? ParseNullableDate(string? value) => value is null ? null : ParseDate(value);
    private static string? SerializeCursor(EtlCursor? cursor) => cursor is null ? null : JsonSerializer.Serialize(cursor, JsonOptions);
    private static EtlCursor? DeserializeCursor(string? json) => json is null ? null : JsonSerializer.Deserialize<EtlCursor>(json, JsonOptions);
    private static CommandStatus ParseCommandStatus(string status) => status switch
    {
        "queued" => CommandStatus.Queued,
        "retry_waiting" => CommandStatus.RetryWaiting,
        "unknown_result" => CommandStatus.UnknownResult,
        _ => Enum.Parse<CommandStatus>(status.Replace("_", string.Empty, StringComparison.Ordinal), true)
    };
    private static EtlBatchStatus ParseBatchStatus(string status) => status switch
    {
        "retry_waiting" => EtlBatchStatus.RetryWaiting,
        "dead_letter" => EtlBatchStatus.DeadLetter,
        _ => Enum.Parse<EtlBatchStatus>(status, true)
    };
    private static string ToDb<TEnum>(TEnum status) where TEnum : struct, Enum =>
        string.Concat(status.ToString().Select((c, index) => char.IsUpper(c) && index > 0 ? "_" + char.ToLowerInvariant(c) : char.ToLowerInvariant(c).ToString()));
}
