using System.Text.Json;
using ErpOnecAgent.Application.Abstractions;
using Microsoft.Data.Sqlite;

namespace ErpOnecAgent.Infrastructure.Persistence.Sqlite;

// Retention and revocation of sensitive entities (agent-bridge to-erp/0031, to-onec/0030):
// a frozen definition with deleteBatchAfterAck keeps only batch metadata once ERP holds the
// data, and a revoked entity (dropped from the configuration) leaves no file and no run that
// could still read or send it.
public sealed partial class SqliteAgentStore
{
    private const string EntityRevoked = "ENTITY_REVOKED";

    // Evaluated against etl_run_entities aliased 're': the run's frozen definition decides.
    private const string DeleteBatchAfterAckFlag = "COALESCE(json_extract(re.entity_definition_json,'$.deleteBatchAfterAck'),0)";

    /// <inheritdoc cref="IAgentStore.GetSensitiveBatchFilesAsync"/>
    public async Task<IReadOnlyList<EtlSensitiveBatchFile>> GetSensitiveBatchFilesAsync(CancellationToken cancellationToken)
    {
        var files = new List<EtlSensitiveBatchFile>();
        await using var connection = await factory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = $"""
            SELECT b.batch_id, b.run_id, b.entity_name, b.file_path, b.status
            FROM etl_batches b
            JOIN etl_run_entities re ON re.run_id=b.run_id AND re.entity_name=b.entity_name
            WHERE b.status <> 'deleted' AND {DeleteBatchAfterAckFlag} = 1
            ORDER BY b.created_at_utc, b.batch_id;
            """;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            files.Add(new EtlSensitiveBatchFile(Guid.Parse(reader.GetString(0)), Guid.Parse(reader.GetString(1)), reader.GetString(2), reader.GetString(3), reader.GetString(4)));
        return files;
    }

    /// <inheritdoc cref="IAgentStore.BlockRunsWithRevokedEntitiesAsync"/>
    public async Task<IReadOnlyList<EtlRevokedRun>> BlockRunsWithRevokedEntitiesAsync(IReadOnlySet<string> activeEntityCodes, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(activeEntityCodes);
        await using var connection = await factory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);

        // The frozen entity list of an unfinished run: resolved at scheduling, or frozen at
        // manual job acceptance. A later configuration never changes it — hence this check.
        var unfinished = new List<(Guid RunId, string EntitiesJson)>();
        await using (var select = connection.CreateCommand())
        {
            select.Transaction = transaction;
            select.CommandText = """
                SELECT r.run_id, COALESCE(r.resolved_entities_json, (SELECT j.entities_json FROM etl_jobs j WHERE j.run_id=r.run_id))
                FROM etl_runs r
                WHERE r.status IN ('pending','running','paused','uploading','completing');
                """;
            await using var reader = await select.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                if (!reader.IsDBNull(1)) unfinished.Add((Guid.Parse(reader.GetString(0)), reader.GetString(1)));
        }

        var blocked = new List<EtlRevokedRun>();
        var now = UtcNow();
        foreach (var (runId, entitiesJson) in unfinished)
        {
            var revoked = RevokedSensitiveEntities(entitiesJson, activeEntityCodes);
            if (revoked.Count == 0) continue;
            var message = $"{EntityRevoked}: the sensitive entity {string.Join(", ", revoked)} was revoked by the configuration; its batch files are deleted. Resolve the run (R1); the other entities are re-read by the next run.";
            // No batch is singled out (Guid.Empty): every pending batch of the run is fenced RUN_BLOCKED.
            await CommitUploadBlockAsync(connection, transaction, runId, Guid.Empty, EntityRevoked, QuarantineRunBlocked, message, now, cancellationToken).ConfigureAwait(false);
            blocked.AddRange(revoked.Select(entity => new EtlRevokedRun(runId, entity)));
        }
        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        return blocked;
    }

    /// <inheritdoc cref="IAgentStore.GetDeadLetterBatchFilesAsync"/>
    public async Task<IReadOnlyList<EtlDeadLetterBatchFile>> GetDeadLetterBatchFilesAsync(CancellationToken cancellationToken)
    {
        var files = new List<EtlDeadLetterBatchFile>();
        await using var connection = await factory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = $"""
            SELECT b.batch_id, b.run_id, b.entity_name, b.file_path, b.created_at_utc,
                   CASE WHEN r.resolved_at_utc IS NOT NULL OR r.status IN ('failed','cancelled','succeeded','partial_success') THEN 1 ELSE 0 END,
                   COALESCE((SELECT {DeleteBatchAfterAckFlag} FROM etl_run_entities re WHERE re.run_id=b.run_id AND re.entity_name=b.entity_name), 0)
            FROM etl_batches b JOIN etl_runs r ON r.run_id=b.run_id
            WHERE b.status='dead_letter'
            ORDER BY b.created_at_utc, b.batch_id;
            """;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            files.Add(new EtlDeadLetterBatchFile(Guid.Parse(reader.GetString(0)), Guid.Parse(reader.GetString(1)), reader.GetString(2), reader.GetString(3),
                ParseDate(reader.GetString(4)), reader.GetInt64(5) == 1, reader.GetInt64(6) == 1));
        return files;
    }

    /// <inheritdoc cref="IAgentStore.GetSensitiveEntityCodesAsync"/>
    public async Task<IReadOnlySet<string>> GetSensitiveEntityCodesAsync(CancellationToken cancellationToken)
    {
        var codes = new HashSet<string>(StringComparer.Ordinal);
        await using var connection = await factory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = $"SELECT DISTINCT re.entity_name FROM etl_run_entities re WHERE {DeleteBatchAfterAckFlag} = 1;";
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false)) codes.Add(reader.GetString(0));
        return codes;
    }

    private static List<string> RevokedSensitiveEntities(string entitiesJson, IReadOnlySet<string> activeEntityCodes)
    {
        var revoked = new List<string>();
        using var document = JsonDocument.Parse(entitiesJson);
        if (document.RootElement.ValueKind != JsonValueKind.Array) return revoked;
        foreach (var entity in document.RootElement.EnumerateArray())
        {
            if (entity.ValueKind != JsonValueKind.Object) continue;
            var sensitive = entity.TryGetProperty("deleteBatchAfterAck", out var flag) && flag.ValueKind == JsonValueKind.True;
            if (sensitive && entity.TryGetProperty("entityCode", out var code) && code.GetString() is { } name && !activeEntityCodes.Contains(name))
                revoked.Add(name);
        }
        return revoked;
    }
}
