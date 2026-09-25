using ErpOnecAgent.Application.Abstractions;
using Microsoft.Data.Sqlite;

namespace ErpOnecAgent.Infrastructure.Persistence.Sqlite;

// D1 DARK storage slice: attested watermark domain reset (migration 011). The reset is the
// explicit exit from DOMAIN_CHANGED/DOMAIN_UNKNOWN: the row is archived verbatim and removed
// in one transaction, under a generation CAS, and only while no active run owns the entity.
// A run that captured the old base before the reset fails its finalize CAS (the row it
// expects is gone), so a stale run can never commit into the new domain.
public sealed partial class SqliteAgentStore
{
    /// <inheritdoc cref="IAgentStore.ResetEtlWatermarkDomainAsync"/>
    public async Task<EtlWatermarkDomainResetOutcome> ResetEtlWatermarkDomainAsync(EtlWatermarkDomainResetRequest request, DateTimeOffset nowUtc, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (string.IsNullOrWhiteSpace(request.EntityName)) throw new ArgumentException("An entity name is required.", nameof(request));
        if (string.IsNullOrWhiteSpace(request.OperatorId)) throw new ArgumentException("An operator identity is required.", nameof(request));
        if (string.IsNullOrWhiteSpace(request.Reason)) throw new ArgumentException("A reset reason is required.", nameof(request));
        ArgumentOutOfRangeException.ThrowIfLessThan(request.ExpectedGeneration, 1);
        var now = nowUtc.ToUniversalTime().ToString("O");

        await using var connection = await factory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);

        long? generation = null;
        string? cursor = null, extracting = null, fingerprint = null, lastRun = null, updatedAt = null;
        await using (var read = connection.CreateCommand())
        {
            read.Transaction = transaction;
            read.CommandText = "SELECT generation,committed_cursor_json,domain_fingerprint,last_run_id,updated_at_utc,extracting_cursor_json FROM watermarks WHERE entity_name=$entity;";
            Add(read, "$entity", request.EntityName);
            await using var reader = await read.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                generation = reader.GetInt64(0);
                cursor = NullableString(reader, 1);
                fingerprint = NullableString(reader, 2);
                lastRun = NullableString(reader, 3);
                updatedAt = NullableString(reader, 4);
                extracting = NullableString(reader, 5);
            }
        }

        EtlWatermarkDomainResetRefusal? refusal = generation is null ? EtlWatermarkDomainResetRefusal.WatermarkMissing
            : generation != request.ExpectedGeneration ? EtlWatermarkDomainResetRefusal.GenerationMismatch
            : await ScalarLongAsync(connection, transaction,
                "SELECT COUNT(*) FROM etl_entity_ownership WHERE entity_name=$entity AND released_at_utc IS NULL;",
                cancellationToken, ("$entity", request.EntityName)).ConfigureAwait(false) != 0 ? EtlWatermarkDomainResetRefusal.EntityOwned
            : null;
        if (refusal is not null)
        {
            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
            return new EtlWatermarkDomainResetOutcome.Refused(refusal.Value);
        }

        var resetId = Guid.NewGuid();
        var archived = await ExecuteAsync(connection, transaction, """
            INSERT INTO watermark_domain_resets(reset_id,entity_name,reset_at_utc,operator_id,reason,prior_committed_cursor_json,prior_extracting_cursor_json,prior_generation,prior_domain_fingerprint,prior_last_run_id,prior_updated_at_utc)
            VALUES($id,$entity,$now,$operator,$reason,$cursor,$extracting,$generation,$fp,$lastRun,$updated);
            """, cancellationToken,
            ("$id", resetId.ToString("D")), ("$entity", request.EntityName), ("$now", now), ("$operator", request.OperatorId), ("$reason", request.Reason),
            ("$cursor", cursor), ("$extracting", extracting), ("$generation", generation), ("$fp", fingerprint), ("$lastRun", lastRun), ("$updated", updatedAt)).ConfigureAwait(false);
        if (archived != 1) throw new InvalidOperationException($"Domain reset archive for '{request.EntityName}' was not written.");
        var removed = await ExecuteAsync(connection, transaction,
            "DELETE FROM watermarks WHERE entity_name=$entity AND generation=$generation;",
            cancellationToken, ("$entity", request.EntityName), ("$generation", generation)).ConfigureAwait(false);
        if (removed != 1) throw new InvalidOperationException($"Watermark '{request.EntityName}' changed mid-reset.");

        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        return new EtlWatermarkDomainResetOutcome.Reset(new EtlWatermarkDomainReset(resetId, request.EntityName, ParseDate(now), generation!.Value, cursor, fingerprint));
    }
}
