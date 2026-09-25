using System.Text.Json;
using ErpOnecAgent.Application.Abstractions;
using Microsoft.Data.Sqlite;

namespace ErpOnecAgent.Infrastructure.Persistence.Sqlite;

// O3 DARK storage slice: scheduled ETL runs (migration 009). A scheduled tick creates a
// pending jobless run carrying its schedule_key and frozen identity (the ordered
// manifest, the full resolved definitions and the configuration version); the partial
// unique index keeps at most one active or unresolved run per key, so a failed or
// blocked run never mints a successor. The claim reuses the O1 claim core verbatim —
// elder quarantine, the shared (created_at_utc, run_id) overlap priority with manual
// jobs, all-entity ownership with immutable bindings, and the fresh extraction claim —
// with owner_job_id NULL. Nothing here is called by OnecEtlWorker or recovery; the
// legacy tick (CreateEtlRunAsync) remains a §9 bypass until cutover.
public sealed partial class SqliteAgentStore
{
    // Statuses that hold a schedule key — the exact predicate of ux_etl_runs_schedule_active:
    // everything except succeeded, cancelled and an explicitly resolved failed/blocked run.
    private const string ScheduleKeyHeldPredicate = """
        (status NOT IN ('succeeded','cancelled')
         AND NOT (status IN ('failed','blocked') AND resolved_at_utc IS NOT NULL))
        """;

    /// <inheritdoc cref="IAgentStore.EnsureScheduledEtlRunAsync"/>
    public async Task<EtlScheduledRunEnsureOutcome> EnsureScheduledEtlRunAsync(EtlScheduledRunRequest request, DateTimeOffset nowUtc, CancellationToken cancellationToken)
    {
        ValidateScheduledRunRequest(request);
        var now = nowUtc.ToUniversalTime().ToString("O");
        var resolvedJson = JsonSerializer.Serialize(request.Entities, JsonOptions);
        var codesJson = JsonSerializer.Serialize(request.Entities.Select(static entity => entity.EntityCode).ToArray(), JsonOptions);

        try
        {
            await using var connection = await factory.OpenAsync(cancellationToken).ConfigureAwait(false);
            // The transaction takes the write lock up front, so concurrent ticks for the same
            // key serialize: the loser observes the winner's committed run. The unique index
            // is the storage-level backstop.
            await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
            var existing = await ReadScheduleHolderAsync(connection, transaction, request.ScheduleKey, cancellationToken).ConfigureAwait(false);
            if (existing is not null)
            {
                await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
                return existing;
            }

            var runId = Guid.NewGuid();
            await ExecuteAsync(connection, transaction, """
                INSERT INTO etl_runs(run_id,mode,requested_entities_json,status,started_at_utc,configuration_version,created_at_utc,updated_at_utc,row_version,schedule_key,resolved_entities_json)
                VALUES($run,$mode,$codes,'pending',NULL,$config,$now,$now,1,$key,$resolved);
                """, cancellationToken,
                ("$run", runId.ToString("D")), ("$mode", request.Mode), ("$codes", codesJson), ("$config", request.ConfigurationVersion),
                ("$now", now), ("$key", request.ScheduleKey), ("$resolved", resolvedJson)).ConfigureAwait(false);
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            return new EtlScheduledRunEnsureOutcome.Created(runId);
        }
        catch (SqliteException ex) when (ex.SqliteErrorCode == 19 && ex.Message.Contains("schedule_key", StringComparison.Ordinal))
        {
            // Unique-index backstop: another writer committed a holder first — report it.
            await using var connection = await factory.OpenAsync(cancellationToken).ConfigureAwait(false);
            await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
            var holder = await ReadScheduleHolderAsync(connection, transaction, request.ScheduleKey, cancellationToken).ConfigureAwait(false);
            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
            return holder ?? throw new InvalidOperationException($"Schedule key '{request.ScheduleKey}' conflicted but no holder run is visible.", ex);
        }
    }

    /// <inheritdoc cref="IAgentStore.TryClaimScheduledRunAsync"/>
    public async Task<EtlScheduledRunClaimOutcome> TryClaimScheduledRunAsync(Guid runId, string ownerId, DateTimeOffset nowUtc, CancellationToken cancellationToken)
    {
        ValidateOwner(ownerId);
        var now = nowUtc.ToUniversalTime().ToString("O");

        await using var connection = await factory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);

        // Probe-guard write (as in the job claim): serializes claimants on the run row; only
        // a pending scheduled run WITHOUT a job row enters the claim. Everything else is
        // NotClaimable and rolls back. Paths that do not proceed also roll this bump back.
        var probe = await ExecuteAsync(connection, transaction, """
            UPDATE etl_runs AS r SET row_version=row_version+1, updated_at_utc=$now
            WHERE r.run_id=$run AND r.status='pending' AND r.schedule_key IS NOT NULL
              AND NOT EXISTS (SELECT 1 FROM etl_jobs j WHERE j.run_id=r.run_id);
            """, cancellationToken, ("$now", now), ("$run", runId.ToString("D"))).ConfigureAwait(false);
        if (probe != 1)
        {
            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
            return new EtlScheduledRunClaimOutcome.NotClaimable();
        }

        var run = await ReadClaimRunRowAsync(connection, transaction, runId, cancellationToken).ConfigureAwait(false)
            ?? throw new InvalidOperationException($"ETL run {runId:D} vanished mid-transaction.");
        var manifest = ParseManifestOrdered(run.RequestedEntitiesJson);
        if (manifest is null || !ScheduledIdentityConsistent(run.Mode, run.ConfigurationVersion, run.ResolvedEntitiesJson, run.RequestedEntitiesJson))
        {
            // Corrupt frozen identity never dispatches. A provably never-started run is
            // blocked (its reservation is released because nothing ever ran); with unproven
            // durable effects it keeps its pending admission hold and nothing is written.
            const string inconsistentMessage = "Scheduled run frozen identity is inconsistent (manifest, resolved definitions, mode or configuration version); corrupt evidence never dispatches.";
            if (!await RunProvablyInertAsync(connection, transaction, runId, cancellationToken).ConfigureAwait(false))
            {
                await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
                return new EtlScheduledRunClaimOutcome.Blocked("SCHEDULED_RUN_INCONSISTENT", inconsistentMessage);
            }
            var (_, runsBlocked) = await CommitJobManifestBlockAsync(connection, transaction, null, runId, "SCHEDULED_RUN_INCONSISTENT", inconsistentMessage, now, cancellationToken).ConfigureAwait(false);
            if (runsBlocked != 1) throw new InvalidOperationException($"Scheduled run {runId:D} failed its quarantine transition mid-transaction.");
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            return new EtlScheduledRunClaimOutcome.Blocked("SCHEDULED_RUN_INCONSISTENT", inconsistentMessage);
        }

        var core = await ClaimPendingRunCoreAsync(connection, transaction, run, manifest, null, ownerId, now, cancellationToken).ConfigureAwait(false);
        switch (core)
        {
            case ClaimCoreResult.Acquired acquired:
                await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
                return new EtlScheduledRunClaimOutcome.Claimed(new EtlScheduledRunClaim(
                    runId, acquired.ExtractionClaimId, run.ScheduleKey!, run.Mode, run.ResolvedEntitiesJson!, run.ConfigurationVersion!.Value));
            case ClaimCoreResult.ElderHold hold:
                return await FinishScheduledDeferralAsync(transaction, hold.Quarantined, EtlJobDeferralReason.ElderManifestInvalid, cancellationToken).ConfigureAwait(false);
            case ClaimCoreResult.Overlap overlap:
                return await FinishScheduledDeferralAsync(transaction, overlap.Quarantined, EtlJobDeferralReason.QueuedOverlap, cancellationToken).ConfigureAwait(false);
            case ClaimCoreResult.Busy busy:
                return await FinishScheduledDeferralAsync(transaction, busy.Quarantined, EtlJobDeferralReason.BusyEntity, cancellationToken).ConfigureAwait(false);
            default:
                throw new InvalidOperationException("Unknown claim core result.");
        }
    }

    /// <inheritdoc cref="IAgentStore.GetDueScheduledRunsAsync"/>
    public async Task<IReadOnlyList<EtlDueScheduledRun>> GetDueScheduledRunsAsync(int limit, DateTimeOffset nowUtc, CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(limit, 1);
        var runs = new List<EtlDueScheduledRun>();
        await using var connection = await factory.OpenAsync(cancellationToken).ConfigureAwait(false);
        // The same typed frozen-identity check the claim applies, as a deterministic scalar
        // on THIS connection, so enumeration can never disagree with the claim.
        connection.CreateFunction<string?, string?, long?, string?, long>(
            "etl_scheduled_identity_consistent",
            static (mode, resolved, config, manifest) => ScheduledIdentityConsistent(mode, config, resolved, manifest) ? 1L : 0L,
            isDeterministic: true);

        // Eligibility BEFORE LIMIT, ranked by run (created_at_utc, run_id): frozen identity,
        // no job row, no older overlapping pending run (the reservation shared with manual
        // jobs), and every manifest entity acquirable.
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT r.run_id, r.schedule_key, r.mode, r.configuration_version
            FROM etl_runs r
            WHERE r.status='pending' AND r.schedule_key IS NOT NULL
              AND NOT EXISTS (SELECT 1 FROM etl_jobs j WHERE j.run_id = r.run_id)
              AND etl_scheduled_identity_consistent(r.mode, r.resolved_entities_json, r.configuration_version, r.requested_entities_json) = 1
              AND NOT EXISTS (SELECT 1 FROM etl_runs r2
                              WHERE r2.status='pending' AND r2.run_id <> r.run_id
                                AND (COALESCE(r2.created_at_utc,'') < COALESCE(r.created_at_utc,'') OR (COALESCE(r2.created_at_utc,'') = COALESCE(r.created_at_utc,'') AND r2.run_id < r.run_id))
                                AND json_valid(r2.requested_entities_json)
                                AND EXISTS (SELECT 1 FROM json_each(CASE WHEN json_valid(r2.requested_entities_json) THEN r2.requested_entities_json ELSE '[]' END) e2
                                            JOIN json_each(CASE WHEN json_valid(r.requested_entities_json) THEN r.requested_entities_json ELSE '[]' END) e1 ON e1.value = e2.value))
              AND NOT EXISTS (SELECT 1 FROM json_each(CASE WHEN json_valid(r.requested_entities_json) THEN r.requested_entities_json ELSE '[]' END) e
                              JOIN etl_entity_ownership o ON o.entity_name = e.value AND o.released_at_utc IS NULL)
            ORDER BY COALESCE(r.created_at_utc,''), r.run_id
            LIMIT $limit;
            """;
        Add(command, "$limit", limit);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            runs.Add(new EtlDueScheduledRun(Guid.Parse(reader.GetString(0)), reader.GetString(1), reader.GetString(2), reader.GetInt64(3)));
        }
        return runs;
    }

    // ---------- O3 internals ----------

    // A deferral of a scheduled run records no deferral state (there is no job row to hold
    // it; the pending run keeps its reservation). Without elder quarantines the whole
    // transaction, including the claimant's probe bump, rolls back. Elder quarantines from
    // the same pass are durable evidence and must commit; the claimant's row_version /
    // updated_at_utc probe bump then commits with them (diagnostic only, no state change).
    private static async Task<EtlScheduledRunClaimOutcome> FinishScheduledDeferralAsync(SqliteTransaction transaction, int quarantined, EtlJobDeferralReason reason, CancellationToken cancellationToken)
    {
        if (quarantined > 0) await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        else await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
        return new EtlScheduledRunClaimOutcome.Deferred(reason);
    }

    private static async Task<EtlScheduledRunEnsureOutcome.ActiveExisting?> ReadScheduleHolderAsync(SqliteConnection connection, SqliteTransaction transaction, string scheduleKey, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = $"SELECT run_id,status FROM etl_runs WHERE schedule_key=$key AND {ScheduleKeyHeldPredicate} ORDER BY COALESCE(created_at_utc,''), run_id LIMIT 1;";
        Add(command, "$key", scheduleKey);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        return await reader.ReadAsync(cancellationToken).ConfigureAwait(false)
            ? new EtlScheduledRunEnsureOutcome.ActiveExisting(Guid.Parse(reader.GetString(0)), reader.GetString(1))
            : null;
    }

    // The frozen identity is written verbatim into the run, so it is validated completely
    // up front with the same structural rules as a durable job's definitions.
    private static void ValidateScheduledRunRequest(EtlScheduledRunRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (string.IsNullOrWhiteSpace(request.ScheduleKey) || request.ScheduleKey.Trim().Length != request.ScheduleKey.Length)
            throw new ArgumentException("A schedule key is required and must not have leading or trailing whitespace.", nameof(request));
        // Scheduled work is incremental only (design §4.4); full/reload runs are manual jobs.
        if (request.Mode != ScheduledRunMode)
            throw new ArgumentException($"Unsupported scheduled ETL mode '{request.Mode}'; scheduled runs are '{ScheduledRunMode}' only.", nameof(request));
        if (request.Entities is null || request.Entities.Count == 0)
            throw new ArgumentException("A scheduled ETL run requires at least one resolved entity definition.", nameof(request));
        if (request.Entities.Any(static entity => entity is null))
            throw new ArgumentException("Resolved entity definitions cannot contain null.", nameof(request));
        if (request.Entities.Select(static entity => entity.EntityCode).Distinct(StringComparer.Ordinal).Count() != request.Entities.Count)
            throw new ArgumentException("Resolved entity codes must be unique.", nameof(request));
        if (request.ConfigurationVersion < 0)
            throw new ArgumentOutOfRangeException(nameof(request), "Configuration version cannot be negative.");
        foreach (var entity in request.Entities)
        {
            if (!IsValidResolvedEntityDefinition(entity))
                throw new ArgumentException($"Resolved entity definition '{entity.EntityCode}' is invalid.", nameof(request));
        }
    }
}
