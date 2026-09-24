using System.Text;
using System.Text.Json;
using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Domain.Common;
using ErpOnecAgent.Domain.Etl;
using Microsoft.Data.Sqlite;

namespace ErpOnecAgent.Infrastructure.Persistence.Sqlite;

public sealed partial class SqliteAgentStore
{
    /// <summary>
    /// Atomic durable manual-ETL-job acceptance under the CURRENT persisted execution owner. The
    /// transaction's first statement is the guarded command UPDATE, so concurrent accept calls on
    /// the same command serialize on the write lock before either side observes the outcome: exactly
    /// one caller applies; the other sees the committed job and replays its identity. The guard is
    /// the never-sent acceptance state — <c>queued</c>/<c>retry_waiting</c>, the exact claim owner,
    /// zero attempt/post/lookup counters, NULL <c>first_sent_at_utc</c>, no attempt rows, no existing
    /// job — evaluated and committed in the same transaction as the run/job/result/outbox writes, so
    /// any failure at any stage rolls all four tables back together. The guard additionally enforces,
    /// against the CURRENT saved row inside the transaction: the canonical saved
    /// <c>command_type → mode</c> mapping (<c>start_full_sync → bootstrap_full</c>,
    /// <c>reload_entity → entity_reload</c>; every other type refuses), the resolved selection's
    /// correspondence to the saved payload (<c>reload_entity</c> requires exactly one resolved entity
    /// matching <c>payload.entity</c>; an explicit non-empty <c>start_full_sync</c>
    /// <c>payload.entities</c> list must equal the resolved selection — an omitted/empty list means
    /// the caller-resolved enabled set), and that the never-sent command is not past
    /// <c>expires_at_utc</c>. The payload hash stored on the job is read from the canonical saved
    /// inbox row, never from caller input.
    /// </summary>
    public async Task<EtlJobAcceptanceOutcome> AcceptEtlJobAndCompleteCommandAsync(Guid commandId, string claimOwner, EtlJobAcceptanceRequest request, CancellationToken cancellationToken)
    {
        ValidateOwner(claimOwner);
        ValidateAcceptanceRequest(request);

        var jobId = Guid.NewGuid();
        var runId = Guid.NewGuid();
        var resultJson = BuildAcceptanceResultJson(commandId, runId, request.Mode);
        var entitiesJson = JsonSerializer.Serialize(request.Entities, JsonOptions);
        var entityCodesJson = JsonSerializer.Serialize(request.Entities.Select(static entity => entity.EntityCode).ToArray(), JsonOptions);
        var resultHash = PayloadHasher.ComputeBytes(Encoding.UTF8.GetBytes(resultJson));

        await using var connection = await factory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        var now = UtcNow();

        await using var update = connection.CreateCommand();
        update.Transaction = transaction;
        update.CommandText = $"""
            UPDATE commands_inbox
            SET status='result_pending',result_status='succeeded_local',result_json=$result,external_ref=NULL,external_number=NULL,finished_at_utc=$now,next_attempt_at_utc=NULL,exec_claim_owner_id=NULL,exec_claim_acquired_at_utc=NULL,row_version=row_version+1
            WHERE command_id=$id
              AND status IN ('queued','retry_waiting')
              AND exec_claim_owner_id=$owner
              AND command_type=$canonicalType
              AND (expires_at_utc IS NULL OR expires_at_utc > $now)
              AND attempt_count=0 AND post_attempt_count=0 AND lookup_attempt_count=0
              AND first_sent_at_utc IS NULL
              AND result_status IS NULL AND result_json IS NULL
              AND NOT EXISTS (SELECT 1 FROM command_attempts ca WHERE ca.command_id=commands_inbox.command_id)
              AND NOT EXISTS (SELECT 1 FROM etl_jobs j WHERE j.command_id=commands_inbox.command_id)
              AND ({EntitySelectionGuard(request.Mode, "commands_inbox")});
            """;
        Add(update, "$id", commandId.ToString("D")); Add(update, "$owner", claimOwner); Add(update, "$result", resultJson); Add(update, "$now", now);
        Add(update, "$canonicalType", CanonicalCommandType(request.Mode));
        Add(update, "$resolvedCodes", entityCodesJson);
        Add(update, "$payloadEntity", request.Entities[0].EntityCode);
        var changed = await update.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        if (changed == 0)
        {
            var outcome = await ClassifyNonAcceptanceAsync(connection, transaction, commandId, claimOwner, request, entityCodesJson, now, cancellationToken).ConfigureAwait(false);
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            return outcome;
        }

        await ExecuteAsync(connection, transaction, """
            INSERT INTO etl_runs(run_id,mode,requested_entities_json,status,started_at_utc,configuration_version,created_at_utc,updated_at_utc,row_version)
            VALUES($run,$mode,$entities,'pending',NULL,$configVersion,$now,$now,1);
            """, cancellationToken,
            ("$run", runId.ToString("D")), ("$mode", request.Mode), ("$entities", entityCodesJson), ("$configVersion", request.ConfigurationVersion), ("$now", now)).ConfigureAwait(false);

        await ExecuteAsync(connection, transaction, """
            INSERT INTO etl_jobs(job_id,command_id,run_id,mode,entities_json,configuration_version,status,command_payload_hash,acceptance_result_json,created_at_utc,updated_at_utc,row_version)
            SELECT $job,$id,$run,$mode,$entities,$configVersion,'pending',c.payload_hash,$result,$now,$now,1
            FROM commands_inbox c WHERE c.command_id=$id;
            """, cancellationToken,
            ("$job", jobId.ToString("D")), ("$id", commandId.ToString("D")), ("$run", runId.ToString("D")), ("$mode", request.Mode), ("$entities", entitiesJson),
            ("$configVersion", request.ConfigurationVersion), ("$result", resultJson), ("$now", now)).ConfigureAwait(false);

        await ExecuteAsync(connection, transaction,
            "INSERT INTO results_outbox(result_id,command_id,payload_json,payload_hash,status,next_attempt_at_utc,created_at_utc) VALUES($resultId,$id,$result,$resultHash,'pending',$now,$now);",
            cancellationToken,
            ("$resultId", Guid.NewGuid().ToString("D")), ("$id", commandId.ToString("D")), ("$result", resultJson), ("$resultHash", resultHash), ("$now", now)).ConfigureAwait(false);

        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        return new EtlJobAcceptanceOutcome.Applied(new EtlAcceptedJob(jobId, runId, request.Mode, resultJson));
    }

    // Reached only when the guarded UPDATE changed zero rows: the row left the never-sent acceptance
    // state, the owner does not match, the saved type/selection does not correspond to the request,
    // the row expired, the row is gone, or a durable job already exists. A repeat of the same
    // command/hash replays the stored acceptance identity verbatim and rewrites nothing; a changed
    // payload against an existing job is refused as PayloadConflict with zero writes — the durable
    // 004 conflict evidence belongs to the admission path, and every NotApplied here is strictly
    // read-only. Owner mismatch outranks state classification so a foreign/late owner is reported
    // as such.
    private static async Task<EtlJobAcceptanceOutcome> ClassifyNonAcceptanceAsync(SqliteConnection connection, SqliteTransaction transaction, Guid commandId, string claimOwner, EtlJobAcceptanceRequest request, string entityCodesJson, string now, CancellationToken cancellationToken)
    {
        string? jobPayloadHash = null;
        EtlAcceptedJob? job = null;
        await using (var existing = connection.CreateCommand())
        {
            existing.Transaction = transaction;
            existing.CommandText = "SELECT job_id,run_id,mode,command_payload_hash,acceptance_result_json FROM etl_jobs WHERE command_id=$id;";
            Add(existing, "$id", commandId.ToString("D"));
            await using var reader = await existing.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                job = new EtlAcceptedJob(Guid.Parse(reader.GetString(0)), Guid.Parse(reader.GetString(1)), reader.GetString(2), reader.GetString(4));
                jobPayloadHash = reader.GetString(3);
            }
        }

        string? commandPayloadHash = null;
        string? commandType = null;
        string? commandOwner = null;
        string? commandExpires = null;
        var commandExists = false;
        await using (var existing = connection.CreateCommand())
        {
            existing.Transaction = transaction;
            existing.CommandText = "SELECT payload_hash,exec_claim_owner_id,command_type,expires_at_utc FROM commands_inbox WHERE command_id=$id;";
            Add(existing, "$id", commandId.ToString("D"));
            await using var reader = await existing.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                commandExists = true;
                commandPayloadHash = reader.GetString(0);
                commandOwner = NullableString(reader, 1);
                commandType = NullableString(reader, 2);
                commandExpires = NullableString(reader, 3);
            }
        }

        if (job is not null)
        {
            if (commandExists && !string.Equals(commandPayloadHash, jobPayloadHash, StringComparison.Ordinal))
                return new EtlJobAcceptanceOutcome.NotApplied(EtlJobAcceptanceRejection.PayloadConflict);
            return new EtlJobAcceptanceOutcome.AlreadyAccepted(job);
        }
        if (!commandExists) return new EtlJobAcceptanceOutcome.NotApplied(EtlJobAcceptanceRejection.CommandNotFound);
        if (!string.Equals(commandOwner, claimOwner, StringComparison.Ordinal)) return new EtlJobAcceptanceOutcome.NotApplied(EtlJobAcceptanceRejection.ClaimNotOwned);
        if (!string.Equals(commandType, CanonicalCommandType(request.Mode), StringComparison.Ordinal)
            || !await SelectionCorrespondsAsync(connection, transaction, commandId, request, entityCodesJson, cancellationToken).ConfigureAwait(false))
            return new EtlJobAcceptanceOutcome.NotApplied(EtlJobAcceptanceRejection.TypeModeMismatch);
        if (ParseNullableDate(commandExpires) is { } expires && expires <= ParseDate(now))
            return new EtlJobAcceptanceOutcome.NotApplied(EtlJobAcceptanceRejection.Expired);
        return new EtlJobAcceptanceOutcome.NotApplied(EtlJobAcceptanceRejection.CommandNotAcceptable);
    }

    // The same selection-correspondence predicate used by the guarded UPDATE, re-evaluated on the
    // current row for classification only.
    private static async Task<bool> SelectionCorrespondsAsync(SqliteConnection connection, SqliteTransaction transaction, Guid commandId, EtlJobAcceptanceRequest request, string entityCodesJson, CancellationToken cancellationToken)
    {
        await using var check = connection.CreateCommand();
        check.Transaction = transaction;
        check.CommandText = $"SELECT CASE WHEN {EntitySelectionGuard(request.Mode, "c")} THEN 1 ELSE 0 END FROM commands_inbox c WHERE c.command_id=$id;";
        Add(check, "$id", commandId.ToString("D"));
        Add(check, "$resolvedCodes", entityCodesJson);
        Add(check, "$payloadEntity", request.Entities[0].EntityCode);
        return (long)(await check.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) ?? 0L) == 1L;
    }

    // Canonical saved command_type for each supported durable mode; any other saved type refuses
    // acceptance, so a reconcile or business command can never masquerade as a durable ETL job.
    private static string CanonicalCommandType(string mode) => mode switch
    {
        "entity_reload" => "reload_entity",
        _ => "start_full_sync"
    };

    // Selection-correspondence predicate evaluated against the saved payload INSIDE the transaction.
    // Conservative shape, enforced on the current row: the root payload must be a JSON object.
    // entity_reload: payload.entity must be a non-empty TEXT value equal to the resolved entity.
    // bootstrap_full: an absent entities key means the caller-resolved enabled set; an explicit
    // field must be an ARRAY of non-empty TEXT codes exactly equal to the resolved set — explicit
    // null, objects, scalars, and arrays with null/non-text/empty elements refuse. json_type guards
    // every branch BEFORE json_each so no NULL three-valued-logic bypass or malformed-json error
    // path remains; all compared values are parameterized.
    private static string EntitySelectionGuard(string mode, string alias) => mode switch
    {
        "entity_reload" => $"""
            json_type({alias}.payload_json) = 'object'
            AND json_type({alias}.payload_json,'$.entity') = 'text'
            AND json_extract({alias}.payload_json,'$.entity') <> ''
            AND json_extract({alias}.payload_json,'$.entity') = $payloadEntity
            """,
        _ => $"""
            json_type({alias}.payload_json) = 'object'
            AND CASE
                WHEN json_type({alias}.payload_json,'$.entities') IS NULL THEN 1
                WHEN json_type({alias}.payload_json,'$.entities') = 'array' THEN CASE
                    WHEN (SELECT COUNT(*) FROM json_each({alias}.payload_json,'$.entities')) = 0 THEN 1
                    WHEN EXISTS (SELECT 1 FROM json_each({alias}.payload_json,'$.entities') pe WHERE pe.type <> 'text' OR pe.value = '') THEN 0
                    WHEN EXISTS (SELECT 1 FROM json_each({alias}.payload_json,'$.entities') pe WHERE pe.value NOT IN (SELECT ce.value FROM json_each($resolvedCodes) ce)) THEN 0
                    WHEN EXISTS (SELECT 1 FROM json_each($resolvedCodes) ce WHERE ce.value NOT IN (SELECT pe.value FROM json_each({alias}.payload_json,'$.entities') pe)) THEN 0
                    ELSE 1 END
                ELSE 0 END = 1
            """
    };

    // Supported manual modes only: reconcile modes are never stored as jobs, so nothing can
    // masquerade as a different mode later. Entities must be the explicit resolved set — non-empty,
    // unique codes, each a complete enabled definition (same structural invariants as the effective
    // configuration) — because they are frozen verbatim into the job. entity_reload additionally
    // requires exactly one resolved entity to match the saved payload.entity inside the transaction.
    private static void ValidateAcceptanceRequest(EtlJobAcceptanceRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (request.Mode is not ("bootstrap_full" or "entity_reload"))
            throw new ArgumentException($"Unsupported durable ETL job mode '{request.Mode}'.", nameof(request));
        ArgumentNullException.ThrowIfNull(request.Entities);
        if (request.Entities.Count == 0)
            throw new ArgumentException("A durable ETL job requires at least one resolved entity definition.", nameof(request));
        if (request.Mode == "entity_reload" && request.Entities.Count != 1)
            throw new ArgumentException("An entity_reload job requires exactly one resolved entity matching payload.entity.", nameof(request));
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

    // The structural invariants every resolved entity definition must satisfy — shared
    // with the F1 capture path so a frozen definition is judged identically everywhere.
    private static bool IsValidResolvedEntityDefinition(EtlEntityDefinition entity)
    {
        var keys = entity.EffectiveKeyFields();
        return !(string.IsNullOrWhiteSpace(entity.EntityCode) || string.IsNullOrWhiteSpace(entity.ODataPath) || string.IsNullOrWhiteSpace(entity.KeyField)
            || keys.Any(string.IsNullOrWhiteSpace) || keys.Distinct(StringComparer.Ordinal).Count() != keys.Count || !keys.Contains(entity.KeyField, StringComparer.Ordinal)
            || entity.PageSize is < 1 or > 10_000 || entity.Select is null || entity.Select.Count == 0 || entity.ODataVersion is < 3 or > 4
            || !entity.Enabled
            || (entity.UpdatedAtField is not null && entity.UpdatedAtEdmType is not ("Edm.DateTime" or "Edm.DateTimeOffset")));
    }

    // Locally proposed CD-ETL-1 acceptance result: succeeded + data:{accepted:true, runId, mode}.
    // Written verbatim into commands_inbox.result_json, results_outbox.payload_json and the job's
    // immutable acceptance_result_json, so replay always returns the exact original result.
    private static string BuildAcceptanceResultJson(Guid commandId, Guid runId, string mode) =>
        JsonSerializer.Serialize(new
        {
            commandId,
            status = "succeeded",
            completedAtUtc = DateTimeOffset.UtcNow,
            data = new { accepted = true, runId, mode },
            warnings = Array.Empty<string>(),
            resultVersion = 1
        }, JsonOptions);
}
