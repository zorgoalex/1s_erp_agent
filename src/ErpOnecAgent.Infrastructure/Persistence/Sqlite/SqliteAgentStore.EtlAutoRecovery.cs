using System.Text.Json;
using ErpOnecAgent.Application.Abstractions;
using Microsoft.Data.Sqlite;

namespace ErpOnecAgent.Infrastructure.Persistence.Sqlite;

// A04 (user decision 2026-09-29, option A): an extraction interrupted by a process stop is not
// resumed mid-entity (1C OData cannot keyset on GUID keys; $skip resumes are unsafe). Instead,
// at startup the agent resolves the interrupted run through R1 as the system operator and, for
// a manual job, re-queues the same work as a new run — resolution and re-queue in ONE commit.
public sealed partial class SqliteAgentStore
{
    internal const string AutoRecoveryOperator = "agent:auto-recovery";
    internal const string AutoRecoveryVerification =
        "none: automatic A04 recovery after a process restart; the run was interrupted during extraction and no batch send outcome was unknown";

    /// <inheritdoc cref="IAgentStore.AutoRecoverInterruptedRunsAsync"/>
    public async Task<IReadOnlyList<EtlAutoRecovery>> AutoRecoverInterruptedRunsAsync(int maxChain, DateTimeOffset nowUtc, CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxChain, 1);
        var candidates = new List<Guid>();
        await using (var connection = await factory.OpenAsync(cancellationToken).ConfigureAwait(false))
        await using (var select = connection.CreateCommand())
        {
            select.CommandText = "SELECT run_id FROM etl_runs WHERE status='blocked' AND finalize_conflict_code='INTERRUPTED_NO_CHECKPOINT' AND resolved_at_utc IS NULL ORDER BY created_at_utc, run_id;";
            await using var reader = await select.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false)) candidates.Add(Guid.Parse(reader.GetString(0)));
        }

        var outcomes = new List<EtlAutoRecovery>();
        foreach (var runId in candidates)
            outcomes.Add(await AutoRecoverRunAsync(runId, maxChain, nowUtc, cancellationToken).ConfigureAwait(false));
        return outcomes;
    }

    private async Task<EtlAutoRecovery> AutoRecoverRunAsync(Guid runId, int maxChain, DateTimeOffset nowUtc, CancellationToken cancellationToken)
    {
        var now = nowUtc.ToUniversalTime().ToString("O");
        var runText = runId.ToString("D");
        await using var connection = await factory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);

        // The job of a manual run; a scheduled run has none (its schedule key is released by R1
        // and the scheduler creates the next run as usual).
        RecoverableJob? job = null;
        await using (var read = connection.CreateCommand())
        {
            read.Transaction = transaction;
            read.CommandText = """
                SELECT j.command_id, j.mode, j.entities_json, j.configuration_version, j.command_payload_hash, j.acceptance_result_json,
                       r.requested_entities_json, r.source_generation
                FROM etl_jobs j JOIN etl_runs r ON r.run_id=j.run_id
                WHERE j.run_id=$run;
                """;
            Add(read, "$run", runText);
            await using var reader = await read.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                job = new RecoverableJob(reader.GetString(0), reader.GetString(1), reader.GetString(2), reader.GetInt64(3), reader.GetString(4),
                    reader.GetString(5), NullableString(reader, 6), NullableString(reader, 7));
        }

        var attempt = job is null ? 0 : RecoveryAttempt(job.AcceptanceResultJson) + 1;
        if (job is not null && attempt > maxChain)
        {
            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
            return new EtlAutoRecovery(runId, EtlAutoRecoveryResult.LimitReached, null, attempt - 1, null);
        }

        var resolution = await ResolveInTransactionAsync(connection, transaction,
            new EtlRunResolutionRequest(runId, AutoRecoveryOperator, EtlRunResolutionDecision.Retry, AutoRecoveryVerification, WorkersQuiesced: true),
            now, cancellationToken).ConfigureAwait(false);
        if (resolution is not EtlRunResolutionOutcome.Resolved)
        {
            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
            return new EtlAutoRecovery(runId, EtlAutoRecoveryResult.Refused, null, attempt, (resolution as EtlRunResolutionOutcome.Refused)?.Reason);
        }

        // A04b: ERP already holds acknowledged batches of this run — queue the closing complete
        // in the same commit, so ERP closes the run at once instead of abandoning it after 24 h.
        var notice = await EnqueueInterruptionNoticeAsync(connection, transaction, runId, nowUtc, now, cancellationToken).ConfigureAwait(false);

        if (job is null)
        {
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            return new EtlAutoRecovery(runId, EtlAutoRecoveryResult.ScheduledReleased, null, 0, null, notice);
        }

        // The same work as a new pending run + job: same mode, frozen entity definitions,
        // configuration version and source generation. The job gets a synthetic command id (no
        // ERP command stands behind it; nothing is ever delivered for it) and its immutable
        // acceptance JSON records the lineage, which bounds the chain.
        var newRunId = Guid.NewGuid();
        var newJobId = Guid.NewGuid();
        var syntheticCommandId = Guid.NewGuid();
        var originCommandId = RecoveryOriginCommand(job.AcceptanceResultJson) ?? job.CommandId;
        var acceptance = JsonSerializer.Serialize(new
        {
            recovery = new { recoveryOf = runId, recoveryAttempt = attempt, originCommandId, reason = "INTERRUPTED_NO_CHECKPOINT" },
            data = new { accepted = true, runId = newRunId, mode = job.Mode }
        }, JsonOptions);
        await ExecuteAsync(connection, transaction, """
            INSERT INTO etl_runs(run_id,mode,requested_entities_json,status,started_at_utc,configuration_version,created_at_utc,updated_at_utc,row_version,source_generation)
            VALUES($run,$mode,$entities,'pending',NULL,$configVersion,$now,$now,1,$generation);
            """, cancellationToken,
            ("$run", newRunId.ToString("D")), ("$mode", job.Mode), ("$entities", job.RequestedEntitiesJson), ("$configVersion", job.ConfigurationVersion),
            ("$now", now), ("$generation", job.SourceGeneration)).ConfigureAwait(false);
        await ExecuteAsync(connection, transaction, """
            INSERT INTO etl_jobs(job_id,command_id,run_id,mode,entities_json,configuration_version,status,command_payload_hash,acceptance_result_json,created_at_utc,updated_at_utc,row_version)
            VALUES($job,$command,$run,$mode,$entities,$configVersion,'pending',$hash,$acceptance,$now,$now,1);
            """, cancellationToken,
            ("$job", newJobId.ToString("D")), ("$command", syntheticCommandId.ToString("D")), ("$run", newRunId.ToString("D")), ("$mode", job.Mode),
            ("$entities", job.EntitiesJson), ("$configVersion", job.ConfigurationVersion), ("$hash", job.CommandPayloadHash), ("$acceptance", acceptance), ("$now", now)).ConfigureAwait(false);
        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        return new EtlAutoRecovery(runId, EtlAutoRecoveryResult.Requeued, newRunId, attempt, null, notice);
    }

    internal const string RunInterruptedCode = "RUN_INTERRUPTED";
    private const string RunInterruptedMessage = "The agent process stopped during extraction; the work was re-queued as a new run.";

    // complete v2 for an interrupted run (to-onec/0049): partial_success, EVERY entity of the run
    // failed RUN_INTERRUPTED (so ERP publishes none of them), batchesAcknowledged exactly the
    // acknowledged batches. Only when ERP acknowledged at least one batch — otherwise ERP does
    // not know the run and there is nothing to close.
    private static async Task<bool> EnqueueInterruptionNoticeAsync(SqliteConnection connection, SqliteTransaction transaction, Guid runId, DateTimeOffset nowUtc, string now, CancellationToken cancellationToken)
    {
        var acknowledged = await ScalarLongAsync(connection, transaction,
            "SELECT COUNT(*) FROM etl_batches WHERE run_id=$run AND status IN ('acknowledged','deleted');",
            cancellationToken, ("$run", runId.ToString("D"))).ConfigureAwait(false);
        if (acknowledged == 0) return false;
        var run = await ReadRunRowAsync(connection, transaction, runId, cancellationToken).ConfigureAwait(false);
        if (run is null || !IsSupportedExtractionMode(run.Mode) || !TryReadRunIdentity(run, out var identity)) return false;
        var entities = await ReadEntityRowsAsync(connection, transaction, runId, cancellationToken).ConfigureAwait(false);
        var items = new List<object>(entities.Count);
        foreach (var entity in entities.OrderBy(static entity => entity.EntityName, StringComparer.Ordinal))
        {
            if (ReadScope(run.Mode, entity) is not { } scope) return false;
            items.Add(new CompletePayloadEntity(entity.EntityName, "failed", scope, entity.RowsRead, entity.BatchesCreated, RunInterruptedCode, RunInterruptedMessage, entity.SnapshotAtUtc));
        }
        var payload = JsonSerializer.Serialize(new CompletePayloadV2(
            runId, "partial_success", run.Mode, identity, run.SourceGeneration,
            entities.Sum(static entity => entity.RowsRead), entities.Sum(static entity => entity.BatchesCreated), acknowledged,
            nowUtc.ToUniversalTime(), items.Count, items), JsonOptions);
        await ExecuteAsync(connection, transaction, """
            INSERT INTO etl_run_interruption_notices(run_id,payload_json,status,attempt_count,next_attempt_at_utc,last_error,created_at_utc,updated_at_utc)
            VALUES($run,$payload,'pending',0,$now,NULL,$now,$now);
            """, cancellationToken, ("$run", runId.ToString("D")), ("$payload", payload), ("$now", now)).ConfigureAwait(false);
        return true;
    }

    /// <inheritdoc cref="IAgentStore.GetDueInterruptionNoticesAsync"/>
    public async Task<IReadOnlyList<EtlInterruptionNotice>> GetDueInterruptionNoticesAsync(int limit, DateTimeOffset nowUtc, CancellationToken cancellationToken)
    {
        var notices = new List<EtlInterruptionNotice>();
        await using var connection = await factory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT run_id,payload_json,attempt_count FROM etl_run_interruption_notices WHERE status='pending' AND (next_attempt_at_utc IS NULL OR next_attempt_at_utc <= $now) ORDER BY created_at_utc, run_id LIMIT $limit;";
        Add(command, "$now", nowUtc.ToUniversalTime().ToString("O")); Add(command, "$limit", limit);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            notices.Add(new EtlInterruptionNotice(Guid.Parse(reader.GetString(0)), reader.GetString(1), (int)reader.GetInt64(2)));
        return notices;
    }

    /// <inheritdoc cref="IAgentStore.RecordInterruptionNoticeAsync"/>
    public async Task RecordInterruptionNoticeAsync(Guid runId, EtlInterruptionNoticeStatus status, string? lastError, DateTimeOffset? nextAttemptAtUtc, CancellationToken cancellationToken)
    {
        var statusText = status switch
        {
            EtlInterruptionNoticeStatus.Pending => "pending",
            EtlInterruptionNoticeStatus.Sent => "sent",
            EtlInterruptionNoticeStatus.Refused => "refused",
            EtlInterruptionNoticeStatus.Exhausted => "exhausted",
            _ => throw new ArgumentOutOfRangeException(nameof(status))
        };
        await using var connection = await factory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = "UPDATE etl_run_interruption_notices SET status=$status, attempt_count=attempt_count+1, next_attempt_at_utc=$next, last_error=$error, updated_at_utc=$now WHERE run_id=$run AND status='pending';";
        Add(command, "$status", statusText); Add(command, "$next", nextAttemptAtUtc?.ToUniversalTime().ToString("O")); Add(command, "$error", lastError);
        Add(command, "$now", UtcNow()); Add(command, "$run", runId.ToString("D"));
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    // The lineage stored in a recovery job's acceptance JSON; an ordinary acceptance has none.
    private static int RecoveryAttempt(string acceptanceJson)
    {
        using var document = JsonDocument.Parse(acceptanceJson);
        return document.RootElement.TryGetProperty("recovery", out var recovery) && recovery.TryGetProperty("recoveryAttempt", out var value) && value.TryGetInt32(out var attempt)
            ? attempt
            : 0;
    }

    private static string? RecoveryOriginCommand(string acceptanceJson)
    {
        using var document = JsonDocument.Parse(acceptanceJson);
        return document.RootElement.TryGetProperty("recovery", out var recovery) && recovery.TryGetProperty("originCommandId", out var value)
            ? value.GetString()
            : null;
    }

    private sealed record RecoverableJob(
        string CommandId, string Mode, string EntitiesJson, long ConfigurationVersion, string CommandPayloadHash,
        string AcceptanceResultJson, string? RequestedEntitiesJson, string? SourceGeneration);
}
