using System.Text.Json;
using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Domain.Common;
using ErpOnecAgent.Domain.Etl;
using Microsoft.Data.Sqlite;

namespace ErpOnecAgent.Infrastructure.Persistence.Sqlite;

// A05b F1 DARK storage slice: durable per-entity expected bases/finals, sealed
// extraction output, claim+fenced atomic finalize, bounded retry and an explicit
// startup-only recovery API. Nothing here is called by workers, RecoverAsync or the
// ERP client; the invariants hold only while no legacy writer participates — the v5
// writers (CommitWatermarkAsync, unguarded RegisterBatchAsync, CompleteEtlRunAsync,
// MarkEtlRunExtractedAsync) bypass generation/seal and must be retired/fenced in one
// cutover change (F2) before any production use of this path.
public sealed partial class SqliteAgentStore
{
    /// <inheritdoc cref="IAgentStore.BeginEtlEntityExtractionAsync"/>
    public async Task<EtlEntityBeginOutcome> BeginEtlEntityExtractionAsync(Guid runId, Guid extractionClaimId, EtlEntityExtractionRequest request, CancellationToken cancellationToken)
    {
        if (extractionClaimId == Guid.Empty) throw new ArgumentException("An extraction claim identity is required.", nameof(extractionClaimId));
        ValidateExtractionRequest(request);
        // A missing source namespace means no domain fingerprint can be computed — the
        // caller blocks the run; nothing is captured against an unidentifiable domain.
        if (string.IsNullOrWhiteSpace(request.SourceNamespace))
            return new EtlEntityBeginOutcome.Rejected(EtlEntityBeginRejection.SourceNamespaceMissing);

        var fingerprint = EtlDomainFingerprint.Compute(request.SourceNamespace, request.EntityName, request.EntityDefinitionJson, request.QueryMode);

        await using var connection = await factory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        var now = UtcNow();
        var claimText = extractionClaimId.ToString("D");

        // Write-first fence: serializes concurrent extraction writers of this run and
        // proves in one statement that the run is still mutable (running + unsealed),
        // holds the LIVE extraction claim, and owns its exact manifest set at the bound
        // epochs. A stale/foreign/missing claim or a broken ownership set changes zero
        // rows and rolls back the row_version bump.
        var fence = await ExecuteAsync(connection, transaction, $"""
            UPDATE etl_runs AS r SET row_version=row_version+1, updated_at_utc=$now
            WHERE r.run_id=$run AND r.status='running' AND r.sealed_at_utc IS NULL AND r.extraction_claim_id=$claim
              AND {RunOwnershipSetPredicate};
            """,
            cancellationToken, ("$now", now), ("$run", runId.ToString("D")), ("$claim", claimText)).ConfigureAwait(false);
        if (fence == 0)
        {
            var fenceRejection = await ClassifyRunFenceRejectionAsync(connection, transaction, runId, claimText, cancellationToken).ConfigureAwait(false);
            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
            return new EtlEntityBeginOutcome.Rejected(fenceRejection);
        }

        var entityExists = await ScalarLongAsync(connection, transaction,
            "SELECT COUNT(*) FROM etl_run_entities WHERE run_id=$run AND entity_name=$entity;",
            cancellationToken, ("$run", runId.ToString("D")), ("$entity", request.EntityName)).ConfigureAwait(false);
        if (entityExists != 0)
        {
            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
            return new EtlEntityBeginOutcome.Rejected(EtlEntityBeginRejection.RunNotAcceptingEntities);
        }

        // Saved run identity is authoritative and is enforced at capture, not deferred to
        // seal: the caller's query mode must equal the run's saved mode (which must itself
        // be a supported watermark-extraction mode) and the entity must be a member of the
        // saved requested set — a request can never extend or override durable identity.
        string? runMode = null;
        string? requestedEntitiesJson = null;
        long? runConfigurationVersion = null;
        await using (var read = connection.CreateCommand())
        {
            read.Transaction = transaction;
            read.CommandText = "SELECT mode,requested_entities_json,configuration_version FROM etl_runs WHERE run_id=$run;";
            Add(read, "$run", runId.ToString("D"));
            await using var reader = await read.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                runMode = reader.GetString(0);
                requestedEntitiesJson = NullableString(reader, 1);
                runConfigurationVersion = NullableLong(reader, 2);
            }
        }

        if (!IsSupportedExtractionMode(runMode) || !string.Equals(request.QueryMode, runMode, StringComparison.Ordinal))
        {
            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
            return new EtlEntityBeginOutcome.Rejected(EtlEntityBeginRejection.RunModeMismatch);
        }

        var requested = ParseRequestedEntities(requestedEntitiesJson);
        if (requested is null)
        {
            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
            return new EtlEntityBeginOutcome.Rejected(EtlEntityBeginRejection.RunManifestInvalid);
        }
        if (!requested.Contains(request.EntityName))
        {
            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
            return new EtlEntityBeginOutcome.Rejected(EtlEntityBeginRejection.EntityNotInManifest);
        }

        // The frozen definition must be a valid typed entity definition FOR this entity —
        // '{}' or a different entity's definition is not a usable extraction domain.
        if (!TryReadEntityDefinition(request.EntityDefinitionJson, request.EntityName))
        {
            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
            return new EtlEntityBeginOutcome.Rejected(EtlEntityBeginRejection.EntityDefinitionInvalid);
        }

        // A run associated with a durable job may only extract against the job's frozen
        // identity: the job's mode and configuration version must still equal the run's
        // durable values, and a caller-supplied definition can never override the job's
        // frozen definition for this entity.
        string? jobMode = null;
        long? jobConfigurationVersion = null;
        string? jobEntitiesJson = null;
        await using (var read = connection.CreateCommand())
        {
            read.Transaction = transaction;
            read.CommandText = "SELECT mode,configuration_version,entities_json FROM etl_jobs WHERE run_id=$run;";
            Add(read, "$run", runId.ToString("D"));
            await using var reader = await read.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                jobMode = reader.GetString(0);
                jobConfigurationVersion = reader.GetInt64(1);
                jobEntitiesJson = reader.GetString(2);
            }
        }
        string? scheduleKey = null;
        string? resolvedEntitiesJson = null;
        await using (var read = connection.CreateCommand())
        {
            read.Transaction = transaction;
            read.CommandText = "SELECT schedule_key,resolved_entities_json FROM etl_runs WHERE run_id=$run;";
            Add(read, "$run", runId.ToString("D"));
            await using var reader = await read.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                scheduleKey = NullableString(reader, 0);
                resolvedEntitiesJson = NullableString(reader, 1);
            }
        }
        if (jobEntitiesJson is null)
        {
            // A jobless run has a frozen identity source only when it is a scheduled run
            // (O3): its resolved definitions must still form a consistent frozen identity
            // with the manifest, and the caller's definition must equal the frozen one. A
            // jobless run without a schedule key (e.g. legacy) has no identity source.
            if (scheduleKey is null)
            {
                await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
                return new EtlEntityBeginOutcome.Rejected(EtlEntityBeginRejection.JobMissing);
            }
            if (!ScheduledIdentityConsistent(runMode, runConfigurationVersion, resolvedEntitiesJson, requestedEntitiesJson)
                || !FrozenDefinitionMatches(resolvedEntitiesJson!, request.EntityName, request.EntityDefinitionJson))
            {
                await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
                return new EtlEntityBeginOutcome.Rejected(EtlEntityBeginRejection.RunDefinitionMismatch);
            }
        }
        // A job-backed run never carries a schedule key: both identity sources at once is
        // corrupt evidence.
        else if (scheduleKey is not null
            || !string.Equals(jobMode, runMode, StringComparison.Ordinal) || jobConfigurationVersion != runConfigurationVersion)
        {
            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
            return new EtlEntityBeginOutcome.Rejected(EtlEntityBeginRejection.JobInconsistent);
        }
        if (jobEntitiesJson is not null && !FrozenDefinitionMatches(jobEntitiesJson, request.EntityName, request.EntityDefinitionJson))
        {
            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
            return new EtlEntityBeginOutcome.Rejected(EtlEntityBeginRejection.JobDefinitionMismatch);
        }

        // Capture the base atomically BEFORE extraction: raw committed cursor bytes,
        // generation and stored domain fingerprint — presence distinguishes an absent
        // row from a present row with NULL values.
        bool basePresent;
        long? baseGeneration = null;
        string? baseCursor = null;
        string? baseFingerprint = null;
        await using (var read = connection.CreateCommand())
        {
            read.Transaction = transaction;
            read.CommandText = "SELECT generation, committed_cursor_json, domain_fingerprint FROM watermarks WHERE entity_name=$entity;";
            Add(read, "$entity", request.EntityName);
            await using var reader = await read.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            basePresent = await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
            if (basePresent)
            {
                baseGeneration = reader.GetInt64(0);
                baseCursor = NullableString(reader, 1);
                baseFingerprint = NullableString(reader, 2);
            }
        }

        var domainStatus = !basePresent ? "absent"
            : baseFingerprint is null ? "unknown"
            : string.Equals(baseFingerprint, fingerprint, StringComparison.Ordinal) ? "same"
            : "changed";

        var proceeded = domainStatus is "absent" or "same";
        await ExecuteAsync(connection, transaction, """
            INSERT INTO etl_run_entities(run_id,entity_name,entity_definition_json,domain_fingerprint,status,base_row_present,
                expected_base_generation,expected_base_cursor_json,expected_base_domain_fingerprint,domain_status,
                watermark_from_json,snapshot_upper_bound_json,final_watermark_json,expected_batch_count,rows_read,batches_created,
                last_error,created_at_utc,updated_at_utc,row_version)
            VALUES($run,$entity,$definition,$fp,$status,$present,$gen,$base,$baseFp,$domain,$from,$upper,NULL,NULL,0,0,$error,$now,$now,1);
            """, cancellationToken,
            ("$run", runId.ToString("D")), ("$entity", request.EntityName), ("$definition", request.EntityDefinitionJson),
            ("$fp", fingerprint), ("$status", proceeded ? "extracting" : "failed"), ("$present", basePresent ? 1 : 0),
            ("$gen", baseGeneration), ("$base", baseCursor), ("$baseFp", baseFingerprint), ("$domain", domainStatus),
            ("$from", baseCursor), ("$upper", request.SnapshotUpperBoundJson),
            ("$error", domainStatus == "changed" ? "DOMAIN_CHANGED" : domainStatus == "unknown" ? "DOMAIN_UNKNOWN" : null),
            ("$now", now)).ConfigureAwait(false);
        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);

        if (!proceeded)
        {
            return new EtlEntityBeginOutcome.Rejected(domainStatus == "changed"
                ? EtlEntityBeginRejection.DomainChanged
                : EtlEntityBeginRejection.DomainUnknown);
        }

        return new EtlEntityBeginOutcome.Begun(new EtlEntityExtractionBase(
            request.EntityName, basePresent, baseCursor, baseGeneration, baseFingerprint, fingerprint, domainStatus));
    }

    /// <inheritdoc cref="IAgentStore.RegisterGuardedEtlBatchAsync"/>
    public async Task<EtlBatchRegistrationOutcome> RegisterGuardedEtlBatchAsync(EtlBatch batch, Guid extractionClaimId, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(batch);
        if (extractionClaimId == Guid.Empty) throw new ArgumentException("An extraction claim identity is required.", nameof(extractionClaimId));
        if (string.IsNullOrWhiteSpace(batch.EntityName)) throw new ArgumentException("Batch entity name is required.", nameof(batch));
        if (batch.RowCount < 0) throw new ArgumentOutOfRangeException(nameof(batch), "Batch row count cannot be negative.");

        await using var connection = await factory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        var now = UtcNow();
        var claimText = extractionClaimId.ToString("D");

        // Guarded counter bump first: only a 'running' unsealed run holding the live
        // extraction claim AND its exact bound ownership set with this entity still
        // 'extracting' accepts a batch; the update also serializes writers. The touched
        // entity itself must be a manifest member with an epoch-bound active ownership
        // row — a stray injected 'extracting' row for an unowned entity can never accept
        // a write. A stale claim or a broken ownership set rejects with zero writes.
        var guarded = await ExecuteAsync(connection, transaction, $"""
            UPDATE etl_run_entities SET batches_created=batches_created+1, rows_read=rows_read+$rows, updated_at_utc=$now, row_version=row_version+1
            WHERE run_id=$run AND entity_name=$entity AND status='extracting'
              AND EXISTS (SELECT 1 FROM etl_runs r WHERE r.run_id=$run AND r.status='running' AND r.sealed_at_utc IS NULL
                          AND r.extraction_claim_id=$claim AND {RunOwnershipSetPredicate}
                          AND {EntityOwnershipPredicate});
            """, cancellationToken,
            ("$rows", batch.RowCount), ("$now", now), ("$run", batch.RunId.ToString("D")), ("$entity", batch.EntityName), ("$claim", claimText)).ConfigureAwait(false);
        if (guarded == 0)
        {
            var rejection = await ClassifyBatchRejectionAsync(connection, transaction, batch.RunId, batch.EntityName, claimText, cancellationToken).ConfigureAwait(false);
            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
            return new EtlBatchRegistrationOutcome.Rejected(rejection);
        }

        await ExecuteAsync(connection, transaction, """
            INSERT INTO etl_batches(batch_id,run_id,entity_name,schema_version,file_path,status,row_count,watermark_from_json,watermark_to_json,sha256,compressed_size,uncompressed_size,attempt_count,created_at_utc,next_attempt_at_utc)
            VALUES($id,$run,$entity,$schema,$path,'ready',$rows,$from,$to,$hash,$compressed,$uncompressed,0,$created,$created);
            """, cancellationToken,
            ("$id", batch.BatchId.ToString("D")), ("$run", batch.RunId.ToString("D")), ("$entity", batch.EntityName), ("$schema", batch.SchemaVersion), ("$path", batch.FilePath), ("$rows", batch.RowCount),
            ("$from", SerializeCursor(batch.WatermarkFrom)), ("$to", SerializeCursor(batch.WatermarkTo)), ("$hash", batch.Sha256), ("$compressed", batch.CompressedSize), ("$uncompressed", batch.UncompressedSize), ("$created", batch.CreatedAtUtc.ToUniversalTime().ToString("O"))).ConfigureAwait(false);
        await ExecuteAsync(connection, transaction,
            "UPDATE etl_runs SET batches_created=batches_created+1, rows_read=rows_read+$rows, updated_at_utc=$now WHERE run_id=$run;",
            cancellationToken, ("$rows", batch.RowCount), ("$now", now), ("$run", batch.RunId.ToString("D"))).ConfigureAwait(false);
        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        return new EtlBatchRegistrationOutcome.Registered();
    }

    /// <inheritdoc cref="IAgentStore.CompleteEtlEntityExtractionAsync"/>
    public async Task<EtlEntityCompletionOutcome> CompleteEtlEntityExtractionAsync(Guid runId, Guid extractionClaimId, string entityName, string finalWatermarkJson, int expectedBatchCount, CancellationToken cancellationToken)
    {
        if (extractionClaimId == Guid.Empty) throw new ArgumentException("An extraction claim identity is required.", nameof(extractionClaimId));
        if (string.IsNullOrWhiteSpace(entityName)) throw new ArgumentException("Entity name is required.", nameof(entityName));
        if (!IsValidCursorJson(finalWatermarkJson, requireMeaningfulComponent: true))
            throw new ArgumentException("The final watermark must be a cursor JSON object with at least one non-NULL component.", nameof(finalWatermarkJson));
        if (expectedBatchCount < 1) throw new ArgumentOutOfRangeException(nameof(expectedBatchCount), "The expected batch count must be at least one (the empty flush always exists).");

        await using var connection = await factory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        var now = UtcNow();
        var claimText = extractionClaimId.ToString("D");

        var guarded = await ExecuteAsync(connection, transaction, $"""
            UPDATE etl_run_entities SET status='done', final_watermark_json=$final, expected_batch_count=$expected, updated_at_utc=$now, row_version=row_version+1
            WHERE run_id=$run AND entity_name=$entity AND status='extracting' AND batches_created=$expected
              AND EXISTS (SELECT 1 FROM etl_runs r WHERE r.run_id=$run AND r.status='running' AND r.sealed_at_utc IS NULL
                          AND r.extraction_claim_id=$claim AND {RunOwnershipSetPredicate}
                          AND {EntityOwnershipPredicate});
            """, cancellationToken,
            ("$final", finalWatermarkJson), ("$expected", expectedBatchCount), ("$now", now),
            ("$run", runId.ToString("D")), ("$entity", entityName), ("$claim", claimText)).ConfigureAwait(false);
        if (guarded == 0)
        {
            var rejection = await ClassifyEntityCompletionRejectionAsync(connection, transaction, runId, entityName, expectedBatchCount, claimText, cancellationToken).ConfigureAwait(false);
            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
            return new EtlEntityCompletionOutcome.Rejected(rejection);
        }

        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        return new EtlEntityCompletionOutcome.Completed();
    }

    /// <inheritdoc cref="IAgentStore.SealEtlRunExtractionAsync"/>
    public async Task<EtlRunSealOutcome> SealEtlRunExtractionAsync(Guid runId, Guid extractionClaimId, CancellationToken cancellationToken)
    {
        if (extractionClaimId == Guid.Empty) throw new ArgumentException("An extraction claim identity is required.", nameof(extractionClaimId));

        await using var connection = await factory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        var now = UtcNow();
        var claimText = extractionClaimId.ToString("D");

        // Same write-first fence as every extraction mutation: live claim + exact
        // ownership set — a stale claim or broken ownership rejects with zero writes.
        var fence = await ExecuteAsync(connection, transaction, $"""
            UPDATE etl_runs AS r SET row_version=row_version+1, updated_at_utc=$now
            WHERE r.run_id=$run AND r.status='running' AND r.sealed_at_utc IS NULL AND r.extraction_claim_id=$claim
              AND {RunOwnershipSetPredicate};
            """,
            cancellationToken, ("$now", now), ("$run", runId.ToString("D")), ("$claim", claimText)).ConfigureAwait(false);
        if (fence == 0)
        {
            var fenceRejection = await ClassifySealFenceRejectionAsync(connection, transaction, runId, claimText, cancellationToken).ConfigureAwait(false);
            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
            return new EtlRunSealOutcome.Rejected(fenceRejection);
        }

        var run = await ReadRunRowAsync(connection, transaction, runId, cancellationToken).ConfigureAwait(false);
        var requested = ParseRequestedEntities(run?.RequestedEntitiesJson);
        if (requested is null)
        {
            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
            return new EtlRunSealOutcome.Rejected(EtlRunSealRejection.ManifestInvalid);
        }

        var entities = await ReadEntityRowsAsync(connection, transaction, runId, cancellationToken).ConfigureAwait(false);
        if (entities.Count != requested.Count || entities.Any(entity => !requested.Contains(entity.EntityName)))
        {
            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
            return new EtlRunSealOutcome.Rejected(EtlRunSealRejection.EntitySetMismatch);
        }

        var batchAggregates = await ReadBatchAggregatesAsync(connection, transaction, runId, cancellationToken).ConfigureAwait(false);
        var expectedTotal = 0;
        foreach (var entity in entities)
        {
            var rejection = ValidateSealedEntity(entity, batchAggregates);
            if (rejection is not null)
            {
                await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
                return new EtlRunSealOutcome.Rejected(rejection.Value);
            }
            expectedTotal += (int)entity.ExpectedBatchCount!.Value;
        }

        // Run-level consistency: no stowaway batches and counters equal the sealed totals.
        var actualTotal = batchAggregates.Values.Sum(static aggregate => aggregate.Count);
        if (actualTotal != expectedTotal || run!.BatchesCreated != expectedTotal || run.RowsRead != entities.Sum(static entity => entity.RowsRead))
        {
            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
            return new EtlRunSealOutcome.Rejected(EtlRunSealRejection.ExpectedBatchCountMismatch);
        }

        // The extraction fence ends at seal: the claim columns are cleared in the same
        // success commit — a post-seal mutation under the old claim can never write.
        await ExecuteAsync(connection, transaction,
            "UPDATE etl_runs SET status='uploading', sealed_at_utc=$now, sealed_entity_count=$entities, sealed_expected_batch_count=$batches, extraction_claim_id=NULL, extraction_claim_owner_id=NULL, extraction_claim_acquired_at_utc=NULL, updated_at_utc=$now, row_version=row_version+1 WHERE run_id=$run;",
            cancellationToken,
            ("$now", now), ("$entities", entities.Count), ("$batches", expectedTotal), ("$run", runId.ToString("D"))).ConfigureAwait(false);
        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        return new EtlRunSealOutcome.Sealed(entities.Count, expectedTotal);
    }

    /// <inheritdoc cref="IAgentStore.FailEtlRunAsync"/>
    public Task<EtlRunTerminationOutcome> FailEtlRunAsync(Guid runId, Guid extractionClaimId, string errorMessage, CancellationToken cancellationToken) =>
        TerminateRunAsync(runId, extractionClaimId, "failed", null, string.IsNullOrWhiteSpace(errorMessage) ? "run failed" : errorMessage, "RUN_FAILED", cancellationToken);

    /// <inheritdoc cref="IAgentStore.BlockEtlRunAsync"/>
    public Task<EtlRunTerminationOutcome> BlockEtlRunAsync(Guid runId, Guid extractionClaimId, string code, string message, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(code)) throw new ArgumentException("A conflict code is required.", nameof(code));
        return TerminateRunAsync(runId, extractionClaimId, "blocked", code, string.IsNullOrWhiteSpace(message) ? code : message, "RUN_BLOCKED", cancellationToken);
    }

    /// <inheritdoc cref="IAgentStore.GetDueRunCompletionsAsync"/>
    public async Task<IReadOnlyList<EtlRunCompletionCandidate>> GetDueRunCompletionsAsync(int limit, DateTimeOffset nowUtc, CancellationToken cancellationToken)
    {
        var candidates = new List<EtlRunCompletionCandidate>();
        await using var connection = await factory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT run_id, status FROM etl_runs
            WHERE status='uploading'
               OR (status='completing' AND completion_claim_id IS NULL
                   AND (next_completion_attempt_at_utc IS NULL OR next_completion_attempt_at_utc <= $now))
            ORDER BY COALESCE(started_at_utc, created_at_utc) LIMIT $limit;
            """;
        Add(command, "$now", nowUtc.ToUniversalTime().ToString("O")); Add(command, "$limit", limit);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            var status = reader.GetString(1);
            candidates.Add(new EtlRunCompletionCandidate(Guid.Parse(reader.GetString(0)), string.Equals(status, "completing", StringComparison.Ordinal)));
        }
        return candidates;
    }

    /// <inheritdoc cref="IAgentStore.TryClaimRunCompletionAsync"/>
    public async Task<EtlRunClaimOutcome> TryClaimRunCompletionAsync(Guid runId, string ownerId, DateTimeOffset nextAttemptAtUtc, int maxAttempts, CancellationToken cancellationToken)
    {
        ValidateOwner(ownerId);
        ArgumentOutOfRangeException.ThrowIfLessThan(maxAttempts, 1);

        await using var connection = await factory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        var now = UtcNow();
        var claimId = Guid.NewGuid();

        // Write-first claim: serializes concurrent claimants — exactly one commits; the
        // loser changes zero rows and rolls back untouched. The attempt bound is part of
        // admission: the first claim persists the policy limit on the run row, a later
        // claim presenting a different limit is refused without writes, and a released
        // due claim is refused once the stored attempt count reached the bound — so
        // crash+recovery cycles can never mint an unbounded completion send.
        var claimed = await ExecuteAsync(connection, transaction, """
            UPDATE etl_runs SET status='completing', completion_claim_id=$claim, completion_claim_owner_id=$owner,
                completion_claim_acquired_at_utc=$now, completion_attempt_count=completion_attempt_count+1,
                completion_max_attempts=COALESCE(completion_max_attempts,$max),
                next_completion_attempt_at_utc=$next, updated_at_utc=$now, row_version=row_version+1
            WHERE run_id=$run AND (
                status='uploading'
                OR (status='completing' AND completion_claim_id IS NULL
                    AND (next_completion_attempt_at_utc IS NULL OR next_completion_attempt_at_utc <= $now)))
              AND completion_attempt_count < COALESCE(completion_max_attempts,$max)
              AND (completion_max_attempts IS NULL OR completion_max_attempts=$max);
            """, cancellationToken,
            ("$claim", claimId.ToString("D")), ("$owner", ownerId), ("$now", now), ("$max", maxAttempts),
            ("$next", nextAttemptAtUtc.ToUniversalTime().ToString("O")), ("$run", runId.ToString("D"))).ConfigureAwait(false);
        if (claimed == 0)
        {
            // Only a released, due, budget-exhausted 'completing' run is durably blocked
            // here; every other not-claimable state changes nothing.
            if (await IsAttemptBoundExhaustedAsync(connection, transaction, runId, maxAttempts, now, cancellationToken).ConfigureAwait(false))
            {
                const string exhaustedMessage = "Run completion attempts exhausted; the stored payload and evidence are preserved for manual resolution.";
                await CommitBlockedRunAsync(connection, transaction, runId, "COMPLETION_ATTEMPTS_EXHAUSTED", exhaustedMessage, now, cancellationToken).ConfigureAwait(false);
                await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
                return new EtlRunClaimOutcome.Blocked("COMPLETION_ATTEMPTS_EXHAUSTED", exhaustedMessage);
            }
            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
            return new EtlRunClaimOutcome.NotClaimed();
        }

        var run = await ReadRunRowAsync(connection, transaction, runId, cancellationToken).ConfigureAwait(false);
        // The claim UPDATE just incremented completion_attempt_count: 1 means this is the
        // FIRST successful claim (the run was 'uploading'); >1 means a prior claim minted
        // the immutable body — it must still be stored. A stored payload on a first
        // claim, or a missing payload on a reclaim, is corrupt persisted evidence:
        // block and preserve, never fabricate a replacement or authorize another send.
        var priorClaimExisted = run!.CompletionAttemptCount > 1;
        if (run.CompletePayloadJson is not null && !priorClaimExisted)
        {
            const string fabricated = "Completion claim found a stored payload on a run that was never claimed; fabricated evidence is preserved for manual resolution.";
            await CommitBlockedRunAsync(connection, transaction, runId, "SEAL_VIOLATED", fabricated, now, cancellationToken).ConfigureAwait(false);
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            return new EtlRunClaimOutcome.Blocked("SEAL_VIOLATED", fabricated);
        }
        if (run.CompletePayloadJson is null && priorClaimExisted)
        {
            const string lost = "Completion reclaim found the immutable payload missing; a previously claimed run can never fabricate a replacement and is preserved for manual resolution.";
            await CommitBlockedRunAsync(connection, transaction, runId, "SEAL_VIOLATED", lost, now, cancellationToken).ConfigureAwait(false);
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            return new EtlRunClaimOutcome.Blocked("SEAL_VIOLATED", lost);
        }
        if (run.CompletePayloadJson is not null)
        {
            // Reclaim: the immutable stored body is replayed verbatim — but only after the
            // payload schema/identity and the FULL seal/entity/ACK readiness re-verify
            // inside this transaction. Regressed or fabricated evidence blocks and
            // preserves the run; an arbitrary object is never a replayable payload.
            if (!IsValidCompletePayload(run.CompletePayloadJson, runId, run))
            {
                const string corruptPayload = "Completion reclaim found a stored payload that fails schema or identity validation; the run is preserved for manual resolution.";
                await CommitBlockedRunAsync(connection, transaction, runId, "SEAL_VIOLATED", corruptPayload, now, cancellationToken).ConfigureAwait(false);
                await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
                return new EtlRunClaimOutcome.Blocked("SEAL_VIOLATED", corruptPayload);
            }
            var reclaimReadiness = await VerifyClaimReadinessAsync(connection, transaction, runId, run, cancellationToken).ConfigureAwait(false);
            if (reclaimReadiness != ClaimReadiness.Ready)
            {
                // The payload exists only because a prior claim proved readiness — any
                // gap now (regressed ACKs, missing entities, corrupt bases, broken
                // ownership) is a violation, not a transient wait: block and preserve,
                // never a new admissible claim.
                var regressedCode = reclaimReadiness == ClaimReadiness.OwnershipMismatch ? "OWNERSHIP_SET_MISMATCH" : "SEAL_VIOLATED";
                var regressed = $"Completion reclaim found seal/ownership evidence that no longer satisfies readiness ({regressedCode}); the run is preserved for manual resolution.";
                await CommitBlockedRunAsync(connection, transaction, runId, regressedCode, regressed, now, cancellationToken).ConfigureAwait(false);
                await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
                return new EtlRunClaimOutcome.Blocked(regressedCode, regressed);
            }

            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            return new EtlRunClaimOutcome.Claimed(new EtlRunCompletionClaim(runId, claimId, ownerId, run.CompletePayloadJson, (int)run.CompletionAttemptCount));
        }

        // Fresh claim: full readiness inside the claim transaction.
        var readiness = await VerifyClaimReadinessAsync(connection, transaction, runId, run, cancellationToken).ConfigureAwait(false);
        if (readiness == ClaimReadiness.Legacy)
        {
            const string message = "Run has no extraction seal or entity manifest; a legacy uploading run can never derive durable bases and is preserved for manual resolution.";
            await CommitBlockedRunAsync(connection, transaction, runId, "LEGACY_UNRESOLVABLE", message, now, cancellationToken).ConfigureAwait(false);
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            return new EtlRunClaimOutcome.Blocked("LEGACY_UNRESOLVABLE", message);
        }
        if (readiness == ClaimReadiness.Violated)
        {
            const string message = "Sealed run evidence failed the in-transaction readiness recheck; the run is preserved for manual resolution.";
            await CommitBlockedRunAsync(connection, transaction, runId, "SEAL_VIOLATED", message, now, cancellationToken).ConfigureAwait(false);
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            return new EtlRunClaimOutcome.Blocked("SEAL_VIOLATED", message);
        }
        if (readiness == ClaimReadiness.OwnershipMismatch)
        {
            const string message = "The run's manifest no longer equals its ownership bindings and active ownership rows at the bound epochs; the run is preserved for manual resolution.";
            await CommitBlockedRunAsync(connection, transaction, runId, "OWNERSHIP_SET_MISMATCH", message, now, cancellationToken).ConfigureAwait(false);
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            return new EtlRunClaimOutcome.Blocked("OWNERSHIP_SET_MISMATCH", message);
        }
        if (readiness == ClaimReadiness.Transient)
        {
            // Uploads still in flight: undo the tentative claim and leave the run 'uploading'.
            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
            return new EtlRunClaimOutcome.NotClaimed();
        }

        var payload = JsonSerializer.Serialize(new
        {
            runId,
            status = "succeeded",
            rowsRead = run.RowsRead,
            batchesCreated = run.BatchesCreated,
            batchesAcknowledged = run.BatchesAcknowledged,
            completedAtUtc = DateTimeOffset.UtcNow
        }, JsonOptions);
        await ExecuteAsync(connection, transaction,
            "UPDATE etl_runs SET complete_payload_json=$payload WHERE run_id=$run;",
            cancellationToken, ("$payload", payload), ("$run", runId.ToString("D"))).ConfigureAwait(false);
        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        return new EtlRunClaimOutcome.Claimed(new EtlRunCompletionClaim(runId, claimId, ownerId, payload, (int)run.CompletionAttemptCount));
    }

    /// <inheritdoc cref="IAgentStore.FinalizeEtlRunAsync"/>
    public async Task<EtlRunFinalizeOutcome> FinalizeEtlRunAsync(Guid runId, Guid claimId, CancellationToken cancellationToken)
    {
        if (claimId == Guid.Empty) throw new ArgumentException("A claim identity is required.", nameof(claimId));

        await using var connection = await factory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        var now = UtcNow();

        // Fenced write-first guard under the EXACT claim identity: serializes finalize
        // calls, so a stale or superseded claim loses with zero committed writes.
        var fence = await ExecuteAsync(connection, transaction,
            "UPDATE etl_runs SET row_version=row_version+1, updated_at_utc=$now WHERE run_id=$run AND status='completing' AND completion_claim_id=$claim;",
            cancellationToken, ("$now", now), ("$run", runId.ToString("D")), ("$claim", claimId.ToString("D"))).ConfigureAwait(false);
        if (fence == 0)
        {
            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
            return new EtlRunFinalizeOutcome.ClaimLost();
        }

        var run = await ReadRunRowAsync(connection, transaction, runId, cancellationToken).ConfigureAwait(false);
        var entities = await ReadEntityRowsAsync(connection, transaction, runId, cancellationToken).ConfigureAwait(false);
        var sealViolation = await VerifyFinalSealAsync(connection, transaction, runId, run!, cancellationToken).ConfigureAwait(false);
        if (sealViolation is not null)
        {
            var violationMessage = sealViolation == "OWNERSHIP_SET_MISMATCH"
                ? "The run's manifest no longer equals its ownership bindings and active ownership rows at the bound epochs; the run is preserved for manual resolution."
                : "Seal evidence failed the in-transaction finalize recheck; the run is preserved for manual resolution.";
            await CommitBlockedRunAsync(connection, transaction, runId, sealViolation, violationMessage, now, cancellationToken).ConfigureAwait(false);
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            return new EtlRunFinalizeOutcome.Blocked(sealViolation, violationMessage);
        }

        // Presence+generation+cursor+domain CAS per entity AND the epoch-bound ownership
        // release inside ONE savepoint: any single mismatch — including a release count
        // that differs from the sealed entity count — rolls back every watermark change
        // AND the partial release, then one commit writes the blocked run + conflict +
        // blocked job — never a two-transaction gap.
        await ExecuteAsync(connection, transaction, "SAVEPOINT etl_finalize_cas;", cancellationToken).ConfigureAwait(false);
        string? conflictCode = null;
        string? conflictEntity = null;
        foreach (var entity in entities.OrderBy(static entity => entity.EntityName, StringComparer.Ordinal))
        {
            long changed;
            if (entity.BaseRowPresent == 1)
            {
                changed = await ExecuteAsync(connection, transaction, """
                    UPDATE watermarks
                    SET committed_cursor_json=$final, extracting_cursor_json=NULL, last_run_id=$run,
                        generation=generation+1, domain_fingerprint=$fp, updated_at_utc=$now
                    WHERE entity_name=$entity
                      AND generation=$gen
                      AND committed_cursor_json IS $base
                      AND domain_fingerprint IS $baseFp;
                    """, cancellationToken,
                    ("$final", entity.FinalWatermarkJson), ("$run", runId.ToString("D")), ("$fp", entity.DomainFingerprint), ("$now", now),
                    ("$entity", entity.EntityName), ("$gen", entity.ExpectedBaseGeneration), ("$base", entity.ExpectedBaseCursorJson), ("$baseFp", entity.ExpectedBaseDomainFingerprint)).ConfigureAwait(false);
            }
            else
            {
                changed = await ExecuteAsync(connection, transaction, """
                    INSERT INTO watermarks(entity_name,committed_cursor_json,extracting_cursor_json,last_run_id,generation,domain_fingerprint,updated_at_utc)
                    SELECT $entity,$final,NULL,$run,1,$fp,$now
                    WHERE NOT EXISTS (SELECT 1 FROM watermarks WHERE entity_name=$entity);
                    """, cancellationToken,
                    ("$entity", entity.EntityName), ("$final", entity.FinalWatermarkJson), ("$run", runId.ToString("D")), ("$fp", entity.DomainFingerprint), ("$now", now)).ConfigureAwait(false);
            }

            if (changed != 1)
            {
                conflictCode = await ClassifyCasConflictAsync(connection, transaction, entity, cancellationToken).ConfigureAwait(false);
                conflictEntity = entity.EntityName;
                break;
            }
        }

        if (conflictCode is null)
        {
            // Hard release is part of the same savepoint: the epoch-bound update can
            // never free a re-acquired (ABA) or foreign row, and it must change EXACTLY
            // the sealed entity count — a short count is a controlled mismatch, not a
            // partial commit.
            var released = await ExecuteAsync(connection, transaction, """
                UPDATE etl_entity_ownership AS o SET released_at_utc=$now, release_reason='finalized', updated_at_utc=$now, row_version=row_version+1
                WHERE o.released_at_utc IS NULL
                  AND EXISTS (SELECT 1 FROM etl_run_ownership_bindings b
                              WHERE b.run_id=$run AND b.entity_name=o.entity_name
                                AND o.owner_run_id=$run AND o.ownership_epoch=b.expected_epoch);
                """, cancellationToken,
                ("$now", now), ("$run", runId.ToString("D"))).ConfigureAwait(false);
            if (released != (int)run!.SealedEntityCount!.Value)
            {
                conflictCode = "OWNERSHIP_RELEASE_MISMATCH";
            }
        }

        if (conflictCode is null)
        {
            await ExecuteAsync(connection, transaction, "RELEASE SAVEPOINT etl_finalize_cas;", cancellationToken).ConfigureAwait(false);
            // Expected-one transitions are checked before commit: a zero-row guard loss
            // here means durable state moved inside the transaction boundary — the whole
            // transaction rolls back and the completing claim is preserved for a fenced
            // retry rather than committing a half-applied success.
            var succeeded = await ExecuteAsync(connection, transaction, """
                UPDATE etl_runs SET status='succeeded', finished_at_utc=$now, completion_acknowledged_at_utc=$now,
                    completion_claim_owner_id=NULL, completion_claim_acquired_at_utc=NULL, next_completion_attempt_at_utc=NULL,
                    updated_at_utc=$now, row_version=row_version+1
                WHERE run_id=$run AND status='completing' AND completion_claim_id=$claim;
                """, cancellationToken,
                ("$now", now), ("$run", runId.ToString("D")), ("$claim", claimId.ToString("D"))).ConfigureAwait(false);
            if (succeeded != 1) throw new InvalidOperationException($"ETL run {runId:D} lost its completing claim mid-transaction.");
            var jobExpected = await ScalarLongAsync(connection, transaction,
                "SELECT COUNT(*) FROM etl_jobs WHERE run_id=$run AND status NOT IN ('finished','cancelled');",
                cancellationToken, ("$run", runId.ToString("D"))).ConfigureAwait(false);
            var finished = await ExecuteAsync(connection, transaction,
                "UPDATE etl_jobs SET status='finished', updated_at_utc=$now, row_version=row_version+1 WHERE run_id=$run AND status NOT IN ('finished','cancelled');",
                cancellationToken, ("$now", now), ("$run", runId.ToString("D"))).ConfigureAwait(false);
            if (finished != jobExpected) throw new InvalidOperationException($"ETL job for run {runId:D} left the finishable state mid-transaction.");
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            return new EtlRunFinalizeOutcome.Finalized();
        }

        await ExecuteAsync(connection, transaction, "ROLLBACK TO SAVEPOINT etl_finalize_cas; RELEASE SAVEPOINT etl_finalize_cas;", cancellationToken).ConfigureAwait(false);
        var message = $"Watermark CAS failed for entity '{conflictEntity}' ({conflictCode}); all watermark changes for the run were rolled back.";
        await CommitBlockedRunAsync(connection, transaction, runId, conflictCode, message, now, cancellationToken).ConfigureAwait(false);
        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        return new EtlRunFinalizeOutcome.Blocked(conflictCode, message);
    }

    /// <inheritdoc cref="IAgentStore.MarkRunCompletionRetryAsync"/>
    public async Task<EtlRunCompletionRetryOutcome> MarkRunCompletionRetryAsync(Guid runId, Guid claimId, string errorMessage, DateTimeOffset nextAttemptAtUtc, int maxAttempts, CancellationToken cancellationToken)
    {
        if (claimId == Guid.Empty) throw new ArgumentException("A claim identity is required.", nameof(claimId));
        ArgumentOutOfRangeException.ThrowIfLessThan(maxAttempts, 1);

        await using var connection = await factory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        var now = UtcNow();

        var released = await ExecuteAsync(connection, transaction, """
            UPDATE etl_runs SET completion_claim_id=NULL, completion_claim_owner_id=NULL, completion_claim_acquired_at_utc=NULL,
                next_completion_attempt_at_utc=$next, last_error=$error, updated_at_utc=$now, row_version=row_version+1
            WHERE run_id=$run AND status='completing' AND completion_claim_id=$claim;
            """, cancellationToken,
            ("$next", nextAttemptAtUtc.ToUniversalTime().ToString("O")), ("$error", errorMessage), ("$now", now),
            ("$run", runId.ToString("D")), ("$claim", claimId.ToString("D"))).ConfigureAwait(false);
        if (released == 0)
        {
            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
            return new EtlRunCompletionRetryOutcome.ClaimLost();
        }

        // The durable bound persisted by the first claim is authoritative — a retry call
        // presenting a different policy limit can never widen it.
        long attempts = 0;
        long? persistedBound = null;
        await using (var read = connection.CreateCommand())
        {
            read.Transaction = transaction;
            read.CommandText = "SELECT completion_attempt_count,completion_max_attempts FROM etl_runs WHERE run_id=$run;";
            Add(read, "$run", runId.ToString("D"));
            await using var reader = await read.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                attempts = reader.GetInt64(0);
                persistedBound = NullableLong(reader, 1);
            }
        }
        if (attempts >= (persistedBound ?? maxAttempts))
        {
            const string message = "Run completion attempts exhausted; the stored payload and evidence are preserved for manual resolution.";
            await CommitBlockedRunAsync(connection, transaction, runId, "COMPLETION_ATTEMPTS_EXHAUSTED", message, now, cancellationToken).ConfigureAwait(false);
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            return new EtlRunCompletionRetryOutcome.Blocked();
        }

        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        return new EtlRunCompletionRetryOutcome.Scheduled();
    }

    /// <inheritdoc cref="IAgentStore.RecoverInterruptedEtlRunsAsync"/>
    public async Task<EtlRecoveryResult> RecoverInterruptedEtlRunsAsync(CancellationToken cancellationToken)
    {
        // Explicit startup-only recovery for the new path (exclusive-host precondition —
        // never run while another live agent could hold a claim). RecoverAsync does NOT
        // call this; there is deliberately no time-based live-claim stealing.
        await using var connection = await factory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        var now = UtcNow();
        var claims = await ExecuteAsync(connection, transaction,
            "UPDATE etl_runs SET completion_claim_id=NULL, completion_claim_owner_id=NULL, completion_claim_acquired_at_utc=NULL, updated_at_utc=$now, row_version=row_version+1 WHERE status='completing' AND completion_claim_id IS NOT NULL;",
            cancellationToken, ("$now", now)).ConfigureAwait(false);
        // O2 fail-closed upload quarantine BEFORE the interruption pass (design §7).
        // The set is decided by the ledger as well as the status, and fixed before any
        // write: every 'uploading' batch (a legacy pre-008 one has no ledger row — the
        // in-flight status itself is the unknown outcome) plus every due batch whose
        // ledger holds a non-precheck attempt. The latter covers the production
        // startup order, where legacy RecoverAsync has already reset 'uploading' to
        // 'ready'. A batch is NEVER returned to 'ready' on this path.
        await ExecuteAsync(connection, transaction, """
            CREATE TEMP TABLE IF NOT EXISTS o2_recovery_batches(batch_id TEXT PRIMARY KEY);
            DELETE FROM temp.o2_recovery_batches;
            INSERT INTO temp.o2_recovery_batches(batch_id)
            SELECT b.batch_id FROM etl_batches b
            WHERE b.status='uploading'
               OR (b.status IN ('creating','ready','retry_waiting')
                   AND EXISTS (SELECT 1 FROM etl_batch_send_attempts x WHERE x.batch_id=b.batch_id AND x.outcome <> 'precheck_failed'));
            """, cancellationToken).ConfigureAwait(false);
        // Every still-'admitted' attempt had a dead owner — orphan it (terminal, never
        // re-armed).
        var orphanedAttempts = await ExecuteAsync(connection, transaction,
            "UPDATE etl_batch_send_attempts SET outcome='orphaned', finished_at_utc=$now, last_error='UPLOAD_OUTCOME_UNKNOWN: admitted send recorded no outcome before process recovery' WHERE outcome='admitted';",
            cancellationToken, ("$now", now)).ConfigureAwait(false);
        var quarantinedBatches = await ExecuteAsync(connection, transaction,
            "UPDATE etl_batches SET status='dead_letter', quarantine_code='UPLOAD_OUTCOME_UNKNOWN', last_error='A send was in flight or its outcome was unknown when the host stopped; the remote outcome is unknowable.', row_version=row_version+1 WHERE batch_id IN (SELECT batch_id FROM temp.o2_recovery_batches);",
            cancellationToken).ConfigureAwait(false);
        // Only runs owning a batch quarantined in THIS pass block (historical
        // quarantine evidence never re-blocks a run); ownership + bindings RETAINED.
        const string recoveredRunFilter = "status IN ('pending','running','paused','uploading','completing') AND run_id IN (SELECT eb.run_id FROM etl_batches eb WHERE eb.batch_id IN (SELECT batch_id FROM temp.o2_recovery_batches))";
        var orphanClaims = await ScalarLongAsync(connection, transaction,
            $"SELECT COUNT(*) FROM etl_runs WHERE extraction_claim_id IS NOT NULL AND {recoveredRunFilter};",
            cancellationToken).ConfigureAwait(false);
        var orphanRuns = await ExecuteAsync(connection, transaction,
            $"UPDATE etl_runs SET status='blocked', finalize_conflict_code='UPLOAD_OUTCOME_UNKNOWN', finalize_conflict_message='A batch send was in flight when the host stopped; the remote outcome is unknowable — manual resolution required.', last_error='A batch send was in flight when the host stopped; the remote outcome is unknowable — manual resolution required.', finished_at_utc=$now, extraction_claim_id=NULL, extraction_claim_owner_id=NULL, extraction_claim_acquired_at_utc=NULL, completion_claim_id=NULL, completion_claim_owner_id=NULL, completion_claim_acquired_at_utc=NULL, updated_at_utc=$now, row_version=row_version+1 WHERE {recoveredRunFilter};",
            cancellationToken, ("$now", now)).ConfigureAwait(false);
        await ExecuteAsync(connection, transaction, "DROP TABLE temp.o2_recovery_batches;", cancellationToken).ConfigureAwait(false);
        claims += (int)orphanClaims;
        // Dead extraction claims die with the interrupted run — cleared inside the same
        // block write and counted as released claims. Ownership rows and bindings are
        // RETAINED: they are the evidence of exactly what the interrupted run owned.
        var extractionClaims = await ScalarLongAsync(connection, transaction,
            "SELECT COUNT(*) FROM etl_runs WHERE status='running' AND extraction_claim_id IS NOT NULL;",
            cancellationToken).ConfigureAwait(false);
        var runs = await ExecuteAsync(connection, transaction,
            "UPDATE etl_runs SET status='blocked', finalize_conflict_code='INTERRUPTED_NO_CHECKPOINT', finalize_conflict_message='Extraction interrupted before seal; manual resolution or A04 resume required.', finished_at_utc=$now, extraction_claim_id=NULL, extraction_claim_owner_id=NULL, extraction_claim_acquired_at_utc=NULL, updated_at_utc=$now, row_version=row_version+1 WHERE status='running';",
            cancellationToken, ("$now", now)).ConfigureAwait(false);
        claims += (int)extractionClaims;
        var entities = await ExecuteAsync(connection, transaction,
            "UPDATE etl_run_entities SET status='failed', last_error='RUN_INTERRUPTED', updated_at_utc=$now, row_version=row_version+1 WHERE status='extracting' AND run_id IN (SELECT run_id FROM etl_runs WHERE status='blocked' AND finalize_conflict_code IN ('INTERRUPTED_NO_CHECKPOINT','UPLOAD_OUTCOME_UNKNOWN'));",
            cancellationToken, ("$now", now)).ConfigureAwait(false);
        var batches = await ExecuteAsync(connection, transaction,
            "UPDATE etl_batches SET status='dead_letter', quarantine_code='RUN_BLOCKED', last_error='RUN_INTERRUPTED', row_version=row_version+1 WHERE status IN ('creating','ready','retry_waiting','uploading') AND run_id IN (SELECT run_id FROM etl_runs WHERE status='blocked' AND finalize_conflict_code IN ('INTERRUPTED_NO_CHECKPOINT','UPLOAD_OUTCOME_UNKNOWN'));",
            cancellationToken).ConfigureAwait(false);
        var jobs = await ExecuteAsync(connection, transaction,
            "UPDATE etl_jobs SET status='blocked', updated_at_utc=$now, row_version=row_version+1 WHERE status NOT IN ('finished','cancelled','blocked') AND run_id IN (SELECT run_id FROM etl_runs WHERE status='blocked' AND finalize_conflict_code IN ('INTERRUPTED_NO_CHECKPOINT','UPLOAD_OUTCOME_UNKNOWN'));",
            cancellationToken, ("$now", now)).ConfigureAwait(false);
        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        return new EtlRecoveryResult(claims, runs + orphanRuns, batches + quarantinedBatches, entities, jobs, orphanedAttempts);
    }

    // ---------- shared internals ----------

    private enum ClaimReadiness { Ready, Transient, Legacy, Violated, OwnershipMismatch }

    private sealed record RunRow(
        string Status,
        string? RequestedEntitiesJson,
        string? SealedAtUtc,
        long? SealedEntityCount,
        long? SealedExpectedBatchCount,
        string? CompletePayloadJson,
        long RowsRead,
        long BatchesCreated,
        long BatchesAcknowledged,
        long CompletionAttemptCount);

    private sealed record EntityRow(
        string EntityName,
        string Status,
        string DomainFingerprint,
        string? FinalWatermarkJson,
        long? ExpectedBatchCount,
        long RowsRead,
        long BatchesCreated,
        long BaseRowPresent,
        long? ExpectedBaseGeneration,
        string? ExpectedBaseCursorJson,
        string? ExpectedBaseDomainFingerprint,
        string DomainStatus);

    private sealed record BatchAggregate(long Count, long RowSum);

    private static async Task<RunRow?> ReadRunRowAsync(SqliteConnection connection, SqliteTransaction transaction, Guid runId, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "SELECT status,requested_entities_json,sealed_at_utc,sealed_entity_count,sealed_expected_batch_count,complete_payload_json,rows_read,batches_created,batches_acknowledged,completion_attempt_count FROM etl_runs WHERE run_id=$run;";
        Add(command, "$run", runId.ToString("D"));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        return await reader.ReadAsync(cancellationToken).ConfigureAwait(false)
            ? new RunRow(reader.GetString(0), NullableString(reader, 1), NullableString(reader, 2), NullableLong(reader, 3), NullableLong(reader, 4), NullableString(reader, 5), reader.GetInt64(6), reader.GetInt64(7), reader.GetInt64(8), reader.GetInt64(9))
            : null;
    }

    private static async Task<List<EntityRow>> ReadEntityRowsAsync(SqliteConnection connection, SqliteTransaction transaction, Guid runId, CancellationToken cancellationToken)
    {
        var entities = new List<EntityRow>();
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "SELECT entity_name,status,domain_fingerprint,final_watermark_json,expected_batch_count,rows_read,batches_created,base_row_present,expected_base_generation,expected_base_cursor_json,expected_base_domain_fingerprint,domain_status FROM etl_run_entities WHERE run_id=$run;";
        Add(command, "$run", runId.ToString("D"));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            entities.Add(new EntityRow(reader.GetString(0), reader.GetString(1), reader.GetString(2), NullableString(reader, 3), NullableLong(reader, 4), reader.GetInt64(5), reader.GetInt64(6), reader.GetInt64(7), NullableLong(reader, 8), NullableString(reader, 9), NullableString(reader, 10), reader.GetString(11)));
        }
        return entities;
    }

    private static async Task<Dictionary<string, BatchAggregate>> ReadBatchAggregatesAsync(SqliteConnection connection, SqliteTransaction transaction, Guid runId, CancellationToken cancellationToken)
    {
        var aggregates = new Dictionary<string, BatchAggregate>(StringComparer.Ordinal);
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "SELECT entity_name, COUNT(*), COALESCE(SUM(row_count),0) FROM etl_batches WHERE run_id=$run GROUP BY entity_name;";
        Add(command, "$run", runId.ToString("D"));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            aggregates[reader.GetString(0)] = new BatchAggregate(reader.GetInt64(1), reader.GetInt64(2));
        }
        return aggregates;
    }

    private static EtlRunSealRejection? ValidateSealedEntity(EntityRow entity, Dictionary<string, BatchAggregate> batchAggregates)
    {
        if (!string.Equals(entity.Status, "done", StringComparison.Ordinal)) return EtlRunSealRejection.EntityNotDone;
        if (!IsValidCursorJson(entity.FinalWatermarkJson, requireMeaningfulComponent: true)) return EtlRunSealRejection.FinalWatermarkInvalid;
        if (entity.ExpectedBatchCount is null or < 1) return EtlRunSealRejection.ExpectedBatchCountMismatch;
        var aggregate = batchAggregates.GetValueOrDefault(entity.EntityName) ?? new BatchAggregate(0, 0);
        if (aggregate.Count != entity.ExpectedBatchCount.Value || entity.BatchesCreated != entity.ExpectedBatchCount.Value || aggregate.RowSum != entity.RowsRead)
            return EtlRunSealRejection.ExpectedBatchCountMismatch;
        return null;
    }

    // Why a write-first fence rejected: the run is not mutable (missing/not running/
    // sealed), the live extraction claim differs/absent, or the exact ownership set no
    // longer holds — the ownership predicate is RE-EVALUATED here, never assumed, so an
    // entity-level miss is not misreported as an ownership failure. Passed means every
    // fence term held; the guarded write could then only have failed on the entity arm.
    // All classification reads happen inside the rejected transaction — nothing was
    // written.
    private enum FenceFailure { NotMutable, ClaimLost, OwnershipMismatch, Passed }

    private static async Task<FenceFailure> ClassifyRunFenceAsync(SqliteConnection connection, SqliteTransaction transaction, Guid runId, string claimText, CancellationToken cancellationToken)
    {
        string? status = null;
        string? sealedAt = null;
        string? liveClaim = null;
        await using (var read = connection.CreateCommand())
        {
            read.Transaction = transaction;
            read.CommandText = "SELECT status,sealed_at_utc,extraction_claim_id FROM etl_runs WHERE run_id=$run;";
            Add(read, "$run", runId.ToString("D"));
            await using var reader = await read.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                status = reader.GetString(0);
                sealedAt = NullableString(reader, 1);
                liveClaim = NullableString(reader, 2);
            }
        }
        if (!string.Equals(status, "running", StringComparison.Ordinal) || sealedAt is not null)
            return FenceFailure.NotMutable;
        if (!string.Equals(liveClaim, claimText, StringComparison.Ordinal))
            return FenceFailure.ClaimLost;
        return await RunOwnershipSetHoldsAsync(connection, transaction, runId, cancellationToken).ConfigureAwait(false)
            ? FenceFailure.Passed
            : FenceFailure.OwnershipMismatch;
    }

    private static async Task<EtlEntityBeginRejection> ClassifyRunFenceRejectionAsync(SqliteConnection connection, SqliteTransaction transaction, Guid runId, string claimText, CancellationToken cancellationToken) =>
        await ClassifyRunFenceAsync(connection, transaction, runId, claimText, cancellationToken).ConfigureAwait(false) switch
        {
            FenceFailure.NotMutable => EtlEntityBeginRejection.RunNotAcceptingEntities,
            FenceFailure.ClaimLost => EtlEntityBeginRejection.ExtractionClaimLost,
            // Passed is unreachable here — the begin fence IS status+claim+ownership — but
            // a rejected write with every term holding is evidence-level, never success.
            _ => EtlEntityBeginRejection.OwnershipSetMismatch,
        };

    private static async Task<EtlRunSealRejection> ClassifySealFenceRejectionAsync(SqliteConnection connection, SqliteTransaction transaction, Guid runId, string claimText, CancellationToken cancellationToken) =>
        await ClassifyRunFenceAsync(connection, transaction, runId, claimText, cancellationToken).ConfigureAwait(false) switch
        {
            FenceFailure.NotMutable => EtlRunSealRejection.RunNotRunningOrAlreadySealed,
            FenceFailure.ClaimLost => EtlRunSealRejection.ExtractionClaimLost,
            _ => EtlRunSealRejection.OwnershipSetMismatch,
        };

    private static async Task<EtlBatchRegistrationRejection> ClassifyBatchRejectionAsync(SqliteConnection connection, SqliteTransaction transaction, Guid runId, string entityName, string claimText, CancellationToken cancellationToken)
    {
        var fence = await ClassifyRunFenceAsync(connection, transaction, runId, claimText, cancellationToken).ConfigureAwait(false);
        if (fence == FenceFailure.NotMutable) return EtlBatchRegistrationRejection.RunNotAcceptingBatches;
        if (fence == FenceFailure.ClaimLost) return EtlBatchRegistrationRejection.ExtractionClaimLost;
        if (fence == FenceFailure.OwnershipMismatch) return EtlBatchRegistrationRejection.OwnershipSetMismatch;
        var entityStatus = await ScalarStringAsync(connection, transaction,
            "SELECT status FROM etl_run_entities WHERE run_id=$run AND entity_name=$entity;",
            cancellationToken, ("$run", runId.ToString("D")), ("$entity", entityName)).ConfigureAwait(false);
        if (!string.Equals(entityStatus, "extracting", StringComparison.Ordinal)
            || !await EntityOwnershipHoldsAsync(connection, transaction, runId, entityName, cancellationToken).ConfigureAwait(false))
            return EtlBatchRegistrationRejection.EntityNotExtracting;
        return EtlBatchRegistrationRejection.RunNotAcceptingBatches;
    }

    // The touched entity must itself be a manifest member with an epoch-bound active
    // ownership row for this run — a stray injected 'extracting' row is not a
    // legitimate extraction target even while the run's full ownership set holds.
    private static async Task<bool> EntityOwnershipHoldsAsync(SqliteConnection connection, SqliteTransaction transaction, Guid runId, string entityName, CancellationToken cancellationToken) =>
        await ScalarLongAsync(connection, transaction,
            "SELECT COUNT(*) FROM etl_entity_ownership eo JOIN etl_run_ownership_bindings eb ON eb.run_id = eo.owner_run_id AND eb.entity_name = eo.entity_name WHERE eo.entity_name=$entity AND eo.owner_run_id=$run AND eo.released_at_utc IS NULL AND eb.expected_epoch > 0 AND eo.ownership_epoch = eb.expected_epoch;",
            cancellationToken, ("$run", runId.ToString("D")), ("$entity", entityName)).ConfigureAwait(false) == 1;

    private static async Task<EtlEntityCompletionRejection> ClassifyEntityCompletionRejectionAsync(SqliteConnection connection, SqliteTransaction transaction, Guid runId, string entityName, int expectedBatchCount, string claimText, CancellationToken cancellationToken)
    {
        // Entity state outranks run/claim/ownership classification — same precedence the
        // pre-O1 classifier used (a 'done' entity on a sealed run is EntityNotExtracting).
        string? entityStatus = null;
        long? entityBatches = null;
        await using (var read = connection.CreateCommand())
        {
            read.Transaction = transaction;
            read.CommandText = "SELECT status,batches_created FROM etl_run_entities WHERE run_id=$run AND entity_name=$entity;";
            Add(read, "$run", runId.ToString("D")); Add(read, "$entity", entityName);
            await using var reader = await read.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                entityStatus = reader.GetString(0);
                entityBatches = reader.GetInt64(1);
            }
        }
        if (!string.Equals(entityStatus, "extracting", StringComparison.Ordinal)) return EtlEntityCompletionRejection.EntityNotExtracting;
        var fence = await ClassifyRunFenceAsync(connection, transaction, runId, claimText, cancellationToken).ConfigureAwait(false);
        if (fence == FenceFailure.NotMutable) return EtlEntityCompletionRejection.RunNotAcceptingEntities;
        if (fence == FenceFailure.ClaimLost) return EtlEntityCompletionRejection.ExtractionClaimLost;
        if (fence == FenceFailure.OwnershipMismatch) return EtlEntityCompletionRejection.OwnershipSetMismatch;
        if (!await EntityOwnershipHoldsAsync(connection, transaction, runId, entityName, cancellationToken).ConfigureAwait(false))
            return EtlEntityCompletionRejection.EntityNotExtracting;
        return entityBatches == expectedBatchCount ? EtlEntityCompletionRejection.RunNotAcceptingEntities : EtlEntityCompletionRejection.BatchCountMismatch;
    }

    private async Task<EtlRunTerminationOutcome> TerminateRunAsync(Guid runId, Guid extractionClaimId, string status, string? conflictCode, string message, string batchError, CancellationToken cancellationToken)
    {
        if (extractionClaimId == Guid.Empty) throw new ArgumentException("An extraction claim identity is required.", nameof(extractionClaimId));
        await using var connection = await factory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        var now = UtcNow();
        var claimText = extractionClaimId.ToString("D");
        // Guarded on 'running' AND the live extraction claim: fail/block can never
        // mutate a sealed, completing, or terminal run, and a stale/foreign claim can
        // never terminate a newer execution. The ownership set is deliberately NOT
        // part of this gate — termination with the current claim preserves/blocks a
        // run whose ownership evidence is corrupted; it never releases ownership.
        // Fencing pending batches bounds FUTURE local dispatch only — a batch mid-POST
        // can still land at ERP; the status flip cannot recall it.
        var changed = await ExecuteAsync(connection, transaction,
            "UPDATE etl_runs SET status=$status, finished_at_utc=$now, last_error=$message, error_count=error_count+1, finalize_conflict_code=$code, finalize_conflict_message=$message, extraction_claim_id=NULL, extraction_claim_owner_id=NULL, extraction_claim_acquired_at_utc=NULL, updated_at_utc=$now, row_version=row_version+1 WHERE run_id=$run AND status='running' AND extraction_claim_id=$claim;",
            cancellationToken,
            ("$status", status), ("$now", now), ("$message", message), ("$code", conflictCode), ("$run", runId.ToString("D")), ("$claim", claimText)).ConfigureAwait(false);
        if (changed == 0)
        {
            // Zero writes either way: distinguish a stale/foreign/absent claim on a
            // still-running run from a run that is not running at all.
            var stillRunning = await ScalarLongAsync(connection, transaction,
                "SELECT COUNT(*) FROM etl_runs WHERE run_id=$run AND status='running' AND extraction_claim_id IS NOT NULL AND extraction_claim_id <> $claim;",
                cancellationToken, ("$run", runId.ToString("D")), ("$claim", claimText)).ConfigureAwait(false);
            var claimCleared = await ScalarLongAsync(connection, transaction,
                "SELECT COUNT(*) FROM etl_runs WHERE run_id=$run AND status='running' AND extraction_claim_id IS NULL;",
                cancellationToken, ("$run", runId.ToString("D"))).ConfigureAwait(false);
            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
            return new EtlRunTerminationOutcome.Rejected(
                stillRunning != 0 || claimCleared != 0 ? EtlRunTerminationRejection.ExtractionClaimLost : EtlRunTerminationRejection.RunNotRunning);
        }

        await ExecuteAsync(connection, transaction,
            "UPDATE etl_run_entities SET status='failed', last_error=$error, updated_at_utc=$now, row_version=row_version+1 WHERE run_id=$run AND status='extracting';",
            cancellationToken, ("$error", conflictCode ?? message), ("$now", now), ("$run", runId.ToString("D"))).ConfigureAwait(false);
        await ExecuteAsync(connection, transaction,
            "UPDATE etl_batches SET status='dead_letter', quarantine_code=$error, last_error=$error, row_version=row_version+1 WHERE run_id=$run AND status IN ('creating','ready','retry_waiting','uploading');",
            cancellationToken, ("$error", batchError), ("$run", runId.ToString("D"))).ConfigureAwait(false);
        // Unresolved work stays unresolved: the job is blocked, never finished — job
        // 'finished' is written only by a successful finalize.
        await ExecuteAsync(connection, transaction,
            "UPDATE etl_jobs SET status='blocked', updated_at_utc=$now, row_version=row_version+1 WHERE run_id=$run AND status NOT IN ('finished','cancelled','blocked');",
            cancellationToken, ("$now", now), ("$run", runId.ToString("D"))).ConfigureAwait(false);
        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        return new EtlRunTerminationOutcome.Applied();
    }

    private static async Task<ClaimReadiness> VerifyClaimReadinessAsync(SqliteConnection connection, SqliteTransaction transaction, Guid runId, RunRow run, CancellationToken cancellationToken)
    {
        // Unsealed 'uploading' runs (or sealed runs with no entity rows) are legacy:
        // their batch graphs cannot prove original watermark generations or frozen
        // definitions — block and preserve, never derive bases.
        if (run.SealedAtUtc is null) return ClaimReadiness.Legacy;
        var requested = ParseRequestedEntities(run.RequestedEntitiesJson);
        if (requested is null) return ClaimReadiness.Violated;
        var entities = await ReadEntityRowsAsync(connection, transaction, runId, cancellationToken).ConfigureAwait(false);
        if (entities.Count == 0) return ClaimReadiness.Legacy;
        if (entities.Count != requested.Count || entities.Any(entity => !requested.Contains(entity.EntityName))) return ClaimReadiness.Violated;
        if (run.SealedEntityCount != entities.Count) return ClaimReadiness.Violated;

        var batchAggregates = await ReadBatchAggregatesAsync(connection, transaction, runId, cancellationToken).ConfigureAwait(false);
        var expectedTotal = 0;
        var rowsTotal = 0L;
        foreach (var entity in entities)
        {
            if (!string.Equals(entity.Status, "done", StringComparison.Ordinal)) return ClaimReadiness.Violated;
            if (!IsValidCursorJson(entity.FinalWatermarkJson, requireMeaningfulComponent: true)) return ClaimReadiness.Violated;
            if (entity.ExpectedBatchCount is null or < 1) return ClaimReadiness.Violated;
            // Frozen per-entity counters must equal the actual batch rows: count AND the
            // summed row volume — a tampered rows_read or phantom batch is a violation.
            var aggregate = batchAggregates.GetValueOrDefault(entity.EntityName) ?? new BatchAggregate(0, 0);
            if (aggregate.Count != entity.ExpectedBatchCount.Value || entity.BatchesCreated != entity.ExpectedBatchCount.Value || aggregate.RowSum != entity.RowsRead)
                return ClaimReadiness.Violated;
            if (!ExpectedBaseShapeIsValid(entity)) return ClaimReadiness.Violated;
            expectedTotal += (int)entity.ExpectedBatchCount.Value;
            rowsTotal += entity.RowsRead;
        }
        if (run.SealedExpectedBatchCount != expectedTotal) return ClaimReadiness.Violated;
        if (run.RowsRead != rowsTotal) return ClaimReadiness.Violated;

        // Completion/finalize require the exact three-way ownership equality in the same
        // transaction BEFORE the transient in-flight check: a run whose manifest no
        // longer equals its bindings/active ownership rows at the bound positive epochs
        // is corrupt durable evidence — it must block OWNERSHIP_SET_MISMATCH (and its
        // still-pending batches are dead-lettered by the block), never wait as
        // 'transient' while broken. Missing, extra, released, foreign or wrong-epoch
        // ownership is a violation — never silently released or adopted.
        if (!await RunOwnershipSetHoldsAsync(connection, transaction, runId, cancellationToken).ConfigureAwait(false))
            return ClaimReadiness.OwnershipMismatch;

        var nonAcknowledged = await ScalarLongAsync(connection, transaction,
            "SELECT COUNT(*) FROM etl_batches WHERE run_id=$run AND status <> 'acknowledged';",
            cancellationToken, ("$run", runId.ToString("D"))).ConfigureAwait(false);
        var deadBatches = await ScalarLongAsync(connection, transaction,
            "SELECT COUNT(*) FROM etl_batches WHERE run_id=$run AND status IN ('dead_letter','deleted');",
            cancellationToken, ("$run", runId.ToString("D"))).ConfigureAwait(false);
        if (deadBatches != 0) return ClaimReadiness.Violated;
        if (nonAcknowledged != 0) return ClaimReadiness.Transient;

        var total = await ScalarLongAsync(connection, transaction,
            "SELECT COUNT(*) FROM etl_batches WHERE run_id=$run;",
            cancellationToken, ("$run", runId.ToString("D"))).ConfigureAwait(false);
        if (total != expectedTotal || run.BatchesCreated != expectedTotal || run.BatchesAcknowledged != expectedTotal) return ClaimReadiness.Violated;

        return ClaimReadiness.Ready;
    }

    // Returns NULL when the seal+ownership evidence is intact, otherwise the durable
    // conflict code to block with (SEAL_VIOLATED or OWNERSHIP_SET_MISMATCH).
    private static async Task<string?> VerifyFinalSealAsync(SqliteConnection connection, SqliteTransaction transaction, Guid runId, RunRow run, CancellationToken cancellationToken)
    {
        // The immutable payload must still be the exact stored body for THIS run — the
        // claim fence alone never proves the payload was not replaced behind the API.
        if (run.CompletePayloadJson is null || !IsValidCompletePayload(run.CompletePayloadJson, runId, run)) return "SEAL_VIOLATED";
        var readiness = await VerifyClaimReadinessAsync(connection, transaction, runId, run, cancellationToken).ConfigureAwait(false);
        // A transient gap at finalize means the acknowledged evidence changed after the
        // claim — that is a violation of the claimed state, not a reason to wait.
        return readiness switch
        {
            ClaimReadiness.Ready => null,
            ClaimReadiness.OwnershipMismatch => "OWNERSHIP_SET_MISMATCH",
            _ => "SEAL_VIOLATED",
        };
    }

    // Classification after a lost claim write: only a released, due 'completing' run
    // whose stored attempt count already reached its durable bound is durably blocked —
    // every other not-claimable state (live claim, not due, wrong status, mismatched
    // policy limit) changes nothing.
    private static async Task<bool> IsAttemptBoundExhaustedAsync(SqliteConnection connection, SqliteTransaction transaction, Guid runId, int requestedMaxAttempts, string now, CancellationToken cancellationToken)
    {
        string? status = null;
        string? claimId = null;
        string? nextAttempt = null;
        long attempts = 0;
        long? bound = null;
        await using (var read = connection.CreateCommand())
        {
            read.Transaction = transaction;
            read.CommandText = "SELECT status,completion_claim_id,next_completion_attempt_at_utc,completion_attempt_count,completion_max_attempts FROM etl_runs WHERE run_id=$run;";
            Add(read, "$run", runId.ToString("D"));
            await using var reader = await read.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                status = reader.GetString(0);
                claimId = NullableString(reader, 1);
                nextAttempt = NullableString(reader, 2);
                attempts = reader.GetInt64(3);
                bound = NullableLong(reader, 4);
            }
        }
        return string.Equals(status, "completing", StringComparison.Ordinal)
            && claimId is null
            && (nextAttempt is null || ParseDate(nextAttempt) <= ParseDate(now))
            && attempts >= (bound ?? requestedMaxAttempts);
    }

    // The captured expected base must still have a coherent shape before any CAS trusts
    // it: a present capture keeps a non-NULL generation and its matched 'same' domain
    // fingerprint, and a non-NULL raw base cursor must be a structurally valid stored
    // cursor (a present NULL cursor with a known domain is valid; a malformed non-NULL
    // base is not). An absent capture carries no base values at all.
    private static bool ExpectedBaseShapeIsValid(EntityRow entity)
    {
        if (entity.BaseRowPresent is not (0 or 1) || string.IsNullOrEmpty(entity.DomainFingerprint)) return false;
        if (entity.BaseRowPresent == 1)
        {
            return string.Equals(entity.DomainStatus, "same", StringComparison.Ordinal)
                && entity.ExpectedBaseGeneration is > 0
                && entity.ExpectedBaseDomainFingerprint is not null
                && string.Equals(entity.ExpectedBaseDomainFingerprint, entity.DomainFingerprint, StringComparison.Ordinal)
                && (entity.ExpectedBaseCursorJson is null || IsValidCursorJson(entity.ExpectedBaseCursorJson, requireMeaningfulComponent: false));
        }
        return string.Equals(entity.DomainStatus, "absent", StringComparison.Ordinal)
            && entity.ExpectedBaseGeneration is null
            && entity.ExpectedBaseCursorJson is null
            && entity.ExpectedBaseDomainFingerprint is null;
    }

    // Full schema/identity validation of the immutable completion body: an unambiguous
    // JSON object with EXACTLY the six written properties — runId equal to this run,
    // status 'succeeded', counters equal to the durable run counters, and a strict
    // timestamp. An arbitrary object, a foreign run's body, drifted counters or
    // duplicate names are corrupt evidence, never a replayable payload.
    private static readonly string[] CompletionPayloadProperties =
        ["runId", "status", "rowsRead", "batchesCreated", "batchesAcknowledged", "completedAtUtc"];

    private static bool IsValidCompletePayload(string payloadJson, Guid runId, RunRow run)
    {
        try
        {
            using var document = JsonDocument.Parse(payloadJson);
            var root = document.RootElement;
            if (root.ValueKind != JsonValueKind.Object) return false;
            JsonAmbiguityGuard.EnsureUnambiguous(root);
            var names = new HashSet<string>(StringComparer.Ordinal);
            foreach (var property in root.EnumerateObject()) names.Add(property.Name);
            if (!names.SetEquals(CompletionPayloadProperties)) return false;
            return root.GetProperty("runId").ValueKind == JsonValueKind.String
                && Guid.TryParse(root.GetProperty("runId").GetString(), out var payloadRunId)
                && payloadRunId == runId
                && root.GetProperty("status").ValueKind == JsonValueKind.String
                && string.Equals(root.GetProperty("status").GetString(), "succeeded", StringComparison.Ordinal)
                && TryReadInt64(root.GetProperty("rowsRead"), out var rowsRead) && rowsRead == run.RowsRead
                && TryReadInt64(root.GetProperty("batchesCreated"), out var created) && created == run.BatchesCreated
                && TryReadInt64(root.GetProperty("batchesAcknowledged"), out var acknowledged) && acknowledged == run.BatchesAcknowledged
                && root.GetProperty("completedAtUtc").ValueKind == JsonValueKind.String
                && root.GetProperty("completedAtUtc").TryGetDateTimeOffset(out _);
        }
        catch (Exception ex) when (ex is JsonException or InvalidDataException)
        {
            return false;
        }
    }

    private static bool TryReadInt64(JsonElement element, out long value)
    {
        value = 0;
        return element.ValueKind == JsonValueKind.Number && element.TryGetInt64(out value);
    }

    // Watermark-bearing extraction modes only: reconcile runs never produce watermark
    // reads, so they can never capture durable bases on this path.
    private static bool IsSupportedExtractionMode(string? mode) =>
        mode is "incremental" or "bootstrap_full" or "entity_reload";

    // The frozen definition must deserialize to a valid typed EtlEntityDefinition whose
    // EntityCode is the requested entity — the same structural invariants a durable job
    // acceptance requires of its resolved entities.
    private static bool TryReadEntityDefinition(string definitionJson, string entityName)
    {
        try
        {
            var definition = JsonSerializer.Deserialize<EtlEntityDefinition>(definitionJson, JsonOptions);
            return definition is not null
                && string.Equals(definition.EntityCode, entityName, StringComparison.Ordinal)
                && IsValidResolvedEntityDefinition(definition);
        }
        catch (Exception ex) when (ex is JsonException or NotSupportedException)
        {
            return false;
        }
    }

    private static async Task CommitBlockedRunAsync(SqliteConnection connection, SqliteTransaction transaction, Guid runId, string code, string message, string now, CancellationToken cancellationToken)
    {
        await ExecuteAsync(connection, transaction, """
            UPDATE etl_runs SET status='blocked', finished_at_utc=$now, last_error=$message,
                finalize_conflict_code=$code, finalize_conflict_message=$message,
                completion_claim_id=NULL, completion_claim_owner_id=NULL, completion_claim_acquired_at_utc=NULL,
                extraction_claim_id=NULL, extraction_claim_owner_id=NULL, extraction_claim_acquired_at_utc=NULL,
                updated_at_utc=$now, row_version=row_version+1
            WHERE run_id=$run;
            """, cancellationToken,
            ("$now", now), ("$message", message), ("$code", code), ("$run", runId.ToString("D"))).ConfigureAwait(false);
        await ExecuteAsync(connection, transaction,
            "UPDATE etl_jobs SET status='blocked', updated_at_utc=$now, row_version=row_version+1 WHERE run_id=$run AND status NOT IN ('finished','cancelled','blocked');",
            cancellationToken, ("$now", now), ("$run", runId.ToString("D"))).ConfigureAwait(false);
        // Fence FUTURE batch dispatch: any batch still pre-acknowledgement becomes
        // dead_letter inside the same commit with the RUN_BLOCKED quarantine code (D3).
        // This claims nothing and cannot recall an in-flight HTTP upload — it only
        // guarantees no new local send is dispatched for a blocked run. Ownership
        // rows/bindings are retained.
        await ExecuteAsync(connection, transaction,
            "UPDATE etl_batches SET status='dead_letter', quarantine_code='RUN_BLOCKED', last_error=$error, row_version=row_version+1 WHERE run_id=$run AND status IN ('creating','ready','retry_waiting','uploading');",
            cancellationToken, ("$error", code), ("$run", runId.ToString("D"))).ConfigureAwait(false);
    }

    private static async Task<string> ClassifyCasConflictAsync(SqliteConnection connection, SqliteTransaction transaction, EntityRow entity, CancellationToken cancellationToken)
    {
        bool present;
        long generation = 0;
        string? cursor = null;
        string? fingerprint = null;
        await using (var read = connection.CreateCommand())
        {
            read.Transaction = transaction;
            read.CommandText = "SELECT generation, committed_cursor_json, domain_fingerprint FROM watermarks WHERE entity_name=$entity;";
            Add(read, "$entity", entity.EntityName);
            await using var reader = await read.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            present = await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
            if (present)
            {
                generation = reader.GetInt64(0);
                cursor = NullableString(reader, 1);
                fingerprint = NullableString(reader, 2);
            }
        }

        if (entity.BaseRowPresent == 0)
            return "ROW_UNEXPECTEDLY_PRESENT";
        if (!present)
            return "ROW_VANISHED";
        if (generation != entity.ExpectedBaseGeneration)
            return "GENERATION_MISMATCH"; // covers ABA and same-cursor commits: generation moved
        if (!string.Equals(cursor, entity.ExpectedBaseCursorJson, StringComparison.Ordinal))
            return "BASE_CURSOR_MISMATCH";
        if (!string.Equals(fingerprint, entity.ExpectedBaseDomainFingerprint, StringComparison.Ordinal))
            return "DOMAIN_MISMATCH";
        return "GENERATION_MISMATCH";
    }

    private static void ValidateExtractionRequest(EtlEntityExtractionRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (string.IsNullOrWhiteSpace(request.EntityName)) throw new ArgumentException("Entity name is required.", nameof(request));
        if (string.IsNullOrWhiteSpace(request.EntityDefinitionJson)) throw new ArgumentException("The frozen entity definition is required.", nameof(request));
        if (string.IsNullOrWhiteSpace(request.QueryMode)) throw new ArgumentException("The query mode is required.", nameof(request));
        try
        {
            using var document = JsonDocument.Parse(request.EntityDefinitionJson);
            if (document.RootElement.ValueKind != JsonValueKind.Object)
                throw new InvalidDataException("not-object");
            JsonAmbiguityGuard.EnsureUnambiguous(document.RootElement);
        }
        catch (Exception ex) when (ex is JsonException or InvalidDataException)
        {
            throw new ArgumentException("The frozen entity definition must be an unambiguous JSON object.", nameof(request), ex);
        }
        if (request.SnapshotUpperBoundJson is not null && !IsValidCursorJson(request.SnapshotUpperBoundJson, requireMeaningfulComponent: true))
            throw new ArgumentException("The snapshot upper bound must be a cursor JSON object with at least one non-NULL component.", nameof(request));
    }

    // Canonical (hash) equality between the caller's frozen definition and the job's
    // stored entities_json element for this entity — a job-associated run can never
    // extract against a different effective definition.
    private static bool FrozenDefinitionMatches(string jobEntitiesJson, string entityName, string entityDefinitionJson)
    {
        try
        {
            using var job = JsonDocument.Parse(jobEntitiesJson);
            using var provided = JsonDocument.Parse(entityDefinitionJson);
            if (job.RootElement.ValueKind != JsonValueKind.Array) return false;
            var providedHash = PayloadHasher.Compute(provided.RootElement);
            foreach (var element in job.RootElement.EnumerateArray())
            {
                if (element.ValueKind != JsonValueKind.Object) continue;
                if (!element.TryGetProperty("entityCode", out var code) && !element.TryGetProperty("EntityCode", out code)) continue;
                if (!string.Equals(code.GetString(), entityName, StringComparison.Ordinal)) continue;
                return string.Equals(PayloadHasher.Compute(element), providedHash, StringComparison.Ordinal);
            }
            return false;
        }
        catch (JsonException)
        {
            return false;
        }
    }

    // Structural validation of a stored cursor through the SAME strict parser the
    // committed-watermark read path uses (System.Text.Json EtlCursor): a JSON object
    // whose only properties are 'updatedAtUtc' (absent/NULL/ISO 8601 timestamp — the
    // System.Text.Json DateTimeOffset grammar via JsonElement.TryGetDateTimeOffset,
    // never broad-culture formats) and 'sourceId' (absent/NULL/non-empty non-whitespace
    // string — composite-id strings included). Duplicate or unknown properties reject;
    // an empty/whitespace sourceId is invalid, never a component. With
    // requireMeaningfulComponent at least one component must be non-NULL, so '{}',
    // (null,null), '[1]', '"x"' and bad dates can never satisfy a final. Both valid
    // shapes — (timestamp,NULL) and (NULL,sourceId) — pass.
    private static bool IsValidCursorJson(string? json, bool requireMeaningfulComponent)
    {
        if (string.IsNullOrWhiteSpace(json)) return false;
        try
        {
            using var document = JsonDocument.Parse(json);
            if (document.RootElement.ValueKind != JsonValueKind.Object) return false;
            var names = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var components = 0;
            foreach (var property in document.RootElement.EnumerateObject())
            {
                if (!names.Add(property.Name)) return false;
                if (string.Equals(property.Name, "updatedAtUtc", StringComparison.OrdinalIgnoreCase))
                {
                    if (property.Value.ValueKind == JsonValueKind.Null) continue;
                    if (property.Value.ValueKind != JsonValueKind.String || !property.Value.TryGetDateTimeOffset(out _))
                        return false;
                    components++;
                }
                else if (string.Equals(property.Name, "sourceId", StringComparison.OrdinalIgnoreCase))
                {
                    if (property.Value.ValueKind == JsonValueKind.Null) continue;
                    if (property.Value.ValueKind != JsonValueKind.String || string.IsNullOrWhiteSpace(property.Value.GetString()))
                        return false;
                    components++;
                }
                else
                {
                    return false;
                }
            }
            return !requireMeaningfulComponent || components > 0;
        }
        catch (JsonException)
        {
            return false;
        }
    }

    private static async Task<long> ScalarLongAsync(SqliteConnection connection, SqliteTransaction transaction, string sql, CancellationToken cancellationToken, params (string Name, object? Value)[] parameters)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = sql;
        foreach (var parameter in parameters) Add(command, parameter.Name, parameter.Value);
        return Convert.ToInt64(await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false), System.Globalization.CultureInfo.InvariantCulture);
    }

    private static async Task<string?> ScalarStringAsync(SqliteConnection connection, SqliteTransaction transaction, string sql, CancellationToken cancellationToken, params (string Name, object? Value)[] parameters)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = sql;
        foreach (var parameter in parameters) Add(command, parameter.Name, parameter.Value);
        return await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) as string;
    }

    private static long? NullableLong(SqliteDataReader reader, int ordinal) => reader.IsDBNull(ordinal) ? null : reader.GetInt64(ordinal);
}
