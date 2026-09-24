using System.Text.Json;
using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Domain.Etl;
using Microsoft.Data.Sqlite;

namespace ErpOnecAgent.Infrastructure.Persistence.Sqlite;

// O1 DARK storage slice: durable per-entity lifetime ownership with immutable
// per-run epoch bindings, the run-level extraction claim fence, and transactional
// fair manual-job claim/enumeration. Nothing here is called by workers,
// RecoverAsync or the ERP client; O1 alone is not a safe production state — the
// send ledger is O2, scheduled runs are O3, and the legacy writers remain §9
// bypasses to be fenced atomically at cutover.
public sealed partial class SqliteAgentStore
{
    /// <inheritdoc cref="IAgentStore.TryClaimEtlJobAsync"/>
    public async Task<EtlJobClaimOutcome> TryClaimEtlJobAsync(Guid jobId, string ownerId, DateTimeOffset deferUntilUtc, DateTimeOffset nowUtc, CancellationToken cancellationToken)
    {
        ValidateOwner(ownerId);
        var now = nowUtc.ToUniversalTime().ToString("O");
        var deferUntil = deferUntilUtc.ToUniversalTime().ToString("O");

        await using var connection = await factory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);

        // Probe-guard write (serializes claimants on the row lock, no status change):
        // only a pending job or a due deferred job may enter the claim transaction.
        var probed = await ExecuteAsync(connection, transaction,
            "UPDATE etl_jobs SET row_version=row_version+1, updated_at_utc=$now WHERE job_id=$job AND (status='pending' OR (status='deferred' AND (available_at_utc IS NULL OR available_at_utc <= $now)));",
            cancellationToken, ("$now", now), ("$job", jobId.ToString("D"))).ConfigureAwait(false);
        if (probed == 0)
        {
            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
            return new EtlJobClaimOutcome.NotClaimable();
        }

        var job = await ReadJobRowAsync(connection, transaction, jobId, cancellationToken).ConfigureAwait(false);
        var run = job is null ? null : await ReadClaimRunRowAsync(connection, transaction, job.RunId, cancellationToken).ConfigureAwait(false);

        // Frozen job identity is enforced at claim time, not deferred to extraction: the
        // run must be pending with the same mode/configuration version, and the job's
        // frozen entities_json manifest must equal the run's requested_entities_json.
        // Corrupt durable evidence never dispatches — job + pending run block together.
        var claimantManifest = run is null ? null : ParseManifestOrdered(run.RequestedEntitiesJson);
        var consistent = job is not null && run is not null
            && string.Equals(run.Status, "pending", StringComparison.Ordinal)
            && FrozenJobIdentityConsistent(job.Mode, run.Mode, job.ConfigurationVersion, run.ConfigurationVersion, job.EntitiesJson, run.RequestedEntitiesJson);
        if (!consistent)
        {
            // The same conservative inert-evidence rule as elder quarantine applies to a
            // direct self-claim: provably-never-started corrupt evidence is quarantined
            // (job blocked + pending run blocked — the reservation is released because
            // nothing ever ran). With UNPROVEN durable effects (batches, entities,
            // claims, bindings, ownership, or a non-zero job claim history) the run must
            // keep its pending admission hold — silently retiring it would let a younger
            // overlapping claim bypass the hold — so only the job is diagnostically
            // blocked and the run stays pending forever until explicit resolution.
            const string inconsistentMessage = "Durable job/run identity is inconsistent (run not pending, mode/configuration drift, or frozen entities no longer equal the requested manifest); corrupt evidence never dispatches.";
            var inert = run is null || await RunProvablyInertAsync(connection, transaction, run.RunId, cancellationToken).ConfigureAwait(false);
            if (inert)
            {
                var (jobsBlocked, runsBlocked) = await CommitJobManifestBlockAsync(connection, transaction, jobId, job?.RunId, "JOB_MANIFEST_INCONSISTENT", inconsistentMessage, now, cancellationToken).ConfigureAwait(false);
                // Guarded expected-one transitions: the job was probed claimable at the
                // top of this transaction so its block MUST land; a still-'pending' run
                // MUST block too. A zero-row guard loss or a swallowed write
                // (RAISE(IGNORE)) aborts the whole claim transaction — a partial
                // quarantine is never committed. A run already past 'pending' keeps its
                // lifecycle (preserved evidence, never falsified).
                if (jobsBlocked != 1 || (run is not null && string.Equals(run.Status, "pending", StringComparison.Ordinal) && runsBlocked != 1))
                    throw new InvalidOperationException($"ETL job {jobId:D} manifest quarantine lost its guarded transition mid-transaction.");
            }
            else
            {
                // Diagnostic block ONLY — prior dispatch/claim evidence is preserved;
                // the blocked status also permanently prevents an inert classification: erasing dispatch_owner_id/dispatch_claimed_at_utc
                // (or any other effect) would make the NEXT claim pass evaluate the same
                // corrupt pending run as "provably inert" and silently release its
                // admission hold. Unresolved evidence keeps the hold forever.
                var jobBlocked = await ExecuteAsync(connection, transaction,
                    "UPDATE etl_jobs SET status='blocked', deferral_message=$message, available_at_utc=NULL, updated_at_utc=$now, row_version=row_version+1 WHERE job_id=$job AND status IN ('pending','deferred');",
                    cancellationToken, ("$job", jobId.ToString("D")), ("$message", $"JOB_MANIFEST_INCONSISTENT: {inconsistentMessage}"), ("$now", now)).ConfigureAwait(false);
                if (jobBlocked != 1) throw new InvalidOperationException($"ETL job {jobId:D} left the claimable state mid-transaction.");
            }
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            return new EtlJobClaimOutcome.Blocked("JOB_MANIFEST_INCONSISTENT", inconsistentMessage);
        }

        // Elder-manifest quarantine BEFORE overlap work (root disposition): every older
        // pending run's manifest is validated as a typed nonempty unique string array
        // consistent with its frozen job identity. A provably never-started corrupt elder
        // is quarantined (blocked, MANIFEST_INVALID) and leaves the reservation; an elder
        // with unproven effects keeps its reservation and the claimant defers
        // elder_manifest_invalid — unresolved evidence stops admission until explicit
        // resolution.
        var elders = await ReadElderRowsAsync(connection, transaction, run!.RunId, run.CreatedAtUtc, cancellationToken).ConfigureAwait(false);
        foreach (var elder in elders)
        {
            var elderManifest = ParseManifestOrdered(elder.ManifestJson);
            var elderInvalid = elderManifest is null
                || (elder.JobId is not null
                    && !FrozenJobIdentityConsistent(elder.JobMode, elder.RunMode, elder.JobConfigurationVersion, elder.RunConfigurationVersion, elder.JobEntitiesJson, elder.ManifestJson));
            if (!elderInvalid) continue;
            if (!elder.ProvablyInert)
            {
                var hold = $"An older pending run '{elder.RunId:D}' holds an invalid manifest with unproven effects; admission is held pending explicit resolution.";
                var held = await DeferJobAsync(connection, transaction, jobId, "elder_manifest_invalid", hold, deferUntil, now, cancellationToken).ConfigureAwait(false);
                if (!held) throw new InvalidOperationException($"ETL job {jobId:D} left the claimable state mid-transaction.");
                await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
                return new EtlJobClaimOutcome.Deferred(EtlJobDeferralReason.ElderManifestInvalid);
            }
            var quarantine = $"Pending run '{elder.RunId:D}' failed typed manifest validation with proof it never started; quarantined (MANIFEST_INVALID).";
            var (jobsBlocked, runsBlocked) = await CommitJobManifestBlockAsync(connection, transaction, elder.JobId, elder.RunId, "MANIFEST_INVALID", quarantine, now, cancellationToken).ConfigureAwait(false);
            // The elder was read 'pending' inside THIS transaction — its quarantine
            // transition is expected-one: a swallowed or zero-row write aborts the whole
            // claim transaction (never a partial quarantine that frees the reservation
            // while the run still stands). The job may legitimately be absent or already
            // terminal (0); a still-blockable job must transition.
            if (runsBlocked != 1)
                throw new InvalidOperationException($"Pending run {elder.RunId:D} failed its quarantine transition mid-transaction.");
            if (elder.JobId is not null && elder.JobStatus is not ("finished" or "cancelled" or "blocked") && jobsBlocked != 1)
                throw new InvalidOperationException($"ETL job {elder.JobId:D} left the blockable state mid-transaction.");
        }

        // Elder-overlap reservation inside the claim transaction (the enumerator can
        // never bypass it): no older pending run may overlap this manifest — a pending
        // elder whose job is terminally blocked still holds its reservation (that IS the
        // durable admission hold for unresolved effects). The order key is
        // (created_at_utc, run_id) — a deterministic total order, not arrival FIFO.
        var elderOverlap = await ScalarLongAsync(connection, transaction, """
            SELECT COUNT(*) FROM etl_runs r2
            WHERE r2.status='pending' AND r2.run_id <> $run
              AND (COALESCE(r2.created_at_utc,'') < $created OR (COALESCE(r2.created_at_utc,'') = $created AND r2.run_id < $run))
              AND json_valid(r2.requested_entities_json)
              AND EXISTS (SELECT 1 FROM json_each(CASE WHEN json_valid(r2.requested_entities_json) THEN r2.requested_entities_json ELSE '[]' END) e2
                          JOIN json_each($manifest) e1 ON e1.value = e2.value);
            """, cancellationToken,
            ("$run", run.RunId.ToString("D")), ("$created", run.CreatedAtUtc ?? string.Empty), ("$manifest", run.RequestedEntitiesJson)).ConfigureAwait(false);
        if (elderOverlap != 0)
        {
            const string overlapMessage = "An older pending run overlaps this manifest; the elder reserves its overlap before any newer claim.";
            var deferred = await DeferJobAsync(connection, transaction, jobId, "queued_overlap", overlapMessage, deferUntil, now, cancellationToken).ConfigureAwait(false);
            if (!deferred) throw new InvalidOperationException($"ETL job {jobId:D} left the claimable state mid-transaction.");
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            return new EtlJobClaimOutcome.Deferred(EtlJobDeferralReason.QueuedOverlap);
        }

        // All-entity ownership acquisition under a SAVEPOINT: the savepoint opens
        // immediately before the acquisition loop so every acquired/re-acquired row and
        // every binding insert is reversible independently of the deferral commit. Any
        // conflict rolls the whole partial acquisition back — a conflict on entity 2
        // leaves entity 1's prior owner untouched and entity 1 unowned by this run.
        await ExecuteAsync(connection, transaction, "SAVEPOINT etl_acquire;", cancellationToken).ConfigureAwait(false);
        var conflict = false;
        foreach (var entity in claimantManifest!.OrderBy(static entity => entity, StringComparer.Ordinal))
        {
            var reacquired = await ExecuteAsync(connection, transaction, """
                UPDATE etl_entity_ownership SET owner_run_id=$run, owner_job_id=$job, ownership_epoch=ownership_epoch+1,
                    acquired_at_utc=$now, released_at_utc=NULL, release_reason=NULL, updated_at_utc=$now, row_version=row_version+1
                WHERE entity_name=$entity AND released_at_utc IS NOT NULL;
                """, cancellationToken,
                ("$run", run.RunId.ToString("D")), ("$job", jobId.ToString("D")), ("$entity", entity), ("$now", now)).ConfigureAwait(false);
            if (reacquired == 0)
            {
                var inserted = await ExecuteAsync(connection, transaction, """
                    INSERT INTO etl_entity_ownership(entity_name,owner_run_id,owner_job_id,ownership_epoch,acquired_at_utc,updated_at_utc,row_version)
                    SELECT $entity,$run,$job,1,$now,$now,1
                    WHERE NOT EXISTS (SELECT 1 FROM etl_entity_ownership o WHERE o.entity_name=$entity);
                    """, cancellationToken,
                    ("$entity", entity), ("$run", run.RunId.ToString("D")), ("$job", jobId.ToString("D")), ("$now", now)).ConfigureAwait(false);
                if (inserted == 0) { conflict = true; break; }
            }

            // Immutable binding: the epoch actually committed is read back in-transaction,
            // never assumed (insert=1, reacquire=old+1). Zero rows means the just-written
            // ownership row is inconsistent — same fail-closed conflict path.
            var bound = await ExecuteAsync(connection, transaction, """
                INSERT INTO etl_run_ownership_bindings(run_id,entity_name,expected_epoch,acquired_at_utc)
                SELECT $run,$entity,o.ownership_epoch,$now FROM etl_entity_ownership o
                WHERE o.entity_name=$entity AND o.owner_run_id=$run AND o.released_at_utc IS NULL;
                """, cancellationToken,
                ("$run", run.RunId.ToString("D")), ("$entity", entity), ("$now", now)).ConfigureAwait(false);
            if (bound != 1) { conflict = true; break; }
        }

        if (conflict)
        {
            await ExecuteAsync(connection, transaction, "ROLLBACK TO SAVEPOINT etl_acquire; RELEASE SAVEPOINT etl_acquire;", cancellationToken).ConfigureAwait(false);
            const string busyMessage = "At least one manifest entity is owned by another active run; the partial acquisition was rolled back and the job is deferred.";
            var deferred = await DeferJobAsync(connection, transaction, jobId, "busy_entity", busyMessage, deferUntil, now, cancellationToken).ConfigureAwait(false);
            if (!deferred) throw new InvalidOperationException($"ETL job {jobId:D} left the claimable state mid-transaction.");
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            return new EtlJobClaimOutcome.Deferred(EtlJobDeferralReason.BusyEntity);
        }
        await ExecuteAsync(connection, transaction, "RELEASE SAVEPOINT etl_acquire;", cancellationToken).ConfigureAwait(false);

        // Success: run pending→running minting the fresh extraction claim fence; job
        // running with dispatch diagnostics and the committed-claim counter incremented.
        var extractionClaimId = Guid.NewGuid();
        var runStarted = await ExecuteAsync(connection, transaction, """
            UPDATE etl_runs SET status='running', started_at_utc=$now,
                extraction_claim_id=$claim, extraction_claim_owner_id=$owner, extraction_claim_acquired_at_utc=$now,
                updated_at_utc=$now, row_version=row_version+1
            WHERE run_id=$run AND status='pending';
            """, cancellationToken,
            ("$now", now), ("$claim", extractionClaimId.ToString("D")), ("$owner", ownerId), ("$run", run.RunId.ToString("D"))).ConfigureAwait(false);
        if (runStarted != 1) throw new InvalidOperationException($"ETL run {run.RunId:D} left 'pending' mid-transaction.");
        var jobStarted = await ExecuteAsync(connection, transaction, """
            UPDATE etl_jobs SET status='running', dispatch_owner_id=$owner, dispatch_claimed_at_utc=$now,
                claim_attempt_count=claim_attempt_count+1, deferral_code=NULL, deferral_message=NULL, available_at_utc=NULL,
                updated_at_utc=$now, row_version=row_version+1
            WHERE job_id=$job AND status IN ('pending','deferred');
            """, cancellationToken,
            ("$owner", ownerId), ("$now", now), ("$job", jobId.ToString("D"))).ConfigureAwait(false);
        if (jobStarted != 1) throw new InvalidOperationException($"ETL job {jobId:D} left the claimable state mid-transaction.");

        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        return new EtlJobClaimOutcome.Claimed(new EtlJobClaim(jobId, run.RunId, extractionClaimId, job!.Mode, job.EntitiesJson!, job.ConfigurationVersion!.Value));
    }

    /// <inheritdoc cref="IAgentStore.GetDispatchableEtlJobsAsync"/>
    public async Task<EtlJobDispatchPage> GetDispatchableEtlJobsAsync(int limit, DateTimeOffset nowUtc, CancellationToken cancellationToken)
    {
        var jobs = new List<EtlDispatchableJob>();
        var quarantined = new List<EtlPendingRunQuarantine>();
        var now = nowUtc.ToUniversalTime().ToString("O");
        await using var connection = await factory.OpenAsync(cancellationToken).ConfigureAwait(false);
        // Deterministic pure scalar on THIS connection: the SAME complete typed frozen
        // identity validation the claim transaction applies (typed nonempty unique
        // manifest == ordered typed entity codes), so enumeration can never disagree
        // with the claim path — and malformed JSON can never throw inside the query.
        connection.CreateFunction<string?, string?, long>(
            "etl_job_manifest_consistent",
            static (entities, manifest) => JobManifestConsistent(entities, manifest) ? 1L : 0L,
            isDeterministic: true);

        // Eligibility BEFORE LIMIT, ranked by run (created_at_utc, run_id): the typed
        // manifest requirement, the frozen job-identity consistency (mode,
        // configuration version, typed unique entity codes equal in order to the
        // manifest), the elder-overlap reservation and the ownership-availability
        // predicate are all evaluated in SQL first, so a busy, deferred or corrupt
        // head can never hide disjoint eligible work.
        await using (var command = connection.CreateCommand())
        {
            command.CommandText = $"""
                SELECT j.job_id, j.run_id, j.mode, j.configuration_version, j.entities_json
                FROM etl_jobs j JOIN etl_runs r ON r.run_id = j.run_id
                WHERE (j.status='pending' OR (j.status='deferred' AND (j.available_at_utc IS NULL OR j.available_at_utc <= $now)))
                  AND r.status='pending'
                  AND j.mode = r.mode
                  AND j.configuration_version IS NOT NULL AND j.configuration_version = r.configuration_version
                  AND etl_job_manifest_consistent(j.entities_json, r.requested_entities_json) = 1
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
            Add(command, "$now", now); Add(command, "$limit", limit);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                jobs.Add(new EtlDispatchableJob(Guid.Parse(reader.GetString(0)), Guid.Parse(reader.GetString(1)), reader.GetString(2), reader.GetInt64(3), reader.GetString(4)));
            }
        }

        // Quarantine diagnostics are surfaced even when no candidate is eligible: every
        // non-terminal pending run whose manifest fails typed validation (or contradicts
        // its frozen job identity) is reported; the claim transaction decides quarantine
        // vs. admission hold.
        await using (var command = connection.CreateCommand())
        {
            command.CommandText = """
                SELECT r2.run_id, r2.requested_entities_json, j2.job_id, j2.entities_json, j2.mode, j2.configuration_version, r2.mode, r2.configuration_version
                FROM etl_runs r2 LEFT JOIN etl_jobs j2 ON j2.run_id = r2.run_id
                WHERE r2.status='pending'
                ORDER BY COALESCE(r2.created_at_utc,''), r2.run_id;
                """;
            await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                var runId = Guid.Parse(reader.GetString(0));
                var manifestJson = NullableString(reader, 1);
                var jobId = reader.IsDBNull(2) ? (Guid?)null : Guid.Parse(reader.GetString(2));
                var entitiesJson = NullableString(reader, 3);
                var jobMode = NullableString(reader, 4);
                var jobConfig = NullableLong(reader, 5);
                var runMode = NullableString(reader, 6);
                var runConfig = NullableLong(reader, 7);
                var invalid = ParseManifestOrdered(manifestJson) is null
                    || (jobId is not null && !FrozenJobIdentityConsistent(jobMode, runMode, jobConfig, runConfig, entitiesJson, manifestJson));
                if (invalid)
                {
                    quarantined.Add(new EtlPendingRunQuarantine(runId, jobId, "MANIFEST_INVALID",
                        "Pending run failed typed manifest validation; the claim transaction quarantines it only when durable state proves it never started."));
                }
            }
        }

        return new EtlJobDispatchPage(jobs, quarantined);
    }

    // ---------- O1 shared internals ----------

    // The exact three-way ownership gate evaluated inside a guarded write on etl_runs
    // (alias r). The manifest itself must satisfy the TYPED nonempty-unique-string
    // contract IN THE SAME PREDICATE — '{}', 'null', '[]', '[null]', '[1]', duplicate or
    // blank entries and malformed JSON all fail closed even when bindings/ownership are
    // empty (an invalid manifest with empty sets is never vacuously "equal"). Then set
    // equality in BOTH directions, never counts: every manifest entity has an immutable
    // binding AND an active ownership row owned by this run at exactly the bound
    // positive epoch; no binding and no active ownership row exists outside the
    // manifest. NOT EXISTS (never NOT IN) so a NULL json_each value can never hide an
    // extra row. CASE around json_each is only an evaluation guard — with the validity
    // clauses above, a malformed manifest already fails before these are consulted.
    private const string RunOwnershipSetPredicate = """
        json_valid(r.requested_entities_json)
        AND json_type(r.requested_entities_json) = 'array'
        AND (SELECT COUNT(*) FROM json_each(r.requested_entities_json)) > 0
        AND NOT EXISTS (SELECT 1 FROM json_each(r.requested_entities_json) ev
                        WHERE ev.type <> 'text' OR ev.value IS NULL OR TRIM(ev.value) = '')
        AND NOT EXISTS (SELECT 1 FROM json_each(r.requested_entities_json) e1
                        JOIN json_each(r.requested_entities_json) e2
                          ON e1.value = e2.value AND e1.key <> e2.key)
        AND NOT EXISTS (SELECT 1 FROM json_each(r.requested_entities_json) e
                        WHERE NOT EXISTS (SELECT 1 FROM etl_run_ownership_bindings b
                                          JOIN etl_entity_ownership o ON o.entity_name = b.entity_name
                                          WHERE b.run_id = r.run_id AND b.entity_name = e.value AND b.expected_epoch > 0
                                            AND o.owner_run_id = r.run_id AND o.released_at_utc IS NULL
                                            AND o.ownership_epoch = b.expected_epoch))
        AND NOT EXISTS (SELECT 1 FROM etl_run_ownership_bindings b
                        WHERE b.run_id = r.run_id
                          AND NOT EXISTS (SELECT 1 FROM json_each(r.requested_entities_json) e
                                          WHERE e.type = 'text' AND e.value = b.entity_name))
        AND NOT EXISTS (SELECT 1 FROM etl_entity_ownership o
                        WHERE o.owner_run_id = r.run_id AND o.released_at_utc IS NULL
                          AND NOT EXISTS (SELECT 1 FROM json_each(r.requested_entities_json) e
                                          WHERE e.type = 'text' AND e.value = o.entity_name))
        """;

    // Touched-entity membership arm used inside the run-level EXISTS subquery (alias r,
    // parameter $entity): the entity being mutated must itself carry an epoch-bound
    // active ownership row AND its immutable binding for this run — a stray injected
    // 'extracting' row for an unowned entity can never accept a write even while the
    // run's full ownership set is intact.
    private const string EntityOwnershipPredicate = """
        EXISTS (SELECT 1 FROM etl_entity_ownership eo
                JOIN etl_run_ownership_bindings eb
                  ON eb.run_id = eo.owner_run_id AND eb.entity_name = eo.entity_name
                WHERE eo.entity_name = $entity AND eo.owner_run_id = r.run_id
                  AND eo.released_at_utc IS NULL AND eb.expected_epoch > 0
                  AND eo.ownership_epoch = eb.expected_epoch)
        """;

    // Code-side evaluation of the same gate (claim/finalize readiness paths).
    private static async Task<bool> RunOwnershipSetHoldsAsync(SqliteConnection connection, SqliteTransaction transaction, Guid runId, CancellationToken cancellationToken) =>
        await ScalarLongAsync(connection, transaction,
            $"SELECT COUNT(*) FROM etl_runs r WHERE r.run_id=$run AND {RunOwnershipSetPredicate};",
            cancellationToken, ("$run", runId.ToString("D"))).ConfigureAwait(false) == 1;

    // Typed manifest validation returning the ordered entity list — a nonempty JSON array
    // of unique nonempty strings. This is the strict shape the overlap/overdraft guards
    // require; json_valid alone is never sufficient.
    private static List<string>? ParseManifestOrdered(string? entitiesJson)
    {
        if (string.IsNullOrWhiteSpace(entitiesJson)) return null;
        try
        {
            using var document = JsonDocument.Parse(entitiesJson);
            if (document.RootElement.ValueKind != JsonValueKind.Array) return null;
            var seen = new HashSet<string>(StringComparer.Ordinal);
            var ordered = new List<string>();
            foreach (var element in document.RootElement.EnumerateArray())
            {
                if (element.ValueKind != JsonValueKind.String) return null;
                var entity = element.GetString();
                if (string.IsNullOrWhiteSpace(entity) || !seen.Add(entity)) return null;
                ordered.Add(entity);
            }
            return ordered.Count > 0 ? ordered : null;
        }
        catch (JsonException)
        {
            return null;
        }
    }

    // The SAME complete typed frozen-identity manifest check the claim transaction
    // applies — also exposed to the eligibility query as the deterministic scalar
    // 'etl_job_manifest_consistent' so enumeration can never disagree with claim or
    // diagnostics. Malformed JSON returns false, never an exception.
    private static bool JobManifestConsistent(string? entitiesJson, string? manifestJson)
    {
        var manifest = ParseManifestOrdered(manifestJson);
        var codes = entitiesJson is null ? null : JobEntityCodes(entitiesJson);
        return manifest is not null && codes is not null && manifest.SequenceEqual(codes, StringComparer.Ordinal);
    }

    // Full frozen job identity: ordered manifest/code equality plus the frozen mode and
    // configuration version still equal the run's durable values.
    private static bool FrozenJobIdentityConsistent(string? jobMode, string? runMode, long? jobConfigurationVersion, long? runConfigurationVersion, string? entitiesJson, string? manifestJson) =>
        string.Equals(jobMode, runMode, StringComparison.Ordinal)
        && jobConfigurationVersion is not null && jobConfigurationVersion == runConfigurationVersion
        && JobManifestConsistent(entitiesJson, manifestJson);

    // Ordered entity codes of a job's frozen entities_json — valid only when every element
    // deserializes to a structurally valid resolved definition with unique codes.
    private static List<string>? JobEntityCodes(string entitiesJson)
    {
        try
        {
            var entities = JsonSerializer.Deserialize<List<EtlEntityDefinition>>(entitiesJson, JsonOptions);
            if (entities is null || entities.Count == 0 || entities.Any(static entity => entity is null)) return null;
            if (entities.Any(static entity => !IsValidResolvedEntityDefinition(entity))) return null;
            var codes = entities.Select(static entity => entity.EntityCode).ToList();
            return codes.Distinct(StringComparer.Ordinal).Count() == codes.Count ? codes : null;
        }
        catch (Exception ex) when (ex is JsonException or NotSupportedException)
        {
            return null;
        }
    }

    // Blocks the job (always) and the run (only while still pending — a run that already
    // has a lifecycle is preserved evidence, never falsified) for corrupt durable
    // identity evidence. Committed inside the caller's transaction. Returns the guarded
    // transition counts so callers can enforce expected-one semantics: a zero-row guard
    // loss or a swallowed write (RAISE(IGNORE)) must abort the claim transaction, never
    // commit a partial quarantine.
    private static async Task<(long JobsBlocked, long RunsBlocked)> CommitJobManifestBlockAsync(SqliteConnection connection, SqliteTransaction transaction, Guid? jobId, Guid? runId, string code, string message, string now, CancellationToken cancellationToken)
    {
        var jobsBlocked = 0L;
        if (jobId is not null)
        {
            jobsBlocked = await ExecuteAsync(connection, transaction,
                "UPDATE etl_jobs SET status='blocked', deferral_message=$message, available_at_utc=NULL, dispatch_owner_id=NULL, dispatch_claimed_at_utc=NULL, updated_at_utc=$now, row_version=row_version+1 WHERE job_id=$job AND status NOT IN ('finished','cancelled','blocked');",
                cancellationToken, ("$job", jobId.Value.ToString("D")), ("$message", $"{code}: {message}"), ("$now", now)).ConfigureAwait(false);
        }
        var runsBlocked = 0L;
        if (runId is not null)
        {
            runsBlocked = await ExecuteAsync(connection, transaction,
                "UPDATE etl_runs SET status='blocked', finalize_conflict_code=$code, finalize_conflict_message=$message, finished_at_utc=$now, updated_at_utc=$now, row_version=row_version+1 WHERE run_id=$run AND status='pending';",
                cancellationToken, ("$run", runId.Value.ToString("D")), ("$code", code), ("$message", message), ("$now", now)).ConfigureAwait(false);
        }
        return (jobsBlocked, runsBlocked);
    }

    private static async Task<bool> DeferJobAsync(SqliteConnection connection, SqliteTransaction transaction, Guid jobId, string code, string message, string deferUntil, string now, CancellationToken cancellationToken) =>
        await ExecuteAsync(connection, transaction,
            "UPDATE etl_jobs SET status='deferred', deferral_code=$code, deferral_message=$message, available_at_utc=$defer, dispatch_owner_id=NULL, dispatch_claimed_at_utc=NULL, updated_at_utc=$now, row_version=row_version+1 WHERE job_id=$job AND status IN ('pending','deferred');",
            cancellationToken,
            ("$code", code), ("$message", message), ("$defer", deferUntil), ("$now", now), ("$job", jobId.ToString("D"))).ConfigureAwait(false) == 1;

    private sealed record JobClaimRow(Guid RunId, string Mode, string? EntitiesJson, long? ConfigurationVersion);

    private static async Task<JobClaimRow?> ReadJobRowAsync(SqliteConnection connection, SqliteTransaction transaction, Guid jobId, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "SELECT run_id,mode,entities_json,configuration_version FROM etl_jobs WHERE job_id=$job;";
        Add(command, "$job", jobId.ToString("D"));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        return await reader.ReadAsync(cancellationToken).ConfigureAwait(false)
            ? new JobClaimRow(Guid.Parse(reader.GetString(0)), reader.GetString(1), NullableString(reader, 2), NullableLong(reader, 3))
            : null;
    }

    private sealed record ClaimRunRow(Guid RunId, string Status, string Mode, string? RequestedEntitiesJson, long? ConfigurationVersion, string? CreatedAtUtc);

    private static async Task<ClaimRunRow?> ReadClaimRunRowAsync(SqliteConnection connection, SqliteTransaction transaction, Guid runId, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "SELECT run_id,status,mode,requested_entities_json,configuration_version,created_at_utc FROM etl_runs WHERE run_id=$run;";
        Add(command, "$run", runId.ToString("D"));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        return await reader.ReadAsync(cancellationToken).ConfigureAwait(false)
            ? new ClaimRunRow(Guid.Parse(reader.GetString(0)), reader.GetString(1), reader.GetString(2), NullableString(reader, 3), NullableLong(reader, 4), NullableString(reader, 5))
            : null;
    }

    // An elder pending run together with the durable never-started evidence set: no
    // extraction/completion claim or attempt, no started_at, no batches/entities/
    // bindings/ownership, no seal, no completion payload, and no job claim history
    // (a committed claim, dispatch diagnostics or claim_attempt_count > 0 are prior
    // effects that must never be retired silently). EVERY older pending run is an
    // elder — including one whose job is already terminally blocked: pending status
    // with a dead job IS the durable admission hold retained for unresolved effects.
    private sealed record ElderRow(Guid RunId, string? ManifestJson, string? RunMode, long? RunConfigurationVersion,
        Guid? JobId, string? JobEntitiesJson, string? JobMode, long? JobConfigurationVersion, string? JobStatus, bool ProvablyInert);

    private static async Task<List<ElderRow>> ReadElderRowsAsync(SqliteConnection connection, SqliteTransaction transaction, Guid runId, string? createdAtUtc, CancellationToken cancellationToken)
    {
        var elders = new List<ElderRow>();
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = $"""
            SELECT r2.run_id, r2.requested_entities_json, r2.mode, r2.configuration_version,
                   j2.job_id, j2.entities_json, j2.mode, j2.configuration_version, j2.status,
                   CASE WHEN {RunInertPredicate("r2")}
                        THEN 1 ELSE 0 END
            FROM etl_runs r2 LEFT JOIN etl_jobs j2 ON j2.run_id = r2.run_id
            WHERE r2.status='pending' AND r2.run_id <> $run
              AND (COALESCE(r2.created_at_utc,'') < $created OR (COALESCE(r2.created_at_utc,'') = $created AND r2.run_id < $run))
            ORDER BY COALESCE(r2.created_at_utc,''), r2.run_id;
            """;
        Add(command, "$run", runId.ToString("D")); Add(command, "$created", createdAtUtc ?? string.Empty);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            elders.Add(new ElderRow(Guid.Parse(reader.GetString(0)), NullableString(reader, 1), NullableString(reader, 2), NullableLong(reader, 3),
                reader.IsDBNull(4) ? null : Guid.Parse(reader.GetString(4)), NullableString(reader, 5), NullableString(reader, 6), NullableLong(reader, 7),
                NullableString(reader, 8), reader.GetInt64(9) == 1));
        }
        return elders;
    }

    // The shared durable never-started predicate (run alias supplied by the caller):
    // true only while NOTHING proves the run was ever touched — no lifecycle timestamps
    // or errors, no extraction/completion claims or attempt policy, no seal or payload,
    // no counters, no batches/entities/bindings/ownership rows, and no job-side claim
    // history (a non-'pending' job status, a committed claim, dispatch diagnostics,
    // deferral evidence or a non-zero claim_attempt_count are ALL prior effects that
    // must never be retired silently). The elder read and the direct self-claim path
    // share this exact predicate so the two can never drift; unknown or unproven
    // durable evidence always keeps the admission hold.
    private static string RunInertPredicate(string alias) => $"""
        {alias}.started_at_utc IS NULL
          AND {alias}.extraction_claim_id IS NULL AND {alias}.extraction_claim_owner_id IS NULL AND {alias}.extraction_claim_acquired_at_utc IS NULL
          AND {alias}.completion_claim_id IS NULL AND {alias}.completion_claim_owner_id IS NULL AND {alias}.completion_claim_acquired_at_utc IS NULL
          AND {alias}.completion_attempt_count = 0 AND {alias}.completion_max_attempts IS NULL
          AND {alias}.next_completion_attempt_at_utc IS NULL AND {alias}.completion_acknowledged_at_utc IS NULL
          AND {alias}.sealed_at_utc IS NULL AND {alias}.sealed_entity_count IS NULL AND {alias}.sealed_expected_batch_count IS NULL
          AND {alias}.complete_payload_json IS NULL
          AND {alias}.finished_at_utc IS NULL AND {alias}.resolved_at_utc IS NULL
          AND {alias}.last_error IS NULL AND {alias}.error_count = 0
          AND {alias}.finalize_conflict_code IS NULL AND {alias}.finalize_conflict_message IS NULL
          AND {alias}.rows_read = 0 AND {alias}.batches_created = 0 AND {alias}.batches_acknowledged = 0
          AND NOT EXISTS (SELECT 1 FROM etl_batches eb WHERE eb.run_id = {alias}.run_id)
          AND NOT EXISTS (SELECT 1 FROM etl_run_entities ee WHERE ee.run_id = {alias}.run_id)
          AND NOT EXISTS (SELECT 1 FROM etl_run_ownership_bindings bb WHERE bb.run_id = {alias}.run_id)
          AND NOT EXISTS (SELECT 1 FROM etl_entity_ownership oo WHERE oo.owner_run_id = {alias}.run_id)
          AND NOT EXISTS (SELECT 1 FROM etl_jobs jj WHERE jj.run_id = {alias}.run_id
                          AND (jj.status <> 'pending'
                               OR jj.claim_attempt_count > 0
                               OR jj.dispatch_owner_id IS NOT NULL OR jj.dispatch_claimed_at_utc IS NOT NULL
                               OR jj.deferral_code IS NOT NULL OR jj.deferral_message IS NOT NULL OR jj.available_at_utc IS NOT NULL))
        """;

    // The identical durable never-started evidence set evaluated for a single run (the
    // direct self-claim path). Unknown prior effects are never retired silently.
    private static async Task<bool> RunProvablyInertAsync(SqliteConnection connection, SqliteTransaction transaction, Guid runId, CancellationToken cancellationToken) =>
        await ScalarLongAsync(connection, transaction, $"""
            SELECT COUNT(*) FROM etl_runs r
            WHERE r.run_id = $run AND {RunInertPredicate("r")};
            """, cancellationToken, ("$run", runId.ToString("D"))).ConfigureAwait(false) == 1;
}
