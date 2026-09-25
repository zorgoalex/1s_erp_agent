using System.Globalization;
using System.Text.Json;
using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Domain.Etl;
using ErpOnecAgent.Infrastructure.Persistence.Sqlite;
using Microsoft.Data.Sqlite;
using Xunit;

namespace ErpOnecAgent.IntegrationTests;

/// <summary>
/// O2 DARK send-ledger suite: the durable admitted-attempt ledger, the one-commit
/// owner-fenced claim (full run ownership/binding three-way equality at positive
/// epoch + run extraction-active + due status + no live admission + durable bound),
/// the single-use attempt lifecycle (admitted → exactly one terminal outcome),
/// fail-closed unknown outcomes (dead_letter UPLOAD_OUTCOME_UNKNOWN + eager run
/// block, ownership retained, never reclaimable), bounded proven-unsent precheck
/// retries with caller-supplied timestamps (D1/D2), the ACK evidence surface
/// (fenced apply, matched-late evidence, first-observation preservation, explicit
/// conflicts, foreign-attempt ClaimLost), the savepoint-vs-throw transaction
/// discipline (real SQLite triggers), recovery admitted→orphaned, and retention
/// reconciliation — all against a real migrated SQLite database. DARK: nothing here
/// wires workers, the ERP client, or production dispatch.
/// </summary>
public sealed class EtlSendLedgerO2Tests : IAsyncLifetime
{
    private const string SourceNamespace = "onec-infobase-a";
    private const string QueryMode = "bootstrap_full";
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);
    private static readonly EtlCursor FinalClients = new(DateTimeOffset.Parse("2026-09-19T09:59:00.0000000+00:00", CultureInfo.InvariantCulture), "A10");
    private static readonly EtlCursor FinalOrders = new(DateTimeOffset.Parse("2026-09-19T09:58:00.0000000+00:00", CultureInfo.InvariantCulture), "O8");

    private readonly SqliteTestDatabase _database = new();
    private readonly Dictionary<Guid, Guid> _extractionClaims = new();
    private SqliteConnectionFactory _factory = null!;
    private SqliteAgentStore _store = null!;

    public async Task InitializeAsync()
    {
        _factory = _database.CreateFactory(Path.Combine("data", "agent.db"));
        _store = new(_factory, new SqliteMigrator(_factory));
        await _store.InitializeAsync(CancellationToken.None);
    }

    public async Task DisposeAsync()
    {
        await _database.DisposeAsync();
    }

    // ---------- claim + due enumeration ----------

    [Fact]
    public async Task Claim_flips_batch_persists_bound_and_ledgers_admitted_in_one_commit()
    {
        var runId = await SealedRunAsync("clients");
        var batchId = await ReadyBatchIdAsync(runId);
        var due = Assert.Single(await _store.GetDueBatchUploadsAsync(10, DateTimeOffset.UtcNow, CancellationToken.None));
        Assert.Equal(batchId, due.BatchId);
        Assert.Equal(runId, due.RunId);
        Assert.Equal(0, due.PriorAttempts);

        var outcome = await _store.TryClaimBatchUploadAsync(batchId, "uploader-1", DateTimeOffset.UtcNow, 5, CancellationToken.None);

        var claim = Assert.IsType<EtlBatchUploadClaimOutcome.Claimed>(outcome).Claim;
        Assert.Equal(batchId, claim.BatchId);
        Assert.Equal(runId, claim.RunId);
        Assert.Equal(1, claim.AttemptNo);
        Assert.Equal("uploader-1", claim.OwnerId);
        Assert.Equal(5, claim.UploadMaxAttempts);
        Assert.Equal("uploading", await ScalarStringAsync($"SELECT status FROM etl_batches WHERE batch_id='{batchId:D}'"));
        Assert.Equal(claim.AttemptId.ToString("D"), await ScalarStringAsync($"SELECT send_attempt_id FROM etl_batches WHERE batch_id='{batchId:D}'"));
        Assert.Equal("5", await ScalarStringAsync($"SELECT upload_max_attempts FROM etl_batches WHERE batch_id='{batchId:D}'"));
        // ONE ledger row, admitted under the minted attempt identity — never the owner string.
        Assert.Equal(1, await ScalarAsync($"SELECT COUNT(*) FROM etl_batch_send_attempts WHERE batch_id='{batchId:D}'"));
        Assert.Equal("admitted", await ScalarStringAsync($"SELECT outcome FROM etl_batch_send_attempts WHERE attempt_id='{claim.AttemptId:D}'"));
        Assert.Equal("uploader-1", await ScalarStringAsync($"SELECT owner_id FROM etl_batch_send_attempts WHERE attempt_id='{claim.AttemptId:D}'"));
        // A live admitted attempt fences the batch out of enumeration and re-claim.
        Assert.Empty(await _store.GetDueBatchUploadsAsync(10, DateTimeOffset.UtcNow, CancellationToken.None));
        Assert.IsType<EtlBatchUploadClaimOutcome.NotClaimed>(await _store.TryClaimBatchUploadAsync(batchId, "uploader-2", DateTimeOffset.UtcNow, 5, CancellationToken.None));
        Assert.Equal(1, await ScalarAsync($"SELECT COUNT(*) FROM etl_batch_send_attempts WHERE batch_id='{batchId:D}'"));
    }

    [Fact]
    public async Task Two_concurrent_claimants_on_two_store_instances_admit_exactly_one_attempt()
    {
        var runId = await SealedRunAsync("clients");
        var batchId = await ReadyBatchIdAsync(runId);
        var other = new SqliteAgentStore(_factory, new SqliteMigrator(_factory));
        var barrier = new Barrier(2);
        var now = DateTimeOffset.UtcNow;

        var first = Task.Run(async () =>
        {
            barrier.SignalAndWait();
            return await _store.TryClaimBatchUploadAsync(batchId, "uploader-a", now, 5, CancellationToken.None);
        });
        var second = Task.Run(async () =>
        {
            barrier.SignalAndWait();
            return await other.TryClaimBatchUploadAsync(batchId, "uploader-b", now, 5, CancellationToken.None);
        });
        var results = await Task.WhenAll(first, second);

        Assert.Single(results, r => r is EtlBatchUploadClaimOutcome.Claimed);
        Assert.Single(results, r => r is EtlBatchUploadClaimOutcome.NotClaimed);
        Assert.Equal(1, await ScalarAsync($"SELECT COUNT(*) FROM etl_batch_send_attempts WHERE batch_id='{batchId:D}' AND outcome='admitted'"));
        Assert.Equal("uploading", await ScalarStringAsync($"SELECT status FROM etl_batches WHERE batch_id='{batchId:D}'"));
    }

    [Theory]
    [InlineData("missing_binding")]
    [InlineData("extra_ownership")]
    [InlineData("wrong_epoch")]
    [InlineData("foreign_owner")]
    [InlineData("released")]
    public async Task Claim_with_a_broken_ownership_set_refuses_with_zero_writes(string corruption)
    {
        var runId = await SealedRunAsync("clients");
        var batchId = await ReadyBatchIdAsync(runId);
        var runText = runId.ToString("D");
        switch (corruption)
        {
            case "missing_binding":
                await ExecuteSqlAsync("DELETE FROM etl_run_ownership_bindings WHERE run_id=$run;", ("$run", runText));
                break;
            case "extra_ownership":
                await ExecuteSqlAsync(
                    "INSERT INTO etl_entity_ownership(entity_name,owner_run_id,ownership_epoch,acquired_at_utc,updated_at_utc) VALUES('payments',$run,1,$now,$now);",
                    ("$run", runText), ("$now", Now()));
                break;
            case "wrong_epoch":
                await ExecuteSqlAsync("UPDATE etl_entity_ownership SET ownership_epoch=ownership_epoch+1, updated_at_utc=$now WHERE owner_run_id=$run;", ("$run", runText), ("$now", Now()));
                break;
            case "foreign_owner":
                var other = await NewRunAsync("orders");
                await ExecuteSqlAsync("UPDATE etl_entity_ownership SET owner_run_id=$other, updated_at_utc=$now WHERE owner_run_id=$run;", ("$other", other.ToString("D")), ("$run", runText), ("$now", Now()));
                break;
            case "released":
                await ExecuteSqlAsync("UPDATE etl_entity_ownership SET released_at_utc=$now, release_reason='manual_release', updated_at_utc=$now WHERE owner_run_id=$run;", ("$run", runText), ("$now", Now()));
                break;
        }

        var outcome = await _store.TryClaimBatchUploadAsync(batchId, "uploader-1", DateTimeOffset.UtcNow, 5, CancellationToken.None);

        Assert.IsType<EtlBatchUploadClaimOutcome.NotClaimed>(outcome);
        // Zero writes: batch unchanged, no ledger row, bound unpersisted.
        Assert.Equal("ready", await ScalarStringAsync($"SELECT status FROM etl_batches WHERE batch_id='{batchId:D}'"));
        Assert.Null(await ScalarStringAsync($"SELECT send_attempt_id FROM etl_batches WHERE batch_id='{batchId:D}'"));
        Assert.Null(await ScalarStringAsync($"SELECT upload_max_attempts FROM etl_batches WHERE batch_id='{batchId:D}'"));
        Assert.Equal(0, await ScalarAsync($"SELECT COUNT(*) FROM etl_batch_send_attempts WHERE batch_id='{batchId:D}'"));
        Assert.Equal("uploading", await RunStatusAsync(runId));
    }

    [Fact]
    public async Task Claim_rejects_on_terminal_batch_inactive_run_or_stray_unowned_entity()
    {
        var runId = await SealedRunAsync("clients");
        var batchId = await ReadyBatchIdAsync(runId);
        var now = DateTimeOffset.UtcNow;

        // A batch of a blocked run is never admissible.
        await ExecuteSqlAsync("UPDATE etl_runs SET status='blocked' WHERE run_id=$run;", ("$run", runId.ToString("D")));
        Assert.IsType<EtlBatchUploadClaimOutcome.NotClaimed>(await _store.TryClaimBatchUploadAsync(batchId, "o", now, 5, CancellationToken.None));
        await ExecuteSqlAsync("UPDATE etl_runs SET status='uploading' WHERE run_id=$run;", ("$run", runId.ToString("D")));

        // A terminal batch is never admissible.
        await ExecuteSqlAsync("UPDATE etl_batches SET status='dead_letter' WHERE batch_id=$batch;", ("$batch", batchId.ToString("D")));
        Assert.IsType<EtlBatchUploadClaimOutcome.NotClaimed>(await _store.TryClaimBatchUploadAsync(batchId, "o", now, 5, CancellationToken.None));
        await ExecuteSqlAsync("UPDATE etl_batches SET status='ready' WHERE batch_id=$batch;", ("$batch", batchId.ToString("D")));

        // A stray 'ready' batch of an entity the run does not own is never admissible
        // (and never enumerated).
        var strayBatch = Guid.NewGuid();
        await ExecuteSqlAsync(
            "INSERT INTO etl_batches(batch_id,run_id,entity_name,schema_version,file_path,status,row_count,sha256,compressed_size,uncompressed_size,created_at_utc) VALUES($b,$run,'orders',1,'spool/stray.gz','ready',3,'h',10,10,$now);",
            ("$b", strayBatch.ToString("D")), ("$run", runId.ToString("D")), ("$now", Now()));
        Assert.IsType<EtlBatchUploadClaimOutcome.NotClaimed>(await _store.TryClaimBatchUploadAsync(strayBatch, "o", now, 5, CancellationToken.None));
        Assert.Equal(0, await ScalarAsync($"SELECT COUNT(*) FROM etl_batch_send_attempts WHERE batch_id='{strayBatch:D}'"));
    }

    [Fact]
    public async Task Claim_is_admissible_while_the_run_is_still_running()
    {
        // 'running' admission: an extraction-phase batch can be sent before seal.
        var runId = await NewRunAsync("clients");
        Assert.IsType<EtlEntityBeginOutcome.Begun>(await _store.BeginEtlEntityExtractionAsync(runId, ClaimOf(runId), Request("clients"), CancellationToken.None));
        var batch = MakeBatch(runId, "clients", 5);
        Assert.IsType<EtlBatchRegistrationOutcome.Registered>(await _store.RegisterGuardedEtlBatchAsync(batch, ClaimOf(runId), CancellationToken.None));
        Assert.Equal("running", await RunStatusAsync(runId));

        var outcome = await _store.TryClaimBatchUploadAsync(batch.BatchId, "uploader-1", DateTimeOffset.UtcNow, 5, CancellationToken.None);

        var claim = Assert.IsType<EtlBatchUploadClaimOutcome.Claimed>(outcome).Claim;
        Assert.Equal(runId, claim.RunId);
        Assert.Equal("uploading", await ScalarStringAsync($"SELECT status FROM etl_batches WHERE batch_id='{batch.BatchId:D}'"));
    }

    [Fact]
    public async Task Mismatched_or_missing_presented_bound_is_refused_zero_writes()
    {
        var runId = await SealedRunAsync("clients");
        var batchId = await ReadyBatchIdAsync(runId);
        var first = Assert.IsType<EtlBatchUploadClaimOutcome.Claimed>(await _store.TryClaimBatchUploadAsync(batchId, "uploader-1", DateTimeOffset.UtcNow, 3, CancellationToken.None));
        Assert.Equal(3, first.Claim.UploadMaxAttempts);
        Assert.IsType<EtlBatchSendRetryOutcome.Scheduled>(await _store.RetryClaimedBatchSendAsync(batchId, first.Claim.AttemptId, "dns precheck failed", DateTimeOffset.UtcNow.AddMinutes(-1), CancellationToken.None));

        // Identical-value enforcement: a different presented limit is refused with
        // zero writes — the persisted bound (3) wins.
        var refused = await _store.TryClaimBatchUploadAsync(batchId, "uploader-2", DateTimeOffset.UtcNow, 7, CancellationToken.None);

        Assert.IsType<EtlBatchUploadClaimOutcome.NotClaimed>(refused);
        Assert.Equal("3", await ScalarStringAsync($"SELECT upload_max_attempts FROM etl_batches WHERE batch_id='{batchId:D}'"));
        Assert.Equal("retry_waiting", await ScalarStringAsync($"SELECT status FROM etl_batches WHERE batch_id='{batchId:D}'"));
        Assert.Equal(1, await ScalarAsync($"SELECT COUNT(*) FROM etl_batch_send_attempts WHERE batch_id='{batchId:D}'"));
        Assert.Equal(0, await ScalarAsync($"SELECT COUNT(*) FROM etl_batch_send_attempts WHERE batch_id='{batchId:D}' AND outcome='admitted'"));

        // The identical bound still claims: a second admitted attempt (attempt_no=2).
        var second = Assert.IsType<EtlBatchUploadClaimOutcome.Claimed>(await _store.TryClaimBatchUploadAsync(batchId, "uploader-2", DateTimeOffset.UtcNow, 3, CancellationToken.None));
        Assert.Equal(2, second.Claim.AttemptNo);
        Assert.Equal(3, second.Claim.UploadMaxAttempts);
        Assert.Equal(2, await ScalarAsync($"SELECT COUNT(*) FROM etl_batch_send_attempts WHERE batch_id='{batchId:D}'"));
    }

    // ---------- outcome paths ----------

    [Fact]
    public async Task Precheck_failed_retries_are_bounded_and_exhaustion_quarantines()
    {
        var runId = await SealedRunAsync("clients");
        var batchId = await ReadyBatchIdAsync(runId);
        var next = DateTimeOffset.UtcNow.AddMinutes(-1);

        // Bound = 2 admitted attempts. First cycle: claim → precheck attestation →
        // retry_waiting under the caller-supplied timestamp (D2: the store invents
        // no schedule).
        var claim1 = Assert.IsType<EtlBatchUploadClaimOutcome.Claimed>(await _store.TryClaimBatchUploadAsync(batchId, "u", DateTimeOffset.UtcNow, 2, CancellationToken.None)).Claim;
        var scheduled = await _store.RetryClaimedBatchSendAsync(batchId, claim1.AttemptId, "local clock skew check failed", next, CancellationToken.None);
        Assert.IsType<EtlBatchSendRetryOutcome.Scheduled>(scheduled);
        Assert.Equal("precheck_failed", await ScalarStringAsync($"SELECT outcome FROM etl_batch_send_attempts WHERE attempt_id='{claim1.AttemptId:D}'"));
        Assert.Equal("retry_waiting", await ScalarStringAsync($"SELECT status FROM etl_batches WHERE batch_id='{batchId:D}'"));
        Assert.Equal(next.ToUniversalTime().ToString("O"), await ScalarStringAsync($"SELECT next_attempt_at_utc FROM etl_batches WHERE batch_id='{batchId:D}'"));

        var due = Assert.Single(await _store.GetDueBatchUploadsAsync(10, DateTimeOffset.UtcNow, CancellationToken.None));
        Assert.Equal(1, due.PriorAttempts);

        // Second admission (attempt_no=2) + precheck failure → the durable bound is
        // reached: batch dead_letter UPLOAD_ATTEMPTS_EXHAUSTED + run + job blocked in
        // one commit.
        var claim2 = Assert.IsType<EtlBatchUploadClaimOutcome.Claimed>(await _store.TryClaimBatchUploadAsync(batchId, "u", DateTimeOffset.UtcNow, 2, CancellationToken.None)).Claim;
        var exhausted = await _store.RetryClaimedBatchSendAsync(batchId, claim2.AttemptId, "crc mismatch", next, CancellationToken.None);
        var blocked = Assert.IsType<EtlBatchSendRetryOutcome.Blocked>(exhausted);
        Assert.Equal("UPLOAD_ATTEMPTS_EXHAUSTED", blocked.Code);
        Assert.Equal("dead_letter", await ScalarStringAsync($"SELECT status FROM etl_batches WHERE batch_id='{batchId:D}'"));
        Assert.Equal("UPLOAD_ATTEMPTS_EXHAUSTED", await ScalarStringAsync($"SELECT quarantine_code FROM etl_batches WHERE batch_id='{batchId:D}'"));
        Assert.Equal("blocked", await RunStatusAsync(runId));
        Assert.Equal("UPLOAD_ATTEMPTS_EXHAUSTED", await ScalarStringAsync($"SELECT finalize_conflict_code FROM etl_runs WHERE run_id='{runId:D}'"));
        Assert.Equal("blocked", await ScalarStringAsync($"SELECT status FROM etl_jobs WHERE run_id='{runId:D}'"));
        Assert.Equal("precheck_failed", await ScalarStringAsync($"SELECT outcome FROM etl_batch_send_attempts WHERE attempt_id='{claim2.AttemptId:D}'"));

        // The batch is never claimable again.
        Assert.IsType<EtlBatchUploadClaimOutcome.NotClaimed>(await _store.TryClaimBatchUploadAsync(batchId, "u", DateTimeOffset.UtcNow, 2, CancellationToken.None));
        Assert.Empty(await _store.GetDueBatchUploadsAsync(10, DateTimeOffset.UtcNow, CancellationToken.None));
    }

    [Fact]
    public async Task Claim_side_bound_exhaustion_quarantines_a_corrupt_retry_waiting_batch()
    {
        // Durable corruption: a 'retry_waiting' batch whose admitted-attempt count
        // already reached its persisted bound (inconsistent persisted state must fail
        // closed, not loop). Claim-side exhaustion fires: dead_letter + run block.
        var runId = await SealedRunAsync("clients");
        var batchId = await ReadyBatchIdAsync(runId);
        var claim1 = Assert.IsType<EtlBatchUploadClaimOutcome.Claimed>(await _store.TryClaimBatchUploadAsync(batchId, "u", DateTimeOffset.UtcNow, 2, CancellationToken.None)).Claim;
        Assert.IsType<EtlBatchSendRetryOutcome.Scheduled>(await _store.RetryClaimedBatchSendAsync(batchId, claim1.AttemptId, "e", DateTimeOffset.UtcNow.AddMinutes(-1), CancellationToken.None));
        // Inject a corrupt second admitted attempt left terminal without consumption.
        await ExecuteSqlAsync(
            "INSERT INTO etl_batch_send_attempts(attempt_id,batch_id,attempt_no,owner_id,admitted_at_utc,finished_at_utc,outcome) VALUES($a,$b,2,'u',$now,$now,'precheck_failed');",
            ("$a", Guid.NewGuid().ToString("D")), ("$b", batchId.ToString("D")), ("$now", Now()));

        var outcome = await _store.TryClaimBatchUploadAsync(batchId, "u", DateTimeOffset.UtcNow, 2, CancellationToken.None);

        var blocked = Assert.IsType<EtlBatchUploadClaimOutcome.Blocked>(outcome);
        Assert.Equal("UPLOAD_ATTEMPTS_EXHAUSTED", blocked.Code);
        Assert.Equal("dead_letter", await ScalarStringAsync($"SELECT status FROM etl_batches WHERE batch_id='{batchId:D}'"));
        Assert.Equal("UPLOAD_ATTEMPTS_EXHAUSTED", await ScalarStringAsync($"SELECT quarantine_code FROM etl_batches WHERE batch_id='{batchId:D}'"));
        Assert.Equal("blocked", await RunStatusAsync(runId));
        // No new attempt was ever admitted.
        Assert.Equal(2, await ScalarAsync($"SELECT COUNT(*) FROM etl_batch_send_attempts WHERE batch_id='{batchId:D}'"));
    }

    [Fact]
    public async Task Unknown_outcome_quarantines_batch_blocks_run_and_retains_ownership()
    {
        var runId = await SealedRunAsync("clients");
        var batchId = await ReadyBatchIdAsync(runId);
        var claim = Assert.IsType<EtlBatchUploadClaimOutcome.Claimed>(await _store.TryClaimBatchUploadAsync(batchId, "u", DateTimeOffset.UtcNow, 5, CancellationToken.None)).Claim;

        var outcome = await _store.FailClaimedBatchSendAsync(batchId, claim.AttemptId, "timeout after 30s — response never arrived", null, CancellationToken.None);

        var blocked = Assert.IsType<EtlBatchSendFailureOutcome.Blocked>(outcome);
        Assert.Equal("UPLOAD_OUTCOME_UNKNOWN", blocked.Code);
        // Attempt terminally 'unknown' — never re-armed.
        Assert.Equal("unknown", await ScalarStringAsync($"SELECT outcome FROM etl_batch_send_attempts WHERE attempt_id='{claim.AttemptId:D}'"));
        Assert.NotNull(await ScalarStringAsync($"SELECT finished_at_utc FROM etl_batch_send_attempts WHERE attempt_id='{claim.AttemptId:D}'"));
        Assert.Equal("dead_letter", await ScalarStringAsync($"SELECT status FROM etl_batches WHERE batch_id='{batchId:D}'"));
        Assert.Equal("UPLOAD_OUTCOME_UNKNOWN", await ScalarStringAsync($"SELECT quarantine_code FROM etl_batches WHERE batch_id='{batchId:D}'"));
        // last_error stays human/diagnostic text (D3).
        Assert.Contains("timeout", await ScalarStringAsync($"SELECT last_error FROM etl_batches WHERE batch_id='{batchId:D}'") ?? "");
        Assert.Equal("blocked", await RunStatusAsync(runId));
        Assert.Equal("UPLOAD_OUTCOME_UNKNOWN", await ScalarStringAsync($"SELECT finalize_conflict_code FROM etl_runs WHERE run_id='{runId:D}'"));
        Assert.Equal("blocked", await ScalarStringAsync($"SELECT status FROM etl_jobs WHERE run_id='{runId:D}'"));
        // Ownership + bindings retained: the unknown outcome proves nothing about the
        // remote side, so the run keeps exactly what it owned.
        Assert.Equal(1, await ScalarAsync($"SELECT COUNT(*) FROM etl_entity_ownership WHERE owner_run_id='{runId:D}' AND released_at_utc IS NULL"));
        Assert.Equal(1, await ScalarAsync($"SELECT COUNT(*) FROM etl_run_ownership_bindings WHERE run_id='{runId:D}'"));
        // Never reclaimable, ever.
        Assert.IsType<EtlBatchUploadClaimOutcome.NotClaimed>(await _store.TryClaimBatchUploadAsync(batchId, "u", DateTimeOffset.UtcNow, 5, CancellationToken.None));
        Assert.Empty(await _store.GetDueBatchUploadsAsync(10, DateTimeOffset.UtcNow, CancellationToken.None));
    }

    [Fact]
    public async Task Eager_block_fences_every_sibling_batch_of_the_blocked_run()
    {
        var runId = await NewRunAsync("clients", "orders");
        await BeginExtractCompleteAsync(runId, "clients", CursorJson(FinalClients), 5, ClaimOf(runId));
        await BeginExtractCompleteAsync(runId, "orders", CursorJson(FinalOrders), 7, ClaimOf(runId));
        Assert.IsType<EtlRunSealOutcome.Sealed>(await _store.SealEtlRunExtractionAsync(runId, ClaimOf(runId), CancellationToken.None));
        var batchA = await ReadyBatchIdAsync(runId, "clients");
        var batchB = await ReadyBatchIdAsync(runId, "orders");
        var claim = Assert.IsType<EtlBatchUploadClaimOutcome.Claimed>(await _store.TryClaimBatchUploadAsync(batchA, "u", DateTimeOffset.UtcNow, 5, CancellationToken.None)).Claim;

        await _store.FailClaimedBatchSendAsync(batchA, claim.AttemptId, "transport reset", 502, CancellationToken.None);

        // One uncertain outcome dead-letters every still-pending sibling RUN_BLOCKED —
        // no sibling batch of a blocked run is claimable (eager block).
        Assert.Equal("dead_letter", await ScalarStringAsync($"SELECT status FROM etl_batches WHERE batch_id='{batchB:D}'"));
        Assert.Equal("RUN_BLOCKED", await ScalarStringAsync($"SELECT quarantine_code FROM etl_batches WHERE batch_id='{batchB:D}'"));
        Assert.Equal("502", await ScalarStringAsync($"SELECT http_status FROM etl_batch_send_attempts WHERE attempt_id='{claim.AttemptId:D}'"));
        Assert.IsType<EtlBatchUploadClaimOutcome.NotClaimed>(await _store.TryClaimBatchUploadAsync(batchB, "u", DateTimeOffset.UtcNow, 5, CancellationToken.None));
        // Ownership for both entities remains — retained evidence.
        Assert.Equal(2, await ScalarAsync($"SELECT COUNT(*) FROM etl_entity_ownership WHERE owner_run_id='{runId:D}' AND released_at_utc IS NULL"));
    }

    // ---------- ACK ----------

    [Fact]
    public async Task Valid_ack_applies_batch_attempt_and_run_counter_in_one_fenced_commit()
    {
        var runId = await SealedRunAsync("clients");
        var batchId = await ReadyBatchIdAsync(runId);
        var claim = Assert.IsType<EtlBatchUploadClaimOutcome.Claimed>(await _store.TryClaimBatchUploadAsync(batchId, "u", DateTimeOffset.UtcNow, 5, CancellationToken.None)).Claim;
        var ack = new EtlBatchAckEvidence("acknowledged", batchId, 5, true, DateTimeOffset.UtcNow);

        var outcome = await _store.AcknowledgeClaimedBatchAsync(batchId, claim.AttemptId, ack, "hash-of-wire-ack", 200, CancellationToken.None);

        Assert.IsType<EtlBatchAckOutcome.Acknowledged>(outcome);
        Assert.Equal("acknowledged", await ScalarStringAsync($"SELECT status FROM etl_batches WHERE batch_id='{batchId:D}'"));
        Assert.NotNull(await ScalarStringAsync($"SELECT acknowledged_at_utc FROM etl_batches WHERE batch_id='{batchId:D}'"));
        Assert.Equal("acknowledged", await ScalarStringAsync($"SELECT outcome FROM etl_batch_send_attempts WHERE attempt_id='{claim.AttemptId:D}'"));
        Assert.Equal("hash-of-wire-ack", await ScalarStringAsync($"SELECT ack_payload_hash FROM etl_batch_send_attempts WHERE attempt_id='{claim.AttemptId:D}'"));
        Assert.NotNull(await ScalarStringAsync($"SELECT ack_observed_at_utc FROM etl_batch_send_attempts WHERE attempt_id='{claim.AttemptId:D}'"));
        Assert.Equal("200", await ScalarStringAsync($"SELECT http_status FROM etl_batch_send_attempts WHERE attempt_id='{claim.AttemptId:D}'"));
        Assert.Equal("1", await ScalarStringAsync($"SELECT batches_acknowledged FROM etl_runs WHERE run_id='{runId:D}'"));
        // The run stays 'uploading' (not sealed-blocked), job untouched, ownership retained.
        Assert.Equal("uploading", await RunStatusAsync(runId));
        Assert.Equal(1, await ScalarAsync($"SELECT COUNT(*) FROM etl_entity_ownership WHERE owner_run_id='{runId:D}' AND released_at_utc IS NULL"));
    }

    [Theory]
    [InlineData("bad_status")]
    [InlineData("wrong_batch_id")]
    [InlineData("checksum_false")]
    [InlineData("wrong_row_count")]
    public async Task Invalid_ack_under_live_fence_rejects_attempt_and_blocks_run(string mutation)
    {
        var runId = await SealedRunAsync("clients");
        var batchId = await ReadyBatchIdAsync(runId);
        var claim = Assert.IsType<EtlBatchUploadClaimOutcome.Claimed>(await _store.TryClaimBatchUploadAsync(batchId, "u", DateTimeOffset.UtcNow, 5, CancellationToken.None)).Claim;
        var ack = new EtlBatchAckEvidence("acknowledged", batchId, 5, true, DateTimeOffset.UtcNow) with
        {
            Status = mutation == "bad_status" ? "accepted" : "acknowledged",
            BatchId = mutation == "wrong_batch_id" ? Guid.NewGuid() : batchId,
            ChecksumValid = mutation != "checksum_false",
            RowsAccepted = mutation == "wrong_row_count" ? 4 : 5
        };

        var outcome = await _store.AcknowledgeClaimedBatchAsync(batchId, claim.AttemptId, ack, "h-invalid", 200, CancellationToken.None);

        var rejected = Assert.IsType<EtlBatchAckOutcome.Rejected>(outcome);
        Assert.Equal("ACK_INVALID", rejected.Code);
        Assert.Equal("rejected_ack", await ScalarStringAsync($"SELECT outcome FROM etl_batch_send_attempts WHERE attempt_id='{claim.AttemptId:D}'"));
        // The observation evidence is recorded on the rejected attempt.
        Assert.Equal("h-invalid", await ScalarStringAsync($"SELECT ack_payload_hash FROM etl_batch_send_attempts WHERE attempt_id='{claim.AttemptId:D}'"));
        Assert.Equal("dead_letter", await ScalarStringAsync($"SELECT status FROM etl_batches WHERE batch_id='{batchId:D}'"));
        Assert.Equal("ACK_INVALID", await ScalarStringAsync($"SELECT quarantine_code FROM etl_batches WHERE batch_id='{batchId:D}'"));
        Assert.Equal("blocked", await RunStatusAsync(runId));
        Assert.Equal("ACK_INVALID", await ScalarStringAsync($"SELECT finalize_conflict_code FROM etl_runs WHERE run_id='{runId:D}'"));
        Assert.Equal("blocked", await ScalarStringAsync($"SELECT status FROM etl_jobs WHERE run_id='{runId:D}'"));
        Assert.Equal(1, await ScalarAsync($"SELECT COUNT(*) FROM etl_entity_ownership WHERE owner_run_id='{runId:D}' AND released_at_utc IS NULL"));
        // The batch was never acknowledged.
        Assert.Null(await ScalarStringAsync($"SELECT acknowledged_at_utc FROM etl_batches WHERE batch_id='{batchId:D}'"));
        Assert.Equal("0", await ScalarStringAsync($"SELECT batches_acknowledged FROM etl_runs WHERE run_id='{runId:D}'"));
    }

    [Fact]
    public async Task Foreign_attempt_id_is_claim_lost_with_zero_writes()
    {
        var runId = await SealedRunAsync("clients");
        var batchId = await ReadyBatchIdAsync(runId);
        var claim = Assert.IsType<EtlBatchUploadClaimOutcome.Claimed>(await _store.TryClaimBatchUploadAsync(batchId, "u", DateTimeOffset.UtcNow, 5, CancellationToken.None)).Claim;
        var foreign = Guid.NewGuid();
        var ack = new EtlBatchAckEvidence("acknowledged", batchId, 5, true, DateTimeOffset.UtcNow);

        Assert.IsType<EtlBatchAckOutcome.ClaimLost>(await _store.AcknowledgeClaimedBatchAsync(batchId, foreign, ack, "h", 200, CancellationToken.None));
        Assert.IsType<EtlBatchSendFailureOutcome.ClaimLost>(await _store.FailClaimedBatchSendAsync(batchId, foreign, "timeout", null, CancellationToken.None));
        Assert.IsType<EtlBatchSendRetryOutcome.ClaimLost>(await _store.RetryClaimedBatchSendAsync(batchId, foreign, "precheck", DateTimeOffset.UtcNow, CancellationToken.None));

        // Zero writes: the real attempt is still admitted, batch still uploading.
        Assert.Equal("admitted", await ScalarStringAsync($"SELECT outcome FROM etl_batch_send_attempts WHERE attempt_id='{claim.AttemptId:D}'"));
        Assert.Equal("uploading", await ScalarStringAsync($"SELECT status FROM etl_batches WHERE batch_id='{batchId:D}'"));
        Assert.Equal("uploading", await RunStatusAsync(runId));
        // A foreign BATCH id pair is likewise lost.
        Assert.IsType<EtlBatchAckOutcome.ClaimLost>(await _store.AcknowledgeClaimedBatchAsync(Guid.NewGuid(), claim.AttemptId, ack, "h", 200, CancellationToken.None));
    }

    // ---------- ACK / fail ordering races ----------

    [Fact]
    public async Task Late_failure_report_for_an_acknowledged_attempt_is_a_noop()
    {
        var runId = await SealedRunAsync("clients");
        var batchId = await ReadyBatchIdAsync(runId);
        var claim = Assert.IsType<EtlBatchUploadClaimOutcome.Claimed>(await _store.TryClaimBatchUploadAsync(batchId, "u", DateTimeOffset.UtcNow, 5, CancellationToken.None)).Claim;
        var ack = new EtlBatchAckEvidence("acknowledged", batchId, 5, true, DateTimeOffset.UtcNow);
        Assert.IsType<EtlBatchAckOutcome.Acknowledged>(await _store.AcknowledgeClaimedBatchAsync(batchId, claim.AttemptId, ack, "h1", 200, CancellationToken.None));

        // ACK/fail race — failure arrives after the ACK committed: the attempt is
        // terminal, so the report is a zero-write no-op; nothing is re-blocked.
        var late = await _store.FailClaimedBatchSendAsync(batchId, claim.AttemptId, "late timeout report", null, CancellationToken.None);

        Assert.IsType<EtlBatchSendFailureOutcome.ClaimLost>(late);
        Assert.Equal("acknowledged", await ScalarStringAsync($"SELECT status FROM etl_batches WHERE batch_id='{batchId:D}'"));
        Assert.Equal("acknowledged", await ScalarStringAsync($"SELECT outcome FROM etl_batch_send_attempts WHERE attempt_id='{claim.AttemptId:D}'"));
        Assert.Equal("uploading", await RunStatusAsync(runId));
        Assert.Equal("running", await ScalarStringAsync($"SELECT status FROM etl_jobs WHERE run_id='{runId:D}'"));
        Assert.Equal("1", await ScalarStringAsync($"SELECT batches_acknowledged FROM etl_runs WHERE run_id='{runId:D}'"));
    }

    [Fact]
    public async Task Late_ack_after_fail_closed_quarantine_is_evidence_only()
    {
        var runId = await SealedRunAsync("clients");
        var batchId = await ReadyBatchIdAsync(runId);
        var claim = Assert.IsType<EtlBatchUploadClaimOutcome.Claimed>(await _store.TryClaimBatchUploadAsync(batchId, "u", DateTimeOffset.UtcNow, 5, CancellationToken.None)).Claim;
        Assert.IsType<EtlBatchSendFailureOutcome.Blocked>(await _store.FailClaimedBatchSendAsync(batchId, claim.AttemptId, "timeout", null, CancellationToken.None));

        // The ACK arrives after the block committed: the (batch,attempt) row exists so
        // the observation is recorded on the attempt ONLY — batch/run/job/ownership
        // untouched; a late ACK never resurrects and never releases.
        var ack = new EtlBatchAckEvidence("acknowledged", batchId, 5, true, DateTimeOffset.UtcNow);
        var outcome = await _store.AcknowledgeClaimedBatchAsync(batchId, claim.AttemptId, ack, "h-late", 200, CancellationToken.None);

        Assert.IsType<EtlBatchAckOutcome.LateEvidenceRecorded>(outcome);
        Assert.Equal("h-late", await ScalarStringAsync($"SELECT ack_payload_hash FROM etl_batch_send_attempts WHERE attempt_id='{claim.AttemptId:D}'"));
        Assert.NotNull(await ScalarStringAsync($"SELECT ack_observed_at_utc FROM etl_batch_send_attempts WHERE attempt_id='{claim.AttemptId:D}'"));
        Assert.Equal("unknown", await ScalarStringAsync($"SELECT outcome FROM etl_batch_send_attempts WHERE attempt_id='{claim.AttemptId:D}'"));
        Assert.Equal("dead_letter", await ScalarStringAsync($"SELECT status FROM etl_batches WHERE batch_id='{batchId:D}'"));
        Assert.Equal("blocked", await RunStatusAsync(runId));
        Assert.Null(await ScalarStringAsync($"SELECT acknowledged_at_utc FROM etl_batches WHERE batch_id='{batchId:D}'"));
        Assert.Equal("0", await ScalarStringAsync($"SELECT batches_acknowledged FROM etl_runs WHERE run_id='{runId:D}'"));
        Assert.Equal(1, await ScalarAsync($"SELECT COUNT(*) FROM etl_entity_ownership WHERE owner_run_id='{runId:D}' AND released_at_utc IS NULL"));
    }

    [Fact]
    public async Task Invalid_late_ack_body_is_recorded_as_evidence_never_applied()
    {
        var runId = await SealedRunAsync("clients");
        var batchId = await ReadyBatchIdAsync(runId);
        var claim = Assert.IsType<EtlBatchUploadClaimOutcome.Claimed>(await _store.TryClaimBatchUploadAsync(batchId, "u", DateTimeOffset.UtcNow, 5, CancellationToken.None)).Claim;
        Assert.IsType<EtlBatchSendFailureOutcome.Blocked>(await _store.FailClaimedBatchSendAsync(batchId, claim.AttemptId, "timeout", null, CancellationToken.None));

        // An INVALID late ACK is still real evidence belonging to the attempt — the
        // same field validation runs, it is recorded, never applied.
        var badAck = new EtlBatchAckEvidence("weird", Guid.NewGuid(), 999, false, null);
        var outcome = await _store.AcknowledgeClaimedBatchAsync(batchId, claim.AttemptId, badAck, "h-bad", 500, CancellationToken.None);

        Assert.IsType<EtlBatchAckOutcome.LateEvidenceRecorded>(outcome);
        Assert.Equal("h-bad", await ScalarStringAsync($"SELECT ack_payload_hash FROM etl_batch_send_attempts WHERE attempt_id='{claim.AttemptId:D}'"));
        Assert.Equal("unknown", await ScalarStringAsync($"SELECT outcome FROM etl_batch_send_attempts WHERE attempt_id='{claim.AttemptId:D}'"));
        Assert.Equal("dead_letter", await ScalarStringAsync($"SELECT status FROM etl_batches WHERE batch_id='{batchId:D}'"));
    }

    [Fact]
    public async Task Exact_ack_replay_preserves_the_first_observation()
    {
        var runId = await SealedRunAsync("clients");
        var batchId = await ReadyBatchIdAsync(runId);
        var claim = Assert.IsType<EtlBatchUploadClaimOutcome.Claimed>(await _store.TryClaimBatchUploadAsync(batchId, "u", DateTimeOffset.UtcNow, 5, CancellationToken.None)).Claim;
        var ack = new EtlBatchAckEvidence("acknowledged", batchId, 5, true, DateTimeOffset.UtcNow);
        Assert.IsType<EtlBatchAckOutcome.Acknowledged>(await _store.AcknowledgeClaimedBatchAsync(batchId, claim.AttemptId, ack, "h-first", 200, CancellationToken.None));
        var observedAt = await ScalarStringAsync($"SELECT ack_observed_at_utc FROM etl_batch_send_attempts WHERE attempt_id='{claim.AttemptId:D}'");

        // Exact replay: the recorded first observation is preserved byte-for-byte.
        var replay = await _store.AcknowledgeClaimedBatchAsync(batchId, claim.AttemptId, ack, "h-first", 200, CancellationToken.None);

        Assert.IsType<EtlBatchAckOutcome.AlreadyObserved>(replay);
        Assert.Equal("h-first", await ScalarStringAsync($"SELECT ack_payload_hash FROM etl_batch_send_attempts WHERE attempt_id='{claim.AttemptId:D}'"));
        Assert.Equal(observedAt, await ScalarStringAsync($"SELECT ack_observed_at_utc FROM etl_batch_send_attempts WHERE attempt_id='{claim.AttemptId:D}'"));
        Assert.Equal("1", await ScalarStringAsync($"SELECT batches_acknowledged FROM etl_runs WHERE run_id='{runId:D}'"));
    }

    [Fact]
    public async Task Conflicting_late_ack_observation_is_explicit_and_never_overwrites()
    {
        var runId = await SealedRunAsync("clients");
        var batchId = await ReadyBatchIdAsync(runId);
        var claim = Assert.IsType<EtlBatchUploadClaimOutcome.Claimed>(await _store.TryClaimBatchUploadAsync(batchId, "u", DateTimeOffset.UtcNow, 5, CancellationToken.None)).Claim;
        var ack = new EtlBatchAckEvidence("acknowledged", batchId, 5, true, DateTimeOffset.UtcNow);
        Assert.IsType<EtlBatchAckOutcome.Acknowledged>(await _store.AcknowledgeClaimedBatchAsync(batchId, claim.AttemptId, ack, "h-first", 200, CancellationToken.None));

        // A conflicting later observation on the same (batch,attempt) is reported
        // explicitly — evidence is never overwritten in place.
        var conflict = await _store.AcknowledgeClaimedBatchAsync(batchId, claim.AttemptId, ack, "h-different", 200, CancellationToken.None);

        Assert.IsType<EtlBatchAckOutcome.ObservationConflict>(conflict);
        Assert.Equal("h-first", await ScalarStringAsync($"SELECT ack_payload_hash FROM etl_batch_send_attempts WHERE attempt_id='{claim.AttemptId:D}'"));
    }

    [Fact]
    public async Task Admitted_attempt_on_a_dead_fence_records_outcome_as_evidence_only()
    {
        // Both batches admitted, then the run blocks on the first's unknown outcome —
        // the second still-'admitted' attempt's late failure lands as 'unknown'
        // evidence on the attempt row only.
        var runId = await NewRunAsync("clients", "orders");
        await BeginExtractCompleteAsync(runId, "clients", CursorJson(FinalClients), 5, ClaimOf(runId));
        await BeginExtractCompleteAsync(runId, "orders", CursorJson(FinalOrders), 7, ClaimOf(runId));
        Assert.IsType<EtlRunSealOutcome.Sealed>(await _store.SealEtlRunExtractionAsync(runId, ClaimOf(runId), CancellationToken.None));
        var batchA = await ReadyBatchIdAsync(runId, "clients");
        var batchB = await ReadyBatchIdAsync(runId, "orders");
        var claimA = Assert.IsType<EtlBatchUploadClaimOutcome.Claimed>(await _store.TryClaimBatchUploadAsync(batchA, "u", DateTimeOffset.UtcNow, 5, CancellationToken.None)).Claim;
        var claimB = Assert.IsType<EtlBatchUploadClaimOutcome.Claimed>(await _store.TryClaimBatchUploadAsync(batchB, "u", DateTimeOffset.UtcNow, 5, CancellationToken.None)).Claim;

        Assert.IsType<EtlBatchSendFailureOutcome.Blocked>(await _store.FailClaimedBatchSendAsync(batchA, claimA.AttemptId, "timeout", null, CancellationToken.None));
        // batchB was fenced to dead_letter by the eager block — its attempt stays
        // 'admitted' (still-live evidence) until a late outcome lands.
        Assert.Equal("dead_letter", await ScalarStringAsync($"SELECT status FROM etl_batches WHERE batch_id='{batchB:D}'"));
        Assert.Equal("admitted", await ScalarStringAsync($"SELECT outcome FROM etl_batch_send_attempts WHERE attempt_id='{claimB.AttemptId:D}'"));

        var late = await _store.FailClaimedBatchSendAsync(batchB, claimB.AttemptId, "late transport error", 502, CancellationToken.None);

        Assert.IsType<EtlBatchSendFailureOutcome.LateOutcomeRecorded>(late);
        Assert.Equal("unknown", await ScalarStringAsync($"SELECT outcome FROM etl_batch_send_attempts WHERE attempt_id='{claimB.AttemptId:D}'"));
        Assert.Equal("502", await ScalarStringAsync($"SELECT http_status FROM etl_batch_send_attempts WHERE attempt_id='{claimB.AttemptId:D}'"));
        Assert.Equal("dead_letter", await ScalarStringAsync($"SELECT status FROM etl_batches WHERE batch_id='{batchB:D}'"));
        Assert.Equal("blocked", await RunStatusAsync(runId));
        Assert.Equal(2, await ScalarAsync($"SELECT COUNT(*) FROM etl_entity_ownership WHERE owner_run_id='{runId:D}' AND released_at_utc IS NULL"));
    }

    // ---------- controlled-vs-thrown transaction discipline ----------

    [Fact]
    public async Task Swallowed_ledger_insert_rolls_back_the_flip_and_commits_a_block()
    {
        var runId = await SealedRunAsync("clients");
        var batchId = await ReadyBatchIdAsync(runId);
        // A real trigger that swallows the ledger write (controlled zero-row outcome).
        await ExecuteSqlAsync("CREATE TRIGGER swallow_attempt BEFORE INSERT ON etl_batch_send_attempts BEGIN SELECT RAISE(IGNORE); END;");

        var outcome = await _store.TryClaimBatchUploadAsync(batchId, "u", DateTimeOffset.UtcNow, 5, CancellationToken.None);

        var blocked = Assert.IsType<EtlBatchUploadClaimOutcome.Blocked>(outcome);
        Assert.Equal("SEND_LEDGER_LOST", blocked.Code);
        // Savepoint rolled back the flip; the committed block dead-letters the batch —
        // a batch that cannot be ledgered can never be sent. No attempt row exists.
        Assert.Equal("dead_letter", await ScalarStringAsync($"SELECT status FROM etl_batches WHERE batch_id='{batchId:D}'"));
        Assert.Equal("SEND_LEDGER_LOST", await ScalarStringAsync($"SELECT quarantine_code FROM etl_batches WHERE batch_id='{batchId:D}'"));
        Assert.Equal(0, await ScalarAsync($"SELECT COUNT(*) FROM etl_batch_send_attempts WHERE batch_id='{batchId:D}'"));
        Assert.Equal("blocked", await RunStatusAsync(runId));
        Assert.Equal("SEND_LEDGER_LOST", await ScalarStringAsync($"SELECT finalize_conflict_code FROM etl_runs WHERE run_id='{runId:D}'"));
        Assert.Equal(1, await ScalarAsync($"SELECT COUNT(*) FROM etl_entity_ownership WHERE owner_run_id='{runId:D}' AND released_at_utc IS NULL"));
    }

    [Fact]
    public async Task Thrown_ledger_insert_error_rolls_back_the_whole_claim_transaction()
    {
        var runId = await SealedRunAsync("clients");
        var batchId = await ReadyBatchIdAsync(runId);
        await ExecuteSqlAsync("CREATE TRIGGER abort_attempt BEFORE INSERT ON etl_batch_send_attempts BEGIN SELECT RAISE(ABORT, 'attempt write sabotaged'); END;");

        // A thrown SQL error must roll back the WHOLE transaction — the batch stays
        // 'ready' and claimable; no half-claimed state, no block.
        var ex = await Assert.ThrowsAsync<SqliteException>(() => _store.TryClaimBatchUploadAsync(batchId, "u", DateTimeOffset.UtcNow, 5, CancellationToken.None));
        Assert.Equal("ready", await ScalarStringAsync($"SELECT status FROM etl_batches WHERE batch_id='{batchId:D}'"));
        Assert.Null(await ScalarStringAsync($"SELECT send_attempt_id FROM etl_batches WHERE batch_id='{batchId:D}'"));
        Assert.Equal(0, await ScalarAsync($"SELECT COUNT(*) FROM etl_batch_send_attempts WHERE batch_id='{batchId:D}'"));
        Assert.Equal("uploading", await RunStatusAsync(runId));

        await ExecuteSqlAsync("DROP TRIGGER abort_attempt;");
        Assert.IsType<EtlBatchUploadClaimOutcome.Claimed>(await _store.TryClaimBatchUploadAsync(batchId, "u", DateTimeOffset.UtcNow, 5, CancellationToken.None));
    }

    // ---------- due enumeration ----------

    [Fact]
    public async Task Due_enumeration_applies_eligibility_before_limit()
    {
        var runId = await NewRunAsync("clients", "orders");
        await BeginExtractCompleteAsync(runId, "clients", CursorJson(FinalClients), 5, ClaimOf(runId));
        await BeginExtractCompleteAsync(runId, "orders", CursorJson(FinalOrders), 7, ClaimOf(runId));
        Assert.IsType<EtlRunSealOutcome.Sealed>(await _store.SealEtlRunExtractionAsync(runId, ClaimOf(runId), CancellationToken.None));
        var clientsBatch = await ReadyBatchIdAsync(runId, "clients");
        var ordersBatch = await ReadyBatchIdAsync(runId, "orders");

        // A not-yet-due retry_waiting batch is not surfaced.
        await ExecuteSqlAsync("UPDATE etl_batches SET status='retry_waiting', next_attempt_at_utc=$future WHERE batch_id=$b;", ("$future", DateTimeOffset.UtcNow.AddHours(1).ToString("O")), ("$b", ordersBatch.ToString("D")));
        var due = await _store.GetDueBatchUploadsAsync(10, DateTimeOffset.UtcNow, CancellationToken.None);
        Assert.Equal(new[] { clientsBatch }, due.Select(d => d.BatchId).ToArray());

        // A due retry_waiting batch is surfaced again (ledger-proven precheck retry).
        await ExecuteSqlAsync("UPDATE etl_batches SET next_attempt_at_utc=$past WHERE batch_id=$b;", ("$past", DateTimeOffset.UtcNow.AddHours(-1).ToString("O")), ("$b", ordersBatch.ToString("D")));
        Assert.Equal(2, (await _store.GetDueBatchUploadsAsync(10, DateTimeOffset.UtcNow, CancellationToken.None)).Count);

        // Eligibility is evaluated before LIMIT: a 'ready' batch of a blocked run
        // never consumes a slot.
        var blockedRun = await NewRunAsync("payments");
        Assert.IsType<EtlEntityBeginOutcome.Begun>(await _store.BeginEtlEntityExtractionAsync(blockedRun, ClaimOf(blockedRun), Request("payments"), CancellationToken.None));
        var blockedBatch = MakeBatch(blockedRun, "payments", 3);
        Assert.IsType<EtlBatchRegistrationOutcome.Registered>(await _store.RegisterGuardedEtlBatchAsync(blockedBatch, ClaimOf(blockedRun), CancellationToken.None));
        await ExecuteSqlAsync("UPDATE etl_runs SET status='blocked' WHERE run_id=$run;", ("$run", blockedRun.ToString("D")));
        var page = await _store.GetDueBatchUploadsAsync(2, DateTimeOffset.UtcNow, CancellationToken.None);
        Assert.Equal(2, page.Count);
        Assert.DoesNotContain(page, d => d.BatchId == blockedBatch.BatchId);
    }

    // ---------- recovery ----------

    [Fact]
    public async Task Recovery_orphans_admitted_attempts_and_quarantines_in_flight_uploads()
    {
        var runId = await SealedRunAsync("clients");
        var batchId = await ReadyBatchIdAsync(runId);
        var claim = Assert.IsType<EtlBatchUploadClaimOutcome.Claimed>(await _store.TryClaimBatchUploadAsync(batchId, "u", DateTimeOffset.UtcNow, 5, CancellationToken.None)).Claim;
        // A legacy pre-ledger 'uploading' batch with no attempt row — the in-flight
        // status itself is the unknown outcome; it quarantines identically.
        var legacyRunId = await NewRunAsync("payments");
        var legacyBatch = Guid.NewGuid();
        await ExecuteSqlAsync(
            "UPDATE etl_runs SET status='uploading' WHERE run_id=$run; INSERT INTO etl_batches(batch_id,run_id,entity_name,schema_version,file_path,status,row_count,sha256,compressed_size,uncompressed_size,created_at_utc) VALUES($b,$run,'payments',1,'spool/legacy.gz','uploading',4,'h',10,10,$now);",
            ("$b", legacyBatch.ToString("D")), ("$run", legacyRunId.ToString("D")), ("$now", Now()));

        var recovery = await _store.RecoverInterruptedEtlRunsAsync(CancellationToken.None);

        Assert.True(recovery.AttemptsOrphaned >= 1);
        Assert.Equal("orphaned", await ScalarStringAsync($"SELECT outcome FROM etl_batch_send_attempts WHERE attempt_id='{claim.AttemptId:D}'"));
        Assert.NotNull(await ScalarStringAsync($"SELECT finished_at_utc FROM etl_batch_send_attempts WHERE attempt_id='{claim.AttemptId:D}'"));
        // A new-path 'uploading' batch is NEVER reset to 'ready' — it is quarantined.
        Assert.Equal("dead_letter", await ScalarStringAsync($"SELECT status FROM etl_batches WHERE batch_id='{batchId:D}'"));
        Assert.Equal("UPLOAD_OUTCOME_UNKNOWN", await ScalarStringAsync($"SELECT quarantine_code FROM etl_batches WHERE batch_id='{batchId:D}'"));
        Assert.Equal("blocked", await RunStatusAsync(runId));
        Assert.Equal("UPLOAD_OUTCOME_UNKNOWN", await ScalarStringAsync($"SELECT finalize_conflict_code FROM etl_runs WHERE run_id='{runId:D}'"));
        Assert.Equal("blocked", await ScalarStringAsync($"SELECT status FROM etl_jobs WHERE run_id='{runId:D}'"));
        // Legacy in-flight batch quarantined the same way; its run blocked.
        Assert.Equal("dead_letter", await ScalarStringAsync($"SELECT status FROM etl_batches WHERE batch_id='{legacyBatch:D}'"));
        Assert.Equal("UPLOAD_OUTCOME_UNKNOWN", await ScalarStringAsync($"SELECT quarantine_code FROM etl_batches WHERE batch_id='{legacyBatch:D}'"));
        Assert.Equal("blocked", await RunStatusAsync(legacyRunId));
        // Ownership + bindings retained through recovery on every blocked run.
        Assert.Equal(1, await ScalarAsync($"SELECT COUNT(*) FROM etl_entity_ownership WHERE owner_run_id='{runId:D}' AND released_at_utc IS NULL"));
        Assert.Equal(1, await ScalarAsync($"SELECT COUNT(*) FROM etl_run_ownership_bindings WHERE run_id='{runId:D}'"));
        Assert.Equal(1, await ScalarAsync($"SELECT COUNT(*) FROM etl_entity_ownership WHERE owner_run_id='{legacyRunId:D}' AND released_at_utc IS NULL"));
        // The orphaned attempt is terminal: a late outcome lands as a no-op.
        Assert.IsType<EtlBatchSendFailureOutcome.ClaimLost>(await _store.FailClaimedBatchSendAsync(batchId, claim.AttemptId, "late", null, CancellationToken.None));
    }

    // ---------- root review: the ledger, not the batch status, decides re-admission ----------

    [Fact]
    public async Task Legacy_recover_reset_then_late_failure_never_readmits_the_batch()
    {
        var runId = await SealedRunAsync("clients");
        var batchId = await ReadyBatchIdAsync(runId);
        var claim = Assert.IsType<EtlBatchUploadClaimOutcome.Claimed>(await _store.TryClaimBatchUploadAsync(batchId, "u", DateTimeOffset.UtcNow, 5, CancellationToken.None)).Claim;
        // Production startup order: legacy RecoverAsync resets 'uploading' -> 'ready'.
        await _store.RecoverAsync(CancellationToken.None);
        Assert.Equal("ready", await ScalarStringAsync($"SELECT status FROM etl_batches WHERE batch_id='{batchId:D}'"));

        // The uncertain outcome of the admitted attempt lands after the reset.
        await _store.FailClaimedBatchSendAsync(batchId, claim.AttemptId, "timeout", null, CancellationToken.None);

        Assert.Empty(await _store.GetDueBatchUploadsAsync(10, DateTimeOffset.UtcNow, CancellationToken.None));
        Assert.IsNotType<EtlBatchUploadClaimOutcome.Claimed>(await _store.TryClaimBatchUploadAsync(batchId, "u2", DateTimeOffset.UtcNow, 5, CancellationToken.None));
        Assert.Equal(1, await ScalarAsync($"SELECT COUNT(*) FROM etl_batch_send_attempts WHERE batch_id='{batchId:D}'"));
        Assert.Equal("dead_letter", await ScalarStringAsync($"SELECT status FROM etl_batches WHERE batch_id='{batchId:D}'"));
        Assert.Equal("UPLOAD_OUTCOME_UNKNOWN", await ScalarStringAsync($"SELECT quarantine_code FROM etl_batches WHERE batch_id='{batchId:D}'"));
        Assert.Equal("blocked", await RunStatusAsync(runId));
        Assert.Equal(1, await ScalarAsync($"SELECT COUNT(*) FROM etl_entity_ownership WHERE owner_run_id='{runId:D}' AND released_at_utc IS NULL"));
    }

    [Fact]
    public async Task Legacy_retry_reset_then_late_failure_never_readmits_the_batch()
    {
        var runId = await SealedRunAsync("clients");
        var batchId = await ReadyBatchIdAsync(runId);
        var claim = Assert.IsType<EtlBatchUploadClaimOutcome.Claimed>(await _store.TryClaimBatchUploadAsync(batchId, "u", DateTimeOffset.UtcNow, 5, CancellationToken.None)).Claim;
        await _store.MarkBatchRetryAsync(batchId, "legacy retry", DateTimeOffset.UtcNow.AddMinutes(-1), CancellationToken.None);

        await _store.FailClaimedBatchSendAsync(batchId, claim.AttemptId, "timeout", null, CancellationToken.None);

        Assert.Empty(await _store.GetDueBatchUploadsAsync(10, DateTimeOffset.UtcNow, CancellationToken.None));
        Assert.IsNotType<EtlBatchUploadClaimOutcome.Claimed>(await _store.TryClaimBatchUploadAsync(batchId, "u2", DateTimeOffset.UtcNow, 5, CancellationToken.None));
        Assert.Equal(1, await ScalarAsync($"SELECT COUNT(*) FROM etl_batch_send_attempts WHERE batch_id='{batchId:D}'"));
        Assert.Equal("dead_letter", await ScalarStringAsync($"SELECT status FROM etl_batches WHERE batch_id='{batchId:D}'"));
        Assert.Equal("blocked", await RunStatusAsync(runId));
    }

    [Fact]
    public async Task Stray_due_batch_with_a_terminal_uncertain_attempt_is_quarantined_by_the_claim()
    {
        var runId = await SealedRunAsync("clients");
        var batchId = await ReadyBatchIdAsync(runId);
        var claim = Assert.IsType<EtlBatchUploadClaimOutcome.Claimed>(await _store.TryClaimBatchUploadAsync(batchId, "u", DateTimeOffset.UtcNow, 5, CancellationToken.None)).Claim;
        // Any writer that bypasses the ledger: attempt terminal-uncertain, batch due again.
        await ExecuteSqlAsync("UPDATE etl_batch_send_attempts SET outcome='orphaned', finished_at_utc=$now WHERE attempt_id=$a; UPDATE etl_batches SET status='ready' WHERE batch_id=$b;",
            ("$now", Now()), ("$a", claim.AttemptId.ToString("D")), ("$b", batchId.ToString("D")));

        Assert.Empty(await _store.GetDueBatchUploadsAsync(10, DateTimeOffset.UtcNow, CancellationToken.None));
        var outcome = await _store.TryClaimBatchUploadAsync(batchId, "u2", DateTimeOffset.UtcNow, 5, CancellationToken.None);

        Assert.IsType<EtlBatchUploadClaimOutcome.Blocked>(outcome);
        Assert.Equal(1, await ScalarAsync($"SELECT COUNT(*) FROM etl_batch_send_attempts WHERE batch_id='{batchId:D}'"));
        Assert.Equal("UPLOAD_OUTCOME_UNKNOWN", await ScalarStringAsync($"SELECT quarantine_code FROM etl_batches WHERE batch_id='{batchId:D}'"));
        Assert.Equal("blocked", await RunStatusAsync(runId));
    }

    [Fact]
    public async Task Recovery_after_legacy_reset_quarantines_the_batch_of_the_admitted_attempt()
    {
        var runId = await SealedRunAsync("clients");
        var batchId = await ReadyBatchIdAsync(runId);
        var claim = Assert.IsType<EtlBatchUploadClaimOutcome.Claimed>(await _store.TryClaimBatchUploadAsync(batchId, "u", DateTimeOffset.UtcNow, 5, CancellationToken.None)).Claim;
        await _store.RecoverAsync(CancellationToken.None);

        await _store.RecoverInterruptedEtlRunsAsync(CancellationToken.None);

        Assert.Equal("orphaned", await ScalarStringAsync($"SELECT outcome FROM etl_batch_send_attempts WHERE attempt_id='{claim.AttemptId:D}'"));
        Assert.Equal("dead_letter", await ScalarStringAsync($"SELECT status FROM etl_batches WHERE batch_id='{batchId:D}'"));
        Assert.Equal("UPLOAD_OUTCOME_UNKNOWN", await ScalarStringAsync($"SELECT quarantine_code FROM etl_batches WHERE batch_id='{batchId:D}'"));
        Assert.Equal("blocked", await RunStatusAsync(runId));
        Assert.IsNotType<EtlBatchUploadClaimOutcome.Claimed>(await _store.TryClaimBatchUploadAsync(batchId, "u2", DateTimeOffset.UtcNow, 5, CancellationToken.None));
    }

    [Fact]
    public async Task Recovery_blocks_only_runs_quarantined_in_this_pass()
    {
        var runId = await SealedRunAsync("clients");
        var batchId = await ReadyBatchIdAsync(runId);
        // Historical evidence: a batch quarantined long ago on a run that is active now
        // (state written directly — recovery must key on what it quarantines itself).
        await ExecuteSqlAsync("UPDATE etl_batches SET status='dead_letter', quarantine_code='UPLOAD_OUTCOME_UNKNOWN' WHERE batch_id=$b;", ("$b", batchId.ToString("D")));

        await _store.RecoverInterruptedEtlRunsAsync(CancellationToken.None);

        Assert.Equal("uploading", await RunStatusAsync(runId));
    }

    // ---------- root review: ACK evidence semantics ----------

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Late_ack_validity_is_recorded_with_the_observation(bool valid)
    {
        var runId = await SealedRunAsync("clients");
        var batchId = await ReadyBatchIdAsync(runId);
        var claim = Assert.IsType<EtlBatchUploadClaimOutcome.Claimed>(await _store.TryClaimBatchUploadAsync(batchId, "u", DateTimeOffset.UtcNow, 5, CancellationToken.None)).Claim;
        Assert.IsType<EtlBatchSendFailureOutcome.Blocked>(await _store.FailClaimedBatchSendAsync(batchId, claim.AttemptId, "timeout", null, CancellationToken.None));
        var ack = valid
            ? new EtlBatchAckEvidence("acknowledged", batchId, 5, true, DateTimeOffset.UtcNow)
            : new EtlBatchAckEvidence("weird", Guid.NewGuid(), 999, false, null);

        Assert.IsType<EtlBatchAckOutcome.LateEvidenceRecorded>(await _store.AcknowledgeClaimedBatchAsync(batchId, claim.AttemptId, ack, "h-late", 200, CancellationToken.None));

        Assert.Equal(valid ? 1 : 0, await ScalarAsync($"SELECT ack_valid FROM etl_batch_send_attempts WHERE attempt_id='{claim.AttemptId:D}'"));
        Assert.Equal("dead_letter", await ScalarStringAsync($"SELECT status FROM etl_batches WHERE batch_id='{batchId:D}'"));
    }

    [Fact]
    public async Task Applied_and_rejected_acks_record_validity()
    {
        var runA = await SealedRunAsync("clients");
        var batchA = await ReadyBatchIdAsync(runA);
        var claimA = Assert.IsType<EtlBatchUploadClaimOutcome.Claimed>(await _store.TryClaimBatchUploadAsync(batchA, "u", DateTimeOffset.UtcNow, 5, CancellationToken.None)).Claim;
        Assert.IsType<EtlBatchAckOutcome.Acknowledged>(await _store.AcknowledgeClaimedBatchAsync(batchA, claimA.AttemptId, new EtlBatchAckEvidence("acknowledged", batchA, 5, true, DateTimeOffset.UtcNow), "h", 200, CancellationToken.None));
        var runB = await SealedRunAsync("orders");
        var batchB = await ReadyBatchIdAsync(runB);
        var claimB = Assert.IsType<EtlBatchUploadClaimOutcome.Claimed>(await _store.TryClaimBatchUploadAsync(batchB, "u", DateTimeOffset.UtcNow, 5, CancellationToken.None)).Claim;
        Assert.IsType<EtlBatchAckOutcome.Rejected>(await _store.AcknowledgeClaimedBatchAsync(batchB, claimB.AttemptId, new EtlBatchAckEvidence("failed", batchB, 5, true, null), "h", 200, CancellationToken.None));

        Assert.Equal(1, await ScalarAsync($"SELECT ack_valid FROM etl_batch_send_attempts WHERE attempt_id='{claimA.AttemptId:D}'"));
        Assert.Equal(0, await ScalarAsync($"SELECT ack_valid FROM etl_batch_send_attempts WHERE attempt_id='{claimB.AttemptId:D}'"));
    }

    [Fact]
    public async Task Ack_for_a_precheck_failed_attempt_contradicts_the_attestation_and_blocks()
    {
        var runId = await SealedRunAsync("clients");
        var batchId = await ReadyBatchIdAsync(runId);
        var claim = Assert.IsType<EtlBatchUploadClaimOutcome.Claimed>(await _store.TryClaimBatchUploadAsync(batchId, "u", DateTimeOffset.UtcNow, 5, CancellationToken.None)).Claim;
        Assert.IsType<EtlBatchSendRetryOutcome.Scheduled>(await _store.RetryClaimedBatchSendAsync(batchId, claim.AttemptId, "spool read failed", DateTimeOffset.UtcNow.AddMinutes(-1), CancellationToken.None));

        // ERP acknowledges the attempt the worker attested as never sent.
        await _store.AcknowledgeClaimedBatchAsync(batchId, claim.AttemptId, new EtlBatchAckEvidence("acknowledged", batchId, 5, true, DateTimeOffset.UtcNow), "h", 200, CancellationToken.None);

        Assert.Equal("precheck_failed", await ScalarStringAsync($"SELECT outcome FROM etl_batch_send_attempts WHERE attempt_id='{claim.AttemptId:D}'"));
        Assert.Equal("h", await ScalarStringAsync($"SELECT ack_payload_hash FROM etl_batch_send_attempts WHERE attempt_id='{claim.AttemptId:D}'"));
        Assert.Equal("dead_letter", await ScalarStringAsync($"SELECT status FROM etl_batches WHERE batch_id='{batchId:D}'"));
        Assert.Equal("UPLOAD_OUTCOME_UNKNOWN", await ScalarStringAsync($"SELECT quarantine_code FROM etl_batches WHERE batch_id='{batchId:D}'"));
        Assert.Equal("blocked", await RunStatusAsync(runId));
        Assert.Equal("PRECHECK_ATTESTATION_CONTRADICTED", await ScalarStringAsync($"SELECT finalize_conflict_code FROM etl_runs WHERE run_id='{runId:D}'"));
        Assert.Empty(await _store.GetDueBatchUploadsAsync(10, DateTimeOffset.UtcNow, CancellationToken.None));
    }

    [Fact]
    public async Task Ack_for_a_precheck_failed_attempt_blocks_the_run_even_when_a_later_attempt_was_acknowledged()
    {
        var runId = await SealedRunAsync("clients");
        var batchId = await ReadyBatchIdAsync(runId);
        var first = Assert.IsType<EtlBatchUploadClaimOutcome.Claimed>(await _store.TryClaimBatchUploadAsync(batchId, "u", DateTimeOffset.UtcNow, 5, CancellationToken.None)).Claim;
        Assert.IsType<EtlBatchSendRetryOutcome.Scheduled>(await _store.RetryClaimedBatchSendAsync(batchId, first.AttemptId, "spool read failed", DateTimeOffset.UtcNow.AddMinutes(-1), CancellationToken.None));
        var second = Assert.IsType<EtlBatchUploadClaimOutcome.Claimed>(await _store.TryClaimBatchUploadAsync(batchId, "u", DateTimeOffset.UtcNow, 5, CancellationToken.None)).Claim;
        var ack = new EtlBatchAckEvidence("acknowledged", batchId, 5, true, DateTimeOffset.UtcNow);
        Assert.IsType<EtlBatchAckOutcome.Acknowledged>(await _store.AcknowledgeClaimedBatchAsync(batchId, second.AttemptId, ack, "h2", 200, CancellationToken.None));

        // ERP also acknowledges the attempt attested as never sent: the batch reached
        // ERP twice. The acknowledged batch stays evidence; the run must not finalize.
        Assert.IsType<EtlBatchAckOutcome.AttestationContradicted>(await _store.AcknowledgeClaimedBatchAsync(batchId, first.AttemptId, ack, "h1", 200, CancellationToken.None));

        Assert.Equal("acknowledged", await ScalarStringAsync($"SELECT status FROM etl_batches WHERE batch_id='{batchId:D}'"));
        Assert.Equal("blocked", await RunStatusAsync(runId));
        Assert.Equal("PRECHECK_ATTESTATION_CONTRADICTED", await ScalarStringAsync($"SELECT finalize_conflict_code FROM etl_runs WHERE run_id='{runId:D}'"));
        Assert.Equal("h1", await ScalarStringAsync($"SELECT ack_payload_hash FROM etl_batch_send_attempts WHERE attempt_id='{first.AttemptId:D}'"));
        Assert.Equal(1, await ScalarAsync($"SELECT COUNT(*) FROM etl_entity_ownership WHERE owner_run_id='{runId:D}' AND released_at_utc IS NULL"));
    }

    [Fact]
    public async Task Late_ack_on_a_legacy_reset_batch_quarantines_it_and_blocks_the_run()
    {
        var runId = await SealedRunAsync("clients");
        var batchId = await ReadyBatchIdAsync(runId);
        var claim = Assert.IsType<EtlBatchUploadClaimOutcome.Claimed>(await _store.TryClaimBatchUploadAsync(batchId, "u", DateTimeOffset.UtcNow, 5, CancellationToken.None)).Claim;
        await _store.MarkBatchRetryAsync(batchId, "legacy retry", DateTimeOffset.UtcNow.AddMinutes(-1), CancellationToken.None);

        var outcome = await _store.AcknowledgeClaimedBatchAsync(batchId, claim.AttemptId, new EtlBatchAckEvidence("acknowledged", batchId, 5, true, DateTimeOffset.UtcNow), "h", 200, CancellationToken.None);

        Assert.IsType<EtlBatchAckOutcome.LateEvidenceRecorded>(outcome);
        Assert.Equal(1, await ScalarAsync($"SELECT ack_valid FROM etl_batch_send_attempts WHERE attempt_id='{claim.AttemptId:D}'"));
        Assert.Equal("dead_letter", await ScalarStringAsync($"SELECT status FROM etl_batches WHERE batch_id='{batchId:D}'"));
        Assert.Equal("UPLOAD_OUTCOME_UNKNOWN", await ScalarStringAsync($"SELECT quarantine_code FROM etl_batches WHERE batch_id='{batchId:D}'"));
        Assert.Equal("blocked", await RunStatusAsync(runId));
        Assert.Equal("0", await ScalarStringAsync($"SELECT batches_acknowledged FROM etl_runs WHERE run_id='{runId:D}'"));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("  ")]
    public async Task Ack_without_a_payload_hash_is_refused(string? hash)
    {
        var runId = await SealedRunAsync("clients");
        var batchId = await ReadyBatchIdAsync(runId);
        var claim = Assert.IsType<EtlBatchUploadClaimOutcome.Claimed>(await _store.TryClaimBatchUploadAsync(batchId, "u", DateTimeOffset.UtcNow, 5, CancellationToken.None)).Claim;

        await Assert.ThrowsAnyAsync<ArgumentException>(() => _store.AcknowledgeClaimedBatchAsync(batchId, claim.AttemptId, new EtlBatchAckEvidence("acknowledged", batchId, 5, true, null), hash, 200, CancellationToken.None));

        Assert.Equal("admitted", await ScalarStringAsync($"SELECT outcome FROM etl_batch_send_attempts WHERE attempt_id='{claim.AttemptId:D}'"));
        Assert.Equal("uploading", await ScalarStringAsync($"SELECT status FROM etl_batches WHERE batch_id='{batchId:D}'"));
    }

    [Fact]
    public async Task Schema_rejects_unknown_quarantine_codes_and_ack_validity_values()
    {
        var runId = await SealedRunAsync("clients");
        var batchId = await ReadyBatchIdAsync(runId);
        var claim = Assert.IsType<EtlBatchUploadClaimOutcome.Claimed>(await _store.TryClaimBatchUploadAsync(batchId, "u", DateTimeOffset.UtcNow, 5, CancellationToken.None)).Claim;

        await Assert.ThrowsAsync<SqliteException>(() => ExecuteSqlAsync("UPDATE etl_batches SET quarantine_code='BOGUS' WHERE batch_id=$b;", ("$b", batchId.ToString("D"))));
        await Assert.ThrowsAsync<SqliteException>(() => ExecuteSqlAsync("UPDATE etl_batch_send_attempts SET ack_valid=2 WHERE attempt_id=$a;", ("$a", claim.AttemptId.ToString("D"))));
        foreach (var code in new[] { "UPLOAD_OUTCOME_UNKNOWN", "ACK_INVALID", "UPLOAD_ATTEMPTS_EXHAUSTED", "RUN_BLOCKED", "RUN_FAILED", "SEND_LEDGER_LOST" })
        {
            await ExecuteSqlAsync("UPDATE etl_batches SET quarantine_code=$c WHERE batch_id=$b;", ("$c", code), ("$b", batchId.ToString("D")));
        }
    }

    [Fact]
    public async Task Failed_run_fences_its_batches_with_the_run_failed_code()
    {
        var runId = await NewRunAsync("clients");
        Assert.IsType<EtlEntityBeginOutcome.Begun>(await _store.BeginEtlEntityExtractionAsync(runId, ClaimOf(runId), Request("clients"), CancellationToken.None));
        var batch = MakeBatch(runId, "clients", 5);
        Assert.IsType<EtlBatchRegistrationOutcome.Registered>(await _store.RegisterGuardedEtlBatchAsync(batch, ClaimOf(runId), CancellationToken.None));

        await _store.FailEtlRunAsync(runId, ClaimOf(runId), "extraction failed", CancellationToken.None);

        Assert.Equal("dead_letter", await ScalarStringAsync($"SELECT status FROM etl_batches WHERE batch_id='{batch.BatchId:D}'"));
        Assert.Equal("RUN_FAILED", await ScalarStringAsync($"SELECT quarantine_code FROM etl_batches WHERE batch_id='{batch.BatchId:D}'"));
    }

    // ---------- retention / FK ----------

    [Fact]
    public async Task Attempt_rows_of_unresolved_runs_are_undeletable()
    {
        var runId = await SealedRunAsync("clients");
        var batchId = await ReadyBatchIdAsync(runId);
        var claim = Assert.IsType<EtlBatchUploadClaimOutcome.Claimed>(await _store.TryClaimBatchUploadAsync(batchId, "u", DateTimeOffset.UtcNow, 5, CancellationToken.None)).Claim;
        Assert.IsType<EtlBatchSendFailureOutcome.Blocked>(await _store.FailClaimedBatchSendAsync(batchId, claim.AttemptId, "timeout", null, CancellationToken.None));

        await _store.CleanupAsync(DateTimeOffset.UtcNow.AddDays(1), DateTimeOffset.UtcNow.AddDays(1), CancellationToken.None);

        // Blocked-run evidence survives retention forever: batch + attempt rows intact.
        Assert.Equal(1, await ScalarAsync($"SELECT COUNT(*) FROM etl_batches WHERE batch_id='{batchId:D}'"));
        Assert.Equal(1, await ScalarAsync($"SELECT COUNT(*) FROM etl_batch_send_attempts WHERE attempt_id='{claim.AttemptId:D}'"));
        Assert.Equal("dead_letter", await ScalarStringAsync($"SELECT status FROM etl_batches WHERE batch_id='{batchId:D}'"));
    }

    [Fact]
    public async Task Succeeded_run_cleanup_deletes_attempt_rows_with_the_batch_no_fk_violation()
    {
        var runId = await SealedRunAsync("clients");
        var batchId = await ReadyBatchIdAsync(runId);
        var claim = Assert.IsType<EtlBatchUploadClaimOutcome.Claimed>(await _store.TryClaimBatchUploadAsync(batchId, "u", DateTimeOffset.UtcNow, 5, CancellationToken.None)).Claim;
        var ack = new EtlBatchAckEvidence("acknowledged", batchId, 5, true, DateTimeOffset.UtcNow);
        Assert.IsType<EtlBatchAckOutcome.Acknowledged>(await _store.AcknowledgeClaimedBatchAsync(batchId, claim.AttemptId, ack, "h", 200, CancellationToken.None));

        // Finalize the run to 'succeeded' (completion claim + finalize).
        var completion = Assert.IsType<EtlRunClaimOutcome.Claimed>(await _store.TryClaimRunCompletionAsync(runId, "finalizer", DateTimeOffset.UtcNow, 3, CancellationToken.None));
        Assert.IsType<EtlRunFinalizeOutcome.Finalized>(await _store.FinalizeEtlRunAsync(runId, completion.Claim.ClaimId, CancellationToken.None));
        Assert.Equal("succeeded", await RunStatusAsync(runId));
        await _store.MarkBatchDeletedAsync(batchId, CancellationToken.None);
        Assert.Equal("deleted", await ScalarStringAsync($"SELECT status FROM etl_batches WHERE batch_id='{batchId:D}'"));

        await _store.CleanupAsync(DateTimeOffset.UtcNow.AddDays(1), DateTimeOffset.UtcNow.AddDays(1), CancellationToken.None);

        // The batch row and its attempt rows leave in the SAME transaction — no FK
        // failure, no orphan attempt rows.
        Assert.Equal(0, await ScalarAsync($"SELECT COUNT(*) FROM etl_batches WHERE batch_id='{batchId:D}'"));
        Assert.Equal(0, await ScalarAsync($"SELECT COUNT(*) FROM etl_batch_send_attempts WHERE batch_id='{batchId:D}'"));
        Assert.Equal(1, await ScalarAsync($"SELECT COUNT(*) FROM pragma_foreign_key_list('etl_batch_send_attempts')"));
    }

    // ---------- helpers ----------

    private Guid ClaimOf(Guid runId) => _extractionClaims[runId];
    private static string Now() => DateTimeOffset.UtcNow.ToString("O");
    private static string CursorJson(EtlCursor cursor) => JsonSerializer.Serialize(cursor, JsonOptions);
    private static string DefinitionJson(string entity) =>
        JsonSerializer.Serialize(MakeEntities().Single(e => e.EntityCode == entity), JsonOptions);
    private static EtlEntityDefinition[] MakeEntities() =>
    [
        new("clients", "Catalog_Clients", "Ref_Key", "UpdatedAt", "DeletionMark", ["Ref_Key", "UpdatedAt", "DeletionMark"], "incremental", 500, 10),
        new("orders", "Document_Orders", "Ref_Key", "Date", "Posted", ["Ref_Key", "Date", "Posted"], "incremental", 500, 10),
        new("payments", "Document_Payments", "Ref_Key", "Date", "Posted", ["Ref_Key", "Date", "Posted"], "incremental", 500, 10)
    ];
    private static EtlEntityExtractionRequest Request(string entity) =>
        new(entity, DefinitionJson(entity), SourceNamespace, QueryMode,
            JsonSerializer.Serialize(new EtlCursor(DateTimeOffset.UtcNow, null), JsonOptions));
    private static EtlBatch MakeBatch(Guid runId, string entity, int rowCount) =>
        new(Guid.NewGuid(), runId, entity, 1, $"spool/{runId:N}-{entity}-{Guid.NewGuid():N}.gz", EtlBatchStatus.Ready, rowCount,
            null, FinalClients, "hash-" + Guid.NewGuid().ToString("N"), 1000, 4000, 0, DateTimeOffset.UtcNow);

    private async Task<(Guid RunId, Guid JobId)> NewPendingJobRunAsync(string mode, string[] entities)
    {
        var runId = Guid.NewGuid();
        var jobId = Guid.NewGuid();
        var created = Now();
        var manifest = JsonSerializer.Serialize(entities, JsonOptions);
        var definitions = JsonSerializer.Serialize(entities.Select(e => MakeEntities().Single(x => x.EntityCode == e)).ToArray(), JsonOptions);
        await ExecuteSqlAsync(
            "INSERT INTO etl_runs(run_id,mode,requested_entities_json,status,configuration_version,created_at_utc,updated_at_utc,row_version) VALUES($run,$mode,$manifest,'pending',7,$now,$now,1);",
            ("$run", runId.ToString("D")), ("$mode", mode), ("$manifest", manifest), ("$now", created));
        await ExecuteSqlAsync(
            "INSERT INTO etl_jobs(job_id,command_id,run_id,mode,entities_json,configuration_version,status,command_payload_hash,acceptance_result_json,created_at_utc,updated_at_utc,row_version) VALUES($job,$cmd,$run,$mode,$defs,7,'pending','hash','{}',$now,$now,1);",
            ("$job", jobId.ToString("D")), ("$cmd", Guid.NewGuid().ToString("D")), ("$run", runId.ToString("D")), ("$mode", mode), ("$defs", definitions), ("$now", created));
        return (runId, jobId);
    }

    private async Task<Guid> NewRunAsync(params string[] entities)
    {
        var (runId, jobId) = await NewPendingJobRunAsync(QueryMode, entities);
        var claimed = Assert.IsType<EtlJobClaimOutcome.Claimed>(
            await _store.TryClaimEtlJobAsync(jobId, "test-dispatcher", DateTimeOffset.UtcNow.AddMinutes(5), DateTimeOffset.UtcNow, CancellationToken.None));
        _extractionClaims[runId] = claimed.Claim.ExtractionClaimId;
        return runId;
    }

    // Full new-path pipeline: claim → extract one batch → seal → run 'uploading' with
    // one 'ready' batch and live bound-epoch ownership.
    private async Task<Guid> SealedRunAsync(string entity)
    {
        var runId = await NewRunAsync(entity);
        await BeginExtractCompleteAsync(runId, entity, CursorJson(FinalClients), 5, ClaimOf(runId));
        Assert.IsType<EtlRunSealOutcome.Sealed>(await _store.SealEtlRunExtractionAsync(runId, ClaimOf(runId), CancellationToken.None));
        return runId;
    }

    private async Task BeginExtractCompleteAsync(Guid runId, string entity, string finalJson, int batchRows, Guid claim)
    {
        Assert.IsType<EtlEntityBeginOutcome.Begun>(await _store.BeginEtlEntityExtractionAsync(runId, claim, Request(entity), CancellationToken.None));
        Assert.IsType<EtlBatchRegistrationOutcome.Registered>(await _store.RegisterGuardedEtlBatchAsync(MakeBatch(runId, entity, batchRows), claim, CancellationToken.None));
        Assert.IsType<EtlEntityCompletionOutcome.Completed>(await _store.CompleteEtlEntityExtractionAsync(runId, claim, entity, finalJson, 1, CancellationToken.None));
    }

    private async Task<Guid> ReadyBatchIdAsync(Guid runId, string? entity = null)
    {
        var filter = entity is null ? "" : $" AND entity_name='{entity}'";
        return Guid.Parse((await ScalarStringAsync($"SELECT batch_id FROM etl_batches WHERE run_id='{runId:D}' AND status='ready'{filter}"))!);
    }

    private async Task<string?> RunStatusAsync(Guid runId) => await ScalarStringAsync($"SELECT status FROM etl_runs WHERE run_id='{runId:D}'");

    private async Task<long> ScalarAsync(string sql)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        return Convert.ToInt64(await command.ExecuteScalarAsync(CancellationToken.None), CultureInfo.InvariantCulture);
    }

    private async Task<string?> ScalarStringAsync(string sql)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        var value = await command.ExecuteScalarAsync(CancellationToken.None);
        return value is null or DBNull ? null : Convert.ToString(value, CultureInfo.InvariantCulture);
    }

    private async Task ExecuteSqlAsync(string sql, params (string Name, object? Value)[] parameters)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        foreach (var (name, value) in parameters) command.Parameters.AddWithValue(name, value ?? DBNull.Value);
        await command.ExecuteNonQueryAsync(CancellationToken.None);
    }
}
