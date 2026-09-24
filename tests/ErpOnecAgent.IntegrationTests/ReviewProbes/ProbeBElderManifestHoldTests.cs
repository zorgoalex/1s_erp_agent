using ErpOnecAgent.Application.Abstractions;
using Xunit;
using Xunit.Abstractions;

namespace ReviewProbes;

/// <summary>
/// Probe B — malformed pending elder with UNPROVEN prior effects. The elder run's
/// manifest is corrupted post-acceptance and its job carries dispatch evidence
/// (dispatch_owner_id/dispatch_claimed_at_utc set, claim_attempt_count=0 — the sole
/// proof a dispatch pass touched it). Normative expectation (root disposition §3):
/// retirement of a corrupt pending elder requires durable proof it never started;
/// unresolved effects keep the admission hold — a younger overlapping claim must stay
/// deferred elder_manifest_invalid until explicit resolution.
///
/// Suspect behavior under review: the direct elder self-claim blocks the job via the
/// "unproven effects" branch — which also CLEARS dispatch_owner_id/dispatch_claimed_at_utc.
/// Once that sole evidence is gone, a later claim pass evaluates the same corrupt elder
/// as "provably inert" and quarantines it, silently releasing the reservation.
/// </summary>
public sealed class ProbeBElderManifestHoldTests(ITestOutputHelper output) : ProbeFixture
{
    private const string ElderCreated = "2026-09-24T00:00:00.0000000+00:00";
    private const string YoungerCreated = "2026-09-24T01:00:00.0000000+00:00";

    private async Task<(Guid ElderJob, Guid ElderRun, Guid YoungerJob, Guid YoungerRun)> ElderAndYoungerAsync()
    {
        var (elderJob, elderRun) = await AcceptJobAsync("clients");
        var (youngerJob, youngerRun) = await AcceptJobAsync("clients");
        // Deterministic overlap priority: fix the (created_at_utc, run_id) order key.
        await ExecAsync("UPDATE etl_runs SET created_at_utc=$c WHERE run_id=$run;",
            ("$c", ElderCreated), ("$run", elderRun.ToString("D")));
        await ExecAsync("UPDATE etl_runs SET created_at_utc=$c WHERE run_id=$run;",
            ("$c", YoungerCreated), ("$run", youngerRun.ToString("D")));
        return (elderJob, elderRun, youngerJob, youngerRun);
    }

    [Theory]
    [InlineData("{malformed")]
    [InlineData("{\"oops\":true}")]
    public async Task B_MalformedElder_SoleDispatchEvidenceCleared_YoungerStaysHeld(string elderManifest)
    {
        var (elderJob, elderRun, youngerJob, youngerRun) = await ElderAndYoungerAsync();
        await ExecAsync("UPDATE etl_runs SET requested_entities_json=$m WHERE run_id=$run;",
            ("$m", elderManifest), ("$run", elderRun.ToString("D")));
        // Sole prior-dispatch evidence on the elder job; committed-claim counter stays 0.
        await ExecAsync("UPDATE etl_jobs SET dispatch_owner_id='ghost-dispatcher', dispatch_claimed_at_utc=$t WHERE job_id=$job;",
            ("$t", "2026-09-24T00:30:00.0000000+00:00"), ("$job", elderJob.ToString("D")));

        // 1) Younger overlapping claim: held behind the unproven corrupt elder.
        var first = await Store.TryClaimEtlJobAsync(youngerJob, "dispatcher-1",
            DateTimeOffset.UtcNow, DateTimeOffset.UtcNow, CancellationToken.None);
        output.WriteLine($"younger claim #1 = {first}");
        var firstDeferred = Assert.IsType<EtlJobClaimOutcome.Deferred>(first);
        Assert.Equal(EtlJobDeferralReason.ElderManifestInvalid, firstDeferred.Reason);

        // 2) Direct claim of the elder job: corrupt manifest + unproven effects — the
        // job is diagnostically blocked but the pending run must RETAIN its admission hold.
        var direct = await Store.TryClaimEtlJobAsync(elderJob, "dispatcher-2",
            DateTimeOffset.UtcNow, DateTimeOffset.UtcNow, CancellationToken.None);
        output.WriteLine($"elder direct claim = {direct}");
        var directBlocked = Assert.IsType<EtlJobClaimOutcome.Blocked>(direct);
        Assert.Equal("JOB_MANIFEST_INCONSISTENT", directBlocked.Code);
        var elderRunStatus = await StringAsync("SELECT status FROM etl_runs WHERE run_id=$run;", ("$run", elderRun.ToString("D")));
        var elderDispatchEvidence = await StringAsync(
            "SELECT COALESCE(dispatch_owner_id,'NULL')||'/'||COALESCE(dispatch_claimed_at_utc,'NULL')||'/attempts='||claim_attempt_count FROM etl_jobs WHERE job_id=$job;",
            ("$job", elderJob.ToString("D")));
        output.WriteLine($"elder run={elderRunStatus}; elder job dispatch evidence after block = {elderDispatchEvidence}");
        Assert.Equal("pending", elderRunStatus);

        // 3) Younger retries: the elder had UNPROVEN effects — its reservation must still
        // hold; the corrupt pending elder is quarantined ONLY with never-started proof.
        var second = await Store.TryClaimEtlJobAsync(youngerJob, "dispatcher-3",
            DateTimeOffset.UtcNow, DateTimeOffset.UtcNow, CancellationToken.None);
        var elderState = await RunStateAsync(elderRun);
        var youngerState = await RunStateAsync(youngerRun);
        output.WriteLine($"younger claim #2 = {second}; elder {elderState}; younger {youngerState}");

        var secondDeferred = Assert.IsType<EtlJobClaimOutcome.Deferred>(second);
        Assert.Equal(EtlJobDeferralReason.ElderManifestInvalid, secondDeferred.Reason);
        Assert.Equal("pending", await StringAsync("SELECT status FROM etl_runs WHERE run_id=$run;", ("$run", elderRun.ToString("D"))));
        Assert.Equal("pending", await StringAsync("SELECT status FROM etl_runs WHERE run_id=$run;", ("$run", youngerRun.ToString("D"))));
        Assert.Equal(0, await LongAsync("SELECT COUNT(*) FROM etl_entity_ownership;"));
        Assert.Equal(0, await LongAsync("SELECT COUNT(*) FROM etl_run_ownership_bindings;"));
    }

    [Theory]
    [InlineData("{malformed")]
    [InlineData("{\"oops\":true}")]
    public async Task B_MalformedElder_ExistingBatch_YoungerStaysHeld(string elderManifest)
    {
        // Control: the elder's unproven effect is a durable batch row — evidence the
        // job-block path cannot clear. The hold must survive the identical sequence.
        var (elderJob, elderRun, youngerJob, youngerRun) = await ElderAndYoungerAsync();
        await ExecAsync("UPDATE etl_runs SET requested_entities_json=$m WHERE run_id=$run;",
            ("$m", elderManifest), ("$run", elderRun.ToString("D")));
        await ExecAsync("""
            INSERT INTO etl_batches(batch_id,run_id,entity_name,schema_version,file_path,status,row_count,sha256,compressed_size,uncompressed_size,attempt_count,created_at_utc)
            VALUES($id,$run,'clients',1,'spool/elder-stray.gz','ready',3,'h',10,40,0,$now);
            """, ("$id", Guid.NewGuid().ToString("D")), ("$run", elderRun.ToString("D")), ("$now", Now()));

        var first = await Store.TryClaimEtlJobAsync(youngerJob, "dispatcher-1",
            DateTimeOffset.UtcNow, DateTimeOffset.UtcNow, CancellationToken.None);
        output.WriteLine($"younger claim #1 = {first}");
        var firstDeferred = Assert.IsType<EtlJobClaimOutcome.Deferred>(first);
        Assert.Equal(EtlJobDeferralReason.ElderManifestInvalid, firstDeferred.Reason);

        var direct = await Store.TryClaimEtlJobAsync(elderJob, "dispatcher-2",
            DateTimeOffset.UtcNow, DateTimeOffset.UtcNow, CancellationToken.None);
        output.WriteLine($"elder direct claim = {direct}");
        Assert.IsType<EtlJobClaimOutcome.Blocked>(direct);
        Assert.Equal("pending", await StringAsync("SELECT status FROM etl_runs WHERE run_id=$run;", ("$run", elderRun.ToString("D"))));

        var second = await Store.TryClaimEtlJobAsync(youngerJob, "dispatcher-3",
            DateTimeOffset.UtcNow, DateTimeOffset.UtcNow, CancellationToken.None);
        var elderState = await RunStateAsync(elderRun);
        output.WriteLine($"younger claim #2 = {second}; elder {elderState}");

        var secondDeferred = Assert.IsType<EtlJobClaimOutcome.Deferred>(second);
        Assert.Equal(EtlJobDeferralReason.ElderManifestInvalid, secondDeferred.Reason);
        Assert.Equal("pending", await StringAsync("SELECT status FROM etl_runs WHERE run_id=$run;", ("$run", elderRun.ToString("D"))));
        Assert.Equal("pending", await StringAsync("SELECT status FROM etl_runs WHERE run_id=$run;", ("$run", youngerRun.ToString("D"))));
        Assert.Equal(0, await LongAsync("SELECT COUNT(*) FROM etl_entity_ownership;"));
        Assert.Equal(0, await LongAsync("SELECT COUNT(*) FROM etl_run_ownership_bindings;"));
    }
}
