using Xunit;
using Xunit.Abstractions;

namespace ReviewProbes;

/// <summary>
/// Probe E — eligibility enumeration BEFORE LIMIT. GetDispatchableEtlJobsAsync ranks
/// candidates by run (created_at_utc, run_id) and applies typed-manifest + elder-overlap
/// + ownership-availability predicates in SQL before LIMIT — but the frozen job identity
/// (job.entities_json manifest equality, mode, configuration_version vs the run) is
/// verified only inside TryClaimEtlJobAsync, not in the enumeration predicate or the
/// quarantine diagnostic scan. Normative expectation (root disposition: typed frozen
/// identity before LIMIT): a pending elder whose frozen job identity contradicts its run
/// must be omitted from dispatchable candidates AND surfaced in Quarantined, so a
/// disjoint younger job is never hidden behind LIMIT.
/// Suspect behavior under review: the corrupt elder is returned as the single dispatchable
/// job (LIMIT 1), hiding the eligible younger; the elder is additionally reported as
/// quarantined (entities corruption) or not reported at all (config drift).
/// </summary>
public sealed class ProbeEEnumerationTests(ITestOutputHelper output) : ProbeFixture
{
    private const string ElderCreated = "2026-09-24T00:00:00.0000000+00:00";
    private const string YoungerCreated = "2026-09-24T01:00:00.0000000+00:00";

    [Theory]
    [InlineData("entities", "[]")]                          // frozen job manifest emptied
    [InlineData("entities", "[{\"entityCode\":\"clients\"}]")] // code present but definition invalid
    [InlineData("config", "8")]                             // job configuration_version drift (run stays 7)
    public async Task Corrupt_elder_frozen_identity_enumeration_omits_and_reports(string axis, string value)
    {
        // Legitimate setup: elder pending job+run on 'clients', disjoint younger on
        // 'orders' — deterministic (created_at_utc, run_id) order, no ownership taken.
        var (elderJob, elderRun) = await AcceptJobAsync("clients");
        var (youngerJob, youngerRun) = await AcceptJobAsync("orders");
        await ExecAsync("UPDATE etl_runs SET created_at_utc=$c WHERE run_id=$run;",
            ("$c", ElderCreated), ("$run", elderRun.ToString("D")));
        await ExecAsync("UPDATE etl_runs SET created_at_utc=$c WHERE run_id=$run;",
            ("$c", YoungerCreated), ("$run", youngerRun.ToString("D")));

        // Corruption: the job's frozen identity no longer equals the (valid) run manifest.
        if (axis == "entities")
            await ExecAsync("UPDATE etl_jobs SET entities_json=$v WHERE job_id=$job;",
                ("$v", value), ("$job", elderJob.ToString("D")));
        else
            await ExecAsync("UPDATE etl_jobs SET configuration_version=$v WHERE job_id=$job;",
                ("$v", long.Parse(value, System.Globalization.CultureInfo.InvariantCulture)),
                ("$job", elderJob.ToString("D")));

        var jobsBefore = await StringAsync(
            "SELECT COALESCE(GROUP_CONCAT(v),'') FROM (SELECT job_id||':'||status||':'||row_version AS v FROM etl_jobs ORDER BY job_id);");
        var runsBefore = await StringAsync(
            "SELECT COALESCE(GROUP_CONCAT(v),'') FROM (SELECT run_id||':'||status||':'||row_version AS v FROM etl_runs ORDER BY run_id);");

        var page = await Store.GetDispatchableEtlJobsAsync(1, DateTimeOffset.UtcNow, CancellationToken.None);
        output.WriteLine($"axis={axis} jobs=[{string.Join(", ", page.Jobs.Select(j => $"{j.JobId:D}/run {j.RunId:D}"))}] " +
            $"quarantined=[{string.Join(", ", page.Quarantined.Select(q => $"{q.RunId:D}:{q.Code}"))}]");

        // Enumeration is strictly read-only — no row_version may move.
        Assert.Equal(jobsBefore, await StringAsync(
            "SELECT COALESCE(GROUP_CONCAT(v),'') FROM (SELECT job_id||':'||status||':'||row_version AS v FROM etl_jobs ORDER BY job_id);"));
        Assert.Equal(runsBefore, await StringAsync(
            "SELECT COALESCE(GROUP_CONCAT(v),'') FROM (SELECT run_id||':'||status||':'||row_version AS v FROM etl_runs ORDER BY run_id);"));

        // The corrupt elder must never be offered as dispatchable; the disjoint younger
        // is the only eligible job and must surface despite LIMIT=1.
        var jobIds = page.Jobs.Select(static j => j.JobId).ToList();
        Assert.DoesNotContain(elderJob, jobIds);
        Assert.Equal([youngerJob], jobIds);

        // The elder's frozen-identity corruption is quarantine evidence, surfaced even
        // though no elder candidate is eligible.
        Assert.Contains(page.Quarantined,
            q => q.RunId == elderRun && q.JobId == elderJob && q.Code == "MANIFEST_INVALID");
        Assert.DoesNotContain(page.Quarantined, q => q.RunId == youngerRun);
    }
}
