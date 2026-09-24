using ErpOnecAgent.Application.Abstractions;
using Xunit;
using Xunit.Abstractions;

namespace ReviewProbes;

/// <summary>
/// Probe C — every extraction mutation API must reject a corrupted
/// requested_entities_json with a typed rejection and ZERO writes, never by throwing a
/// raw malformed-json SQLite error out of json_each evaluation inside
/// RunOwnershipSetPredicate. The run is legitimately claimed (ownership + bindings +
/// live extraction claim for 'clients'), a 'clients' entity is mid-extraction with one
/// registered batch, then the manifest column is corrupted in place via SQL — a
/// mutation under a valid claim on corrupt durable evidence must fail closed.
/// </summary>
public sealed class ProbeCMalformedManifestTests(ITestOutputHelper output) : ProbeFixture
{
    [Theory]
    [InlineData("{malformed")]        // not JSON at all — raw json_each would throw if reached
    [InlineData("[]")]                // valid JSON, empty array
    [InlineData("null")]              // JSON null literal
    [InlineData("[null]")]            // array with a null element
    [InlineData("   ")]               // whitespace only
    [InlineData("{}")]                // valid JSON, non-array
    [InlineData("[1]")]               // valid JSON, non-text element
    [InlineData("[\"clients\",\"clients\"]")] // duplicate entries
    [InlineData(null)]                // SQL NULL column value
    public async Task Corrupt_manifest_rejects_all_extraction_mutations_with_zero_writes(string? corruptManifest)
    {
        // Legitimate setup: accepted job → claimed run → begun entity + registered batch.
        var (_, runId, claimId) = await AcceptAndClaimAsync("clients");
        await BeginEntityAsync(runId, claimId, "clients");
        await RegisterBatchAsync(runId, claimId, "clients", 5);

        var before = await RunStateAsync(runId);
        var runRowVersion = await LongAsync("SELECT row_version FROM etl_runs WHERE run_id=$run;", ("$run", runId.ToString("D")));
        var entityRowVersion = await LongAsync("SELECT row_version FROM etl_run_entities WHERE run_id=$run AND entity_name='clients';", ("$run", runId.ToString("D")));

        // Corruption injection (a CHECK-free TEXT column — the update itself must succeed).
        await ExecAsync("UPDATE etl_runs SET requested_entities_json=$m WHERE run_id=$run;",
            ("$m", corruptManifest), ("$run", runId.ToString("D")));

        // Every mutation API under the still-live claim must reject, not throw.
        var begin = await Store.BeginEtlEntityExtractionAsync(runId, claimId,
            new EtlEntityExtractionRequest("orders", DefinitionJson("orders"), SourceNamespace, JobMode, SnapshotJson()),
            CancellationToken.None);
        var register = await Store.RegisterGuardedEtlBatchAsync(MakeBatch(runId, "clients", 3), claimId, CancellationToken.None);
        var complete = await Store.CompleteEtlEntityExtractionAsync(runId, claimId, "clients", FinalJson(), 1, CancellationToken.None);
        var seal = await Store.SealEtlRunExtractionAsync(runId, claimId, CancellationToken.None);
        output.WriteLine($"manifest={(corruptManifest ?? "NULL")}: begin={begin}; register={register}; complete={complete}; seal={seal}");

        var beginRejected = Assert.IsType<EtlEntityBeginOutcome.Rejected>(begin);
        Assert.Equal(EtlEntityBeginRejection.OwnershipSetMismatch, beginRejected.Reason);
        var registerRejected = Assert.IsType<EtlBatchRegistrationOutcome.Rejected>(register);
        Assert.Equal(EtlBatchRegistrationRejection.OwnershipSetMismatch, registerRejected.Reason);
        var completeRejected = Assert.IsType<EtlEntityCompletionOutcome.Rejected>(complete);
        Assert.Equal(EtlEntityCompletionRejection.OwnershipSetMismatch, completeRejected.Reason);
        var sealRejected = Assert.IsType<EtlRunSealOutcome.Rejected>(seal);
        Assert.Equal(EtlRunSealRejection.OwnershipSetMismatch, sealRejected.Reason);

        // Zero writes: run row (claim fence intact, version unchanged), entity row,
        // batch set and ownership/bindings are all exactly as before the corruption.
        var after = await RunStateAsync(runId);
        output.WriteLine($"before={before}; after={after}");
        Assert.Equal(runRowVersion, await LongAsync("SELECT row_version FROM etl_runs WHERE run_id=$run;", ("$run", runId.ToString("D"))));
        Assert.Equal("running", await StringAsync("SELECT status FROM etl_runs WHERE run_id=$run;", ("$run", runId.ToString("D"))));
        Assert.Equal(claimId.ToString("D"), await StringAsync("SELECT extraction_claim_id FROM etl_runs WHERE run_id=$run;", ("$run", runId.ToString("D"))));
        Assert.Equal(entityRowVersion, await LongAsync("SELECT row_version FROM etl_run_entities WHERE run_id=$run AND entity_name='clients';", ("$run", runId.ToString("D"))));
        Assert.Equal("extracting", await StringAsync("SELECT status FROM etl_run_entities WHERE run_id=$run AND entity_name='clients';", ("$run", runId.ToString("D"))));
        Assert.Equal(1, await LongAsync("SELECT COUNT(*) FROM etl_batches WHERE run_id=$run;", ("$run", runId.ToString("D"))));
        Assert.Equal(1, await LongAsync("SELECT COUNT(*) FROM etl_entity_ownership WHERE owner_run_id=$run AND released_at_utc IS NULL;", ("$run", runId.ToString("D"))));
        Assert.Equal(1, await LongAsync("SELECT COUNT(*) FROM etl_run_ownership_bindings WHERE run_id=$run;", ("$run", runId.ToString("D"))));
    }
}
