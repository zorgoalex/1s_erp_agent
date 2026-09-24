using ErpOnecAgent.Application.Abstractions;
using Xunit;
using Xunit.Abstractions;

namespace ReviewProbes;

/// <summary>
/// Probe D — a run that legitimately owns ONLY 'clients' gets a stray 'extracting'
/// etl_run_entities row for 'orders' injected behind the API. RegisterGuardedEtlBatchAsync
/// gates on (run running + unsealed + live claim + exact ownership set) plus the entity
/// row being 'extracting' — but never re-checks that the batch entity is a member of the
/// manifest/owned set. Normative expectation under review: a batch for an unowned entity
/// must reject with zero writes. (Downstream, Seal's manifest==entity-row-set check
/// would catch the injected row — the question is whether the batch write itself is
/// fenced at registration.)
/// </summary>
public sealed class ProbeDStrayEntityTests(ITestOutputHelper output) : ProbeFixture
{
    [Fact]
    public async Task Stray_extracting_entity_row_cannot_register_batch_for_unowned_entity()
    {
        var (_, runId, claimId) = await AcceptAndClaimAsync("clients");
        await BeginEntityAsync(runId, claimId, "clients");

        // Corruption injection: an 'extracting' row for an entity the run never owned —
        // no manifest member, no binding, no ownership row for 'orders'.
        await ExecAsync("""
            INSERT INTO etl_run_entities(run_id,entity_name,entity_definition_json,domain_fingerprint,status,
                base_row_present,domain_status,rows_read,batches_created,created_at_utc,updated_at_utc,row_version)
            VALUES($run,'orders',$def,'injected','extracting',0,'absent',0,0,$now,$now,1);
            """,
            ("$run", runId.ToString("D")), ("$def", DefinitionJson("orders")), ("$now", Now()));

        var outcome = await Store.RegisterGuardedEtlBatchAsync(MakeBatch(runId, "orders", 7), claimId, CancellationToken.None);
        var strayBatches = await LongAsync("SELECT COUNT(*) FROM etl_batches WHERE run_id=$run AND entity_name='orders';", ("$run", runId.ToString("D")));
        var strayCounter = await LongAsync("SELECT batches_created FROM etl_run_entities WHERE run_id=$run AND entity_name='orders';", ("$run", runId.ToString("D")));
        output.WriteLine($"outcome={outcome}; orders batches={strayBatches}; orders batches_created={strayCounter}; {await RunStateAsync(runId)}");

        Assert.IsType<EtlBatchRegistrationOutcome.Rejected>(outcome);
        Assert.Equal(0, strayBatches);
        Assert.Equal(0, strayCounter);
    }

    [Fact]
    public async Task Stray_extracting_entity_row_cannot_complete_for_unowned_entity()
    {
        var (_, runId, claimId) = await AcceptAndClaimAsync("clients");
        await BeginEntityAsync(runId, claimId, "clients");

        // Stray 'extracting' row whose stored batch counter MATCHES the declared
        // expected count — without the touched-entity ownership gate this row would
        // complete 'done' under the live claim.
        await ExecAsync("""
            INSERT INTO etl_run_entities(run_id,entity_name,entity_definition_json,domain_fingerprint,status,
                base_row_present,domain_status,rows_read,batches_created,created_at_utc,updated_at_utc,row_version)
            VALUES($run,'orders',$def,'injected','extracting',0,'absent',7,2,$now,$now,1);
            """,
            ("$run", runId.ToString("D")), ("$def", DefinitionJson("orders")), ("$now", Now()));
        var strayBefore = await StringAsync(
            "SELECT status||'|'||batches_created||'|'||COALESCE(final_watermark_json,'-')||'|'||row_version FROM etl_run_entities WHERE run_id=$run AND entity_name='orders';",
            ("$run", runId.ToString("D")));

        var outcome = await Store.CompleteEtlEntityExtractionAsync(runId, claimId, "orders", FinalJson(), 2, CancellationToken.None);
        output.WriteLine($"outcome={outcome}; strayBefore={strayBefore}; {await RunStateAsync(runId)}");

        var rejected = Assert.IsType<EtlEntityCompletionOutcome.Rejected>(outcome);
        Assert.Equal(EtlEntityCompletionRejection.EntityNotExtracting, rejected.Reason);
        // Full zero-write snapshot: stray row untouched, no run mutation, claim intact.
        Assert.Equal(strayBefore, await StringAsync(
            "SELECT status||'|'||batches_created||'|'||COALESCE(final_watermark_json,'-')||'|'||row_version FROM etl_run_entities WHERE run_id=$run AND entity_name='orders';",
            ("$run", runId.ToString("D"))));
        Assert.Equal("running", await StringAsync("SELECT status FROM etl_runs WHERE run_id=$run;", ("$run", runId.ToString("D"))));
        Assert.Equal(claimId.ToString("D"), await StringAsync("SELECT extraction_claim_id FROM etl_runs WHERE run_id=$run;", ("$run", runId.ToString("D"))));
        Assert.Equal(0, await LongAsync("SELECT COUNT(*) FROM etl_batches WHERE run_id=$run AND entity_name='orders';", ("$run", runId.ToString("D"))));
    }
}
