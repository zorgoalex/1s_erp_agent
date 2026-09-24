using ErpOnecAgent.Application.Abstractions;
using Xunit;
using Xunit.Abstractions;

namespace ReviewProbes;

/// <summary>
/// Probe A — ordering inside TryClaimRunCompletionAsync. VerifyClaimReadinessAsync
/// evaluates the transient non-ACK check BEFORE the exact three-way ownership set gate
/// (SqliteAgentStore.EtlFinalize.cs). Normative expectation (design §6.2 + root
/// disposition): a run whose manifest no longer equals bindings/active ownership is
/// corrupt durable evidence — it must durably block OWNERSHIP_SET_MISMATCH in the claim
/// transaction and CommitBlockedRunAsync must fence the still-'ready' batch to
/// dead_letter. Suspect behavior under review: the corrupted run instead returns a
/// transient NotClaimed, stays 'uploading', and leaves the 'ready' batch dispatchable.
/// </summary>
public sealed class ProbeACompletionClaimOrderingTests(ITestOutputHelper output) : ProbeFixture
{
    private async Task<(Guid RunId, Guid BatchId)> SealedRunWithUnackedBatchAsync()
    {
        var (_, runId, claimId) = await AcceptAndClaimAsync("clients");
        await BeginEntityAsync(runId, claimId, "clients");
        var batchId = await RegisterBatchAsync(runId, claimId, "clients", 5);
        await CompleteEntityAsync(runId, claimId, "clients", 1);
        await SealAsync(runId, claimId);
        return (runId, batchId);
    }

    [Fact]
    public async Task A_BindingDeleted_UnackedBatch_CompletionClaim_MustBlockOwnershipMismatch()
    {
        var (runId, _) = await SealedRunWithUnackedBatchAsync();
        // Corruption: the immutable binding row is gone; the ownership row still exists.
        await ExecAsync("DELETE FROM etl_run_ownership_bindings WHERE run_id=$run;", ("$run", runId.ToString("D")));

        var outcome = await Store.TryClaimRunCompletionAsync(runId, "completion-owner",
            DateTimeOffset.UtcNow, 8, CancellationToken.None);
        var state = await RunStateAsync(runId);
        output.WriteLine($"outcome={outcome}; {state}");

        var blocked = Assert.IsType<EtlRunClaimOutcome.Blocked>(outcome);
        Assert.Equal("OWNERSHIP_SET_MISMATCH", blocked.Code);
        Assert.Equal("blocked", await StringAsync("SELECT status FROM etl_runs WHERE run_id=$run;", ("$run", runId.ToString("D"))));
        Assert.Equal("dead_letter", await StringAsync("SELECT status FROM etl_batches WHERE run_id=$run;", ("$run", runId.ToString("D"))));
    }

    [Fact]
    public async Task A_OwnershipRowDeleted_UnackedBatch_CompletionClaim_MustBlockOwnershipMismatch()
    {
        var (runId, _) = await SealedRunWithUnackedBatchAsync();
        // Corruption: the active ownership row is gone; the binding is left dangling.
        await ExecAsync("DELETE FROM etl_entity_ownership WHERE owner_run_id=$run;", ("$run", runId.ToString("D")));

        var outcome = await Store.TryClaimRunCompletionAsync(runId, "completion-owner",
            DateTimeOffset.UtcNow, 8, CancellationToken.None);
        var state = await RunStateAsync(runId);
        output.WriteLine($"outcome={outcome}; {state}");

        var blocked = Assert.IsType<EtlRunClaimOutcome.Blocked>(outcome);
        Assert.Equal("OWNERSHIP_SET_MISMATCH", blocked.Code);
        Assert.Equal("blocked", await StringAsync("SELECT status FROM etl_runs WHERE run_id=$run;", ("$run", runId.ToString("D"))));
        Assert.Equal("dead_letter", await StringAsync("SELECT status FROM etl_batches WHERE run_id=$run;", ("$run", runId.ToString("D"))));
    }

    [Fact]
    public async Task A_BindingDeleted_AllBatchesAcked_CompletionClaim_BlocksOwnershipMismatch()
    {
        // Contrast: with every batch acknowledged the readiness check reaches the
        // ownership gate — the corrupted set must block OWNERSHIP_SET_MISMATCH here.
        var (runId, batchId) = await SealedRunWithUnackedBatchAsync();
        await AcknowledgeBatchAsync(batchId);
        await ExecAsync("DELETE FROM etl_run_ownership_bindings WHERE run_id=$run;", ("$run", runId.ToString("D")));

        var outcome = await Store.TryClaimRunCompletionAsync(runId, "completion-owner",
            DateTimeOffset.UtcNow, 8, CancellationToken.None);
        var state = await RunStateAsync(runId);
        output.WriteLine($"outcome={outcome}; {state}");

        var blocked = Assert.IsType<EtlRunClaimOutcome.Blocked>(outcome);
        Assert.Equal("OWNERSHIP_SET_MISMATCH", blocked.Code);
        Assert.Equal("blocked", await StringAsync("SELECT status FROM etl_runs WHERE run_id=$run;", ("$run", runId.ToString("D"))));
        // The acknowledged batch is preserved evidence — never re-fenced.
        Assert.Equal("acknowledged", await StringAsync("SELECT status FROM etl_batches WHERE run_id=$run;", ("$run", runId.ToString("D"))));
    }

    [Fact]
    public async Task A_OwnedUnacked_CompletionClaim_RemainsTransientNotClaimed()
    {
        // Positive control: intact ownership + unacknowledged batch = transient wait,
        // zero durable writes (claim write rolls back, run stays 'uploading').
        var (runId, _) = await SealedRunWithUnackedBatchAsync();

        var outcome = await Store.TryClaimRunCompletionAsync(runId, "completion-owner",
            DateTimeOffset.UtcNow, 8, CancellationToken.None);
        var state = await RunStateAsync(runId);
        output.WriteLine($"outcome={outcome}; {state}");

        Assert.IsType<EtlRunClaimOutcome.NotClaimed>(outcome);
        Assert.Equal("uploading", await StringAsync("SELECT status FROM etl_runs WHERE run_id=$run;", ("$run", runId.ToString("D"))));
        Assert.Equal("ready", await StringAsync("SELECT status FROM etl_batches WHERE run_id=$run;", ("$run", runId.ToString("D"))));
        Assert.Equal(0, await LongAsync("SELECT completion_attempt_count FROM etl_runs WHERE run_id=$run;", ("$run", runId.ToString("D"))));
    }
}
