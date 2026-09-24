using System.Globalization;
using System.Text.Json;
using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Domain.Commands;
using ErpOnecAgent.Domain.Common;
using ErpOnecAgent.Domain.Etl;
using ErpOnecAgent.Infrastructure.Persistence.Sqlite;
using Microsoft.Data.Sqlite;
using Xunit;

namespace ReviewProbes;

/// <summary>
/// Shared real-SQLite fixture for the O1 review probes. Each test class gets a migrated
/// store over a fresh temp database (same pattern as SqliteTestDatabase). Helpers drive
/// only the legitimate path: a durable bootstrap_full manual job accepted under the
/// command execution claim, then claimed via TryClaimEtlJobAsync (which mints the
/// extraction claim fence and acquires ownership + bindings), then the fenced F1
/// capture/register/complete/seal calls. Corruption for each probe is injected directly
/// via SQL afterwards; no fake store APIs are used.
/// </summary>
public abstract class ProbeFixture : IAsyncLifetime
{
    protected const string SourceNamespace = "onec-infobase-a";
    protected const string JobMode = "bootstrap_full";
    protected static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);
    protected static readonly EtlCursor FinalCursor =
        new(DateTimeOffset.Parse("2026-09-19T09:59:00.0000000+00:00", CultureInfo.InvariantCulture), "A10");

    private readonly List<SqliteConnectionFactory> _factories = [];
    private string _root = null!;
    protected SqliteConnectionFactory Factory { get; private set; } = null!;
    protected SqliteAgentStore Store { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        _root = Path.Combine(Path.GetTempPath(), "EtlO1ReviewProbes", Guid.NewGuid().ToString("N"));
        Factory = new SqliteConnectionFactory(Path.Combine(_root, "agent.db"));
        _factories.Add(Factory);
        Store = new SqliteAgentStore(Factory, new SqliteMigrator(Factory));
        await Store.InitializeAsync(CancellationToken.None);
    }

    public async Task DisposeAsync()
    {
        foreach (var factory in _factories)
        {
            await using var connection = await factory.OpenAsync(CancellationToken.None);
            SqliteConnection.ClearPool(connection);
        }

        if (Directory.Exists(_root)) Directory.Delete(_root, recursive: true);
    }

    // ---------- legitimate path helpers ----------

    protected static EtlEntityDefinition Definition(string code) =>
        new(code, "Catalog_" + code, "Ref_Key", "UpdatedAt", "DeletionMark",
            ["Ref_Key", "UpdatedAt", "DeletionMark"], "incremental", 500, 10);

    protected static string DefinitionJson(string code) => JsonSerializer.Serialize(Definition(code), JsonOptions);
    protected static string CursorJson(EtlCursor cursor) => JsonSerializer.Serialize(cursor, JsonOptions);
    protected static string SnapshotJson() => CursorJson(new EtlCursor(DateTimeOffset.UtcNow, null));
    protected static string FinalJson() => CursorJson(FinalCursor);
    protected static string Now() => DateTimeOffset.UtcNow.ToString("O");

    /// <summary>
    /// Accepts one durable manual bootstrap_full job: command stored + execution-claimed,
    /// then accepted under that exact claim owner. Returns (jobId, runId) of the pending pair.
    /// </summary>
    protected async Task<(Guid JobId, Guid RunId)> AcceptJobAsync(params string[] entityCodes)
    {
        var entities = entityCodes.Select(Definition).ToArray();
        using var document = JsonDocument.Parse("{}");
        var payload = document.RootElement.Clone();
        var command = new CommandEnvelope(Guid.NewGuid(), "start_full_sync", 1, 100, null, null,
            DateTimeOffset.UtcNow, null, null, null, PayloadHasher.Compute(payload), payload);
        Assert.Equal(StoreCommandOutcome.Stored,
            await Store.StoreCommandAsync(command, DateTimeOffset.UtcNow, CancellationToken.None));
        var owner = "exec-" + Guid.NewGuid().ToString("N");
        var claim = await Store.TryAcquireCommandExecutionClaimAsync(command.CommandId, owner,
            DateTimeOffset.UtcNow, DateTimeOffset.MinValue, CancellationToken.None);
        Assert.NotNull(claim);
        var outcome = await Store.AcceptEtlJobAndCompleteCommandAsync(command.CommandId, owner,
            new EtlJobAcceptanceRequest(JobMode, entities, 7), CancellationToken.None);
        var applied = Assert.IsType<EtlJobAcceptanceOutcome.Applied>(outcome);
        return (applied.Job.JobId, applied.Job.RunId);
    }

    /// <summary>Claims a pending job through the real claim transaction; returns the minted claim.</summary>
    protected async Task<EtlJobClaim> ClaimJobAsync(Guid jobId)
    {
        var outcome = await Store.TryClaimEtlJobAsync(jobId, "dispatch-" + Guid.NewGuid().ToString("N"),
            DateTimeOffset.UtcNow, DateTimeOffset.UtcNow, CancellationToken.None);
        return Assert.IsType<EtlJobClaimOutcome.Claimed>(outcome).Claim;
    }

    /// <summary>Accept + claim in one step: pending run becomes 'running' with ownership, bindings and a fresh extraction claim.</summary>
    protected async Task<(Guid JobId, Guid RunId, Guid ExtractionClaimId)> AcceptAndClaimAsync(params string[] entityCodes)
    {
        var (jobId, runId) = await AcceptJobAsync(entityCodes);
        var claim = await ClaimJobAsync(jobId);
        return (jobId, runId, claim.ExtractionClaimId);
    }

    protected async Task BeginEntityAsync(Guid runId, Guid claimId, string entity)
    {
        var outcome = await Store.BeginEtlEntityExtractionAsync(runId, claimId,
            new EtlEntityExtractionRequest(entity, DefinitionJson(entity), SourceNamespace, JobMode, SnapshotJson()),
            CancellationToken.None);
        Assert.IsType<EtlEntityBeginOutcome.Begun>(outcome);
    }

    protected static EtlBatch MakeBatch(Guid runId, string entity, int rows) =>
        new(Guid.NewGuid(), runId, entity, 1, $"spool/{runId:N}-{entity}-{Guid.NewGuid():N}.gz",
            EtlBatchStatus.Ready, rows, null, FinalCursor, "sha256-" + Guid.NewGuid().ToString("N"),
            1000, 4000, 0, DateTimeOffset.UtcNow);

    protected async Task<Guid> RegisterBatchAsync(Guid runId, Guid claimId, string entity, int rows)
    {
        var batch = MakeBatch(runId, entity, rows);
        var outcome = await Store.RegisterGuardedEtlBatchAsync(batch, claimId, CancellationToken.None);
        Assert.IsType<EtlBatchRegistrationOutcome.Registered>(outcome);
        return batch.BatchId;
    }

    protected async Task CompleteEntityAsync(Guid runId, Guid claimId, string entity, int expectedBatches)
    {
        var outcome = await Store.CompleteEtlEntityExtractionAsync(runId, claimId, entity,
            FinalJson(), expectedBatches, CancellationToken.None);
        Assert.IsType<EtlEntityCompletionOutcome.Completed>(outcome);
    }

    protected async Task SealAsync(Guid runId, Guid claimId)
    {
        var outcome = await Store.SealEtlRunExtractionAsync(runId, claimId, CancellationToken.None);
        Assert.IsType<EtlRunSealOutcome.Sealed>(outcome);
    }

    /// <summary>Acknowledges one 'ready' batch via the real (legacy) ACK API after flipping it to 'uploading'.</summary>
    protected async Task AcknowledgeBatchAsync(Guid batchId)
    {
        await ExecAsync("UPDATE etl_batches SET status='uploading' WHERE batch_id=$id;", ("$id", batchId.ToString("D")));
        await Store.AcknowledgeBatchAsync(batchId, DateTimeOffset.UtcNow, CancellationToken.None);
    }

    // ---------- raw SQL helpers (corruption injection + verification) ----------

    protected async Task ExecAsync(string sql, params (string Name, object? Value)[] parameters)
    {
        await using var connection = await Factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        foreach (var (name, value) in parameters) command.Parameters.AddWithValue(name, value ?? DBNull.Value);
        await command.ExecuteNonQueryAsync(CancellationToken.None);
    }

    protected async Task<long> LongAsync(string sql, params (string Name, object? Value)[] parameters)
    {
        await using var connection = await Factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        foreach (var (name, value) in parameters) command.Parameters.AddWithValue(name, value ?? DBNull.Value);
        return Convert.ToInt64(await command.ExecuteScalarAsync(CancellationToken.None), CultureInfo.InvariantCulture);
    }

    protected async Task<string?> StringAsync(string sql, params (string Name, object? Value)[] parameters)
    {
        await using var connection = await Factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        foreach (var (name, value) in parameters) command.Parameters.AddWithValue(name, value ?? DBNull.Value);
        return await command.ExecuteScalarAsync(CancellationToken.None) as string;
    }

    /// <summary>One-line durable state dump for failure evidence (run, batches, ownership).</summary>
    protected async Task<string> RunStateAsync(Guid runId)
    {
        var run = await StringAsync(
            "SELECT status||'|claim='||COALESCE(completion_claim_id,'-')||'|attempts='||completion_attempt_count||'|conflict='||COALESCE(finalize_conflict_code,'-')||'|rv='||row_version FROM etl_runs WHERE run_id=$run;",
            ("$run", runId.ToString("D")));
        var batches = await StringAsync(
            "SELECT COALESCE(GROUP_CONCAT(entity_name||':'||status),'-') FROM etl_batches WHERE run_id=$run;",
            ("$run", runId.ToString("D")));
        var ownership = await LongAsync(
            "SELECT COUNT(*) FROM etl_entity_ownership o JOIN etl_runs r ON r.run_id=$run WHERE o.owner_run_id=$run AND o.released_at_utc IS NULL;",
            ("$run", runId.ToString("D")));
        var bindings = await LongAsync(
            "SELECT COUNT(*) FROM etl_run_ownership_bindings WHERE run_id=$run;",
            ("$run", runId.ToString("D")));
        return $"run[{run}] batches[{batches}] activeOwnership={ownership} bindings={bindings}";
    }
}
