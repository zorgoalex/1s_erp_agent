using System.Text.Json;
using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Contracts.Erp;
using ErpOnecAgent.Domain.Etl;
using ErpOnecAgent.Infrastructure.Persistence.Sqlite;
using ErpOnecAgent.Infrastructure.Spool;
using Microsoft.Data.Sqlite;
using Xunit;

namespace ErpOnecAgent.IntegrationTests;

public sealed class RetentionReadinessTests : IAsyncLifetime
{
    private readonly SqliteTestDatabase _database = new();
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

    [Fact]
    public async Task Aged_acknowledged_batches_of_unfinished_or_failed_runs_are_not_offered()
    {
        var acknowledged = DateTimeOffset.UtcNow.AddDays(-40);
        var cutoff = DateTimeOffset.UtcNow.AddDays(-30);

        // Parent still extracting (running): batches can already be acknowledged by ERP.
        var running = await NewRunAsync(["clients"]);
        await RegisterAndAcknowledgeAsync(running, "clients", 5, Cursor("r1"), acknowledged);

        // Parent extracted, still delivering the run-completion to ERP (uploading).
        var uploading = await NewRunAsync(["clients"]);
        await RegisterAndAcknowledgeAsync(uploading, "clients", 5, Cursor("u1"), acknowledged);
        await _store.MarkEtlRunExtractedAsync(uploading, CancellationToken.None);

        // Parent failed: acknowledged batches are evidence for a later manual policy.
        var failed = await NewRunAsync(["clients"]);
        await RegisterAndAcknowledgeAsync(failed, "clients", 5, Cursor("f1"), acknowledged);
        await _store.CompleteEtlRunAsync(failed, EtlRunStatus.Failed, "extraction failed", CancellationToken.None);

        Assert.Empty(await _store.GetAcknowledgedBatchesAsync(cutoff, CancellationToken.None));
    }

    [Fact]
    public async Task Acknowledged_batches_of_succeeded_run_are_deleted_and_purged()
    {
        var acknowledged = DateTimeOffset.UtcNow.AddDays(-40);
        var cutoff = DateTimeOffset.UtcNow.AddDays(-30);
        var runId = await NewRunAsync(["clients"]);
        var batch = await RegisterAndAcknowledgeAsync(runId, "clients", 5, Cursor("s1"), acknowledged);
        await _store.MarkEtlRunExtractedAsync(runId, CancellationToken.None);
        await _store.CompleteEtlRunAsync(runId, EtlRunStatus.Succeeded, null, CancellationToken.None);

        var offered = Assert.Single(await _store.GetAcknowledgedBatchesAsync(cutoff, CancellationToken.None));
        Assert.Equal(batch.BatchId, offered.BatchId);
        await _store.MarkBatchDeletedAsync(batch.BatchId, CancellationToken.None);
        Assert.Equal("deleted", await BatchStatusAsync(batch.BatchId));
        await _store.CleanupAsync(DateTimeOffset.UtcNow, cutoff, CancellationToken.None);
        Assert.Equal(0L, await BatchRowCountAsync(runId));
    }

    [Fact]
    public async Task Mark_batch_deleted_refuses_batch_of_unresolved_run()
    {
        var acknowledged = DateTimeOffset.UtcNow.AddDays(-40);
        var runId = await NewRunAsync(["clients"]);
        var batch = await RegisterAndAcknowledgeAsync(runId, "clients", 5, Cursor("m1"), acknowledged);
        await _store.MarkEtlRunExtractedAsync(runId, CancellationToken.None);

        await _store.MarkBatchDeletedAsync(batch.BatchId, CancellationToken.None);

        Assert.Equal("acknowledged", await BatchStatusAsync(batch.BatchId));
    }

    [Fact]
    public async Task Cleanup_preserves_legacy_deleted_rows_of_unresolved_runs()
    {
        var acknowledged = DateTimeOffset.UtcNow.AddDays(-40);
        var cutoff = DateTimeOffset.UtcNow.AddDays(-30);
        var uploading = await NewRunAsync(["clients"]);
        var uploadingBatch = await RegisterAndAcknowledgeAsync(uploading, "clients", 5, Cursor("u1"), acknowledged);
        await _store.MarkEtlRunExtractedAsync(uploading, CancellationToken.None);
        var failed = await NewRunAsync(["clients"]);
        var failedBatch = await RegisterAndAcknowledgeAsync(failed, "clients", 5, Cursor("f1"), acknowledged);
        await _store.CompleteEtlRunAsync(failed, EtlRunStatus.Failed, "extraction failed", CancellationToken.None);

        // Legacy/manual deletion marks on unresolved runs are evidence, not garbage.
        await ExecAsync("UPDATE etl_batches SET status='deleted' WHERE batch_id IN ($u,$f);",
            ("$u", uploadingBatch.BatchId.ToString("D")), ("$f", failedBatch.BatchId.ToString("D")));
        await _store.CleanupAsync(DateTimeOffset.UtcNow, cutoff, CancellationToken.None);

        Assert.Equal(1L, await BatchRowCountAsync(uploading));
        Assert.Equal(1L, await BatchRowCountAsync(failed));
        Assert.Equal("deleted", await BatchStatusAsync(uploadingBatch.BatchId));
        Assert.Equal("deleted", await BatchStatusAsync(failedBatch.BatchId));
    }

    [Fact]
    public async Task Run_with_missing_batch_rows_is_not_ready()
    {
        var acknowledged = DateTimeOffset.UtcNow.AddDays(-40);
        // One of two batch rows is gone: the manifest count can no longer be proven.
        var partial = await NewRunAsync(["clients", "orders"]);
        await RegisterAndAcknowledgeAsync(partial, "clients", 5, Cursor("c1"), acknowledged);
        var missing = await RegisterAndAcknowledgeAsync(partial, "orders", 5, Cursor("o1"), acknowledged);
        await _store.MarkEtlRunExtractedAsync(partial, CancellationToken.None);
        await ExecAsync("DELETE FROM etl_batches WHERE batch_id=$id;", ("$id", missing.BatchId.ToString("D")));

        // Every batch row is gone while the run counter still claims one.
        var empty = await NewRunAsync(["clients"]);
        await RegisterAndAcknowledgeAsync(empty, "clients", 5, Cursor("e1"), acknowledged);
        await _store.MarkEtlRunExtractedAsync(empty, CancellationToken.None);
        await ExecAsync("DELETE FROM etl_batches WHERE run_id=$id;", ("$id", empty.ToString("D")));

        Assert.Empty(await _store.GetRunsReadyToCompleteAsync(CancellationToken.None));
    }

    [Theory]
    [InlineData("creating")]
    [InlineData("ready")]
    [InlineData("uploading")]
    [InlineData("retry_waiting")]
    [InlineData("dead_letter")]
    [InlineData("deleted")]
    public async Task Run_with_non_acknowledged_batch_status_is_not_ready(string batchStatus)
    {
        var acknowledged = DateTimeOffset.UtcNow.AddDays(-40);
        var runId = await NewRunAsync(["clients", "orders"]);
        await RegisterAndAcknowledgeAsync(runId, "clients", 5, Cursor("c1"), acknowledged);
        var suspect = await RegisterAndAcknowledgeAsync(runId, "orders", 5, Cursor("o1"), acknowledged);
        await _store.MarkEtlRunExtractedAsync(runId, CancellationToken.None);
        await ExecAsync("UPDATE etl_batches SET status=$status WHERE batch_id=$id;",
            ("$status", batchStatus), ("$id", suspect.BatchId.ToString("D")));

        Assert.Empty(await _store.GetRunsReadyToCompleteAsync(CancellationToken.None));
    }

    [Fact]
    public async Task Run_with_inconsistent_counters_is_not_ready()
    {
        var acknowledged = DateTimeOffset.UtcNow.AddDays(-40);
        var runId = await NewRunAsync(["clients"]);
        await RegisterAndAcknowledgeAsync(runId, "clients", 5, Cursor("c1"), acknowledged);
        await _store.MarkEtlRunExtractedAsync(runId, CancellationToken.None);
        await ExecAsync("UPDATE etl_runs SET batches_acknowledged=0 WHERE run_id=$id;", ("$id", runId.ToString("D")));

        var shrunk = await NewRunAsync(["clients"]);
        await RegisterAndAcknowledgeAsync(shrunk, "clients", 5, Cursor("c2"), acknowledged);
        await _store.MarkEtlRunExtractedAsync(shrunk, CancellationToken.None);
        await ExecAsync("UPDATE etl_runs SET batches_created=0,batches_acknowledged=0 WHERE run_id=$id;", ("$id", shrunk.ToString("D")));

        Assert.Empty(await _store.GetRunsReadyToCompleteAsync(CancellationToken.None));
    }

    [Fact]
    public async Task Run_with_entity_coverage_mismatch_is_not_ready()
    {
        var acknowledged = DateTimeOffset.UtcNow.AddDays(-40);
        // Requested entity has no batch at all.
        var missing = await NewRunAsync(["clients", "orders"]);
        await RegisterAndAcknowledgeAsync(missing, "clients", 5, Cursor("c1"), acknowledged);
        await _store.MarkEtlRunExtractedAsync(missing, CancellationToken.None);
        await ExecAsync("UPDATE etl_runs SET batches_created=1,batches_acknowledged=1 WHERE run_id=$id;", ("$id", missing.ToString("D")));

        // Unexpected entity batch the manifest never requested.
        var extra = await NewRunAsync(["clients"]);
        await RegisterAndAcknowledgeAsync(extra, "clients", 5, Cursor("c2"), acknowledged);
        await RegisterAndAcknowledgeAsync(extra, "stowaway", 5, Cursor("s1"), acknowledged);
        await _store.MarkEtlRunExtractedAsync(extra, CancellationToken.None);

        Assert.Empty(await _store.GetRunsReadyToCompleteAsync(CancellationToken.None));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("[]")]
    [InlineData("{}")]
    [InlineData("not-json")]
    [InlineData("[\"\"]")]
    [InlineData("[1]")]
    [InlineData("[null]")]
    [InlineData("[\"clients\",\"clients\"]")]
    public async Task Run_with_missing_or_malformed_manifest_is_not_ready(string? manifest)
    {
        var acknowledged = DateTimeOffset.UtcNow.AddDays(-40);
        var runId = await NewRunAsync(["clients"]);
        await RegisterAndAcknowledgeAsync(runId, "clients", 5, Cursor("c1"), acknowledged);
        await _store.MarkEtlRunExtractedAsync(runId, CancellationToken.None);
        await ExecAsync("UPDATE etl_runs SET requested_entities_json=$manifest WHERE run_id=$id;",
            ("$manifest", manifest), ("$id", runId.ToString("D")));

        Assert.Empty(await _store.GetRunsReadyToCompleteAsync(CancellationToken.None));
    }

    [Fact]
    public async Task Run_with_zero_selected_entities_is_not_ready()
    {
        var runId = await NewRunAsync([]);
        await _store.MarkEtlRunExtractedAsync(runId, CancellationToken.None);

        Assert.Empty(await _store.GetRunsReadyToCompleteAsync(CancellationToken.None));
    }

    [Fact]
    public async Task Multi_entity_run_with_explicit_zero_row_entity_is_ready()
    {
        var acknowledged = DateTimeOffset.UtcNow.AddDays(-40);
        var runId = await NewRunAsync(["clients", "orders"]);
        var created = new DateTimeOffset(2026, 9, 20, 0, 0, 0, TimeSpan.Zero);
        await RegisterAndAcknowledgeAsync(runId, "clients", 5, Cursor("c1"), acknowledged, created);
        var lastClients = Cursor("c2");
        await RegisterAndAcknowledgeAsync(runId, "clients", 5, lastClients, acknowledged, created.AddSeconds(1));
        var emptyUpper = new EtlCursor(new DateTimeOffset(2026, 9, 20, 1, 0, 0, TimeSpan.Zero), null);
        await RegisterAndAcknowledgeAsync(runId, "orders", 0, emptyUpper, acknowledged, created.AddSeconds(2));
        await _store.MarkEtlRunExtractedAsync(runId, CancellationToken.None);

        var completion = Assert.Single(await _store.GetRunsReadyToCompleteAsync(CancellationToken.None));
        Assert.Equal(runId, completion.RunId);
        Assert.Equal(3, completion.BatchCount);
        Assert.Equal(lastClients, completion.Watermarks["clients"]);
        Assert.Equal(emptyUpper, completion.Watermarks["orders"]);
    }

    [Fact]
    public async Task Batches_are_retained_until_erp_run_completion_succeeds()
    {
        var spool = new FileSpoolStore(Path.Combine(_database.Root, "spool"));
        var entity = new EtlEntityDefinition("clients", "Catalog_Clients", "Ref_Key", "UpdatedAt", null, ["Ref_Key", "UpdatedAt"], "incremental", 500, 0);
        var runId = await NewRunAsync(["clients"]);
        using var document = JsonDocument.Parse("{\"Ref_Key\":\"K1\",\"UpdatedAt\":\"2026-09-01T00:00:00Z\"}");
        var cursor = new EtlCursor(new DateTimeOffset(2026, 9, 1, 0, 0, 0, TimeSpan.Zero), "K1");
        var batch = await spool.WriteBatchAsync(runId, entity, [document.RootElement.Clone()], null, cursor, CancellationToken.None);
        await _store.RegisterBatchAsync(batch, CancellationToken.None);
        await _store.MarkEtlRunExtractedAsync(runId, CancellationToken.None);
        var erp = new FakeErpClient { CompleteFails = true };
        var claimed = Assert.Single(await _store.GetPendingBatchesAsync(10, DateTimeOffset.UtcNow, CancellationToken.None));
        await using (var content = await spool.OpenReadAsync(claimed, CancellationToken.None))
        {
            var acknowledgement = await erp.UploadBatchAsync(claimed, content, CancellationToken.None);
            await _store.AcknowledgeBatchAsync(claimed.BatchId, acknowledgement.AcknowledgedAtUtc, CancellationToken.None);
        }

        // ERP run-completion endpoint keeps failing: the run stays 'uploading' and the
        // maintenance path must not offer the acknowledged spool file for deletion.
        var completion = Assert.Single(await _store.GetRunsReadyToCompleteAsync(CancellationToken.None));
        var cutoff = DateTimeOffset.UtcNow.AddDays(1);
        await Assert.ThrowsAsync<InvalidOperationException>(() => erp.CompleteEtlRunAsync(completion.RunId, new { }, CancellationToken.None));
        Assert.Empty(await _store.GetAcknowledgedBatchesAsync(cutoff, CancellationToken.None));
        Assert.True(File.Exists(batch.FilePath));

        // Once ERP accepts the completion, retention can release the batch normally.
        erp.CompleteFails = false;
        await erp.CompleteEtlRunAsync(completion.RunId, new { }, CancellationToken.None);
        foreach (var watermark in completion.Watermarks)
            await _store.CommitWatermarkAsync(watermark.Key, watermark.Value, completion.RunId, CancellationToken.None);
        await _store.CompleteEtlRunAsync(runId, EtlRunStatus.Succeeded, null, CancellationToken.None);
        var offered = Assert.Single(await _store.GetAcknowledgedBatchesAsync(cutoff, CancellationToken.None));
        await spool.DeleteAcknowledgedAsync(offered, CancellationToken.None);
        await _store.MarkBatchDeletedAsync(offered.BatchId, CancellationToken.None);
        Assert.False(File.Exists(batch.FilePath));
        await _store.CleanupAsync(DateTimeOffset.UtcNow, cutoff, CancellationToken.None);
        Assert.Equal(0L, await BatchRowCountAsync(runId));
    }

    [Fact]
    public async Task Succeeded_run_cannot_be_reopened()
    {
        var runId = await NewRunAsync(["clients"]);
        await _store.MarkEtlRunExtractedAsync(runId, CancellationToken.None);
        await _store.CompleteEtlRunAsync(runId, EtlRunStatus.Succeeded, null, CancellationToken.None);
        await _store.CompleteEtlRunAsync(runId, EtlRunStatus.Failed, "late failure", CancellationToken.None);

        Assert.Equal("succeeded", await RunStatusAsync(runId));
    }

    private async Task<Guid> NewRunAsync(IReadOnlyList<string> entities, string mode = "incremental")
    {
        var runId = Guid.NewGuid();
        await _store.CreateEtlRunAsync(new(runId, mode, entities, EtlRunStatus.Running), CancellationToken.None);
        return runId;
    }

    private static EtlCursor Cursor(string sourceId) => new(new DateTimeOffset(2026, 9, 1, 0, 0, 0, TimeSpan.Zero), sourceId);

    private async Task<EtlBatch> RegisterAndAcknowledgeAsync(Guid runId, string entity, int rowCount, EtlCursor? watermarkTo, DateTimeOffset acknowledgedAtUtc, DateTimeOffset? createdAtUtc = null)
    {
        var batch = new EtlBatch(Guid.NewGuid(), runId, entity, 1,
            Path.Combine(_database.Root, "spool", $"{entity}-{Guid.NewGuid():N}.ndjson.gz"),
            EtlBatchStatus.Ready, rowCount, null, watermarkTo, Convert.ToBase64String(new byte[32]), 10, 100, 0, createdAtUtc ?? DateTimeOffset.UtcNow);
        await _store.RegisterBatchAsync(batch, CancellationToken.None);
        await _store.GetPendingBatchesAsync(100, DateTimeOffset.UtcNow.AddMinutes(1), CancellationToken.None);
        await _store.AcknowledgeBatchAsync(batch.BatchId, acknowledgedAtUtc, CancellationToken.None);
        return batch;
    }

    private async Task ExecAsync(string sql, params (string Name, object? Value)[] parameters)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        foreach (var (name, value) in parameters) command.Parameters.AddWithValue(name, value ?? DBNull.Value);
        await command.ExecuteNonQueryAsync(CancellationToken.None);
    }

    private async Task<object?> ScalarAsync(string sql, params (string Name, object? Value)[] parameters)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        foreach (var (name, value) in parameters) command.Parameters.AddWithValue(name, value ?? DBNull.Value);
        return await command.ExecuteScalarAsync(CancellationToken.None);
    }

    private async Task<string?> RunStatusAsync(Guid runId) => Convert.ToString(await ScalarAsync("SELECT status FROM etl_runs WHERE run_id=$id;", ("$id", runId.ToString("D"))), System.Globalization.CultureInfo.InvariantCulture);
    private async Task<string?> BatchStatusAsync(Guid batchId) => Convert.ToString(await ScalarAsync("SELECT status FROM etl_batches WHERE batch_id=$id;", ("$id", batchId.ToString("D"))), System.Globalization.CultureInfo.InvariantCulture);
    private async Task<long> BatchRowCountAsync(Guid runId) => Convert.ToInt64(await ScalarAsync("SELECT COUNT(*) FROM etl_batches WHERE run_id=$id;", ("$id", runId.ToString("D"))), System.Globalization.CultureInfo.InvariantCulture);

    private sealed class FakeErpClient : IErpClient
    {
        public bool CompleteFails { get; set; }

        public Task<BatchAcknowledgement> UploadBatchAsync(EtlBatch batch, Stream content, CancellationToken cancellationToken) =>
            Task.FromResult(new BatchAcknowledgement(batch.BatchId, "acknowledged", batch.RowCount, true, DateTimeOffset.UtcNow));

        public Task CompleteEtlRunAsync(Guid runId, object summary, CancellationToken cancellationToken) =>
            CompleteFails ? Task.FromException(new InvalidOperationException("ERP run completion endpoint unavailable.")) : Task.CompletedTask;

        public Task<SessionStartResponse> StartSessionAsync(SessionStartRequest request, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task<LeaseResponse> LeaseCommandAsync(LeaseRequest request, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task AcknowledgeReceivedAsync(Guid commandId, CommandReceivedRequest request, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task AcknowledgeResultAsync(Guid commandId, string resultJson, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task SendHeartbeatAsync(HeartbeatRequest request, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task<RemoteConfigurationResponse?> GetConfigurationAsync(long currentVersion, CancellationToken cancellationToken) => throw new NotSupportedException();
    }
}
