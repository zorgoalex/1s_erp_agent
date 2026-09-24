using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.Json;
using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Application.Configuration;
using ErpOnecAgent.Contracts.Erp;
using ErpOnecAgent.Domain.Etl;
using ErpOnecAgent.Infrastructure.Persistence.Sqlite;
using ErpOnecAgent.Infrastructure.Spool;
using ErpOnecAgent.Service.Runtime;
using ErpOnecAgent.Service.Workers;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace ErpOnecAgent.IntegrationTests;

/// <summary>
/// Runtime reproduction of the missing batch-acknowledgement status validation in
/// <see cref="EtlBatchUploadWorker"/> (baseline RED): the ERP batch acknowledgement contract
/// (<c>contracts/erp-agent-api.openapi.yaml</c>, schema <c>BatchAck</c>) requires
/// <c>status: acknowledged</c> (const), but the worker only compared batch id, checksum flag and
/// row count — any other <see cref="BatchAcknowledgement.Status"/> the DTO can carry
/// ("failed"/"pending"/empty/null) was treated as a positive acknowledgement, bumping the
/// durable ack counter, releasing run completion and committing the watermark.
///
/// Fixture: real worker + real migrated temporary SQLite + real <see cref="FileSpoolStore"/>
/// (genuine gzip batch file and SHA-256) + fake <see cref="IErpClient"/> returning a
/// matching-id/checksum/rows acknowledgement with a scripted status. Determinism: a
/// <see cref="DispatchProxy"/> store wrapper raises <see cref="SweepGate.BatchSettled"/> when the
/// worker's terminal batch write (<c>AcknowledgeBatchAsync</c> or <c>MarkBatchRetryAsync</c>)
/// completes, and <see cref="SweepGate.SecondSweep"/> when the NEXT <c>GetPendingBatchesAsync</c>
/// call begins — by construction the whole first loop iteration (including any run-completion
/// delivery and watermark commit) has finished. The second sweep is then PARKED on the worker's
/// stopping token without touching the store, so a delayed test continuation can never let a
/// due retry be re-uploaded while assertions run; <c>StopAsync</c> unwinds the park. No sleeps
/// are used to mask races; all waits are bounded.
/// </summary>
public sealed class EtlBatchAckStatusTests : IAsyncLifetime
{
    private static readonly TimeSpan BoundedWait = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan StopTimeout = TimeSpan.FromSeconds(5);
    private static readonly EtlCursor Watermark = new(new DateTimeOffset(2026, 9, 1, 0, 0, 0, TimeSpan.Zero), "K1");

    private readonly SqliteTestDatabase _database = new();
    private SqliteConnectionFactory _factory = null!;
    private SqliteAgentStore _store = null!;

    public async Task InitializeAsync()
    {
        _factory = _database.CreateFactory(Path.Combine("data", "etl-ack-status.db"));
        _store = new(_factory, new SqliteMigrator(_factory));
        await _store.InitializeAsync(CancellationToken.None);
    }

    public async Task DisposeAsync() => await _database.DisposeAsync();

    [Theory]
    [InlineData("failed")]
    [InlineData("pending")]
    [InlineData("")]
    [InlineData(null)]
    public async Task Non_acknowledged_status_is_not_accepted_batch_stays_retryable(string? status)
    {
        var spool = new FileSpoolStore(Path.Combine(_database.Root, "spool"));
        var runId = await NewRunAsync();
        var batch = await WriteAndRegisterAsync(spool, runId);
        await _store.MarkEtlRunExtractedAsync(runId, CancellationToken.None);

        var state = ReadyState();
        var erp = new ScriptedErpClient { Status = status };
        var gate = new SweepGate();
        using var worker = CreateWorker(SettleTrackingStoreProxy.Create(_store, gate), spool, erp, state);
        try
        {
            await worker.StartAsync(CancellationToken.None);
            var settled = await Task.WhenAny(gate.BatchSettled.Task, Task.Delay(BoundedWait)) == gate.BatchSettled.Task;
            Assert.True(settled, "Worker did not settle the uploaded batch within the bounded window; fixture broken.");
            var swept = await Task.WhenAny(gate.SecondSweep.Task, Task.Delay(BoundedWait)) == gate.SecondSweep.Task;
            Assert.True(swept, "Worker did not finish the processing sweep within the bounded window; fixture broken.");
        }
        finally
        {
            using var stop = new CancellationTokenSource(StopTimeout);
            await worker.StopAsync(stop.Token);
        }

        // The acknowledgement must be rejected: no ack counter bump, no run completion, no
        // watermark commit; the batch returns to the existing retry policy with its spool
        // evidence preserved.
        Assert.Equal("retry_waiting", await BatchStatusAsync(batch.BatchId));
        Assert.Equal(1L, await AttemptCountAsync(batch.BatchId));
        Assert.False(string.IsNullOrEmpty(await LastErrorAsync(batch.BatchId)));
        Assert.Equal(0L, await BatchesAcknowledgedAsync(runId));
        Assert.Equal("uploading", await RunStatusAsync(runId));
        Assert.Equal(1, erp.UploadCalls);
        Assert.Equal(0, erp.CompleteCalls);
        Assert.Null(await _store.GetCommittedWatermarkAsync("clients", CancellationToken.None));
        Assert.True(File.Exists(batch.FilePath), "Spool evidence must be preserved for the retryable batch.");

        // Existing retry policy is preserved: once the persisted backoff elapses the batch is
        // offered for upload again.
        var reoffered = await _store.GetPendingBatchesAsync(10, DateTimeOffset.UtcNow.AddHours(1), CancellationToken.None);
        var again = Assert.Single(reoffered);
        Assert.Equal(batch.BatchId, again.BatchId);
    }

    [Fact]
    public async Task Acknowledged_status_completes_run_and_commits_watermark()
    {
        var spool = new FileSpoolStore(Path.Combine(_database.Root, "spool"));
        var runId = await NewRunAsync();
        var batch = await WriteAndRegisterAsync(spool, runId);
        await _store.MarkEtlRunExtractedAsync(runId, CancellationToken.None);

        var state = ReadyState();
        var erp = new ScriptedErpClient { Status = "acknowledged" };
        var gate = new SweepGate();
        using var worker = CreateWorker(SettleTrackingStoreProxy.Create(_store, gate), spool, erp, state);
        try
        {
            await worker.StartAsync(CancellationToken.None);
            var settled = await Task.WhenAny(gate.BatchSettled.Task, Task.Delay(BoundedWait)) == gate.BatchSettled.Task;
            Assert.True(settled, "Worker did not settle the uploaded batch within the bounded window; fixture broken.");
            var swept = await Task.WhenAny(gate.SecondSweep.Task, Task.Delay(BoundedWait)) == gate.SecondSweep.Task;
            Assert.True(swept, "Worker did not finish the processing sweep within the bounded window; fixture broken.");
        }
        finally
        {
            using var stop = new CancellationTokenSource(StopTimeout);
            await worker.StopAsync(stop.Token);
        }

        // The contract-positive acknowledgement still drives the full success path: durable ack,
        // ERP run-completion delivery, watermark commit and run success.
        Assert.Equal("acknowledged", await BatchStatusAsync(batch.BatchId));
        Assert.Equal(1L, await BatchesAcknowledgedAsync(runId));
        Assert.Equal(1, erp.UploadCalls);
        Assert.Equal(1, erp.CompleteCalls);
        Assert.Equal("succeeded", await RunStatusAsync(runId));
        Assert.Equal(Watermark, await _store.GetCommittedWatermarkAsync("clients", CancellationToken.None));
        Assert.NotNull(state.LastEtlSuccessAtUtc);
    }

    private async Task<Guid> NewRunAsync()
    {
        var runId = Guid.NewGuid();
        await _store.CreateEtlRunAsync(new(runId, "incremental", ["clients"], EtlRunStatus.Running), CancellationToken.None);
        return runId;
    }

    private async Task<EtlBatch> WriteAndRegisterAsync(FileSpoolStore spool, Guid runId)
    {
        var entity = new EtlEntityDefinition("clients", "Catalog_Clients", "Ref_Key", "UpdatedAt", null, ["Ref_Key", "UpdatedAt"], "incremental", 500, 0);
        using var document = JsonDocument.Parse("{\"Ref_Key\":\"K1\",\"UpdatedAt\":\"2026-09-01T00:00:00Z\"}");
        var batch = await spool.WriteBatchAsync(runId, entity, [document.RootElement.Clone()], null, Watermark, CancellationToken.None);
        await _store.RegisterBatchAsync(batch, CancellationToken.None);
        return batch;
    }

    private static AgentRuntimeState ReadyState()
    {
        var state = new AgentRuntimeState();
        state.CompleteBootstrap();
        return state;
    }

    private static EtlBatchUploadWorker CreateWorker(IAgentStore store, ISpoolStore spool, ScriptedErpClient erp, AgentRuntimeState state) =>
        new(store, spool, erp, state, Options.Create(new EtlOptions { MaxConcurrentBatchUploads = 2 }), NullLogger<EtlBatchUploadWorker>.Instance);

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
    private async Task<long> AttemptCountAsync(Guid batchId) => Convert.ToInt64(await ScalarAsync("SELECT attempt_count FROM etl_batches WHERE batch_id=$id;", ("$id", batchId.ToString("D"))), System.Globalization.CultureInfo.InvariantCulture);
    private async Task<string?> LastErrorAsync(Guid batchId) => Convert.ToString(await ScalarAsync("SELECT last_error FROM etl_batches WHERE batch_id=$id;", ("$id", batchId.ToString("D"))), System.Globalization.CultureInfo.InvariantCulture);
    private async Task<long> BatchesAcknowledgedAsync(Guid runId) => Convert.ToInt64(await ScalarAsync("SELECT batches_acknowledged FROM etl_runs WHERE run_id=$id;", ("$id", runId.ToString("D"))), System.Globalization.CultureInfo.InvariantCulture);

    private sealed class ScriptedErpClient : IErpClient
    {
        private int _uploadCalls;
        private int _completeCalls;

        public string? Status { get; init; }
        public int UploadCalls => Volatile.Read(ref _uploadCalls);
        public int CompleteCalls => Volatile.Read(ref _completeCalls);

        public Task<BatchAcknowledgement> UploadBatchAsync(EtlBatch batch, Stream content, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _uploadCalls);
            return Task.FromResult(new BatchAcknowledgement(batch.BatchId, Status!, batch.RowCount, true, DateTimeOffset.UtcNow));
        }

        public Task CompleteEtlRunAsync(Guid runId, object summary, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _completeCalls);
            return Task.CompletedTask;
        }

        public Task<SessionStartResponse> StartSessionAsync(SessionStartRequest request, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task<LeaseResponse> LeaseCommandAsync(LeaseRequest request, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task AcknowledgeReceivedAsync(Guid commandId, CommandReceivedRequest request, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task AcknowledgeResultAsync(Guid commandId, string resultJson, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task SendHeartbeatAsync(HeartbeatRequest request, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task<RemoteConfigurationResponse?> GetConfigurationAsync(long currentVersion, CancellationToken cancellationToken) => throw new NotSupportedException();
    }

    public sealed class SweepGate
    {
        public TaskCompletionSource<bool> BatchSettled { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource<bool> SecondSweep { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    /// <summary>
    /// IAgentStore pass-through that exposes two deterministic barriers: <c>BatchSettled</c>
    /// completes after the worker's terminal batch write returns (whichever of
    /// <c>AcknowledgeBatchAsync</c>/<c>MarkBatchRetryAsync</c> the evaluation took), and
    /// <c>SecondSweep</c> completes when the second <c>GetPendingBatchesAsync</c> call begins —
    /// proving the entire previous loop iteration, including any run-completion delivery and
    /// watermark commit, is finished. The second call does NOT reach the store: it is parked on
    /// the worker's stopping token so the loop cannot re-fetch or re-upload the batch while the
    /// test asserts, and is released only by <c>StopAsync</c>. All other members delegate
    /// unchanged.
    /// </summary>
    public class SettleTrackingStoreProxy : DispatchProxy
    {
        private sealed record ProxyTarget(IAgentStore Inner, SweepGate Gate);

        private static readonly ConditionalWeakTable<DispatchProxy, ProxyTarget> Targets = new();
        private int _pendingCalls;

        public static IAgentStore Create(IAgentStore inner, SweepGate gate)
        {
            var proxy = DispatchProxy.Create<IAgentStore, SettleTrackingStoreProxy>();
            Targets.Add((DispatchProxy)(object)proxy, new(inner, gate));
            return proxy;
        }

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            if (!Targets.TryGetValue(this, out var target)) throw new InvalidOperationException("Proxy state is missing.");
            if (targetMethod?.Name == nameof(IAgentStore.GetPendingBatchesAsync)
                && Interlocked.Increment(ref _pendingCalls) == 2)
            {
                target.Gate.SecondSweep.TrySetResult(true);
                return ParkedSweepAsync(args);
            }
            if (targetMethod?.Name is nameof(IAgentStore.AcknowledgeBatchAsync) or nameof(IAgentStore.MarkBatchRetryAsync))
            {
                return SettleAsync(target, targetMethod, args);
            }
            return targetMethod!.Invoke(target.Inner, args);
        }

        private static async Task<IReadOnlyList<EtlBatch>> ParkedSweepAsync(object?[]? args)
        {
            var cancellationToken = (CancellationToken)args![2]!;
            try { await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken).ConfigureAwait(false); }
            catch (OperationCanceledException) { }
            return [];
        }

        private static async Task SettleAsync(ProxyTarget target, MethodInfo method, object?[]? args)
        {
            await ((Task)method.Invoke(target.Inner, args)!).ConfigureAwait(false);
            target.Gate.BatchSettled.TrySetResult(true);
        }
    }
}
