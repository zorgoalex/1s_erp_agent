using System.Net;
using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Infrastructure.Persistence.Sqlite;
using ErpOnecAgent.Service.Workers.Etl;
using Xunit;

namespace ErpOnecAgent.IntegrationTests;

/// <summary>E2: store transitions for the agreed permanent ERP refusals.</summary>
public sealed class ErpErrorCodeStoreTests : IAsyncLifetime
{
    private readonly SqliteTestDatabase _database = new();
    private SqliteConnectionFactory _factory = null!;
    private SqliteAgentStore _store = null!;

    public async Task InitializeAsync()
    {
        _factory = _database.CreateFactory(Path.Combine("data", "agent.db"));
        _store = new SqliteAgentStore(_factory, new SqliteMigrator(_factory));
        await _store.InitializeAsync(CancellationToken.None);
    }

    public async Task DisposeAsync() => await _database.DisposeAsync();

    [Fact]
    public async Task A_conflicting_result_stops_delivery_and_is_kept_as_evidence()
    {
        var commandId = await InsertPendingResultAsync();

        Assert.True(await _store.MarkResultConflictAsync(commandId, "RESULT_CONFLICT", CancellationToken.None));

        Assert.Equal("dead_letter", await ScalarAsync($"SELECT status FROM results_outbox WHERE command_id='{commandId:D}'"));
        Assert.Equal("RESULT_CONFLICT", await ScalarAsync($"SELECT last_error FROM results_outbox WHERE command_id='{commandId:D}'"));
        Assert.Empty(await _store.GetPendingResultsAsync(10, DateTimeOffset.UtcNow.AddDays(1), CancellationToken.None));
        // Idempotent: a second call finds nothing deliverable.
        Assert.False(await _store.MarkResultConflictAsync(commandId, "RESULT_CONFLICT", CancellationToken.None));
    }

    [Fact]
    public async Task A_refused_completion_blocks_the_run_under_its_exact_claim_only()
    {
        var runId = Guid.NewGuid();
        var claim = Guid.NewGuid();
        await ExecuteAsync($"INSERT INTO etl_runs(run_id,mode,requested_entities_json,status,started_at_utc,configuration_version,created_at_utc,updated_at_utc,row_version,completion_claim_id,completion_claim_owner_id,completion_claim_acquired_at_utc) VALUES('{runId:D}','bootstrap_full','[\"clients\"]','completing','2026-09-27T00:00:00.0000000+00:00',1,'2026-09-27T00:00:00.0000000+00:00','2026-09-27T00:00:00.0000000+00:00',1,'{claim:D}','owner','2026-09-27T00:00:00.0000000+00:00');");

        Assert.False(await _store.BlockRunCompletionAsync(runId, Guid.NewGuid(), "BATCH_PAYLOAD_INVALID", "refused", CancellationToken.None));
        Assert.Equal("completing", await ScalarAsync($"SELECT status FROM etl_runs WHERE run_id='{runId:D}'"));

        Assert.True(await _store.BlockRunCompletionAsync(runId, claim, "BATCH_PAYLOAD_INVALID", "refused", CancellationToken.None));

        Assert.Equal("blocked", await ScalarAsync($"SELECT status FROM etl_runs WHERE run_id='{runId:D}'"));
        Assert.Equal("BATCH_PAYLOAD_INVALID", await ScalarAsync($"SELECT finalize_conflict_code FROM etl_runs WHERE run_id='{runId:D}'"));
        Assert.Null(await ScalarAsync($"SELECT completion_claim_id FROM etl_runs WHERE run_id='{runId:D}'"));
    }

    [Fact]
    public async Task A_conflicting_result_leaves_the_pending_count_and_joins_the_dead_letters()
    {
        var commandId = await InsertPendingResultAsync();
        var before = await _store.GetQueueMetricsAsync(CancellationToken.None);

        await _store.MarkResultConflictAsync(commandId, "RESULT_CONFLICT", CancellationToken.None);
        var after = await _store.GetQueueMetricsAsync(CancellationToken.None);

        Assert.Equal(before.ResultsPending - 1, after.ResultsPending);
        Assert.Equal(before.DeadLetters + 1, after.DeadLetters);
    }

    [Theory]
    [InlineData("RESULT_CONFLICT", "dead_letter")]
    [InlineData(null, "retry_waiting")]
    public async Task The_delivery_worker_stops_only_on_the_agreed_conflict(string? code, string expectedStatus)
    {
        var commandId = await InsertPendingResultAsync();
        var erp = new ConflictErp(code);
        var state = new ErpOnecAgent.Service.Runtime.AgentRuntimeState();
        state.CompleteBootstrap();
        using var worker = new ErpOnecAgent.Service.Workers.ResultDeliveryWorker(_store, erp, state, Microsoft.Extensions.Logging.Abstractions.NullLogger<ErpOnecAgent.Service.Workers.ResultDeliveryWorker>.Instance);

        await worker.StartAsync(CancellationToken.None);
        try
        {
            var deadline = DateTime.UtcNow.AddSeconds(15);
            while (DateTime.UtcNow < deadline && await ScalarAsync($"SELECT status FROM results_outbox WHERE command_id='{commandId:D}'") == "pending")
                await Task.Delay(50);
        }
        finally
        {
            using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            await worker.StopAsync(stop.Token);
        }

        Assert.Equal(expectedStatus, await ScalarAsync($"SELECT status FROM results_outbox WHERE command_id='{commandId:D}'"));
        Assert.Equal(1, erp.Puts);
    }

    private sealed class ConflictErp(string? code) : IErpClient
    {
        private int _puts;
        public int Puts => Volatile.Read(ref _puts);

        public Task AcknowledgeResultAsync(Guid commandId, string resultJson, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _puts);
            throw new ErpApiException(HttpStatusCode.Conflict, code, null);
        }

        public Task<ErpOnecAgent.Contracts.Erp.SessionStartResponse> StartSessionAsync(ErpOnecAgent.Contracts.Erp.SessionStartRequest request, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task<ErpOnecAgent.Contracts.Erp.LeaseResponse> LeaseCommandAsync(ErpOnecAgent.Contracts.Erp.LeaseRequest request, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task AcknowledgeReceivedAsync(Guid commandId, ErpOnecAgent.Contracts.Erp.CommandReceivedRequest request, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task<ErpOnecAgent.Contracts.Erp.BatchAcknowledgement> UploadBatchAsync(ErpOnecAgent.Domain.Etl.EtlBatch batch, Stream content, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task CompleteEtlRunAsync(Guid runId, object summary, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task SendHeartbeatAsync(ErpOnecAgent.Contracts.Erp.HeartbeatRequest request, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task<ErpOnecAgent.Contracts.Erp.RemoteConfigurationResponse?> GetConfigurationAsync(long currentVersion, CancellationToken cancellationToken) => throw new NotSupportedException();
    }

    [Theory]
    [InlineData(422, "BATCH_PAYLOAD_INVALID", true)]
    [InlineData(409, "SOURCE_IDENTITY_MISMATCH", true)]
    [InlineData(409, "RUN_GENERATION_CLOSED", true)]
    [InlineData(409, "BATCH_PAYLOAD_INVALID", false)]
    [InlineData(422, null, false)]
    [InlineData(503, "BATCH_NOT_STORED_RETRYABLE", false)]
    [InlineData(500, null, false)]
    public void Only_the_agreed_permanent_refusals_stop_completion_retries(int status, string? code, bool permanent) =>
        Assert.Equal(permanent, EtlCompletionWorker.IsPermanentCompletionRefusal(new ErpApiException((HttpStatusCode)status, code, null)));

    private async Task<Guid> InsertPendingResultAsync()
    {
        var commandId = Guid.NewGuid();
        const string at = "2026-09-27T00:00:00.0000000+00:00";
        await ExecuteAsync($"INSERT INTO commands_inbox(command_id,command_type,payload_version,priority,correlation_id,payload_json,payload_hash,status,created_at_utc,received_at_utc,queue_sequence,result_status,result_json) VALUES('{commandId:D}','synthetic',1,0,'c','{{}}','h','result_pending','{at}','{at}',(SELECT COALESCE(MAX(queue_sequence),0)+1 FROM commands_inbox),'succeeded','{{}}');");
        await ExecuteAsync($"INSERT INTO results_outbox(result_id,command_id,payload_json,payload_hash,status,created_at_utc) VALUES('{Guid.NewGuid():D}','{commandId:D}','{{}}','h','pending','{at}');");
        return commandId;
    }

    private async Task<string?> ScalarAsync(string sql)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        var value = await command.ExecuteScalarAsync(CancellationToken.None);
        return value is null or DBNull ? null : Convert.ToString(value, System.Globalization.CultureInfo.InvariantCulture);
    }

    private async Task ExecuteAsync(string sql)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync(CancellationToken.None);
    }
}
