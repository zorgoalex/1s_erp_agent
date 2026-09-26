using ErpOnecAgent.Application.Configuration;
using ErpOnecAgent.Domain.Agent;
using ErpOnecAgent.Infrastructure.Persistence.Sqlite;
using ErpOnecAgent.Service.Runtime;
using ErpOnecAgent.Service.Workers;
using Xunit;

namespace ErpOnecAgent.IntegrationTests;

/// <summary>
/// Stage 6 health: queue ages and unresolved ETL runs are measured by the store; unresolved
/// runs and ETL lag degrade the health state reported in the heartbeat.
/// </summary>
public sealed class HealthMetricsTests : IAsyncLifetime
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
    public async Task An_empty_store_has_no_ages_and_no_unresolved_runs()
    {
        var queues = await _store.GetQueueMetricsAsync(CancellationToken.None);

        Assert.Null(queues.OldestPendingCommandAtUtc);
        Assert.Null(queues.OldestPendingResultAtUtc);
        Assert.Equal(0, queues.EtlRunsUnresolved);
    }

    [Fact]
    public async Task The_oldest_pending_command_and_result_are_reported()
    {
        var older = new DateTimeOffset(2026, 9, 20, 8, 0, 0, TimeSpan.Zero);
        var newer = new DateTimeOffset(2026, 9, 25, 8, 0, 0, TimeSpan.Zero);
        await InsertCommandAsync("queued", older);
        await InsertCommandAsync("retry_waiting", newer);
        await InsertCommandAsync("succeeded", older.AddDays(-3)); // finished: not pending
        await InsertResultAsync("pending", newer);
        await InsertResultAsync("acknowledged", older.AddDays(-3)); // delivered: not pending

        var queues = await _store.GetQueueMetricsAsync(CancellationToken.None);

        Assert.Equal(older, queues.OldestPendingCommandAtUtc);
        Assert.Equal(newer, queues.OldestPendingResultAtUtc);
    }

    [Fact]
    public async Task Blocked_and_failed_runs_count_until_they_are_resolved()
    {
        var blocked = await InsertRunAsync("blocked");
        await InsertRunAsync("failed");
        await InsertRunAsync("succeeded");
        await InsertRunAsync("uploading");
        Assert.Equal(2, (await _store.GetQueueMetricsAsync(CancellationToken.None)).EtlRunsUnresolved);

        await ExecuteAsync($"INSERT INTO etl_run_resolutions(run_id,resolution_id,resolved_at_utc,operator_id,decision,remote_verification,workers_quiesced,prior_status,ownership_released,batches_fenced) VALUES('{blocked:D}','{Guid.NewGuid():D}','2026-09-26T00:00:00.0000000+00:00','ops','abandon','checked',1,'blocked',0,0);");

        Assert.Equal(1, (await _store.GetQueueMetricsAsync(CancellationToken.None)).EtlRunsUnresolved);
    }

    [Fact]
    public void Unresolved_etl_runs_degrade_health()
    {
        var state = HealthyState();

        Assert.Equal("healthy", Health(state, new QueueMetrics(0, 0, 0, 0)));
        Assert.Equal("degraded", Health(state, new QueueMetrics(0, 0, 0, 0, EtlRunsUnresolved: 1)));
    }

    [Fact]
    public void Etl_lag_beyond_the_limit_degrades_health_only_after_a_first_success()
    {
        var state = HealthyState();
        Assert.Equal("healthy", Health(state, new QueueMetrics(0, 0, 0, 0), TimeSpan.FromMinutes(45)));

        state.LastEtlSuccessAtUtc = DateTimeOffset.UtcNow.AddMinutes(-10);
        Assert.Equal("healthy", Health(state, new QueueMetrics(0, 0, 0, 0), TimeSpan.FromMinutes(45)));

        state.LastEtlSuccessAtUtc = DateTimeOffset.UtcNow.AddHours(-2);
        Assert.Equal("degraded", Health(state, new QueueMetrics(0, 0, 0, 0), TimeSpan.FromMinutes(45)));
        Assert.Equal("healthy", Health(state, new QueueMetrics(0, 0, 0, 0), etlLagLimit: null));
    }

    [Fact]
    public void Storage_and_compatibility_still_outrank_etl_degradation()
    {
        var state = HealthyState();
        var full = new AgentMetrics(0, 0, 0, 0, 0);

        Assert.Equal("storage_critical", HeartbeatWorker.GetHealthState(state, full, new StorageOptions { MinimumReservedBytesForCommands = 1 }, TimeSpan.FromSeconds(30), new QueueMetrics(0, 0, 0, 0, EtlRunsUnresolved: 3)));
    }

    [Fact]
    public void The_degraded_reason_is_reported()
    {
        var state = HealthyState();
        var metrics = new AgentMetrics(long.MaxValue, 0, 0, 0, 0);

        Assert.Equal(("healthy", "OK"), HeartbeatWorker.EvaluateHealth(state, metrics, new StorageOptions(), TimeSpan.FromSeconds(30), new QueueMetrics(0, 0, 0, 0)));
        Assert.Equal(("degraded", "ETL_RUNS_UNRESOLVED"), HeartbeatWorker.EvaluateHealth(state, metrics, new StorageOptions(), TimeSpan.FromSeconds(30), new QueueMetrics(0, 0, 0, 0, EtlRunsUnresolved: 1)));
        state.OnecODataAvailable = false;
        Assert.Equal(("degraded", "ONEC_ODATA_UNAVAILABLE"), HeartbeatWorker.EvaluateHealth(state, metrics, new StorageOptions(), TimeSpan.FromSeconds(30), new QueueMetrics(0, 0, 0, 0, EtlRunsUnresolved: 1)));
    }

    [Fact]
    public async Task A_dead_letter_result_does_not_count_as_waiting_for_delivery()
    {
        await InsertResultAsync("dead_letter", new DateTimeOffset(2026, 9, 1, 0, 0, 0, TimeSpan.Zero));

        Assert.Null((await _store.GetQueueMetricsAsync(CancellationToken.None)).OldestPendingResultAtUtc);
    }

    [Fact]
    public async Task An_entity_skipped_in_its_latest_finalized_run_is_failing_until_a_later_run_completes_it()
    {
        var first = await InsertRunAsync("succeeded");
        await ExecuteAsync($"UPDATE etl_runs SET finished_at_utc='2026-09-26T01:00:00.0000000+00:00' WHERE run_id='{first:D}';");
        await InsertEntityAsync(first, "clients", "done", null);
        await InsertEntityAsync(first, "orders", "failed", "ODATA_HTTP_500");
        // A run-termination 'failed' row without a code in an unfinished run never counts.
        var blocked = await InsertRunAsync("blocked");
        await InsertEntityAsync(blocked, "stock", "failed", null);

        Assert.Equal(1, (await _store.GetQueueMetricsAsync(CancellationToken.None)).EtlEntitiesFailing);
        Assert.Equal(("degraded", "ETL_ENTITIES_FAILING"),
            HeartbeatWorker.EvaluateHealth(HealthyState(), new AgentMetrics(long.MaxValue, 0, 0, 0, 0), new StorageOptions(), TimeSpan.FromSeconds(30), new QueueMetrics(0, 0, 0, 0, EtlEntitiesFailing: 1)));

        var second = await InsertRunAsync("succeeded");
        await ExecuteAsync($"UPDATE etl_runs SET finished_at_utc='2026-09-26T02:00:00.0000000+00:00' WHERE run_id='{second:D}';");
        await InsertEntityAsync(second, "orders", "done", null);

        Assert.Equal(0, (await _store.GetQueueMetricsAsync(CancellationToken.None)).EtlEntitiesFailing);
    }

    private async Task InsertEntityAsync(Guid runId, string entity, string status, string? failureCode)
    {
        var code = failureCode is null ? "NULL" : $"'{failureCode}'";
        await ExecuteAsync($"INSERT INTO etl_run_entities(run_id,entity_name,entity_definition_json,domain_fingerprint,status,base_row_present,domain_status,rows_read,batches_created,failure_code,created_at_utc,updated_at_utc,row_version) VALUES('{runId:D}','{entity}','{{}}','fp','{status}',0,'absent',0,0,{code},'2026-09-26T00:00:00.0000000+00:00','2026-09-26T00:00:00.0000000+00:00',1);");
    }

    private static string Health(AgentRuntimeState state, QueueMetrics queues, TimeSpan? etlLagLimit = null) =>
        HeartbeatWorker.GetHealthState(state, new AgentMetrics(long.MaxValue, 0, 0, 0, 0), new StorageOptions(), TimeSpan.FromSeconds(30), queues, etlLagLimit);

    private static AgentRuntimeState HealthyState()
    {
        var state = new AgentRuntimeState();
        state.SetRemoteMode(AgentMode.Normal);
        state.CompleteBootstrap();
        state.OnecCommandApiAvailable = true;
        state.OnecODataAvailable = true;
        return state;
    }

    private async Task InsertCommandAsync(string status, DateTimeOffset receivedAt)
    {
        var id = Guid.NewGuid();
        var at = receivedAt.ToString("O", System.Globalization.CultureInfo.InvariantCulture);
        await ExecuteAsync($"INSERT INTO commands_inbox(command_id,command_type,payload_version,priority,correlation_id,payload_json,payload_hash,status,created_at_utc,received_at_utc,queue_sequence) VALUES('{id:D}','synthetic',1,0,'c','{{}}','h','{status}','{at}','{at}',(SELECT COALESCE(MAX(queue_sequence),0)+1 FROM commands_inbox));");
    }

    private async Task InsertResultAsync(string status, DateTimeOffset createdAt)
    {
        var commandId = Guid.NewGuid();
        var at = createdAt.ToString("O", System.Globalization.CultureInfo.InvariantCulture);
        await ExecuteAsync($"INSERT INTO commands_inbox(command_id,command_type,payload_version,priority,correlation_id,payload_json,payload_hash,status,created_at_utc,received_at_utc,queue_sequence) VALUES('{commandId:D}','synthetic',1,0,'c','{{}}','h','succeeded','{at}','{at}',(SELECT COALESCE(MAX(queue_sequence),0)+1 FROM commands_inbox));");
        await ExecuteAsync($"INSERT INTO results_outbox(result_id,command_id,payload_json,payload_hash,status,created_at_utc) VALUES('{Guid.NewGuid():D}','{commandId:D}','{{}}','h','{status}','{at}');");
    }

    private async Task<Guid> InsertRunAsync(string status)
    {
        var runId = Guid.NewGuid();
        await ExecuteAsync($"INSERT INTO etl_runs(run_id,mode,requested_entities_json,status,started_at_utc,configuration_version,created_at_utc,updated_at_utc,row_version) VALUES('{runId:D}','incremental','[]','{status}','2026-09-26T00:00:00.0000000+00:00',1,'2026-09-26T00:00:00.0000000+00:00','2026-09-26T00:00:00.0000000+00:00',1);");
        return runId;
    }

    private async Task ExecuteAsync(string sql)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync(CancellationToken.None);
    }
}
