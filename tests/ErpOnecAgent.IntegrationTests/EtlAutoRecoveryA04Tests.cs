using System.Globalization;
using System.Text.Json;
using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Domain.Etl;
using ErpOnecAgent.Infrastructure.Persistence.Sqlite;
using ErpOnecAgent.Infrastructure.Spool;
using Xunit;

namespace ErpOnecAgent.IntegrationTests;

/// <summary>
/// A04 (option A, user decision 2026-09-29): an interrupted extraction is resolved by the agent
/// at startup and, for a manual job, the same work is re-queued as a new run — atomically and with
/// a bounded chain. Runs with an unknown send outcome keep waiting for a manual R1.
/// </summary>
public sealed class EtlAutoRecoveryA04Tests : IAsyncLifetime
{
    private const string QueryMode = "bootstrap_full";
    private const string SourceNamespace = "1c-identity:v1:11111111-1111-1111-1111-111111111111:22222222-2222-2222-2222-222222222222:test";
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);
    private readonly SqliteTestDatabase _database = new();
    private SqliteConnectionFactory _factory = null!;
    private SqliteAgentStore _store = null!;

    public async Task InitializeAsync()
    {
        _factory = _database.CreateFactory(Path.Combine("data", "agent.db"));
        _store = new(_factory, new SqliteMigrator(_factory));
        await _store.InitializeAsync(CancellationToken.None);
    }

    public async Task DisposeAsync() => await _database.DisposeAsync();

    [Fact]
    public async Task An_interrupted_manual_run_is_resolved_and_its_work_requeued_in_one_commit()
    {
        var (runId, jobId, _) = await InterruptedManualRunAsync(["clients", "orders"], generation: "gen-7");

        var outcome = Assert.Single(await _store.AutoRecoverInterruptedRunsAsync(3, DateTimeOffset.UtcNow, CancellationToken.None));

        Assert.Equal(EtlAutoRecoveryResult.Requeued, outcome.Result);
        Assert.Equal(1, outcome.Attempt);
        var newRunId = outcome.NewRunId!.Value;
        // The interrupted run: resolved by the system operator (R1, decision retry), ownership released.
        Assert.NotNull(await ScalarStringAsync($"SELECT resolved_at_utc FROM etl_runs WHERE run_id='{runId:D}'"));
        Assert.Equal("agent:auto-recovery", await ScalarStringAsync($"SELECT operator_id FROM etl_run_resolutions WHERE run_id='{runId:D}'"));
        Assert.Equal("retry", await ScalarStringAsync($"SELECT decision FROM etl_run_resolutions WHERE run_id='{runId:D}'"));
        Assert.Equal(0, await ScalarAsync($"SELECT COUNT(*) FROM etl_entity_ownership WHERE owner_run_id='{runId:D}' AND released_at_utc IS NULL"));
        // The same work as a new pending run + job.
        Assert.Equal("pending", await ScalarStringAsync($"SELECT status FROM etl_runs WHERE run_id='{newRunId:D}'"));
        Assert.Equal(QueryMode, await ScalarStringAsync($"SELECT mode FROM etl_runs WHERE run_id='{newRunId:D}'"));
        Assert.Equal("gen-7", await ScalarStringAsync($"SELECT source_generation FROM etl_runs WHERE run_id='{newRunId:D}'"));
        Assert.Equal(await ScalarStringAsync($"SELECT requested_entities_json FROM etl_runs WHERE run_id='{runId:D}'"),
            await ScalarStringAsync($"SELECT requested_entities_json FROM etl_runs WHERE run_id='{newRunId:D}'"));
        Assert.Equal(await ScalarStringAsync($"SELECT entities_json FROM etl_jobs WHERE job_id='{jobId:D}'"),
            await ScalarStringAsync($"SELECT entities_json FROM etl_jobs WHERE run_id='{newRunId:D}'"));
        Assert.Equal("pending", await ScalarStringAsync($"SELECT status FROM etl_jobs WHERE run_id='{newRunId:D}'"));
        using var acceptance = JsonDocument.Parse((await ScalarStringAsync($"SELECT acceptance_result_json FROM etl_jobs WHERE run_id='{newRunId:D}'"))!);
        Assert.Equal(runId, acceptance.RootElement.GetProperty("recovery").GetProperty("recoveryOf").GetGuid());
        Assert.Equal(1, acceptance.RootElement.GetProperty("recovery").GetProperty("recoveryAttempt").GetInt32());

        // The re-queued job is claimable like any accepted job.
        var newJobId = Guid.Parse((await ScalarStringAsync($"SELECT job_id FROM etl_jobs WHERE run_id='{newRunId:D}'"))!);
        Assert.IsType<EtlJobClaimOutcome.Claimed>(await _store.TryClaimEtlJobAsync(newJobId, "test-dispatcher", DateTimeOffset.UtcNow.AddMinutes(5), DateTimeOffset.UtcNow, CancellationToken.None));

        // Idempotent: nothing left to recover.
        Assert.Empty(await _store.AutoRecoverInterruptedRunsAsync(3, DateTimeOffset.UtcNow, CancellationToken.None));
    }

    [Fact]
    public async Task The_recovery_chain_stops_at_the_limit_and_leaves_the_run_for_manual_resolution()
    {
        var (runId, _, _) = await InterruptedManualRunAsync(["clients"]);
        var current = runId;
        for (var attempt = 1; attempt <= 2; attempt++)
        {
            var recovered = Assert.Single(await _store.AutoRecoverInterruptedRunsAsync(2, DateTimeOffset.UtcNow, CancellationToken.None));
            Assert.Equal(EtlAutoRecoveryResult.Requeued, recovered.Result);
            Assert.Equal(attempt, recovered.Attempt);
            // The recovery run is interrupted too.
            current = recovered.NewRunId!.Value;
            await InterruptClaimedAsync(current, "clients");
        }

        var limit = Assert.Single(await _store.AutoRecoverInterruptedRunsAsync(2, DateTimeOffset.UtcNow, CancellationToken.None));

        Assert.Equal(EtlAutoRecoveryResult.LimitReached, limit.Result);
        Assert.Equal(current, limit.RunId);
        Assert.Null(await ScalarStringAsync($"SELECT resolved_at_utc FROM etl_runs WHERE run_id='{current:D}'"));
        Assert.Equal("INTERRUPTED_NO_CHECKPOINT", await ScalarStringAsync($"SELECT finalize_conflict_code FROM etl_runs WHERE run_id='{current:D}'"));
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM etl_runs WHERE status='pending'"));
    }

    [Fact]
    public async Task A_run_with_an_unknown_send_outcome_is_never_auto_recovered()
    {
        var (runId, _, claim) = await ClaimedManualRunAsync(["clients"]);
        var spool = new FileSpoolStore(Path.Combine(_database.Root, "spool"));
        Assert.IsType<EtlEntityBeginOutcome.Begun>(await _store.BeginEtlEntityExtractionAsync(runId, claim, Request("clients"), CancellationToken.None));
        var batch = await spool.WriteBatchAsync(runId, Entity("clients"), Rows("clients", 2), null, null, CancellationToken.None);
        Assert.IsType<EtlBatchRegistrationOutcome.Registered>(await _store.RegisterGuardedEtlBatchAsync(batch, claim, CancellationToken.None));
        // A send was admitted and the process died before its outcome was recorded.
        Assert.IsType<EtlBatchUploadClaimOutcome.Claimed>(await _store.TryClaimBatchUploadAsync(batch.BatchId, "uploader", DateTimeOffset.UtcNow, 5, CancellationToken.None));
        await _store.RecoverInterruptedEtlRunsAsync(CancellationToken.None);
        Assert.Equal("UPLOAD_OUTCOME_UNKNOWN", await ScalarStringAsync($"SELECT finalize_conflict_code FROM etl_runs WHERE run_id='{runId:D}'"));

        Assert.Empty(await _store.AutoRecoverInterruptedRunsAsync(3, DateTimeOffset.UtcNow, CancellationToken.None));
        Assert.Null(await ScalarStringAsync($"SELECT resolved_at_utc FROM etl_runs WHERE run_id='{runId:D}'"));
    }

    [Fact]
    public async Task An_interrupted_scheduled_run_is_resolved_and_its_schedule_key_released()
    {
        var created = Assert.IsType<EtlScheduledRunEnsureOutcome.Created>(await _store.EnsureScheduledEtlRunAsync(
            new EtlScheduledRunRequest("etl:incremental", "incremental", [Entity("clients")], 7), DateTimeOffset.UtcNow, CancellationToken.None));
        Assert.IsType<EtlScheduledRunClaimOutcome.Claimed>(await _store.TryClaimScheduledRunAsync(created.RunId, "scheduler", DateTimeOffset.UtcNow, CancellationToken.None));
        await _store.RecoverInterruptedEtlRunsAsync(CancellationToken.None);

        var outcome = Assert.Single(await _store.AutoRecoverInterruptedRunsAsync(3, DateTimeOffset.UtcNow, CancellationToken.None));

        Assert.Equal(EtlAutoRecoveryResult.ScheduledReleased, outcome.Result);
        Assert.Null(outcome.NewRunId);
        Assert.NotNull(await ScalarStringAsync($"SELECT resolved_at_utc FROM etl_runs WHERE run_id='{created.RunId:D}'"));
        // The key is free: the scheduler creates the next run as usual.
        Assert.IsType<EtlScheduledRunEnsureOutcome.Created>(await _store.EnsureScheduledEtlRunAsync(
            new EtlScheduledRunRequest("etl:incremental", "incremental", [Entity("clients")], 7), DateTimeOffset.UtcNow, CancellationToken.None));
    }

    // ---------- A04b: the closing complete of an interrupted run ----------

    [Fact]
    public async Task A_closing_complete_is_queued_when_erp_acknowledged_a_batch_of_the_interrupted_run()
    {
        var (runId, _, claim) = await ClaimedManualRunAsync(["clients", "orders"], generation: "gen-7");
        await AcknowledgedBatchAsync(runId, claim, "clients", rows: 3);
        Assert.IsType<EtlEntityBeginOutcome.Begun>(await _store.BeginEtlEntityExtractionAsync(runId, claim, Request("orders"), CancellationToken.None));
        await _store.RecoverInterruptedEtlRunsAsync(CancellationToken.None);

        var outcome = Assert.Single(await _store.AutoRecoverInterruptedRunsAsync(3, DateTimeOffset.UtcNow, CancellationToken.None));

        Assert.True(outcome.InterruptionNoticeQueued);
        var notice = Assert.Single(await _store.GetDueInterruptionNoticesAsync(10, DateTimeOffset.UtcNow, CancellationToken.None));
        Assert.Equal(runId, notice.RunId);
        using var payload = JsonDocument.Parse(notice.PayloadJson);
        var root = payload.RootElement;
        // complete v2 exactly as agreed (to-onec/0049): no extra property, every entity failed RUN_INTERRUPTED.
        Assert.Equal(["batchesAcknowledged", "batchesCreated", "completedAtUtc", "entities", "entitiesFailed", "mode", "rowsRead", "runId", "sourceGeneration", "sourceIdentity", "status"],
            root.EnumerateObject().Select(static property => property.Name).Order(StringComparer.Ordinal).ToArray());
        Assert.Equal(runId, root.GetProperty("runId").GetGuid());
        Assert.Equal("partial_success", root.GetProperty("status").GetString());
        Assert.Equal(QueryMode, root.GetProperty("mode").GetString());
        Assert.Equal("gen-7", root.GetProperty("sourceGeneration").GetString());
        Assert.Equal(1, root.GetProperty("batchesAcknowledged").GetInt64());
        Assert.Equal(2, root.GetProperty("entitiesFailed").GetInt64());
        var entities = root.GetProperty("entities").EnumerateArray().ToArray();
        Assert.Equal("clients,orders", string.Join(',', entities.Select(static entity => entity.GetProperty("entity").GetString())));
        Assert.All(entities, static entity =>
        {
            Assert.Equal("failed", entity.GetProperty("status").GetString());
            Assert.Equal("RUN_INTERRUPTED", entity.GetProperty("errorCode").GetString());
        });
    }

    [Fact]
    public async Task No_closing_complete_is_queued_when_erp_holds_no_batch_of_the_run()
    {
        await InterruptedManualRunAsync(["clients"]);

        var outcome = Assert.Single(await _store.AutoRecoverInterruptedRunsAsync(3, DateTimeOffset.UtcNow, CancellationToken.None));

        Assert.False(outcome.InterruptionNoticeQueued);
        Assert.Empty(await _store.GetDueInterruptionNoticesAsync(10, DateTimeOffset.UtcNow, CancellationToken.None));
    }

    [Theory]
    [InlineData(200, null, "sent", false)]
    [InlineData(409, "RUN_BATCHES_MISMATCH", "refused", false)]
    [InlineData(422, "BATCH_PAYLOAD_INVALID", "refused", false)]
    [InlineData(503, "RUN_NOT_READY", "pending", true)]
    [InlineData(409, null, "pending", true)]
    public async Task The_notice_worker_sends_the_exact_bytes_and_never_retries_a_coded_refusal(int status, string? code, string expectedStatus, bool retryScheduled)
    {
        var (runId, _, claim) = await ClaimedManualRunAsync(["clients"]);
        await AcknowledgedBatchAsync(runId, claim, "clients", rows: 2);
        await _store.RecoverInterruptedEtlRunsAsync(CancellationToken.None);
        await _store.AutoRecoverInterruptedRunsAsync(3, DateTimeOffset.UtcNow, CancellationToken.None);
        var stored = Assert.Single(await _store.GetDueInterruptionNoticesAsync(10, DateTimeOffset.UtcNow, CancellationToken.None)).PayloadJson;
        var erp = new CompleteOnlyErp(status == 200 ? null : new ErpApiException((System.Net.HttpStatusCode)status, code, null, "req-1"));
        var worker = new ErpOnecAgent.Service.Workers.Etl.EtlInterruptionNoticeWorker(_store, erp, ReadyState(),
            Microsoft.Extensions.Logging.Abstractions.NullLogger<ErpOnecAgent.Service.Workers.Etl.EtlInterruptionNoticeWorker>.Instance);

        await worker.RunOnceAsync(CancellationToken.None);
        await worker.RunOnceAsync(CancellationToken.None);

        // Exactly the stored bytes, once: a scheduled retry lies in the future, a terminal status is never re-sent.
        var sent = Assert.Single(erp.Sent);
        Assert.Equal(runId, sent.RunId);
        Assert.Equal(stored, sent.Payload);
        Assert.Equal(expectedStatus, await ScalarStringAsync($"SELECT status FROM etl_run_interruption_notices WHERE run_id='{runId:D}'"));
        Assert.Equal(1, await ScalarAsync($"SELECT attempt_count FROM etl_run_interruption_notices WHERE run_id='{runId:D}'"));
        Assert.Equal(retryScheduled, await ScalarStringAsync($"SELECT next_attempt_at_utc FROM etl_run_interruption_notices WHERE run_id='{runId:D}'") is not null);
    }

    [Fact]
    public async Task A_run_erp_knows_from_the_command_result_is_closed_even_without_a_batch()
    {
        // to-onec/0051: ERP opens a manual run from the start_full_sync result (data.runId) —
        // before any batch. Interrupted before its first read, the run has no recorded namespace.
        var commandId = Guid.NewGuid();
        var (runId, _, _) = await ClaimedManualRunAsync(["clients", "orders"], generation: "gen-7", commandId: commandId);
        await DeliveredCommandResultAsync(commandId);
        await _store.RecoverInterruptedEtlRunsAsync(CancellationToken.None);
        Assert.Equal("INTERRUPTED_NO_CHECKPOINT", await ScalarStringAsync($"SELECT finalize_conflict_code FROM etl_runs WHERE run_id='{runId:D}'"));

        var outcome = Assert.Single(await _store.AutoRecoverInterruptedRunsAsync(3, DateTimeOffset.UtcNow, CancellationToken.None, SourceNamespace));

        Assert.True(outcome.InterruptionNoticeQueued);
        using var payload = JsonDocument.Parse(Assert.Single(await _store.GetDueInterruptionNoticesAsync(10, DateTimeOffset.UtcNow, CancellationToken.None)).PayloadJson);
        var root = payload.RootElement;
        Assert.Equal("partial_success", root.GetProperty("status").GetString());
        Assert.Equal(0, root.GetProperty("batchesAcknowledged").GetInt64());
        Assert.Equal(0, root.GetProperty("batchesCreated").GetInt64());
        Assert.Equal(0, root.GetProperty("rowsRead").GetInt64());
        Assert.Equal(2, root.GetProperty("entitiesFailed").GetInt64());
        // Identity from the current binding (the run never recorded one).
        Assert.Equal("11111111-1111-1111-1111-111111111111", root.GetProperty("sourceIdentity").GetProperty("databaseId").GetString());
        // Every FROZEN entity, never begun ones included, failed RUN_INTERRUPTED with zeros.
        var entities = root.GetProperty("entities").EnumerateArray().ToArray();
        Assert.Equal("clients,orders", string.Join(',', entities.Select(static entity => entity.GetProperty("entity").GetString())));
        Assert.All(entities, static entity =>
        {
            Assert.Equal("failed", entity.GetProperty("status").GetString());
            Assert.Equal("RUN_INTERRUPTED", entity.GetProperty("errorCode").GetString());
            Assert.Equal("full", entity.GetProperty("readScope").GetString());
            Assert.Equal(0, entity.GetProperty("rowsRead").GetInt64());
        });
    }

    [Fact]
    public async Task A_run_erp_does_not_know_and_without_a_binding_is_not_closed()
    {
        var commandId = Guid.NewGuid();
        await ClaimedManualRunAsync(["clients"], commandId: commandId);
        await DeliveredCommandResultAsync(commandId);
        await _store.RecoverInterruptedEtlRunsAsync(CancellationToken.None);

        // The result reached ERP, but with no recorded namespace and no binding there is no identity to send.
        var outcome = Assert.Single(await _store.AutoRecoverInterruptedRunsAsync(3, DateTimeOffset.UtcNow, CancellationToken.None));

        Assert.Equal(EtlAutoRecoveryResult.Requeued, outcome.Result);
        Assert.False(outcome.InterruptionNoticeQueued);
    }

    private async Task DeliveredCommandResultAsync(Guid commandId)
    {
        using var document = JsonDocument.Parse("{\"entities\":[]}");
        var payload = document.RootElement.Clone();
        await _store.StoreCommandAsync(new ErpOnecAgent.Domain.Commands.CommandEnvelope(commandId, "start_full_sync", 1, 100, null, null, DateTimeOffset.UtcNow, null, null, null,
            ErpOnecAgent.Domain.Common.PayloadHasher.Compute(payload), payload), DateTimeOffset.UtcNow, CancellationToken.None);
        await ExecuteSqlAsync(
            "INSERT INTO results_outbox(result_id,command_id,payload_json,payload_hash,status,created_at_utc,acknowledged_at_utc) VALUES($id,$command,'{}','hash','acknowledged',$now,$now);",
            ("$id", Guid.NewGuid().ToString("D")), ("$command", commandId.ToString("D")), ("$now", DateTimeOffset.UtcNow.ToString("O")));
    }

    private async Task AcknowledgedBatchAsync(Guid runId, Guid claim, string entity, int rows)
    {
        var spool = new FileSpoolStore(Path.Combine(_database.Root, "spool"));
        Assert.IsType<EtlEntityBeginOutcome.Begun>(await _store.BeginEtlEntityExtractionAsync(runId, claim, Request(entity), CancellationToken.None));
        var batch = await spool.WriteBatchAsync(runId, Entity(entity), Rows(entity, rows), null, null, CancellationToken.None);
        Assert.IsType<EtlBatchRegistrationOutcome.Registered>(await _store.RegisterGuardedEtlBatchAsync(batch, claim, CancellationToken.None));
        var send = Assert.IsType<EtlBatchUploadClaimOutcome.Claimed>(await _store.TryClaimBatchUploadAsync(batch.BatchId, "uploader", DateTimeOffset.UtcNow, 5, CancellationToken.None));
        Assert.IsType<EtlBatchAckOutcome.Acknowledged>(await _store.AcknowledgeClaimedBatchAsync(batch.BatchId, send.Claim.AttemptId,
            new EtlBatchAckEvidence("acknowledged", batch.BatchId, rows, true, DateTimeOffset.UtcNow), "ack-hash", 200, CancellationToken.None));
    }

    private static ErpOnecAgent.Service.Runtime.AgentRuntimeState ReadyState()
    {
        var state = new ErpOnecAgent.Service.Runtime.AgentRuntimeState();
        state.RestoreLocalEtlPause(false);
        state.SetRemoteMode(ErpOnecAgent.Domain.Agent.AgentMode.Normal);
        state.CompleteBootstrap();
        return state;
    }

    private sealed class CompleteOnlyErp(Exception? failure) : IErpClient
    {
        public List<(Guid RunId, string Payload)> Sent { get; } = [];

        public Task CompleteEtlRunRawAsync(Guid runId, string completePayloadJson, CancellationToken cancellationToken)
        {
            Sent.Add((runId, completePayloadJson));
            return failure is null ? Task.CompletedTask : Task.FromException(failure);
        }

        public Task<ErpOnecAgent.Contracts.Erp.SessionStartResponse> StartSessionAsync(ErpOnecAgent.Contracts.Erp.SessionStartRequest request, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task<ErpOnecAgent.Contracts.Erp.LeaseResponse> LeaseCommandAsync(ErpOnecAgent.Contracts.Erp.LeaseRequest request, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task AcknowledgeReceivedAsync(Guid commandId, ErpOnecAgent.Contracts.Erp.CommandReceivedRequest request, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task AcknowledgeResultAsync(Guid commandId, string resultJson, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task<ErpOnecAgent.Contracts.Erp.BatchAcknowledgement> UploadBatchAsync(EtlBatch batch, Stream content, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task CompleteEtlRunAsync(Guid runId, object summary, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task SendHeartbeatAsync(ErpOnecAgent.Contracts.Erp.HeartbeatRequest request, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task<ErpOnecAgent.Contracts.Erp.RemoteConfigurationResponse?> GetConfigurationAsync(long currentVersion, CancellationToken cancellationToken) => throw new NotSupportedException();
    }

    // ---------- helpers ----------

    private async Task<(Guid RunId, Guid JobId, Guid Claim)> InterruptedManualRunAsync(string[] entities, string? generation = null)
    {
        var (runId, jobId, claim) = await ClaimedManualRunAsync(entities, generation);
        Assert.IsType<EtlEntityBeginOutcome.Begun>(await _store.BeginEtlEntityExtractionAsync(runId, claim, Request(entities[0]), CancellationToken.None));
        await _store.RecoverInterruptedEtlRunsAsync(CancellationToken.None);
        Assert.Equal("INTERRUPTED_NO_CHECKPOINT", await ScalarStringAsync($"SELECT finalize_conflict_code FROM etl_runs WHERE run_id='{runId:D}'"));
        return (runId, jobId, claim);
    }

    private async Task InterruptClaimedAsync(Guid runId, string entity)
    {
        var jobId = Guid.Parse((await ScalarStringAsync($"SELECT job_id FROM etl_jobs WHERE run_id='{runId:D}'"))!);
        var claimed = Assert.IsType<EtlJobClaimOutcome.Claimed>(await _store.TryClaimEtlJobAsync(jobId, "test-dispatcher", DateTimeOffset.UtcNow.AddMinutes(5), DateTimeOffset.UtcNow, CancellationToken.None));
        Assert.IsType<EtlEntityBeginOutcome.Begun>(await _store.BeginEtlEntityExtractionAsync(runId, claimed.Claim.ExtractionClaimId, Request(entity), CancellationToken.None));
        await _store.RecoverInterruptedEtlRunsAsync(CancellationToken.None);
    }

    private async Task<(Guid RunId, Guid JobId, Guid Claim)> ClaimedManualRunAsync(string[] entities, string? generation = null, Guid? commandId = null)
    {
        var runId = Guid.NewGuid();
        var jobId = Guid.NewGuid();
        var now = DateTimeOffset.UtcNow.ToString("O");
        await ExecuteSqlAsync(
            "INSERT INTO etl_runs(run_id,mode,requested_entities_json,status,configuration_version,created_at_utc,updated_at_utc,row_version,source_generation) VALUES($run,$mode,$manifest,'pending',7,$now,$now,1,$gen);",
            ("$run", runId.ToString("D")), ("$mode", QueryMode), ("$manifest", JsonSerializer.Serialize(entities, JsonOptions)), ("$now", now), ("$gen", generation));
        await ExecuteSqlAsync(
            "INSERT INTO etl_jobs(job_id,command_id,run_id,mode,entities_json,configuration_version,status,command_payload_hash,acceptance_result_json,created_at_utc,updated_at_utc,row_version) VALUES($job,$cmd,$run,$mode,$defs,7,'pending','hash','{}',$now,$now,1);",
            ("$job", jobId.ToString("D")), ("$cmd", (commandId ?? Guid.NewGuid()).ToString("D")), ("$run", runId.ToString("D")), ("$mode", QueryMode),
            ("$defs", JsonSerializer.Serialize(entities.Select(Entity).ToArray(), JsonOptions)), ("$now", now));
        var claimed = Assert.IsType<EtlJobClaimOutcome.Claimed>(
            await _store.TryClaimEtlJobAsync(jobId, "test-dispatcher", DateTimeOffset.UtcNow.AddMinutes(5), DateTimeOffset.UtcNow, CancellationToken.None));
        return (runId, jobId, claimed.Claim.ExtractionClaimId);
    }

    private static EtlEntityDefinition Entity(string code) =>
        new(code, "Catalog_" + code, "Ref_Key", "UpdatedAt", "DeletionMark", ["Ref_Key", "UpdatedAt", "DeletionMark"], "incremental", 500, 10);

    private static EtlEntityExtractionRequest Request(string entity) =>
        new(entity, JsonSerializer.Serialize(Entity(entity), JsonOptions), SourceNamespace, QueryMode,
            JsonSerializer.Serialize(new EtlCursor(DateTimeOffset.UtcNow, null), JsonOptions));

    private static List<JsonElement> Rows(string entity, int count) =>
        Enumerable.Range(0, count).Select(index => JsonDocument.Parse($"{{\"Ref_Key\":\"{entity}-{index}\",\"UpdatedAt\":\"2026-09-19T09:00:00Z\",\"DeletionMark\":false}}").RootElement.Clone()).ToList();

    private async Task<long> ScalarAsync(string sql)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        return Convert.ToInt64(await command.ExecuteScalarAsync(CancellationToken.None), CultureInfo.InvariantCulture);
    }

    private async Task<string?> ScalarStringAsync(string sql)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        var value = await command.ExecuteScalarAsync(CancellationToken.None);
        return value is null or DBNull ? null : Convert.ToString(value, CultureInfo.InvariantCulture);
    }

    private async Task ExecuteSqlAsync(string sql, params (string Name, object? Value)[] parameters)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        foreach (var (name, value) in parameters) command.Parameters.AddWithValue(name, value ?? DBNull.Value);
        await command.ExecuteNonQueryAsync(CancellationToken.None);
    }
}
