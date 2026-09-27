using System.Net;
using System.Text;
using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Application.Configuration;
using ErpOnecAgent.Contracts.Erp;
using ErpOnecAgent.Domain.Etl;
using ErpOnecAgent.Infrastructure.ErpApi;
using ErpOnecAgent.Infrastructure.Persistence.Sqlite;
using ErpOnecAgent.Service.Runtime;
using Microsoft.Extensions.Options;
using Xunit;

namespace ErpOnecAgent.IntegrationTests;

/// <summary>E6 (spec В-4): GET etl/batches/{id} and the read-only --etl-check-batches evidence for R1.</summary>
public sealed class E6BatchLookupTests : IAsyncLifetime
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
    public async Task Ok_returns_the_original_ack_and_404_means_not_stored()
    {
        var batchId = Guid.NewGuid();
        var handler = new RoutingHandler(request => request.RequestUri!.AbsolutePath.EndsWith($"/etl/batches/{batchId:D}", StringComparison.Ordinal)
            ? Json(HttpStatusCode.OK, Ack(batchId))
            : new HttpResponseMessage(HttpStatusCode.NotFound));
        var client = Client(handler);

        var stored = await client.GetBatchStatusAsync(batchId, CancellationToken.None);
        var missing = await client.GetBatchStatusAsync(Guid.NewGuid(), CancellationToken.None);

        Assert.True(stored.Stored);
        Assert.Equal(7, stored.Acknowledgement!.RowsAccepted);
        Assert.False(missing.Stored);
        Assert.Null(missing.Acknowledgement);
        Assert.All(handler.Requests, static r => Assert.Equal(HttpMethod.Get, r.Method));
    }

    [Fact]
    public async Task An_ack_for_another_batch_or_an_error_status_is_not_an_answer()
    {
        var other = Guid.NewGuid();
        await Assert.ThrowsAsync<InvalidDataException>(() => Client(new RoutingHandler(_ => Json(HttpStatusCode.OK, Ack(other)))).GetBatchStatusAsync(Guid.NewGuid(), CancellationToken.None));
        var failure = await Assert.ThrowsAsync<ErpApiException>(() => Client(new RoutingHandler(_ => Json(HttpStatusCode.InternalServerError, "{\"code\":\"INTERNAL\"}"))).GetBatchStatusAsync(Guid.NewGuid(), CancellationToken.None));
        Assert.Equal(HttpStatusCode.InternalServerError, failure.StatusCode);
    }

    [Fact]
    public async Task The_store_lists_only_unacknowledged_batches_of_the_run_with_their_last_outcome()
    {
        var runId = Guid.NewGuid();
        var otherRun = Guid.NewGuid();
        await InsertRunAsync(runId);
        await InsertRunAsync(otherRun);
        var unknown = await InsertBatchAsync(runId, "dead_letter", "UPLOAD_OUTCOME_UNKNOWN", "2026-09-28T00:00:01.0000000+00:00");
        await InsertAttemptAsync(unknown, 1, "precheck_failed");
        await InsertAttemptAsync(unknown, 2, "unknown");
        var ready = await InsertBatchAsync(runId, "ready", null, "2026-09-28T00:00:02.0000000+00:00");
        await InsertBatchAsync(runId, "acknowledged", null, "2026-09-28T00:00:03.0000000+00:00");
        await InsertBatchAsync(otherRun, "dead_letter", "UPLOAD_OUTCOME_UNKNOWN", "2026-09-28T00:00:04.0000000+00:00");

        var batches = await _store.GetUnacknowledgedRunBatchesAsync(runId, CancellationToken.None);

        Assert.Equal([unknown, ready], batches.Select(static b => b.BatchId).ToArray());
        Assert.Equal(("dead_letter", "UPLOAD_OUTCOME_UNKNOWN", 2, "unknown"), (batches[0].Status, batches[0].QuarantineCode, batches[0].SendAttempts, batches[0].LastOutcome));
        Assert.Equal((0, (string?)null), (batches[1].SendAttempts, batches[1].LastOutcome));
    }

    [Fact]
    public async Task Check_batches_reports_erp_answers_and_writes_nothing_locally()
    {
        var runId = Guid.NewGuid();
        await InsertRunAsync(runId);
        var stored = await InsertBatchAsync(runId, "dead_letter", "UPLOAD_OUTCOME_UNKNOWN", "2026-09-28T00:00:01.0000000+00:00");
        var notStored = await InsertBatchAsync(runId, "dead_letter", "UPLOAD_OUTCOME_UNKNOWN", "2026-09-28T00:00:02.0000000+00:00");
        var failing = await InsertBatchAsync(runId, "dead_letter", "ACK_INVALID", "2026-09-28T00:00:03.0000000+00:00");
        var erp = new LookupErp(new Dictionary<Guid, Func<EtlBatchRemoteStatus>>
        {
            [stored] = () => new EtlBatchRemoteStatus(true, new BatchAcknowledgement(stored, "accepted", 5, true, DateTimeOffset.UtcNow)),
            [notStored] = () => new EtlBatchRemoteStatus(false, null),
            [failing] = () => throw new ErpApiException(HttpStatusCode.BadGateway, null, null)
        });
        var before = await SnapshotAsync();
        using var output = new StringWriter();

        var exit = await CliRunner.CheckRunBatchesAsync(erp, _store, runId.ToString("D"), output, CancellationToken.None);

        Assert.Equal(2, exit);
        var text = output.ToString();
        Assert.Contains($"{stored:D} entity=clients local=dead_letter/UPLOAD_OUTCOME_UNKNOWN", text, StringComparison.Ordinal);
        Assert.Contains("erp=STORED rowsAccepted=5 checksumValid=True", text, StringComparison.Ordinal);
        Assert.Contains($"{notStored:D} entity=clients", text, StringComparison.Ordinal);
        Assert.Contains("erp=NOT_STORED", text, StringComparison.Ordinal);
        Assert.Contains("erp=LOOKUP_FAILED http=502", text, StringComparison.Ordinal);
        Assert.Contains("ERP stored 1, not stored 1, lookup failed 1", text, StringComparison.Ordinal);
        Assert.Equal(before, await SnapshotAsync());
    }

    [Fact]
    public async Task Check_batches_with_nothing_unacknowledged_succeeds_without_calling_erp()
    {
        var runId = Guid.NewGuid();
        await InsertRunAsync(runId);
        using var output = new StringWriter();

        var exit = await CliRunner.CheckRunBatchesAsync(new LookupErp([]), _store, runId.ToString("D"), output, CancellationToken.None);

        Assert.Equal(0, exit);
        Assert.Contains("no unacknowledged batches", output.ToString(), StringComparison.Ordinal);
        await Assert.ThrowsAsync<ArgumentException>(() => CliRunner.CheckRunBatchesAsync(new LookupErp([]), _store, "not-a-guid", output, CancellationToken.None));
    }

    private static ErpClient Client(HttpMessageHandler handler) =>
        new(new HttpClient(handler) { BaseAddress = new Uri("https://erp.test/api/integration/1c-agents/v1/") },
            Options.Create(new AgentOptions { AgentId = "agent-1", SiteId = "site-1" }));

    private static string Ack(Guid batchId) =>
        $"{{\"batchId\":\"{batchId:D}\",\"status\":\"accepted\",\"rowsAccepted\":7,\"checksumValid\":true,\"acknowledgedAtUtc\":\"2026-09-28T00:00:00+00:00\"}}";

    private static HttpResponseMessage Json(HttpStatusCode status, string body) =>
        new(status) { Content = new StringContent(body, Encoding.UTF8, "application/json") };

    private async Task InsertRunAsync(Guid runId) =>
        await ExecuteAsync($"INSERT INTO etl_runs(run_id,mode,requested_entities_json,status,configuration_version,created_at_utc,updated_at_utc,row_version) VALUES('{runId:D}','bootstrap_full','[\"clients\"]','blocked',1,'2026-09-28T00:00:00.0000000+00:00','2026-09-28T00:00:00.0000000+00:00',1);");

    private async Task<Guid> InsertBatchAsync(Guid runId, string status, string? quarantine, string createdAt)
    {
        var batchId = Guid.NewGuid();
        var code = quarantine is null ? "NULL" : $"'{quarantine}'";
        await ExecuteAsync($"INSERT INTO etl_batches(batch_id,run_id,entity_name,schema_version,file_path,status,row_count,sha256,compressed_size,uncompressed_size,created_at_utc,quarantine_code) VALUES('{batchId:D}','{runId:D}','clients',1,'spool/x.gz','{status}',5,'hash',10,20,'{createdAt}',{code});");
        return batchId;
    }

    private async Task InsertAttemptAsync(Guid batchId, int attemptNo, string outcome) =>
        await ExecuteAsync($"INSERT INTO etl_batch_send_attempts(attempt_id,batch_id,attempt_no,owner_id,admitted_at_utc,finished_at_utc,outcome) VALUES('{Guid.NewGuid():D}','{batchId:D}',{attemptNo},'owner','2026-09-28T00:00:00.0000000+00:00','2026-09-28T00:00:01.0000000+00:00','{outcome}');");

    private async Task<string> SnapshotAsync()
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT group_concat(batch_id || status || COALESCE(quarantine_code,'') || row_version, '|') FROM (SELECT * FROM etl_batches ORDER BY batch_id);";
        return Convert.ToString(await command.ExecuteScalarAsync(CancellationToken.None), System.Globalization.CultureInfo.InvariantCulture) ?? string.Empty;
    }

    private async Task ExecuteAsync(string sql)
    {
        await using var connection = await _factory.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync(CancellationToken.None);
    }

    private sealed class RoutingHandler(Func<HttpRequestMessage, HttpResponseMessage> route) : HttpMessageHandler
    {
        public List<HttpRequestMessage> Requests { get; } = [];

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            Requests.Add(request);
            return Task.FromResult(route(request));
        }
    }

    private sealed class LookupErp(Dictionary<Guid, Func<EtlBatchRemoteStatus>> answers) : IErpClient
    {
        public Task<EtlBatchRemoteStatus> GetBatchStatusAsync(Guid batchId, CancellationToken cancellationToken) => Task.FromResult(answers[batchId]());

        public Task<SessionStartResponse> StartSessionAsync(SessionStartRequest request, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task<LeaseResponse> LeaseCommandAsync(LeaseRequest request, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task AcknowledgeReceivedAsync(Guid commandId, CommandReceivedRequest request, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task AcknowledgeResultAsync(Guid commandId, string resultJson, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task<BatchAcknowledgement> UploadBatchAsync(EtlBatch batch, Stream content, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task CompleteEtlRunAsync(Guid runId, object summary, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task SendHeartbeatAsync(HeartbeatRequest request, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task<RemoteConfigurationResponse?> GetConfigurationAsync(long currentVersion, CancellationToken cancellationToken) => throw new NotSupportedException();
    }
}
