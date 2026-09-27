using System.Net;
using System.Text;
using System.Text.Json;
using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Application.Configuration;
using ErpOnecAgent.Contracts.Erp;
using ErpOnecAgent.Domain.Etl;
using ErpOnecAgent.Infrastructure.ErpApi;
using ErpOnecAgent.Service.Runtime;
using Microsoft.Extensions.Options;
using Xunit;

namespace ErpOnecAgent.IntegrationTests;

/// <summary>E3: source labels on the wire — batch headers, session/start identity and capability.</summary>
public sealed class E3SourceLabelsWireTests
{
    private static readonly JsonSerializerOptions WebJson = new(JsonSerializerDefaults.Web);
    private const string Namespace = "1c-identity:v1:11111111-1111-1111-1111-111111111111:22222222-2222-2222-2222-222222222222:test";

    [Theory]
    [InlineData(Namespace, "gen-1")]
    [InlineData(Namespace, null)]
    [InlineData(null, null)]
    public async Task Batch_headers_are_sent_only_when_the_label_is_set(string? sourceNamespace, string? generation)
    {
        var handler = new CapturingHandler("{\"batchId\":\"00000000-0000-0000-0000-000000000001\",\"status\":\"accepted\",\"rowsAccepted\":1,\"checksumValid\":true,\"acknowledgedAtUtc\":\"2026-09-28T00:00:00+00:00\"}");
        var client = Client(handler);
        var batch = new EtlBatch(Guid.NewGuid(), Guid.NewGuid(), "clients", 1, "spool/x.gz", EtlBatchStatus.Ready, 1, null, null, "hash", 10, 20, 0, DateTimeOffset.UtcNow);

        await client.UploadBatchWithEvidenceAsync(batch, new EtlBatchSourceLabels(sourceNamespace, generation), new MemoryStream([1]), CancellationToken.None);

        var request = Assert.Single(handler.Requests);
        Assert.Equal(sourceNamespace, Header(request, "X-Source-Namespace"));
        Assert.Equal(generation, Header(request, "X-Source-Generation"));
    }

    [Fact]
    public async Task The_three_argument_upload_sends_no_labels()
    {
        var handler = new CapturingHandler("{\"batchId\":\"00000000-0000-0000-0000-000000000001\",\"status\":\"accepted\",\"rowsAccepted\":1,\"checksumValid\":true,\"acknowledgedAtUtc\":\"2026-09-28T00:00:00+00:00\"}");
        var batch = new EtlBatch(Guid.NewGuid(), Guid.NewGuid(), "clients", 1, "spool/x.gz", EtlBatchStatus.Ready, 1, null, null, "hash", 10, 20, 0, DateTimeOffset.UtcNow);

        await Client(handler).UploadBatchWithEvidenceAsync(batch, new MemoryStream([1]), CancellationToken.None);

        var request = Assert.Single(handler.Requests);
        Assert.Null(Header(request, "X-Source-Namespace"));
        Assert.Null(Header(request, "X-Source-Generation"));
    }

    [Fact]
    public async Task The_default_interface_overload_reaches_a_three_argument_test_double()
    {
        IErpClient erp = new ThreeArgumentDouble();

        var response = await erp.UploadBatchWithEvidenceAsync(null!, new EtlBatchSourceLabels(Namespace, "g"), Stream.Null, CancellationToken.None);

        Assert.Equal(299, response.HttpStatus);
    }

    [Fact]
    public async Task Session_start_carries_the_bound_identity_and_the_source_labels_capability()
    {
        var erp = new SessionErp();
        var onec = Options.Create(new OnecOptions
        {
            ODataBaseUrl = "http://127.0.0.1/test/odata/standard.odata",
            SourceBinding = new OnecSourceBindingOptions
            {
                DatabaseId = "11111111-1111-1111-1111-111111111111",
                ExportEpoch = "22222222-2222-2222-2222-222222222222",
                Environment = "test",
                ODataEndpoint = "http://127.0.0.1/test/odata/standard.odata"
            }
        });
        using var sessions = new ErpSessionManager(erp, AgentOptions(), new AgentRuntimeState(), null, onec);

        await sessions.GetSessionAsync(CancellationToken.None);

        var request = Assert.IsType<SessionStartRequest>(erp.LastRequest);
        Assert.Equal(new SourceIdentity("11111111-1111-1111-1111-111111111111", "22222222-2222-2222-2222-222222222222", "test"), request.SourceIdentity);
        Assert.Contains("etl.source-labels.v1", request.Capabilities);
        using var json = JsonDocument.Parse(JsonSerializer.Serialize(request, WebJson));
        Assert.Equal("test", json.RootElement.GetProperty("sourceIdentity").GetProperty("environment").GetString());
    }

    [Fact]
    public async Task Session_start_without_a_binding_sends_a_null_identity()
    {
        var erp = new SessionErp();
        using var sessions = new ErpSessionManager(erp, AgentOptions(), new AgentRuntimeState());

        await sessions.GetSessionAsync(CancellationToken.None);

        Assert.Null(erp.LastRequest!.SourceIdentity);
    }

    private static string? Header(HttpRequestMessage request, string name) =>
        request.Headers.TryGetValues(name, out var values) ? string.Join(",", values) : null;

    private static ErpClient Client(HttpMessageHandler handler) =>
        new(new HttpClient(handler) { BaseAddress = new Uri("https://erp.test/api/integration/1c-agents/v1/") },
            Options.Create(new AgentOptions { AgentId = "agent-1", SiteId = "site-1" }));

    private static IOptions<AgentOptions> AgentOptions() => Options.Create(new AgentOptions
    {
        AgentId = $"e3-{Guid.NewGuid():N}",
        SiteId = "e3-site",
        DataDirectory = Path.GetTempPath(),
        MaxClockDriftSeconds = 30
    });

    private sealed class CapturingHandler(string body) : HttpMessageHandler
    {
        public List<HttpRequestMessage> Requests { get; } = [];

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            Requests.Add(request);
            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(body, Encoding.UTF8, "application/json") });
        }
    }

    private sealed class ThreeArgumentDouble : IErpClient
    {
        public Task<BatchUploadResponse> UploadBatchWithEvidenceAsync(EtlBatch batch, Stream content, CancellationToken cancellationToken) =>
            Task.FromResult(new BatchUploadResponse(new BatchAcknowledgement(Guid.Empty, "accepted", 0, true, DateTimeOffset.UtcNow), [], 299));

        public Task<SessionStartResponse> StartSessionAsync(SessionStartRequest request, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task<LeaseResponse> LeaseCommandAsync(LeaseRequest request, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task AcknowledgeReceivedAsync(Guid commandId, CommandReceivedRequest request, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task AcknowledgeResultAsync(Guid commandId, string resultJson, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task<BatchAcknowledgement> UploadBatchAsync(EtlBatch batch, Stream content, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task CompleteEtlRunAsync(Guid runId, object summary, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task SendHeartbeatAsync(HeartbeatRequest request, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task<RemoteConfigurationResponse?> GetConfigurationAsync(long currentVersion, CancellationToken cancellationToken) => throw new NotSupportedException();
    }

    private sealed class SessionErp : IErpClient
    {
        public SessionStartRequest? LastRequest { get; private set; }

        public Task<SessionStartResponse> StartSessionAsync(SessionStartRequest request, CancellationToken cancellationToken)
        {
            LastRequest = request;
            return Task.FromResult(new SessionStartResponse(Guid.NewGuid(), DateTimeOffset.UtcNow, true, "1.0.0", 0, false));
        }

        public Task<LeaseResponse> LeaseCommandAsync(LeaseRequest request, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task AcknowledgeReceivedAsync(Guid commandId, CommandReceivedRequest request, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task AcknowledgeResultAsync(Guid commandId, string resultJson, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task<BatchAcknowledgement> UploadBatchAsync(EtlBatch batch, Stream content, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task CompleteEtlRunAsync(Guid runId, object summary, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task SendHeartbeatAsync(HeartbeatRequest request, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task<RemoteConfigurationResponse?> GetConfigurationAsync(long currentVersion, CancellationToken cancellationToken) => throw new NotSupportedException();
    }
}
