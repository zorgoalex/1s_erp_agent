using System.Net;
using System.Text;
using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Application.Configuration;
using ErpOnecAgent.Domain.Etl;
using ErpOnecAgent.Infrastructure.OneC;
using Microsoft.Extensions.Options;
using Xunit;

namespace ErpOnecAgent.UnitTests;

public sealed class OnecODataClientTests
{
    [Fact]
    public async Task V3_pagination_falls_back_to_skip_and_selects_cursor_fields()
    {
        var handler = new SequenceHandler(
            ("{\"d\":{\"results\":[{\"Ref_Key\":\"A\",\"UpdatedAt\":\"2026-08-30T10:00:00Z\",\"DeletionMark\":false},{\"Ref_Key\":\"B\",\"UpdatedAt\":\"2026-08-30T10:01:00Z\",\"DeletionMark\":false}]}}", "application/json"),
            ("{\"d\":{\"results\":[{\"Ref_Key\":\"C\",\"UpdatedAt\":\"2026-08-30T10:02:00Z\",\"DeletionMark\":false}]}}", "application/json"));
        var client = CreateClient(handler);
        var entity = new EtlEntityDefinition("clients", "Catalog_Clients", "Ref_Key", "UpdatedAt", "DeletionMark", ["Description"], "incremental", 2, 10, ODataVersion: 3);
        var rows = new List<string>();

        await foreach (var row in client.ReadEntityAsync(entity, new(DateTimeOffset.Parse("2026-08-30T09:00:00Z", System.Globalization.CultureInfo.InvariantCulture), "A"), new(DateTimeOffset.Parse("2026-08-30T11:00:00Z", System.Globalization.CultureInfo.InvariantCulture), null), false, CancellationToken.None))
            rows.Add(row.GetProperty("Ref_Key").GetString()!);

        Assert.Equal(["A", "B", "C"], rows);
        Assert.Equal(2, handler.Requests.Count);
        var first = Uri.UnescapeDataString(handler.Requests[0].Query);
        var second = Uri.UnescapeDataString(handler.Requests[1].Query);
        Assert.Contains("Ref_Key", first, StringComparison.Ordinal);
        Assert.Contains("UpdatedAt", first, StringComparison.Ordinal);
        Assert.Contains("DeletionMark", first, StringComparison.Ordinal);
        Assert.Contains("datetimeoffset'", first, StringComparison.Ordinal);
        Assert.Contains("$skip=2", second, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Metadata_probe_uses_authenticated_onec_endpoint()
    {
        var handler = new SequenceHandler(("<metadata />", "application/xml"));
        var client = CreateClient(handler);

        Assert.True(await client.CheckAsync(CancellationToken.None));
        Assert.Equal("/odata/$metadata", handler.Requests.Single().AbsolutePath);
        Assert.NotNull(handler.Authorization);
    }

    [Fact]
    public async Task V3_datetime_and_composite_key_match_flat_1c_register_entity()
    {
        var handler = new SequenceHandler(
            ("{\"value\":[{\"Recorder\":\"R1\",\"LineNumber\":1,\"Recorder_Type\":\"Document_Order\",\"Period\":\"2026-08-30T10:00:00\"}]}", "application/json"));
        var client = CreateClient(handler);
        var entity = new EtlEntityDefinition(
            "order_movements",
            "AccumulationRegister_Orders_RecordType",
            "Recorder",
            "Period",
            null,
            ["RecordType"],
            "incremental",
            100,
            10,
            ODataVersion: 3,
            KeyFields: ["Recorder", "LineNumber", "Recorder_Type"],
            UpdatedAtEdmType: "Edm.DateTime");

        var rows = new List<string>();
        await foreach (var row in client.ReadEntityAsync(entity, new(DateTimeOffset.Parse("2026-08-30T09:00:00Z", System.Globalization.CultureInfo.InvariantCulture), null), new(DateTimeOffset.Parse("2026-08-30T11:00:00Z", System.Globalization.CultureInfo.InvariantCulture), null), false, CancellationToken.None))
            rows.Add(entity.SourceIdFrom(row));

        Assert.Equal(["[\"R1\",1,\"Document_Order\"]"], rows);
        var query = Uri.UnescapeDataString(handler.Requests.Single().Query);
        Assert.Contains("$orderby=Period,Recorder,LineNumber,Recorder_Type", query, StringComparison.Ordinal);
        Assert.Contains("datetime'2026-08-30T", query, StringComparison.Ordinal);
        Assert.DoesNotContain("datetimeoffset'", query, StringComparison.Ordinal);
        Assert.Contains("LineNumber", query, StringComparison.Ordinal);
        Assert.Contains("Recorder_Type", query, StringComparison.Ordinal);
    }

    [Fact]
    public void Manual_only_entity_is_not_scheduled()
    {
        var entity = new EtlEntityDefinition("clients", "Catalog_Clients", "Ref_Key", null, "DeletionMark", ["Description"], "manual_only", 500, 10);

        Assert.False(entity.RunsOnSchedule());
    }

    [Fact]
    public async Task Continuation_terminal_full_page_stops_without_skip_fallback()
    {
        var handler = new SequenceHandler(
            ("{\"d\":{\"results\":[{\"Ref_Key\":\"A\"},{\"Ref_Key\":\"B\"}]},\"__next\":\"Catalog_Clients?$skiptoken=page2\"}", "application/json"),
            ("{\"d\":{\"results\":[{\"Ref_Key\":\"C\"},{\"Ref_Key\":\"D\"}]}}", "application/json"),
            ("{\"d\":{\"results\":[{\"Ref_Key\":\"C\"},{\"Ref_Key\":\"D\"}]}}", "application/json"),
            ("{\"d\":{\"results\":[{\"Ref_Key\":\"C\"},{\"Ref_Key\":\"D\"}]}}", "application/json"),
            ("{\"d\":{\"results\":[]}}", "application/json"));
        var client = CreateClient(handler);
        var entity = CreateEntity(pageSize: 2, odataVersion: 3);
        var rows = new List<string>();

        await foreach (var row in client.ReadEntityAsync(entity, null, UpperBound(), full: true, CancellationToken.None))
            rows.Add(row.GetProperty("Ref_Key").GetString()!);

        Assert.Equal(["A", "B", "C", "D"], rows);
        Assert.Equal(2, handler.Requests.Count);
        Assert.All(handler.Requests, request => Assert.Equal("127.0.0.1", request.Host));
    }

    [Fact]
    public async Task Continuation_terminal_empty_page_stops()
    {
        var handler = new SequenceHandler(
            ("{\"value\":[{\"Ref_Key\":\"A\"},{\"Ref_Key\":\"B\"}],\"@odata.nextLink\":\"Catalog_Clients?$skiptoken=page2\"}", "application/json"),
            ("{\"value\":[]}", "application/json"),
            ("{\"value\":[{\"Ref_Key\":\"Z\"}]}", "application/json"));
        var client = CreateClient(handler);
        var entity = CreateEntity(pageSize: 2, odataVersion: 4);
        var rows = new List<string>();

        await foreach (var row in client.ReadEntityAsync(entity, null, UpperBound(), full: true, CancellationToken.None))
            rows.Add(row.GetProperty("Ref_Key").GetString()!);

        Assert.Equal(["A", "B"], rows);
        Assert.Equal(2, handler.Requests.Count);
    }

    [Fact]
    public async Task Skip_pagination_multi_page_without_next_link()
    {
        var handler = new SequenceHandler(
            ("{\"d\":{\"results\":[{\"Ref_Key\":\"A\"},{\"Ref_Key\":\"B\"}]}}", "application/json"),
            ("{\"d\":{\"results\":[{\"Ref_Key\":\"C\"},{\"Ref_Key\":\"D\"}]}}", "application/json"),
            ("{\"d\":{\"results\":[{\"Ref_Key\":\"E\"}]}}", "application/json"));
        var client = CreateClient(handler);
        var entity = CreateEntity(pageSize: 2, odataVersion: 3);
        var rows = new List<string>();

        await foreach (var row in client.ReadEntityAsync(entity, null, UpperBound(), full: true, CancellationToken.None))
            rows.Add(row.GetProperty("Ref_Key").GetString()!);

        Assert.Equal(["A", "B", "C", "D", "E"], rows);
        Assert.Equal(3, handler.Requests.Count);
        Assert.Contains("$skip=0", Uri.UnescapeDataString(handler.Requests[0].Query), StringComparison.Ordinal);
        Assert.Contains("$skip=2", Uri.UnescapeDataString(handler.Requests[1].Query), StringComparison.Ordinal);
        Assert.Contains("$skip=4", Uri.UnescapeDataString(handler.Requests[2].Query), StringComparison.Ordinal);
    }

    [Fact]
    public async Task Valid_relative_next_link_is_followed_inside_service_boundary()
    {
        var handler = new SequenceHandler(
            ("{\"value\":[{\"Ref_Key\":\"A\"},{\"Ref_Key\":\"B\"}],\"@odata.nextLink\":\"Catalog_Clients?$skiptoken=abc\"}", "application/json"),
            ("{\"value\":[{\"Ref_Key\":\"C\"}]}", "application/json"));
        var client = CreateClient(handler);
        var entity = CreateEntity(pageSize: 2, odataVersion: 4);
        var rows = new List<string>();

        await foreach (var row in client.ReadEntityAsync(entity, null, UpperBound(), full: true, CancellationToken.None))
            rows.Add(row.GetProperty("Ref_Key").GetString()!);

        Assert.Equal(["A", "B", "C"], rows);
        Assert.Equal(2, handler.Requests.Count);
        Assert.Equal("/odata/Catalog_Clients", handler.Requests[1].AbsolutePath);
        Assert.Contains("skiptoken=abc", handler.Requests[1].Query, StringComparison.Ordinal);
        Assert.All(handler.Requests, request => Assert.Equal("127.0.0.1", request.Host));
    }

    [Fact]
    public async Task Query_only_next_link_retains_entity_path()
    {
        var handler = new SequenceHandler(
            ("{\"value\":[{\"Ref_Key\":\"A\"},{\"Ref_Key\":\"B\"}],\"@odata.nextLink\":\"?$skiptoken=page2\"}", "application/json"),
            ("{\"value\":[{\"Ref_Key\":\"C\"}]}", "application/json"));
        var client = CreateClient(handler);
        var entity = CreateEntity(pageSize: 2, odataVersion: 4);
        var rows = new List<string>();

        await foreach (var row in client.ReadEntityAsync(entity, null, UpperBound(), full: true, CancellationToken.None))
            rows.Add(row.GetProperty("Ref_Key").GetString()!);

        Assert.Equal(["A", "B", "C"], rows);
        Assert.Equal(2, handler.Requests.Count);
        Assert.Equal("/odata/Catalog_Clients", handler.Requests[1].AbsolutePath);
        Assert.Equal("?$skiptoken=page2", handler.Requests[1].Query);
    }

    [Fact]
    public async Task Nested_v3_d_next_is_followed()
    {
        var handler = new SequenceHandler(
            ("{\"d\":{\"results\":[{\"Ref_Key\":\"A\"},{\"Ref_Key\":\"B\"}],\"__next\":\"Catalog_Clients?$skiptoken=page2\"}}", "application/json"),
            ("{\"d\":{\"results\":[{\"Ref_Key\":\"C\"}]}}", "application/json"));
        var client = CreateClient(handler);
        var entity = CreateEntity(pageSize: 2, odataVersion: 3);
        var rows = new List<string>();

        await foreach (var row in client.ReadEntityAsync(entity, null, UpperBound(), full: true, CancellationToken.None))
            rows.Add(row.GetProperty("Ref_Key").GetString()!);

        Assert.Equal(["A", "B", "C"], rows);
        Assert.Equal(2, handler.Requests.Count);
        Assert.Equal("/odata/Catalog_Clients", handler.Requests[1].AbsolutePath);
        Assert.Contains("skiptoken=page2", handler.Requests[1].Query, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Valid_absolute_same_origin_next_link_is_followed()
    {
        var handler = new SequenceHandler(
            ("{\"value\":[{\"Ref_Key\":\"A\"},{\"Ref_Key\":\"B\"}],\"@odata.nextLink\":\"http://127.0.0.1:5099/odata/Catalog_Clients?$skiptoken=abc\"}", "application/json"),
            ("{\"value\":[{\"Ref_Key\":\"C\"}]}", "application/json"));
        var client = CreateClient(handler);
        var entity = CreateEntity(pageSize: 2, odataVersion: 4);
        var rows = new List<string>();

        await foreach (var row in client.ReadEntityAsync(entity, null, UpperBound(), full: true, CancellationToken.None))
            rows.Add(row.GetProperty("Ref_Key").GetString()!);

        Assert.Equal(["A", "B", "C"], rows);
        Assert.Equal(2, handler.Requests.Count);
        Assert.Equal("/odata/Catalog_Clients", handler.Requests[1].AbsolutePath);
        Assert.Contains("skiptoken=abc", handler.Requests[1].Query, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Next_link_with_odata_path_prefix_confusion_is_rejected()
    {
        var handler = new SequenceHandler(
            ("{\"value\":[{\"Ref_Key\":\"A\"}],\"@odata.nextLink\":\"/odata-other/steal\"}", "application/json"),
            ("{\"value\":[{\"Ref_Key\":\"B\"}]}", "application/json"));
        var client = CreateClient(handler);
        var entity = CreateEntity(pageSize: 2, odataVersion: 4);

        var exception = await Assert.ThrowsAsync<InvalidDataException>(async () =>
        {
            await foreach (var _ in client.ReadEntityAsync(entity, null, UpperBound(), full: true, CancellationToken.None)) { }
        });

        Assert.Contains("boundary", exception.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Single(handler.Requests);
        Assert.Equal("/odata/Catalog_Clients", handler.Requests[0].AbsolutePath);
    }

    [Fact]
    public async Task Malicious_absolute_next_link_is_rejected_before_credentials()
    {
        var handler = new SequenceHandler(
            ("{\"value\":[{\"Ref_Key\":\"A\"}],\"@odata.nextLink\":\"http://evil.example/steal\"}", "application/json"),
            ("{\"value\":[{\"Ref_Key\":\"B\"}]}", "application/json"));
        var client = CreateClient(handler);
        var entity = CreateEntity(pageSize: 2, odataVersion: 4);

        var exception = await Assert.ThrowsAsync<InvalidDataException>(async () =>
        {
            await foreach (var _ in client.ReadEntityAsync(entity, null, UpperBound(), full: true, CancellationToken.None)) { }
        });

        Assert.Contains("outside", exception.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Single(handler.Requests);
        Assert.Equal("127.0.0.1", handler.Requests[0].Host);
        Assert.DoesNotContain(handler.Requests, request => string.Equals(request.Host, "evil.example", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public async Task Malicious_protocol_relative_next_link_is_rejected_before_credentials()
    {
        var handler = new SequenceHandler(
            ("{\"value\":[{\"Ref_Key\":\"A\"}],\"@odata.nextLink\":\"//evil.example/steal\"}", "application/json"),
            ("{\"value\":[{\"Ref_Key\":\"B\"}]}", "application/json"));
        var client = CreateClient(handler);
        var entity = CreateEntity(pageSize: 2, odataVersion: 4);

        var exception = await Assert.ThrowsAsync<InvalidDataException>(async () =>
        {
            await foreach (var _ in client.ReadEntityAsync(entity, null, UpperBound(), full: true, CancellationToken.None)) { }
        });

        Assert.Contains("outside", exception.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Single(handler.Requests);
        Assert.Equal("127.0.0.1", handler.Requests[0].Host);
        Assert.DoesNotContain(handler.Requests, request => string.Equals(request.Host, "evil.example", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public async Task Next_link_outside_odata_service_boundary_is_rejected_before_credentials()
    {
        var handler = new SequenceHandler(
            ("{\"value\":[{\"Ref_Key\":\"A\"}],\"@odata.nextLink\":\"http://127.0.0.1:5099/other/steal\"}", "application/json"),
            ("{\"value\":[{\"Ref_Key\":\"B\"}]}", "application/json"));
        var client = CreateClient(handler);
        var entity = CreateEntity(pageSize: 2, odataVersion: 4);

        var exception = await Assert.ThrowsAsync<InvalidDataException>(async () =>
        {
            await foreach (var _ in client.ReadEntityAsync(entity, null, UpperBound(), full: true, CancellationToken.None)) { }
        });

        Assert.Contains("boundary", exception.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Single(handler.Requests);
        Assert.Equal("/odata/Catalog_Clients", handler.Requests[0].AbsolutePath);
    }

    [Fact]
    public async Task Relative_next_link_path_escape_is_rejected_before_credentials()
    {
        var handler = new SequenceHandler(
            ("{\"value\":[{\"Ref_Key\":\"A\"}],\"@odata.nextLink\":\"../steal\"}", "application/json"),
            ("{\"value\":[{\"Ref_Key\":\"B\"}]}", "application/json"));
        var client = CreateClient(handler);
        var entity = CreateEntity(pageSize: 2, odataVersion: 4);

        var exception = await Assert.ThrowsAsync<InvalidDataException>(async () =>
        {
            await foreach (var _ in client.ReadEntityAsync(entity, null, UpperBound(), full: true, CancellationToken.None)) { }
        });

        Assert.Contains("boundary", exception.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Single(handler.Requests);
        Assert.DoesNotContain(handler.Requests, request => request.AbsolutePath.Contains("steal", StringComparison.OrdinalIgnoreCase));
    }

    private static EtlEntityDefinition CreateEntity(int pageSize, int odataVersion) =>
        new("clients", "Catalog_Clients", "Ref_Key", "UpdatedAt", "DeletionMark", ["Description"], "incremental", pageSize, 10, ODataVersion: odataVersion);

    private static EtlCursor UpperBound() =>
        new(DateTimeOffset.Parse("2026-08-30T11:00:00Z", System.Globalization.CultureInfo.InvariantCulture), null);

    private static OnecODataClient CreateClient(SequenceHandler handler)
    {
        var http = new HttpClient(handler) { BaseAddress = new Uri("http://127.0.0.1:5099/odata/") };
        var authentication = new OnecAuthentication(new StaticSecretStore(), Options.Create(new OnecOptions { CredentialSecretName = "test" }));
        return new OnecODataClient(http, authentication);
    }

    private sealed class StaticSecretStore : ISecretStore
    {
        public Task SaveAsync(string name, string secret, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task<string?> ReadAsync(string name, CancellationToken cancellationToken) => Task.FromResult<string?>("user:password");
    }

    private sealed class SequenceHandler : HttpMessageHandler
    {
        private readonly Queue<(string Body, string ContentType)> _responses;
        public List<Uri> Requests { get; } = [];
        public System.Net.Http.Headers.AuthenticationHeaderValue? Authorization { get; private set; }

        public SequenceHandler(params (string Body, string ContentType)[] responses) => _responses = new(responses);

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            Requests.Add(request.RequestUri!);
            Authorization = request.Headers.Authorization;
            var response = _responses.Dequeue();
            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(response.Body, Encoding.UTF8, response.ContentType) });
        }
    }
}
