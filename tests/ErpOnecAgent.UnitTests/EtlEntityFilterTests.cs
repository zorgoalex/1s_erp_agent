using System.Net;
using System.Text;
using System.Text.Json;
using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Application.Configuration;
using ErpOnecAgent.Domain.Etl;
using ErpOnecAgent.Infrastructure.OneC;
using Microsoft.Extensions.Options;
using Xunit;

namespace ErpOnecAgent.UnitTests;

/// <summary>Static entity filter (counterparty phones: Тип eq 'Телефон').</summary>
public sealed class EtlEntityFilterTests
{
    private static readonly JsonSerializerOptions Web = new(JsonSerializerDefaults.Web);
    private static readonly EtlEntityDefinition Phones = new("counterparty_phones", "Catalog_Контрагенты_КонтактнаяИнформация", "Ref_Key", null, null,
        ["Ref_Key", "LineNumber", "Тип", "Вид_Key", "Представление"], "manual_only", 200, 0, KeyFields: ["Ref_Key", "LineNumber"], Filter: "Тип eq 'Телефон'");

    [Fact]
    public void A_definition_without_a_filter_serializes_exactly_as_before()
    {
        var plain = new EtlEntityDefinition("items", "Catalog_Items", "Ref_Key", null, "DeletionMark", ["Ref_Key"], "manual_only", 200, 0);

        var json = JsonSerializer.Serialize(plain, Web);

        // No "filter" key: definition JSON and therefore every existing domain fingerprint stay byte-identical.
        Assert.Equal("{\"entityCode\":\"items\",\"oDataPath\":\"Catalog_Items\",\"keyField\":\"Ref_Key\",\"updatedAtField\":null,\"deletedField\":\"DeletionMark\",\"select\":[\"Ref_Key\"],\"syncMode\":\"manual_only\",\"pageSize\":200,\"overlapMinutes\":0,\"schemaVersion\":1,\"oDataVersion\":3,\"enabled\":true,\"keyFields\":null,\"updatedAtEdmType\":\"Edm.DateTimeOffset\"}", json);
    }

    [Fact]
    public void A_filter_is_part_of_the_definition_and_changes_the_fingerprint()
    {
        var json = JsonSerializer.Serialize(Phones, Web);
        var other = JsonSerializer.Serialize(Phones with { Filter = "Тип eq 'Адрес'" }, Web);

        // Web defaults escape non-ASCII and the apostrophe; the value round-trips exactly.
        Assert.Contains("\"filter\":", json, StringComparison.Ordinal);
        Assert.Equal("Тип eq 'Телефон'", JsonDocument.Parse(json).RootElement.GetProperty("filter").GetString());
        var back = JsonSerializer.Deserialize<EtlEntityDefinition>(json, Web)!;
        Assert.Equal(Phones.Filter, back.Filter);
        Assert.Equal(Phones.KeyFields, back.KeyFields);
        Assert.NotEqual(EtlDomainFingerprint.Compute("ns", Phones.EntityCode, json, "bootstrap_full"), EtlDomainFingerprint.Compute("ns", Phones.EntityCode, other, "bootstrap_full"));
    }

    [Fact]
    public void The_retention_flag_is_serialized_only_when_set_and_never_changes_the_domain()
    {
        var sensitive = Phones with { DeleteBatchAfterAck = true };
        var json = JsonSerializer.Serialize(sensitive, Web);
        var plain = JsonSerializer.Serialize(Phones, Web);

        Assert.DoesNotContain("deleteBatchAfterAck", plain, StringComparison.Ordinal);
        Assert.True(JsonDocument.Parse(json).RootElement.GetProperty("deleteBatchAfterAck").GetBoolean());
        Assert.True(JsonSerializer.Deserialize<EtlEntityDefinition>(json, Web)!.DeleteBatchAfterAck);
        // Retention is local file handling, not the data read: toggling it keeps the watermark domain.
        Assert.Equal(EtlDomainFingerprint.Compute("ns", Phones.EntityCode, plain, "bootstrap_full"), EtlDomainFingerprint.Compute("ns", Phones.EntityCode, json, "bootstrap_full"));
    }

    [Fact]
    public async Task The_filter_applies_to_the_data_read_the_count_and_the_key_pass()
    {
        var handler = new Handler();
        var client = new OnecODataClient(new HttpClient(handler) { BaseAddress = new Uri("http://127.0.0.1/test/odata/standard.odata/") },
            new OnecAuthentication(new StaticSecretStore(), Options.Create(new OnecOptions { CredentialSecretName = "test" })));

        await foreach (var _ in client.ReadEntityAsync(Phones, null, new EtlCursor(DateTimeOffset.UtcNow, null), full: true, CancellationToken.None)) { }
        await client.CountAsync(Phones, CancellationToken.None);
        await foreach (var _ in client.ReadKeysAsync(Phones, CancellationToken.None)) { }

        var queries = handler.Requests.Select(static r => Uri.UnescapeDataString(r.RequestUri!.Query)).ToArray();
        Assert.Equal(3, queries.Length);
        Assert.Contains("$filter=(Тип eq 'Телефон')", queries[0], StringComparison.Ordinal);
        Assert.Equal("?$filter=Тип eq 'Телефон'", queries[1]);
        Assert.EndsWith("/$count", Uri.UnescapeDataString(handler.Requests[1].RequestUri!.AbsolutePath), StringComparison.Ordinal);
        Assert.Contains("$filter=Тип eq 'Телефон'", queries[2], StringComparison.Ordinal);
    }

    [Fact]
    public async Task A_dated_entity_keeps_its_date_bounds_next_to_the_parenthesised_filter()
    {
        var handler = new Handler();
        var client = new OnecODataClient(new HttpClient(handler) { BaseAddress = new Uri("http://127.0.0.1/test/odata/standard.odata/") },
            new OnecAuthentication(new StaticSecretStore(), Options.Create(new OnecOptions { CredentialSecretName = "test" })));
        var dated = new EtlEntityDefinition("orders", "Document_Orders", "Ref_Key", "Date", null, ["Ref_Key", "Date"], "incremental", 200, 0, Filter: "Posted eq true or Amount gt 0");

        await foreach (var _ in client.ReadEntityAsync(dated, null, new EtlCursor(new DateTimeOffset(2026, 9, 28, 0, 0, 0, TimeSpan.Zero), null), full: true, CancellationToken.None)) { }

        var query = Uri.UnescapeDataString(handler.Requests.Single().RequestUri!.Query);
        Assert.Contains("$filter=(Posted eq true or Amount gt 0) and Date le ", query, StringComparison.Ordinal);
    }

    private sealed class Handler : HttpMessageHandler
    {
        public List<HttpRequestMessage> Requests { get; } = [];

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            Requests.Add(request);
            var body = request.RequestUri!.AbsolutePath.EndsWith("/$count", StringComparison.Ordinal) ? "0" : "{\"value\":[]}";
            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(body, Encoding.UTF8, "application/json") });
        }
    }

    private sealed class StaticSecretStore : ISecretStore
    {
        public Task<string?> ReadAsync(string name, CancellationToken cancellationToken) => Task.FromResult<string?>("user:password");
        public Task SaveAsync(string name, string value, CancellationToken cancellationToken) => throw new NotSupportedException();
    }
}
