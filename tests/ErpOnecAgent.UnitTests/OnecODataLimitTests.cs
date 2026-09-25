using System.Net;
using System.Text;
using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Application.Configuration;
using ErpOnecAgent.Domain.Etl;
using ErpOnecAgent.Infrastructure.OneC;
using Microsoft.Extensions.Options;
using Xunit;

namespace ErpOnecAgent.UnitTests;

/// <summary>A10: OData page and record size bounds.</summary>
public sealed class OnecODataLimitTests
{
    private static readonly EtlEntityDefinition Entity =
        new("clients", "Catalog_Clients", "Ref_Key", "UpdatedAt", "DeletionMark", ["Description"], "incremental", 500, 10, ODataVersion: 4);
    private static readonly EtlCursor Upper = new(DateTimeOffset.Parse("2026-09-26T00:00:00Z", System.Globalization.CultureInfo.InvariantCulture), null);

    [Fact]
    public async Task A_page_within_the_limits_is_read()
    {
        var client = Client(Page(3, 10), pageBytes: 1024 * 1024, rowBytes: 64 * 1024);

        var rows = await ReadAllAsync(client);

        Assert.Equal(3, rows.Count);
    }

    [Fact]
    public async Task A_declared_oversized_page_is_refused_before_reading()
    {
        var body = Page(2000, 1000);
        var client = Client(body, pageBytes: 1024 * 1024, rowBytes: 64 * 1024);

        await Assert.ThrowsAsync<InvalidDataException>(() => ReadAllAsync(client));
    }

    [Fact]
    public async Task A_streamed_page_without_length_is_cut_at_the_limit()
    {
        var body = Page(2000, 1000);
        var client = Client(body, pageBytes: 1024 * 1024, rowBytes: 64 * 1024, withLength: false);

        await Assert.ThrowsAsync<InvalidDataException>(() => ReadAllAsync(client));
    }

    [Fact]
    public async Task An_oversized_record_is_refused()
    {
        var client = Client(Page(1, 200 * 1024), pageBytes: 8 * 1024 * 1024, rowBytes: 64 * 1024);

        var ex = await Assert.ThrowsAsync<InvalidDataException>(() => ReadAllAsync(client));

        Assert.Contains("row limit", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task The_row_limit_counts_utf8_bytes_not_characters()
    {
        // 40,000 Cyrillic characters: ~40 KB of chars but ~80 KB of UTF-8 — over a 64 KB limit.
        var body = "{\"value\":[{\"Ref_Key\":\"" + Guid.NewGuid() + "\",\"UpdatedAt\":\"2026-09-25T00:00:00Z\",\"DeletionMark\":false,\"Description\":\"" + new string('ж', 40_000) + "\"}]}";
        var client = Client(body, pageBytes: 8 * 1024 * 1024, rowBytes: 64 * 1024);

        var ex = await Assert.ThrowsAsync<InvalidDataException>(() => ReadAllAsync(client));

        Assert.Contains("row limit", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Defaults_apply_without_options()
    {
        var http = new HttpClient(new FixedHandler(Page(1, 10), true)) { BaseAddress = new Uri("http://127.0.0.1:5099/odata/") };
        var client = new OnecODataClient(http, Auth());

        Assert.Single(await ReadAllAsync(client));
    }

    private static async Task<List<System.Text.Json.JsonElement>> ReadAllAsync(OnecODataClient client)
    {
        var rows = new List<System.Text.Json.JsonElement>();
        await foreach (var row in client.ReadEntityAsync(Entity, null, Upper, full: true, CancellationToken.None)) rows.Add(row);
        return rows;
    }

    private static string Page(int rows, int descriptionLength)
    {
        var items = Enumerable.Range(0, rows).Select(i =>
            $$"""{"Ref_Key":"{{Guid.NewGuid()}}","UpdatedAt":"2026-09-25T00:00:00Z","DeletionMark":false,"Description":"{{new string('d', descriptionLength)}}"}""");
        return "{\"value\":[" + string.Join(',', items) + "]}";
    }

    private static OnecODataClient Client(string body, long pageBytes, int rowBytes, bool withLength = true)
    {
        var http = new HttpClient(new FixedHandler(body, withLength)) { BaseAddress = new Uri("http://127.0.0.1:5099/odata/") };
        return new OnecODataClient(http, Auth(), Options.Create(new EtlOptions { MaxODataPageBytes = pageBytes, MaxODataRowBytes = rowBytes }));
    }

    private static OnecAuthentication Auth() =>
        new(new StaticSecretStore(), Options.Create(new OnecOptions { CredentialSecretName = "test" }));

    private sealed class FixedHandler(string body, bool withLength) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var bytes = Encoding.UTF8.GetBytes(body);
            HttpContent content = withLength ? new ByteArrayContent(bytes) : new UnknownLengthContent(bytes);
            content.Headers.ContentType = new("application/json");
            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK) { Content = content });
        }
    }

    private sealed class UnknownLengthContent(byte[] bytes) : HttpContent
    {
        protected override Task SerializeToStreamAsync(Stream stream, TransportContext? context) => stream.WriteAsync(bytes, 0, bytes.Length);
        protected override bool TryComputeLength(out long length) { length = -1; return false; }
    }

    private sealed class StaticSecretStore : ISecretStore
    {
        public Task SaveAsync(string name, string secret, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task<string?> ReadAsync(string name, CancellationToken cancellationToken) => Task.FromResult<string?>("user:password");
    }
}
