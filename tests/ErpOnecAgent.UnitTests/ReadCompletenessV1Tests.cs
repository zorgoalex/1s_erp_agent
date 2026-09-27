using System.Net;
using System.Text;
using System.Text.Json;
using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Application.Configuration;
using ErpOnecAgent.Application.Etl;
using ErpOnecAgent.Domain.Etl;
using ErpOnecAgent.Infrastructure.OneC;
using Microsoft.Extensions.Options;
using Xunit;

namespace ErpOnecAgent.UnitTests;

/// <summary>V1: the key-set digest, OData $count and the unfiltered key-only pass.</summary>
public sealed class ReadCompletenessV1Tests
{
    private static readonly EtlEntityDefinition Entity = new("clients", "Catalog_Clients", "Ref_Key", "UpdatedAt", "DeletionMark", ["Ref_Key", "UpdatedAt", "DeletionMark", "Description"], "incremental", 2, 10);
    private static readonly EtlEntityDefinition Composite = new("lines", "Document_Lines", "Ref_Key", null, null, ["Ref_Key", "LineNumber"], "incremental", 2, 10, KeyFields: ["Ref_Key", "LineNumber"]);

    [Fact]
    public void The_digest_ignores_order_and_detects_missing_extra_and_duplicate_keys()
    {
        var abc = Digest(Entity, "A", "B", "C");

        Assert.Equal(abc.Value, Digest(Entity, "C", "A", "B").Value);
        Assert.Equal(3, abc.Count);
        Assert.NotEqual(abc.Value, Digest(Entity, "A", "B").Value);
        Assert.NotEqual(abc.Value, Digest(Entity, "A", "B", "C", "D").Value);
        // Same count, a duplicate instead of C.
        Assert.NotEqual(abc.Value, Digest(Entity, "A", "B", "B").Value);
    }

    [Fact]
    public void The_digest_uses_every_key_field_and_json_types()
    {
        var one = new EtlKeySetDigest();
        one.Add(Row("{\"Ref_Key\":\"A\",\"LineNumber\":1}"), Composite);
        var other = new EtlKeySetDigest();
        other.Add(Row("{\"Ref_Key\":\"A\",\"LineNumber\":2}"), Composite);
        var text = new EtlKeySetDigest();
        text.Add(Row("{\"Ref_Key\":\"A\",\"LineNumber\":\"1\"}"), Composite);

        Assert.NotEqual(one.Value, other.Value);
        Assert.NotEqual(one.Value, text.Value);
        Assert.Throws<InvalidDataException>(() => new EtlKeySetDigest().Add(Row("{\"Ref_Key\":\"A\"}"), Composite));
        Assert.Throws<InvalidDataException>(() => new EtlKeySetDigest().Add(Row("{\"Ref_Key\":null,\"LineNumber\":1}"), Composite));
    }

    [Theory]
    [InlineData("42", 42L)]
    [InlineData("﻿7\r\n", 7L)]
    [InlineData("0", 0L)]
    public async Task Count_reads_the_plain_integer_of_the_whole_set(string body, long expected)
    {
        var handler = new Handler(_ => Text(HttpStatusCode.OK, body));

        Assert.Equal(expected, await Client(handler).CountAsync(Entity, CancellationToken.None));

        var request = Assert.Single(handler.Requests);
        Assert.Equal("/odata/Catalog_Clients/$count", request.RequestUri!.AbsolutePath);
        Assert.Equal(string.Empty, request.RequestUri.Query);
        Assert.NotNull(request.Headers.Authorization);
        // 1C refuses Accept: text/plain on $count with 406.
        Assert.Equal("application/json", string.Join(",", request.Headers.Accept.Select(static a => a.ToString())));
    }

    [Theory]
    [InlineData("-1")]
    [InlineData("12abc")]
    [InlineData("")]
    [InlineData("{\"value\":3}")]
    public async Task A_count_that_is_not_a_non_negative_integer_is_refused(string body)
    {
        await Assert.ThrowsAsync<InvalidDataException>(() => Client(new Handler(_ => Text(HttpStatusCode.OK, body))).CountAsync(Entity, CancellationToken.None));
    }

    [Fact]
    public async Task A_failed_count_is_an_http_failure()
    {
        await Assert.ThrowsAsync<HttpRequestException>(() => Client(new Handler(_ => Text(HttpStatusCode.NotFound, "no"))).CountAsync(Entity, CancellationToken.None));
    }

    [Fact]
    public async Task The_key_pass_selects_only_keys_without_a_filter_and_pages_to_the_end()
    {
        var handler = new Handler(request => request.RequestUri!.Query.Contains("%24skip=0", StringComparison.Ordinal) || request.RequestUri.Query.Contains("$skip=0", StringComparison.Ordinal)
            ? Json("{\"value\":[{\"Ref_Key\":\"A\"},{\"Ref_Key\":\"B\"}]}")
            : Json("{\"value\":[{\"Ref_Key\":\"C\"}]}"));

        var keys = new List<string>();
        await foreach (var row in Client(handler).ReadKeysAsync(Entity, CancellationToken.None)) keys.Add(row.GetProperty("Ref_Key").GetString()!);

        Assert.Equal(["A", "B", "C"], keys);
        Assert.Equal(2, handler.Requests.Count);
        var query = Uri.UnescapeDataString(handler.Requests[0].RequestUri!.Query);
        Assert.Contains("$select=Ref_Key&", query, StringComparison.Ordinal);
        Assert.Contains("$orderby=Ref_Key", query, StringComparison.Ordinal);
        Assert.DoesNotContain("$filter", query, StringComparison.Ordinal);
        Assert.DoesNotContain("UpdatedAt", query, StringComparison.Ordinal);
    }

    [Fact]
    public void Verdicts_are_well_formed()
    {
        Assert.True(EtlReadCompleteness.Verified.IsValid);
        Assert.True(EtlReadCompleteness.NotChecked.IsValid);
        Assert.True(EtlReadCompleteness.Unverified("COUNT_CHANGED").IsValid);
        Assert.False(new EtlReadCompleteness("verified", "X").IsValid);
        Assert.False(new EtlReadCompleteness("unverified", null).IsValid);
        Assert.False(new EtlReadCompleteness("unverified", "lower_case").IsValid);
        Assert.False(new EtlReadCompleteness("maybe", "X").IsValid);
    }

    private static EtlKeySetDigest Digest(EtlEntityDefinition entity, params string[] keys)
    {
        var digest = new EtlKeySetDigest();
        foreach (var key in keys) digest.Add(Row($"{{\"Ref_Key\":\"{key}\"}}"), entity);
        return digest;
    }

    private static JsonElement Row(string json) => JsonDocument.Parse(json).RootElement.Clone();

    private static OnecODataClient Client(HttpMessageHandler handler) =>
        new(new HttpClient(handler) { BaseAddress = new Uri("http://127.0.0.1:5099/odata/") },
            new OnecAuthentication(new StaticSecretStore(), Options.Create(new OnecOptions { CredentialSecretName = "test" })));

    private static HttpResponseMessage Text(HttpStatusCode status, string body) => new(status) { Content = new StringContent(body, Encoding.UTF8, "text/plain") };

    private static HttpResponseMessage Json(string body) => new(HttpStatusCode.OK) { Content = new StringContent(body, Encoding.UTF8, "application/json") };

    private sealed class Handler(Func<HttpRequestMessage, HttpResponseMessage> answer) : HttpMessageHandler
    {
        public List<HttpRequestMessage> Requests { get; } = [];

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            Requests.Add(request);
            return Task.FromResult(answer(request));
        }
    }

    private sealed class StaticSecretStore : ISecretStore
    {
        public Task<string?> ReadAsync(string name, CancellationToken cancellationToken) => Task.FromResult<string?>("user:password");
        public Task SaveAsync(string name, string value, CancellationToken cancellationToken) => throw new NotSupportedException();
    }
}
