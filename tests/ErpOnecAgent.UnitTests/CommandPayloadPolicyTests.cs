using System.Text;
using System.Text.Json;
using ErpOnecAgent.Application.Commands;
using ErpOnecAgent.Domain.Commands;
using ErpOnecAgent.Domain.Common;
using Xunit;

namespace ErpOnecAgent.UnitTests;

/// <summary>
/// Command payload limits agreed with ERP (agent-bridge to-onec/0002, to-erp/0002) and aligned
/// with the 1C extension's BSL parser, which judges the WHOLE request body (envelope root at
/// depth 0, payload at depth 1; every value is a node; the envelope costs 7 nodes):
/// canonical payload ≤ 60 KiB, payload depth ≤ 31 (payload root = 0), payload nodes ≤ 4089,
/// ≤ 128 properties per object, no duplicate / case-duplicate names, no unpaired surrogates.
/// </summary>
public sealed class CommandPayloadPolicyTests
{
    private const int Limit = 61_440;

    [Theory]
    [InlineData("{\"a\":1,\"a\":2}")]
    [InlineData("{\"A\":1,\"a\":2}")]
    [InlineData("{\"x\":[{\"A\":1,\"a\":2}]}")]
    public void Ambiguous_property_names_are_invalid(string json) =>
        Assert.Equal("INVALID_PAYLOAD", Validate(json).ErrorCode);

    [Theory]
    [InlineData("{\"s\":\"\\ud800\"}")]
    [InlineData("{\"s\":\"\\udc00\"}")]
    [InlineData("{\"\\ud800\":1}")]
    public void Unpaired_surrogates_are_invalid(string json) =>
        Assert.Equal("INVALID_PAYLOAD", Validate(json).ErrorCode);

    [Fact]
    public void Depth_31_is_accepted_and_32_is_refused()
    {
        Assert.True(Validate(Nested(31)).IsValid);
        Assert.Equal("INVALID_PAYLOAD", Validate(Nested(32)).ErrorCode);
    }

    [Fact]
    public void Node_count_4089_is_accepted_and_4090_is_refused()
    {
        // An array with N-1 elements is N nodes.
        Assert.True(Validate(ArrayOfNodes(4089)).IsValid);
        Assert.Equal("INVALID_PAYLOAD", Validate(ArrayOfNodes(4090)).ErrorCode);
    }

    [Fact]
    public void Properties_128_are_accepted_and_129_are_refused()
    {
        Assert.True(Validate(ObjectWithProperties(128)).IsValid);
        Assert.Equal("INVALID_PAYLOAD", Validate(ObjectWithProperties(129)).ErrorCode);
    }

    [Fact]
    public void The_size_limit_is_judged_on_canonical_bytes_not_on_the_raw_text()
    {
        // 12 000 Cyrillic characters: about 24 KB of raw UTF-8, but 72 KB canonical (\uXXXX).
        var json = "{\"s\":\"" + new string('ж', 12_000) + "\"}";
        Assert.True(Encoding.UTF8.GetByteCount(json) < Limit);

        Assert.Equal("PAYLOAD_TOO_LARGE", Validate(json).ErrorCode);
    }

    [Fact]
    public void A_payload_at_the_canonical_limit_is_accepted()
    {
        var filler = new string('a', Limit - "{\"s\":\"\"}".Length);
        Assert.True(Validate("{\"s\":\"" + filler + "\"}").IsValid);
        Assert.Equal("PAYLOAD_TOO_LARGE", Validate("{\"s\":\"" + filler + "a\"}").ErrorCode);
    }

    [Fact]
    public void A_limit_violation_is_reported_before_a_hash_mismatch()
    {
        using var document = JsonDocument.Parse("{\"a\":1,\"A\":2}");
        var command = new CommandEnvelope(Guid.NewGuid(), "create_customer_order", 1, 100, null, null, DateTimeOffset.UtcNow, null, null, null, "wrong", document.RootElement.Clone());

        Assert.Equal("INVALID_PAYLOAD", CommandValidator.Validate(command, ["create_customer_order"], Limit).ErrorCode);
    }

    [Fact]
    public void An_older_one_megabyte_setting_is_clamped_to_the_agreed_limit()
    {
        var filler = new string('a', Limit);
        using var document = JsonDocument.Parse("{\"s\":\"" + filler + "\"}");
        var payload = document.RootElement.Clone();
        var command = new CommandEnvelope(Guid.NewGuid(), "create_customer_order", 1, 100, null, null, DateTimeOffset.UtcNow, null, null, null, PayloadHasher.Compute(payload), payload);

        Assert.Equal("PAYLOAD_TOO_LARGE", CommandValidator.Validate(command, ["create_customer_order"], 1_048_576).ErrorCode);
    }

    [Theory]
    [InlineData("{}")]
    [InlineData("{ \"z\": 1, \"a\": 2 }")]
    [InlineData("{\"текст\":\"Привет <tag>&'+`\"}")]
    [InlineData("[-0,0,0.0,-0.00,1E+02,1e-02,123456789012345678901234567890]")]
    [InlineData("{\"s\":\"\\ud83d\\ude00é中\\u2028\"}")]
    public void Canonical_bytes_hash_to_the_payload_hasher_result(string json)
    {
        using var document = JsonDocument.Parse(json);
        Assert.Equal(PayloadHasher.Compute(document.RootElement),
            Convert.ToBase64String(System.Security.Cryptography.SHA256.HashData(CommandPayloadPolicy.CanonicalBytes(document.RootElement))));
    }

    private static ValidationResult Validate(string json)
    {
        using var document = JsonDocument.Parse(json);
        var payload = document.RootElement.Clone();
        string hash;
        try { hash = PayloadHasher.Compute(payload); }
        catch (InvalidOperationException) { hash = "x"; }
        var command = new CommandEnvelope(Guid.NewGuid(), "create_customer_order", 1, 100, null, null, DateTimeOffset.UtcNow, null, null, null, hash, payload);
        return CommandValidator.Validate(command, ["create_customer_order"], Limit);
    }

    private static string Nested(int depth) =>
        string.Concat(Enumerable.Repeat("[", depth)) + "0" + string.Concat(Enumerable.Repeat("]", depth));

    private static string ArrayOfNodes(int nodes) => "[" + string.Join(',', Enumerable.Repeat("0", nodes - 1)) + "]";

    private static string ObjectWithProperties(int count) =>
        "{" + string.Join(',', Enumerable.Range(0, count).Select(i => $"\"p{i:000}\":0")) + "}";
}
