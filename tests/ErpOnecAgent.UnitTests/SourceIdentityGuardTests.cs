using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Application.Configuration;
using ErpOnecAgent.Application.Etl;
using ErpOnecAgent.Infrastructure.OneC;
using Microsoft.Extensions.Options;
using Xunit;

namespace ErpOnecAgent.UnitTests;

public sealed class SourceIdentityGuardTests
{
    private static readonly Guid DatabaseId = Guid.Parse("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
    private static readonly Guid ExportEpoch = Guid.Parse("b1eebc99-9c0b-4ef8-bb6d-6bb9bd380a22");
    private const string ODataBaseUrl = "http://srv/base/odata";

    private static OnecSourceBindingOptions BindingOptions(string? databaseId = null) => new()
    {
        DatabaseId = databaseId ?? DatabaseId.ToString("D"),
        ExportEpoch = ExportEpoch.ToString("D"),
        Environment = "test",
        ODataEndpoint = ODataBaseUrl
    };

    private static OnecSourceIdentity MatchingIdentity() =>
        new(1, "ready", "whole-infobase", DatabaseId, ExportEpoch, "test");

    private static SourceIdentityGuard NewGuard(FakeIdentityClient client, OnecSourceBindingOptions? binding) =>
        new(client, Options.Create(new OnecOptions { ODataBaseUrl = ODataBaseUrl, SourceBinding = binding }));

    // G1: a missing or invalid binding is decided without touching the source.
    [Fact]
    public async Task G1_missing_binding_returns_binding_missing_without_client_call()
    {
        var client = new FakeIdentityClient(OnecIdentityFetchResult.Ready(MatchingIdentity()));
        var guard = NewGuard(client, null);

        var check = await guard.CheckAsync(CancellationToken.None);

        Assert.Equal(SourceIdentityVerdict.BindingMissing, check.Verdict);
        Assert.Null(check.SourceNamespace);
        Assert.Equal(0, client.Calls);
    }

    [Fact]
    public async Task G1_invalid_binding_returns_binding_invalid_without_client_call()
    {
        var client = new FakeIdentityClient(OnecIdentityFetchResult.Ready(MatchingIdentity()));
        var guard = NewGuard(client, BindingOptions(databaseId: "not-a-guid"));

        var check = await guard.CheckAsync(CancellationToken.None);

        Assert.Equal(SourceIdentityVerdict.BindingInvalid, check.Verdict);
        Assert.Null(check.SourceNamespace);
        Assert.Equal(0, client.Calls);
    }

    // G2: a valid binding fetches exactly once and the verdict passes through.
    [Fact]
    public async Task G2_valid_binding_calls_client_once_and_passes_match_through()
    {
        var client = new FakeIdentityClient(OnecIdentityFetchResult.Ready(MatchingIdentity()));
        var guard = NewGuard(client, BindingOptions());

        var check = await guard.CheckAsync(CancellationToken.None);

        Assert.Equal(1, client.Calls);
        Assert.Equal(SourceIdentityVerdict.Match, check.Verdict);
        Assert.Equal($"1c-identity:v1:{DatabaseId:D}:{ExportEpoch:D}:test", check.SourceNamespace);
    }

    [Fact]
    public async Task G2_valid_binding_calls_client_once_and_passes_failure_through()
    {
        var client = new FakeIdentityClient(OnecIdentityFetchResult.Unavailable(500, "unexpected status"));
        var guard = NewGuard(client, BindingOptions());

        var check = await guard.CheckAsync(CancellationToken.None);

        Assert.Equal(1, client.Calls);
        Assert.Equal(SourceIdentityVerdict.Unavailable, check.Verdict);
        Assert.Equal("unexpected status", check.Detail);
        Assert.Null(check.SourceNamespace);
    }

    // G3: SameSource is true only when both checks Match the same namespace.
    [Fact]
    public void G3_same_source_requires_two_matches_with_equal_namespace()
    {
        var before = new SourceIdentityCheck(SourceIdentityVerdict.Match, $"1c-identity:v1:{DatabaseId:D}:{ExportEpoch:D}:test", string.Empty);
        var sameNamespace = new SourceIdentityCheck(SourceIdentityVerdict.Match, before.SourceNamespace, string.Empty);
        // exportEpoch rotated between the two checks -> a different namespace.
        var rotatedNamespace = new SourceIdentityCheck(SourceIdentityVerdict.Match, $"1c-identity:v1:{DatabaseId:D}:{Guid.NewGuid():D}:test", string.Empty);
        var nonMatch = new SourceIdentityCheck(SourceIdentityVerdict.Unavailable, null, string.Empty);

        Assert.True(SourceIdentityGuard.SameSource(before, sameNamespace));
        Assert.False(SourceIdentityGuard.SameSource(before, rotatedNamespace));
        Assert.False(SourceIdentityGuard.SameSource(before, nonMatch));
        Assert.False(SourceIdentityGuard.SameSource(nonMatch, before));
        Assert.False(SourceIdentityGuard.SameSource(nonMatch, nonMatch));
    }

    private sealed class FakeIdentityClient(OnecIdentityFetchResult result) : IOnecIdentityClient
    {
        public int Calls { get; private set; }

        public Task<OnecIdentityFetchResult> GetIdentityAsync(CancellationToken cancellationToken)
        {
            Calls++;
            return Task.FromResult(result);
        }
    }
}
