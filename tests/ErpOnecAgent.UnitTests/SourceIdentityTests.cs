using ErpOnecAgent.Application.Configuration;
using ErpOnecAgent.Application.Etl;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Xunit;

namespace ErpOnecAgent.UnitTests;

public sealed class SourceIdentityTests
{
    private static readonly Guid DatabaseId = Guid.Parse("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
    private static readonly Guid ExportEpoch = Guid.Parse("b1eebc99-9c0b-4ef8-bb6d-6bb9bd380a22");
    private const string BoundEndpoint = "http://srv/base/odata";

    private static OnecSourceBindingOptions BindingOptions(
        string? databaseId = null,
        string? exportEpoch = null,
        string? environment = null,
        string? odataEndpoint = null) => new()
    {
        DatabaseId = databaseId ?? DatabaseId.ToString("D"),
        ExportEpoch = exportEpoch ?? ExportEpoch.ToString("D"),
        Environment = environment ?? "test",
        ODataEndpoint = odataEndpoint ?? BoundEndpoint
    };

    // Like BindingOptions but every field is passed verbatim, so null is tested too.
    private static OnecSourceBindingOptions RawBindingOptions(
        string? databaseId,
        string? exportEpoch,
        string? environment,
        string? odataEndpoint) => new()
    {
        DatabaseId = databaseId!,
        ExportEpoch = exportEpoch!,
        Environment = environment!,
        ODataEndpoint = odataEndpoint!
    };

    private static OnecSourceIdentity MatchingIdentity() =>
        new(1, "ready", "whole-infobase", DatabaseId, ExportEpoch, "test");

    // B1: a valid binding produces "1c-identity:v1:{databaseId D lower}:{exportEpoch}:{env}".
    [Fact]
    public void B1_valid_binding_produces_stable_lowercase_namespace()
    {
        var binding = OnecSourceBinding.TryCreate(BindingOptions(), out var reason);

        Assert.NotNull(binding);
        Assert.Empty(reason);
        Assert.Equal($"1c-identity:v1:{DatabaseId:D}:{ExportEpoch:D}:test", binding.SourceNamespace);
        Assert.Equal(DatabaseId, binding.DatabaseId);
        Assert.Equal(ExportEpoch, binding.ExportEpoch);
        Assert.Equal("test", binding.Environment);
        Assert.Equal(BoundEndpoint, binding.ODataEndpoint);
    }

    [Fact]
    public void B1_uppercase_guid_input_produces_lowercase_d_output()
    {
        var binding = OnecSourceBinding.TryCreate(
            BindingOptions(databaseId: "A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11", exportEpoch: "B1EEBC99-9C0B-4EF8-BB6D-6BB9BD380A22"),
            out _);

        Assert.NotNull(binding);
        Assert.Equal($"1c-identity:v1:{DatabaseId:D}:{ExportEpoch:D}:test", binding.SourceNamespace);
    }

    // B2: null options and every invalid field produce null with a reason.
    [Fact]
    public void B2_null_options_returns_null_with_reason()
    {
        var binding = OnecSourceBinding.TryCreate(null, out var reason);

        Assert.Null(binding);
        Assert.False(string.IsNullOrEmpty(reason));
    }

    [Theory]
    [InlineData("00000000-0000-0000-0000-000000000000")] // zero GUID
    [InlineData("a0eebc999c0b4ef8bb6d6bb9bd380a11")] // N format, not D
    [InlineData("{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}")] // B format, not D
    [InlineData("not-a-guid")]
    [InlineData("")]
    [InlineData(null)]
    public void B2_invalid_database_id_is_rejected(string? databaseId)
    {
        var binding = OnecSourceBinding.TryCreate(
            RawBindingOptions(databaseId, ExportEpoch.ToString("D"), "test", BoundEndpoint), out var reason);

        Assert.Null(binding);
        Assert.False(string.IsNullOrEmpty(reason));
    }

    [Theory]
    [InlineData("00000000-0000-0000-0000-000000000000")] // zero GUID
    [InlineData("b1eebc999c0b4ef8bb6d6bb9bd380a22")] // N format, not D
    [InlineData("not-a-guid")]
    [InlineData("")]
    [InlineData(null)]
    public void B2_invalid_export_epoch_is_rejected(string? exportEpoch)
    {
        var binding = OnecSourceBinding.TryCreate(
            RawBindingOptions(DatabaseId.ToString("D"), exportEpoch, "test", BoundEndpoint), out var reason);

        Assert.Null(binding);
        Assert.False(string.IsNullOrEmpty(reason));
    }

    [Theory]
    [InlineData("Test")] // environment match is case-sensitive
    [InlineData("PRODUCTION")]
    [InlineData("staging")]
    [InlineData(" test")]
    [InlineData("")]
    [InlineData(null)]
    public void B2_environment_outside_test_or_production_is_rejected(string? environment)
    {
        var binding = OnecSourceBinding.TryCreate(
            RawBindingOptions(DatabaseId.ToString("D"), ExportEpoch.ToString("D"), environment, BoundEndpoint), out var reason);

        Assert.Null(binding);
        Assert.False(string.IsNullOrEmpty(reason));
    }

    [Theory]
    [InlineData("/base/odata")] // relative
    [InlineData("srv/base/odata")] // relative, no scheme
    [InlineData("ftp://srv/base/odata")] // not http(s)
    [InlineData("http://user:pass@srv/base/odata")] // credentials
    [InlineData("http://srv/base/odata?api-version=1")] // query
    [InlineData("http://srv/base/odata#section")] // fragment
    [InlineData("   ")] // blank
    [InlineData(null)]
    public void B2_invalid_odata_endpoint_is_rejected(string? odataEndpoint)
    {
        var binding = OnecSourceBinding.TryCreate(
            RawBindingOptions(DatabaseId.ToString("D"), ExportEpoch.ToString("D"), "test", odataEndpoint), out var reason);

        Assert.Null(binding);
        Assert.False(string.IsNullOrEmpty(reason));
    }

    // N1: scheme and host are folded, default port elided, non-default port kept, one
    // trailing slash trimmed, path case preserved.
    [Fact]
    public void N1_normalize_folds_scheme_host_and_default_port()
    {
        Assert.Equal("http://host/Base/odata", SourceEndpoint.Normalize("http://Host:80/Base/odata/"));
        Assert.Equal("http://host/Base/odata", SourceEndpoint.Normalize("HTTP://HOST/Base/odata"));
        Assert.Equal("https://host/base", SourceEndpoint.Normalize("https://host:443/base"));
        Assert.Equal("http://host:8080/base", SourceEndpoint.Normalize("http://host:8080/base"));
        Assert.Equal("http://host", SourceEndpoint.Normalize("http://host/"));
    }

    [Fact]
    public void N1_equivalent_endpoints_normalize_equal_and_different_paths_do_not()
    {
        Assert.Equal(SourceEndpoint.Normalize("http://Host:80/Base/odata/"), SourceEndpoint.Normalize("http://host/Base/odata"));
        Assert.NotEqual(SourceEndpoint.Normalize("http://host/base"), SourceEndpoint.Normalize("http://host/other"));
        Assert.NotEqual(SourceEndpoint.Normalize("http://host/Base"), SourceEndpoint.Normalize("http://host/base")); // path case is preserved
    }

    // V1: a ready identity equal to the binding yields Match with the namespace.
    [Fact]
    public void V1_matching_identity_returns_match_with_source_namespace()
    {
        var check = SourceIdentityVerifier.Verify(BindingOptions(), "http://srv/base/odata/", OnecIdentityFetchResult.Ready(MatchingIdentity()));

        Assert.Equal(SourceIdentityVerdict.Match, check.Verdict);
        Assert.True(check.IsMatch);
        Assert.Equal($"1c-identity:v1:{DatabaseId:D}:{ExportEpoch:D}:test", check.SourceNamespace);
    }

    // V2: no binding configured -> BindingMissing.
    [Fact]
    public void V2_missing_binding_returns_binding_missing()
    {
        var check = SourceIdentityVerifier.Verify(null, BoundEndpoint, OnecIdentityFetchResult.Ready(MatchingIdentity()));

        Assert.Equal(SourceIdentityVerdict.BindingMissing, check.Verdict);
        Assert.Null(check.SourceNamespace);
        Assert.False(check.IsMatch);
    }

    // V3: an unparseable binding -> BindingInvalid with the TryCreate reason.
    [Fact]
    public void V3_invalid_binding_returns_binding_invalid()
    {
        var check = SourceIdentityVerifier.Verify(BindingOptions(databaseId: "not-a-guid"), BoundEndpoint, OnecIdentityFetchResult.Ready(MatchingIdentity()));

        Assert.Equal(SourceIdentityVerdict.BindingInvalid, check.Verdict);
        Assert.Null(check.SourceNamespace);
        Assert.False(string.IsNullOrEmpty(check.Detail));
    }

    // V4: the OData root must equal the bound endpoint; this is decided before the
    // fetched result is consulted.
    [Fact]
    public void V4_endpoint_mismatch_takes_precedence_over_fetched_result()
    {
        var check = SourceIdentityVerifier.Verify(BindingOptions(), "http://other/base/odata", OnecIdentityFetchResult.Ready(MatchingIdentity()));

        Assert.Equal(SourceIdentityVerdict.EndpointMismatch, check.Verdict);
        Assert.Null(check.SourceNamespace);
    }

    [Fact]
    public void V4_endpoint_mismatch_takes_precedence_over_malformed_fetch()
    {
        var check = SourceIdentityVerifier.Verify(BindingOptions(), "http://other/base/odata", OnecIdentityFetchResult.Malformed(200, "bad body"));

        Assert.Equal(SourceIdentityVerdict.EndpointMismatch, check.Verdict);
        Assert.Null(check.SourceNamespace);
    }

    // V5: Unavailable, NotReady and Malformed fetch kinds pass through unchanged.
    [Fact]
    public void V5_unavailable_fetch_passes_through()
    {
        var check = SourceIdentityVerifier.Verify(BindingOptions(), BoundEndpoint, OnecIdentityFetchResult.Unavailable(null, "timeout"));

        Assert.Equal(SourceIdentityVerdict.Unavailable, check.Verdict);
        Assert.Equal("timeout", check.Detail);
        Assert.Null(check.SourceNamespace);
    }

    [Fact]
    public void V5_not_ready_fetch_passes_through()
    {
        var check = SourceIdentityVerifier.Verify(BindingOptions(), BoundEndpoint, OnecIdentityFetchResult.NotReady(503, "blocked"));

        Assert.Equal(SourceIdentityVerdict.NotReady, check.Verdict);
        Assert.Contains("blocked", check.Detail, StringComparison.Ordinal);
        Assert.Null(check.SourceNamespace);
    }

    [Fact]
    public void V5_malformed_fetch_passes_through()
    {
        var check = SourceIdentityVerifier.Verify(BindingOptions(), BoundEndpoint, OnecIdentityFetchResult.Malformed(200, "not valid JSON"));

        Assert.Equal(SourceIdentityVerdict.Malformed, check.Verdict);
        Assert.Equal("not valid JSON", check.Detail);
        Assert.Null(check.SourceNamespace);
    }

    // V6: a scope other than whole-infobase is unsupported.
    [Fact]
    public void V6_unsupported_scope_returns_scope_unsupported()
    {
        var identity = new OnecSourceIdentity(1, "ready", "catalog-only", DatabaseId, ExportEpoch, "test");
        var check = SourceIdentityVerifier.Verify(BindingOptions(), BoundEndpoint, OnecIdentityFetchResult.Ready(identity));

        Assert.Equal(SourceIdentityVerdict.ScopeUnsupported, check.Verdict);
        Assert.Contains("catalog-only", check.Detail, StringComparison.Ordinal);
        Assert.Null(check.SourceNamespace);
    }

    // V7: each identity field that differs from the binding yields its own verdict.
    [Fact]
    public void V7_different_database_id_returns_database_mismatch()
    {
        var identity = new OnecSourceIdentity(1, "ready", "whole-infobase", Guid.NewGuid(), ExportEpoch, "test");
        var check = SourceIdentityVerifier.Verify(BindingOptions(), BoundEndpoint, OnecIdentityFetchResult.Ready(identity));

        Assert.Equal(SourceIdentityVerdict.DatabaseMismatch, check.Verdict);
        Assert.Null(check.SourceNamespace);
    }

    [Fact]
    public void V7_different_export_epoch_returns_epoch_mismatch()
    {
        var identity = new OnecSourceIdentity(1, "ready", "whole-infobase", DatabaseId, Guid.NewGuid(), "test");
        var check = SourceIdentityVerifier.Verify(BindingOptions(), BoundEndpoint, OnecIdentityFetchResult.Ready(identity));

        Assert.Equal(SourceIdentityVerdict.EpochMismatch, check.Verdict);
        Assert.Null(check.SourceNamespace);
    }

    [Fact]
    public void V7_different_environment_returns_environment_mismatch()
    {
        var identity = new OnecSourceIdentity(1, "ready", "whole-infobase", DatabaseId, ExportEpoch, "production");
        var check = SourceIdentityVerifier.Verify(BindingOptions(), BoundEndpoint, OnecIdentityFetchResult.Ready(identity));

        Assert.Equal(SourceIdentityVerdict.EnvironmentMismatch, check.Verdict);
        Assert.Null(check.SourceNamespace);
    }

    // V8: SourceNamespace is populated only for Match.
    [Theory]
    [MemberData(nameof(NonMatchChecks))]
    public void V8_every_non_match_verdict_has_null_source_namespace(SourceIdentityCheck check)
    {
        Assert.NotEqual(SourceIdentityVerdict.Match, check.Verdict);
        Assert.False(check.IsMatch);
        Assert.Null(check.SourceNamespace);
    }

    public static TheoryData<SourceIdentityCheck> NonMatchChecks()
    {
        var options = BindingOptions();
        var ready = OnecIdentityFetchResult.Ready(MatchingIdentity());
        var data = new TheoryData<SourceIdentityCheck>
        {
            SourceIdentityVerifier.Verify(null, BoundEndpoint, ready),
            SourceIdentityVerifier.Verify(BindingOptions(databaseId: "bad"), BoundEndpoint, ready),
            SourceIdentityVerifier.Verify(options, "http://other/base", ready),
            SourceIdentityVerifier.Verify(options, BoundEndpoint, OnecIdentityFetchResult.Unavailable(503, "down")),
            SourceIdentityVerifier.Verify(options, BoundEndpoint, OnecIdentityFetchResult.NotReady(503, "uninitialized")),
            SourceIdentityVerifier.Verify(options, BoundEndpoint, OnecIdentityFetchResult.Malformed(200, "bad")),
            SourceIdentityVerifier.Verify(options, BoundEndpoint, OnecIdentityFetchResult.Ready(
                new OnecSourceIdentity(1, "ready", "catalog-only", DatabaseId, ExportEpoch, "test"))),
            SourceIdentityVerifier.Verify(options, BoundEndpoint, OnecIdentityFetchResult.Ready(
                new OnecSourceIdentity(1, "ready", "whole-infobase", Guid.NewGuid(), ExportEpoch, "test"))),
            SourceIdentityVerifier.Verify(options, BoundEndpoint, OnecIdentityFetchResult.Ready(
                new OnecSourceIdentity(1, "ready", "whole-infobase", DatabaseId, Guid.NewGuid(), "test"))),
            SourceIdentityVerifier.Verify(options, BoundEndpoint, OnecIdentityFetchResult.Ready(
                new OnecSourceIdentity(1, "ready", "whole-infobase", DatabaseId, ExportEpoch, "production")))
        };
        return data;
    }
}

// O1: the OneC:SourceBinding option validation added to Program.cs (the two SourceBinding
// Validate lines) driven through the real options validation pipeline.
public sealed class SourceBindingOptionsValidationTests
{
    private static readonly Guid DatabaseId = Guid.Parse("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
    private static readonly Guid ExportEpoch = Guid.Parse("b1eebc99-9c0b-4ef8-bb6d-6bb9bd380a22");

    private static List<string> ValidationFailures(OnecOptions value)
    {
        // The same predicates as Program.cs ConfigureOptions for OneC:SourceBinding.
        var services = new ServiceCollection();
        services.AddOptions<OnecOptions>()
            .Validate(static v => v.SourceBinding is null || OnecSourceBinding.TryCreate(v.SourceBinding, out _) is not null,
                "OneC:SourceBinding is invalid (non-empty UUIDs in D format, environment test|production, absolute ODataEndpoint without credentials/query).")
            .Validate(static v => v.SourceBinding is null || string.Equals(SourceEndpoint.Normalize(v.ODataBaseUrl), SourceEndpoint.Normalize(v.SourceBinding.ODataEndpoint), StringComparison.Ordinal),
                "OneC:ODataBaseUrl must equal the bound SourceBinding.ODataEndpoint.");
        using var provider = services.BuildServiceProvider();
        return provider.GetServices<IValidateOptions<OnecOptions>>()
            .Select(v => v.Validate(Options.DefaultName, value))
            .Where(r => r.Failed)
            .SelectMany(r => r.Failures ?? [])
            .ToList();
    }

    private static OnecSourceBindingOptions Binding(string endpoint) => new()
    {
        DatabaseId = DatabaseId.ToString("D"),
        ExportEpoch = ExportEpoch.ToString("D"),
        Environment = "test",
        ODataEndpoint = endpoint
    };

    [Fact]
    public void O1_null_binding_is_valid()
    {
        var failures = ValidationFailures(new OnecOptions { ODataBaseUrl = "http://srv/base/odata", SourceBinding = null });

        Assert.Empty(failures);
    }

    [Fact]
    public void O1_binding_endpoint_equal_to_odata_base_url_modulo_normalization_is_valid()
    {
        var options = new OnecOptions
        {
            ODataBaseUrl = "http://srv/base/odata",
            SourceBinding = Binding("HTTP://SRV:80/base/odata/")
        };

        Assert.Empty(ValidationFailures(options));
    }

    [Fact]
    public void O1_mismatching_endpoint_is_invalid()
    {
        var options = new OnecOptions
        {
            ODataBaseUrl = "http://srv/base/odata",
            SourceBinding = Binding("http://srv/other/odata")
        };

        var failures = ValidationFailures(options);

        Assert.Contains("OneC:ODataBaseUrl must equal the bound SourceBinding.ODataEndpoint.", failures);
    }

    [Fact]
    public void O1_invalid_guid_is_invalid()
    {
        var options = new OnecOptions
        {
            ODataBaseUrl = "http://srv/base/odata",
            SourceBinding = new OnecSourceBindingOptions
            {
                DatabaseId = "not-a-guid",
                ExportEpoch = ExportEpoch.ToString("D"),
                Environment = "test",
                ODataEndpoint = "http://srv/base/odata"
            }
        };

        Assert.NotEmpty(ValidationFailures(options));
    }
}
