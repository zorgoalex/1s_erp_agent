using System.Diagnostics;
using System.Net;
using System.Text;
using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Application.Configuration;
using ErpOnecAgent.Contracts.Erp;
using ErpOnecAgent.Infrastructure.ErpApi;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Xunit;

namespace ErpOnecAgent.UnitTests;

/// <summary>
/// A06 regression tests for the long-poll HTTP pipeline. Every test drives the real production
/// registration (<see cref="ErpClientRegistration.AddErpApi"/>) and resolves <see cref="IErpClient"/>
/// through <c>ServiceCollection</c>; the transport is the only stub. They prove a full 25s long-poll
/// hold is NOT cut off by the original production bug (a standard 10s attempt timeout that aborted a
/// legitimate 25s hold), the lease POST is sent exactly once (never replayed on 503), the lease
/// travels the dedicated long-poll channel with the correct headers, and caller cancellation
/// propagates promptly.
/// </summary>
public sealed class ErpLongPollResilienceTests
{
    private const int LongPollSeconds = 25;

    [Fact]
    public async Task Long_poll_hold_beyond_attempt_timeout_is_not_cut_off_and_is_not_retried()
    {
        var probe = new ProbeHandler(async (request, ct) =>
        {
            // Holds the full configured long-poll window (25s), which exceeds the original
            // production 10s attempt timeout but stays within the real budget (hold + margin = 40s).
            // A pipeline that keeps the standard 10s attempt timeout cuts this off.
            await Task.Delay(TimeSpan.FromSeconds(LongPollSeconds), ct);
            return EmptyLease();
        });
        await using var provider = NewServices(probe);
        var client = provider.GetRequiredService<IErpClient>();

        var stopwatch = Stopwatch.StartNew();
        var response = await client.LeaseCommandAsync(NewLeaseRequest(), CancellationToken.None);
        stopwatch.Stop();

        Assert.False(response.HasCommand);
        // Meaningful lower bound: the probe holds 25s, so a full-hold pipeline must not return before 24s.
        Assert.True(stopwatch.Elapsed >= TimeSpan.FromSeconds(24), $"hold was cut short: {stopwatch.Elapsed}");
        Assert.Equal(1, probe.Requests); // never replayed
    }

    [Fact]
    public void Long_poll_budget_is_strictly_ordered_attempt_total_httpclient()
    {
        var options = new ErpOptions { LongPollSeconds = 25 };
        var attempt = ErpClientRegistration.LongPollAttemptTimeout(options);
        var total = ErpClientRegistration.LongPollTotalTimeout(options);
        var http = ErpClientRegistration.LongPollHttpClientTimeout(options);

        Assert.Equal(TimeSpan.FromSeconds(40), attempt); // 25s hold + 15s margin
        Assert.Equal(TimeSpan.FromSeconds(55), total);   // attempt + 15s margin
        Assert.Equal(TimeSpan.FromSeconds(70), http);    // total + 15s margin
        // Required nesting: HttpClient > total > attempt > hold, so the attempt timeout is the
        // innermost cap (visible to the circuit breaker) and never masked by the outer budgets.
        Assert.True(attempt > TimeSpan.FromSeconds(25), "attempt must exceed the configured hold");
        Assert.True(total > attempt, "total budget must exceed the attempt timeout");
        Assert.True(http > total, "HttpClient timeout must exceed the total budget");

        var longHold = new ErpOptions { LongPollSeconds = 40 };
        Assert.Equal(TimeSpan.FromSeconds(55), ErpClientRegistration.LongPollAttemptTimeout(longHold));
        Assert.True(ErpClientRegistration.LongPollAttemptTimeout(longHold) > TimeSpan.FromSeconds(40));
    }

    [Fact]
    public async Task Lease_post_is_never_re_sent_on_503()
    {
        var probe = new ProbeHandler((request, ct) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)
        {
            Content = new StringContent("{\"code\":\"UNAVAILABLE\"}", Encoding.UTF8, "application/json")
        }));
        await using var provider = NewServices(probe);
        var client = provider.GetRequiredService<IErpClient>();

        // 503 is transient, so any retry strategy would replay the non-idempotent lease POST.
        await Assert.ThrowsAsync<HttpRequestException>(() => client.LeaseCommandAsync(NewLeaseRequest(), CancellationToken.None));

        Assert.Equal(1, probe.Requests); // exactly one send, no automatic replay
    }

    [Fact]
    public async Task Cancellation_of_a_hung_long_poll_propagates_promptly()
    {
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var probe = new ProbeHandler(async (request, ct) =>
        {
            entered.TrySetResult();
            await Task.Delay(TimeSpan.FromSeconds(60), ct);
            return EmptyLease();
        });
        await using var provider = NewServices(probe);
        var client = provider.GetRequiredService<IErpClient>();
        using var cts = new CancellationTokenSource();

        var task = client.LeaseCommandAsync(NewLeaseRequest(), cts.Token);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        var stopwatch = Stopwatch.StartNew();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => task);
        stopwatch.Stop();
        Assert.True(stopwatch.Elapsed < TimeSpan.FromSeconds(5), $"cancellation was not prompt: {stopwatch.Elapsed}");
        Assert.Equal(1, probe.Requests);
    }

    [Fact]
    public async Task Lease_is_routed_through_the_long_poll_channel_with_agent_headers()
    {
        var erpProbe = new ProbeHandler((request, ct) => Task.FromResult(EmptyLease()));
        var longPollProbe = new ProbeHandler((request, ct) => Task.FromResult(EmptyLease()));
        await using var provider = NewServices(longPollProbe, erpProbe);
        var client = provider.GetRequiredService<IErpClient>();

        await client.LeaseCommandAsync(NewLeaseRequest(), CancellationToken.None);

        Assert.Equal(1, longPollProbe.Requests); // lease used the long-poll channel
        Assert.Equal(0, erpProbe.Requests);      // ordinary channel untouched

        // Correct channel and headers on the long-poll request.
        Assert.Equal("/api/integration/1c-agents/v1/commands/lease", longPollProbe.LastPathAndQuery);
        Assert.Equal("POST", longPollProbe.LastMethod);
        Assert.Equal("agent-a", longPollProbe.LastHeaders["X-Agent-Id"]);
        Assert.Equal("site-s", longPollProbe.LastHeaders["X-Site-Id"]);
        Assert.True(longPollProbe.LastHeaders.ContainsKey("X-Request-Id"));
        Assert.True(longPollProbe.LastHeaders.ContainsKey("X-Correlation-Id"));
    }

    /// <summary>
    /// Builds the exact production DI graph via <see cref="ErpClientRegistration.AddErpApi"/> and
    /// overrides only the primary handlers of the two named channels (<c>Erp</c>, <c>ErpLongPoll</c>).
    /// </summary>
    private static ServiceProvider NewServices(HttpMessageHandler longPollProbe, HttpMessageHandler? erpProbe = null)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<IOptions<ErpOptions>>(Options.Create(new ErpOptions
        {
            BaseUrl = "https://erp.test",
            ApiVersion = "v1",
            LongPollSeconds = LongPollSeconds,
            RequestTimeoutSeconds = 120,
            RequireClientCertificate = false
        }));
        services.AddSingleton<IOptions<AgentOptions>>(Options.Create(new AgentOptions { AgentId = "agent-a", SiteId = "site-s" }));

        // Production registration: named clients "Erp" and "ErpLongPoll" + IErpClient wiring.
        services.AddErpApi();

        // Override the primary handler of each named channel. The later
        // ConfigurePrimaryHttpMessageHandler registration wins, so these stubs replace the real
        // HttpClientHandler without exposing any internal constructor to tests.
        services.AddHttpClient(ErpClientRegistration.ErpHttpClientName)
            .ConfigurePrimaryHttpMessageHandler(_ => erpProbe ?? new ProbeHandler((request, ct) => Task.FromResult(EmptyLease())));
        services.AddHttpClient(ErpClientRegistration.LongPollHttpClientName)
            .ConfigurePrimaryHttpMessageHandler(_ => longPollProbe);

        return services.BuildServiceProvider();
    }

    private static LeaseRequest NewLeaseRequest() => new(Guid.NewGuid(), ["create_customer_order"], LongPollSeconds, new LeaseLoad(0, 1));

    private static HttpResponseMessage EmptyLease() => new(HttpStatusCode.OK)
    {
        Content = new StringContent("{\"hasCommand\":false}", Encoding.UTF8, "application/json")
    };

    private sealed class ProbeHandler(Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> respond) : HttpMessageHandler
    {
        private int _requests;
        public int Requests => Volatile.Read(ref _requests);
        public string LastPathAndQuery { get; private set; } = string.Empty;
        public string LastMethod { get; private set; } = string.Empty;
        public Dictionary<string, string> LastHeaders { get; } = new(StringComparer.OrdinalIgnoreCase);

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            Interlocked.Increment(ref _requests);
            LastPathAndQuery = request.RequestUri?.PathAndQuery ?? string.Empty;
            LastMethod = request.Method.Method;
            foreach (var header in request.Headers) LastHeaders[header.Key] = string.Join(",", header.Value);
            return await respond(request, ct).ConfigureAwait(false);
        }
    }
}