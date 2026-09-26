using System.Net.Http;
using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Application.Configuration;
using ErpOnecAgent.Infrastructure.Security;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Http.Resilience;
using Microsoft.Extensions.Options;
using Polly;
using Polly.CircuitBreaker;
using Polly.Timeout;

namespace ErpOnecAgent.Infrastructure.ErpApi;

/// <summary>
/// Production registration of the ERP HTTP pipelines (A06).
///
/// The ERP lease request is a long poll held up to <c>LongPollSeconds</c>. It needs its own
/// pipeline whose attempt/total/HttpClient timeouts are derived from the configured wait plus
/// a margin. The non-idempotent <c>POST commands/lease</c> is sent exactly once per call: the
/// long-poll pipeline contains no retry strategy at all, so a 5xx/timeout is surfaced to the
/// caller and the next poll is driven by the worker, never an automatic replay. The ordinary ERP
/// pipeline keeps standard resilience but with unsafe-method retries disabled so lease/result
/// POSTs and PUTs are never silently re-sent. Extracted from the service composition root so
/// tests exercise the exact production pipeline.
/// </summary>
public static class ErpClientRegistration
{
    public const string ErpHttpClientName = "Erp";
    public const string LongPollHttpClientName = "ErpLongPoll";
    public const string TransferHttpClientName = "ErpTransfer";

    /// <summary>Single-attempt cap on one ETL transfer call.</summary>
    public static TimeSpan TransferAttemptTimeout(ErpOptions options) => TimeSpan.FromSeconds(options.TransferTimeoutSeconds);

    /// <summary>Extra time the transfer HttpClient grants over the Polly attempt cap.</summary>
    public static readonly TimeSpan TransferMargin = TimeSpan.FromSeconds(15);

    /// <summary>
    /// HttpClient timeout for the transfer channel = attempt + margin. The channel reads the whole
    /// response (ResponseContentRead), so this also bounds a stalled ACK body.
    /// </summary>
    public static TimeSpan TransferHttpClientTimeout(ErpOptions options) => TransferAttemptTimeout(options) + TransferMargin;

    /// <summary>An ACK or completion response larger than this is not a protocol answer.</summary>
    public const long TransferMaxResponseBytes = 1024 * 1024;

    /// <summary>Extra time granted over the configured server hold time.</summary>
    public static readonly TimeSpan LongPollMargin = TimeSpan.FromSeconds(15);

    /// <summary>
    /// Single-attempt cap on one long-poll request = configured wait + margin. There is no retry,
    /// so this is also the effective per-call budget.
    /// </summary>
    public static TimeSpan LongPollAttemptTimeout(ErpOptions options) => TimeSpan.FromSeconds(options.LongPollSeconds) + LongPollMargin;

    /// <summary>Total long-poll budget = attempt + margin. Strictly greater than the attempt.</summary>
    public static TimeSpan LongPollTotalTimeout(ErpOptions options) => LongPollAttemptTimeout(options) + LongPollMargin;

    /// <summary>HttpClient timeout = total + margin. Strictly greater than the total budget.</summary>
    public static TimeSpan LongPollHttpClientTimeout(ErpOptions options) => LongPollTotalTimeout(options) + LongPollMargin;

    /// <summary>
    /// Registers both ERP pipelines and wires <see cref="IErpClient"/> so that
    /// <c>commands/lease</c> travels the dedicated long-poll pipeline while every other call
    /// uses the ordinary pipeline. Single production registration entry point (A06).
    /// </summary>
    public static IServiceCollection AddErpApi(this IServiceCollection services)
    {
        var ordinary = services.AddHttpClient(ErpHttpClientName, (sp, client) => ConfigureErpClient(client, sp.GetRequiredService<IOptions<ErpOptions>>().Value));
        ordinary.ConfigurePrimaryHttpMessageHandler(static sp => CreateBaseHandler(sp.GetRequiredService<IOptions<ErpOptions>>().Value));
        ordinary.AddStandardResilienceHandler(options =>
        {
            options.AttemptTimeout.Timeout = TimeSpan.FromSeconds(10);
            options.TotalRequestTimeout.Timeout = TimeSpan.FromSeconds(30);
            // A06: never replay unsafe (POST/PUT) requests such as lease/result.
            options.Retry.DisableForUnsafeHttpMethods();
        });
        // Resolve IErpClient against the real ordinary pipeline and route lease via the long-poll channel.
        ordinary.AddTypedClient(static (httpClient, sp) =>
        {
            var longPoll = sp.GetRequiredService<LongPollErpClient>();
            var transfer = sp.GetRequiredService<TransferErpClient>();
            var agent = sp.GetRequiredService<IOptions<AgentOptions>>().Value;
            return (IErpClient)new ErpClient(httpClient, agent, null, longPoll.SendAsync, transfer.SendAsync);
        });

        AddErpLongPollClient(services);
        AddErpTransferClient(services);
        return services;
    }

    /// <summary>
    /// Registers the ETL transfer channel (batch upload, run completion). A 100 MB upload or a
    /// completion that makes ERP finalize a run can legitimately outlast the ordinary 10 s
    /// attempt cap, and a cut-off after sending is an UNKNOWN outcome (quarantine + blocked
    /// run). The channel makes exactly one attempt, bounded by Erp:TransferTimeoutSeconds; the
    /// callers own every retry decision (none for batches, fenced replay for completion).
    /// </summary>
    public static IHttpClientBuilder AddErpTransferClient(this IServiceCollection services)
    {
        var builder = services.AddHttpClient(TransferHttpClientName, (sp, client) =>
        {
            var options = sp.GetRequiredService<IOptions<ErpOptions>>().Value;
            ConfigureErpClient(client, options);
            client.Timeout = TransferHttpClientTimeout(options);
            client.MaxResponseContentBufferSize = TransferMaxResponseBytes;
        });
        builder.ConfigurePrimaryHttpMessageHandler(static sp => CreateBaseHandler(sp.GetRequiredService<IOptions<ErpOptions>>().Value));
        builder.AddResilienceHandler(TransferHttpClientName, static (pipeline, context) =>
            pipeline.AddTimeout(new TimeoutStrategyOptions { Timeout = TransferAttemptTimeout(context.ServiceProvider.GetRequiredService<IOptions<ErpOptions>>().Value) }));
        builder.AddTypedClient(static (httpClient, _) => new TransferErpClient(httpClient));
        return builder;
    }

    /// <summary>Registers the long-poll ERP pipeline and its typed holder client.</summary>
    public static IHttpClientBuilder AddErpLongPollClient(this IServiceCollection services)
    {
        var builder = services.AddHttpClient(LongPollHttpClientName, (sp, client) =>
        {
            var options = sp.GetRequiredService<IOptions<ErpOptions>>().Value;
            ConfigureErpClient(client, options);
            client.Timeout = LongPollHttpClientTimeout(options);
        });
        builder.ConfigurePrimaryHttpMessageHandler(static sp => CreateBaseHandler(sp.GetRequiredService<IOptions<ErpOptions>>().Value));
        builder.AddResilienceHandler(LongPollHttpClientName, ConfigureLongPollPipeline);
        // Explicit typed-client factory: the holder's ctor stays internal (no test-only surface),
        // and ActivatorUtilities never has to reflect over it.
        builder.AddTypedClient(static (httpClient, _) => new LongPollErpClient(httpClient));
        return builder;
    }

    private static void ConfigureLongPollPipeline(ResiliencePipelineBuilder<HttpResponseMessage> pipeline, ResilienceHandlerContext context)
    {
        var options = context.ServiceProvider.GetRequiredService<IOptions<ErpOptions>>().Value;
        var attempt = LongPollAttemptTimeout(options);
        var total = LongPollTotalTimeout(options);

        // Single attempt only. There is deliberately NO retry strategy here: adding one with
        // MaxRetryAttempts = 0 is an invalid Polly configuration, and even a valid one could
        // replay the non-idempotent lease POST. The first failure is surfaced to the caller and
        // the worker schedules the next poll.
        //
        // Ordering matters: the attempt timeout is the innermost cap so that it fires first and
        // its TimeoutRejectedException is what the breaker observes. The breaker therefore opens
        // on genuinely hung/slow long polls, and the outer total timeout stays larger than the
        // attempt so it never masks an attempt timeout.
        pipeline.AddTimeout(new TimeoutStrategyOptions { Timeout = total });

        pipeline.AddCircuitBreaker(new CircuitBreakerStrategyOptions<HttpResponseMessage>
        {
            FailureRatio = 0.5, MinimumThroughput = 10, SamplingDuration = TimeSpan.FromSeconds(60), BreakDuration = TimeSpan.FromSeconds(30),
            ShouldHandle = static args => new ValueTask<bool>(HttpClientResiliencePredicates.IsTransient(args.Outcome))
        });

        pipeline.AddTimeout(new TimeoutStrategyOptions { Timeout = attempt });
    }

    private static void ConfigureErpClient(HttpClient client, ErpOptions value)
    {
        client.BaseAddress = new Uri(value.BaseUrl.TrimEnd('/') + "/api/integration/1c-agents/" + value.ApiVersion.Trim('/') + "/");
        client.Timeout = TimeSpan.FromSeconds(value.RequestTimeoutSeconds);
    }

    private static HttpClientHandler CreateBaseHandler(ErpOptions value)
    {
        var handler = new HttpClientHandler
        {
            AutomaticDecompression = System.Net.DecompressionMethods.GZip | System.Net.DecompressionMethods.Deflate | System.Net.DecompressionMethods.Brotli,
            UseCookies = false,
            AllowAutoRedirect = false,
            CheckCertificateRevocationList = true
        };
        if (value.RequireClientCertificate) handler.ClientCertificates.Add(CertificateLoader.LoadClientCertificate(value.ClientCertificateThumbprint));
        return handler;
    }
}

/// <summary>Typed holder for the long-poll <see cref="HttpClient"/>; routes <c>commands/lease</c>.</summary>
public sealed class LongPollErpClient
{
    private readonly HttpClient _httpClient;

    // Constructed only by the typed-client factory in AddErpLongPollClient; internal so tests
    // must go through the real production registration instead of new-ing it up (A06).
    internal LongPollErpClient(HttpClient httpClient) => _httpClient = httpClient;

    public Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken) =>
        _httpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
}

/// <summary>Typed holder for the ETL transfer <see cref="HttpClient"/>; routes batch uploads and run completion.</summary>
public sealed class TransferErpClient
{
    private readonly HttpClient _httpClient;

    internal TransferErpClient(HttpClient httpClient) => _httpClient = httpClient;

    // ResponseContentRead: the ACK body is buffered inside the send, so the HttpClient timeout
    // bounds a response that stalls after its headers (a hang here would stall the upload worker).
    public Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken) =>
        _httpClient.SendAsync(request, HttpCompletionOption.ResponseContentRead, cancellationToken);
}
