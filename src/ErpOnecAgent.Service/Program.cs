using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Application.Configuration;
using ErpOnecAgent.Domain.Etl;
using ErpOnecAgent.Infrastructure.ErpApi;
using ErpOnecAgent.Infrastructure.OneC;
using ErpOnecAgent.Infrastructure.Persistence.Sqlite;
using ErpOnecAgent.Infrastructure.Security;
using ErpOnecAgent.Infrastructure.Spool;
using ErpOnecAgent.Service.Diagnostics;
using ErpOnecAgent.Service.Runtime;
using ErpOnecAgent.Service.Workers;
using Microsoft.Extensions.Options;
using Serilog;

return await RunAsync(args).ConfigureAwait(false);

static async Task<int> RunAsync(string[] args)
{
    if (args.Contains("--version", StringComparer.Ordinal)) { Console.WriteLine(typeof(Program).Assembly.GetName().Version?.ToString(3) ?? "1.0.0"); return 0; }
    // Windows services commonly start with C:\Windows\System32 as their working
    // directory. Configuration and content files must therefore be resolved from
    // the installed executable directory, not Environment.CurrentDirectory.
    var builder = Host.CreateApplicationBuilder(new HostApplicationBuilderSettings
    {
        Args = args,
        ContentRootPath = AppContext.BaseDirectory
    });
    builder.Services.AddWindowsService(options => options.ServiceName = "ErpOnecAgent");
    ConfigureOptions(builder.Services, builder.Configuration);

    var dataDirectory = builder.Configuration[$"{AgentOptions.SectionName}:DataDirectory"] ?? Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.CommonApplicationData), "ErpOnecAgent");
    Directory.CreateDirectory(Path.Combine(dataDirectory, "logs"));
    Log.Logger = new LoggerConfiguration().MinimumLevel.Information()
        .MinimumLevel.Override("Microsoft", Serilog.Events.LogEventLevel.Warning)
        .MinimumLevel.Override("System.Net.Http", Serilog.Events.LogEventLevel.Warning)
        .MinimumLevel.Override("Polly", Serilog.Events.LogEventLevel.Warning)
        .Enrich.FromLogContext().WriteTo.Console(formatProvider: System.Globalization.CultureInfo.InvariantCulture)
        .WriteTo.File(new Serilog.Formatting.Json.JsonFormatter(), Path.Combine(dataDirectory, "logs", "agent-.json"), rollingInterval: RollingInterval.Day, retainedFileCountLimit: 30, shared: true)
        .CreateLogger();
    builder.Services.AddSerilog(Log.Logger, dispose: true);

    builder.Services.AddSingleton(sp => new SqliteConnectionFactory(Path.Combine(sp.GetRequiredService<IOptions<AgentOptions>>().Value.DataDirectory, "data", "agent.db")));
    builder.Services.AddSingleton<SqliteMigrator>();
    builder.Services.AddSingleton<IAgentStore, SqliteAgentStore>();
    builder.Services.AddSingleton<ErpOnecAgent.Application.Etl.IDiskSpaceProbe, DriveInfoDiskSpaceProbe>();
    builder.Services.AddSingleton<ISpoolStore>(sp => new FileSpoolStore(
        Path.Combine(sp.GetRequiredService<IOptions<AgentOptions>>().Value.DataDirectory, "spool"),
        sp.GetRequiredService<IOptions<StorageOptions>>().Value.MaxBatchCompressedBytes,
        sp.GetRequiredService<IOptions<StorageOptions>>().Value.MaxSpoolBytes,
        sp.GetRequiredService<ErpOnecAgent.Application.Etl.IDiskSpaceProbe>(),
        sp.GetRequiredService<IOptions<StorageOptions>>().Value.MinimumReservedBytesForCommands));
    builder.Services.AddSingleton<ISecretStore>(sp => new DpapiSecretStore(Path.Combine(sp.GetRequiredService<IOptions<AgentOptions>>().Value.DataDirectory, "secrets")));
    builder.Services.AddSingleton<OnecAuthentication>();
    builder.Services.AddSingleton<AgentRuntimeState>(); builder.Services.AddSingleton<DynamicConfigurationState>(); builder.Services.AddSingleton<LocalEtlPauseController>(); builder.Services.AddSingleton<SingleInstanceLock>(); builder.Services.AddSingleton<ErpSessionManager>(); builder.Services.AddSingleton<AgentMetricsCollector>();
    builder.Services.AddSingleton<DiagnosticsCollector>();

    builder.Services.AddErpApi();

    builder.Services.AddHttpClient<IOnecCommandClient, OnecCommandClient>((sp, client) => ConfigureOnecClient(client, sp.GetRequiredService<IOptions<OnecOptions>>().Value.CommandApiBaseUrl, sp.GetRequiredService<IOptions<OnecOptions>>().Value.RequestTimeoutSeconds)).ConfigurePrimaryHttpMessageHandler(static () => CreateBaseHandler());
    builder.Services.AddHttpClient<IOnecHealthClient, OnecHealthClient>((sp, client) => ConfigureOnecClient(client, sp.GetRequiredService<IOptions<OnecOptions>>().Value.CommandApiBaseUrl, sp.GetRequiredService<IOptions<OnecOptions>>().Value.HealthTimeoutSeconds)).ConfigurePrimaryHttpMessageHandler(static () => CreateBaseHandler()).AddStandardResilienceHandler();
    builder.Services.AddHttpClient<IOnecODataClient, OnecODataClient>((sp, client) => ConfigureOnecClient(client, sp.GetRequiredService<IOptions<OnecOptions>>().Value.ODataBaseUrl, sp.GetRequiredService<IOptions<OnecOptions>>().Value.RequestTimeoutSeconds)).ConfigurePrimaryHttpMessageHandler(static () => CreateBaseHandler());
    // A10c: no resilience handler on the OData client — OnecODataClient retries whole pages
    // itself (bounded, body included); a second layer would multiply attempts and its 10 s
    // attempt timeout would cut long 1C queries.
    // S1: read-only identity of the 1C source (extension GET identity). No resilience
    // retries: a failed read is classified and the ETL gate simply does not start work.
    builder.Services.AddHttpClient<IOnecIdentityClient, OnecIdentityClient>((sp, client) => ConfigureOnecClient(client, sp.GetRequiredService<IOptions<OnecOptions>>().Value.CommandApiBaseUrl, sp.GetRequiredService<IOptions<OnecOptions>>().Value.HealthTimeoutSeconds)).ConfigurePrimaryHttpMessageHandler(static () => CreateBaseHandler());
    builder.Services.AddSingleton(sp => new SourceIdentityGuard(() => sp.GetRequiredService<IOnecIdentityClient>(), sp.GetRequiredService<IOptions<OnecOptions>>()));

    builder.Services.AddHostedService<BootstrapService>();
    builder.Services.AddHostedService<ConfigurationWorker>();
    builder.Services.AddHostedService<ResultDeliveryWorker>();
    builder.Services.AddHostedService<CommandLeaseWorker>();
    builder.Services.AddHostedService<CommandExecutionWorker>();
    // C1: the durable ETL path. The legacy OnecEtlWorker/EtlBatchUploadWorker and the RAM
    // EtlTrigger are removed; the legacy writers are fenced out of IAgentStore.
    builder.Services.AddHostedService<ErpOnecAgent.Service.Workers.Etl.EtlExtractionWorker>();
    builder.Services.AddHostedService<ErpOnecAgent.Service.Workers.Etl.EtlUploadWorker>();
    builder.Services.AddHostedService<ErpOnecAgent.Service.Workers.Etl.EtlCompletionWorker>();
    builder.Services.AddHostedService<HealthMonitorWorker>();
    builder.Services.AddHostedService<HeartbeatWorker>();
    builder.Services.AddHostedService<MaintenanceWorker>();

    using var host = builder.Build();
    try
    {
        if (CliRunner.HasCommand(args)) return await CliRunner.RunAsync(host.Services, args, CancellationToken.None).ConfigureAwait(false);
        await host.RunAsync().ConfigureAwait(false); return 0;
    }
    catch (Exception ex) { Log.Fatal(ex, "Agent terminated unexpectedly"); return 1; }
    finally { await Log.CloseAndFlushAsync().ConfigureAwait(false); }
}

static void ConfigureOnecClient(HttpClient client, string baseUrl, int timeoutSeconds)
{
    client.BaseAddress = new Uri(baseUrl.TrimEnd('/') + "/"); client.Timeout = TimeSpan.FromSeconds(timeoutSeconds);
}

static HttpClientHandler CreateBaseHandler() => new()
{
    AutomaticDecompression = System.Net.DecompressionMethods.GZip | System.Net.DecompressionMethods.Deflate | System.Net.DecompressionMethods.Brotli,
    UseCookies = false,
    AllowAutoRedirect = false,
    CheckCertificateRevocationList = true
};

static void ConfigureOptions(IServiceCollection services, IConfiguration configuration)
{
    services.AddOptions<AgentOptions>().Bind(configuration.GetSection(AgentOptions.SectionName))
        .Validate(static value => !string.IsNullOrWhiteSpace(value.AgentId) && !string.IsNullOrWhiteSpace(value.SiteId), "AgentId and SiteId are required.")
        .Validate(static value => value.HeartbeatIntervalSeconds >= 10 && value.HealthCheckIntervalSeconds >= 5, "Agent monitoring intervals are invalid.")
        .Validate(static value => IsSafeLocalDirectory(value.DataDirectory), "DataDirectory must be a local, non-temporary, non-cloud-synchronized path.").ValidateOnStart();
    services.AddOptions<ErpOptions>().Bind(configuration.GetSection(ErpOptions.SectionName))
        .Validate(static value => IsSecureErpUrl(value), "ERP BaseUrl must use HTTPS; HTTP is allowed only on loopback with AllowInsecureLoopbackForTesting.")
        .Validate(static value => value.LongPollSeconds is >= 1 and <= 120 && value.RequestTimeoutSeconds > value.LongPollSeconds, "ERP timeout values are invalid.")
        .Validate(static value => !value.RequireClientCertificate || !string.IsNullOrWhiteSpace(value.ClientCertificateThumbprint), "mTLS certificate thumbprint is required.").ValidateOnStart();
    services.AddOptions<OnecOptions>().Bind(configuration.GetSection(OnecOptions.SectionName))
        .Validate(static value => Uri.TryCreate(value.ODataBaseUrl, UriKind.Absolute, out _) && Uri.TryCreate(value.CommandApiBaseUrl, UriKind.Absolute, out _), "1C endpoints must be absolute URLs.")
        .Validate(static value => !string.IsNullOrWhiteSpace(value.CredentialSecretName), "1C secret reference is required.")
        .Validate(static value => value.SourceBinding is null || ErpOnecAgent.Application.Etl.OnecSourceBinding.TryCreate(value.SourceBinding, out _) is not null, "OneC:SourceBinding is invalid (non-empty UUIDs in D format, environment test|production, absolute ODataEndpoint without credentials/query).")
        .Validate(static value => value.SourceBinding is null || string.Equals(ErpOnecAgent.Application.Etl.SourceEndpoint.Normalize(value.ODataBaseUrl), ErpOnecAgent.Application.Etl.SourceEndpoint.Normalize(value.SourceBinding.ODataEndpoint), StringComparison.Ordinal), "OneC:ODataBaseUrl must equal the bound SourceBinding.ODataEndpoint.")
        .Validate(static value => value.SourceBinding is null || ErpOnecAgent.Application.Etl.SourceEndpoint.SamePublication(value.ODataBaseUrl, value.CommandApiBaseUrl), "OneC:ODataBaseUrl and OneC:CommandApiBaseUrl must belong to the same 1C publication when a SourceBinding is configured.").ValidateOnStart();
    services.AddOptions<CommandOptions>().Bind(configuration.GetSection(CommandOptions.SectionName))
        .Validate(static value => value.MaxConcurrency is >= 1 and <= 4 && value.DefaultTimeoutSeconds > 0 && value.MaxPayloadBytes > 0, "Command limits are invalid.").ValidateOnStart();
    services.AddOptions<EtlOptions>().Bind(configuration.GetSection(EtlOptions.SectionName))
        .Validate(static value => value.IntervalMinutes > 0 && value.SafetyLagSeconds >= 0 && value.DefaultPageSize is >= 1 and <= 10_000 && value.TargetBatchUncompressedBytes > 0 && value.MaxConcurrentRequests is >= 1 and <= 8 && value.MaxConcurrentBatchUploads is >= 1 and <= 8 && value.MaxBatchUploadAttempts is >= 1 and <= 20 && value.MaxODataPageBytes is >= 1024 * 1024 and <= 256L * 1024 * 1024 && value.MaxODataRowBytes is >= 64 * 1024 && value.MaxODataRowBytes <= value.MaxODataPageBytes && value.ODataPageRetries is >= 0 and <= 10 && value.ODataRetryBaseDelayMilliseconds is >= 0 and <= 60_000 && value.MaxRunCompletionAttempts is >= 1 and <= 100, "ETL limits are invalid.")
        .Validate(static value => value.Entities.Select(static entity => entity.EntityCode).Distinct(StringComparer.Ordinal).Count() == value.Entities.Count, "ETL entity codes must be unique.")
        .Validate(static value => value.Entities.All(static entity =>
        {
            var keys = entity.EffectiveKeyFields();
            return !string.IsNullOrWhiteSpace(entity.EntityCode) && !string.IsNullOrWhiteSpace(entity.ODataPath) && !string.IsNullOrWhiteSpace(entity.KeyField)
                && keys.All(static key => !string.IsNullOrWhiteSpace(key)) && keys.Distinct(StringComparer.Ordinal).Count() == keys.Count && keys.Contains(entity.KeyField, StringComparer.Ordinal)
                && entity.Select.Count > 0 && entity.PageSize is >= 1 and <= 10_000 && entity.ODataVersion is >= 3 and <= 4
                && (entity.UpdatedAtField is null || entity.UpdatedAtEdmType is "Edm.DateTime" or "Edm.DateTimeOffset");
        }), "One or more ETL entities are invalid.").ValidateOnStart();
    services.AddOptions<StorageOptions>().Bind(configuration.GetSection(StorageOptions.SectionName))
        .Validate(static value => value.MaxSpoolBytes > value.MinimumReservedBytesForCommands && value.MaxSqliteBytes > 0 && value.MaxBatchCompressedBytes > 0 && value.MaxBatchCompressedBytes <= value.MaxSpoolBytes && value.BackupRetentionCount > 0 && value.MaintenanceIntervalHours > 0, "Storage limits are invalid.").ValidateOnStart();
}

static bool IsSafeLocalDirectory(string path)
{
    if (string.IsNullOrWhiteSpace(path) || path.StartsWith("\\\\", StringComparison.Ordinal)) return false;
    var full = Path.GetFullPath(path); var temp = Path.GetFullPath(Path.GetTempPath());
    return !full.StartsWith(temp, StringComparison.OrdinalIgnoreCase) && !full.Contains("\\OneDrive\\", StringComparison.OrdinalIgnoreCase) && !full.Contains("\\Dropbox\\", StringComparison.OrdinalIgnoreCase);
}

static bool IsSecureErpUrl(ErpOptions options)
{
    if (!Uri.TryCreate(options.BaseUrl, UriKind.Absolute, out var uri)) return false;
    if (uri.Scheme == Uri.UriSchemeHttps) return true;
    return options.AllowInsecureLoopbackForTesting && uri.Scheme == Uri.UriSchemeHttp && (uri.IsLoopback || string.Equals(uri.Host, "localhost", StringComparison.OrdinalIgnoreCase));
}

internal partial class Program { }
