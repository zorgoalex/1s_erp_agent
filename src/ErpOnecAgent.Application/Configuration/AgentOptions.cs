using ErpOnecAgent.Domain.Etl;

namespace ErpOnecAgent.Application.Configuration;

public sealed class AgentOptions
{
    public const string SectionName = "Agent";
    public string AgentId { get; init; } = string.Empty;
    public string SiteId { get; init; } = string.Empty;
    public string DataDirectory { get; init; } = string.Empty;
    public string Environment { get; init; } = "Production";
    public int GracefulShutdownSeconds { get; init; } = 30;
    public int HeartbeatIntervalSeconds { get; init; } = 60;
    public int HealthCheckIntervalSeconds { get; init; } = 30;
}

public sealed class ErpOptions
{
    public const string SectionName = "Erp";
    public string BaseUrl { get; init; } = string.Empty;
    public string ApiVersion { get; init; } = "v1";
    public int LongPollSeconds { get; init; } = 25;
    public string ClientCertificateThumbprint { get; init; } = string.Empty;
    public bool RequireClientCertificate { get; init; } = true;
    public bool AllowInsecureLoopbackForTesting { get; init; }
    public int RequestTimeoutSeconds { get; init; } = 60;
}

public sealed class OnecOptions
{
    public const string SectionName = "OneC";
    public string ODataBaseUrl { get; init; } = string.Empty;
    public string CommandApiBaseUrl { get; init; } = string.Empty;
    public string CredentialSecretName { get; init; } = "onec-main";
    public int HealthTimeoutSeconds { get; init; } = 5;
    public int RequestTimeoutSeconds { get; init; } = 30;

    /// <summary>
    /// The expected identity of the 1C source (extension 0.3.0+ GET identity), configured
    /// explicitly after the source was verified — never adopted from a first server
    /// response. Absent: no source namespace exists and the new ETL path cannot capture.
    /// </summary>
    public OnecSourceBindingOptions? SourceBinding { get; init; }
}

public sealed class OnecSourceBindingOptions
{
    public string DatabaseId { get; init; } = string.Empty;
    public string ExportEpoch { get; init; } = string.Empty;
    public string Environment { get; init; } = string.Empty;
    /// <summary>The OData service root the binding was verified for; must equal the normalized <see cref="OnecOptions.ODataBaseUrl"/>.</summary>
    public string ODataEndpoint { get; init; } = string.Empty;
}

public sealed class CommandOptions
{
    public const string SectionName = "Commands";
    public int MaxConcurrency { get; init; } = 1;
    public int DefaultTimeoutSeconds { get; init; } = 30;
    public int MaxPayloadBytes { get; init; } = 1_048_576;
    public int MaxOperationalAttempts { get; init; } = 12;
    /// <summary>
    /// Maximum status-lookup (resolution) attempts before the outcome stays explicitly unknown and is dead-lettered for manual investigation.
    /// Applied dynamically through the execution-options factory; values &lt;= 0 fall back to 24 (the historical default) so legacy construction keeps working.
    /// </summary>
    public int MaxLookupAttempts { get; init; } = 24;
    /// <summary>
    /// Maximum POST attempts per command (fresh sends and NotFound re-sends share this budget) before explicit unknown-result dead-letter.
    /// Applied dynamically through the execution-options factory; values &lt;= 0 fall back to 12 (the historical default).
    /// </summary>
    public int MaxPostAttempts { get; init; } = 12;
    /// <summary>Conservative documented age limit for resolving a command result, measured from the persistent first-send timestamp (never overwritten on retries).</summary>
    public int MaxResolutionAgeHours { get; init; } = 72;
    public int RetryBaseDelaySeconds { get; init; } = 2;
    public int RetryMaxDelaySeconds { get; init; } = 300;
    public IReadOnlyList<string> SupportedTypes { get; init; } = [];
}

public sealed class EtlOptions
{
    public const string SectionName = "Etl";
    public bool Enabled { get; init; } = true;
    public bool RunOnStartup { get; init; }
    public int IntervalMinutes { get; init; } = 60;
    public int SafetyLagSeconds { get; init; } = 30;
    public int DefaultPageSize { get; init; } = 500;
    public int OverlapMinutes { get; init; } = 10;
    public int MaxConcurrentRequests { get; init; } = 1;
    public int MaxConcurrentBatchUploads { get; init; } = 2;
    /// <summary>Durable bound on ADMITTED batch send attempts (O2 send ledger, decision D1). Uncertain outcomes never retry, so this bounds ledger-proven-unsent (precheck_failed) admissions; persisted on the batch at first claim with identical-value enforcement.</summary>
    public int MaxBatchUploadAttempts { get; init; } = 5;
    /// <summary>C1: bound on completion sends of one run (F1 completion_max_attempts); exhaustion blocks the run.</summary>
    public int MaxRunCompletionAttempts { get; init; } = 20;
    public int TargetBatchUncompressedBytes { get; init; } = 10 * 1024 * 1024;
    /// <summary>A10: hard bound on one OData page response body (bytes, after decompression); exceeding it fails the read.
    /// The page is parsed as a whole, so peak memory is a small multiple of this value (parse buffer plus
    /// token metadata); the validated maximum is 256 MB.</summary>
    public long MaxODataPageBytes { get; init; } = 64L * 1024 * 1024;
    /// <summary>A10: hard bound on one OData record's JSON text (bytes).</summary>
    public int MaxODataRowBytes { get; init; } = 8 * 1024 * 1024;
    /// <summary>A10c: retries of one OData page after a transient failure (network, timeout, 408/429/5xx); 0 disables.
    /// Reading is idempotent and no row of a failed page has been yielded, so a retry cannot duplicate rows.</summary>
    public int ODataPageRetries { get; init; } = 3;
    /// <summary>A10c: first retry delay; doubles per attempt.</summary>
    public int ODataRetryBaseDelayMilliseconds { get; init; } = 1000;
    public IReadOnlyList<EtlEntityDefinition> Entities { get; init; } = [];
}

public sealed class StorageOptions
{
    public const string SectionName = "Storage";
    public long MaxSpoolBytes { get; init; } = 10L * 1024 * 1024 * 1024;
    public long MaxSqliteBytes { get; init; } = 2L * 1024 * 1024 * 1024;
    public long MaxBatchCompressedBytes { get; init; } = 100L * 1024 * 1024;
    public long MinimumReservedBytesForCommands { get; init; } = 1L * 1024 * 1024 * 1024;
    public int CompletedCommandRetentionDays { get; init; } = 30;
    public int AcknowledgedBatchRetentionDays { get; init; } = 7;
    public int BackupRetentionCount { get; init; } = 7;
    public int MaintenanceIntervalHours { get; init; } = 6;
}

public sealed class RemoteAgentConfiguration
{
    public string Mode { get; init; } = "Normal";
    public IReadOnlyList<string> CommandTypes { get; init; } = [];
    public IReadOnlyList<EtlEntityDefinition> EtlEntities { get; init; } = [];
    public int EtlIntervalMinutes { get; init; } = 60;
}
