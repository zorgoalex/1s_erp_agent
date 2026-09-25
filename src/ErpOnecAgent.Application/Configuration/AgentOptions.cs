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
    public int TargetBatchUncompressedBytes { get; init; } = 10 * 1024 * 1024;
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
