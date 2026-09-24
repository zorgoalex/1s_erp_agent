using System.Text.Json;

namespace ErpOnecAgent.Contracts.Erp;

public sealed record SessionStartRequest(
    string AgentId,
    string SiteId,
    string AgentVersion,
    int LocalSchemaVersion,
    IReadOnlyList<string> Capabilities,
    DateTimeOffset SystemTimeUtc);

public sealed record SessionStartResponse(
    Guid SessionId,
    DateTimeOffset ServerTimeUtc,
    bool Accepted,
    string MinimumAgentVersion,
    long ConfigVersion,
    bool MaintenanceMode);

public sealed record LeaseRequest(Guid SessionId, IReadOnlyList<string> SupportedCommandTypes, int MaxWaitSeconds, LeaseLoad CurrentLoad);
public sealed record LeaseLoad(int Executing, int Capacity);
public sealed record LeaseResponse(bool HasCommand, Guid? LeaseId, DateTimeOffset? LeaseExpiresAtUtc, JsonElement? Command);
public sealed record CommandReceivedRequest(Guid LeaseId, DateTimeOffset ReceivedAtUtc, string PayloadHash);
public sealed record BatchAcknowledgement(Guid BatchId, string Status, int RowsAccepted, bool ChecksumValid, DateTimeOffset AcknowledgedAtUtc);
public sealed record RemoteConfigurationResponse(long ConfigVersion, string ConfigHash, JsonElement Configuration);

public sealed record HeartbeatRequest(
    string AgentId,
    string Version,
    string State,
    long UptimeSeconds,
    OnecHeartbeat OneC,
    QueueHeartbeat Queues,
    EtlHeartbeat Etl,
    MachineHeartbeat Machine,
    CertificateHeartbeat Certificate);

public sealed record OnecHeartbeat(bool ODataAvailable, bool CommandApiAvailable, DateTimeOffset? LastSuccessAtUtc, string? LastError);
public sealed record QueueHeartbeat(long CommandsPending, long ResultsPending, long EtlBatchesPending, long DeadLetters);
public sealed record EtlHeartbeat(DateTimeOffset? LastSuccessAtUtc, Guid? CurrentRunId);
public sealed record MachineHeartbeat(long DiskFreeBytes, long WorkingSetBytes, double CpuPercent, long SqliteSizeBytes, long SpoolSizeBytes);
public sealed record CertificateHeartbeat(DateTimeOffset? ExpiresAtUtc);

public sealed record ApiError(string Code, string Message, bool Retryable, JsonElement? Details = null);
