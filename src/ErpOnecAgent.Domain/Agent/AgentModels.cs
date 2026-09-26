namespace ErpOnecAgent.Domain.Agent;

public enum AgentMode { Normal, PauseEtl, PauseCommands, Drain, Maintenance, Disabled }
public enum HealthState { Healthy, Degraded, OfflineErp, OfflineOnec, StorageWarning, StorageCritical, CertificateWarning, Maintenance, IncompatibleVersion, Fatal }

/// <summary>
/// Operational dead-letter accounting. <see cref="QueueMetrics.DeadLetters"/> counts every command
/// whose local outcome is a dead letter (<c>result_status='dead_letter'</c>, including the
/// <c>result_pending</c> delivery state) plus ETL batch dead letters, so the count is non-zero as
/// soon as a command is dead-lettered locally and is unchanged by later result ACK. A command is
/// counted exactly once: after the result is acknowledged the <c>result_status</c> still carries the
/// dead-letter outcome, so the inclusion point (local completion) is the only one.
/// </summary>
public sealed record QueueMetrics(long CommandsPending, long ResultsPending, long EtlBatchesPending, long DeadLetters, long CommandsDeadLetter = 0,
    DateTimeOffset? OldestPendingCommandAtUtc = null, DateTimeOffset? OldestPendingResultAtUtc = null, long EtlRunsUnresolved = 0,
    long EtlEntitiesFailing = 0);

