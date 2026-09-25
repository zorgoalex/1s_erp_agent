using System.Text.Json;
using ErpOnecAgent.Contracts.Erp;
using ErpOnecAgent.Contracts.OneC;
using ErpOnecAgent.Domain.Commands;
using ErpOnecAgent.Domain.Etl;

namespace ErpOnecAgent.Application.Abstractions;

public interface IErpClient
{
    Task<SessionStartResponse> StartSessionAsync(SessionStartRequest request, CancellationToken cancellationToken);
    Task<LeaseResponse> LeaseCommandAsync(LeaseRequest request, CancellationToken cancellationToken);
    Task AcknowledgeReceivedAsync(Guid commandId, CommandReceivedRequest request, CancellationToken cancellationToken);
    Task AcknowledgeResultAsync(Guid commandId, string resultJson, CancellationToken cancellationToken);
    Task<BatchAcknowledgement> UploadBatchAsync(EtlBatch batch, Stream content, CancellationToken cancellationToken);
    Task CompleteEtlRunAsync(Guid runId, object summary, CancellationToken cancellationToken);
    Task SendHeartbeatAsync(HeartbeatRequest request, CancellationToken cancellationToken);
    Task<RemoteConfigurationResponse?> GetConfigurationAsync(long currentVersion, CancellationToken cancellationToken);
}

public enum OnecExecutionKind { Succeeded, Processing, NotFound, BusinessError, TechnicalError, PayloadConflict }
public sealed record OnecExecutionResult(OnecExecutionKind Kind, OnecCommandResponse? Response, int? HttpStatus, string? ErrorCode, string? ErrorMessage);

public interface IOnecCommandClient
{
    Task<OnecExecutionResult> ExecuteAsync(CommandEnvelope command, CancellationToken cancellationToken);
    Task<OnecExecutionResult> GetStatusAsync(Guid commandId, CancellationToken cancellationToken);
}

public interface IOnecODataClient
{
    IAsyncEnumerable<JsonElement> ReadEntityAsync(EtlEntityDefinition entity, EtlCursor? committedCursor, EtlCursor upperBound, bool full, CancellationToken cancellationToken);
    Task<bool> CheckAsync(CancellationToken cancellationToken);
}

public interface IOnecHealthClient
{
    Task<OnecHealthResponse?> CheckAsync(CancellationToken cancellationToken);
}

/// <summary>
/// S1: reads the extension's GET identity (read-only; never initializes or rotates). Every
/// outcome is classified — the call never throws for transport, status or body errors.
/// </summary>
public interface IOnecIdentityClient
{
    Task<ErpOnecAgent.Application.Etl.OnecIdentityFetchResult> GetIdentityAsync(CancellationToken cancellationToken);
}
