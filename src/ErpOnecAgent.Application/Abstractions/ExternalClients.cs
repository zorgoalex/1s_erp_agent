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

    /// <summary>
    /// C1: the upload with its ACK evidence — the received response body bytes and HTTP status,
    /// so the send ledger hashes what ERP actually returned. The default (test doubles) derives
    /// the body from the parsed ACK.
    /// </summary>
    async Task<BatchUploadResponse> UploadBatchWithEvidenceAsync(EtlBatch batch, Stream content, CancellationToken cancellationToken)
    {
        var ack = await UploadBatchAsync(batch, content, cancellationToken).ConfigureAwait(false);
        return new BatchUploadResponse(ack, JsonSerializer.SerializeToUtf8Bytes(ack, JsonSerializerOptions.Web), 200);
    }

    /// <summary>
    /// E3: the upload carrying the run's source labels as <c>X-Source-Namespace</c> /
    /// <c>X-Source-Generation</c> (each only when set). The default (test doubles) ignores them.
    /// </summary>
    Task<BatchUploadResponse> UploadBatchWithEvidenceAsync(EtlBatch batch, EtlBatchSourceLabels labels, Stream content, CancellationToken cancellationToken) =>
        UploadBatchWithEvidenceAsync(batch, content, cancellationToken);

    /// <summary>
    /// E6 (spec В-4): asks ERP whether it stored a batch. <c>200</c> returns the original ACK,
    /// <c>404</c> means not stored; anything else throws. Read-only; used by the operator
    /// before an R1 resolution, never by the upload path.
    /// </summary>
    Task<EtlBatchRemoteStatus> GetBatchStatusAsync(Guid batchId, CancellationToken cancellationToken) =>
        throw new NotSupportedException("This ERP client cannot look up batches.");
    Task CompleteEtlRunAsync(Guid runId, object summary, CancellationToken cancellationToken);

    /// <summary>
    /// H1: sends the run completion with the EXACT stored payload text (F1
    /// <c>complete_payload_json</c>) as UTF-8 bytes, never re-serialized, with
    /// <c>Idempotency-Key</c> = run id. Every fenced retry of one completion therefore
    /// presents identical body bytes and dedup identity. ERP-side dedup is still unproven;
    /// this only guarantees the agent never varies what it re-sends.
    /// </summary>
    Task CompleteEtlRunRawAsync(Guid runId, string completePayloadJson, CancellationToken cancellationToken) =>
        throw new NotSupportedException("This ERP client does not support byte-identical completion replay.");
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

    /// <summary>V1: every key of the entity set, unfiltered (a second, key-only pass).</summary>
    IAsyncEnumerable<JsonElement> ReadKeysAsync(EtlEntityDefinition entity, CancellationToken cancellationToken) =>
        throw new NotSupportedException("This OData client cannot read keys.");

    /// <summary>V1: the size of the whole entity set (<c>$count</c>).</summary>
    Task<long> CountAsync(EtlEntityDefinition entity, CancellationToken cancellationToken) =>
        throw new NotSupportedException("This OData client cannot count.");
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

/// <summary>C1: a parsed batch ACK with the exact response body bytes and HTTP status it came from.</summary>
public sealed record BatchUploadResponse(BatchAcknowledgement Ack, byte[] Body, int HttpStatus);

/// <summary>E3: the run's source labels sent with each ETL batch; null values are not sent.</summary>
public sealed record EtlBatchSourceLabels(string? SourceNamespace, string? SourceGeneration);

/// <summary>E6: ERP's answer about one batch — stored (with its original ACK) or not.</summary>
public sealed record EtlBatchRemoteStatus(bool Stored, BatchAcknowledgement? Acknowledgement);
