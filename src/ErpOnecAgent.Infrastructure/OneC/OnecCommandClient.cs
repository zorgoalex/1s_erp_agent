using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Contracts.OneC;
using ErpOnecAgent.Domain.Commands;

namespace ErpOnecAgent.Infrastructure.OneC;

public sealed class OnecCommandClient(HttpClient httpClient, OnecAuthentication authentication) : IOnecCommandClient
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    public async Task<OnecExecutionResult> ExecuteAsync(CommandEnvelope command, CancellationToken cancellationToken)
    {
        var body = new ExecuteCommandRequest(command.CommandId, command.CommandType, command.PayloadVersion, command.PayloadHash, command.CorrelationId, command.CreatedAtUtc, command.Payload);
        using var request = new HttpRequestMessage(HttpMethod.Post, "commands/execute") { Content = JsonContent.Create(body, options: JsonOptions) };
        await authentication.ApplyAsync(request, cancellationToken).ConfigureAwait(false);
        return await SendAsync(request, cancellationToken).ConfigureAwait(false);
    }

    public async Task<OnecExecutionResult> GetStatusAsync(Guid commandId, CancellationToken cancellationToken)
    {
        using var request = new HttpRequestMessage(HttpMethod.Get, $"commands/{commandId:D}");
        await authentication.ApplyAsync(request, cancellationToken).ConfigureAwait(false);
        return await SendAsync(request, cancellationToken).ConfigureAwait(false);
    }

    private async Task<OnecExecutionResult> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        try
        {
            using var response = await httpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken).ConfigureAwait(false);
            if (response.StatusCode == HttpStatusCode.NotFound) return new(OnecExecutionKind.NotFound, null, 404, "NOT_FOUND", "Command is not registered in 1C.");
            OnecCommandResponse? payload = null;
            if (response.Content.Headers.ContentLength != 0)
            {
                try { payload = await response.Content.ReadFromJsonAsync<OnecCommandResponse>(JsonOptions, cancellationToken).ConfigureAwait(false); }
                catch (JsonException) when (!response.IsSuccessStatusCode) { }
            }
            if (response.StatusCode == HttpStatusCode.Conflict) return new(OnecExecutionKind.PayloadConflict, payload, 409, payload?.Error?.Code ?? "COMMAND_PAYLOAD_CONFLICT", payload?.Error?.Message);
            if (response.StatusCode == HttpStatusCode.UnprocessableEntity || response.StatusCode is HttpStatusCode.BadRequest or HttpStatusCode.Unauthorized or HttpStatusCode.Forbidden)
                return new(OnecExecutionKind.BusinessError, payload, (int)response.StatusCode, payload?.Error?.Code ?? "ONEC_BUSINESS_ERROR", payload?.Error?.Message ?? response.ReasonPhrase);
            if (response.StatusCode == HttpStatusCode.Accepted) return new(OnecExecutionKind.Processing, payload, (int)response.StatusCode, null, null);
            if (response.IsSuccessStatusCode && payload is not null)
            {
                return payload.Status.Trim().ToLowerInvariant() switch
                {
                    "succeeded" or "completed" or "created" => new(OnecExecutionKind.Succeeded, payload, (int)response.StatusCode, null, null),
                    "processing" or "executing" or "pending" => new(OnecExecutionKind.Processing, payload, (int)response.StatusCode, null, null),
                    "not_found" => new(OnecExecutionKind.NotFound, payload, (int)response.StatusCode, payload.Error?.Code ?? "NOT_FOUND", payload.Error?.Message),
                    "business_failed" or "failed" or "rejected" or "validation_error" => new(OnecExecutionKind.BusinessError, payload, (int)response.StatusCode, payload.Error?.Code ?? "ONEC_BUSINESS_ERROR", payload.Error?.Message),
                    _ => new(OnecExecutionKind.TechnicalError, payload, (int)response.StatusCode, "ONEC_UNKNOWN_STATUS", $"1C returned unsupported command status '{payload.Status}'.")
                };
            }
            return new(OnecExecutionKind.TechnicalError, payload, (int)response.StatusCode, payload?.Error?.Code ?? "ONEC_TECHNICAL_ERROR", payload?.Error?.Message ?? response.ReasonPhrase);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            return new(OnecExecutionKind.TechnicalError, null, null, "ONEC_TIMEOUT", "1C request timed out; result is unknown.");
        }
        catch (HttpRequestException ex)
        {
            return new(OnecExecutionKind.TechnicalError, null, ex.StatusCode is null ? null : (int)ex.StatusCode, "ONEC_TRANSPORT_ERROR", ex.Message);
        }
    }
}
