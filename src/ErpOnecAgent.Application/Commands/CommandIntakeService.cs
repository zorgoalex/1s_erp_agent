using System.Text.Json;
using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Contracts.Erp;
using ErpOnecAgent.Domain.Commands;

namespace ErpOnecAgent.Application.Commands;

public sealed record CommandIntakeResult(
    Guid CommandId,
    StoreCommandOutcome Outcome,
    ValidationResult Validation,
    bool Acknowledged,
    Exception? AckError);

/// <param name="withholdReceived">
/// Stage-only redelivery test hook: when it returns true for a freshly admitted command, the
/// <c>received</c> acknowledgement is not sent (ERP then re-leases the command after its lease
/// expires, and the duplicate intake acknowledges it). Null in normal operation.
/// </param>
public sealed class CommandIntakeService(IErpClient erp, IAgentStore store, Func<CommandEnvelope, StoreCommandOutcome, bool>? withholdReceived = null)
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web) { PropertyNameCaseInsensitive = true };

    public async Task<CommandIntakeResult> IntakeAsync(
        Guid leaseId,
        JsonElement? leaseCommand,
        IReadOnlyCollection<string> supportedTypes,
        int maxPayloadBytes,
        DateTimeOffset receivedAtUtc,
        CancellationToken cancellationToken)
    {
        if (leaseCommand is null) throw new InvalidDataException("ERP command is empty.");
        var command = leaseCommand.Value.Deserialize<CommandEnvelope>(JsonOptions) ?? throw new InvalidDataException("ERP command is empty.");
        var validation = CommandValidator.Validate(command, supportedTypes, maxPayloadBytes);
        var outcome = await store.AdmitCommandAsync(command, receivedAtUtc, validation, cancellationToken).ConfigureAwait(false);
        Exception? ackError = null;
        var acknowledged = false;
        if (withholdReceived?.Invoke(command, outcome) == true)
            return new CommandIntakeResult(command.CommandId, outcome, validation, false, null);
        try
        {
            await erp.AcknowledgeReceivedAsync(command.CommandId, new CommandReceivedRequest(leaseId, receivedAtUtc, command.PayloadHash ?? string.Empty), cancellationToken).ConfigureAwait(false);
            acknowledged = true;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            ackError = ex;
        }
        return new CommandIntakeResult(command.CommandId, outcome, validation, acknowledged, ackError);
    }
}
