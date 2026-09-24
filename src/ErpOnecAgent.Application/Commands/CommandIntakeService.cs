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

public sealed class CommandIntakeService(IErpClient erp, IAgentStore store)
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
