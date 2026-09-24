using System.Text;
using System.Text.Json;
using ErpOnecAgent.Domain.Commands;
using ErpOnecAgent.Domain.Common;

namespace ErpOnecAgent.Application.Commands;

public sealed record ValidationResult(bool IsValid, string? ErrorCode, string? Message)
{
    public static ValidationResult Success { get; } = new(true, null, null);
}

public static class CommandValidator
{
    public const int SupportedPayloadVersion = 1;

    public static ValidationResult Validate(CommandEnvelope command, IReadOnlyCollection<string> supportedTypes, int maxPayloadBytes)
    {
        if (command.CommandId == Guid.Empty)
            return new(false, "INVALID_COMMAND_ID", "commandId must be a non-empty UUID.");
        if (string.IsNullOrWhiteSpace(command.CommandType) || !supportedTypes.Contains(command.CommandType, StringComparer.Ordinal))
            return new(false, "UNSUPPORTED_COMMAND_TYPE", $"Unsupported command type '{command.CommandType}'.");
        if (command.PayloadVersion != SupportedPayloadVersion)
            return new(false, "UNSUPPORTED_PAYLOAD_VERSION", $"payloadVersion '{command.PayloadVersion}' is not supported; supported versions: {SupportedPayloadVersion}.");
        if (command.Payload.ValueKind is JsonValueKind.Undefined or JsonValueKind.Null)
            return new(false, "INVALID_PAYLOAD", "Command payload is required.");
        if (string.IsNullOrWhiteSpace(command.PayloadHash))
            return new(false, "PAYLOAD_HASH_MISMATCH", "payloadHash is required.");
        if (Encoding.UTF8.GetByteCount(command.Payload.GetRawText()) > maxPayloadBytes)
            return new(false, "PAYLOAD_TOO_LARGE", "Command payload exceeds configured limit.");
        if (!PayloadHasher.Matches(command.Payload, command.PayloadHash))
            return new(false, "PAYLOAD_HASH_MISMATCH", "Payload checksum does not match payloadHash.");
        return ValidationResult.Success;
    }
}
