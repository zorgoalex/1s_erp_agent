using System.Text.Json;

namespace ErpOnecAgent.Contracts.OneC;

public sealed record OnecHealthResponse(string Status, string Application, string? ConfigurationVersion, string? PlatformVersion, DateTimeOffset? Time);

public sealed record ExecuteCommandRequest(
    Guid CommandId,
    string CommandType,
    int PayloadVersion,
    string PayloadHash,
    Guid? CorrelationId,
    DateTimeOffset RequestedAtUtc,
    JsonElement Payload);

public sealed record OnecCommandResponse(
    Guid CommandId,
    string Status,
    JsonElement? Document,
    ApiErrorBody? Error,
    IReadOnlyList<string>? Warnings,
    int ResultVersion = 1);

public sealed record ApiErrorBody(string Code, string Message, bool Retryable, JsonElement? Details);

public sealed record ODataPage(IReadOnlyList<JsonElement> Value, string? NextLink);

