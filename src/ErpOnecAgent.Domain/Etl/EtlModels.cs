using System.Text.Json;

namespace ErpOnecAgent.Domain.Etl;

public enum EtlRunStatus { Pending, Running, Paused, Uploading, Completing, Succeeded, PartialSuccess, Failed, Cancelled }
public enum EtlBatchStatus { Creating, Ready, Uploading, Acknowledged, RetryWaiting, DeadLetter, Deleted }

public sealed record EtlCursor(DateTimeOffset? UpdatedAtUtc, string? SourceId);

public sealed record EtlEntityDefinition(
    string EntityCode,
    string ODataPath,
    string KeyField,
    string? UpdatedAtField,
    string? DeletedField,
    IReadOnlyList<string> Select,
    string SyncMode,
    int PageSize,
    int OverlapMinutes,
    int SchemaVersion = 1,
    int ODataVersion = 3,
    bool Enabled = true,
    IReadOnlyList<string>? KeyFields = null,
    string UpdatedAtEdmType = "Edm.DateTimeOffset");

public static class EtlEntityDefinitionExtensions
{
    public static IReadOnlyList<string> EffectiveKeyFields(this EtlEntityDefinition entity) =>
        entity.KeyFields is { Count: > 0 } ? entity.KeyFields : [entity.KeyField];

    public static string SourceIdFrom(this EtlEntityDefinition entity, JsonElement row)
    {
        if (row.ValueKind != JsonValueKind.Object)
            throw new InvalidDataException($"OData row for '{entity.EntityCode}' is not an object.");

        var fields = entity.EffectiveKeyFields();
        var values = new string[fields.Count];
        for (var index = 0; index < fields.Count; index++)
        {
            var field = fields[index];
            if (!row.TryGetProperty(field, out var value))
                throw new InvalidDataException($"OData row for '{entity.EntityCode}' has no key field '{field}'.");
            values[index] = value.GetRawText();
        }

        return values.Length == 1
            ? row.GetProperty(fields[0]).ToString()
            : "[" + string.Join(',', values) + "]";
    }

    public static bool RunsOnSchedule(this EtlEntityDefinition entity) =>
        !string.Equals(entity.SyncMode, "manual_only", StringComparison.OrdinalIgnoreCase);
}

public sealed record EtlRun(Guid RunId, string Mode, IReadOnlyList<string> Entities, EtlRunStatus Status);

public sealed record EtlBatch(
    Guid BatchId,
    Guid RunId,
    string EntityName,
    int SchemaVersion,
    string FilePath,
    EtlBatchStatus Status,
    int RowCount,
    EtlCursor? WatermarkFrom,
    EtlCursor? WatermarkTo,
    string Sha256,
    long CompressedSize,
    long UncompressedSize,
    int AttemptCount,
    DateTimeOffset CreatedAtUtc);
