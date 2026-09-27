using System.Text.Json;
using ErpOnecAgent.Domain.Common;

namespace ErpOnecAgent.Application.Commands;

/// <summary>
/// Command payload limits agreed with ERP (agent-bridge to-onec/0002) and aligned with the 1C
/// extension's BSL parser, which judges the WHOLE request body: the envelope root is depth 0 and
/// the payload depth 1, every value (scalars included, property names excluded) is a node, and
/// the envelope has 7 non-payload nodes (its root plus 6 fields). A payload within these limits
/// therefore always passes the 1C parser. Case-duplicate names use .NET OrdinalIgnoreCase while
/// 1C uses ВРег; they may differ on exotic non-ASCII names, which v1 payloads do not use.
/// </summary>
public static class CommandPayloadPolicy
{
    /// <summary>Canonical payload bytes (60 KiB): leaves room for the envelope within 1C's 64 KiB body.</summary>
    public const int MaxCanonicalBytes = 61_440;
    /// <summary>Payload nesting, payload root = 0 (1C: body depth ≤ 32, payload sits at depth 1).</summary>
    public const int MaxDepth = 31;
    /// <summary>Payload values (1C: body nodes ≤ 4096 minus 7 for the envelope).</summary>
    public const int MaxNodes = 4_089;
    /// <summary>Properties of one object (1C: ≤ 128 pairs).</summary>
    public const int MaxPropertiesPerObject = 128;

    /// <summary>Null when the payload is acceptable, otherwise the validation failure.</summary>
    public static ValidationResult? Check(JsonElement payload, int maxCanonicalBytes)
    {
        try
        {
            JsonAmbiguityGuard.EnsureUnambiguous(payload);
        }
        catch (Exception ex) when (ex is InvalidDataException or InvalidOperationException)
        {
            return new(false, "INVALID_PAYLOAD", "Command payload contains duplicate or case-duplicate property names, or an unpaired surrogate.");
        }
        var nodes = 0;
        if (Walk(payload, 0, ref nodes) is { } structural) return structural;
        byte[] canonical;
        try
        {
            canonical = CanonicalBytes(payload);
        }
        catch (Exception ex) when (ex is InvalidOperationException or ArgumentException)
        {
            return new(false, "INVALID_PAYLOAD", "Command payload cannot be canonicalized.");
        }
        return canonical.Length > Math.Min(maxCanonicalBytes, MaxCanonicalBytes)
            ? new(false, "PAYLOAD_TOO_LARGE", $"Canonical payload is {canonical.Length} bytes; the limit is {Math.Min(maxCanonicalBytes, MaxCanonicalBytes)}.")
            : null;
    }

    private static ValidationResult? Walk(JsonElement value, int depth, ref int nodes)
    {
        nodes++;
        if (depth > MaxDepth) return new(false, "INVALID_PAYLOAD", $"Command payload is nested deeper than {MaxDepth} levels.");
        if (nodes > MaxNodes) return new(false, "INVALID_PAYLOAD", $"Command payload has more than {MaxNodes} values.");
        switch (value.ValueKind)
        {
            case JsonValueKind.Object:
                var properties = 0;
                foreach (var property in value.EnumerateObject())
                {
                    if (++properties > MaxPropertiesPerObject) return new(false, "INVALID_PAYLOAD", $"A payload object has more than {MaxPropertiesPerObject} properties.");
                    if (!IsWellFormed(property.Name)) return Unpaired();
                    if (Walk(property.Value, depth + 1, ref nodes) is { } failure) return failure;
                }
                break;
            case JsonValueKind.Array:
                foreach (var item in value.EnumerateArray())
                    if (Walk(item, depth + 1, ref nodes) is { } failure) return failure;
                break;
            case JsonValueKind.String:
                string? text;
                try { text = value.GetString(); }
                catch (InvalidOperationException) { return Unpaired(); }
                if (text is not null && !IsWellFormed(text)) return Unpaired();
                break;
        }
        return null;
    }

    private static ValidationResult Unpaired() => new(false, "INVALID_PAYLOAD", "Command payload contains an unpaired surrogate.");

    private static bool IsWellFormed(string text)
    {
        for (var i = 0; i < text.Length; i++)
        {
            if (char.IsHighSurrogate(text[i]))
            {
                if (i + 1 >= text.Length || !char.IsLowSurrogate(text[i + 1])) return false;
                i++;
            }
            else if (char.IsLowSurrogate(text[i])) return false;
        }
        return true;
    }

    /// <summary>
    /// The canonical form hashed by <see cref="ErpOnecAgent.Domain.Common.PayloadHasher"/>: compact
    /// UTF-8, object keys ordinal-sorted, default <see cref="Utf8JsonWriter"/> escaping. PayloadHasher
    /// itself is a byte-pinned snapshot shared with the 1C extension contract and is not changed;
    /// tests assert both produce identical bytes.
    /// </summary>
    public static byte[] CanonicalBytes(JsonElement payload)
    {
        using var stream = new MemoryStream();
        using (var writer = new Utf8JsonWriter(stream, new JsonWriterOptions { Indented = false }))
        {
            WriteCanonical(payload, writer);
        }
        return stream.ToArray();
    }

    private static void WriteCanonical(JsonElement value, Utf8JsonWriter writer)
    {
        switch (value.ValueKind)
        {
            case JsonValueKind.Object:
                writer.WriteStartObject();
                foreach (var property in value.EnumerateObject().OrderBy(static p => p.Name, StringComparer.Ordinal))
                {
                    writer.WritePropertyName(property.Name);
                    WriteCanonical(property.Value, writer);
                }
                writer.WriteEndObject();
                break;
            case JsonValueKind.Array:
                writer.WriteStartArray();
                foreach (var item in value.EnumerateArray()) WriteCanonical(item, writer);
                writer.WriteEndArray();
                break;
            default:
                value.WriteTo(writer);
                break;
        }
    }
}
