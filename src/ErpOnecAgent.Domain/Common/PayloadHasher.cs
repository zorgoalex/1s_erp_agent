using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace ErpOnecAgent.Domain.Common;

public static class PayloadHasher
{
    public static string Compute(JsonElement payload)
    {
        using var stream = new MemoryStream();
        using (var writer = new Utf8JsonWriter(stream, new JsonWriterOptions { Indented = false }))
        {
            WriteCanonical(payload, writer);
        }

        return Convert.ToBase64String(SHA256.HashData(stream.ToArray()));
    }

    public static bool Matches(JsonElement payload, string expected) =>
        CryptographicOperations.FixedTimeEquals(
            Encoding.UTF8.GetBytes(Compute(payload)),
            Encoding.UTF8.GetBytes(expected));

    public static string ComputeBytes(ReadOnlySpan<byte> payload) =>
        Convert.ToBase64String(SHA256.HashData(payload));

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
                foreach (var item in value.EnumerateArray())
                {
                    WriteCanonical(item, writer);
                }
                writer.WriteEndArray();
                break;
            default:
                value.WriteTo(writer);
                break;
        }
    }
}

