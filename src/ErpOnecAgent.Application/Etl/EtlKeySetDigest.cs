using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using ErpOnecAgent.Domain.Etl;

namespace ErpOnecAgent.Application.Etl;

/// <summary>
/// V1: an order-independent digest of the multiset of row keys seen in one pass. Each key is
/// hashed with SHA-256 and the first 128 bits are summed modulo 2^128, so two passes that
/// return the same keys in any order produce the same digest, and a missing, extra or
/// duplicated key changes it (with overwhelming probability). Memory stays constant.
/// </summary>
public sealed class EtlKeySetDigest
{
    private UInt128 _sum;

    public long Count { get; private set; }

    public string Value => _sum.ToString("X32", System.Globalization.CultureInfo.InvariantCulture);

    /// <summary>Adds the row's key; throws <see cref="InvalidDataException"/> when a key field is missing or null.</summary>
    public void Add(JsonElement row, EtlEntityDefinition entity)
    {
        var builder = new StringBuilder();
        foreach (var field in entity.EffectiveKeyFields())
        {
            if (row.ValueKind != JsonValueKind.Object || !row.TryGetProperty(field, out var value) || value.ValueKind is JsonValueKind.Null or JsonValueKind.Undefined)
                throw new InvalidDataException($"A row of '{entity.EntityCode}' has no key field '{field}'.");
            // Raw JSON text: a GUID string, a number and a string of the same digits never collide.
            builder.Append(value.GetRawText()).Append('\u001f');
        }
        Span<byte> hash = stackalloc byte[SHA256.HashSizeInBytes];
        SHA256.HashData(Encoding.UTF8.GetBytes(builder.ToString()), hash);
        _sum = unchecked(_sum + System.Buffers.Binary.BinaryPrimitives.ReadUInt128BigEndian(hash[..16]));
        Count++;
    }
}
