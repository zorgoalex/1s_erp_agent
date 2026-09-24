using System.Text.Json;

namespace ErpOnecAgent.Domain.Common;

public static class JsonAmbiguityGuard
{
    public static void EnsureUnambiguous(JsonElement element)
    {
        switch (element.ValueKind)
        {
            case JsonValueKind.Object:
                var names = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                foreach (var property in element.EnumerateObject())
                {
                    if (!names.Add(property.Name)) throw new InvalidDataException("JSON object contains duplicate property names that differ only by case.");
                    EnsureUnambiguous(property.Value);
                }
                break;
            case JsonValueKind.Array:
                foreach (var item in element.EnumerateArray()) EnsureUnambiguous(item);
                break;
        }
    }
}
