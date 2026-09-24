using ErpOnecAgent.Domain.Etl;

namespace ErpOnecAgent.Application.Etl;

public static class EtlCursorPolicy
{
    public static EtlCursor QueryFrom(EtlCursor? committed, int overlapMinutes) =>
        committed?.UpdatedAtUtc is { } updated
            ? committed with { UpdatedAtUtc = updated.AddMinutes(-Math.Max(0, overlapMinutes)), SourceId = null }
            : new EtlCursor(null, null);

    public static int Compare(EtlCursor left, EtlCursor right)
    {
        var byTime = Nullable.Compare(left.UpdatedAtUtc, right.UpdatedAtUtc);
        return byTime != 0 ? byTime : StringComparer.Ordinal.Compare(left.SourceId, right.SourceId);
    }
}
