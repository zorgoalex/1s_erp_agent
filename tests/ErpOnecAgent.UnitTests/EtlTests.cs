using ErpOnecAgent.Application.Etl;
using ErpOnecAgent.Domain.Etl;
using ErpOnecAgent.Infrastructure.Spool;
using Xunit;

namespace ErpOnecAgent.UnitTests;

public sealed class EtlTests
{
    [Fact]
    public void Overlap_moves_query_start_back_without_reusing_tie_breaker()
    {
        var committed = new EtlCursor(DateTimeOffset.Parse("2026-08-29T10:00:00Z", System.Globalization.CultureInfo.InvariantCulture), "A42");
        var from = EtlCursorPolicy.QueryFrom(committed, 10);
        Assert.Equal(DateTimeOffset.Parse("2026-08-29T09:50:00Z", System.Globalization.CultureInfo.InvariantCulture), from.UpdatedAtUtc);
        Assert.Null(from.SourceId);
    }

    [Theory]
    [InlineData("clients/../../x", "clients_.._.._x")]
    [InlineData("", "entity")]
    public void Spool_filename_is_sanitized(string input, string expected) => Assert.Equal(expected, FileSpoolStore.Sanitize(input));
}
