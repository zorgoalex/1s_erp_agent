using ErpOnecAgent.Application.Security;
using Xunit;

namespace ErpOnecAgent.UnitTests;

public sealed class CertificateExpiryPolicyTests
{
    private static readonly DateTimeOffset Now = new(2026, 9, 26, 0, 0, 0, TimeSpan.Zero);

    [Theory]
    [InlineData(60.0, null)]
    [InlineData(30.5, null)]
    [InlineData(30.0, 30)]
    [InlineData(20.0, 30)]
    [InlineData(14.0, 14)]
    [InlineData(8.0, 14)]
    [InlineData(7.0, 7)]
    [InlineData(3.0, 3)]
    [InlineData(2.0, 3)]
    [InlineData(1.0, 1)]
    [InlineData(0.01, 1)]
    [InlineData(0.0, 0)]
    [InlineData(-5.0, 0)]
    public void Crossed_threshold_follows_the_30_14_7_3_1_schedule(double daysLeft, int? expected) =>
        Assert.Equal(expected, CertificateExpiryPolicy.CrossedThreshold(Now.AddDays(daysLeft), Now));

    [Fact]
    public void Thresholds_are_the_documented_set() =>
        Assert.Equal([30, 14, 7, 3, 1], CertificateExpiryPolicy.ThresholdDays);
}
