namespace ErpOnecAgent.Application.Security;

/// <summary>
/// Stage 6 (TZ §21): warnings before the mTLS client certificate expires, at 30, 14, 7, 3
/// and 1 days left, and a critical signal once it has expired.
/// </summary>
public static class CertificateExpiryPolicy
{
    public static readonly IReadOnlyList<int> ThresholdDays = [30, 14, 7, 3, 1];

    /// <summary>
    /// The tightest threshold (in days) the certificate has crossed: 0 when it has already
    /// expired, otherwise the smallest of 30/14/7/3/1 that is not below the remaining time,
    /// or null while more than 30 days remain.
    /// </summary>
    public static int? CrossedThreshold(DateTimeOffset notAfterUtc, DateTimeOffset nowUtc)
    {
        var left = notAfterUtc - nowUtc;
        if (left <= TimeSpan.Zero) return 0;
        int? crossed = null;
        foreach (var days in ThresholdDays)
        {
            if (left <= TimeSpan.FromDays(days)) crossed = days;
        }
        return crossed;
    }
}
