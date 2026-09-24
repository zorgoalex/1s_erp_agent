using System.Net;
using ErpOnecAgent.Domain.Commands;

namespace ErpOnecAgent.Application.Commands;

public static class CommandPolicy
{
    private static readonly TimeSpan[] RetrySchedule =
    [
        TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(5),
        TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(30), TimeSpan.FromMinutes(1),
        TimeSpan.FromMinutes(5)
    ];

    public static bool IsTechnicalStatus(HttpStatusCode status) => status is
        HttpStatusCode.RequestTimeout or HttpStatusCode.TooManyRequests or
        HttpStatusCode.InternalServerError or HttpStatusCode.BadGateway or
        HttpStatusCode.ServiceUnavailable or HttpStatusCode.GatewayTimeout;

    public static TimeSpan RetryDelay(int attempt, Random? random = null)
    {
        var baseDelay = RetrySchedule[Math.Clamp(attempt - 1, 0, RetrySchedule.Length - 1)];
        random ??= Random.Shared;
        return baseDelay + TimeSpan.FromMilliseconds(random.Next(50, 750));
    }

    public static TimeSpan BackoffDelay(int attempt, TimeSpan baseDelay, TimeSpan maxDelay, Random? random = null)
    {
        if (attempt < 1) attempt = 1;
        random ??= Random.Shared;
        var exponent = Math.Min(attempt - 1, 16);
        var factor = Math.Pow(2, exponent);
        var ticks = (long)(baseDelay.Ticks * factor);
        var delay = ticks < 0 || factor > long.MaxValue / baseDelay.Ticks ? maxDelay : TimeSpan.FromTicks(Math.Min(ticks, maxDelay.Ticks));
        var jitterMs = random.Next(50, 750);
        var withJitter = delay.TotalMilliseconds + jitterMs;
        return withJitter > maxDelay.TotalMilliseconds && maxDelay.TotalMilliseconds >= 750
            ? maxDelay
            : TimeSpan.FromMilliseconds(withJitter);
    }

    public static bool IsExpired(CommandEnvelope command, DateTimeOffset nowUtc) =>
        command.ExpiresAtUtc is { } expiry && expiry <= nowUtc;
}

