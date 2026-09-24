using ErpOnecAgent.Domain.Commands;
using Xunit;

namespace ErpOnecAgent.UnitTests;

/// <summary>
/// A09 ordering slice: the stable total order of the command queue and the claim-staleness boundary
/// are defined in one place (<see cref="CommandQueueOrder"/>) and used by both the ready query and the
/// atomic claims, so equal receive times can never produce two heads of one ordering key.
/// </summary>
public sealed class CommandOrderTests
{
    [Fact]
    public void Total_order_breaks_equal_received_time_ties_by_durable_queue_sequence()
    {
        var receivedAt = DateTimeOffset.UtcNow;
        var first = Stored(100, receivedAt, queueSequence: 1);
        var second = Stored(100, receivedAt, queueSequence: 2);

        Assert.True(CommandQueueOrder.IsEarlier(first, second));
        Assert.False(CommandQueueOrder.IsEarlier(second, first));
        var sorted = CommandQueueOrder.Sort([second, first]);
        Assert.Collection(sorted,
            c => Assert.Equal(1L, c.QueueSequence),
            c => Assert.Equal(2L, c.QueueSequence));
    }

    [Fact]
    public void Total_order_never_reorders_rows_with_different_receive_times()
    {
        var earlier = Stored(100, DateTimeOffset.UtcNow.AddSeconds(-5), queueSequence: 99);
        var later = Stored(100, DateTimeOffset.UtcNow, queueSequence: 1);

        Assert.True(CommandQueueOrder.IsEarlier(earlier, later));
        Assert.False(CommandQueueOrder.IsEarlier(later, earlier));
    }

    [Fact]
    public void Total_order_sorts_by_priority_desc_then_received_time_then_sequence()
    {
        var now = DateTimeOffset.UtcNow;
        var high = Stored(200, now.AddSeconds(1), queueSequence: 3);
        var low = Stored(100, now, queueSequence: 1);
        var mid = Stored(150, now, queueSequence: 2);

        var sorted = CommandQueueOrder.Sort([low, mid, high]);

        Assert.Collection(sorted,
            c => Assert.Equal(200, c.Envelope.Priority),
            c => Assert.Equal(150, c.Envelope.Priority),
            c => Assert.Equal(100, c.Envelope.Priority));
    }

    [Fact]
    public void Claim_is_stale_only_after_the_documented_boundary()
    {
        var acquiredAt = DateTimeOffset.UtcNow;
        var boundary = acquiredAt.AddMinutes(5);

        // Exclusive boundary on the instant: a claim is stale only once the boundary instant has
        // passed (acquiredAtUtc > staleBeforeUtc), never while it is still in the future.
        Assert.False(CommandQueueOrder.IsClaimStale(acquiredAt, acquiredAt));
        Assert.False(CommandQueueOrder.IsClaimStale(acquiredAt, boundary.AddSeconds(1)));
        Assert.True(CommandQueueOrder.IsClaimStale(acquiredAt, acquiredAt.AddSeconds(-30)));
    }

    private static StoredCommand Stored(int priority, DateTimeOffset receivedAtUtc, long queueSequence) =>
        new(new(System.Guid.NewGuid(), "create_customer_order", 1, priority, "order:42", null, DateTimeOffset.UtcNow, null, null, null, "hash", default),
            CommandStatus.Queued, receivedAtUtc, 0, null, null, null, 0, 0, null, queueSequence);
}
