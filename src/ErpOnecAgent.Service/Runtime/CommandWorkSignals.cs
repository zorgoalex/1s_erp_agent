namespace ErpOnecAgent.Service.Runtime;

/// <summary>
/// L1: in-process wake-ups between the command workers. SQLite stays the only source of truth;
/// a signal only says "look now". Workers keep their timed re-check as a fallback, so a lost
/// or missing signal costs at most the old polling delay and never loses work.
/// </summary>
public sealed class CommandWorkSignals
{
    /// <summary>A command was stored (lease → execution).</summary>
    public WorkSignal Commands { get; } = new();

    /// <summary>A result may be ready to send (execution/intake → delivery).</summary>
    public WorkSignal Results { get; } = new();

    /// <summary>L2: an execution slot was released (execution → lease).</summary>
    public WorkSignal Capacity { get; } = new();
}

/// <summary>
/// A coalescing auto-reset signal for EXACTLY ONE consumer (a second waiter can swallow the
/// first one's wake-up). Pulses while nobody waits collapse into one
/// pending wake-up, so a pulse between the consumer's empty query and its wait is never lost.
/// </summary>
public sealed class WorkSignal
{
    private TaskCompletionSource _pending = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public void Pulse() => Volatile.Read(ref _pending).TrySetResult();

    /// <summary>Waits for a pulse or the timeout; returns true when woken by a pulse.</summary>
    public async Task<bool> WaitAsync(TimeSpan timeout, CancellationToken cancellationToken)
    {
        var pending = Volatile.Read(ref _pending);
        if (!pending.Task.IsCompleted)
        {
            // The timer is cancelled and its token registration released as soon as a pulse
            // wins, so short cycles leave no live timers behind.
            using var timer = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            var delay = Task.Delay(timeout, timer.Token);
            if (await Task.WhenAny(pending.Task, delay).ConfigureAwait(false) == delay)
            {
                await delay.ConfigureAwait(false); // surfaces cancellation
                return false;
            }
            await timer.CancelAsync().ConfigureAwait(false);
        }
        // Consume the wake-up. A pulse racing with this swap is covered: its data was written
        // before the pulse, and the consumer queries after this returns.
        Interlocked.CompareExchange(ref _pending, new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously), pending);
        return true;
    }
}
