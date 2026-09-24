namespace ErpOnecAgent.Domain.Commands;

public static class CommandStateMachine
{
    private static readonly Dictionary<CommandStatus, HashSet<CommandStatus>> Allowed =
        new Dictionary<CommandStatus, HashSet<CommandStatus>>
        {
            [CommandStatus.Received] = [CommandStatus.Queued, CommandStatus.DeadLetter],
            [CommandStatus.Queued] = [CommandStatus.Executing, CommandStatus.Expired, CommandStatus.Cancelled, CommandStatus.DeadLetter],
            [CommandStatus.Executing] = [CommandStatus.SucceededLocal, CommandStatus.BusinessFailedLocal, CommandStatus.RetryWaiting, CommandStatus.UnknownResult],
            [CommandStatus.UnknownResult] = [CommandStatus.Executing, CommandStatus.SucceededLocal, CommandStatus.BusinessFailedLocal, CommandStatus.RetryWaiting, CommandStatus.DeadLetter],
            [CommandStatus.RetryWaiting] = [CommandStatus.Queued, CommandStatus.DeadLetter],
            [CommandStatus.SucceededLocal] = [CommandStatus.ResultPending],
            [CommandStatus.BusinessFailedLocal] = [CommandStatus.ResultPending],
            [CommandStatus.Expired] = [CommandStatus.ResultPending],
            [CommandStatus.Cancelled] = [CommandStatus.ResultPending],
            [CommandStatus.DeadLetter] = [CommandStatus.ResultPending],
            [CommandStatus.ResultPending] = [CommandStatus.Completed],
            [CommandStatus.Completed] = []
        };

    public static bool CanTransition(CommandStatus from, CommandStatus to) =>
        Allowed.TryGetValue(from, out var targets) && targets.Contains(to);

    public static void EnsureTransition(CommandStatus from, CommandStatus to)
    {
        if (!CanTransition(from, to))
        {
            throw new InvalidOperationException($"Invalid command state transition: {from} -> {to}.");
        }
    }

    public static bool IsFinalForExecution(CommandStatus status) => status is
        CommandStatus.SucceededLocal or CommandStatus.BusinessFailedLocal or CommandStatus.Expired or
        CommandStatus.Cancelled or CommandStatus.DeadLetter or CommandStatus.ResultPending or CommandStatus.Completed;
}
