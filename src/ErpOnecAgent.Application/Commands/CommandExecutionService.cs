using System.Text.Json;
using ErpOnecAgent.Application.Abstractions;
using ErpOnecAgent.Contracts.OneC;
using ErpOnecAgent.Domain.Commands;

namespace ErpOnecAgent.Application.Commands;

public sealed record CommandExecutionHooks(
    Action<Guid, string, int>? OnExecutionStarted = null,
    Func<Guid, string?, Task>? OnExecutionSucceeded = null,
    Action<Guid, string>? OnBusinessFailed = null);

/// <summary>
/// Stable identity of one command executor (one <c>CommandExecutionService</c> instance). It stamps
/// the durable fresh-execution claim so a stale ready snapshot from any other pass cannot acquire or
/// release that claim. When omitted (legacy construction) a per-instance id is generated; production
/// passes one id per agent run and tests pass explicit ids to model concurrent claimants
/// deterministically. Terminal completion is guarded by the same per-pass owner in the production path.
/// </summary>
public static class CommandExecutionIdentity
{
    public static string NewOwnerId() => Guid.NewGuid().ToString("D");
}

/// <summary>Durable, separate attempt budgets for status-lookup and POST resolution branches with persisted exponential backoff.</summary>
public sealed record CommandExecutionOptions
{
    public int MaxLookupAttempts { get; init; } = 24;
    public int MaxPostAttempts { get; init; } = 12;
    public int MaxResolutionAgeHours { get; init; } = 72;
    public int RetryBaseDelaySeconds { get; init; } = 2;
    public int RetryMaxDelaySeconds { get; init; } = 300;
}

public sealed class CommandExecutionService(IAgentStore store, IOnecCommandClient onec, Func<int> maxOperationalAttempts, CommandExecutionHooks? hooks = null, Func<CommandExecutionOptions>? executionOptions = null, string? executorId = null, Func<DateTimeOffset, DateTimeOffset>? expiryNow = null)
{
    // A07 time: ERP-defined expiry is judged against the later of the local clock and the
    // latest possible ERP clock, so a lagging local clock never executes an expired command.
    private readonly Func<DateTimeOffset, DateTimeOffset> _expiryNow = expiryNow ?? (static now => now);
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);
    private readonly CommandExecutionHooks? _hooks = hooks;
    private readonly Func<CommandExecutionOptions>? _executionOptions = executionOptions;
    private readonly string _executorId = string.IsNullOrWhiteSpace(executorId) ? CommandExecutionIdentity.NewOwnerId() : executorId;

    private CommandExecutionOptions Options => _executionOptions?.Invoke() ?? new CommandExecutionOptions();

    private static readonly Func<int> LegacyLookupAttempts = static () => 24;
    private static readonly Func<int> LegacyPostAttempts = static () => 12;
    private readonly Func<int> _lookupAttemptLimit = executionOptions is null ? Compose(maxOperationalAttempts, LegacyLookupAttempts) : () => PositiveOr(executionOptions().MaxLookupAttempts, LegacyLookupAttempts());
    private readonly Func<int> _postAttemptLimit = executionOptions is null ? Compose(maxOperationalAttempts, LegacyPostAttempts) : () => PositiveOr(executionOptions().MaxPostAttempts, LegacyPostAttempts());

    private static Func<int> Compose(Func<int> legacy, Func<int> fallback) => () =>
    {
        var value = legacy();
        return value > 0 ? value : fallback();
    };

    private static int PositiveOr(int value, int fallback) => value > 0 ? value : fallback;

    private DateTimeOffset BackoffAt(int attempt, DateTimeOffset nowUtc)
    {
        var options = Options;
        return nowUtc + CommandPolicy.BackoffDelay(attempt, TimeSpan.FromSeconds(Math.Max(1, options.RetryBaseDelaySeconds)), TimeSpan.FromSeconds(Math.Max(1, options.RetryMaxDelaySeconds)));
    }

    private const string UnknownResultLimitCode = "UNKNOWN_RESULT_LIMIT";
    private const string UnknownResultLimitMessage = "Unable to resolve command result: the configured lookup/POST attempt budget is exhausted and the outcome is unknown. Manual investigation is needed; this is not a confirmed business failure and not a confirmed absence of effect in 1C.";

    /// <summary>
    /// Time-based live takeover of a held claim is removed: a crashed pass of a DEAD process is
    /// cleared by the startup <c>RecoverAsync</c> (and any pass that aborts in this process releases
    /// its own claim in <c>finally</c>), so a live pass's claim is never stolen mid-flight. The store
    /// keeps the explicit stale boundary parameter for deterministic store-level tests; production
    /// passes this floor, which no live claim can ever predate.
    /// </summary>
    private static readonly DateTimeOffset NoTimeTakeover = DateTimeOffset.MinValue;

    /// <summary>
    /// <paramref name="mayStartNewWork"/> is the A07b admission decision point (default: allow,
    /// keeping legacy callers unrestricted). It is evaluated inside the owned pass before any
    /// new-work side effect (fresh expiry, administrative callback, fresh POST claim) and again
    /// immediately before each durable POST claim — including the resolve-before-retry NotFound
    /// re-POST and a POST that follows an asynchronous administrative callback returning false.
    /// This is a linearization of the admission decision, NOT an atomic mode+network transaction:
    /// a restriction taking effect after the check can still race one in-flight call, whose outcome
    /// then resolves through the normal unknown-result machinery. In-flight calls are never
    /// cancelled by a mode change. Status lookup of already-sent work is not gated by it.
    /// </summary>
    public async Task ProcessAsync(StoredCommand stored, Func<CommandEnvelope, string, CancellationToken, Task<bool>> tryAdministrativeAsync, CancellationToken cancellationToken, Func<bool>? mayStartNewWork = null)
    {
        var admit = mayStartNewWork ?? (static () => true);
        var command = stored.Envelope;
        var wasSent = stored.AttemptCount > 0 || stored.PostAttemptCount > 0 || stored.FirstSentAtUtc is not null || stored.Status is CommandStatus.UnknownResult;

        // A09 claim fencing: ONE unique per-pass token is acquired for the ENTIRE pass (status lookup,
        // administrative execution, and fresh POST alike), so concurrent passes of one command make at
        // most one side effect or network call and a stale future-scheduled snapshot is fenced
        // atomically (due/not-before/current state and the same-key predecessor are all checked in the
        // claim transaction). A NULL claim means "do not call 1C and do not mutate anything" — including
        // administrative side effects and expiry: an unowned pass (terminal row, future schedule, live
        // claim elsewhere) never overwrites via the expiry path either.
        var claimOwner = $"{_executorId}:{CommandExecutionIdentity.NewOwnerId()}";
        var claim = await store.TryAcquireCommandExecutionClaimAsync(
            command.CommandId, claimOwner, DateTimeOffset.UtcNow, NoTimeTakeover, cancellationToken).ConfigureAwait(false);
        if (claim is null) return;
        try
        {
            // Refresh the routing decision from the CURRENT row state returned with the claim: a stale
            // snapshot that still looks fresh must never skip the resolve-before-retry lookup after a
            // prior send (and stale budgets must not re-POST past the limit on a row another pass
            // already worked). The live row decides, not the snapshot.
            var live = stored with
            {
                Status = claim.CurrentStatus,
                AttemptCount = claim.CurrentAttemptCount,
                LookupAttemptCount = claim.CurrentLookupAttemptCount,
                PostAttemptCount = claim.CurrentPostAttemptCount,
                FirstSentAtUtc = claim.CurrentFirstSentAtUtc,
            };
            var sentNow = wasSent
                || live.AttemptCount > 0
                || live.PostAttemptCount > 0
                || live.FirstSentAtUtc is not null
                || live.Status is CommandStatus.UnknownResult;
            if (sentNow)
            {
                if (AdministrativeCommandRouting.IsAdministrativeCommandType(command.CommandType))
                {
                    await SaveAdministrativeUnknownResultAsync(command, claimOwner).ConfigureAwait(false);
                    return;
                }
                await ResolveStatusThenRetryAsync(live, claimOwner, admit, cancellationToken).ConfigureAwait(false);
                return;
            }
            // A07b admission decision point for never-sent work: a denied pass leaves the row
            // untouched — not expired, not dead-lettered, simply ineligible this pass (identical
            // effect to the former loop-level skip). The claim is released by finally below.
            if (!admit()) return;
            // Operational expiry is evaluated INSIDE the owned pass from live persisted send evidence
            // (A02): the claim proved the row is still active AND un-sent right now, so a never-sent
            // expired command still expires — while a stale "never sent" snapshot whose row was
            // actually sent took the resolve path above instead of expiring a potentially-sent
            // command or overwriting a newer terminal result.
            if (CommandPolicy.IsExpired(command, _expiryNow(DateTimeOffset.UtcNow)))
            {
                await SaveErrorAsync(command, claimOwner, CommandStatus.Expired, "COMMAND_EXPIRED", "Command expired before execution.", false, null, cancellationToken).ConfigureAwait(false);
                return;
            }
            try
            {
                if (await tryAdministrativeAsync(command, claimOwner, cancellationToken).ConfigureAwait(false)) return;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception)
            {
                await SaveAdministrativeUnknownResultAsync(command, claimOwner).ConfigureAwait(false);
                throw;
            }
            await ExecuteFreshAsync(live, claimOwner, admit, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            // Release ONLY this pass's own token. An owner-aware scheduling transition may already
            // have cleared it, in which case this is a no-op. Cleanup uses a token independent of the
            // pass cancellation so a cancelled pass never leaks its claim.
            await store.ReleaseCommandExecutionClaimAsync(command.CommandId, claimOwner, CancellationToken.None).ConfigureAwait(false);
        }
    }

    private async Task SaveAdministrativeUnknownResultAsync(CommandEnvelope command, string claimOwner)
    {
        var result = JsonSerializer.Serialize(new
        {
            commandId = command.CommandId,
            status = "dead_letter",
            completedAtUtc = DateTimeOffset.UtcNow,
            error = new
            {
                code = "ADMINISTRATIVE_EXECUTION_UNKNOWN",
                message = "The administrative command outcome is unknown. Manual investigation is required before any retry.",
                retryable = false,
                details = new { outcomeUnknown = true }
            },
            resultVersion = 1
        }, JsonOptions);
        await store.CompleteLocallyAsync(command.CommandId, claimOwner, CommandStatus.DeadLetter, result, null, null, null, CancellationToken.None).ConfigureAwait(false);
    }

    private async Task ResolveStatusThenRetryAsync(StoredCommand stored, string claimOwner, Func<bool> admit, CancellationToken cancellationToken)
    {
        var options = Options;
        var command = stored.Envelope;
        var lookupLimit = _lookupAttemptLimit();
        var ageLimit = TimeSpan.FromHours(Math.Max(1, options.MaxResolutionAgeHours));
        var now = DateTimeOffset.UtcNow;

        if (stored.LookupAttemptCount >= lookupLimit)
        {
            await SaveErrorAsync(command, claimOwner, CommandStatus.DeadLetter, UnknownResultLimitCode, UnknownResultLimitMessage, false, null, cancellationToken).ConfigureAwait(false);
            return;
        }
        if (stored.FirstSentAtUtc is { } firstSent && now - firstSent >= ageLimit)
        {
            await SaveErrorAsync(command, claimOwner, CommandStatus.DeadLetter, UnknownResultLimitCode, UnknownResultLimitMessage, false, null, cancellationToken).ConfigureAwait(false);
            return;
        }

        var lookupAttempt = stored.LookupAttemptCount + 1;
        // ONE atomic durable lookup claim (item 1): counter + backoff schedule + open attempt row are
        // committed in a single transaction BEFORE the network call, so a crash cannot consume a
        // lookup without persisting its backoff/schedule. The claim keeps the attempt UNFINISHED
        // across the network call (item 2) and only applies to active rows (item 3: terminal
        // result_pending/completed never claim a lookup).
        var lookupClaim = await store.ClaimLookupAttemptAsync(
            command.CommandId,
            claimOwner,
            stored.LastErrorCode ?? "STATUS_PENDING",
            stored.LastErrorMessage ?? "1C result is still unknown.",
            BackoffAt(lookupAttempt, now),
            cancellationToken).ConfigureAwait(false);
        // A failed claim means the row left the active state (terminal/completed): do not perform any
        // network call and do not mutate anything.
        if (lookupClaim is null) return;

        var status = await onec.GetStatusAsync(command.CommandId, cancellationToken).ConfigureAwait(false);
        // Record the audited post-network outcome, closing ONLY the exact attempt claimed above
        // (never rewriting historical unknown/crashed lookup rows).
        switch (status.Kind)
        {
            case OnecExecutionKind.Succeeded:
                await store.CompleteAttemptAsync(lookupClaim.AttemptId, CommandStatus.SucceededLocal, status.ErrorCode, status.ErrorMessage, status.HttpStatus, cancellationToken).ConfigureAwait(false);
                await SaveSuccessAsync(command, claimOwner, status, lookupClaim.AttemptId, cancellationToken).ConfigureAwait(false);
                return;
            case OnecExecutionKind.BusinessError:
                await store.CompleteAttemptAsync(lookupClaim.AttemptId, CommandStatus.BusinessFailedLocal, status.ErrorCode ?? "ONEC_BUSINESS_ERROR", status.ErrorMessage, status.HttpStatus, cancellationToken).ConfigureAwait(false);
                await SaveBusinessErrorAsync(command, claimOwner, status, lookupClaim.AttemptId, cancellationToken).ConfigureAwait(false);
                return;
            case OnecExecutionKind.PayloadConflict:
                await store.CompleteAttemptAsync(lookupClaim.AttemptId, CommandStatus.DeadLetter, status.ErrorCode ?? "COMMAND_PAYLOAD_CONFLICT", status.ErrorMessage, status.HttpStatus, cancellationToken).ConfigureAwait(false);
                await SaveErrorAsync(command, claimOwner, CommandStatus.DeadLetter, status.ErrorCode ?? "COMMAND_PAYLOAD_CONFLICT", status.ErrorMessage ?? "1C reported a payload conflict.", false, lookupClaim.AttemptId, cancellationToken).ConfigureAwait(false);
                return;
            case OnecExecutionKind.NotFound:
                // NotFound is NOT a dead-letter: the command is still being worked (the service
                // immediately re-POSTs). Close ONLY this lookup attempt as still-unknown with a
                // distinct NOT_FOUND code (non-terminal), never as dead_letter.
                // A07b: the same-id re-POST is new-work admission. While admission is denied, close
                // this lookup attempt with accurate NOT_FOUND text and reschedule via the existing
                // owner-aware backoff — no POST claim, no POST budget consumed; a later pass looks
                // up again before retrying (resolve-before-retry order preserved).
                if (!admit())
                {
                    const string deferredMessage = "1C has no record of the command; same-id re-POST deferred while command admission is restricted.";
                    await store.CompleteAttemptAsync(lookupClaim.AttemptId, CommandStatus.UnknownResult, "NOT_FOUND", deferredMessage, status.HttpStatus, cancellationToken).ConfigureAwait(false);
                    await store.MarkUnknownResultAsync(command.CommandId, lookupClaim.AttemptId, claimOwner, "NOT_FOUND", deferredMessage, BackoffAt(lookupAttempt, DateTimeOffset.UtcNow), cancellationToken).ConfigureAwait(false);
                    return;
                }
                await store.CompleteAttemptAsync(lookupClaim.AttemptId, CommandStatus.UnknownResult, "NOT_FOUND", "1C has no record of the command; re-POSTing the same id.", status.HttpStatus, cancellationToken).ConfigureAwait(false);
                await ExecuteFreshAsync(stored, claimOwner, admit, cancellationToken).ConfigureAwait(false);
                return;
            default:
                if (lookupAttempt >= lookupLimit)
                {
                    await store.CompleteAttemptAsync(lookupClaim.AttemptId, CommandStatus.DeadLetter, UnknownResultLimitCode, UnknownResultLimitMessage, status.HttpStatus, cancellationToken).ConfigureAwait(false);
                    await SaveErrorAsync(command, claimOwner, CommandStatus.DeadLetter, UnknownResultLimitCode, UnknownResultLimitMessage, false, lookupClaim.AttemptId, cancellationToken).ConfigureAwait(false);
                    return;
                }
                // Unknown/pending result: the counter and backoff were already claimed before the
                // network call; close this attempt as still-unknown and retry later.
                await store.CompleteAttemptAsync(lookupClaim.AttemptId, CommandStatus.UnknownResult, status.ErrorCode ?? "STATUS_PENDING", status.ErrorMessage, status.HttpStatus, cancellationToken).ConfigureAwait(false);
                await store.MarkUnknownResultAsync(command.CommandId, lookupClaim.AttemptId, claimOwner, status.ErrorCode ?? "STATUS_PENDING", status.ErrorMessage ?? "1C result is still unknown.", BackoffAt(lookupAttempt, DateTimeOffset.UtcNow), cancellationToken).ConfigureAwait(false);
                return;
        }
    }

    /// <summary>
    /// Fresh POST under the per-pass claim already held by <see cref="ProcessAsync"/> (and for the
    /// resolve-before-retry NotFound re-POST of the same command id). The atomic claim guard lives in
    /// ProcessAsync so two stale ready snapshots or two concurrent claimants can never both execute
    /// the same command/key; if the claim cannot be held no code here ever reaches the network. A NULL
    /// post claim means the row left the active state: no network call, and ProcessAsync's finally
    /// releases only this pass's own token.
    /// </summary>
    private async Task ExecuteFreshAsync(StoredCommand stored, string claimOwner, Func<bool> admit, CancellationToken cancellationToken)
    {
        var command = stored.Envelope;
        var postLimit = _postAttemptLimit();
        if (stored.PostAttemptCount >= postLimit)
        {
            await SaveErrorAsync(command, claimOwner, CommandStatus.DeadLetter, UnknownResultLimitCode, UnknownResultLimitMessage, false, null, cancellationToken).ConfigureAwait(false);
            return;
        }
        // A07b admission decision point, re-evaluated immediately before the durable POST claim:
        // the check must precede ClaimPostAttemptAsync because that claim itself stamps
        // first_sent_at_utc/post_attempt_count send evidence before the network call — a denial
        // after it would fabricate send evidence for a call never made. This also covers a POST
        // reached after an asynchronous administrative callback returned false: a restriction that
        // landed during the callback still denies here. Denied: leave the row untouched under this
        // pass's claim (released by ProcessAsync's finally).
        if (!admit()) return;
        var postClaim = await store.ClaimPostAttemptAsync(command.CommandId, claimOwner, cancellationToken).ConfigureAwait(false);
        if (postClaim is null) return;
        if (_hooks?.OnExecutionStarted is { } onExecutionStarted) onExecutionStarted(command.CommandId, command.CommandType, postClaim.OperationalAttemptCount);
        var execution = await onec.ExecuteAsync(command, cancellationToken).ConfigureAwait(false);
        switch (execution.Kind)
        {
            case OnecExecutionKind.Succeeded: await SaveSuccessAsync(command, claimOwner, execution, postClaim.AttemptId, cancellationToken).ConfigureAwait(false); break;
            case OnecExecutionKind.BusinessError: await SaveBusinessErrorAsync(command, claimOwner, execution, postClaim.AttemptId, cancellationToken).ConfigureAwait(false); break;
            case OnecExecutionKind.PayloadConflict: await SaveErrorAsync(command, claimOwner, CommandStatus.DeadLetter, execution.ErrorCode ?? "COMMAND_PAYLOAD_CONFLICT", execution.ErrorMessage ?? "1C reported a payload conflict.", false, postClaim.AttemptId, cancellationToken).ConfigureAwait(false); break;
            default:
                // Ambiguous POST outcome (Processing / TechnicalError): the result is still unknown, so
                // close THIS POST attempt as still-unknown (not left dangling-unfinished) and schedule
                // the resolve-before-retry path. A later outcome never rewrites this row.
                await store.CompleteAttemptAsync(postClaim.AttemptId, CommandStatus.UnknownResult, execution.ErrorCode ?? "UNKNOWN_RESULT", execution.ErrorMessage, execution.HttpStatus, cancellationToken).ConfigureAwait(false);
                await store.MarkUnknownResultAsync(command.CommandId, postClaim.AttemptId, claimOwner, execution.ErrorCode ?? "UNKNOWN_RESULT", execution.ErrorMessage ?? "1C execution result is unknown.", BackoffAt(postClaim.OperationalAttemptCount, DateTimeOffset.UtcNow), cancellationToken).ConfigureAwait(false);
                break;
        }
    }

    private async Task SaveSuccessAsync(CommandEnvelope command, string claimOwner, OnecExecutionResult execution, Guid? resolvedAttemptId, CancellationToken cancellationToken)
    {
        var response = execution.Response ?? throw new InvalidDataException("Successful 1C response has no body.");
        var result = JsonSerializer.Serialize(new { commandId = command.CommandId, status = "succeeded", completedAtUtc = DateTimeOffset.UtcNow, document = response.Document, warnings = response.Warnings ?? [], resultVersion = response.ResultVersion }, JsonOptions);
        var externalRef = GetString(response.Document, "ref"); var externalNumber = GetString(response.Document, "number");
        var changed = await store.CompleteLocallyAsync(command.CommandId, claimOwner, CommandStatus.SucceededLocal, result, externalRef, externalNumber, resolvedAttemptId, cancellationToken).ConfigureAwait(false);
        if (changed && _hooks?.OnExecutionSucceeded is { } onExecutionSucceeded) await onExecutionSucceeded(command.CommandId, externalRef).ConfigureAwait(false);
    }

    private Task SaveBusinessErrorAsync(CommandEnvelope command, string claimOwner, OnecExecutionResult execution, Guid? resolvedAttemptId, CancellationToken cancellationToken) =>
        SaveErrorAsync(command, claimOwner, CommandStatus.BusinessFailedLocal, execution.ErrorCode ?? execution.Response?.Error?.Code ?? "ONEC_BUSINESS_ERROR", execution.ErrorMessage ?? execution.Response?.Error?.Message ?? "1C rejected the command.", false, resolvedAttemptId, cancellationToken);

    private async Task SaveErrorAsync(CommandEnvelope command, string claimOwner, CommandStatus status, string code, string message, bool retryable, Guid? resolvedAttemptId, CancellationToken cancellationToken)
    {
        var details = string.Equals(code, UnknownResultLimitCode, StringComparison.Ordinal)
            ? new { outcomeUnknown = true }
            : (object)new { };
        var result = JsonSerializer.Serialize(new { commandId = command.CommandId, status = status == CommandStatus.Expired ? "expired" : status == CommandStatus.DeadLetter ? "dead_letter" : "business_error", completedAtUtc = DateTimeOffset.UtcNow, error = new { code, message, retryable, details }, resultVersion = 1 }, JsonOptions);
        var changed = await store.CompleteLocallyAsync(command.CommandId, claimOwner, status, result, null, null, resolvedAttemptId, cancellationToken).ConfigureAwait(false);
        if (changed && _hooks?.OnBusinessFailed is { } onBusinessFailed) onBusinessFailed(command.CommandId, code);
    }

    private static string? GetString(JsonElement? document, string property)
    {
        if (document is null || document.Value.ValueKind != JsonValueKind.Object) return null;
        foreach (var item in document.Value.EnumerateObject()) if (string.Equals(item.Name, property, StringComparison.OrdinalIgnoreCase)) return item.Value.ToString();
        return null;
    }
}
