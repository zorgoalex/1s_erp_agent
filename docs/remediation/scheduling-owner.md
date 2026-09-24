# Scheduling owner fence

Date: 2026-09-24.

This bounded stage-1 slice fences only the scheduling transitions that move an active command to `unknown_result` or `retry_waiting` and release a fresh-execution claim. It does not broaden terminal completion, post/lookup claim acquisition, administrative execution, or the rest of the command state machine.

## Changed methods and semantics

- `SqliteAgentStore.MarkUnknownResultAsync` has attempt-aware and scheduling-only overloads with and without `claimOwner`; `ScheduleRetryAsync` has the same legacy/owner-aware pair. All return `Task<bool>`.
- An owner-aware transition uses one SQLite `UPDATE` guarded by the active states and `exec_claim_owner_id = $claimOwner`. The same update writes status, error, and `next_attempt_at_utc`, clears `exec_claim_owner_id` and `exec_claim_acquired_at_utc`, and increments `row_version` atomically. It returns `true` only when that row changed.
- A legacy transition uses the same active-state guard plus `exec_claim_owner_id IS NULL`; it can update an unclaimed row but can never bypass a live claim. It is the safe boundary used by the worker catch path.
- Wrong, released, replaced, terminal, or nonexistent-owner transitions return `false` and commit no row mutation. They do not write `results_outbox` or `command_attempts`; the `attemptId` overload retains the existing scheduling/audit contract and does not add a second audit mutation.
- `CommandExecutionService` passes the exact per-pass `claimOwner` through `ResolveStatusThenRetryAsync` and `ExecuteFreshAsync`, including the NotFound re-POST path. Ambiguous POST and pending lookup outcomes therefore schedule retry and release their own claim in the same transaction. The `finally` release remains owner-conditioned and is a no-op after the atomic scheduling update.
- Production still does not use time-based live takeover. `TryAcquireCommandExecutionClaimAsync` keeps its explicit store-level stale boundary for deterministic tests; startup recovery remains the production cleanup path.

## Regression coverage

`tests/ErpOnecAgent.IntegrationTests/SchedulingOwnerFencingTests.cs` uses real SQLite, fixed ownership generations A/B, and no sleeps. It covers:

- released generation A cannot mutate generation B's active row, claim, counters, result/outbox, or attempt audit;
- wrong-owner attempt-aware, scheduling-only, and retry transitions are no-ops;
- legacy worker-boundary calls cannot clear B;
- matching owners can schedule and release their own claims without changing audit/counters;
- released-owner calls are no-ops;
- all owner-aware and legacy variants are no-ops after local terminal completion and ERP acknowledgement;
- ordinary executor ambiguous POST and pending lookup responses persist backoff and release the pass claim.

The existing claim test was updated to use the owner-aware transition when it deliberately models an in-flight owner. One retry fixture now uses an unclaimed `ClaimPostAttemptAsync` setup before the intentionally unclaimed-only legacy retry call.

## Evidence

- Runtime red before production fix: 2 focused real-SQLite tests executed, 0 passed, 2 failed. Because the owner-aware overloads did not yet exist, the red used the existing ownerless API against a live B claim as the equivalent wrong-owner/legacy race; it was a runtime failure, not a compilation failure. The failures showed both legacy `MarkUnknownResultAsync` and `ScheduleRetryAsync` clearing owner B and changing scheduling state. TRX: `local-data/remediation-2026-09-24/state-guards/scheduling-owner/TestResults/scheduling-owner-red-20260924T163000Z_net10.0_20260924162409.trx`; log: `local-data/remediation-2026-09-24/state-guards/scheduling-owner/red-targeted-20260924T163000Z.log`.
- Focused green: 10/10 owner-fence tests. TRX: `local-data/remediation-2026-09-24/state-guards/scheduling-owner/TestResults/scheduling-owner-final-targeted-20260924T175000Z_net10.0_20260924163730.trx`.
- Native Windows Release rebuild: 0 warnings, 0 errors. Log: `local-data/remediation-2026-09-24/state-guards/scheduling-owner/final-rebuild-20260924T180000Z.log`.
- Full native Windows Release tests: 39/39 unit and 92/92 integration, 131/131 total. Logs/TRX: `final-tests-20260924T180000Z.log`, `TestResults/scheduling-owner-final-20260924T180000Z_net10.0_20260924163820.trx` (unit), and `TestResults/scheduling-owner-final-20260924T180000Z_net10.0_20260924163800.trx` (integration).
- Touched-file formatting verification passed: `local-data/remediation-2026-09-24/state-guards/scheduling-owner/format-verify-touched-20260924T171000Z.log`. The exact solution-wide CI check still reports only the pre-existing findings in `ErpClientRegistration.cs`, `SqliteAgentStore.cs`, and `ErpLongPollResilienceTests.cs`: `format-verify-solution-20260924T181000Z.log`.
- Migrations `001_initial.sql`, `002_retry_budgets.sql`, and `003_ordering_claims.sql` are byte-identical to checkpoint-121. SHA-256 evidence: `local-data/remediation-2026-09-24/state-guards/scheduling-owner/migration-hashes-20260924T171500Z.log`.

## Remaining gaps

- `CompleteLocallyAsync` is state-guarded but not owner-conditioned; a direct legacy terminal completion can still clear a live claim. This slice intentionally does not change terminal-completion semantics.
- `ClaimPostAttemptAsync` and `ClaimLookupAttemptAsync` remain outside this owner fence, as do administrative completion, result ACK/retry mutators, and broader state-machine/cancel fencing.
- `MarkExecutingAsync` remains a legacy entry point that records a claim; a subsequent ownerless scheduling call is intentionally unclaimed-only and therefore does not act while that claim is held. New execution passes use the owner-aware API.
- There is no live ERP/1C test or process-kill test in this slice; all executor tests use a fake client and real local SQLite.
