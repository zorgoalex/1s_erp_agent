# Attempt and completion owner fencing

Date: 2026-09-24.

## Scope

This bounded stage-1 slice extends the durable per-pass claim fence to POST attempt acquisition, lookup attempt acquisition, and local terminal completion. Migrations `001_initial.sql`, `002_retry_budgets.sql`, and `003_ordering_claims.sql` are unchanged.

## Changed APIs and semantics

- `IAgentStore` and `SqliteAgentStore` now expose owner-aware overloads of `ClaimPostAttemptAsync`, `ClaimLookupAttemptAsync`, and `CompleteLocallyAsync`, in addition to the legacy overloads.
- Each owner-aware mutation uses one SQLite update guarded by the active/terminal state and `exec_claim_owner_id = $claimOwner`. The owner check and counter/status/result mutation commit atomically. A wrong, released, replaced, or terminal owner returns `null`/`false` without changing counters, attempt audit, result outbox, or row version.
- Ownerless attempt and completion overloads require `exec_claim_owner_id IS NULL`; they remain valid for unclaimed rows but cannot bypass a live claim. Blank or null explicit owner tokens are rejected before any database work.
- `CompleteLocallyAsync` retains the active-to-`result_pending` state guard and atomic result/outbox/attempt update. Its exact attempt close also predicates `command_id`, so a foreign `resolvedAttemptId` cannot close another command's audit row. Acknowledged and other terminal rows remain immutable.
- `CommandExecutionService.ProcessAsync` passes its exact acquired per-pass owner through lookup, POST, scheduling, and completion calls, including the lookup `NotFound` re-POST. A refused attempt claim returns before POST/GET; a refused completion does not fire success or business-failure hooks.
- `MarkExecutingAsync` generates its own owner, uses the owner-aware POST claim, and uses the no-time-takeover boundary. A successful legacy call keeps its generated claim until the existing recovery/lifecycle boundary; a failed POST claim releases that generated owner. It is retained for compatibility and is not the active production execution path.

## Regression coverage

`tests/ErpOnecAgent.IntegrationTests/AttemptCompletionOwnerFencingTests.cs` uses real migrated SQLite and covers owner A/B replacement and release, current-owner POST/lookup/completion success, legacy refusal, counter/audit/outbox preservation, token validation, terminal/acknowledged immutability, foreign attempt identity, `MarkExecuting` takeover and failure cleanup, and service POST/GET/completion owner-loss races. Existing A01/A02/A06, ordering, recovery, migration, retry, expiry, and normal POST/lookup/NotFound tests remain in the suite.

## Evidence

- Runtime RED before the production fix: 4/4 focused tests executed, 0 passed, 4 failed. The unchanged ownerless paths mutated live claims and the service sent POST after owner replacement. Evidence: `local-data/remediation-2026-09-24/state-guards/attempt-completion-owner/red-targeted-20260924T170000Z.log` and `TestResults/attempt-completion-owner-red-20260924T170000Z_net10.0_20260924165340.trx`.
- Focused GREEN: 16/16 passed. Evidence: `local-data/remediation-2026-09-24/state-guards/attempt-completion-owner/focused-final-20260924T190000Z.log` and `TestResults/attempt-completion-owner-focused-final-20260924T190000Z_net10.0_20260924170735.trx`.
- Native Windows Release Rebuild: 0 warnings, 0 errors. Evidence: `final-rebuild-20260924T190000Z.log`.
- Full native Windows Release tests: 39/39 unit and 113/113 integration, 152 total. Evidence: `final-unit-20260924T190000Z.log` with `TestResults/attempt-completion-owner-final-unit-20260924T190000Z_net10.0_20260924170801.trx`, and `final-integration-20260924T190000Z.log` with `TestResults/attempt-completion-owner-final-integration-20260924T190000Z_net10.0_20260924170808.trx`.
- Touched-file format verification passed: `format-verify-touched-20260924T184000Z.log`. A solution-wide check still reports only pre-existing findings in `ErpClientRegistration.cs` and `ErpLongPollResilienceTests.cs`.
- No migration file was modified. SHA-256 comparison with checkpoint-136 is recorded in `migration-hashes-20260924T183500Z.log`; `001_initial.sql`, `002_retry_budgets.sql`, and `003_ordering_claims.sql` are byte-identical.

## Remaining constraints

- Administrative commands still execute before claim acquisition. Their bounded administrative side effect can occur before a later terminal no-op; moving that protocol boundary is deferred.
- Result ACK/retry mutators, ETL, cancellation fencing, and broader state-machine fencing are outside this slice.
- Production still uses startup recovery rather than live age-based takeover. Store-level stale-boundary tests remain available for deterministic recovery scenarios.
- The legacy `MarkExecutingAsync` count-only compatibility surface does not return its generated owner; callers that need to complete or release that claim must use the explicit owner-aware store APIs or the normal `CommandExecutionService` lifecycle.

## Independent reviewer validation

Reviewer strengthened the live-claim regression to a claim acquired ten minutes earlier (past the former five-minute takeover threshold), and verified reacquisition after an injected POST-write failure. Full native Release Rebuild:0warnings/errors; independent39unit+113integration=152/152. Evidence: `local-data/remediation-2026-09-24/state-guards/attempt-completion-owner/independent-review/`.

One prior owner-capture test intentionally releases its owner during the simulated network call; its expected result is now unknown_result/nooutbox, since the late success no longer owns the row. This is an intentional stronger ownership assertion, not removal of the scenario.
