# A02 — Expiry vs unknown-result in command execution

Scope: `repo_1c-agent` code/tests only. A01 preserved. A10 (`OnecODataClient` + `OnecODataClientTests`) untouched. No A09/ETL/contract/plan changes. No schema migration.

## Problem

`CommandExecutionWorker.ProcessAsync` checked `CommandPolicy.IsExpired` before the
`UnknownResult` branch. A command already sent to 1C (document may exist) whose response was
lost became `unknown_result` after restart (`RecoverAsync`), then hit the expiry short-circuit:
reported `expired` with **zero** `GetStatusAsync` lookups and no resolution of the real 1C
result. Risk: ERP retries business action under a new command while the original document exists.
`RetryWaiting` with prior send also skipped status resolution and could re-POST or expire blindly.

FR-CMD-007: expiry applies only before execution starts. FR-CMD-012: after a send, resolve via
status lookup; absent status retries the **same** `command_id`; never mint a new ID.

## Changes

| Area | File | Change |
|---|---|---|
| Orchestration | `src/ErpOnecAgent.Application/Commands/CommandExecutionService.cs` (new) | Production execution path extracted from worker (tests avoid self-contained Service ref). First-send evidence: `AttemptCount > 0` (persisted transactionally in `MarkExecutingAsync` before POST) or `UnknownResult`. Expiry short-circuit only when `!wasSent`. After a possible send: always `GetStatusAsync` first (Succeeded/BusinessError/PayloadConflict preserved; Processing/Technical stays `unknown_result`; NotFound retries same ID via `MarkExecuting`+`ExecuteAsync`). `RetryWaiting` with prior send goes through the same resolve-before-retry path. |
| Hooks | same file, `CommandExecutionHooks` | App-boundary callbacks (no Service dependency in Application): `OnExecutionStarted` after `MarkExecuting`, `OnExecutionSucceeded` **after** `CompleteLocallyAsync`, `OnBusinessFailed` after persist. |
| Worker | `src/ErpOnecAgent.Service/Workers/CommandExecutionWorker.cs` | Delegates to `CommandExecutionService`; hooks restore `COMMAND_EXECUTION_STARTED`, `COMMAND_EXECUTION_SUCCEEDED` + `state.LastOnecSuccessAtUtc`, `COMMAND_BUSINESS_FAILED`. `MaxOperationalAttempts` read via `() => options.Value.MaxOperationalAttempts` per processing (not ctor-captured). Admin handler unchanged (`reload_entity` queue-full `retryable:true` preserved) and invoked after never-sent expiry check. Same ID reused end-to-end. |

No migration: `commands_inbox.attempt_count`, `started_at_utc`, `command_attempts` already record send evidence.

## Review corrections (2026-09-24)

1. Restored observable behavior lost in extraction via `CommandExecutionHooks` (logs + `LastOnecSuccessAtUtc` after successful persist).
2. `MaxOperationalAttempts` now dynamic (`Func<int>`); regression proves limit change is visible mid-lifetime.
3. Reverted unintended `reload_entity` queue-full `retryable` `true`→`false` and fully-qualified `System.Text.Json` churn in the worker.
4. Added tests: expired unknown + NotFound retries same ID; attempt persisted before fake `ExecuteAsync`; success notification after persistence (fresh + status-resolve paths).

## Tests (added first, red captured)

`tests/ErpOnecAgent.IntegrationTests/CommandExecutionTests.cs` — real `SqliteAgentStore` + fake `IOnecCommandClient` through `CommandExecutionService` (production path):

| Test | Expectation |
|---|---|
| `Expired_never_sent_expires_without_lookup_or_post` | `expired`, StatusCalls=0, ExecuteCalls=0 |
| `Expired_unknown_resolves_via_status_lookup_success_without_post` | lookup `succeeded`, StatusCalls=1, ExecuteCalls=0 |
| `Recovered_executing_after_send_looks_up_status_despite_expiry` | RecoverAsync→`unknown_result`, lookup despite expiry, no POST |
| `Pending_status_stays_unknown_and_is_never_reported_expired` | Processing×3 stays `unknown_result`, never `expired`, no POST |
| `Retry_waiting_with_prior_send_resolves_status_before_retry_post` | lookup before retry, success saved, no POST |
| `Unknown_result_with_business_error_preserves_business_semantics_despite_expiry` | lookup `business_error` preserved |
| `NotFound_after_send_retries_same_command_id_without_minting_new_id` | lookup + re-POST same ID (unexpired) |
| `Expired_unknown_with_status_notfound_retries_same_command_id` | lookup + re-POST same ID (expired envelope) |
| `Attempt_is_persisted_before_onec_execute_post` | `attempt_count=1` + 1 `command_attempts` row visible inside `ExecuteAsync` |
| `Success_notification_fires_after_result_is_persisted` | hook after `CompleteLocallyAsync`; `pendingWhenNotified==1` |
| `Success_notification_on_status_resolve_fires_after_result_is_persisted` | same for status-resolve success |
| `Max_operational_attempts_is_read_dynamically_per_processing` | limit change between calls is observed (`UNKNOWN_RESULT_LIMIT`) |

## Evidence

| Item | Path |
|---|---|
| Original red (5 failed / 35 passed / 40) | `local-data/remediation-2026-09-24/a02/red-test.log` |
| Review red (3 failed / 53 passed / 56) | `local-data/remediation-2026-09-24/a02/review/red-test.log` |
| Review green/final | `local-data/remediation-2026-09-24/a02/review/green-test.log`, `final-test.log` |
| Review TRX | `local-data/remediation-2026-09-24/a02/review/TestResults/*.trx` |
| Release build (0 warn / 0 err) | `local-data/remediation-2026-09-24/a02/review/build.log` |

Original red: `StatusCalls expected 1, actual 0` on post-send expiry cases. Review red: two success-notification tests failed before hooks were wired (`pendingWhenNotified -1`); the third failure was an incorrect test expectation for the successful NotFound re-POST, corrected during review. Only the two notification failures are regression evidence.

## Test counts

| Run | Unit | Integration | Total |
|---|---|---|---|
| After A01 (accepted) | 18 | 15 | 33 |
| A02 first red | 18 passed | 5F / 17P / 22 | 5F / 35P / 40 |
| A02 first green | 18 | 22 | 40 |
| Review baseline (incl. A10) | 30 | 22 | 52 |
| Review red (hooks tests first) | 30 passed | 3F / 23P / 26 | 3F / 53P / 56 |
| Final Release | 30 passed | 27 passed | **57 passed / 0 failed** |

Delta vs review baseline: +5 integration tests. A01 and A10 suites remain green.

## Limitations / explicitly open

- **A09 limits/ordering: still open, unchanged.** Status lookups do not increment `AttemptCount`; `UNKNOWN_RESULT_LIMIT` keys off POST attempts only.
- First-send evidence is `AttemptCount > 0` / `UnknownResult` (MarkExecuting is transactional before POST). A crash between commit and HTTP write still takes the lookup path (safe: lookup is read-only; NotFound retries same ID).
- `NotFound` after send re-POSTs the same `command_id` even if `expires_at` has passed (FR-CMD-007 / FR-CMD-012.4). Expired **never-sent** still short-circuits to `expired`.
- Admin commands unchanged; expired never-sent admin still expires before the admin handler runs.
- A10 `OnecODataClient` + tests: reviewer-integrated, not modified here.
- No live 1C/ERP; fakes only. No OData/ETL/mode/ordering changes.
