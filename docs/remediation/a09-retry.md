# A09 — Command retry budgets, persisted backoff, `002` first-send backfill (bounded slice)

Date: 24.09.2026. Scope: `repo_1c-agent` command retry/service/store + migration `002_retry_budgets.sql`
+ corresponding tests only. A01/A02/A10 semantics preserved. `001_initial.sql` immutable. No changes to
HTTP/ETL/order/duplicateACK. Not the full A09/plan — this is a bounded slice covering (a) the six failing
`CommandExecutionTests` plus the real retry/store defects they exposed, and (b) a bounded migration review
fixing the `002_retry_budgets` first-send backfill defect. `002_retry_budgets.sql` is **new in the A09
slice** (not a pre-existing migration); see "The A09 retry slice vs the accepted 57-test baseline".

## Test-count correction

Full suite is **66 = 30 unit + 36 integration** (60 passed / 6 failed before this fix). The earlier
report incorrectly used the integration count (36) as the total. Per-assembly counts are reported
separately below and in the TRX files.

## Root causes

| # | Test | Cause | Fix |
|---|---|---|---|
| 1 | `Pending_status_hits_lookup_attempt_budget…dead_letter` | Fixture re-drove the command via `SingleReadyAsync()` (ready query excludes terminal rows) instead of verifying the terminal DeadLetter through the DB/result. `ProcessAsync` also never checked the lookup budget **before** the lookup using the persisted count. | Assert DeadLetter→`result_pending` via `StatusAsync(commandId)` + `ResultForAsync(commandId)`; service now gates the lookup on `stored.LookupAttemptCount` before `GetStatusAsync`. |
| 2 | `Lookup_technical_errors_are_bounded_by_lookup_budget` | `SingleResultAsync()` selected the first pending result in a shared per-test DB (wrong/empty row); no lookup-budget check before the lookup. | `ResultForAsync(commandId)`; lookup budget enforced before the lookup → `UNKNOWN_RESULT_LIMIT` dead-letter. |
| 3 | `Success_and_business_results_remain_unchanged` | Two commands shared `ordering_key "order:42"`, so `GetReadyCommandsAsync` blocked the second; `FakeOnec.ExecuteAsync` always returned success (`StatusKind` only affects `GetStatusAsync`). | Distinct ordering keys; new `FakeOnec.ExecuteKind` drives the **POST** outcome (default `Succeeded`, unchanged for other tests). |
| 4 | `Backoff_is_persisted_before_lookup_network_call…` | Backoff and lookup-attempt were persisted **after** `GetStatusAsync` (network) — crash could reset budget/delay. | `RecordLookupAttemptAsync` + `MarkUnknownResultAsync(…, BackoffAt(lookupAttempt,…))` now run **before** `GetStatusAsync` (counters/backoff before I/O). |
| 5 | `First_send_timestamp_is_not_overwritten_on_retries` | `ReadyForAsync`/`SingleResultAsync` over a shared DB picked the wrong/absent row across the retry loop; re-POST returned default success and terminated early. | `ReadyForAsync(commandId)` per pass + `RecoverAsync` to re-resolve; `ExecuteKind = Processing` so the re-POST stays non-terminal and the POST budget bounds it. |
| 6 | `NotFound_after_send_reaches_post_attempt_budget…dead_letter` | Same wrong-row selection + default-success POST; POST budget not re-checked before the re-POST on the resolve path. | `ReadyForAsync`/`ResultForAsync(commandId)`; `ExecuteKind = Processing`; `PostAttemptCount` budget checked at the top of `ExecuteFreshAsync` (which the NotFound branch re-enters). |

## Changes

| Area | File | Change |
|---|---|---|
| Service | `src/ErpOnecAgent.Application/Commands/CommandExecutionService.cs` | Resolve path (`ResolveStatusThenRetryAsync`): enforce **lookup budget** from `stored.LookupAttemptCount` before `GetStatusAsync`; persist the lookup attempt and exponential backoff **before** the network call (crash-safe); on lookup-budget exhaustion emit `UNKNOWN_RESULT_LIMIT` dead-letter with `details.outcomeUnknown=true`. POST budget (`stored.PostAttemptCount`) remains separate and is checked inside `ExecuteFreshAsync` on both the fresh and NotFound-re-POST branches. Explicit `outcomeUnknown` in `SaveErrorAsync` retained. |
| Store | `src/ErpOnecAgent.Infrastructure/Persistence/Sqlite/SqliteAgentStore.Commands.cs` | `UpdateCommandAsync` (used by `MarkUnknownResultAsync`/`ScheduleRetryAsync`) is now **terminal-safe**: `WHERE … status IN ('queued','retry_waiting','executing','unknown_result')`. Never re-opens `completed` (A01/A02 preserved) and never overwrites a resolved local result, so it is safe to persist backoff before a lookup. `GetReadyCommandsAsync` unchanged — terminal rows stay excluded (terminal commands are never made runnable). |
| Tests | `tests/ErpOnecAgent.IntegrationTests/CommandExecutionTests.cs` | Added id-scoped helpers `ReadyForAsync(commandId)` and `ResultForAsync(commandId)` (select by `command_id` from the persisted DB), `StatusAsync(commandId)`; kept `SingleReadyAsync`/`SingleResultAsync` for single-command tests. `MakeCommand` takes an optional `orderingKey`. `FakeOnec` gained `ExecuteKind` (POST outcome) via a shared `ForKind` mapping. Assertions verify terminal state via persisted result/DB, never via the ready query. |

The six-test fix above did **not** change any schema migration. `002_retry_budgets.sql` was created as
part of the A09 retry slice (it is **not** a pre-existing migration) and shipped with the slice in its
original form; its `first_sent_at_utc` backfill defect was found and fixed in the bounded migration
review below (§ "A09 slice — bounded migration review").

## The A09 retry slice vs the accepted 57-test baseline

The accepted baseline at the start of the A09 slice is the A02 final + A10 review green: **57 tests
passed / 0 failed (30 unit + 27 integration)**. Everything below is the A09 retry slice, measured
against that baseline. Two distinct classes of work are kept separate on purpose:

- **Test-fixture defects** — the earlier *six-test fix*: six `CommandExecutionTests` failed because the
  **tests** selected the wrong row/outcome (`SingleReadyAsync`/`SingleResultAsync` over a shared
  per-test DB), drove terminal rows through the ready query, or relied on `FakeOnec`'s silent
  default-success POST. Fixing the fixtures exposed real production gaps (below) which were then fixed.
  No invented causes: the concrete cause per test is recorded in the "Root causes" table above.
- **Production defects** — real code behavior, independent of the fixtures:
  1. *Retry budget / backoff persistence* (the six-fix slice): lookup budget was not checked before the
     lookup using the persisted count; backoff + lookup-attempt were persisted **after** the
     `GetStatusAsync` network call; POST budget was not re-checked on the NotFound re-POST path.
     Fixed in `CommandExecutionService` / `SqliteAgentStore.Commands` (see "Changes" above).
  2. *`002_retry_budgets` first-send backfill* (this migration review): both backfill statements
     excluded `retry_waiting` (and `result_pending`/`completed`) by **status**, so a previously-sent
     `retry_waiting`/`result_pending` command lost its earliest send age (`first_sent_at_utc` → NULL)
     even though `command_attempts` proved it had been sent. Fixed below using evidence of prior send.

### Slice test-count progression

| Run | Unit | Integration | Total | Note |
|---|---|---|---|---|
| Accepted baseline (A02 final + A10) | 30 | 27 | **57 passed / 0 failed** | start of A09 slice |
| A09 six-test fix (source passes, prior to this review) | 30 | 36 | **66 passed / 0 failed** | 9 added: 6 previously-failing corrected + `Migrated_existing_database_…` fixture test |
| A09 migration review — red (current `002`) | 30 | 36 + 7 new: **2 failed / 5 passed** | 2 failed / 69 passed / 71 | 7 `RetryBudgetMigrationTests` added; 2 red = the verified defect |
| A09 migration review — final (fixed `002`) | 30 | 43 | **73 passed / 0 failed** | 66 + 7 new migration tests |

The 66-test source ("current source passes 66 tests") is preserved: all 30 unit and all 36 prior
integration tests still pass unchanged after this review; only the 7 new migration tests were added.

## Semantics preserved

- **Terminal commands are never made runnable.** `GetReadyCommandsAsync` excludes
  `succeeded_local/business_failed_local/expired/cancelled/dead_letter/result_pending/completed`;
  the tests assert DeadLetter→`result_pending` (terminal for execution) through the DB and
  `Assert.Empty(ReadyAsync())`, never by feeding the terminal row back to `ProcessAsync`.
- **Separate POST and lookup budgets** (`post_attempt_count` vs `lookup_attempt_count`), both
  persisted and read per processing.
- **Counters/backoff before I/O.** Lookup-attempt count and `next_attempt_at_utc` are durable
  before `GetStatusAsync`; `MarkExecutingAsync`/attempt persist before `ExecuteAsync` (POST).
- **Explicit `outcomeUnknown`** on budget-exhaustion dead-letter (`UNKNOWN_RESULT_LIMIT` +
  `details.outcomeUnknown=true`, message states manual investigation, not a confirmed business
  failure nor confirmed absence of effect in 1C).
- **A01/A02**: `completed` is never re-opened; first-send evidence (`AttemptCount`/`UnknownResult`)
  and resolve-before-retry unchanged; NotFound re-POSTs the **same** `command_id`.
- **A10** (`OnecODataClient`) untouched.

## A09 slice — bounded migration review (`002_retry_budgets` backfill)

Reviewer-verified defect in `src/ErpOnecAgent.Infrastructure/Persistence/Migrations/002_retry_budgets.sql`:
both `first_sent_at_utc` backfills carried `AND status NOT IN ('queued','retry_waiting','result_pending','completed')`.
This excluded `retry_waiting`/`queued` **irrespective of actual persisted attempts**. A `retry_waiting`
command that had already been sent (A02: `retry_waiting` may follow a prior send) lost its earliest send
age — `first_sent_at_utc` was left NULL — even though `command_attempts` rows proved the send. Never-sent
`queued`/`retry_waiting` must remain NULL; previously-sent rows must retain the **earliest** send age.

Tests were written **first** and run against the then-current `002` for a meaningful runtime red, then
`002` was fixed. `001_initial.sql` is immutable (v1 `schema_migrations.checksum` = SHA-256 of its text)
and was **not** modified.

### Seeded v1 database (real migration path)

`tests/ErpOnecAgent.IntegrationTests/RetryBudgetMigrationTests.cs` seeds an actual v1 DB per test —
immutable `001_initial.sql` + the correct v1 ledger row + `tests/.../Fixtures/v1-populated/populated-v1.sql`
(deterministic timestamps) — then runs `002` exactly as `SqliteMigrator` does (full script text on one
connection in a transaction, recording the SHA-256 checksum). Seeded rows and expectations:

| Row | State | Send evidence | `first_sent_at_utc` expectation |
|---|---|---|---|
| 101 | `queued` | none (0 attempts, `attempt_count=0`) | stays **NULL** |
| 202 | `retry_waiting` | none | stays **NULL** |
| 303 | `retry_waiting` | 3 attempts, earliest `2026-09-10T08:00Z`; `started_at_utc` overwritten `09-16` | earliest attempt `…08:00Z` (≪ `started_at_utc`) |
| 404 | `unknown_result` | 2 attempts, earliest `09-13`; `started_at_utc` `09-18` | earliest attempt `…09-13` |
| 505 | `executing` | `attempt_count=1`, **no** attempt rows | conservative fallback `= started_at_utc` |
| 606 | `unknown_result` | `attempt_count=1`, **no** attempt rows | conservative fallback `= started_at_utc` |
| 707 | `result_pending` (terminal) | 1 attempt, earliest `09-10T09:00Z` | earliest attempt preserved |
| 808 | `completed` (terminal) + result | 1 attempt | preserved untouched (A01/A02) |

Plus `results_outbox` rows for 707 (pending) and 808 (acknowledged) to assert result preservation.

### Fix applied (`002_retry_budgets.sql` only)

Evidence of prior send, **not** broad status exclusion:

- Rows **with** `command_attempts` → `first_sent_at_utc = MIN(command_attempts.started_at_utc)`
  (earliest actual attempt, never `commands.started_at`), gated by `EXISTS(... command_attempts ...)`
  instead of a status blacklist. This covers `retry_waiting`/`queued`/`unknown_result`/`executing` and
  terminal rows alike, whenever an attempt record exists.
- Conservative fallback `= started_at_utc` only for potentially-sent rows (`attempt_count > 0`,
  `started_at_utc IS NOT NULL`) that have **no** attempt rows (`NOT EXISTS(...)`).
- Never-sent `queued`/`retry_waiting` (no attempts, `attempt_count = 0`) satisfy neither predicate → stay NULL.

`post_attempt_count` (= `attempt_count`) and `command_attempts.attempt_kind` backfills are unchanged
(counters/data preserved). Idempotency via `SqliteMigrator` (run twice): second run is a checksum-verified
no-op — 2 `schema_migrations` rows, `applied_at_utc`/checksums stable, all backfilled values unchanged.

### New tests (7) and red→green

| Test | Current `002` (red) | Fixed `002` (green) |
|---|---|---|
| `Sent_retry_waiting_keeps_earliest_send_age_from_attempts` | **FAIL** (`null` vs `…08:00Z`) | PASS |
| `Terminal_rows_keep_first_send_evidence_and_results` | **FAIL** (`null` vs `…09:00Z`) | PASS |
| `Never_sent_queued_and_retry_waiting_stay_null_without_fallback` | PASS | PASS |
| `Sent_unknown_result_keeps_earliest_attempt_older_than_started_at` | PASS | PASS |
| `Executing_and_unknown_result_without_attempts_use_conservative_started_at_fallback` | PASS | PASS |
| `Retry_budget_counters_and_attempt_kinds_are_backfilled` | PASS | PASS |
| `Migrator_is_idempotent_and_checksums_stay_stable_across_second_run` | PASS | PASS |

Red run = 2 failed / 5 passed (the two that encode the reviewer's defect). The 5 green-on-red tests pin
behavior that must be preserved (never-sent NULL, fallback, counters/kind, idempotency/checksum).

## Evidence

| Item | Path |
|---|---|
| Full Release rebuild (0 warn / 0 err) | `local-data/remediation-2026-09-24/a09-retry/six-fixes/build.log` |
| Unit tests log | `local-data/remediation-2026-09-24/a09-retry/six-fixes/tests-unit.log` |
| Integration tests log | `local-data/remediation-2026-09-24/a09-retry/six-fixes/tests-integration.log` |
| Per-assembly TRX (unique prefixes) | `local-data/remediation-2026-09-24/a09-retry/six-fixes/TestResults/tests-20260924T120000Z-unit.trx`, `…-int.trx` |
| Migration review — Release rebuild (0 warn / 0 err) | `local-data/remediation-2026-09-24/a09-retry/migration-review/build.log` |
| Migration review — red run (current `002`) | `local-data/remediation-2026-09-24/a09-retry/migration-review/red-migration-tests.log`, `…/TestResults/red-migration-*.trx` |
| Migration review — green run (fixed `002`) | `local-data/remediation-2026-09-24/a09-retry/migration-review/green-migration-tests.log`, `…/TestResults/green-migration-*.trx` |
| Migration review — final all-tests logs | `…/migration-review/final-tests-unit.log`, `…/migration-review/final-tests-integration.log` |
| Migration review — final per-assembly TRX (unique prefixes) | `…/migration-review/TestResults/final-*-unit.trx`, `…/final-*-int.trx` |

## Test counts

Slice progression (vs the accepted **57** baseline) is in "Slice test-count progression" above. Final for
this migration review (Release):

| Assembly | Total | Passed | Failed |
|---|---|---|---|
| ErpOnecAgent.UnitTests | 30 | 30 | 0 |
| ErpOnecAgent.IntegrationTests | 43 | 43 | 0 |
| **Total** | **73** | **73** | **0** |

73 = 66 (source preserved: 30 unit + 36 integration) + 7 new `RetryBudgetMigrationTests`. The earlier
six-fix slice final was 66 (30 unit + 36 integration); its prior state was 60 passed / 6 failed.

## Remaining gaps / explicitly open

- **No live 1C/ERP verification** — fakes only (`FakeOnec`). Crash-safety of backoff-before-network is
  proven by the pre-call `next_attempt_at_utc`/`lookup_attempt_count` assertions, not by a real kill.
- **Migration-test coverage**: the pre-002 backfill gap is now closed by `RetryBudgetMigrationTests`
  (seeds a real v1 DB and asserts `first_sent_at_utc` / `post_attempt_count` / `attempt_kind` backfill,
  terminal/result preservation, and idempotency). Still no live migration of a production volume DB.
- `MaxResolutionAgeHours` (resolution-age budget) is honored but only covered by the existing
  `Resolution_age_limit_expires…` test.
- **This does not close A09/plan.** Retry **ordering** (`ordering_key` head-of-line across restarts)
  and reconcile/sync strategies remain open.

## Correctness note on the six tests

The reviewer's finding is confirmed and applied: `Pending_status…` (and `Lookup_technical…`) must not
drive a terminal DeadLetter through `SingleReadyAsync`/the ready query. The intended semantics —
verify terminal outcome via the persisted result/database and never make a terminal command
runnable — are now what the tests assert. Assertions were **not** weakened: budget counts
(`lookup_attempt_count`/`post_attempt_count`), `outcomeUnknown`, `first_sent_at_utc` stability, and
the manual-investigation message are still asserted; only the wrong-row/wrong-outcome fixture
selection and `FakeOnec`'s silent default-success POST were corrected.