# A09 — Ordering, atomic claim guard, dead-letter metric (bounded slice)

Date: 24.09.2026. Scope: `repo_1c-agent` command ordering / fresh-execution claim guard / dead-letter
metric + migration `003_ordering_claims.sql` and their tests. **Read-only review in this pass — no
production or test code was changed.** `001_initial.sql` and `002_retry_budgets.sql` are immutable
(unchanged). A01/A02/A06/A10 and the earlier A09 retry slice ([a09-retry.md](a09-retry.md)) semantics
are preserved. This does **not** close A09/plan — it is a bounded slice covering the ordering/metrics
items of `spec_1c-agent/reviews/architecture-audit-2026-09-23.md` A09 (bullets 2 and 3) plus the
fresh-execution claim guard required by the task prompts. Retry budget / lookup semantics were covered
by the earlier slice and are unchanged here.

Source of the requirements for this slice:
`agents/runs/2026-09-24/a09-ordering-metrics-prompt.txt` (initial) and
`agents/runs/2026-09-24/a09-ordering-finish-prompt.txt` (continuation).

## Current state (independent verification)

Root independently ran full Release rebuild + all tests against the **current** source before this
review. Result: **106 / 106 = 39 unit + 67 integration, 0 warnings / 0 errors.**

Evidence (absolute paths, all under
`D:/WORK/CNC_Milling/WORK_CNC/SOFT/1C/1C-agent/repo_1c-agent/local-data/remediation-2026-09-24/a09-ordering-metrics/orchestrator/`):

| Item | Path | Result |
|---|---|---|
| Release build | `orchestrator/build.log` | 0 warn / 0 err |
| Test run log | `orchestrator/test.log` | 67 integration + 39 unit, all passed |
| Integration TRX | `orchestrator/TestResults/review_net10.0_20260924141326.trx` | `Counters total="67" passed="67" failed="0"` |
| Unit TRX | `orchestrator/TestResults/review_net10.0_20260924141350.trx` | `Counters total="39" passed="39" failed="0"` |

Because the root run is authoritative and the code did **not** change in this pass, the expensive
build/test was **not** re-run here. Earlier `dbg*.trx` files in the same directory are **fixture/debug
failures from development** (see "Historical dbg failures" below), not current runtime red.

## Schema: `003_ordering_claims.sql` (exact)

`src/ErpOnecAgent.Infrastructure/Persistence/Migrations/003_ordering_claims.sql` is **new and additive
only**. `001_initial.sql` (checksum 1) and `002_retry_budgets.sql` (checksum 2) are untouched.

```sql
ALTER TABLE commands_inbox ADD COLUMN queue_sequence INTEGER NOT NULL DEFAULT 0;
ALTER TABLE commands_inbox ADD COLUMN exec_claim_owner_id TEXT NULL;
ALTER TABLE commands_inbox ADD COLUMN exec_claim_acquired_at_utc TEXT NULL;

UPDATE commands_inbox
SET queue_sequence = (
    SELECT rn FROM (
        SELECT command_id,
               ROW_NUMBER() OVER (ORDER BY received_at_utc, command_id) AS rn
        FROM commands_inbox
    ) ordered
    WHERE ordered.command_id = commands_inbox.command_id
)
WHERE queue_sequence = 0;
```

Three columns, all on `commands_inbox`:

| Column | Purpose |
|---|---|
| `queue_sequence INTEGER NOT NULL DEFAULT 0` | Durable monotonic admission order (`MAX(queue_sequence)+1` at insert, `SqliteAgentStore.Commands.cs:124-135` `NextQueueSequenceAsync`). Stable tie-break of the total order when two commands share `received_at_utc`. |
| `exec_claim_owner_id TEXT NULL` | Owner identity of one **fresh-execution** claim generation. |
| `exec_claim_acquired_at_utc TEXT NULL` | When that claim was taken (staleness boundary input). |

**Backfill semantics (003):** pre-existing rows get `queue_sequence` deterministically from
`(received_at_utc, command_id)` (ROW_NUMBER) — equal receive times get a stable total order and no row
loses data. `exec_claim_*` is **never fabricated at migration** and stays `NULL` (in-flight claims do
not survive a migration; `RecoverAsync` handles crashed rows at restart). Terminal rows
(`completed`/`result_pending`/results) are never rewritten. Idempotent and checksum-stable when run
twice (migrator no-ops on an applied version, `SqliteMigrator.cs:32-40`).

`SqliteMigrator.CurrentSchemaVersion = 3`. There is **no `004`** — only `001`, `002`, `003` exist
(`src/ErpOnecAgent.Infrastructure/Persistence/Migrations/`).

## Ordering: deterministic per-ordering-key head-of-line

The required total order lives in **one place**, `CommandQueueOrder`
(`src/ErpOnecAgent.Domain/Commands/CommandModels.cs:66-99`):

- `Compare` / `Sort`: `priority DESC`, then `received_at_utc ASC`, then `queue_sequence ASC`.
- `IsEarlier(predecessor, command)`: `received_at_utc <` **or** (`==` **and** `queue_sequence <`).

Both the ready query and the claims use these definitions, so equal `received_at_utc` can never yield
two heads of one ordering key.

`GetReadyCommandsAsync` (`SqliteAgentStore.Commands.cs:140-177`) selects
`status IN ('queued','retry_waiting','unknown_result')`, honors `not_before_utc` / `next_attempt_at_utc`,
and blocks a successor while an earlier predecessor of the same `ordering_key` is still in the queue:

```sql
AND (c.ordering_key IS NULL OR NOT EXISTS (
      SELECT 1 FROM commands_inbox p WHERE p.ordering_key=c.ordering_key
      AND (p.received_at_utc < c.received_at_utc OR (p.received_at_utc = c.received_at_utc AND p.queue_sequence < c.queue_sequence))
      AND p.status NOT IN ('completed','cancelled','expired','dead_letter')))
ORDER BY c.priority DESC,c.received_at_utc,c.queue_sequence LIMIT $limit;
```

## Waiting-for-ERP-ACK semantics (ordering release)

This is the **preserved baseline** behavior, confirmed against checkpoint-86 and re-confirmed here:

- A locally completed predecessor in `result_pending` **still blocks** its successor. Its result is
  persisted but **not yet acknowledged to ERP**, so the ordering key's ERP action is not finished.
- Only `AcknowledgeResultAsync` (→ `status='completed'`, `erp_acknowledged_at_utc`) releases the
  successor. A non-deliverable terminal (`cancelled`/`expired`/`dead_letter`) also releases it (those
  rows leave the queue and are excluded by the `NOT IN (...)` guard).
- `result_pending` is therefore a **waiting-for-ERP-ACK** state and is deliberately **not** in the ready
  query's active set and **not** in the predecessor-release set.

`CommandQueueOrder.IsEarlier`'s doc comment (`CommandModels.cs:81-88`) and the ready-query comment
(`SqliteAgentStore.Commands.cs:143-154`) both state this explicitly and match the implementation. The
`a09-ordering-finish` prompt's concern (a test expecting the successor after `CompleteLocallyAsync`
without ACK) is resolved in favor of **preserving until-ACK** semantics — there is no requirement change
that would release on local completion. The test now ACKs before asserting the successor
(`CommandOrderingClaimGuardTests.Equal_received_time_head_is_released_only_after_the_predecessor_is_acknowledged`).


## Concurrency / ownership / recovery

### Fresh-execution claim (POST path)

`TryAcquireCommandExecutionClaimAsync` (`SqliteAgentStore.Commands.cs:206-229`) takes the durable claim
**before any 1C call** with a single guarded `UPDATE ... RETURNING`:

```sql
UPDATE commands_inbox
SET exec_claim_owner_id=$owner, exec_claim_acquired_at_utc=$now, row_version=row_version+1
WHERE command_id=$id
  AND status IN ('queued','retry_waiting','unknown_result')
  AND (exec_claim_owner_id IS NULL OR exec_claim_acquired_at_utc IS NULL OR exec_claim_acquired_at_utc < $staleBefore)
RETURNING exec_claim_acquired_at_utc;
```

A `NULL` result means "do not execute": the row is terminal or owned by another live pass. Exactly one
claimant wins per command generation, so two stale ready snapshots or two concurrent claimants can never
both execute the same command/key.

- **Ownership token:** `exec_claim_owner_id` identifies exactly one claim generation
  (`ExecutionClaim`, `CommandModels.cs:44`). `ReleaseCommandExecutionClaimAsync`
  (`SqliteAgentStore.Commands.cs:252-259`) releases **only** the claim matching `ownerId`
  (`WHERE ... exec_claim_owner_id=$owner`), so a stale snapshot cannot release a newer claim. The
  scheduling transitions now require either that exact owner or an explicitly unclaimed row; see
  [`scheduling-owner.md`](scheduling-owner.md). `CompleteLocallyAsync` remains state-guarded but is not
  owner-conditioned and is a remaining gap.
- **Executor identity:** `CommandExecutionService` stamps claims with `_executorId`
  (`CommandExecutionService.cs:40`, `CommandExecutionIdentity.NewOwnerId()`). Production passes one id
  per agent run (`CommandExecutionWorker.cs:50`); tests pass explicit ids to model concurrent claimants.
- **Staleness boundary:** `CommandQueueOrder.IsClaimStale(acquiredAtUtc, staleBeforeUtc)` remains an
  explicit store-level takeover boundary for deterministic tests. Production passes
  `DateTimeOffset.MinValue` from `CommandExecutionService`, so a live pass is not taken over by time;
  startup `RecoverAsync` clears claims left by a dead process.

### Lookup claim (resolve-before-retry path)

`ClaimLookupAttemptAsync` (`SqliteAgentStore.Commands.cs:261-288`) is the **accepted atomic lookup
claim** (from the A09 retry slice), unchanged here: counter + backoff schedule + open `attempt_kind='lookup'`
row are committed in ONE transaction **before** the network call. It is scoped to the resolve path and
**does not touch** the fresh-execution claim. Active-state guard
`status IN ('queued','retry_waiting','executing','unknown_result')`; `unknown_result` stays reclaimable
(FR-CMD-012), terminal rows are never re-opened.

### Recovery

`RecoverAsync` (`SqliteAgentStore.cs:18-33`), called at startup by `BootstrapService.StartAsync` after
`InitializeAsync` + `IntegrityCheckAsync`:

```sql
UPDATE commands_inbox SET status='unknown_result', next_attempt_at_utc=$now,
       exec_claim_owner_id=NULL, exec_claim_acquired_at_utc=NULL,
       last_error_code='PROCESS_RESTART', last_error_message='Agent restarted during command execution',
       row_version=row_version+1
WHERE status='executing';
```

A claim left by a crashed/aborted pass is released and the row is re-scheduled as `unknown_result`; a
later pass re-resolves the outcome before any re-POST. Nothing accepted is lost and backoff/schedule
semantics are preserved. (`SingleInstanceLock` (`SingleInstanceLock.cs`) enforces one live process per
`agentId`, so cross-process racing is out of scope; the claim guard covers concurrent passes/threads and
stale in-memory snapshots within the process.)


## Dead-letter metric: before / after ACK

**Before (audit defect A09 bullet 3):** `SqliteAgentStore.Etl.cs` counted `status='dead_letter'`, but
local completion writes `status='result_pending'` and keeps the dead-letter outcome separately in
`result_status`. A real dead-letter result therefore reported **0**.

**After:** `result_status` is the single source of truth. `GetQueueMetricsAsync`
(`SqliteAgentStore.Etl.cs:162-182`):

```sql
(SELECT COUNT(*) FROM commands_inbox WHERE result_status='dead_letter')  -- + etl batch dead letters
(SELECT COUNT(*) FROM commands_inbox WHERE result_status='dead_letter')  -- CommandsDeadLetter (breakdown)
```

- `DeadLetters` = command dead-letters (`result_status='dead_letter'`, covering the `result_pending`
  delivery state and rejected A01 admissions) **plus** ETL batch dead letters.
- `CommandsDeadLetter` = command-only breakdown.

**Inclusion point = local completion (before ACK).** A command is counted the moment its local outcome is
a dead letter (result written with `result_status='dead_letter'` while `status='result_pending'`), i.e.
**before** the ERP result ACK. The count is **unchanged by the later ACK**: `AcknowledgeResultAsync`
(`SqliteAgentStore.Commands.cs:388-396`) moves `status` → `completed` and sets `erp_acknowledged_at_utc`
but preserves `result_status`. So a command is counted **exactly once** — there is no double count across
the ACK boundary. Documented on `QueueMetrics` (`src/ErpOnecAgent.Domain/Agent/AgentModels.cs:6-14`).
Consumed by `HeartbeatWorker` (`HeartbeatWorker.cs:28,36`) into `QueueHeartbeat.DeadLetters`
(`ErpContracts.cs:40`, contract `deadLetters`, `contracts/erp-agent-api.openapi.yaml:159`).

## Actual test coverage

Unit — `tests/ErpOnecAgent.UnitTests/CommandOrderTests.cs` (4):

| Test | Asserts |
|---|---|
| `Total_order_breaks_equal_received_time_ties_by_durable_queue_sequence` | `IsEarlier`/`Sort` tie-break by `queue_sequence` |
| `Total_order_never_reorders_rows_with_different_receive_times` | timestamp dominates sequence |
| `Total_order_sorts_by_priority_desc_then_received_time_then_sequence` | priority DESC → received → sequence |
| `Claim_is_stale_only_after_the_documented_boundary` | `IsClaimStale` exclusive boundary |

Integration — `tests/ErpOnecAgent.IntegrationTests/CommandOrderingClaimGuardTests.cs` (11, real SQLite +
real `SqliteAgentStore`, `FakeOnec` only):

| Test | Asserts |
|---|---|
| `Equal_received_time_same_ordering_key_yields_exactly_one_ready_head` | one head for equal timestamps |
| `Equal_received_time_head_is_released_only_after_the_predecessor_is_acknowledged` | until-ERP-ACK release |
| `Different_ordering_keys_with_equal_received_time_are_both_ready_and_independent` | independent keys |
| `Different_ordering_keys_are_claimed_independently_and_both_execute` | independent claims |
| `Ready_order_is_deterministic_and_stable_across_restarts_for_equal_timestamps` | order stable across restart |
| `Concurrent_claims_on_the_same_command_yield_exactly_one_winner` | concurrent claim → one winner |
| `Stale_ready_snapshot_cannot_claim_release_or_complete_a_newer_claim` | owner-token release/complete guard |
| `Stale_claim_beyond_the_documented_boundary_is_taken_over_by_a_later_pass` | stale takeover |
| `Recover_releases_claims_and_no_two_claims_overlap_at_the_same_key_after_restart` | recovery + no overlap |
| `Dead_letter_metric_counts_from_local_completion_and_once_across_ack` | metric before/after ACK, single count |
| `Success_result_is_not_counted_as_a_dead_letter_and_stays_counted_once_after_ack` | success never counted |

Integration migration — `tests/ErpOnecAgent.IntegrationTests/OrderingClaimsMigrationTests.cs` (5, seeds a
real v2 DB (`001`+`002`+`Fixtures/v2-populated/populated-v2.sql`), applies `003`):

| Test | Asserts |
|---|---|
| `Equal_received_time_rows_get_deterministic_queue_sequence_from_command_id` | backfill order |
| `Equal_received_time_pair_is_ordered_by_command_id_and_stable_across_a_second_run` | stable across 2nd run |
| `Execution_claims_are_null_after_migration` | `exec_claim_*` never fabricated |
| `Terminal_row_keeps_its_state_and_result_and_is_never_rewritten` | non-destructive |
| `Migrator_is_idempotent_and_checksums_stay_stable_across_second_run` | idempotent + checksums |

Broader ordering/metric behavior is also exercised by the preserved suites (all green in the 106 run):
`CommandExecutionTests` (29, incl. `Terminal_result_pending_row_is_not_claimable_and_service_skips_network`,
`Migrated_existing_database_preserves_command_attempts_and_unknown_data`), `SqliteStoreTests` (10),
`CommandIntakeTests` (5), `RetryBudgetMigrationTests` (7), `CommandTests`/`Onec*`/`Etl`/long-poll unit
tests. Total 106 = 39 unit + 67 integration.


## Missing coverage / risks (honest)

1. **No true crash-kill test.** Recovery (`RecoverAsync`) and crash-safety of pre-network persistence are
   proven by pre-call assertions and state inspection, not by a real process kill between commit and HTTP
   write. (Same limitation as the A09 retry slice.)
2. **Stale-takeover can overlap a still-running pass.** `TryAcquireCommandExecutionClaimAsync` re-opens a
   claim purely on the 5-minute staleness clock with **no network fencing / cancellation** of the
   original pass. If a single POST legitimately outlives 5 minutes (hung 1C), a later pass could take the
   claim and both could reach the network. In practice `SingleInstanceLock` (one process) +
   startup `RecoverAsync` make this narrow, and `StaleClaimAge` is deliberately generous, but a robust
   lease+fencing mechanism is **not** implemented. This matches the reviewer note in
   `a09-ordering-review-findings.txt` item 5 and is intentionally left as a bounded-slice limitation.
3. **Per-pass owner propagation is now covered for scheduling transitions.** `CommandExecutionService`
   mints a unique per-pass token, passes it through resolve/POST helpers, and uses the owner-aware
   scheduling overloads. `ReleaseCommandExecutionClaimAsync` is owner-conditioned. The remaining
   owner-fencing gap is `CompleteLocallyAsync`, which is state-guarded but still not owner-conditioned;
   administrative completion and other mutators remain outside this slice. See
   [`scheduling-owner.md`](scheduling-owner.md).
4. **No ordering-key predecessor guard inside the claim UPDATE** (reviewer item 1). The ready query's
   `NOT EXISTS` predecessor guard enforces head-of-line at selection; the claim itself only guards
   active-state + free/stale claim. Within the single-process scheduler this is sufficient (a non-head
   row is never selected), but the claim SQL alone would not stop a direct claim on a non-head row.
5. **Lookup path is not serialized by the fresh-execution claim** (reviewer item 2). Two concurrent
   callers can both enter `ResolveStatusThenRetryAsync` and each `ClaimLookupAttemptAsync` (each gets its
   own attempt row / budget increment). The lookup claim is atomic per attempt but is not an ownership
   claim over the whole pass. In the single-process scheduler, duplicate concurrent lookup passes for one
   command do not occur; multi-pass/multi-instance would need a lookup ownership claim.
6. **No live ERP/1C verification** — `FakeOnec` only. Contract behavior (dead-letter metric on the
   heartbeat, ordering release on real ACK) is asserted at the store/service boundary, not against a real
   ERP endpoint.
7. **Metric scope:** `DeadLetters` intentionally includes rejected A01 admissions
   (`result_status='dead_letter'`) and ETL batch dead letters in the total; only `CommandsDeadLetter` is
   the command-only figure. Consumers wanting "executed-but-unknown" vs "rejected" should use the
   breakdown, not the aggregate.

## Historical dbg failures (fixtures, not current red)

`local-data/.../a09-ordering-metrics/dbg*.trx` are **development-fixture failures**, superseded by the
green 106 run (`orchestrator/TestResults/*.trx`). They are recorded here only so they are not mistaken for
current runtime red:

| File | Failure | Nature |
|---|---|---|
| `dbg.trx` / `dbg2.trx` / `dbg3.trx` | `CommandOrderTests.Claim_is_stale_only_after_the_documented_boundary` (`Assert.False` at lines 60/62/63) | test-fixture expectation about `IsClaimStale` boundary while the boundary definition was being fixed |
| `dbg4.trx` | `CommandOrderingClaimGuardTests.Ready_order_is_deterministic...` (`Assert.Single` empty at line 103) | fixture drove the predecessor through `CompleteLocally` and expected the successor before the ERP ACK; corrected to ACK first (until-ACK semantics preserved) |

All four are green in the current suite. No source change was made in this review pass.

## Evidence

| Item | Path |
|---|---|
| Independent full Release build + all tests (106 = 39 unit + 67 integration) | `local-data/remediation-2026-09-24/a09-ordering-metrics/orchestrator/{build.log,test.log}` |
| Per-assembly TRX | `orchestrator/TestResults/review_net10.0_20260924141326.trx` (integration), `..._141350.trx` (unit) |
| Migration `003` | `src/ErpOnecAgent.Infrastructure/Persistence/Migrations/003_ordering_claims.sql` |
| Populated v2→v3 fixture | `tests/ErpOnecAgent.IntegrationTests/Fixtures/v2-populated/populated-v2.sql` |

## Status

**Bounded slice complete.** Ordering/claim/metrics behavior matches checkpoint-86's preserved
until-ERP-ACK semantics and satisfies the A09 ordering/metrics audit items. `001`/`002` immutable; only
`003` added. **This does not claim A09/plan complete** — retry reconcile/sync strategies, terminal
owner fencing, lookup ownership, and live ERP verification remain open.

