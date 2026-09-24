# Durable ETL design — A03 acceptance, A04 resume/spool, A05 completion

Date: 2026-09-24, revision 2 after mandatory root review.
Baseline: isolated worktree at checkpoint258-equivalent code.
Status: **design proposal only — not implemented, not tested, not externally agreed.**
No code, schema, tests, or packages were changed for this document. Local implementation of
[PROPOSED] items is authorized by `contract-decisions.md` §0; no item here is claimed as an
externally accepted contract.

Sources: `spec_1c-agent/plans/remediation-plan-2026-09-23.md` (RP), `spec_1c-agent/starter-pack/
TZ_Local_1C_Agent_CSharp_Windows_v1.0.md` (TZ), `docs/architecture-audit-2026-09-23.md` (AA),
`docs/remediation/contract-decisions.md` (CD), and the current code cited inline.
Line numbers are 1-based in this worktree.

---

## 1. Verified current state (defects this design must close)

Confirmed against current source:

- `EtlTrigger` is a bounded channel (capacity 16, `DropWrite`, `SingleReader`)
  (`src/ErpOnecAgent.Service/Runtime/EtlTrigger.cs:9-17`). With `DropWrite`, `TryWrite`
  commonly returns `true` while the item is discarded, so the `ETL_TRIGGER_QUEUE_FULL`
  rejection branch in `CommandExecutionWorker.cs:87` is *not reliably reached*; the actual
  defect is `accepted=true` persisted for a request that was silently dropped.
- Administrative ETL commands (`start_full_sync`, `reload_entity`, `reconcile_keys`,
  `reconcile_totals`) write only to that channel, then persist a `succeeded` result in a
  separate operation (`CommandExecutionWorker.cs:85-113`). No durable job exists at
  result-commit time.
- `OnecEtlWorker` races `Task.Delay` against an outstanding `ReadAsync`
  (`OnecEtlWorker.cs:32-35`); the losing reader is never cancelled and silently consumes a
  later manual request. Pause/disabled/spool checks run *after* consumption and drop the
  request (`OnecEtlWorker.cs:37-38`). Every trigger creates a new run GUID; there is no
  resume (`OnecEtlWorker.cs:52-55`). The extraction cursor `from` is in-memory only
  (`OnecEtlWorker.cs:65`); `watermarks.extracting_cursor_json` has no writer.
- `CreateEtlRunAsync` hardcodes status `running`
  (`SqliteAgentStore.Etl.cs:14`); `pending`/`paused`/`completing`/`partial_success`/
  `cancelled` exist only in the enum (`EtlModels.cs:5`). No `etl_jobs`/job table exists in
  migrations 001–004.
- Batch dispatch claims by status only — no owner token, no affected-row check
  (`SqliteAgentStore.Etl.cs:42-60`); retry and ACK are guarded only by `status='uploading'`
  (lines 63-84). `DeadLetter` is never written by production code.
- Run completion is three separate operations: ERP complete → per-entity
  `CommitWatermarkAsync` → `CompleteEtlRunAsync`
  (`EtlBatchUploadWorker.cs:24-35`). `CommitWatermarkAsync` overwrites unconditionally
  (`SqliteAgentStore.Etl.cs:144-150`); `CompleteEtlRunAsync` is unguarded (lines 153-160).
  ACK `status` is not validated (`EtlBatchUploadWorker.cs:47-49` vs.
  `contracts/erp-agent-api.openapi.yaml:191-197`, `status` const `acknowledged`).
- `GetRunsReadyToCompleteAsync` requires every batch row `acknowledged` and rebuilds
  watermarks from those rows (`SqliteAgentStore.Etl.cs:107-132`), while retention deletes
  `acknowledged` rows by age alone (`MaintenanceWorker.cs:28-32`,
  `SqliteAgentStore.Etl.cs:87-105`). A deleted batch both blocks and starves completion (A05).
- `RecoverAsync` resets commands/outbox/`uploading` batches only
  (`SqliteAgentStore.cs:16-30`). No run resume, no job recovery, no ready-file/DB
  reconciliation. `FileSpoolStore` moves `.tmp`→`ready` before `RegisterBatchAsync`
  (`FileSpoolStore.cs:43`, `OnecEtlWorker.cs:91-95`); startup only quarantines `*.tmp`
  (`FileSpoolStore.cs:70-79`, `BootstrapService.cs:38`).
- Whole-run `catch` marks the run `failed` on any entity error — the current as-built
  default is effectively `fail_run` (`OnecEtlWorker.cs:83-86`).
- Administrative quarantine (`ADMINISTRATIVE_EXECUTION_UNKNOWN` dead-letter, no callback
  replay) is the accepted convention for uncertain admin side effects
  (`docs/remediation/administrative-claim.md:5-17,62-72`).
- Schema is version 4 (`SqliteMigrator.cs:8`); migrations are embedded `*.sql`, sorted by
  name, SHA-256-verified against `schema_migrations`, applied one transaction each.
- Normative rules: SQLite is the source of truth, Channels only accelerate (TZ §8.4);
  watermark commits only after window read + all batches stored + uploaded + batch ACK +
  ERP run-complete ACK (TZ FR-ETL-005); `.tmp`→atomic rename (FR-ETL-010); batch states
  include `creating`/`dead_letter` (TZ §14.2); jobs bind to `commandId`, acceptance atomic
  (RP §7); run lifecycle pending→…→terminal (TZ §14.1, CD-ETL-3).

**Honesty note carried through this document:** a fixed upper timestamp is *not* a source
snapshot (RP §7, CD-P-3). Deletes and updates can occur in the source during any reread, so
no step below claims gap-free progress, no-loss rereads, or regression-free commits. Where
evidence is insufficient, the design **blocks and exposes manual recovery** instead of
advancing.

---

## 2. Durable model (suggested schema packaging)

Three additive migrations are suggested for reviewability, but **atomicity governs, not the
file split** — if safe staging requires merging or reordering, merge; do not ship a weaker
boundary just to keep the numbering. Nothing touches 001–004.

| Stage | Suggested migration | Content |
|---|---|---|
| Storage foundation (A03 core) | `005_durable_etl_jobs.sql` | `etl_jobs`, `etl_entity_leases`; additive `etl_runs` columns |
| Resume/spool (A04) | `006_etl_run_checkpoints.sql` | `etl_run_entities`; `etl_batches` intent/`batch_no` columns |
| Completion (A05) | `007_etl_completion.sql` | batch owner/dead-letter columns; run completion claim/finalize columns |

### `etl_jobs` — one row per accepted request

```text
job_id TEXT PRIMARY KEY
command_id TEXT NULL UNIQUE          -- NULL for scheduled; one job per command ever
schedule_key TEXT NULL               -- 'incremental' etc.; one active per key
run_id TEXT NOT NULL UNIQUE          -- run identity fixed at acceptance
mode TEXT NOT NULL
entities_json TEXT NOT NULL          -- frozen [{code, definition, configVersion}] — §5
status TEXT NOT NULL                 -- pending|deferred|running|blocked|finished|cancelled
deferral_code TEXT NULL              -- paused|disabled|backpressure|spool_full|busy_entity|unsupported_mode
deferral_message TEXT NULL
command_payload_hash TEXT NULL       -- immutable acceptance evidence, §6
acceptance_result_json TEXT NULL     -- exact result for replay independent of inbox row
available_at_utc TEXT NOT NULL
claim_owner_id TEXT NULL
claim_acquired_at_utc TEXT NULL
attempt_count INTEGER NOT NULL DEFAULT 0
last_error TEXT NULL
created_at_utc / updated_at_utc / row_version — project conventions
CHECK on status; dispatch index (status, available_at_utc, created_at_utc, job_id)
FK run_id → etl_runs(run_id); NO FK on command_id (see §6 evidence rules)
```

### `etl_entity_leases` — durable per-entity ownership (review point 1)

Single-worker extraction does **not** serialize an entity through upload/ERP-complete/local
watermark commit: a run can finish extraction and sit in `uploading`/`completing` while a
later job already re-reads the same entity. Ownership must therefore be persisted and held
**from first window capture through finalize or manual resolution**, regardless of whether
parallel extraction ever exists:

```text
entity_name TEXT PRIMARY KEY         -- at most one active owner per entity
run_id TEXT NOT NULL
job_id TEXT NOT NULL
acquired_at_utc TEXT NOT NULL
released_at_utc TEXT NULL            -- NULL = active lease
release_reason TEXT NULL             -- finalized|failed|manual_release|superseded
```

- Acquired for **every entity of the job inside the job-claim transaction** (the same tx
  that flips job/run to `running` and persists resolved bounds). Any conflict → claim
  fails, job stays `deferred` with `deferral_code='busy_entity'`. Overlapping entity sets
  serialize correctly: job {X,Y} holds X,Y; job {Y,Z} defers until Y releases.
- Released **only** in the finalize transaction (success/partial per-entity), the
  run-failure transaction, or an explicit manual-resolution path — never by cleanup.
- Restart recovery clears *job/batch/completion claim owner tokens* (dead-process claims)
  but does **not** release entity leases: a `running`/`uploading`/`completing` run still
  owns its entities until it resolves. This is what prevents a competing window commit
  (RP §7) across restarts.
- Regressions required: overlapping entity sets; two sequential jobs where the first is
  still `uploading` when the second is claimed; out-of-order completion across different
  entities; lease held through finalize and released exactly once; lease surviving restart
  while its run is resumable/blocked.

### `etl_runs` additive columns

005: `configuration_version`, `resolved_entities_json` (per-entity resolved
`watermark_from`/fixed bound, written once at first claim), `updated_at_utc`,
`row_version`.
007: `completion_attempt_count`, `next_completion_attempt_at_utc`,
`completion_claim_owner_id`, `completion_claim_acquired_at_utc`,
`completion_acknowledged_at_utc`, `finalize_conflict_code`/`finalize_conflict_message`.

Status values `pending|completing|partial_success|blocked` are new writes only; `status`
is TEXT with no CHECK.

### `etl_run_entities` (A04)

```text
PRIMARY KEY(run_id, entity_name)
entity_definition_json TEXT NOT NULL     -- frozen effective mapping at materialization
sync_mode / status                       -- pending|extracting|done|failed|blocked
watermark_from_json TEXT NULL            -- resolved query start, fixed once
snapshot_upper_bound_json TEXT NULL      -- fixed window end (bounded read; NOT a source snapshot)
expected_base_cursor_json TEXT NULL      -- committed watermark observed at claim; CAS base, §7
extracting_cursor_json TEXT NULL         -- advances only in the batch-registration tx
final_watermark_json TEXT NULL           -- terminal per-entity cursor; survives batch deletion
expected_batch_count INTEGER NULL        -- batches this entity must have acknowledged
rows_read / batches_created / batches_acknowledged / last_error / row_version
FK run_id → etl_runs
```

### `etl_batches` additive

006: `batch_no` (ordinal per run+entity; deterministic backfill
`ROW_NUMBER() OVER (PARTITION BY run_id,entity_name ORDER BY created_at_utc,batch_id)`),
intent metadata columns populated at `creating` (see §7.1: `expected_row_count`,
`expected_watermark_from_json`, `expected_watermark_to_json`, `expected_file_path`).
007: `claim_owner_id`, `claim_acquired_at_utc`, `dead_lettered_at_utc`,
`quarantine_reason`, `row_version`.

---

## 3. Stage A — storage foundation (independently reviewable)

The schema and the new transactional store APIs (`AcceptEtlJobAndCompleteCommandAsync`,
`TryClaimEtlJobAsync`, `BeginBatchIntentAsync`, `FinalizeBatchAndCheckpointAsync`,
owner-fenced batch ops, `TryClaimRunCompletionAsync`, `FinalizeEtlRunAsync`,
`ReconcileEtlSpoolAsync`, extended `RecoverAsync`) can be implemented and reviewed
**without switching service acceptance**: admin commands keep writing the RAM trigger and
current result path while the durable path is dark. Cutover is Stage B and is gated (§8).

## 4. Stage B — acceptance cutover + notification-only dispatch

### 4.1 Atomic acceptance

`AcceptEtlJobAndCompleteCommandAsync` — **one transaction**:

1. Same guard as owner-aware `CompleteLocallyAsync` (`SqliteAgentStore.Commands.cs:507-542`).
2. `INSERT etl_runs(status='pending')` + `INSERT etl_jobs(status='pending', command_id,
   run_id, entities_json, command_payload_hash, acceptance_result_json)`.
3. Command → `result_pending` + `result_json` (`data:{accepted:true, runId, mode}`) +
   `results_outbox` pending + close attempt — same tx.
4. Duplicate `command_id`: read back `job.run_id`, write the result with that `runId`,
   never a second job (CD-ETL-1).

Validation stays **before** acceptance (unknown type, missing `payload.entity`, entity not
present/enabled in effective config) — legitimate rejection. After acceptance there is no
rejection path, only deferral (CD-ETL-2). The `ETL_TRIGGER_QUEUE_FULL` branch is removed;
it was effectively unreachable anyway under `DropWrite` (§1).

**Administrative-quarantine precedence (unchanged, mandatory):** the acceptance tx is the
only producer of a proven job. `ADMINISTRATIVE_EXECUTION_UNKNOWN` handling must consult
`etl_jobs` first; a proven job is never deleted/rewritten by quarantine and is reported
via its stored `acceptance_result_json`. No quarantine path issues DELETE/UPDATE on
`etl_jobs`, `etl_entity_leases`, or their runs.

### 4.2 Channel: notification only, no accumulated readers (review point 3)

- After the acceptance tx commits, the handler calls `TryWrite` as a pure wake-up and
  ignores the result. The worker discards payload content; SQLite is the work source.
- The losing-reader race must be **resolved, not tolerated**: either
  (a) link a `CancellationTokenSource` per loop iteration; when the delay wins,
  `Cancel()` it and **await the `ReadAsync` task to completion** (swallowing
  `OperationCanceledException`) so no pending reader survives the iteration; or
  (b) replace the read with a bounded wait primitive (`WaitToReadAsync` with a bounded
  delay, or `TryRead` drain on each poll) so at most one bounded wait exists and no read
  can linger past its iteration.
  Recommended: (a), smallest diff from the current shape. Regression: schedule tick wins
  while a manual job arrives in the same window → the reader is cancelled+awaited, the
  manual job is claimed from SQLite on the next iteration, and no reader accumulates
  across iterations.

### 4.3 Eligibility, deferral, reconcile honesty (review point 8)

- `state.Snapshot.CanExtract`, spool headroom, and **entity-lease availability** are
  evaluated before claiming; blocked work stays `pending`/`deferred` with an explicit
  `deferral_code`. Timer wake re-evaluates on a bounded interval — no busy loop.
- Scheduled incremental: transactional check-insert of a `schedule_key='incremental'`
  job on timer wake, then normal claim. Nothing durable is lost if the process dies
  between tick and insert — the schedule is periodic.
- `reconcile_keys`/`reconcile_totals`: no algorithm is invented. The job persists the
  mode faithfully, but the dispatcher **defers them with
  `deferral_code='unsupported_mode'`** until real reconcile semantics land (stage 5/A10),
  rather than silently running a full import under a reconcile label. The admin command
  still gets an honest accepted runId (accepted ≠ executed); run progress/blocked reason
  is visible through run state. Alternative — reject at acceptance as unsupported — is
  listed as an open question (§9); silently masquerading as full import is not allowed.
- `fail_run` remains the default per-entity failure policy (current as-built behavior;
  FR-ETL-014 requires supporting `continue_other_entities`/`retry_entity` but does not
  set a default). `partial_success` is reachable only when a non-default continue policy
  is explicitly configured/recorded on the job — not silently.

### 4.4 Restart recovery — blocked, not fake-resumed (review point 2)

Extend `RecoverAsync` (startup-only, no time-based takeover):

- Clear dead-process job/batch/completion **claim owner tokens**; release **no** entity
  leases.
- `running` job with `pending` run → back to `pending` (re-claimable).
- Run `running` at recovery **before A04 checkpoints exist** → run/job → `blocked` with
  `interrupted_no_checkpoint`, **not** auto-restarted: a whole-window reread under new
  batch IDs cannot prove gap-freedom or freshness against concurrent source
  deletes/updates, and a fixed upper bound is not a snapshot. Blocked runs await A04
  recovery semantics or manual resolution; they are never finalized, never reported
  complete, and never release their entity leases silently. This is the "explicitly keep
  interrupted work blocked awaiting recovery" alternative per review — and it is the
  reason Stage B cutover is safe only because interruption degrades to *blocked*, not to
  *claimed progress*.
- Legacy v4 `running`/`uploading` runs: `uploading` continues the existing completion
  path (data already durable); `running` → `blocked`/`failed` with
  `LEGACY_UNRESUMABLE`; nothing fabricated.

Attempt cap: increment `job.attempt_count` per claim; above a conservative bound →
`blocked` awaiting manual resolution (not silent `finished`).

---

## 5. Entity serialization and immutable config snapshot

- At acceptance, resolve entity codes against `dynamicConfiguration.Entities` (enabled;
  `RunsOnSchedule` rules per mode) and serialize the **full effective
  `EtlEntityDefinition`** into `etl_jobs.entities_json` with the config version.
- At claim/materialization, copy each frozen definition verbatim into
  `etl_run_entities.entity_definition_json`; the OData/spool path consumes the frozen
  copy, never live config.
- Duplication is chosen over referencing `config_snapshots` because remote-configuration
  activation is being changed in a different worktree and snapshot lifecycle is open
  A07b work; a job must not depend on a snapshot row surviving. `configuration_version`
  keeps traceability.
- A config change after acceptance never alters an accepted job's entity set/mappings; it
  applies to the next job. Execution eligibility is still gated by current mode —
  `Disabled` defers, never mutates, the job.

---

## 6. Evidence preservation for active jobs (review point 5)

No-FK `command_id` alone would let normal command cleanup delete the inbox row, its
`result_json`, and its `payload_hash` while the job is still active — after which a
re-delivered `commandId` with a *changed* payload could be admitted with nothing to check
against, and result replay could not reproduce the original. Two layered mechanisms:

1. **Immutable acceptance evidence on the job:** `command_payload_hash` and
   `acceptance_result_json` are copied into `etl_jobs` at acceptance. Replay of a
   duplicate `commandId` uses the stored result verbatim (CD-RS-3 convention), and
   payload-conflict comparison has a durable hash even if the inbox row is gone.
2. **Cleanup guard:** command/outbox cleanup skips a command while a non-terminal
   `etl_jobs` row references it (subquery guard, same pattern as the existing outbox
   guards in `SqliteAgentStore.cs:115-117`). The job row itself outlives the command;
   job retention is independent but evidence is never destroyed while unresolved.

Conflicting re-admission (same `commandId`, different hash) goes through the existing
payload-conflict path and is rejected; the original job/run/result are untouched.
Regressions: repeated `commandId` returns the same runId/result after inbox cleanup age;
conflicting re-admission produces conflict evidence, not a second job; cleanup retains a
command whose job is non-terminal and releases it after finalize.

---

## 7. Stage C — A04 checkpoints/spool and A05 completion

### 7.1 Checkpoints and durable batch intent (review point 4)

File-first + orphan quarantine + automatic reread cannot justify progress: source rows
can disappear between the crash and the reread, and the orphan file is the sole copy of
already-read data. The spool protocol therefore gains **durable batch intent** — a
`creating` `etl_batches` row written **before** the file write, carrying the stable
`batch_id`, `run_id`, `entity_name`, `expected_file_path`, `expected_row_count`, and
`expected_watermark_from/to` (all known once the rows are read). The file is then written
`.tmp` → renamed → a **finalize transaction** fills hash/sizes, flips `creating→ready`,
bumps run/entity counters, and advances `extracting_cursor` — atomically.

Crash states and dispositions (`ReconcileEtlSpoolAsync`, startup, after `RecoverAsync`):

| Observed state | Disposition |
|---|---|
| `creating` intent row, no `.tmp`, no ready file | Crash before write. Mark intent `abandoned` (new batch status value; statuses are unconstrained TEXT, same convention as `blocked` for runs); cursor unadvanced; resume re-reads the range under a new batch intent. |
| `creating` intent + `.tmp` | Quarantine `.tmp` with diagnostics; abandon intent; resume re-reads. |
| `creating` intent + ready file | Crash between rename and finalize. Recompute hash/size/row-count by streaming the **sole original payload**; if consistent with intent → finalize registration + checkpoint in one tx (file preserved and usable, no reread); if inconsistent → quarantine file + **block** run, manual recovery. |
| `ready` row + file, hash/size mismatch at recovery validation | `dead_letter` + quarantine file; run blocked. |
| `ready` row, file missing | External loss → run **blocked**, diagnostics, manual; never advance. |
| File present, no intent row (legacy v4 orphan) | Cannot prove ownership/bounds → quarantine + **block**; no auto-reread claiming the data is covered. |

Every ambiguous direction ends in `blocked` + diagnostics, not silent advancement. The
resume guarantee is therefore *durable-state-based*, not reread-based: cursor advances
only behind a committed batch registration.

Resume semantics (A04): entity `done` → skip; `extracting` → re-query from
`extracting_cursor` to the **stored** fixed bound; `pending` → from `watermark_from`.
Same `run_id`, no new run. Expired continuation token → fresh bounded query from the
durable cursor (still not a snapshot; documented consistency is "bounded read with
durable progress"). Per-entity failure handling: `fail_run` default marks the run failed
and holds leases until the failure transaction releases them; a configured
`continue_other_entities` marks the entity `failed` and continues — `partial_success` at
finalize.

### 7.2 Readiness and finalize (review points 6, 7)

- Readiness is **positive**, not absence-of-bad: run `uploading`/`completing` AND every
  `etl_run_entities` row `done` (or `failed` only under recorded continue policy) AND
  `expected_batch_count` per entity equals the count of that entity's `acknowledged`
  batches AND no batch of the run exists in `creating|ready|uploading|retry_waiting|
  dead_letter` — i.e. every *expected* durable batch is accounted for and acknowledged.
  `abandoned` intent rows are tombstones (never fulfilled, superseded by re-extraction)
  and are excluded from both the blocking set and the count. A run with zero `etl_run_entities`, missing rows, or absent expected
  metadata is **not** ready — missing metadata must not make completion vacuously ready.
  `deleted` batches are allowed only post-finalize (retention gate below).
- Completion claim: `TryClaimRunCompletionAsync` (`uploading→completing` + owner +
  `next_completion_attempt_at_utc`) persisted **before** the ERP POST.
- Finalize — one transaction, after ERP 204 (repeat idempotent): for each `done` entity,
  CAS the watermark: `UPDATE watermarks … WHERE entity_name=$e AND committed_cursor_json
  IS $expected_base` (NULL-safe). **Expected-base compare-and-set, not blind max:**
  `EtlCursorPolicy.Compare` is meaningful only between cursors of the same entity under
  the same definition/policy — it is invalid across full-reload resets, null timestamps,
  composite/typed keys, or a config-changed definition. On CAS mismatch the entity's
  watermark is **not** written; the run goes to `blocked`/`finalize_conflict` for manual
  resolution rather than regressing or blindly advancing. Policy-ordered advancement
  (`Compare`) may be used *inside* the CAS only as an additional guard when base
  equality cannot be proven but same-policy ordering can (documented per-entity; default
  remains strict base equality).
- `succeeded` requires all entities `done`; `partial_success` only under recorded
  continue policy with explicit per-entity outcomes; `failed`/`blocked` never commit any
  watermark.
- Entity leases release inside the same finalize/failure transaction.
- Empty window: keep the existing empty-batch flush as the durable evidence of a
  completed bounded read; final cursor = stored fixed bound. Semantics remain an open
  question (§9) — no stronger claim is made.
- Negative/invalid ACK (`status != 'acknowledged'`, checksum false, row mismatch) →
  guarded `dead_letter` + file quarantine; bounded upload-attempt budget first. Such a
  run is then permanently not-ready until manual resolution — never auto-advance.
- Retention: acknowledged batches eligible only when the parent run is finalized
  (`completion_acknowledged_at_utc IS NOT NULL`); file delete then `MarkBatchDeleted`
  order preserved (idempotent delete). Final cursors live in `etl_run_entities`, so
  cleanup can neither starve nor falsify completion.
- A08 real free-space/reserve checks are out of scope; `spool_full` deferral is the seam.

---

## 8. Dependencies and cutover gates (review points 2, 8)

```text
Stage A: schema + store tx APIs (dark)      — independently reviewable, no behavior change
Stage B: admin acceptance cutover + dispatch — safe because interruption ⇒ blocked, not
         claimed progress; requires Stage A landed and its recovery paths in place
Stage C: A04 checkpoints/spool + A05 finalize/retention — may ship together or staged,
         but never in an order that creates new unsafe windows
```

- The 005/006/007 numbering is a packaging suggestion. If review finds the atomicity of
  B+C requires a single migration or a different order, follow the atomicity, not the
  numbers.
- Production cutover of admin acceptance (Stage B) depends on interrupted work degrading
  to `blocked` rather than auto-reread (§4.4); full resumable progress depends on Stage C.
- Entity leases (Stage A) must exist before any multi-job overlap can occur — i.e. before
  cutover — because the single-worker serialization argument does not cover
  upload/finalize interleavings.
- Cross-worktree: frozen definitions decouple jobs from the in-flight
  configuration-activation change; only `configuration_version` is recorded.

## 9. Open questions (code/spec-tied)

1. External acceptance contract (`data:{accepted:true,runId,mode}`) is [PROPOSED]
   (CD-ETL-1); ERP Agent API absent from the supplied archive — local authorization only.
2. Reconcile modes: defer-as-unsupported (recommended) vs reject-at-acceptance; real
   algorithms are stage-5 scope. ERP-facing visibility of the deferred state is
   undefined.
3. Job attempt-cap value and whether `blocked` expiry/manual-release tooling is needed
   beyond diagnostics (no admin UI/endpoint exists; manual = DB-level per runbook — flag
   as a gap, do not invent an endpoint).
4. Lease handoff vs queue-fairness: deferred `busy_entity` jobs currently wait FIFO; a
   starvation bound is undefined in spec.
5. Empty-window commit beyond "proven bounded read" (CD-P-3).
6. CAS vs policy-ordered advancement per mode (full reload vs incremental) — strict base
   equality is the default; relaxations need per-entity justification.
7. Binary rollback safety for new tables/columns — explicit update→work→rollback→
   queue-verification required (CD §11); additive DDL alone proves nothing.
8. `blocked`/`finalize_conflict` manual-resolution procedure — spec requires recovery
   possibility but no mechanics; runbook-level only in v1.
9. Full-reload watermark semantics under CAS (base expected to differ by design?) — to
   be pinned before implementing `entity_reload` finalize.

## 10. Unsafe interim states to avoid

1. `succeeded` persisted before/independently of the job insert — one transaction or no
   cutover.
2. Eligibility evaluated after claim — must precede it; a claimed/pending row is never
   discarded.
3. A `running` run auto-restarted by whole-window reread and reported as continued
   progress — without durable checkpoints this is unprovable; it must block.
4. Checkpoint in a separate transaction from batch registration — one commit or stale
   cursor behind durable data.
5. Orphan file auto-quarantined **and** the gap auto-filled by reread presented as
   covered — the file is the sole copy; either register it via intent metadata or block.
6. `etl_jobs`/lease/run rows deleted by quarantine/cleanup while unresolved —
   `ADMINISTRATIVE_EXECUTION_UNKNOWN` especially must never remove a proven job.
7. Command inbox/result/hash deleted while a non-terminal job references the command —
   cleanup guard + on-job immutable evidence both required.
8. Readiness expressed as "no non-terminal batch" — must be positive: all expected
   batches acknowledged; dead-letter/missing/creating must block, not slip through.
9. Watermark upsert via unconditional write or cross-policy `max` — expected-base CAS
   only; stale-run finalize must produce conflict, not regression.
10. Reconcile modes executed as full reads — defer as unsupported until real semantics
    exist.
11. Completion claimed without persisted `completing` owner — repeat-complete after
    crash becomes indistinguishable.
12. Pending channel reader surviving a loop iteration — cancel+await (or bounded wait),
    verified by regression.
13. Rolling back to a binary without job support while jobs are pending — abandoned
    accepted work; rollback verification is mandatory (CD §11).

---

*End of design proposal (revision 2). Nothing above is implemented, built, or tested.
Blocked invariants are explicit by design: unprovable states degrade to `blocked` +
diagnostics + manual recovery, never to claimed progress. Root review required before any
implementation.*

## Root review disposition — bounded foundation only (2026-09-24)

Revision2 is a working design reference, not approval for production cutover. The authorized next slice is limited to schema and atomic manual-job acceptance storage, exact replay/identity, owner guards and unresolved-job retention; it does not switch service workers.

Remaining mandatory corrections before cutover: (1) Stage B must not continue through the unsafe existing A05 completion/retention path; install the required finalize guards first or keep execution disabled/pending. (2) Any expected-base CAS mismatch must roll back ALL watermark changes for the run; persist conflict separately or in a transaction that has not committed any watermark updates. (3) Failed runs must retain entity ownership while their pending/in-flight batches can still affect ERP; quiesce/fence all such uploads before release, otherwise late old data can overwrite newer data. (4) Missing/partial creating payload cannot justify source-snapshot guarantees; preserve available original data and explicitly classify any reread consistency limitation. (5) Leases are durable ownership without automatic time expiry; reuse/release must be owner-fenced. Full implementation must add explicit tests for these interleavings.
