# Durable ETL job acceptance — storage foundation (A03a storage slice)

Date: 2026-09-24. Worktree: `agents/worktrees/etl-job-foundation`, baseline checkpoint258 (258 tests).
Model: swe-2-high. Native Windows SDK: `D:/WORK/CNC_Milling/WORK_CNC/SOFT/1C/1C-agent/repo_1c-agent/.dotnet/dotnet.exe` (10.0.400), cwd = this worktree.

## Scope

Bounded storage foundation ONLY: durable manual-ETL-job acceptance storage. This task explicitly
overrides the broad Stage A of `etl-durable-design.md` (a proposal, not an approved design): only
job acceptance storage, no dispatch/claims/extraction/completion/lease tables, no `etl_entity_leases`.
Nothing is wired into `CommandExecutionWorker` or `OnecEtlWorker`; their behavior is unchanged.
This does NOT close A03 — the channel/`accepted:true` defects remain live until the cutover stage.

## What was implemented

- `005_durable_etl_jobs.sql` (additive): `etl_jobs` (one row per accepted manual job: NOT NULL
  UNIQUE `command_id`, stable `job_id`/`run_id`, `mode` CHECKed to `bootstrap_full`/`entity_reload`
  only; frozen `entities_json` + `configuration_version`; immutable `command_payload_hash` +
  `acceptance_result_json`; `status` CHECKed to the documented lifecycle domain, only `pending`
  is written) plus pending-run `etl_runs` metadata (`configuration_version`, `created_at_utc`,
  `updated_at_utc`, `row_version`, deterministic backfill). `etl_jobs.run_id` FK → `etl_runs`;
  deliberately no FK on `command_id`.
- Schema version 5 (`SqliteMigrator.CurrentSchemaVersion`). Migrations 001-004 untouched
  (SHA-256 verified byte-for-byte, see below).
- Typed store API `IAgentStore.AcceptEtlJobAndCompleteCommandAsync(commandId, claimOwner, request)`
  → `EtlJobAcceptanceOutcome` (`Applied` / `AlreadyAccepted` / `NotApplied` + rejection reason).
  ONE SQLite transaction: guarded command UPDATE first (acquires the write lock, so concurrent
  callers serialize) requiring `queued`/`retry_waiting`, the exact current `exec_claim_owner_id`,
  zero attempt/post/lookup counters, NULL `first_sent_at_utc`, no attempt rows, no existing job,
  not past `expires_at_utc`; then pending `etl_runs` insert, `etl_jobs` insert (hash read from the
  canonical saved inbox row), `result_pending` + `succeeded`/`data:{accepted:true,runId,mode}`
  result, `results_outbox` pending. Any stage failure rolls all four tables back.
- Canonical saved `command_type → mode` enforcement inside the same guarded UPDATE (evaluated on
  the CURRENT row in-transaction — no stale caller identity): `start_full_sync → bootstrap_full`,
  `reload_entity → entity_reload`; EVERY other saved type (reconcile, business, scheduled) refuses
  with `NotApplied(TypeModeMismatch)`, so a reconcile command can never masquerade as a durable
  job — this guard did not exist in the first draft and was added after runtime RED evidence.
- Selection correspondence against the saved payload in-transaction, with conservative shape
  validation (added in review round 2 after a NULL/type bypass was proven against the first
  revision): the root payload must be a JSON object; `entity_reload` requires `payload.entity`
  to be non-empty TEXT equal to the single resolved entity; `start_full_sync` accepts an absent
  `entities` key or an empty array as the caller-resolved enabled set, while an explicit field
  must be an ARRAY of non-empty TEXT codes exactly equal to the resolved selection — explicit
  `null`, objects, scalars, and arrays containing null/non-text/empty elements refuse.
  `json_type` gates every branch before `json_each` (no three-valued `NOT IN` NULL bypass, no
  malformed-json error path); compared values are parameterized; the store never invents
  configuration loading.
- A never-sent command already past `expires_at_utc` is refused `NotApplied(Expired)` with zero
  writes, preserving the normal executor expiry path (durable-store contract only — no wire
  semantics invented).
- Repeat command/hash returns the original identity and exact result (`AlreadyAccepted`) without a
  second job or rewriting terminal/delivery history. EVERY `NotApplied` outcome — wrong
  type/mode/selection, foreign/stale/missing owner, expired, non-active state, send evidence, or a
  payload conflicting with an existing job's frozen hash (`PayloadConflict`) — is strictly
  read-only: nothing is written, including `command_payload_conflicts` evidence, which belongs to
  the admission path (004) alone.
  `accepted:true`/`runId` is the locally proposed CD-ETL-1 result only; production is not wired.
- Request validation before any write: mode must be exactly `bootstrap_full`/`entity_reload`;
  entities must be a non-empty explicit resolved set (unique codes, complete enabled definitions,
  non-null `Select`, same structural invariants as the effective configuration); `entity_reload`
  additionally requires exactly one resolved entity. Invalid input throws `ArgumentException`.
- `CleanupAsync` guard: `command_attempts`, `results_outbox`, and `commands_inbox` deletes skip a
  command referenced by a non-terminal `etl_jobs` row (`status NOT IN ('finished','cancelled')`),
  so inbox/attempts/result/outbox survive even after ERP result ACK and past the age cutoff while
  the job is unresolved. The job's frozen hash/result are independent evidence.
- No terminal job closure, no job retention deletion, no dispatch/claim/recovery semantics —
  `RecoverAsync` intentionally leaves pending jobs/runs untouched.

## Changed files

- `src/ErpOnecAgent.Infrastructure/Persistence/Migrations/005_durable_etl_jobs.sql` (new)
- `src/ErpOnecAgent.Infrastructure/Persistence/Sqlite/SqliteMigrator.cs` (version 4 → 5)
- `src/ErpOnecAgent.Infrastructure/Persistence/Sqlite/SqliteAgentStore.cs` (cleanup job guard)
- `src/ErpOnecAgent.Infrastructure/Persistence/Sqlite/SqliteAgentStore.EtlJobs.cs` (new: acceptance tx)
- `src/ErpOnecAgent.Application/Abstractions/EtlJobAcceptance.cs` (new: request/outcome records)
- `src/ErpOnecAgent.Application/Abstractions/Persistence.cs` (new IAgentStore member + docs)
- `tests/ErpOnecAgent.IntegrationTests/EtlJobFoundationTests.cs` (new, 53 cases)
- `tests/ErpOnecAgent.IntegrationTests/EtlJobMigrationTests.cs` (new, 1 case)
- `tests/ErpOnecAgent.IntegrationTests/Fixtures/v4-populated/populated-v4.sql` (new fixture)
- `tests/ErpOnecAgent.IntegrationTests/ErpOnecAgent.IntegrationTests.csproj` (v4 fixture glob)
- `tests/ErpOnecAgent.IntegrationTests/PayloadConflictMigrationTests.cs` (v3→v5 expectations)
- `tests/ErpOnecAgent.IntegrationTests/CommandExecutionTests.cs` (ledger count 4 → 5)
- `docs/remediation/etl-job-foundation.md` (this file)

## Commands and counts

| Command | Result |
|---|---|
| `dotnet restore ErpOnecAgent.sln --locked-mode` | 0 — all projects restored |
| `dotnet build ErpOnecAgent.sln -c Release -t:Rebuild --no-restore` | 0 warnings, 0 errors |
| `dotnet test tests/ErpOnecAgent.IntegrationTests -c Release --no-build --no-restore --filter "FullyQualifiedName~EtlJob"` | 23/23 passed (first draft) |
| `dotnet test ErpOnecAgent.sln -c Release --no-build --no-restore --logger "trx;LogFilePrefix=etl-job-foundation-full-20260924T" --results-directory local-data/etl-job-foundation/TestResults` | 242 integration + 39 unit = **281/281** (first draft — NOT accepted by root review) |
| `dotnet test ... --filter "FullyQualifiedName~Saved_\|FullyQualifiedName~Expired_never_sent"` against the FIRST-DRAFT implementation | **runtime RED: 9 failed** (reconcile/business command accepted as bootstrap_full; start_full_sync accepted as entity_reload; reload_entity accepted as bootstrap_full; mismatched/multi resolved entity accepted; expired never-sent command accepted), 6 passed |
| `dotnet test ... --filter "FullyQualifiedName~EtlJobFoundationTests\|FullyQualifiedName~EtlJobMigrationTests"` after the round-1 guard fix | GREEN 35/35 |
| `dotnet test ErpOnecAgent.sln -c Release --no-build --no-restore --logger "trx;LogFilePrefix=etl-job-foundation-r3"` (review-1) | 254 integration + 39 unit = 293/293 (round 1 — did NOT close round-2 finding) |
| `dotnet test ... --filter "invalid_entities_shape\|valid_entities_shapes\|nonstring_or_missing"` against the ROUND-1 implementation | **runtime RED-2: 5 failed** (`entities:[null]` and `["clients",null]` wrongly Applied via NULL three-valued logic; `entities:null` and `{}` wrongly Applied as omitted; `entities:"clients"` threw SqliteException malformed-json instead of clean refusal), 14 passed |
| `dotnet test ... --filter "FullyQualifiedName~EtlJobFoundationTests\|FullyQualifiedName~EtlJobMigrationTests"` after the shape fix | **GREEN 54/54** |
| `dotnet test ErpOnecAgent.sln -c Release --no-build --no-restore --logger "trx;LogFilePrefix=etl-job-foundation-r4" --results-directory local-data/etl-job-foundation/review-2/TestResults` | 273 integration + 39 unit = **312/312**, 0 failed, 0 skipped |
| `dotnet format whitespace ErpOnecAgent.sln --verify-no-changes --include <touched files>` | clean, no output |

258 baseline + 54 new = 312. The first-draft 281/281 is preserved under
`local-data/etl-job-foundation/` (not accepted — it lacked the canonical type→mode guard);
round-1 evidence is under `review-1/` (293/293 did NOT close the round-2 finding — the
selection predicate still had a NULL/type bypass); round-2 evidence is under `review-2/`.
Compile errors were not counted as RED, and no claim is made that the existing service A03
defect is fixed — it remains live by design of this bounded slice.

## Regression coverage (real migrated temporary SQLite)

- Atomic all-four-table rollback: injected `BEFORE UPDATE`/`BEFORE INSERT` SQL triggers at the
  result/run/job/outbox stages — every case leaves the command `queued` with its claim held and
  zero run/job/outbox rows; a post-drop retry applies cleanly.
- 17 independently claimed commands yield 17 durable jobs + pending runs + results without any channel.
- Repeated commandId returns the original run/job identity and exact stored result; no second job,
  no result/outbox/attempt rewrite.
- Foreign owner and released/never-claimed owner: `NotApplied(ClaimNotOwned)`, claim untouched.
- `executing` (post evidence) and `unknown_result` (lookup evidence): `NotApplied`, no job.
- Canonical mapping negative coverage (runtime RED captured against the first draft, then GREEN):
  saved `reconcile_keys`, ordinary business command, `start_full_sync` requested as
  `entity_reload`, and `reload_entity` requested as `bootstrap_full` all refuse
  `NotApplied(TypeModeMismatch)` — no job/run/result/outbox row and the claim is untouched.
- Selection correspondence: `reload_entity` with a mismatched resolved entity or multiple resolved
  entities refuses (the former `NotApplied`, the latter `ArgumentException`); `start_full_sync`
  with a resolved super/subset of the saved explicit `payload.entities` list refuses
  `NotApplied(TypeModeMismatch)`; exact selection and omitted-list cases apply.
- Payload shape validation (runtime RED-2 captured against the round-1 predicate, then GREEN):
  `entities` as explicit `null`, object, scalar number/bool/string, or an array containing
  null/number/empty-string elements or extra codes all refuse `NotApplied(TypeModeMismatch)`
  with zero mutation — including the `[clients,null]` missing-order bypass and the
  `entities:"clients"` malformed-json_each path (now a clean refusal, no exception). `entity`
  as number/null/object/empty/array/missing for `reload_entity` refuses identically. Valid
  shapes still accepted: absent key, empty array, reordered exact set.
- Expired never-sent command: `NotApplied(Expired)`, row untouched, and the normal executor expiry
  path (`CompleteLocallyAsync` → `expired`) still resolves it afterwards.
- Changed saved payload vs. existing job: `NotApplied(PayloadConflict)` with ZERO writes — no
  conflict event recorded by this API (evidence is the admission path's job), originals intact;
  a foreign owner probing the same conflicting stored identity is refused identically read-only.
- Cleanup after ERP result ACK and aged cutoff preserves inbox/result/outbox/job/run while a
  control command without a job is deleted; replay still returns the original identity.
  NOTE: a legitimately accepted command can never hold attempt rows (the guard requires zero), so
  `command_attempts` preservation under the job guard is verified by inspection only; the control
  row proves attempt deletion still works for non-job commands.
- Fresh store + `RecoverAsync` preserves the pending job, run, identity, and deliverable result.
- Concurrent accepts: TWO independent `SqliteAgentStore` instances over the same factory/file,
  `Task.Run` + shared TCS barrier, direct API calls with NO retry wrapper — exactly one `Applied`
  + one `AlreadyAccepted`, one job/run/result; no SQLite lock failure was observed. (The first
  draft wrapped calls in a test-only SQLITE_BUSY/LOCKED retry loop — removed per review; the
  write-first transaction serializes the calls without it.)
- Unsupported/reconcile/incremental modes and empty/disabled/duplicate/mismatched entity definitions
  are rejected before any write.
- Populated v4→v5 migration: all commands, attempts, outbox, runs, batches, watermarks, state,
  snapshots, conflict events preserved; deterministic `etl_runs` backfill; checksums 1-5; idempotent rerun.

## Evidence

`local-data/etl-job-foundation/` in this worktree — first draft (NOT accepted):
`restore-locked-final.log`, `rebuild-final.log`, `test-full-final.log`, `format-verify-touched.log`,
`migration-sha256.log`, `TestResults/etl-job-foundation-full-20260924T_*.trx`.

`local-data/etl-job-foundation/review-1/` — round 1: `red-console.log` +
`TestResults/etl-job-foundation-red_net10.0_20260924222252.trx` (runtime RED, 9 failed before the
guard existed), `green4-console.log` + `TestResults/etl-job-foundation-green4_*.trx` (35/35),
`restore-locked.log`, `rebuild.log`, `test-full-final.log` +
`TestResults/etl-job-foundation-r3_*.trx` (254 + 39 = 293/293), `format-verify.log`,
`migration-sha256.log`.

`local-data/etl-job-foundation/review-2/` — round 2: `red-console.log` +
`TestResults/etl-job-foundation-red2_*.trx` (runtime RED, 5 failed on the round-1 predicate —
NULL/type bypass proof), `green2-console.log` + `TestResults/etl-job-foundation-green6_*.trx`
(54/54), `restore-locked.log`, `rebuild.log`, `test-full.log` +
`TestResults/etl-job-foundation-r4_*.trx` (273 + 39 = 312/312), `format-verify.log`,
`migration-sha256.log`. Root's independent SQL proof:
`agents/runs/2026-09-24/etl-entity-selection-null-sql-proof.json`.

Migration SHA-256 (001-003 match the values recorded in `result-delivery-guards.md` — byte-for-byte):

- `001_initial.sql`: `6bb8ec130ca7fdd303c08bef28c117452613d768ba9a995d627f9d4e31f7959a`
- `002_retry_budgets.sql`: `eb4d22e6a0b747b89fc09b8b9b2857c3ad893f1f15b9010ba38cdf6447ca3590`
- `003_ordering_claims.sql`: `0bf8e5122b6d094c6e65e0781a72b967fce361130bebe63f9d9ca47f3aac44cb`
- `004_command_payload_conflicts.sql`: `e75caf3ef711be74a91e15be8fc467330b83c8f943c7c1626ddb38f91c23a6ae`
- `005_durable_etl_jobs.sql`: `38bb53a4732a258a5b528bb1b2feeb6e6e8a5acb4743813f954bb09117911a34`

## Limits

- Jobs never leave `pending` in this slice: no dispatch, claims, entity leases, extraction,
  completion, deferral, retry, or manual-resolution tooling exists for them yet.
- Reconcile modes have no storage path at all: the saved `command_type → mode` guard rejects any
  command whose type is not `start_full_sync`/`reload_entity`, so nothing can masquerade as a
  durable job; scheduled `incremental` jobs are out of scope (`command_id` is NOT NULL for
  manual-only acceptance).
- `acceptance_result_json` is the locally proposed CD-ETL-1 form; no external contract exists
  (ERP Agent API absent from the archive) and nothing is wired to production paths.
- `etl_runs` claim-time columns (`resolved_entities_json`, completion fields) and `etl_run_entities`
  are deliberately not added — they belong to the checkpoint/finalize stages.
- The acceptance API is store-level; wiring it into `CommandExecutionWorker` is the cutover stage.

## Remaining root-review conditions for future cutover (NOT implemented here)

- Entity ownership must persist through upload AND an ALL-or-nothing finalize — per-entity
  ownership released only in the finalize/failure transaction or an explicit manual path.
- CAS mismatch on ANY entity must roll back ALL watermark writes for the run — expected-base
  compare-and-set, never an unconditional write or cross-policy `max`.
- A failure must never release an entity while the old run's batches or an in-flight upload can
  still mutate ERP — ownership is held across restart until the run resolves.
- The old A05-style completion path must not be called safe: retained-batch deletion still both
  blocks and starves completion, and unguarded `CompleteEtlRunAsync`/unconditional
  `CommitWatermarkAsync` remain unsafe for cutover until the finalize stage lands.

## Independent review

Round 1 (root preliminary review, `agents/runs/2026-09-24/etl-foundation-preliminary-review.txt`):
first-draft 281/281 was NOT accepted. Blockers resolved in this round — runtime RED captured first
(9 failures), then: canonical saved `command_type → mode` + payload-selection correspondence +
never-sent expiry enforced inside the guarded transaction; `NotApplied` made strictly read-only
(the first draft recorded a conflict event before the owner check — now zero writes for every
rejection, tested including the foreign-owner path); `entity.Select` null validated explicitly;
concurrent test rewritten as two independent store instances with direct calls and no retry
(no lock failure observed — write-first ordering suffices); `entity_reload` positive fixtures
corrected to saved `reload_entity` commands only AFTER the negative RED; attempts-preservation
claim narrowed to inspection-verified.

Round 2 (root review): the round-1 `EntitySelectionGuard` still had a NULL/type bypass —
`{entities:[null]}` and `["clients",null]` wrongly accepted via three-valued `NOT IN` NULL logic,
`{entities:{}}` and explicit `entities:null` were treated as the omitted-list case, and a scalar
string `entities:"clients"` reached `json_each` and threw malformed-json. Root executed the exact
predicate in SQLite as proof. Runtime RED-2 captured first (5 failures), then the predicate was
rebuilt as a shape-validated `json_type`-gated CASE: absent key or empty array → caller-resolved
set; explicit field must be an array of non-empty text codes with exact set equality; every other
shape refuses. `entity_reload` additionally requires `payload.entity` to be non-empty TEXT.

Pending — root independent re-review required before merge (main is now checkpoint278; root
handles the merge, including the accepted configuration activation).

## Independent root acceptance — 24.09.2026 22:44

Accepted as storage foundation only after two review rounds (9 type/mode/selection/expiry runtime failures, then5 JSON-shape failures reproduced and fixed). Root inspected production guards, transaction rollback, direct concurrency test without test-only retries, migration001-004 hashes, retention and real v4 upgrade evidence. Changes merged with accepted configuration activation by three-way comparison; no workers wired. Native locked restore exit0; full Release -t:Rebuild0 warnings/errors; full tests39 unit+293 integration=332/332, no failures/skips. Source manifest stable throughout validation. Independent evidence: local-data/remediation-2026-09-24/etl-job-foundation-root/. Agent evidence copied to local-data/remediation-2026-09-24/etl-job-foundation-devin/ (historical paths above refer to its contents). Checkpoint-332 retained. A03 remains open until safe durable dispatch/recovery/finalization and worker cutover; existing channel behavior unchanged.
