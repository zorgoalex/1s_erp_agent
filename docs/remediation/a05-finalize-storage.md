# A05b F1 — durable bases/finals + atomic run finalize (DARK storage slice)

Date: 2026-09-24; **revision 2 (2026-09-25)** — root review of the first implementation
(448/448) returned six notes; the review fixes are implemented below with real runtime
RED evidence recorded before any production edit (`dev-iter2-red/`).
**Revision 3 (2026-09-25)** — second root review (476/476) found the immutable-payload
transition hole and a nonpositive-generation gap in the base-shape check; fixed below
with real runtime RED recorded first (`dev-iter3-red/`).
Worktree: `agents/worktrees/a05-finalize-storage`, baseline main
`c2c04f9` / checkpoint-380 (380 tests: 341 integration + 39 unit). Model: swe-2-high.
Native Windows SDK: `D:/WORK/CNC_Milling/WORK_CNC/SOFT/1C/1C-agent/repo_1c-agent/.dotnet/dotnet.exe` (10.0.400), cwd = this worktree.
Design: `docs/remediation/a05-finalize-design.md` revision 3 + root review disposition
(2026-09-24) — "the next implementation slice is bounded F1 storage with populated-v5
upgrade tests and explicit mixed-writer limitations". Scope per design §11 F1: schema +
new store APIs + real-SQLite tests only.

## Scope

Bounded F1 DARK storage ONLY: the coherent new storage path — durable entity expected
bases/finals, guarded registration, extraction seal, completion claim with immutable
payload, single-transaction all-or-conflict watermark finalize, fenced retry, and an
explicit startup-only recovery API. **Nothing is wired**: no production worker changes,
no `IErpClient` changes (no HTTP wire-byte claim — the raw-body overload is F2), no
runtime configuration, no `RecoverAsync` changes, no service/package changes, no
commit/push. Legacy v5 APIs (`RegisterBatchAsync`, `GetCommittedWatermarkAsync`,
`CommitWatermarkAsync`, `GetRunsReadyToCompleteAsync`, `MarkEtlRunExtractedAsync`,
`CompleteEtlRunAsync`) are byte-for-byte unchanged and still serve production until F2.
This slice does NOT close A03/A04/A05 and approves no production use of the new APIs.

## What was implemented

- `006_etl_finalize.sql` (additive; 001–005 byte-identical, checksums below):
  - `watermarks` +`generation INTEGER NOT NULL DEFAULT 1` (ABA token, bumped on every
    finalize commit) and +`domain_fingerprint TEXT NULL` (NULL on pre-existing rows =
    unknown domain ⇒ fail closed).
  - `etl_run_entities` (PK `run_id,entity_name`, FK→`etl_runs`): frozen
    `entity_definition_json`, `domain_fingerprint`, `status`
    (`extracting|done|failed`, `pending` reserved), `base_row_present` (absent row vs
    present-NULL row are different CAS shapes), `expected_base_generation`,
    `expected_base_cursor_json` (raw committed text, never re-serialized),
    `expected_base_domain_fingerprint`, `domain_status` (`absent|same|changed|unknown`),
    `watermark_from_json`, `snapshot_upper_bound_json`, `final_watermark_json`
    (explicit terminal cursor, survives batch deletion), `expected_batch_count`,
    `rows_read`, `batches_created`, `last_error`, timestamps + `row_version`.
  - `etl_runs` +`sealed_at_utc`/`sealed_entity_count`/`sealed_expected_batch_count`,
    +`completion_claim_id` (new GUID per successful claim — the fence),
    `completion_claim_owner_id`/`completion_claim_acquired_at_utc` (diagnostics only),
    `completion_attempt_count`, `completion_max_attempts` (durable attempt bound
    persisted by the first claim — added in rev2 so crash+recovery cycles can never
    mint unbounded sends), `next_completion_attempt_at_utc`,
    `complete_payload_json` (immutable verbatim body written once at first claim),
    `completion_acknowledged_at_utc` (evidence only, not a cleanup key),
    `finalize_conflict_code`/`finalize_conflict_message`, `resolved_at_utc` (column
    only — no resolution API built in this slice).
- Schema version 6 (`SqliteMigrator.CurrentSchemaVersion`); `EtlRunStatus.Blocked`
  added (additive enum member).
- `EtlDomainFingerprint` (new, `Application/Abstractions/EtlFinalize.cs`): project-hash
  (`PayloadHasher`) over the canonical tuple {source namespace, entity, entire frozen
  entity definition, query mode}. The source namespace is a required explicit
  NON-SECRET argument of `BeginEtlEntityExtractionAsync` — missing ⇒ typed
  `SourceNamespaceMissing` rejection; no configuration item or secret is read (the
  config item is a recorded F2 dependency). Equal serialized cursors under different
  fingerprints are different domains.
- New `IAgentStore` members (implemented in `SqliteAgentStore.EtlFinalize.cs`, typed
  outcome records in `EtlFinalize.cs`):
  - `BeginEtlEntityExtractionAsync` — ONE tx, write-first fence: run `running` +
    unsealed; no existing entity row. **Rev2 — saved run identity is enforced at
    capture, not deferred to seal:** the request `QueryMode` must equal the run's
    saved `mode` AND be a supported watermark-extraction mode
    (`incremental`/`bootstrap_full`/`entity_reload`; reconcile modes reject) —
    `RunModeMismatch`; the saved `requested_entities_json` must be a valid manifest —
    `RunManifestInvalid`; the entity must be a member of it — `EntityNotInManifest`;
    the supplied definition must deserialize to a valid typed `EtlEntityDefinition`
    whose `EntityCode` equals the requested entity (same structural invariants as job
    acceptance, shared via `IsValidResolvedEntityDefinition`) —
    `EntityDefinitionInvalid`; an associated `etl_jobs` row must still agree with the
    run on mode and `configuration_version` — `JobInconsistent` — and the supplied
    definition must canonically equal the job's frozen definition for that entity
    (`JobDefinitionMismatch`, zero writes — a job-associated run can never extract
    against a different effective definition). Captures the base atomically
    BEFORE extraction: presence + generation + raw committed cursor + stored
    fingerprint; classifies `domain_status` fail-closed — `changed`/`unknown` insert a
    `failed` entity row with `DOMAIN_CHANGED`/`DOMAIN_UNKNOWN` and reject (no
    adoption, no reset, no guessing); `absent`/`same` insert `extracting` and return
    the exact committed base the query must use.
  - `RegisterGuardedEtlBatchAsync` — batch insert + entity counters + run counters in
    one transaction, guarded to `running`+unsealed runs and `extracting` entities; a
    late batch after done/seal/completing is rejected with zero writes. Legacy
    `RegisterBatchAsync` is untouched.
  - `CompleteEtlEntityExtractionAsync` — explicit typed final cursor validated
    through the SAME strict parser the committed-watermark read path uses
    (rev2: `JsonElement.TryGetDateTimeOffset` — the System.Text.Json DateTimeOffset
    grammar, never broad-culture/`DateTimeOffset.TryParse` formats; `sourceId` must
    be a non-empty non-whitespace string when non-NULL): a JSON object whose only
    components are `updatedAtUtc` (absent/NULL/ISO 8601) and `sourceId`
    (absent/NULL/non-empty string) with ≥1 non-NULL component — `{}`, `(null,null)`,
    scalars, arrays, broad-culture dates, empty/whitespace source ids and
    duplicate/unknown properties reject; the valid `(timestamp,NULL)`,
    `(NULL,sourceId)` and composite-id shapes pass. `expected_batch_count >= 1`
    must equal the durable per-entity batch counter. Entity metadata/counts freeze
    after `done`.
  - `SealEtlRunExtractionAsync` — one tx: requested manifest must be a non-empty
    array of unique non-empty strings (`[]`, `{}`, `[null]`, `["a","a"]`, `["a",""]`,
    non-array, non-string elements refuse); entity-row set must equal it exactly; all
    entities `done` with valid meaningful finals; `expected_batch_count >= 1` equals
    actual batch rows per entity; run totals equal the sealed totals (no stowaway
    batches, counters consistent). On success the run becomes `uploading` with frozen
    seal counts; afterwards every new-path mutation API is fenced.
  - `FailEtlRunAsync` / `BlockEtlRunAsync` — guarded `WHERE status='running'`:
    fail_run only (no partial-success machinery); still-extracting entities fail;
    pending batches (`creating`/`ready`/`retry_waiting`/`uploading`) fence to
    `dead_letter`; the job becomes `blocked`, never `finished`. A SQL status flip can
    never cancel an in-flight HTTP upload or recall remote effects.
  - `GetDueRunCompletionsAsync` — `uploading` runs plus `completing` runs with a
    released claim whose retry is due.
  - `TryClaimRunCompletionAsync(runId, ownerId, nextAttemptAtUtc, maxAttempts, ct)` —
    write-first tentative claim serializes concurrent claimants (exactly one wins;
    the loser rolls back untouched). **Rev2 — the attempt bound is enforced at claim
    admission:** the first successful claim persists `maxAttempts` on the run row
    (`completion_max_attempts`), a later claim presenting a different limit is refused
    with zero writes, and a released due claim whose stored attempt count already
    reached the bound commits `blocked COMPLETION_ATTEMPTS_EXHAUSTED` + blocked job —
    crash+recovery cycles can never mint an unbounded send. **Rev3 — the
    fresh/reclaim transition is fenced by the attempt invariant:** the claim UPDATE's
    just-incremented `completion_attempt_count` is the discriminator — `1` means the
    run was `uploading` (first claim, payload must be absent and is generated after
    readiness); `>1` means a prior claim minted the immutable body and it MUST still
    be stored. A stored payload on a first claim (planted body) or a missing payload
    on a reclaim (lost body) commits `blocked SEAL_VIOLATED` — never fabricates a
    replacement, never authorizes another send. Reclaim (`completing` +
    NULL claim + due) now re-verifies the SAME full invariant as a fresh claim
    (rev2: exact manifest/entity set, sealed counts, all entities `done` with valid
    finals, per-entity batch count AND `rows_read` aggregates, expected-base shapes —
    rev3 requires a positive generation), every batch acknowledged) plus complete
    payload schema/identity validation
    (unambiguous object with exactly the six written properties, `runId` equal to
    this run, `status='succeeded'`, counters equal to the durable run counters) —
    an arbitrary object, a foreign run's body, drifted counters or regressed ACK
    evidence commits `blocked SEAL_VIOLATED`, never a new admissible claim; the
    stored payload is replayed byte-identical, never regenerated. Fresh claim
    verifies the full invariant in-tx (sealed, exact manifest/entity set, all done +
    valid finals, per-entity + run count/row consistency, no non-acknowledged or
    dead batch, coherent expected-base shapes). Unsealed or entity-less `uploading`
    runs commit `blocked LEGACY_UNRESOLVABLE` with evidence preserved — never
    derived, never fabricated. Corrupted sealed evidence commits `blocked
    SEAL_VIOLATED`. Transient upload-in-flight rolls back to `uploading` untouched.
    First claim writes the immutable `{runId,status:'succeeded',rowsRead,
    batchesCreated,batchesAcknowledged,completedAtUtc}` body once.
  - `FinalizeEtlRunAsync` — ONE transaction under the exact claim identity
    (`completing` + `completion_claim_id`): stale/superseded → `ClaimLost`, zero
    writes. Full guard recheck (rev2: also the stored payload's schema/identity —
    violation → conflict `SEAL_VIOLATED`). Then
    `SAVEPOINT`: every `done` entity applies its CAS shape — expected-present rows get
    a pure UPDATE matching `generation` + `committed_cursor_json IS base` +
    `domain_fingerprint IS base_fp` (never INSERT, generation bumped even for
    same-cursor writes, no MAX/comparison — a non-monotonic final still commits);
    expected-absent rows get a guarded `INSERT ... WHERE NOT EXISTS` (never UPDATE).
    Any 0-changed entity → `ROLLBACK TO` the savepoint, classify
    (`ROW_UNEXPECTEDLY_PRESENT`/`ROW_VANISHED`/`GENERATION_MISMATCH` — covers ABA and
    same-cursor commits — `BASE_CURSOR_MISMATCH`/`DOMAIN_MISMATCH`), and ONE commit
    writes `blocked` run + conflict + `blocked` job. All-1 → `succeeded` run +
    `finished` job in the same commit. SQL/fault exceptions roll the whole
    transaction back, preserving the claim and the unknown outcome.
  - `MarkRunCompletionRetryAsync` — fenced on the exact claim identity: releases the
    claim (next claim mints a fresh identity), preserves the payload verbatim, sets
    `next_completion_attempt_at_utc`/`last_error`; reaching the attempt bound commits
    `blocked COMPLETION_ATTEMPTS_EXHAUSTED` + blocked job. Rev2: the bound persisted
    on the run at first claim is authoritative — a retry call presenting a different
    `maxAttempts` argument can never widen it (the argument still exists so callers
    must state a policy limit; the durable value wins on mismatch).
  - `RecoverInterruptedEtlRunsAsync` — explicit dead-process recovery, startup-only /
    exclusive-host precondition, NOT called by `RecoverAsync` (unchanged) and never a
    live/time-based takeover: releases `completing` claims preserving payload and
    evidence; blocks `running` runs `INTERRUPTED_NO_CHECKPOINT`; marks their
    extracting entities `failed`; fences their pending batches to `dead_letter`;
    blocks their jobs. `pending` runs untouched. No manual-resolution or
    ownership-release API is built (the `resolved_at_utc` column exists for the
    documented manual procedure only).

## Changed files

- `src/ErpOnecAgent.Infrastructure/Persistence/Migrations/006_etl_finalize.sql` (new;
  rev2 adds `completion_max_attempts` — 001–005 remain byte-identical)
- `src/ErpOnecAgent.Infrastructure/Persistence/Sqlite/SqliteMigrator.cs` (version 5 → 6)
- `src/ErpOnecAgent.Infrastructure/Persistence/Sqlite/SqliteAgentStore.EtlFinalize.cs` (new)
- `src/ErpOnecAgent.Infrastructure/Persistence/Sqlite/SqliteAgentStore.EtlJobs.cs`
  (rev2: `IsValidResolvedEntityDefinition` extracted, shared with the capture path —
  no behavior change to job acceptance)
- `src/ErpOnecAgent.Application/Abstractions/EtlFinalize.cs` (new records/outcomes +
  fingerprint; rev2 adds the five capture-identity rejection reasons)
- `src/ErpOnecAgent.Application/Abstractions/Persistence.cs` (new IAgentStore members +
  docs; rev2 adds `maxAttempts` to `TryClaimRunCompletionAsync`)
- `src/ErpOnecAgent.Domain/Etl/EtlModels.cs` (`EtlRunStatus.Blocked`)
- `tests/ErpOnecAgent.IntegrationTests/EtlFinalizeStorageTests.cs` (new)
- `tests/ErpOnecAgent.IntegrationTests/EtlFinalizeMigrationTests.cs` (new)
- `tests/ErpOnecAgent.IntegrationTests/Fixtures/v5-populated/populated-v5.sql` (new fixture)
- `tests/ErpOnecAgent.IntegrationTests/ErpOnecAgent.IntegrationTests.csproj` (v5 fixture glob)
- `tests/ErpOnecAgent.IntegrationTests/EtlJobMigrationTests.cs` (v4→v6 expectations + 006 checksum)
- `tests/ErpOnecAgent.IntegrationTests/PayloadConflictMigrationTests.cs` (v3→v6 ledger counts)
- `tests/ErpOnecAgent.IntegrationTests/CommandExecutionTests.cs` (ledger count 5 → 6)
- `docs/remediation/a05-finalize-storage.md` (this file)

## Commands and counts

| Command | Result |
|---|---|
| `dotnet restore ErpOnecAgent.sln --locked-mode` | 0 |
| `dotnet build ErpOnecAgent.sln -c Release -t:Rebuild --no-restore` | 0 warnings, 0 errors (8 projects) |
| `dotnet test tests/ErpOnecAgent.IntegrationTests -c Release --no-build --no-restore --filter "FullyQualifiedName~EtlFinalize"` (first implementation pass) | 63/66 — three test-side defects found and fixed (NULL parameter binding in the test seed helper; classifier ordering for sealed-vs-done batch rejection; a wrong expected rejection reason in the post-seal fencing test). No production-code defect was hit. |
| same, after fixes + 2 added cases | **68/68 GREEN** |
| `dotnet test ErpOnecAgent.sln -c Release --no-build --no-restore` (first full pass) | 404/407 — the three pre-existing migration-ledger assertions (`EtlJobMigrationTests`, `PayloadConflictMigrationTests`, `CommandExecutionTests`) expected schema 5; updated to 6 with a 006 checksum assertion, as for the 005 landing. |
| `dotnet test ErpOnecAgent.sln -c Release --no-build --no-restore --logger "trx;LogFilePrefix=a05f1-final" --results-directory local-data/remediation-2026-09-24/a05-finalize-storage/final/TestResults` | **409 integration + 39 unit = 448/448**, 0 failed, 0 skipped, exit 0 |
| `dotnet format whitespace ErpOnecAgent.sln --verify-no-changes --include <touched files>` | clean, exit 0 |
| **Rev2 RED** `dotnet test ... --filter "FullyQualifiedName~EtlFinalizeStorageTests" --logger "trx;LogFilePrefix=a05f1-red"` on the UNCHANGED first implementation | exit 1, **35 real runtime failures** (`dev-iter2-red/`) — every review note reproduced: broad-culture/empty-source finals accepted (6), non-ISO persisted final sealed (1), query-mode/manifest/typed-definition/job-identity captures accepted (5 + 10 manifest + 1 entity-set), corrupt/foreign payload reclaimed as `Claimed` (5), regressed entity/ACK/base reclaim claimed (3), third post-recovery claim admitted past the bound (1). `Seal_with_malformed_manifest_refuses` Begin assertions RED ×10. |
| **Rev2** `dotnet restore --locked-mode` | 0 |
| **Rev2** `dotnet build -c Release -t:Rebuild --no-restore` | 0 warnings, 0 errors (8 projects) |
| **Rev2** `dotnet test` integration + unit, `--logger "trx;LogFilePrefix=a05f1-r2-*"` | **437 integration + 39 unit = 476/476**, 0 failed, 0 skipped, exit 0 |
| **Rev2** `dotnet format --verify-no-changes --include <touched files>` | clean, exit 0 |
| **Rev3 RED** `dotnet test ... --filter "FullyQualifiedName~EtlFinalizeStorageTests" --logger "trx;LogFilePrefix=a05f1-r3-red"` on the UNCHANGED rev2 implementation | exit 1, **4 real runtime failures** (`dev-iter3-red/`): NULL'd payload after a prior claim reclaimed as `Claimed` with a fabricated new body (1), a planted schema-valid payload on a never-claimed run accepted (1), `expected_base_generation` 0 and -3 both claimed (2). The finalize-NULL/corrupt-payload and mismatched-`maxAttempts`-refusal cases passed already (rev2 coverage), so they are not counted as RED. |
| **Rev3** `dotnet restore --locked-mode` / `dotnet build -c Release -t:Rebuild --no-restore` | 0; 0 warnings, 0 errors (8 projects) |
| **Rev3** `dotnet test` integration + unit, `--logger "trx;LogFilePrefix=a05f1-r3-*"` | **444 integration + 39 unit = 483/483**, 0 failed, 0 skipped, exit 0 |
| **Rev3** `dotnet format --verify-no-changes --include <touched files>` | clean, exit 0 |

Rev1: 380 baseline + 68 new = 448. Rev2 adds 28 regression cases (476 total). The rev1
new-API work had no possible runtime RED (missing APIs do not compile); the rev2
regressions DID produce real runtime failures on the unchanged implementation —
captured in `dev-iter2-red/test.log` + TRX before any production edit.
Honest accounting of the rev2 RED count (35): 11 were pre-existing tests UPDATED to
assert the new capture guards (`Seal_with_malformed_manifest_refuses` ×10 asserting
`RunManifestInvalid` at Begin, `Seal_with_missing_or_extra_entity_rows_refuses` ×1
asserting `EntityNotInManifest`), and the crash+recovery attempt-bound case exercised
a contract CHANGE — `TryClaimRunCompletionAsync` gained the `maxAttempts` parameter
in rev2 (the bound did not exist as an API concept before; the test was written
against the old 3-argument signature for the RED run and then updated). All other rev2
failures were genuine behavioral gaps on the unchanged code.
Rev3 adds 7 cases (483 total): the payload transition-invariant pair, the
finalize payload block, the bound-mismatch refusal, and the two
nonpositive-generation corruptions.

## Regression coverage (real migrated temporary SQLite — `SqliteTestDatabase`)

- Populated v5→v6 upgrade (`Fixtures/v5-populated/populated-v5.sql` on real 001–005
  schema + correct v1–v5 ledger): every command, attempt, outbox, job, run, batch,
  watermark, state, snapshot and conflict row preserved byte-for-byte; watermark rows
  get `generation=1` + NULL fingerprint (unknown domain, fail closed); new run columns
  NULL/defaults; `etl_run_entities` empty; checksums 1–6; idempotent rerun.
- Base capture: absent row vs present-NULL row vs present populated row; raw committed
  cursor captured verbatim; `same`/`changed`/`unknown` classification — NULL
  fingerprint blocks `DOMAIN_UNKNOWN` preserving the row; identical cursor bytes under
  a different fingerprint block `DOMAIN_CHANGED`; missing source namespace rejects
  with zero writes; malformed/ambiguous/duplicate definition JSON rejects;
  job-associated runs enforce the frozen definition (`JobDefinitionMismatch`,
  including an entity absent from the job selection).
- Guarded registration: atomic batch+counters; after-`done`, post-seal and post-claim
  registrations rejected with zero writes; unknown entity and negative row counts
  rejected.
- Entity completion: `{}`/`(null,null)`/array/scalar/malformed/bad-date/duplicate/
  unknown-property finals rejected before write; `(timestamp,NULL)`, `(NULL,sourceId)`
  and single-component shapes accepted; count mismatch and wrong state rejected;
  zero-row entity with an explicit empty batch is valid.
- Seal: frozen `uploading` + manifest counts; malformed manifests (10 shapes), missing
  and extra entity rows, non-`done` entities, corrupted stored final, count mismatch —
  all refused; post-seal `Begin`/`RegisterBatch`/`CompleteEntity`/`Seal`/`Fail`/`Block`
  all fenced.
- fail/block: run `failed`/`blocked`, extracting entities `failed`, pending batches
  `dead_letter`, job `blocked` (never `finished`), zero watermark writes; sealed runs
  reject termination.
- Claim: due-list membership; GUID claim identity + `attempt=1` + payload shape and
  values; claimed runs excluded until released-and-due; in-flight uploads → transient
  `NotClaimed` with zero writes; reclaim mints a new claim id and replays the exact
  stored payload string; scheduled-not-due then due reclaim sequence; legacy
  `uploading` run → `blocked LEGACY_UNRESOLVABLE`, all rows preserved, zero watermark
  writes; corrupted sealed evidence → `blocked SEAL_VIOLATED`; TWO independent
  `SqliteAgentStore` instances racing on one barrier with direct calls and NO retry —
  exactly one `Claimed`.
- Finalize: present-UPDATE (gen+1) and absent-INSERT (gen=1) CAS shapes; controlled
  mismatch on entity 2/3 rolls back ALL entities and commits `blocked
  GENERATION_MISMATCH` + conflict + preserved payload; ABA X→Y→X detected by
  generation despite identical bytes; same-cursor commit under gen+1 detected and a
  same-cursor final still commits with gen+1; absent→present
  `ROW_UNEXPECTEDLY_PRESENT` (no insert-overwrite); present→deleted `ROW_VANISHED`
  (an INSERT can never fire for a captured base); domain fingerprint drift
  `DOMAIN_MISMATCH`; non-monotonic ("older") final commits — no MAX/timestamp
  ordering anywhere; durable final wins over identical `created_at_utc` batch
  timestamps; stale claim finalize/retry → `ClaimLost`, replay after success → zero
  extra writes; job `finished` only in the success commit.
- Fault injection: real SQLite `BEFORE UPDATE` abort trigger at the k-th CAS write —
  the thrown `SqliteException` rolls back the whole transaction (fenced guard and all
  watermark writes), the run stays `completing` with the claim and payload intact;
  after dropping the trigger the same claim finalizes to exactly one success.
- Rev2 — strict cursor parsing: broad-culture (`09/25/2026`, `Sep 25, 2026`,
  `09/25/2026 10:30:00 +00:00`) and empty/whitespace `sourceId` finals rejected at
  write; a non-ISO persisted final inserted behind the API fails the seal recheck;
  a valid finalized cursor round-trips through `GetCommittedWatermarkAsync` (the
  strict `System.Text.Json` `EtlCursor` read path).
- Rev2 — capture identity: query mode differing from the saved run mode and
  unsupported saved modes (`reconcile_keys`) reject `RunModeMismatch`; malformed
  manifests reject `RunManifestInvalid` at capture (10 shapes); non-member entities
  reject `EntityNotInManifest` at capture (the seal `EntitySetMismatch` guard still
  proven against a fabricated row); `{}`/missing-field/wrong-`EntityCode`
  definitions reject `EntityDefinitionInvalid`; job mode and configuration-version
  drift reject `JobInconsistent`.
- Rev2 — reclaim integrity: corrupted payload shapes (`{}`, wrong status, wrong
  counters, missing/extra property, case-duplicate names), a foreign run's payload,
  a deleted entity row, a regressed (un-acknowledged) batch and a malformed
  expected base each block `SEAL_VIOLATED` with the run and payload preserved.
- Rev2 — attempt bound at admission: claim → crash → recovery → claim → crash →
  recovery → third claim is `Blocked COMPLETION_ATTEMPTS_EXHAUSTED` with
  `completion_max_attempts` persisted, no phantom attempt minted, payload retained;
  a mismatched `maxAttempts` on a later claim is refused with zero writes
  (guard-level `NotClaimed`).
- Rev2 — honest same-cursor test: a final byte-identical to the captured base X
  finalizes successfully and bumps generation exactly once (captured 2 → 3).
- Rev2 — job-associated atomicity: `BEFORE UPDATE ON etl_jobs` abort trigger at the
  finalize job-write rolls back ALL watermarks + run + job in one transaction
  (claim preserved; retry converges); a CAS conflict on a job-associated run blocks
  the job and rolls back the watermark in one commit.
- Rev3 — payload transition invariant: a `complete_payload_json` set to NULL behind
  the API after a real claim + recovery blocks `SEAL_VIOLATED` on reclaim (no
  regenerated body, no send authorized, NULL preserved, job never finished); a
  planted schema-valid payload on a never-claimed run blocks `SEAL_VIOLATED`;
  finalize under a live claim with NULL or corrupted payload blocks
  `SEAL_VIOLATED` with zero watermark writes.
- Rev3 — bound refusal: a reclaim presenting `maxAttempts` different from the
  persisted `completion_max_attempts` is `NotClaimed` with zero writes (no phantom
  attempt, durable bound intact); the identical original limit still claims and
  replays the exact stored body.
- Rev3 — base-shape strictness: `expected_base_generation` corrupted to `0` or
  negative blocks `SEAL_VIOLATED` on reclaim (real generations are always ≥ 1).
- Retry: bounded `maxAttempts` → `blocked COMPLETION_ATTEMPTS_EXHAUSTED` + blocked job,
  payload preserved; stale claim → `ClaimLost`.
- Recovery: existing `RecoverAsync` does not touch new claims (asserted unchanged);
  `RecoverInterruptedEtlRunsAsync` releases dead `completing` claims preserving
  payload/status (reclaim replays identical body under a fresh GUID), blocks
  `running` runs `INTERRUPTED_NO_CHECKPOINT` with entities failed + batches fenced +
  jobs blocked, and leaves `pending` runs untouched.

## Evidence

`local-data/remediation-2026-09-24/a05-finalize-storage/`:

- `baseline/` — pre-change checkpoint-380: `restore.log`, `rebuild.log`, `test.log`
  (341 + 39 = 380/380, exit 0), `TestResults/a05f1-baseline_*.trx`.
- `dev-iter1/` — implementation iterations: `test.log` (63/66), `test2.log` (65/66),
  `test3.log` (66/66), `test4.log` (68/68), `test-full.log` (404/407 — the three
  schema-version assertions), `test-full2.log` (407+39=446 interim).
- `final/` — `restore.log` (exit 0), `rebuild.log` (`-t:Rebuild --no-restore`,
  0 warnings/0 errors, exit 0), `test.log` (**448/448**, exit 0),
  `TestResults/a05f1-final_net10.0_*.trx` (integration + unit),
  `format-verify.log` (clean), `migration-sha256.log`.
- `dev-iter2-red/` — rev2 regression tests against the UNCHANGED first
  implementation: `test.log` (60 passed / **35 real runtime RED**, exit 1),
  `TestResults/a05f1-red_*.trx`, `exit-code.log`.
- `dev-iter2-final/` — rev2 pipeline: `restore.log`, `rebuild.log`,
  `test-integration.log` (**437/437**), `test-unit.log` (**39/39**),
  `TestResults/a05f1-r2-{int,unit}_*.trx`, `format-verify.log` (clean),
  `migration-sha256.log`, `exit-codes.log` (all 0).
- `dev-iter3-red/` — rev3 regression tests against the UNCHANGED rev2
  implementation: `test.log` (98 passed / **4 real runtime RED**, exit 1),
  `TestResults/a05f1-r3-red_*.trx`, `exit-code.log`.
- `dev-iter3-final/` — rev3 pipeline: `restore.log`, `rebuild.log`,
  `test-integration.log` (**444/444**), `test-unit.log` (**39/39**),
  `TestResults/a05f1-r3-{int,unit}_*.trx`, `format-verify.log` (clean),
  `migration-sha256.log`, `exit-codes.log` (all 0).

Migration SHA-256 (001–005 match the values recorded in `etl-job-foundation.md` —
byte-for-byte unchanged):

- `001_initial.sql`: `6bb8ec130ca7fdd303c08bef28c117452613d768ba9a995d627f9d4e31f7959a`
- `002_retry_budgets.sql`: `eb4d22e6a0b747b89fc09b8b9b2857c3ad893f1f15b9010ba38cdf6447ca3590`
- `003_ordering_claims.sql`: `0bf8e5122b6d094c6e65e0781a72b967fce361130bebe63f9d9ca47f3aac44cb`
- `004_command_payload_conflicts.sql`: `e75caf3ef711be74a91e15be8fc467330b83c8f943c7c1626ddb38f91c23a6ae`
- `005_durable_etl_jobs.sql`: `38bb53a4732a258a5b528bb1b2feeb6e6e8a5acb4743813f954bb09117911a34`
- `006_etl_finalize.sql`: `1634cfeb915bb7530f911104267575af158f3446d828297d775faa8693d3166a`
  (rev2 — `completion_max_attempts` added; the rev1 checksum was
  `086d6f97574cdab61deac62bc1cf83cf2128aad83e78d3cfc9130f6a7daf077a`)

No live ERP, 1C, external database, network endpoint, deployment, credential,
certificate, secret read, service install, or commit was used. Only this worktree was
modified; main, other trees and the plan were not touched.

## Isolated-API guarantees and explicit remaining gates

**What F1 proves (new path only, real SQLite):** atomic expected-base capture;
presence+generation+cursor+domain CAS with ABA and same-cursor detection; sealed
immutable manifests; claim-ID fencing; immutable payload replay; all-or-conflict
finalize in ONE transaction (all watermarks + succeeded run + finished job, or zero
watermark changes + blocked run + conflict + blocked job); bounded fenced retry;
explicit startup-only recovery preserving evidence; fail-closed legacy quarantine.

**F1 mixed-writer limitation (prominent, unchanged from design §11):** these
guarantees do NOT exist while legacy writers coexist. `CommitWatermarkAsync` upserts
without bumping `generation` (a legacy write between capture and finalize can defeat
the ABA token), `RegisterBatchAsync` is unguarded, `CompleteEtlRunAsync` and
`MarkEtlRunExtractedAsync` can move a new-path run outside its state machine, and a
sealed `uploading` run also satisfies the legacy `GetRunsReadyToCompleteAsync`
readiness — running both paths concurrently would let the v5 worker complete the same
run through the non-atomic route. **No production use of the new APIs until F2
retires/fences every bypass atomically in one change.** F1 adds no behavior-changing
triggers and keeps tests on the isolated path — that is the stated contract.

**Remaining F2 gates (recorded, not waived):**

- Worker diffs (`Begin`→query→`CompleteEntity`→`Seal`, claim→POST→finalize loop),
  `RegisterBatch` guard wiring, `RecoverAsync` additions, `IErpClient` raw-body
  overload — all deferred and not started here.
- Ownership + upload fencing (A03/A04 leases or approved equivalent) — absent.
- A configured stable `source_namespace` and a verified domain-establishment/reset
  procedure for `unknown`/`changed` rows — absent; every pre-v6 watermark is `unknown`
  and will block on first post-cutover contact (intended fail-closed).
- `MaxRunCompletionAttempts` as a bound `EtlOptions` value — currently an explicit
  `maxAttempts` argument on `TryClaimRunCompletionAsync` (persisted per run at first
  claim; identical value required on reclaims) and `MarkRunCompletionRetryAsync`
  (durable value authoritative).
- ERP complete-endpoint idempotency remains unproven — the honest replay of the
  identical stored body is the only retry semantic built.
- Blocked/failed runs and their jobs stay unresolved: no manual-resolution endpoint
  or ownership release exists; `resolved_at_utc` is present for the documented manual
  procedure only. Fencing bounds future local dispatch — a batch mid-POST can still
  land at ERP; SQL status never proves remote quiescence.
- Conservative definition/mode fingerprinting may block ordinary
  full→incremental transitions; an explicit tested domain-transition policy is still
  a cutover prerequisite (root disposition note).
- `pending`-entity status and `etl_run_entities` retention horizon remain A04/F3 scope.

Pending root review; no merge or production cutover is claimed by this document.


## Root acceptance — 2026-09-25

Accepted as the bounded F1 DARK storage foundation after two review/fix rounds. Root read the implementation and actual RED logs (35 revision-2 failures with the disclosed fixture/contract qualifications; four revision-3 behavioral failures), checked transaction/fault tests and the final diff, and merged the 15 changed files onto the independently accepted A07 B6 baseline (401 tests). No overlapping source changes or deleted files; migrations 001–005 unchanged.

Independent native Windows verification in `repo_1c-agent`: locked restore exit 0; Release Rebuild exit 0, zero warnings/errors; complete solution test exit 0, **465 integration + 39 unit = 504/504**, zero failures/skips. Source manifest remained unchanged throughout verification. Evidence: `local-data/remediation-2026-09-25/a05-finalize-storage-root/` (restore/rebuild/test logs and separate TRX files). Worker result 483/483 used its original 380-test baseline; combined main adds the 21 accepted A07 B6 tests.

The job-final-write fault and job-associated conflict tests use one entity; the separate k-th-CAS fault/conflict tests cover multiple entities. Store tests do not demonstrate production worker or HTTP behavior. Acceptance does not waive the F2 prerequisites above: ownership/upload fencing, configured source identity and explicit domain transitions, legacy evidence resolution, raw-body client dispatch, and atomic retirement/fencing of every old writer before cutover. A03/A04/A05 remain open end-to-end.
