# O3 — scheduled ETL runs with frozen identity and dedup (migration 009) — DARK storage slice

Date: 2026-09-26. Baseline: main `03d182f` (O2, 653/653). Worktree `agents/worktrees/etl-scheduled-o3`.
Design: [etl-ownership-upload-design.md](etl-ownership-upload-design.md) §3.3, §4.2–4.5, §6.1, §10 (O3 row).

**Division of work** (per the user's instruction of 2026-09-25):
- The orchestrator wrote the contract, the migration and all production logic.
- Devin swe-2-high, working in a separate worktree from the contract commit, wrote the
  test suites to the orchestrator's scenario list, built the v8 fixture and adapted
  existing tests to schema version 9.
- Independent fresh-context agents reviewed both the implementation and the tests.

**DARK:** nothing here is wired into `OnecEtlWorker`, recovery or the ERP client. The legacy
tick (`CreateEtlRunAsync`) remains a §9 bypass until cutover.

## Schema (`009_etl_scheduled_runs.sql`, LF, additive)

- `etl_runs.schedule_key TEXT NULL` identifies the schedule that owns a jobless run. It is
  NULL for job runs and for legacy runs.
- `etl_runs.resolved_entities_json TEXT NULL` holds the full frozen effective
  definitions. Together with `configuration_version` it is the only identity source for a
  run without a job.
- `ux_etl_runs_schedule_active` is a partial UNIQUE index on `schedule_key` covering every
  status except `succeeded`, `cancelled` and a failed/blocked run with
  `resolved_at_utc` set. The predicate fails closed: `paused`, partial or unknown statuses
  also hold the key.
- LF SHA-256: `C9A69671C4DE9471C03E073E6796D4F9C5C3BB18F03583FF3AE1B30964A9ED07`.
  Files 001–008 are byte-identical.

## API (`IAgentStore`)

**`EnsureScheduledEtlRunAsync(request, now)`**
- `Created(runId)` inserts a pending run with the ordered code manifest, the frozen
  definitions, the configuration version and the key.
- `ActiveExisting(runId, status)` returns the run that already holds the key, with zero
  writes; that run's frozen identity is never replaced.
- Serialized by the `BEGIN IMMEDIATE` write lock. The unique index is the storage backstop.
- Validation rules:
  - the mode must be `incremental` (design §4.4); full and reload runs are manual jobs;
  - the key must be non-blank and must not have leading or trailing whitespace;
  - the definitions must be non-empty, unique and structurally valid, using the same rule
    as durable jobs;
  - the configuration version must be ≥ 0.

**`TryClaimScheduledRunAsync(runId, owner, now)`** runs the claim transaction through
the shared `ClaimPendingRunCoreAsync`. This core was extracted from the O1 job claim
without behaviour change (same order, messages and expected-one checks). It performs, in
order:
1. elder-manifest quarantine;
2. the elder-overlap reservation by run `(created_at_utc, run_id)`, shared by manual and
   scheduled runs in both directions;
3. all-entity ownership acquisition with immutable bindings under a savepoint, with
   `owner_job_id` NULL;
4. the run start, minting a fresh extraction claim.

It returns one of these outcomes:
- **`NotClaimable`** (read-only): the run is missing, not pending, not scheduled, or has a job
  row.
- **`Blocked(SCHEDULED_RUN_INCONSISTENT)`**: the frozen identity is corrupt. A provably
  never-started run is blocked. With unproven effects the run keeps its pending hold and
  nothing is written.
- **`Deferred(reason)`**: the claimant writes nothing of its own (no job row exists to hold
  deferral state). Only elder quarantines from the same pass commit.

**`GetDueScheduledRunsAsync(limit, now)`**
- Returns eligible pending scheduled runs.
- Eligibility is checked before `LIMIT`: frozen identity, no job row, no older overlapping
  pending run, and every entity acquirable.
- Read-only.
- Corrupt runs are omitted here. Their diagnostic is surfaced by
  `GetDispatchableEtlJobsAsync().Quarantined` with `JobId=null`.

**`BeginEtlEntityExtractionAsync`**
- A jobless scheduled run must have a consistent frozen identity, and the caller's
  definition must equal the frozen one; otherwise `RunDefinitionMismatch`.
- A jobless run without a key is still `JobMissing`.
- A job run carrying a `schedule_key` is `JobInconsistent`.

**Job path hardening**
- The job claim and job enumeration refuse runs that carry a `schedule_key`.
- Elder validation and diagnostics also validate the identity of scheduled elders.

## Evidence

Evidence lives in `repo_1c-agent/local-data/remediation-2026-09-26/etl-scheduled-o3-root/`
and the Devin worktree `local-data`.

**RED on the contract stubs** (Devin, verified from TRX counters):
- 648 integration tests: 609 passed, 39 failed. All 39 failures are the new O3 suite.
- Unit tests: 48/48 passed.
- One O3 case (a scheduled elder reserving against a younger job) already passed. It
  pins O1 behaviour that covers every pending run, so it is not vacuous.

**Review round 1** (fresh agent, implementation at `08b57a1`):
- No correctness defect found in the claim, refactor or dedup logic.
- Findings fixed:
  - S1: scheduled modes widened beyond spec → now `incremental` only.
  - S3: the key was released by `paused`, `partial_success` or unknown statuses → the
    predicate now fails closed.
  - N4: whitespace-padded keys are now rejected.
  - N5: the job-path inconsistency message now names `schedule_key`.
- S2 (diagnostics for corrupt scheduled runs) was already covered by the dispatch page.
  It is now pinned by a test.
- The review-fix tests give **6 runtime RED on `08b57a1`**
  (`review-red`: 42/48 of the O3 class) and pass after the fix.

**Review round 2** (fresh agent: Devin's tests plus the fix commit):
- No blockers. No existing assertion was weakened, the fixture matches the v8 schema, and
  the scenario list is covered.
- Orchestrator additions:
  - a blocked inconsistent run keeps its key (`ActiveExisting(blocked)`);
  - a stored non-incremental scheduled row fails closed everywhere (never due, direct
    claim `SCHEDULED_RUN_INCONSISTENT`, quarantined as an elder, reported in diagnostics);
  - the raw index releases the key for `cancelled` and for resolved `failed`/`blocked`;
  - a deferral leaves the claimant's `row_version` unchanged;
  - leading-whitespace and tab keys are rejected.
- These additions pin behaviour that was already correct, so they had no RED phase.
- The comment on the deferral commit rule now states the one diagnostic side effect: the
  probe bump commits together with an elder quarantine.

**Test infrastructure:** `A07CommandBoundaryTests.Handshake_maintenance_…` failed
intermittently in teardown with `IOException` on the DB file. A stopped worker finishes
its in-flight write with `CancellationToken.None` by design, and `SqliteTestDatabase`
deleted the directory first. The cleanup now retries (pool clear plus delete, up to 5
s). Only teardown is affected; no assertion changed.

**GREEN (final):** locked restore 0, Release Rebuild with 0 warnings / 0 errors, full run
**663 integration + 48 unit = 711/711**:
- in the main working copy twice (`main-final2`, `main-final3`; that copy keeps
  historical CRLF 002/003/005/006);
- in a clean `git archive` LF checkout (`clean-final2`).

Sources were stable in every run.

## Remaining limits and cutover gates

- **The first fault stops a schedule permanently** (review S4). Nothing writes
  `resolved_at_utc`, and the §8 resolution API is not built. Any scheduled run that is
  blocked (recovery `INTERRUPTED_NO_CHECKPOINT`/`UPLOAD_OUTCOME_UNKNOWN`,
  `SCHEDULED_RUN_INCONSISTENT`, `MANIFEST_INVALID`) or failed holds its key forever. This
  is intended fail-closed behaviour, and **resolution tooling is a cutover prerequisite**.
- Scheduled deferrals store no backoff; the future driver re-probes on its own tick.
- Full → incremental domain transition: the fingerprint includes the query mode, so the
  first scheduled incremental run after a manual `bootstrap_full` hits `DOMAIN_CHANGED`.
  That policy remains a separate cutover gate, as do the stable `source_namespace` (see
  the identity reconciliation) and legacy watermark reset.
- Workers, the legacy tick, `RecoverAsync` and the ERP client are not wired. A03, A04 and
  A05 are not claimed closed.
