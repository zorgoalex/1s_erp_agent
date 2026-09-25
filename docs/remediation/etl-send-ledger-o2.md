# O2 — durable ETL send-attempt ledger (migration 008) — DARK storage slice

Date: 2026-09-25. Worktree `agents/worktrees/etl-send-ledger-o2`, baseline `fb89fda`
(local review commit `8d1bda6`). Decisions D1–D3 approved by the user 2026-09-25.

**Scope delivered:** the durable admitted-attempt send ledger, owner-fenced
claim/ACK/outcome APIs on `IAgentStore`, fail-closed unknown-outcome handling,
recovery `admitted→orphaned` quarantine, retention/FK reconciliation, and the
`MaxBatchUploadAttempts` option with validation. **DARK only — not wired into
workers, `RecoverAsync`, or the ERP client.** The §8 manual-resolution API, O3,
legacy-bypass fencing, and production cutover are not part of this slice.

## Schema (008_etl_send_attempts.sql, LF, additive)

- `etl_batch_send_attempts(attempt_id PK, batch_id FK→etl_batches, attempt_no,
  owner_id, admitted_at_utc, finished_at_utc, outcome, http_status,
  ack_payload_hash, ack_observed_at_utc, last_error)` with
  `UNIQUE(batch_id,attempt_no)`, `CHECK(attempt_no>0)`, `CHECK(outcome IN
  ('admitted','precheck_failed','acknowledged','rejected_ack','unknown','orphaned'))`,
  `ix_etl_batch_send_attempts_batch`, and the partial unique
  `ux_etl_batch_send_attempts_admitted (batch_id) WHERE outcome='admitted'`
  (at most one live admitted send per batch at storage level).
- `etl_batches` += `send_attempt_id`, `upload_max_attempts`,
  `quarantine_code` (CHECK: `UPLOAD_OUTCOME_UNKNOWN|ACK_INVALID|
  UPLOAD_ATTEMPTS_EXHAUSTED|RUN_BLOCKED` or NULL), `row_version DEFAULT 1`.
- 001–007 byte-identical; catalog entry `[8] = 008_etl_send_attempts.sql` with
  canonical LF SHA-256 `2984DC879A62064E03F76CA1F2369DEF62DDBD21C2231F382F5583EFB1C2F373`;
  `SqliteMigrator.CurrentSchemaVersion = 8`.

## Changed files

| File | Change |
|---|---|
| `Persistence/Migrations/008_etl_send_attempts.sql` | new (LF) |
| `SqliteMigrator.cs` | `CurrentSchemaVersion` 7→8 |
| `MigrationChecksumCatalog.cs` | `[8]` entry, strict name+hash |
| `Abstractions/EtlSendAttempts.cs` | new contracts: `EtlDueBatchUpload`, `EtlBatchSendClaim`, `EtlBatchUploadClaimOutcome`, `EtlBatchAckEvidence`, `EtlBatchAckOutcome`, `EtlBatchSendFailureOutcome`, `EtlBatchSendRetryOutcome` |
| `Abstractions/Persistence.cs` | 5 new `IAgentStore` members + recovery doc |
| `Abstractions/EtlFinalize.cs` | `EtlRecoveryResult.AttemptsOrphaned` |
| `SqliteAgentStore.EtlSendAttempts.cs` | new partial: GetDue/TryClaim/Acknowledge/Fail/Retry + block helper |
| `SqliteAgentStore.EtlFinalize.cs` | recovery orphan/quarantine pass; `CommitBlockedRunAsync` + `TerminateRunAsync` set `quarantine_code='RUN_BLOCKED'` where applicable |
| `SqliteAgentStore.cs` | `CleanupAsync` deletes attempt rows in the same tx as the batch purge |
| `AgentOptions.cs` | `EtlOptions.MaxBatchUploadAttempts` (default 5) |
| `Program.cs` | one-line `Validate` extension `MaxBatchUploadAttempts is >= 1 and <= 20` |
| tests | `EtlSendLedgerRedTests` (runtime-RED suite), `EtlSendLedgerO2Tests` (25 methods / 32 cases incl. theories), `EtlSendAttemptsMigrationTests` + `Fixtures/v7-populated/populated-v7.sql`, `ErpClientUploadWireIdentityTests`, schema-version/pin updates in existing migration/checksum tests |

## API semantics (design §5)

- `TryClaimBatchUploadAsync` — ONE tx: savepoint → guarded write-first flip to
  `uploading` + `send_attempt_id` + persisted `upload_max_attempts` (identical-value
  enforcement) → `admitted` ledger insert (`RETURNING attempt_no`). Predicates: due
  status, run `running|uploading`, **full O1 three-way ownership/binding equality at
  positive epoch** (`RunOwnershipSetPredicate` + entity arm — no per-entity shortcut),
  no live `admitted` attempt, admitted-count < bound. Controlled zero-row insert →
  savepoint rollback + committed block (`SEND_LEDGER_LOST`); thrown SQL error →
  whole-tx rollback, batch stays claimable. Bound exhaustion → `Blocked`
  (`UPLOAD_ATTEMPTS_EXHAUSTED` + dead_letter + run/job block, same commit).
- `AcknowledgeClaimedBatchAsync` — fenced apply needs batch `uploading` + exact
  `send_attempt_id` + attempt `admitted` + run active + full ownership; valid ACK
  (status/batchId/checksumValid/rowsAccepted==row_count) acknowledges batch+attempt
  + run counter in one commit. Invalid under live fence → `Rejected` +
  `rejected_ack` + dead_letter `ACK_INVALID` + eager block. Dead fence / terminal
  attempt → first-observation evidence only (`LateEvidenceRecorded`); exact replay →
  `AlreadyObserved`; conflicting hash → `ObservationConflict`, never overwritten.
  Foreign attempt → `ClaimLost`, zero writes.
- `FailClaimedBatchSendAsync` — live fence → `unknown` + dead_letter
  `UPLOAD_OUTCOME_UNKNOWN` + run + job blocked + siblings `RUN_BLOCKED`, ownership
  retained; dead fence + admitted attempt → `unknown` evidence only; terminal/foreign
  → `ClaimLost` no-op (covers the late failure report on an acknowledged attempt).
- `RetryClaimedBatchSendAsync` — `precheck_failed` attestation only; live fence +
  durable bound → `retry_waiting` with caller `nextAttemptAtUtc`; bound reached →
  exhaustion block. Dead fence → attestation recorded on the attempt only.
- `RecoverInterruptedEtlRunsAsync` — orphans every `admitted` attempt, quarantines
  every `uploading` batch `UPLOAD_OUTCOME_UNKNOWN` (including ledger-less legacy
  in-flight batches), blocks their runs with ownership retained, then the existing
  interrupted-run pass (predicates extended to `UPLOAD_OUTCOME_UNKNOWN` runs).
  New-path `uploading` is never reset to `ready`. `RecoverAsync` unchanged.
- `CleanupAsync` — attempt rows delete only inside the batch-purge transaction
  (succeeded run + `deleted` + past retention). Unresolved-run evidence never
  passes the guard, so attempts are undeletable and never FK-orphaned.

## RED → GREEN

RED (runtime, baseline `fb89fda`): `EtlSendLedgerRedTests` — **6/6 real runtime
failures** (no compile-only asserts): missing ledger table/columns (SQLite errors),
`IAgentStore` lacking the five members (reflection), `EtlOptions` lacking the bound
(reflection), recovery leaving an in-flight `uploading` batch dispatchable, and
retention unable to reconcile attempt rows. Evidence:
`local-data/remediation-2026-09-25/etl-send-ledger-o2/red/` (exit=1, TRX
`o2-red_net10.0_20260925231432.trx`).

GREEN targeted: `EtlSendLedger*` + `EtlSendAttemptsMigration` — **41/41** (trx
`o2-int10`). Covers: one-commit claim+ledger+fence+bound; concurrent claimants on two
stores → exactly one admitted; ownership-set mismatch ×5 (missing/extra/wrong-epoch/
foreign/released) refused zero writes; admission while run `running`; bound
mismatch refused; precheck retry cycle + exhaustion block (both retry-side and
claim-side); unknown outcome quarantine + eager sibling block + retained ownership +
never reclaimable; ACK apply; invalid-ACK ×4 → `rejected_ack`+block; foreign attempt
ClaimLost; ACK/fail both orderings; exact replay preserved; conflicting observation
explicit; RAISE(IGNORE) controlled mismatch → block after savepoint rollback vs
RAISE(ABORT) → whole-tx rollback then convergent re-claim; due-enumeration
eligibility-before-LIMIT; recovery admitted→orphaned + legacy no-attempt uploading
quarantine; unresolved-run attempts undeletable; succeeded-run cleanup deletes
attempts FK-clean.

GREEN full suite: locked restore (exit 0), Release rebuild (exit 0, **0 warnings /
0 errors**), full `dotnet test` — **589 integration + 48 unit = 637/637** (baseline
594 → +43 new cases: 32 O2 + 6 RED + 3 migration + 1 wire identity + 1 hash pin). TRX `o2-final_*`. Source hash
manifest before/after the final run identical (`source-hashes-*.txt`).

## Decisions applied

- **D1** — `EtlOptions.MaxBatchUploadAttempts` default 5, validated 1–20 via the
  single existing `Validate` predicate at `Program.cs:110`. Store takes `maxAttempts`
  at claim (`$max`, `completion_max_attempts` pattern); persisted at first claim;
  identical-value enforced; bound counts ADMITTED attempts; exhaustion →
  `UPLOAD_ATTEMPTS_EXHAUSTED` + eager run block in the same tx.
- **D2** — the store receives caller `nextAttemptAtUtc`; no store-side schedule.
  Caller policy deferred to cutover wiring: `CommandPolicy.BackoffDelay(attempt,
  base: 5s, max: 5min)` + jitter against `EtlDueBatchUpload.PriorAttempts`.
  busy_entity/queued_overlap deferral semantics unchanged.
- **D3** — `status='dead_letter'` kept; `quarantine_code` carries the stable codes
  on batches (and `SEND_LEDGER_LOST` on the run for the controlled write-loss path);
  `last_error` stays diagnostic; no backfill — pre-008 rows keep `quarantine_code`
  NULL (legacy/unknown).

## Remaining limits — NOT closed here

- **DARK only**: nothing calls the new APIs in production; `EtlBatchUploadWorker`,
  `IErpClient`/`ErpClient`, `OnecEtlWorker`, `Program.cs` wiring are untouched, and
  `RecoverAsync` still resets legacy `uploading→ready` (the documented §9 bypass).
- **ERP dedup unproven**: wire-identity is proven byte-identical across re-sends
  (`ErpClientUploadWireIdentityTests`), but server-side dedup/lookup under
  `Idempotency-Key` is unverified — unknown outcomes stay permanently quarantined.
- **§9 legacy bypasses remain open** until the atomic cutover; the §8
  manual-resolution API is not implemented (no release path exists for
  `UPLOAD_OUTCOME_UNKNOWN` quarantines).
- A03/A04/A05 are not claimed closed by this slice.

## Root review and acceptance (orchestrator, 2026-09-26)

The implementer's 637/637 was reproduced independently. Two independent fresh-context
reviews followed; the orchestrator fixed their findings itself, runtime RED first.
Numbers above describe the implementer draft; this section is authoritative.

**Round 1 — 14 runtime RED on the draft** (`local-data/.../etl-send-ledger-o2-root/review-red`):

- **B1 (blocker): re-admission was decided by batch status.** After legacy
  `RecoverAsync` (`uploading→ready`, the production startup order) or
  `MarkBatchRetryAsync`, a batch whose attempt later became `unknown`/`orphaned` was due
  again and re-claimable, which meant a resend under unproven ERP dedup. Fix: a batch is
  admissible only while every prior attempt is `precheck_failed` (`GetDueBatchUploadsAsync`
  and the claim flip). A due batch with any other attempt is quarantined by the claim
  (`SEND_LEDGER_CONFLICT`, batch `UPLOAD_OUTCOME_UNKNOWN`). The dead-fence fail path
  also quarantines a batch left due.
- **B2 (blocker): recovery keyed on `status='uploading'` only.** Fix: the quarantine set
  is fixed before any write and includes due batches whose ledger holds a non-precheck
  attempt.
- **S4:** runs are blocked only for batches quarantined in *this* pass; historical
  quarantine evidence never re-blocks a run.
- **S1:** new `ack_valid` column. Every ACK observation records the store's field
  validation: 1 on apply, 0 on rejection, computed for late evidence.
- **S2:** an ACK for a `precheck_failed` attempt returns `AttestationContradicted`. The
  observation is recorded and the batch quarantined.
- **S3:** `ackPayloadHash` is required and non-empty. Previously a null hash made
  conflict detection vacuous.
- **S5:** every new-path `dead_letter` carries a code; `RUN_FAILED` and
  `SEND_LEDGER_LOST` were added to the CHECK list.

**Round 2 — 2 runtime RED on the round-1 fix** (`review2-red`):

- An ACK for a `precheck_failed` attempt now blocks an active run even when a later
  attempt already acknowledged the batch (double delivery; the run must not finalize).
- A late ACK on a batch reset outside the ledger quarantines the batch and blocks the
  run, the same as the late fail path.
- A schema pin test for the quarantine-code and `ack_valid` CHECK constraints was added.

Round 2 found no blockers: no path gives a batch with a non-precheck attempt a new
admission or returns it as due.

**Migration 008 changed before acceptance** (it was never accepted or published):
`ack_valid`, and 6 quarantine codes. The new LF SHA-256 is
`2984DC879A62064E03F76CA1F2369DEF62DDBD21C2231F382F5583EFB1C2F373`. Files 001–007 are
byte-identical (checked against the pre-work sha256 manifest).

**Behaviour changes to note:**

- The explicit, not production-wired `RecoverInterruptedEtlRunsAsync` now also
  quarantines ledger-less legacy `uploading` batches and blocks their runs. It fails
  closed, is consistent with the F1/O1 policy of blocking interrupted runs, and does not
  change legacy `RecoverAsync`.
- Dead-fence outcomes may now block runs in `pending`/`paused`/`completing` when their
  batch is still due. Terminal runs (`failed`/`blocked`/`succeeded`) keep their status;
  only the batch is fenced.
- Kept by design: a `failed` run whose in-flight batch later reports an unknown outcome
  stays `failed`. The unknown attempt is the evidence, and §8 resolution requires zero
  `admitted` attempts.

**Final verification (orchestrator):** locked restore 0, Release Rebuild with 0 warnings
and 0 errors, full test run **605 integration + 48 unit = 653/653**. Run both in the
worktree (`root-final2`) and in a clean `git archive` LF checkout (`clean-final`).
Source hashes were stable during each run. The baseline replay of the implementer's 6
RED tests on clean `fb89fda` gave 6/6 failed (5 structural absence, 1 behavioural
recovery). Evidence: `repo_1c-agent/local-data/remediation-2026-09-25/etl-send-ledger-o2-root/`.

**Checkout independence (found during the transfer to main):** the historical main
working copy keeps 002/003/005/006 as CRLF. Two implementer tests failed there
(603/605) and passed only in LF checkouts. The first was the resource-pin test, which
required LF bytes where the catalog accepts the known CRLF variant. The second was the
v7→v8 migration test, which expected raw-resource hashes where the ledger was seeded
canonically. Both were corrected. 008 must stay byte-exact LF in every checkout.
After the transfer: **653/653** in main (`main-final2`) and in a clean LF checkout
(`clean-final2`), with locked restore, Rebuild 0/0 and sources stable.
