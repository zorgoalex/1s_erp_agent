# ETL ownership + owner-fenced upload — bounded next-slice design (007)

Date: 2026-09-25, **revision 3** — root rejected revision 1, then returned a
bounded final correction on revision 2; both rounds are incorporated below.
Status: **design proposal only — not implemented, not tested, not externally
agreed. Root review required before implementation.** No code, schema, tests,
or packages were changed for this document; no production cutover is approved
or claimed.

Worktree: `agents/worktrees/etl-ownership-design`, worktree baseline `28750d8`
(504 tests — historical). Current main is `37c7012` / 509 tests (the legacy-path
ACK `status` fix landed separately on main; §2.4). Model: swe-2-high.

Sources: `docs/remediation/a05-finalize-design.md` rev3 + root disposition,
`docs/remediation/a05-finalize-storage.md` (accepted F1 DARK slice, schema v6),
`docs/remediation/etl-durable-design.md` rev2 + root disposition,
`docs/remediation/etl-job-foundation.md` (accepted 005 slice),
`docs/remediation/contract-decisions.md` (CD), `docs/architecture-audit-2026-09-23.md`
(AA §A03/A04/A05), current code cited inline. Line numbers are 1-based in this
worktree.

---

## 0. Scope, staging, and the two superseded rules

This document proposes the **next bounded DARK storage work** — durable
per-entity ownership held from run/job claim through extraction, upload and
completion, plus owner-fenced batch send/ACK/outcome — delivered as additive
migrations, new typed `IAgentStore` members, and real-SQLite tests in
**three staged slices** (§10) with one immutable migration each:

- **O1 / `007_etl_ownership.sql`** — `etl_entity_ownership` +
  `etl_run_ownership_bindings` + run-level extraction fence +
  `etl_jobs` dispatch/deferral/counter columns; exact-set ownership gates on
  every F1 API; transactional fair job claim; hard release inside finalize.
- **O2 / `008_etl_send_attempts.sql`** — `etl_batch_send_attempts` ledger +
  batch fence/bound columns; fail-closed unknown-outcome handling (no
  auto-replay); owner-fenced send/ACK/outcome APIs; eager run blocking.
- **O3 / `009_etl_scheduled_runs.sql`** — `schedule_key` + dedup index +
  `resolved_entities_json`; scheduled participation in the same claim/fairness
  machinery.

Each migration is immutable once accepted — no "007 migration(s)" plural, no
later edits; corrections go in the next numbered migration.

O1 alone is **not** a safe production state; it is a reviewable dark stage.
Production cutover requires O1+O2+O3 landed plus every §9 bypass fenced and the
§13 prerequisites — nothing here wires workers, `IErpClient`, `RecoverAsync`,
or configuration.

**Superseded rule 1 — release on failure.** The old `etl-durable-design.md`
released entity leases in the run-failure transaction. **Replaced:**
`failed`/`blocked`/unresolved runs **retain** ownership until attested manual
resolution (§8). A SQL status, a `dead_letter` row, or a local process exit
never proves remote quiescence.

**Superseded rule 2 — auto-replay of uncertain sends (root correction 3).**
Rev1 replayed an uncertain batch under the same `batch_id`. **Replaced:** a
crash/timeout/any non-ACK outcome after send admission is an *unknown remote
outcome* — it quarantines the batch and **blocks the run retaining ownership**.
Retry is admissible only with durable evidence the send was definitively never
performed (§5.3) or under a future verified dedup/lookup contract. The
`Idempotency-Key`/`X-Batch-Id` headers (`ErpClient.cs:57-64`) are sent but are
**not** a server-side dedup guarantee. The F1 `complete_payload_json` replay
remains isolated and externally gated — its existence proves nothing about
upload safety.

---

## 1. Terminology — lifetime ownership vs. execution fence (root correction 1)

Two different mechanisms, different lifetimes, never conflated:

- **Lifetime ownership** — `etl_entity_ownership` rows. Acquired atomically for
  the run's *entire* manifest at claim time; held through extraction, upload,
  `completing`, failure and restart; released only inside the successful
  finalize commit (§6) or the attested resolution commit (§8). It answers:
  *which run owns this entity's watermark domain until resolution.*
- **Execution fence** — a fresh GUID minted per attempt fencing one action:
  `etl_runs.extraction_claim_id` (one extraction pass over a run — §4.3),
  `etl_batches.send_attempt_id` → `etl_batch_send_attempts.attempt_id` (one
  admitted send — §5), `etl_runs.completion_claim_id` (one completion send —
  006). Owner columns are diagnostics only and are never the fence; a
  caller-supplied `ownerId` string can never substitute for the minted GUID.
  Released per attempt outcome, at seal, or by dead-process recovery — never by
  ownership release. It answers: *which single attempt may act right now.*

Releasing a fence never releases ownership; retaining ownership never implies
a live fence.

---

## 2. Verified current-state defects this design must close

1. **No durable ownership.** Nothing serializes an entity across the
   extraction→upload→finalize window; the F1 watermark CAS detects conflict
   only *after* a second run already read and uploaded overlapping ranges.
2. **F1 APIs are themselves unowned bypasses** (root correction 1):
   `BeginEtlEntityExtractionAsync`, `RegisterGuardedEtlBatchAsync`,
   `CompleteEtlEntityExtractionAsync`, `SealEtlRunExtractionAsync` and
   `TryClaimRunCompletionAsync` admit any run id in the right status — a run
   that never acquired ownership can capture bases, register batches, seal, and
   reach completion. They must be amended/wrapped, with **no compatibility
   exception admitting an unowned F1 run**.
3. **No job dispatch / no fair queue.** `etl_jobs` rows stay `pending` forever
   (`etl-job-foundation.md` §Limits); `OnecEtlWorker.RunAsync` mints a run GUID
   per trigger (`OnecEtlWorker.cs:52-55`).
4. **Batch upload is status-only** (`SqliteAgentStore.Etl.cs:42-60`), ACK/retry
   ownerless (`:63-85`), and `Status == "acknowledged"` is never validated
   (`EtlBatchUploadWorker.cs:47`; CD-B-1; `erp-agent-api.openapi.yaml:191-197`).
   *Note: the worker-side ACK `status` check on the legacy path was a separate
   narrow fix, accepted and pushed on main as `37c7012` (509 tests); this
   document changes no production wire behavior and leaves the existing retry
   policy there unchanged.*
5. **Uncertain sends are silently retried** (`MarkBatchRetryAsync` on any
   exception, `EtlBatchUploadWorker.cs:53-57`) — under unproven ERP dedup a
   replay can double-apply rows.
6. **No admitted-attempt ledger**: a late ACK has nothing to attach to; today
   it hits only the `status='uploading'` guard and leaves no evidence.

---

## 3. Data model — additive migrations (001–006 byte-identical; each new
migration immutable once accepted)

### 3.1 `007_etl_ownership.sql` (O1)

`etl_entity_ownership` — lifetime ownership (NOT a lease):

```text
entity_name TEXT PRIMARY KEY            -- at most one owner per entity, ever
owner_run_id TEXT NOT NULL              -- FK → etl_runs(run_id)
owner_job_id TEXT NULL                  -- FK → etl_jobs(job_id); NULL for scheduled runs
ownership_epoch INTEGER NOT NULL        -- +1 on every (re)acquisition; positive ABA token
acquired_at_utc TEXT NOT NULL
released_at_utc TEXT NULL               -- NULL = ACTIVE ownership
release_reason TEXT NULL                -- 'finalized' | 'manual_release' (reserved, §8)
updated_at_utc TEXT NOT NULL
row_version INTEGER NOT NULL DEFAULT 1
```

No TTL, no expiry predicate, no stale-takeover path — ever. A released row is
re-acquirable only via `released_at_utc IS NOT NULL`-guarded re-acquisition
(bumping `ownership_epoch`); an active row can never be taken over.

`etl_run_ownership_bindings` — immutable per-run/entity record of the exact
epoch captured at acquisition (root correction 3):

```text
run_id TEXT NOT NULL                  -- FK → etl_runs(run_id)
entity_name TEXT NOT NULL
expected_epoch INTEGER NOT NULL       -- the ownership_epoch this claim committed;
                                      -- always positive (insert=1, reacquire=old+1)
acquired_at_utc TEXT NOT NULL
PRIMARY KEY(run_id, entity_name)
```

Written inside the same claim transaction that acquires the ownership row;
immutable thereafter (no UPDATE path exists — a binding mismatch is evidence,
never repairable in place). Every ownership gate then checks `owner_run_id`
AND `released_at_utc IS NULL` AND `ownership_epoch = bindings.expected_epoch`,
so a released-then-reacquired row (epoch moved) fails closed even for the same
`owner_run_id`.

`etl_jobs` additive columns — dispatch fence + deferral:

```text
dispatch_owner_id TEXT NULL          -- diagnostics of the claiming pass; cleared on deferral
dispatch_claimed_at_utc TEXT NULL    -- diagnostics only
claim_attempt_count INTEGER NOT NULL DEFAULT 0   -- COMMITTED claims only (§4.3)
deferral_code TEXT NULL              -- 'busy_entity' | 'queued_overlap' | 'elder_manifest_invalid'
deferral_message TEXT NULL
available_at_utc TEXT NULL           -- next dispatch evaluation for 'deferred' jobs
```

`etl_runs` additive columns — the extraction execution fence (run-level so
scheduled/jobless runs use the identical mechanism in O3):

```text
extraction_claim_id TEXT NULL         -- fresh GUID minted by a committed run claim;
                                      -- REQUIRED by every extraction mutation API (§6.1)
extraction_claim_owner_id TEXT NULL   -- diagnostics only, never the fence
extraction_claim_acquired_at_utc TEXT NULL  -- diagnostics only
```

`status` CHECK already includes `deferred` (`005_durable_etl_jobs.sql:48`).
`command_id` stays `NOT NULL UNIQUE` — additive schema cannot relax it, so
scheduled work never becomes a job row (§4.4).

### 3.2 `008_etl_send_attempts.sql` (O2)

`etl_batch_send_attempts` — durable admitted-attempt ledger:

```text
attempt_id TEXT PRIMARY KEY          -- the send-attempt fence identity (fresh GUID)
batch_id TEXT NOT NULL               -- FK → etl_batches(batch_id)
attempt_no INTEGER NOT NULL          -- per-batch monotonic (COALESCE(MAX+1))
owner_id TEXT NOT NULL               -- dispatcher identity (diagnostics)
admitted_at_utc TEXT NOT NULL        -- claim commit time — admission, NOT "sent"
finished_at_utc TEXT NULL
outcome TEXT NOT NULL                -- 'admitted'|'precheck_failed'|'acknowledged'|
                                     -- 'rejected_ack'|'unknown'|'orphaned'
http_status INTEGER NULL
ack_payload_hash TEXT NULL           -- evidence hash of the observed ACK body
ack_observed_at_utc TEXT NULL        -- late-ACK evidence timestamp (§5.4)
last_error TEXT NULL
UNIQUE(batch_id, attempt_no)
```

`admitted` is the only pre-network state: it proves a send was *admitted*,
never that bytes left the process. `precheck_failed` records a **trusted worker
attestation** that the network call was never invoked — the ledger stores the
attestation; it is not independent network proof (root correction 5). An
attempt is single-use: `acknowledged`, `rejected_ack`, `unknown`, `orphaned`
are terminal — no attempt is ever re-armed; a bounded *new* attempt exists
only for `precheck_failed` retries (§5.3).

`etl_batches` additive columns:

```text
send_attempt_id TEXT NULL            -- == etl_batch_send_attempts.attempt_id of the live attempt
upload_max_attempts INTEGER NULL     -- durable bound on ADMITTED sends, persisted at first claim
row_version INTEGER NOT NULL DEFAULT 1
```

### 3.3 `009_etl_scheduled_runs.sql` (O3)

```text
etl_runs.schedule_key TEXT NULL            -- e.g. 'incremental'; NULL for job runs
etl_runs.resolved_entities_json TEXT NULL  -- frozen effective definitions for runs WITHOUT
                                           -- an etl_jobs row (root correction: names alone
                                           -- give Begin no authority to compare against)
```

```sql
CREATE UNIQUE INDEX IF NOT EXISTS ux_etl_runs_schedule_active ON etl_runs(schedule_key)
WHERE schedule_key IS NOT NULL
  AND (status IN ('pending','running','uploading','completing')
       OR (status IN ('failed','blocked') AND resolved_at_utc IS NULL));
```

---

## 4. Durable claim — exact ownership, transactional fairness

### 4.1 Atomic all-entity acquisition + immutable bindings

Inside the claim transaction's `SAVEPOINT` (§4.3), for each manifest entity in
Ordinal order, in this exact shape order:

```sql
-- (a) re-acquire a released row
UPDATE etl_entity_ownership SET owner_run_id=$run, owner_job_id=$job,
    ownership_epoch=ownership_epoch+1, acquired_at_utc=$now, released_at_utc=NULL,
    release_reason=NULL, updated_at_utc=$now, row_version=row_version+1
WHERE entity_name=$e AND released_at_utc IS NOT NULL;
-- (b) only if (a) changed 0: insert guarded against ANY existing row — never
-- a PK collision against a released row left in place, never an overwrite of
-- an active one
INSERT INTO etl_entity_ownership(entity_name,owner_run_id,owner_job_id,ownership_epoch,
    acquired_at_utc,updated_at_utc,row_version)
SELECT $e,$run,$job,1,$now,$now,1
WHERE NOT EXISTS (SELECT 1 FROM etl_entity_ownership o WHERE o.entity_name=$e);
```

Exactly one of (a)/(b) must change 1 row; both changing 0 means the row exists
and is active → conflict → `busy_entity` deferral (§4.3). On success the same
statement group inserts the immutable binding with the epoch actually
committed (`expected_epoch = 1` for (b), `old+1` for (a) — read back in-tx,
never assumed):

```sql
INSERT INTO etl_run_ownership_bindings(run_id,entity_name,expected_epoch,acquired_at_utc)
SELECT $run,$e,o.ownership_epoch,$now FROM etl_entity_ownership o
WHERE o.entity_name=$e AND o.owner_run_id=$run AND o.released_at_utc IS NULL;
```

0 rows → the just-written ownership row is inconsistent → conflict, same
deferral path (fail closed; a binding can never be repaired in place).

### 4.2 Elder-overlap reservation — queue policy enforced in the claim tx
(root correction 2)

Rev1's `ORDER BY created_at_utc, job_id LIMIT n` + retry-later was **not**
fair: a busy head could hide disjoint jobs behind `LIMIT`, a not-yet-due
deferred job left the window so a newer overlapping job kept winning, and a
direct `TryClaim`/scheduled start bypassed enumeration entirely. The corrected
rule is enforced **inside every claim transaction**, not by the enumerator:

A claimant run `R` with manifest `S` may acquire only if **no older pending
run `R'` overlaps `S`**:

```sql
EXISTS (SELECT 1 FROM etl_runs r2
        WHERE r2.status='pending' AND r2.run_id <> $run
          AND (r2.created_at_utc < $cCreated
               OR (r2.created_at_utc = $cCreated AND r2.run_id < $run))
          AND NOT EXISTS (SELECT 1 FROM etl_jobs jx
                          WHERE jx.run_id=r2.run_id
                            AND jx.status IN ('blocked','finished','cancelled'))
          AND json_valid(r2.requested_entities_json)
          AND EXISTS (SELECT 1 FROM json_each(CASE WHEN json_valid(r2.requested_entities_json)
                                      THEN r2.requested_entities_json ELSE '[]' END) e2
                      JOIN json_each($manifestJson) e1 ON e1.value = e2.value))
```

- Any non-pending non-terminal run already holds ownership (acquired at its
  claim), so the reservation only needs to order *pending* runs against each
  other — older pending waiters reserve their overlap before any newer claim.
- This is deterministic overlap priority — **not** a literal arrival FIFO:
  `(r2.created_at_utc, r2.run_id)` is a total order on *runs* that tolerates
  timestamp ties and backdated rows (root correction 6). The oldest pending
  overlapping run has no elder blocker and claims as soon as ownership frees;
  a newer overlapping claim can never pass it — including via direct
  `TryClaim` by id or a scheduled start, which evaluate the identical
  predicate. This is an ETL-scope order — not a reuse of the command-side
  `queue_sequence` semantics (003).
- **Malformed elder manifest — fail closed, detectable, never silently empty:**
  validate the full typed manifest, not just JSON syntax (see root disposition).
  An invalid elder is quarantined with visible MANIFEST_INVALID diagnostics and
  the claimant defers elder_manifest_invalid. It may leave the reservation only
  when durable state proves it never started and has no effects/evidence as
  specified in the root disposition; otherwise unresolved quarantine stops
  admission pending explicit resolution. CASE around json_each is only an SQL
  evaluation guard and never a semantic assumption that the manifest is empty.

### 4.3 `TryClaimEtlJobAsync(jobId, ownerId, deferUntilUtc, nowUtc, ct)` — ONE tx

Savepoint discipline (root corrections 1+6): ownership writes must be
reversible independently of the deferral commit, so the `SAVEPOINT` is opened
**immediately before** the acquisition loop — not after any tentative status
write. The transaction is:

1. **Probe-guard write** (serializes claimants on the row lock, no status
   change): `UPDATE etl_jobs SET row_version=row_version+1, updated_at_utc=$now
   WHERE job_id=$j AND (status='pending' OR (status='deferred' AND
   (available_at_utc IS NULL OR available_at_utc <= $now)))` → 0 rows:
   classify read-only → `NotClaimable`.
2. Read job+run in-tx; verify run `pending`, mode/`configuration_version`
   agree, frozen `entities_json` manifest equals run `requested_entities_json`.
   Violation → job `blocked` + run `blocked` `JOB_MANIFEST_INCONSISTENT`
   (committed; corrupt evidence never dispatches).
3. **Malformed-elder quarantine** (§4.2): a pending elder with an invalid typed
   manifest is handled under the never-started/evidence preconditions of §4.2
   and the root disposition; claimant → deferred elder_manifest_invalid.
   Unproven effects retain a visible admission quarantine.
4. **Elder-overlap check** (§4.2): any elder → job `deferred` +
   `queued_overlap` + `available_at_utc=$deferUntil`, dispatch fields cleared,
   `claim_attempt_count` **unchanged** → commit `Deferred(QueuedOverlap)`.
5. `SAVEPOINT acquire;` — **ownership + binding acquisition** (§4.1). Any
   conflict → `ROLLBACK TO acquire; RELEASE acquire;` — every acquired and
   re-acquired row and every binding insert is undone (a conflict on entity 2
   leaves entity 1's prior owner untouched and entity 1 unowned by this run)
   → then the guarded deferral UPDATE `status='deferred', deferral_code=
   'busy_entity', available_at_utc=$deferUntil, dispatch_owner_id=NULL,
   dispatch_claimed_at_utc=NULL` → commit `Deferred(BusyEntity)`.
6. Success → `RELEASE acquire` → run `pending→running` + `started_at_utc` +
   **`extraction_claim_id=$newGuid`** (+ owner/acquired diagnostics) → job
   `status='running', dispatch_owner_id=$owner, dispatch_claimed_at_utc=$now,
   claim_attempt_count=claim_attempt_count+1, deferral_code=NULL,
   available_at_utc=NULL` — one commit → `Claimed{jobId, runId,
   extractionClaimId, mode, frozen definitions}`.

The minted `extraction_claim_id` is the **authoritative execution fence for
the extraction phase** (root correction 4): a fresh GUID, not the caller's
`ownerId`. `BeginEtlEntityExtractionAsync`, `RegisterGuardedEtlBatchAsync`,
`CompleteEtlEntityExtractionAsync` and `SealEtlRunExtractionAsync` take it as a
required argument and gate on it (§6.1); `Seal` clears it (the extraction
fence ends at seal), and fail/block/recovery clear it appropriately — all
while ownership is retained. No unguarded overload of these APIs exists or is
added — no bypass.

`claim_attempt_count` counts committed claims only (diagnostic, unbounded —
the claim is a single atomic tx with no external call inside; no bound needed,
§12).

### 4.4 Scheduled work — `EnsureScheduledEtlRunAsync` + same claim path (O3)

A tick inserts a **`pending`** run (`mode='incremental'`, `schedule_key`,
`requested_entities_json` = selected codes, **`resolved_entities_json` = the
full frozen effective definitions + `configuration_version`** — root
correction 5: names alone leave `Begin` with no authority to compare against,
since no job row exists). The dedup index (§3.3) makes this idempotent: an
existing active/unresolved run → `ActiveExisting(runId)`, zero writes; a
`failed`/`blocked` unresolved run keeps the key, so faults can never mint
successor queues. The pending scheduled run then claims through
`TryClaimScheduledRunAsync` — the same reservation + ownership + bindings +
`pending→running` transaction as jobs (`owner_job_id NULL`, minting its own
fresh `extraction_claim_id`), so scheduled and manual waiters share one
deterministic overlap priority (§4.2). In O3,
`BeginEtlEntityExtractionAsync` additionally compares the caller-supplied
definition against `resolved_entities_json` for jobless runs
(`RunDefinitionMismatch`; §6.1). **Until O3 exists there is no frozen identity
source for a jobless run — `Begin` rejects any run without a job row; there is
no compatibility path admitting one.**

### 4.5 Eligibility enumeration — filter BEFORE LIMIT

`GetDispatchableEtlJobsAsync(limit, now)` / `GetDueRunStartsAsync(limit, now)`
apply the §4.2 reservation predicate **and** the ownership-availability
predicate in SQL *before* `LIMIT`, ranked by **run** `(created_at_utc,
run_id)` — the §4.2 order key, not job fields — with keyset pagination. A
busy/deferred head can never hide disjoint eligible work and the enumerator
can never bypass the in-tx authority. Eligibility (`CanExtract`, pause, spool)
stays a worker-side decision point evaluated before claiming — the same
documented non-atomic admission seam as A07b `mayStartNewWork`
(`CommandExecutionWorker.cs:61-71`).

---

## 5. Owner-fenced send — admission ledger, fail-closed outcomes (O2)

### 5.1 `GetDueBatchUploadsAsync(limit, nowUtc, ct)`

```sql
-- 'retry_waiting' exists ONLY for proven-unsent precheck retries (§5.3)
SELECT ... FROM etl_batches b
WHERE (b.status='ready'
       OR (b.status='retry_waiting' AND (b.next_attempt_at_utc IS NULL OR b.next_attempt_at_utc <= $now)))
  AND EXISTS (SELECT 1 FROM etl_runs r WHERE r.run_id=b.run_id
              AND r.status IN ('running','uploading'))
  AND EXISTS (SELECT 1 FROM etl_entity_ownership o WHERE o.entity_name=b.entity_name
              AND o.owner_run_id=b.run_id AND o.released_at_utc IS NULL)
  AND NOT EXISTS (SELECT 1 FROM etl_batch_send_attempts a WHERE a.batch_id=b.batch_id
                  AND a.outcome='admitted')
ORDER BY b.created_at_utc, b.batch_id LIMIT $limit;
```

Run-active + entity-owned are admission predicates evaluated again in the
claim; `blocked`/`failed` runs admit nothing (eager block, §5.5).

### 5.2 `TryClaimBatchUploadAsync(batchId, ownerId, nowUtc, maxAttempts, ct)` — ONE tx

Guarded write-first on the batch (`status IN ('ready','retry_waiting')` + due +
run-active + entity-owned + no live `admitted` attempt + `attempt` bound
`upload_max_attempts` persisted at first claim, identical-value enforcement —
same pattern as `completion_max_attempts`, `SqliteAgentStore.EtlFinalize.cs:368-381`):
flips `status='uploading'`, sets `send_attempt_id=$attemptId`, and **inserts
the `admitted` ledger row in the same commit**. The ledger row is the durable
record that a send was *admitted* under this identity; it says nothing about
the network. Loser/exhausted → `NotClaimed` / `Quarantined` (bound reached →
`dead_letter UPLOAD_ATTEMPTS_EXHAUSTED` + run block in the same tx, §5.5).

### 5.3 Outcome taxonomy — no uncertain auto-replay

| Worker observation | Durable outcome | Disposition |
|---|---|---|
| Failure **before** the network call is invoked (spool open/hash read throws inside `UploadBatchAsync` argument preparation — positively *pre-invocation*) | attempt `precheck_failed`; batch `retry_waiting` + backoff | Bounded retry is admissible: the ledger proves no send occurred. Bound exhaustion → `dead_letter` + run block. |
| Valid ACK (§5.4) | attempt `acknowledged`; batch `acknowledged` + run counter | normal |
| Invalid/malformed ACK (wrong `status`/`batchId`/`checksumValid`/`rowsAccepted`) | attempt `rejected_ack`; batch `dead_letter ACK_INVALID` | **run blocked eagerly**, evidence kept |
| Timeout, transport exception, non-2xx, process death after admission | attempt `unknown` (or `orphaned` at recovery) | **batch `dead_letter UPLOAD_OUTCOME_UNKNOWN`; run blocked; ownership retained; NO resend** |

A non-2xx response is *not* proof of no remote effect; absence of response is
never proof either. The only safe replays are `precheck_failed` (ledger-proven
unsent) and whatever a future verified ERP dedup/lookup contract authorizes.

### 5.4 `AcknowledgeClaimedBatchAsync(batchId, attemptId, ackFields, ackPayloadHash, ct)`
(root corrections 4+5)

The ACK is bound to the durable admitted attempt, never to a bare batch id or
the `uploading` pointer alone. In ONE transaction:

1. **Fenced apply** — all predicates must hold together: batch
   `status='uploading' AND send_attempt_id=$attemptId`, the ledger row
   `(batch_id, attempt_id)` exists with `outcome='admitted'` (the attempt is
   still current — an `acknowledged`/`unknown`/`orphaned` attempt can never be
   re-acknowledged), the parent run is active (`running`/`uploading`), and the
   entity's ownership row is active for this run at the bound epoch (§6). Then
   validate `ack.Status='acknowledged'` + `BatchId` + `ChecksumValid` +
   `RowsAccepted==row_count` → batch `acknowledged`, attempt `acknowledged`
   (`ack_payload_hash`, `finished_at_utc`), run `batches_acknowledged+1` —
   one commit → `Acknowledged`.
2. **Matched late outcome** — the live fence fails (run already `blocked`,
   batch already fenced, attempt already terminal) **but** the `(batch_id,
   attempt_id)` ledger row exists → the ACK is real evidence belonging to that
   attempt: the same field validation runs (`status`/`batchId`/`checksum`/
   `rows` — an invalid ACK body is recorded as evidence, never applied), then
   `ack_observed_at_utc` + `ack_payload_hash` are written **on that attempt
   row only** → `LateEvidenceRecorded`. Batch status, run, job, ownership
   untouched — a late ACK never resurrects, never releases.
3. **Foreign attempt** — no ledger row for `(batch_id, attempt_id)` →
   `ClaimLost`, **zero writes**: an arbitrary or foreign attempt identity can
   write nothing.
4. **Invalid ACK under a live fence** → `Rejected` with the §5.3 quarantine
   (`rejected_ack` + batch `dead_letter ACK_INVALID`) + eager run block in the
   same commit.

Ordering rule (root correction 5): a **late failure report for a
same-attempt, already-acknowledged** attempt is a no-op (`ClaimLost` — the
attempt is terminal); a *run-level* failure arriving after the ACK
(`FailEtlRunAsync`/block on other evidence) still blocks the run normally —
the acknowledged batch row is evidence and stays `acknowledged`.

### 5.5 Eager run blocking — stop sibling admissions

`FailClaimedBatchSendAsync(batchId, attemptId, error, ct)` (uncertain outcome)
and the invalid-ACK path commit, in ONE fenced transaction: attempt outcome +
batch `dead_letter` + run `blocked` (`UPLOAD_OUTCOME_UNKNOWN`/`ACK_INVALID`)
+ job `blocked` — **ownership retained**. A blocked run instantly fails the
run-active admission predicate, so no *new* sibling batch claims are admitted.
Batches already `admitted` at that instant cannot be recalled — the local
fence never cancels in-flight HTTP; their late outcomes land as
`LateEvidenceRecorded`/`unknown` on a blocked run, evidence preserved.

### 5.6 `RetryClaimedBatchSendAsync(batchId, attemptId, error, nextAttemptAtUtc, ct)`

Fenced on `(uploading, send_attempt_id)` + ledger `outcome='admitted'`;
admissible only to record `precheck_failed` — the worker's trusted attestation
that the network call was never invoked (the ledger stores the attestation; it
is not independent network proof) → batch `retry_waiting` +
`next_attempt_at_utc`; bound reached → `dead_letter` + eager run block. Any
post-invocation failure uses §5.5 instead — there is no generic retry API for
uncertain sends. Stale `attempt_id` → `ClaimLost`, zero writes.

---

## 6. Ownership as a hard gate on the whole F1 path (root correction 1)

The CAS sees only the committed cursor — it cannot see another owner's reads
or in-flight uploads. Ownership is therefore a **transactional precondition**,
not diagnostics:

### 6.1 Extraction-fence + binding-epoch gate on every mutation

Every extraction mutation API gains **two** required guards inside its
existing guarded write (root corrections 3+4):

- **Execution fence**: `BeginEtlEntityExtractionAsync`,
  `RegisterGuardedEtlBatchAsync`, `CompleteEtlEntityExtractionAsync`,
  `SealEtlRunExtractionAsync` take the `extractionClaimId` minted by the run
  claim (§4.3) as a required argument and predicate every guarded write on
  `r.extraction_claim_id=$claim` — a stale/foreign/missing claim rejects with
  zero writes. There is no unguarded overload. `Seal` clears
  `extraction_claim_id` in its success commit — the extraction fence ends at
  seal; `Fail`/`Block`/recovery clear it while retaining ownership.
- **Ownership gate**: the run must hold **active ownership of the exact
  manifest set** — evaluated against `etl_run_ownership_bindings` at the
  captured epoch, not just "this entity is owned":

```sql
-- Run only after validating the manifest as a nonempty unique string array.
-- Every requested entity has a binding AND active ownership at that exact positive epoch.
NOT EXISTS (SELECT 1 FROM json_each($manifestJson) e
  WHERE NOT EXISTS (SELECT 1 FROM etl_run_ownership_bindings b
    JOIN etl_entity_ownership o ON o.entity_name=b.entity_name
    WHERE b.run_id=$run AND b.entity_name=e.value AND b.expected_epoch>0
      AND o.owner_run_id=$run AND o.released_at_utc IS NULL
      AND o.ownership_epoch=b.expected_epoch))
-- No extra binding, and no extra active ownership: counts alone are insufficient.
AND NOT EXISTS (SELECT 1 FROM etl_run_ownership_bindings b
  WHERE b.run_id=$run AND b.entity_name NOT IN (SELECT value FROM json_each($manifestJson)))
AND NOT EXISTS (SELECT 1 FROM etl_entity_ownership o
  WHERE o.owner_run_id=$run AND o.released_at_utc IS NULL
    AND o.entity_name NOT IN (SELECT value FROM json_each($manifestJson)))
```

Foreign-owned, released, wrong-epoch (released-then-reacquired — including
re-acquired by the *same* run id, which bumps the epoch and invalidates the
binding), missing, or extra ownership state rejects fail-closed
(`EntityNotOwned`/`OWNERSHIP_SET_MISMATCH`), zero writes. `Begin` additionally
enforces the frozen identity source: job's `entities_json` when a job row
exists (existing `JobDefinitionMismatch`); **a jobless run is rejected
outright until O3** lands `resolved_entities_json` (§4.4) — no compatibility
loophole admits unowned or unidentifiable legacy F1 runs.

### 6.2 Exact-set re-verification — seal, completion claim, finalize

`SealEtlRunExtractionAsync`, `VerifyClaimReadinessAsync` (inside
`TryClaimRunCompletionAsync`) and `FinalizeEtlRunAsync` re-verify **three-way
set equality** — manifest (`requested_entities_json`) == bindings
(`etl_run_ownership_bindings` for this run) == active ownership rows owned by
this run at the bound epochs — in both directions. A missing binding, a
binding without its active ownership row, an active row without a binding, a
foreign-owned or released entity, or a wrong epoch all fail closed →
`OWNERSHIP_SET_MISMATCH` block with evidence preserved (never a claim, never
a send).

### 6.3 Release is a HARD gate — savepoint spans watermarks AND release

Rev2 released the CAS savepoint before the ownership release, which made a
release-mismatch rollback impossible inside the same commit — corrected. The
success path of `FinalizeEtlRunAsync`
(`SqliteAgentStore.EtlFinalize.cs:559-580`) becomes:

1. `SAVEPOINT etl_finalize_cas` — all per-entity CAS writes, then the
   ownership release **inside the same savepoint**:

   ```sql
   UPDATE etl_entity_ownership AS o SET released_at_utc=$now,
       release_reason='finalized', updated_at_utc=$now, row_version=row_version+1
   WHERE o.released_at_utc IS NULL
     AND EXISTS (SELECT 1 FROM etl_run_ownership_bindings b
                 WHERE b.run_id=$run AND b.entity_name=o.entity_name
                   AND o.owner_run_id=$run AND o.ownership_epoch=b.expected_epoch);
   ```

   The epoch-bound release can never free a re-acquired (ABA) or foreign row.
2. Count check: CAS writes must each have changed 1 AND the release must have
   changed **exactly `sealed_entity_count`** rows.
3a. All satisfied → `RELEASE etl_finalize_cas` → `succeeded` run +
    `finished` job → `COMMIT` → `Finalized`. Success is exactly: *all
    watermarks + succeeded run + finished job + exactly-manifest ownership
    release* in ONE commit.
3b. **Any** mismatch (CAS or release count) → `ROLLBACK TO etl_finalize_cas;
    RELEASE etl_finalize_cas` — watermark writes AND the partial release are
    both undone — then one commit writes `blocked` run + conflict
    (`GENERATION_MISMATCH`/`OWNERSHIP_RELEASE_MISMATCH`/…) + `blocked` job.
3c. A SQL/fault exception anywhere → the whole transaction rolls back:
    `completing` + claim + payload persist for fenced retry — distinct from
    the controlled-mismatch path, which always commits the block.

`FailEtlRunAsync`/`BlockEtlRunAsync`/`CommitBlockedRunAsync` never release
ownership; `CommitBlockedRunAsync` additionally fences the run's pending
batches to `dead_letter` (`RUN_BLOCKED`) so a blocked run leaves no
dispatchable rows.

---

## 7. Startup recovery — exclusive host, retain ownership, quarantine admits

`RecoverInterruptedEtlRunsAsync` (`SqliteAgentStore.EtlFinalize.cs:635-660`),
still explicit/startup-only under the exclusive-host precondition
(`SingleInstanceLock`, `BootstrapService.cs:28`), is extended — and its batch
rule **changes** under the fail-closed model:

- `completing` claims released unchanged (completion replay policy stays
  isolated/externally gated as today).
- `running` runs → `blocked INTERRUPTED_NO_CHECKPOINT`; extracting entities
  `failed`; jobs `blocked`; dead `extraction_claim_id` fences cleared —
  **ownership and bindings retained** in all cases (the bindings stay the
  evidence of exactly what the interrupted run owned).
- `pending`/`deferred` jobs and `pending` runs untouched.
- **`uploading` batches are NEVER reset to `ready`** on the new path: an
  `admitted` attempt with no finish = unknown remote outcome → attempt
  `orphaned`, batch `dead_letter UPLOAD_OUTCOME_UNKNOWN`, run `blocked`,
  ownership retained. (The legacy `RecoverAsync` `uploading→ready` reset —
  `SqliteAgentStore.cs:31` — is a §9 bypass to be fenced at cutover, not reused.)
- No time-based stealing exists anywhere: no `acquired_at_utc < stale`
  predicate is defined for ownership, job dispatch, batch sends, or completion.

---

## 8. Manual resolution — quiescence + attestation boundary (deferred)

`resolved_at_utc` exists (006); the resolution API is **not** built in this
work. Its boundary is fixed now (root correction 7 — fencing `uploading` rows
inside the release tx cannot stop an already-admitted sender acting later):

- Preconditions (all durable, checked in the resolution tx): run
  `failed`/`blocked` + `resolved_at_utc IS NULL` + **no `admitted` send
  attempts exist for any batch of the run** + no live completion claim
  (`completion_claim_id IS NULL`) + no live extraction/send/dispatch fence.
- Plus a **local quiescence boundary** (root correction 7): the API is
  invocable only under the documented exclusive-maintenance procedure in which
  the dispatchers/upload workers are **stopped AND drained** — already
  admitted in-flight sends have run to a recorded outcome — not merely a mode
  that blocks *new* sends. Blocking new admissions does not quiesce a send
  already in flight; the procedure must prove the in-flight set is empty
  (durable: zero `admitted` attempts; operational: workers stopped/drained).
  The store verifies the durable preconditions; the procedure verifies
  dispatcher quiescence — neither is substituted for the other.
- Operator attestation records *verified remote quiescence/reconciliation*
  (what was checked at ERP, what re-baseline/re-extract decision was taken) —
  it is the boundary of honesty, not a generic note; the API never derives or
  invents ERP proof.
- Effect, ONE commit: `resolved_at_utc` + attestation recorded; remaining
  pending batches `dead_letter RUN_RESOLVED`; ownership released
  `manual_release`; job stays `blocked` (never `finished`). After resolution a
  **new** command/job may retry the work; original evidence is immutable.

---

## 9. Bypasses to fence atomically at cutover — including the F1 path

| # | Bypass | Site | Defeats |
|---|---|---|---|
| 1 | `CreateEtlRunAsync` mints unowned `running` runs | `SqliteAgentStore.Etl.cs:10-17`; `OnecEtlWorker.cs:52-55` | ownership, dedup, manifest identity |
| 2 | Unguarded `RegisterBatchAsync` | `SqliteAgentStore.Etl.cs:19-31` | per-entity ownership, seal |
| 3 | `GetPendingBatchesAsync` status-only claim | `SqliteAgentStore.Etl.cs:42-60` | admission predicates, attempt ledger |
| 4 | `MarkBatchRetryAsync`/`AcknowledgeBatchAsync` ownerless | `SqliteAgentStore.Etl.cs:63-85` | attempt fence, ACK validation, unknown-outcome rule |
| 5 | `GetRunsReadyToCompleteAsync` + `CommitWatermarkAsync` + `CompleteEtlRunAsync` | `SqliteAgentStore.Etl.cs:113-219`; `EtlBatchUploadWorker.cs:24-36` | atomic finalize, generation, ownership release |
| 6 | `MarkEtlRunExtractedAsync` unguarded | `SqliteAgentStore.Etl.cs:33-40` | seal |
| 7 | `RecoverAsync` `uploading→ready` reset | `SqliteAgentStore.cs:31` | uncertain-outcome quarantine (auto-replays ambiguous sends) |
| 8 | `EtlTrigger` RAM channel as work source | `EtlTrigger.cs`; `CommandExecutionWorker.cs:91-126` | durable acceptance |
| 9 | `OnecEtlWorker` tick + extraction loop | `OnecEtlWorker.cs:23-39,60-87` | scheduled dedup, frozen definitions, capture gates |
| 10 | Retention paths | `SqliteAgentStore.Etl.cs:87-111`; `SqliteAgentStore.cs:317-336` | parent-`succeeded` guard must hold; `deleted` never satisfies readiness |
| 11 | `EtlBatchUploadWorker` completion loop | `EtlBatchUploadWorker.cs:24-36` | whole legacy complete route |
| 12 | **F1 APIs without ownership/fence** | `SqliteAgentStore.EtlFinalize.cs` `Begin`/`RegisterGuarded`/`CompleteEntity`/`Seal`/`TryClaimRunCompletion`/`FinalizeEtlRun` | admit unowned runs and unclaimed extractions; release ungated — amended per §4.3/§6 (claim-id signatures, exact-set gates, hard release); no unguarded overload is kept or added; O1 adapts the F1 tests to the new signatures and proves unowned-legacy rejection |

Recorded F2 gates unchanged (`a05-finalize-storage.md` §Remaining): worker
wiring, `IErpClient` raw-body overload, configured `source_namespace`, domain
reset/transition procedure, fault tests, runbook, and the A04 spool
intent/checkpoint protocol — batch rows in this design still appear only after
the file is `ready`; the `FileSpoolStore.WriteBatchAsync`→registration crash
window stays open until A04 lands.

---

## 10. Staging and dependencies (bounded slices)

| Stage | Migration | Content | Depends on | Behaviour change |
|---|---|---|---|---|
| **O1** | `007_etl_ownership.sql` | ownership + bindings tables; `etl_runs` extraction-fence columns; `etl_jobs` dispatch/deferral/counter columns; job claim tx (reservation + all-entity acquisition + bindings + claim mint); §6 fence+ownership gates on F1 APIs (new signatures); hard release inside finalize; recovery retaining ownership and clearing dead extraction claims; `CommitBlockedRunAsync` batch fencing; F1 tests adapted to the claim-id signatures; unowned-legacy rejection proven | none — standalone dark | none wired |
| **O2** | `008_etl_send_attempts.sql` | send-attempt ledger + batch fence/bound columns; fenced claim/ACK/outcome APIs; fail-closed unknown handling; eager run block; recovery `admitted→orphaned` quarantine | O1 (admission needs ownership predicates; block path needs §6 machinery) | none wired |
| **O3** | `009_etl_scheduled_runs.sql` | `schedule_key` + dedup index + `resolved_entities_json`; `EnsureScheduledEtlRunAsync` + `TryClaimScheduledRunAsync`; scheduled↔manual shared overlap priority; `Begin` `RunDefinitionMismatch` path | O1 (claim machinery); schema application follows O2/008 because migration numbering is fixed; API design may be reviewed independently | none wired |

The split is fixed, not advisory: **exactly** `007` for O1, `008` for O2,
`009` for O3; each migration immutable once accepted — any later correction
ships as the next numbered migration, never an edit. O1 deliberately rejects
jobless capture until O3 provides the frozen identity — that is a designed
restriction, not a gap. All stages remain DARK; **O1 alone is not a safe
production state** and no interim cutover is claimed.

---

## 11. Required tests (real migrated SQLite, `SqliteTestDatabase`; conventions
per `EtlFinalizeStorageTests.cs`/`EtlJobFoundationTests.cs`)

**Ownership gates + extraction fence (O1):**
- Begin/RegisterGuarded/CompleteEntity on an unowned, foreign-owned, released,
  or **wrong-epoch** entity (released-then-reacquired — including by the same
  run id, which invalidates the binding) → rejected, zero writes; three-way
  set equality enforced at every extraction mutation, not only at seal; replacing
  a non-touched required entity B with extra C at equal count must also reject.
- Stale/foreign/missing `extraction_claim_id` on any extraction mutation →
  rejected, zero writes; no unguarded overload exists.
- `Seal` clears `extraction_claim_id` in its success commit — a post-seal
  mutation under the old claim rejects; fail/block/recovery clear the fence
  while ownership+bindings persist.
- Jobless `Begin` (legacy run, no job row) → rejected outright in O1 (no
  frozen-identity source until O3).
- Exact-set at seal/claim/finalize: missing, foreign, extra, or
  prematurely-released ownership row → `OWNERSHIP_SET_MISMATCH` block,
  evidence preserved.
- Hard release: delete one ownership row before finalize → finalize commits
  `blocked` + zero watermark writes; an extra active row owned by the run →
  same; success asserts released count == sealed count, positive
  `ownership_epoch` increments across re-acquisition, and bindings intact.
- Controlled release-count mismatch: a real SQLite trigger using RAISE(IGNORE)
  on one release row can produce fewer changed rows; rollback to the still-open
  savepoint restores ALL watermark and ownership changes, then ONE commit blocks
  the run/job with OWNERSHIP_RELEASE_MISMATCH. Test multiple entities.
- SQL exception: a real abort trigger on an ownership-release write rolls back
  the WHOLE transaction; run remains completing with claim/payload and all
  ownership intact. Removing the trigger allows a convergent fenced retry.
- Claim savepoint: ownership conflict on entity 2 of {X,Y} → entity 1's prior
  owner untouched and entity 1 holds no new ownership/binding row for this
  run (rollback of partial acquisition proven, not assumed).

**Fairness (O1/O3):**
- Old `{X,Y}` waits on `X` held by an active run while a *stream* of newer
  `{Y,…}` jobs arrive — every one defers `queued_overlap`; when `X` frees, the
  eldest claims, never a newcomer.
- `LIMIT=1` enumeration with a busy head still surfaces the disjoint eligible
  job `W` (eligibility before LIMIT).
- Concurrent out-of-order `TryClaim` on two stores over one barrier → exactly
  one `Claimed`, deterministic winner; direct-by-id claim of a younger
  overlapping job defers even when the elder is not yet due.
- Malformed elder manifest: a pending elder with invalid
  `requested_entities_json` is quarantined `MANIFEST_INVALID` in the claim tx
  and the claimant defers `elder_manifest_invalid` — next pass proceeds only for a proven
  never-started elder; unproven effects preserve admission quarantine. Test both
  malformed JSON and valid JSON with invalid shape; no silent empty fallback.
- Scheduled `pending` run vs. later manual job on the same entity — the
  scheduled run reserves; earlier manual job vs. later scheduled run — the
  manual job reserves; unresolved `blocked` scheduled run mints no successor.

**Send ledger (O2):**
- Claim commits `admitted` attempt + fence in one tx; two claimants → one.
- Timeout/exception/non-2xx after admission → attempt `unknown`, batch
  quarantined, run `blocked`, ownership retained, **no reclaim ever**; crash
  after admission → recovery marks `orphaned` + quarantine + block.
- `precheck_failed` → bounded `retry_waiting` re-claim; exhaustion →
  `dead_letter` + run block; mismatched `maxAttempts` refused.
- ACK apply requires attempt `admitted` + run active + bound-epoch ownership —
  a live-looking `uploading` pointer alone is insufficient.
- ACK/fail race **both orders**: ACK committed then failure → batch stays
  `acknowledged`, run blocks, evidence kept; failure then late ACK →
  `LateEvidenceRecorded` on the attempt only (fields still validated —
  invalid late ACK bodies are recorded, never applied), batch/run unchanged;
  a late failure report for an already-`acknowledged` attempt is a no-op;
  foreign `attempt_id` → `ClaimLost` zero writes.
- Eager block stops sibling admissions: after one batch goes `unknown`,
  remaining `ready` batches of the run are never claimable.
- Wire identity: real `ErpClient` over a stub `HttpMessageHandler` — admitted
  send bytes identical across attempts (F1 payload-byte discipline).

**Migration:** populated v6→v7 fixture — all rows preserved, defaults applied,
checksums 1–7, idempotent rerun; 008/009 fixtures follow the same convention
when those stages are implemented.

---

## 12. Open technical decisions for root

1. `upload_max_attempts` bound value and `EtlOptions` home
   (`MaxBatchUploadAttempts`; sibling to the open `MaxRunCompletionAttempts`).
2. Backoff value for `precheck_failed` retries and `deferUntilUtc` for
   `busy_entity`/`queued_overlap` (caller-supplied policy parameters; no store
   default invented).
3. Job `claim_attempt_count` committed-claims-only diagnostic (chosen) vs.
   counting deferrals — §4.3.
4. Quarantine vocabulary: `dead_letter` + distinct `last_error` codes (chosen —
   reuses existing status/metrics) vs. a separate `quarantined` batch status.
5. `ResolveEtlRunAsync` mechanics when built — §8 boundary fixed (durable
   preconditions + exclusive-maintenance quiescence + attestation), signature
   deferred.
6. `etl_run_entities.status='pending'` (reserved, 006) as the A04 resume
   marker — A04 scope.
7. Normalized frozen-request table (`job_id, entity_name` + backfill) as a
   performance substitute for the `json_each` overlap predicate — permissible
   only if schema-exact; default is the predicate over
   `requested_entities_json` (already canonical and enforced at capture).

---

## 13. Recorded prerequisites this design does NOT waive

- Configured stable non-secret `source_namespace` — `Begin` already refuses
  without it (`SourceNamespaceMissing`); the configuration item does not exist.
- Explicit tested domain-transition policy (the conservative
  `EtlDomainFingerprint` blocks ordinary full→incremental mode changes as
  `DOMAIN_CHANGED`) — a separate cutover gate per root disposition.
- A04 spool intent/checkpoint (`creating` intents, orphan reconciliation).
- ERP `batchId` dedup and complete-endpoint idempotency remain unproven —
  the fail-closed unknown-outcome model exists precisely because of this;
  the F1 completion replay is an isolated gated mechanism, not evidence that
  upload replay is safe.
- Exclusive-host assumption (`SingleInstanceLock`) for all startup recovery and
  the §8 maintenance boundary.

---

*End of revision 3. Invariants: lifetime ownership is durable, epoch-bound via
immutable bindings, exact-set verified at every mutation, all-entity-atomic at
claim, held through extraction/upload/completion/failure/restart, and released
only inside the successful finalize commit (hard gate) or an attested,
drained-quiescence resolution commit; execution fences are fresh minted GUIDs
(extraction claim, send attempt, completion claim) — never caller owner ids; a
send admission never claims network success; an uncertain outcome never
replays; overlap priority is enforced transactionally, not by enumeration.
Migrations are fixed (007/008/009) and immutable once accepted; all stages are
strictly dark; O1 alone is not safe for production; F2 remains gated on §9 +
§13. Changed files in this worktree: this document only. No new user
permission is requested — these are technical acceptance gates for the
orchestrator within the authorized remediation work.*


## Root disposition and binding implementation clarifications — 2026-09-25

Accepted as a planning baseline only, after three Devin revisions and root corrections.
Main code is independently accepted at 509/509, commit 37c7012. No 007 code,
worker cutover, remote idempotency guarantee or completed A03/A04/A05 is claimed.
The following precise requirements govern implementation where prose above is abbreviated:

1. The SAME complete three-way set/positive-epoch predicate in §6.1/§6.2 applies
   before every extraction mutation, not a touched-row check plus equal counts.
   Add an equal-count substitution test: while touching A, replace required B
   with extra C; reject without writes. Do not weaken old F1 assertions while
   adapting setup to accepted jobs and newly required claims.
2. Public running-run FailEtlRunAsync/BlockEtlRunAsync also require the current
   extractionClaimId. Termination may preserve/block corrupted ownership without
   requiring the corrupt set to become valid first; it never releases ownership.
   Internal block helpers run only inside an already-authorized claim/finalize/
   recovery transaction. A stale callback must not fail a newer execution.
3. Elder-manifest validation is typed: a nonempty array of unique nonempty strings,
   consistent with the frozen job identity. json_valid alone is insufficient for
   {}, null, [], numeric entries or duplicate names. Validate BEFORE overlap work;
   CASE protects json_each from malformed JSON without treating an invalid elder
   as semantically empty. Enumeration must surface quarantine diagnostics even when
   no eligible candidate exists. Automatic retirement of a corrupt pending elder
   from reservation is permitted ONLY when durable state proves it never started:
   no extraction/completion claim or attempt, no started_at, no batches/entities/
   bindings/ownership or completion payload. Otherwise preserve unresolved evidence
   and stop admission with a visible quarantine condition until explicit resolution;
   do not silently remove its reservation because parsing failed.
4. A controlled zero/short row-count outcome and a thrown SQL error are distinct:
   the former rolls back the open savepoint then commits a block; the latter rolls
   back the whole transaction and preserves the current completing claim. All
   expected-one run/job transitions are checked before commit.
5. O2 ACK/send admission must use full run ownership/binding equality, not merely
   the affected entity. Attempt evidence is never remote proof. Preserve the first
   recorded ACK observation on exact replay; a conflicting later observation must
   be explicit, not silently overwrite prior evidence. The attempt ledger FK must
   be reconciled with terminal batch retention before O2/cutover: unresolved attempt
   evidence is undeletable, and successful cleanup must not fail on orphaned FKs.
6. O1 is exactly migration007 plus manual-job claim, ownership/bindings, extraction
   fence, fair admission, F1 guards/release and exclusive-startup recovery/tests.
   O2 and O3 remain future numbered migrations008/009. No migration is rewritten
   after acceptance. Acceptance of this document authorizes the bounded O1 work
   within the existing user task; it does not authorize an interim production switch.
