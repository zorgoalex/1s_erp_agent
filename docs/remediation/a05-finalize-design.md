# A05 follow-on — durable final cursors + atomic run finalize (revision 3)

Date: 2026-09-24, revision 3 after mandatory root review of revision 2.
Baseline: checkpoint-355 worktree (schema v5; A03 bounded storage foundation —
`etl_jobs` + `AcceptEtlJobAndCompleteCommandAsync` — landed dark).
Status: **design proposal only — not implemented, not tested, not externally agreed.
Root does not approve an interim production cutover at this time; §11 records the
gate.** No code, schema, tests, or packages were changed for this document. Local
implementation of [PROPOSED] items is authorized by `contract-decisions.md` §0; no item
here is claimed as an externally accepted contract. ERP complete-endpoint idempotency
is **not proven** (`contracts/erp-agent-api.openapi.yaml:58-63` labels the 204
"idempotent run completion ACK"; the ERP Agent API is absent from the supplied
archive — `erp-integration-inventory.md`).

Sources: `docs/remediation/etl-durable-design.md` (DD, root disposition),
`docs/remediation/contract-decisions.md` (CD), `docs/architecture-audit-2026-09-23.md`
(AA §A05), current code cited inline. Line numbers are 1-based in this worktree.

---

## 0. Scope and boundary with parallel A05a

**A05a (parallel worktree) is code-only on the existing v5 schema** — no migrations:
it repairs the positive readiness predicate for `uploading` runs and the retention
gate so service rows are not deleted before the parent run is locally finished.
This document does not redesign either. Boundary terms required by review:

- A05a's retention gate permits cleanup only for runs in terminal success —
  **`status='succeeded'`, never `partial_success`** (a partial run retains
  unconfirmed ranges; CD-P-1). ERP acknowledgement alone must never permit
  cleanup. `completion_acknowledged_at_utc` below is recorded only as *evidence*
  that a 204 was observed; it is diagnostic, not a cleanup key.
- A05a's batch-only readiness predicate governs the **current v5 path** (which
  keeps serving until cutover and for pre-cutover runs). The new path cannot reuse
  it: readiness there is inherent to a transactional **seal + claim** check on
  `etl_run_entities` (§4–§6). Same invariant, different schema — not duplication.

This slice: durable per-entity bases/finals, sealed extraction output, and a
completion protocol making *ERP complete accepted → ALL watermark updates + local
run success* one atomic outcome (CD-ORD-1 / TZ FR-ETL-005 step 4). **fail_run
semantics only** — no partial-success machinery is built where no real per-entity
failure policy exists.

**Explicitly not in this slice:** durable job dispatch of `etl_jobs` (A03
continuation), `etl_entity_leases` and upload fencing (A03/A04 — the ownership
mechanism F2 depends on), batch-ACK `status` validation (CD-B-1), reconcile modes,
disk-space admission (A08), live claim takeover, domain-reset tooling.

---

## 1. Verified current-state defects this slice must close

1. **Non-atomic finalize.** ERP POST → per-entity unconditional watermark upsert →
   unguarded run UPDATE are three operations (`EtlBatchUploadWorker.cs:28-30`;
   `SqliteAgentStore.Etl.cs:144-151`, `153-160`).
2. **Arbitrary cursor ordering.** Final watermarks are rebuilt `ORDER BY
   created_at_utc` (`SqliteAgentStore.Etl.cs:122`); ties on the stored `"O"`
   timestamp are possible and there is no monotonic term to break them.
3. **No durable expected base / no ABA protection.** The committed base survives
   only implicitly as batch 0's `watermark_from_json` (`OnecEtlWorker.cs:62-66`).
   Even persisted, **cursor-value equality cannot prove "unchanged"**: ABA
   (X→Y→X) and legitimate same-cursor commits both defeat a value CAS. A
   monotonic per-row generation is required, plus row presence and domain
   identity.
4. **Blind overwrite, no fencing.** `CommitWatermarkAsync` upserts unconditionally
   (`SqliteAgentStore.Etl.cs:148`) — and would not bump any generation it doesn't
   know about; runs serialize only through extraction, never through
   upload/finalize.
5. **Cursor-domain hazards.** `EtlCursorPolicy.Compare` orders
   `(UpdatedAtUtc, SourceId)` (`EtlCursorPolicy.cs:12-16`) — valid only inside one
   entity under one definition and **one source**. It is invalid across
   full-reload resets, `NULL` timestamps, composite keys
   (`EtlModels.cs:36-49`), definition changes — and a changed definition does NOT
   imply changed cursor bytes; a config or source switch can leave identical
   serialized cursors with different meaning.
6. **Zero-row window implicit** (`OnecEtlWorker.cs:79-80`, `to = upper`); must be
   durable, not re-derived.
7. **Retry sends a different payload / no completion state**
   (`EtlBatchUploadWorker.cs:28`); after a crash the durable state cannot
   distinguish never-sent from maybe-sent, and `JsonElement` reserialization is
   not byte-identity.
8. **No seal / unguarded mutations.** `RegisterBatchAsync` inserts unconditionally
   (`SqliteAgentStore.Etl.cs:19-31`); `CompleteEtlRunAsync` updates any run
   (lines 153-160); nothing stops mutation of a run being completed.
9. **fail_run default; no real partial policy** (`OnecEtlWorker.cs:84-87`). A
   failed run's pending batches keep uploading — `GetPendingBatchesAsync` has no
   run-status filter (`SqliteAgentStore.Etl.cs:49`) — and a local status flip
   cannot cancel an HTTP upload already in flight or recall remote effects.
10. **v5 backward edge.** Pre-existing runs carry no entity metadata, no
    generation/base evidence, no frozen definitions — nothing about them may be
    finalized as "proven".

---

## 2. Data model — one additive migration

Suggested `006_etl_finalize.sql`; additive only (001–005 checksums untouched).
A05a ships no DDL; this is simply the next migration.

### `watermarks` additive columns

```text
generation INTEGER NOT NULL DEFAULT 1      -- bumped on EVERY finalize commit; ABA token.
                                           -- NOTE: legacy CommitWatermarkAsync writes without
                                           -- bumping it — safe only because new APIs are dark
                                           -- until every legacy writer is retired (§11).
domain_fingerprint TEXT NULL               -- fp of (source namespace + frozen definition + mode)
                                           -- that wrote the cursor; NULL on pre-existing rows
```

### `etl_run_entities`

```text
run_id TEXT NOT NULL
entity_name TEXT NOT NULL
PRIMARY KEY(run_id, entity_name)
entity_definition_json TEXT NOT NULL        -- frozen effective definition used for this read
domain_fingerprint TEXT NOT NULL            -- fp computed for this read (§3.2)
status TEXT NOT NULL                        -- extracting|done|failed ('pending' reserved A04)
base_row_present INTEGER NOT NULL           -- 1: watermarks row existed at capture; 0: absent
expected_base_generation INTEGER NULL       -- watermarks.generation observed at capture
expected_base_cursor_json TEXT NULL         -- EXACT committed_cursor_json text observed
expected_base_domain_fingerprint TEXT NULL  -- fp observed on the row (NULL = unknown domain)
domain_status TEXT NOT NULL                 -- absent|same|changed|unknown (§3.3)
watermark_from_json TEXT NULL               -- resolved query start actually used (informational)
snapshot_upper_bound_json TEXT NULL         -- fixed window end (bounded read; NOT a snapshot)
final_watermark_json TEXT NULL              -- terminal cursor; survives batch deletion
expected_batch_count INTEGER NULL           -- >= 1; batches this entity must have acknowledged
rows_read INTEGER NOT NULL DEFAULT 0
batches_created INTEGER NOT NULL DEFAULT 0
last_error TEXT NULL
created_at_utc / updated_at_utc / row_version — project conventions
FK run_id → etl_runs(run_id)
```

`expected_base_*` stores **raw column values** read inside the capture tx — never
a re-serialization. `base_row_present` distinguishes *absent row* from *NULL-valued
row*: different CAS shapes and different domain dispositions (§3.3, §5).

### `etl_runs` additive columns

```text
sealed_at_utc TEXT NULL                     -- extraction seal (§4); NULL = mutable/unsealed
sealed_entity_count INTEGER NULL
sealed_expected_batch_count INTEGER NULL
completion_claim_id TEXT NULL               -- NEW unpredictable GUID per successful claim —
                                            -- the exact claim identity (not a timestamp)
completion_claim_owner_id TEXT NULL         -- diagnostics
completion_claim_acquired_at_utc TEXT NULL  -- diagnostics only, NOT the fence
completion_attempt_count INTEGER NOT NULL DEFAULT 0
next_completion_attempt_at_utc TEXT NULL
complete_payload_json TEXT NULL             -- verbatim wire body, written once at first claim
completion_acknowledged_at_utc TEXT NULL    -- evidence a 204 was observed (NOT a cleanup key)
finalize_conflict_code TEXT NULL
finalize_conflict_message TEXT NULL
resolved_at_utc TEXT NULL                   -- manual-resolution marker (§9.2); NULL = unresolved
```

New status values: `completing`, `blocked`. `partial_success` is **not** produced
(§7). `EtlRunStatus` gains `Blocked` (additive enum member; `status` is
unconstrained TEXT). `etl_jobs` unchanged; job transitions per §7 (`finished`
only on successful finalize; `blocked` on run failure/block).

---

## 3. Cursor semantics — generation CAS, source+domain identity, never MAX

### 3.1 Final cursor is durable, not order-derived

`final_watermark_json` is written explicitly at entity completion; no `ORDER BY`,
`batch_no`, or `MAX` participates in finalization.

### 3.2 Domain fingerprint — conservative identity

`domain_fingerprint` = project-hash (`PayloadHasher` convention) over a canonical
tuple:

```text
{ source_namespace,                       -- stable NON-SECRET source identity (below)
  entity_name,
  entire frozen entity_definition_json,   -- conservative: whole definition, not a field subset
  effective query shape }                 -- mode (bootstrap_full|entity_reload|incremental) +
                                          -- whether the UpdatedAtField predicate applies
```

- `source_namespace` identifies the 1C source/base namespace the cursor was read
  from (infobase / OData service-root identity). It must be a **stable,
  explicitly configured, non-secret** value — a new configuration item is
  required; `OnecOptions` today exposes only `ODataBaseUrl`/`CredentialSecretName`
  (`AgentOptions.cs` OnecOptions) and credentials are **never** hashed. A
  normalized service-root (scheme+authority+path, no credentials/query) may seed
  it, accepting safe-side blocking when the endpoint legitimately moves —
  **harmless extra blocking is preferred over missing a source switch**.
- **If a stable source namespace cannot be established, F2 is blocked** — this is
  a recorded hard dependency (§11, §14), not a detail to improvise. No
  fingerprint ⇒ no domain comparison ⇒ no safe commit.
- Including the whole frozen definition plus mode errs toward extra blocking: any
  definition change that could plausibly alter cursor semantics produces a
  different fp, and the CAS/conflict path handles it explicitly.

> **D1 update (2026-09-26):** the implemented fingerprint no longer includes the read mode.
> It hashes { source namespace, entity, entire frozen definition, cursor class }, where all
> watermark modes share `watermark-cursor/v1`. Whether the `UpdatedAtField` predicate
> applies is still covered, because the whole definition is hashed. An incremental read
> with no watermark row is refused (`BaselineRequired`), and the attested
> `ResetEtlWatermarkDomainAsync` is the reset procedure referenced below. See
> [domain-transition-d1.md](domain-transition-d1.md).

### 3.3 `domain_status` at capture — fail closed, no adoption

| stored row | stored fp | current fp | status | policy |
|---|---|---|---|---|
| absent | — | — | `absent` | INSERT path at finalize; a **new** row may establish the domain |
| present | non-NULL, equal | | `same` | normal CAS |
| present | non-NULL, different | | `changed` | **block** `DOMAIN_CHANGED` |
| present | NULL | any | `unknown` | **block** `DOMAIN_UNKNOWN` |

`unknown` is the correction from rev2: a pre-existing watermark row carries no
domain evidence; generation equality proves only that the **bytes** are unchanged,
not that the stored cursor's *semantics* are compatible with the current
definition/source. There is **no adoption path** — a present row with unknown
domain blocks and is preserved until an explicit, separately-verified
migration/reset procedure (out of scope, §14) establishes its domain. Only an
absent row may establish a new domain via INSERT.

**Operational consequence (stated, not hidden):** every `watermarks` row that
predates the fingerprint column is `unknown`. On an upgraded production database,
the first post-cutover run touching such an entity blocks until the reset
procedure exists — this is the intended fail-closed behavior and a hard F2
prerequisite, not a bug in the design.

### 3.4 CAS shapes — presence + generation + cursor + domain

Expected **present** (`base_row_present=1`) — pure UPDATE, can never INSERT:

```sql
UPDATE watermarks
SET committed_cursor_json=$final, extracting_cursor_json=NULL, last_run_id=$run,
    generation=generation+1, domain_fingerprint=$fpCurrent, updated_at_utc=$now
WHERE entity_name=$e
  AND generation=$expectedGen
  AND committed_cursor_json IS $expectedBase
  AND domain_fingerprint IS $expectedFp;
```

Expected **absent** (`base_row_present=0`) — pure guarded INSERT, can never
UPDATE; only fires with `domain_status='absent'` (base NULL):

```sql
INSERT INTO watermarks(entity_name,committed_cursor_json,extracting_cursor_json,
                       last_run_id,generation,domain_fingerprint,updated_at_utc)
SELECT $e,$final,NULL,$run,1,$fpCurrent,$now
WHERE NOT EXISTS (SELECT 1 FROM watermarks WHERE entity_name=$e);
```

- `changed == 1` per entity required; any 0 → whole finalize rolls back (§6.3).
- Classification on conflict (row re-read in the same tx):
  `ROW_UNEXPECTEDLY_PRESENT` / `ROW_VANISHED` (defensive — rows are never
  deleted) / `GENERATION_MISMATCH` (covers ABA and same-cursor commits — the
  generation moved even when the cursor text didn't) / `BASE_CURSOR_MISMATCH` /
  `DOMAIN_MISMATCH`. A false conflict always blocks — safe direction.
- `EtlCursorPolicy.Compare` is never used in finalize.

---

## 4. Extraction capture and seal

### 4.1 `BeginEtlEntityExtractionAsync(runId, entity, upperBound)` — one tx

- Guard: run `status='running'` AND `sealed_at_utc IS NULL` AND no existing entity
  row.
- Read `watermarks` row inside the tx: presence, `generation`,
  `committed_cursor_json` (raw), `domain_fingerprint`. Compute current fp
  (§3.2 — requires a configured `source_namespace`; absent ⇒ return
  `SourceNamespaceMissing`, worker blocks the run `SOURCE_NAMESPACE_UNCONFIGURED`).
- Classify `domain_status`: `changed`/`unknown` → return conflict (entity row
  `failed`, `last_error=<code>`) → worker blocks the run — **no extraction is
  started against an ambiguous-domain base**.
- `absent`/`same` → INSERT `extracting` row with all `expected_base_*`; return the
  committed cursor the caller must use for the query — recorded base and query
  base identical by construction.

### 4.2 `CompleteEtlEntityExtractionAsync(runId, entity, final, expectedBatchCount)`

Guarded UPDATE `WHERE status='extracting'` and run unsealed. Validation at write
(§4.5): `final` must be a well-formed cursor with at least one non-NULL
component — `'{}'`/`(null,null)`/malformed JSON are rejected — and
`expectedBatchCount >= 1` (the empty flush always exists).

### 4.3 Seal — `SealEtlRunExtractionAsync(runId)` replaces `MarkEtlRunExtractedAsync`

One transaction; refuses unless all hold on live rows:

- `requested_entities_json` parses to a **non-empty array of unique non-empty
  TEXT values** — `[]`, `{}`, `[null]`, `["a","a"]`, `["a",""]` are malformed and
  refuse the seal (defect-class: structural garbage must not make a run
  vacuously ready);
- entity-row set **exactly equals** the validated requested set (both directions);
- every entity `status='done'`, `final_watermark_json` valid non-empty cursor,
  `expected_batch_count >= 1`, `expected_batch_count == COUNT(etl_batches)`;
- run `status='running'`, unsealed.

On success: `status='uploading'` + seal fields. The sealed entity rows are the
frozen manifest; afterwards every mutation API is fenced:

- `RegisterBatchAsync` gains guards: run `status='running'` AND unsealed AND
  entity `status='extracting'` — a late batch after `done`/seal/`completing` is
  rejected and moves no counters (today it inserts unconditionally, defect 8).
- `Begin`/`CompleteEntity`/`Seal`/`Fail`/`Block` are guarded `WHERE
  status='running'` — none can mutate sealed/completing/terminal runs.
- `CompleteEtlRunAsync` (unguarded v5 writer) is retired at cutover (§11).

### 4.4 Worker extraction path

`GetCommittedWatermarkAsync` → `BeginEtlEntityExtraction` (conflict outcomes →
block run); returned base feeds `from`/`committed`; after the loop
`CompleteEtlEntityExtraction`; after all entities `SealEtlRunExtraction`.
Failure path → `FailEtlRunAsync` (§7). The read/flush loop is otherwise
unchanged — but capture+seal+fencing is a real semantic change, exercised by the
§12 fault tests, not a cosmetic diff.

### 4.5 Typed validation of durable cursor/entity fields

Written and re-verified values must be structurally valid, not merely non-NULL:

- cursor JSON: object with `UpdatedAtUtc` absent-or-nullable-ISO-date and
  `SourceId` absent-or-nullable-string; anything else (scalar, array, `[]`,
  `'{}'` where a component is required, non-parseable date) rejects;
- `final_watermark_json` additionally requires ≥1 non-NULL component;
- `requested_entities_json` per §4.3.

Validation lives in the store layer at write time AND is re-executed by
seal/claim/finalize on the persisted text — a malformed stored value can never
satisfy readiness.

---

## 5. Completion claim — atomic readiness + unique claim identity

### 5.1 `GetDueRunCompletionsAsync(limit, now)`

`status='uploading'` claimable, OR `status='completing'` with `completion_claim_id
IS NULL` and (`next_completion_attempt_at_utc` NULL or `<= now`).

### 5.2 `TryClaimRunCompletionAsync(runId, ownerId, nextAttemptAtUtc)` — one tx

Returns `null` unless all verified in-tx:

- `uploading`: `sealed_at_utc IS NOT NULL`; `requested_entities_json` valid per
  §4.3; entity set exactly equal; every entity `done` with valid non-empty final
  and `expected_batch_count >= 1`; per entity `expected_batch_count` == count of
  its `acknowledged` batches; no batch of the run in any other status
  (`creating/ready/uploading/retry_waiting/dead_letter` all block); seal counts
  consistent.
- `completing` reclaim: `completion_claim_id IS NULL` + due; payload and sealed
  entity rows re-validated cheaply (defense-in-depth).
- `uploading` run with zero entity rows → **not claimed**; the tx marks it
  `blocked LEGACY_UNRESOLVABLE` (§10).
- On claim: `status='completing'`, `completion_claim_id=<new GUID>` (unique,
  unpredictable — the fence; owner+acquired recorded for diagnostics only),
  `attempt_count+1`, `next_completion_attempt_at_utc`,
  `complete_payload_json = COALESCE(existing, <built-once>)` — built in-tx from
  run counters, same shape as today (`{runId,status:'succeeded',rowsRead,
  batchesCreated,batchesAcknowledged,completedAtUtc}`; CD-P-2 `entities[]` not
  adopted — unproven).
- Returns `{RunId, ClaimId, OwnerId, CompletePayloadJson, Attempt}`.

### 5.3 Uncertain-outcome honesty

After a crash, `completing` + payload + no `completion_acknowledged_at_utc`
**cannot distinguish** "POST never sent" from "POST sent and possibly applied" —
explicitly an *uncertain* outcome. Recovery replays the **identical stored body**
(§6.1) — the only honest retry for an endpoint of unproven idempotency — and
durable success is never recorded outside the finalize transaction.

---

## 6. ERP call and finalize

### 6.1 Raw-body send — byte identity at the wire

`IErpClient` gains `CompleteEtlRunAsync(Guid runId, string payloadJson, ct)`
posting `new StringContent(payloadJson, UTF8, "application/json")` — posted bytes
are stored bytes by construction. Tests capture real request bytes (real
`ErpClient` over a stub `HttpMessageHandler`), not fake-interface equivalence.

### 6.2 Worker completion path

```csharp
foreach (var candidate in await store.GetDueRunCompletionsAsync(limit, now, ct))
{
    var claim = await store.TryClaimRunCompletionAsync(candidate.RunId, ownerId, retryAt, ct);
    if (claim is null) continue;
    try
    {
        await erp.CompleteEtlRunAsync(claim.RunId, claim.CompletePayloadJson, ct);
        var outcome = await store.FinalizeEtlRunAsync(claim.RunId, claim.ClaimId, ct);
        // Finalized → LastEtlSuccessAtUtc + ETL_RUN_SUCCEEDED;
        // Blocked → ETL_RUN_FINALIZE_CONFLICT; ClaimLost → silent.
    }
    catch (OperationCanceledException) when (ct.IsCancellationRequested) { return; }
    catch (Exception ex)
    {
        await store.MarkRunCompletionRetryAsync(
            claim.RunId, claim.ClaimId, ex.Message, retryAt, CancellationToken.None);
    }
}
```

### 6.3 `FinalizeEtlRunAsync(runId, claimId)` — ONE transaction

1. Fenced guard: `status='completing' AND completion_claim_id=$cid` — exact claim
   identity; stale/superseded → `ClaimLost`, zero writes.
2. **Full guard recheck**: sealed; requested set valid; entity set exact; all
   `done` + valid finals + counts; per-entity `expected_batch_count` ==
   acknowledged count; no non-acknowledged batch. Violation → conflict path
   `SEAL_VIOLATED`.
3. `SAVEPOINT cas;` — per `done` entity, its §3.4 CAS shape; collect `changed`.
4a. **All 1** → `RELEASE cas`; run → `succeeded` (`finished_at_utc`,
    `completion_acknowledged_at_utc`; `completion_claim_id` retained as audit of
    the winning claim; owner/next-attempt cleared; `row_version+1`, same fenced
    guard); job → `finished`; `COMMIT`. Outcome `Finalized`.
4b. **Any 0** → `ROLLBACK TO cas; RELEASE cas;` — all watermark writes undone;
    classify; run → `blocked` + `finalize_conflict_*`, `completion_claim_id=NULL`
    (terminal — never re-claimed); job → `blocked`; `COMMIT`. Outcome `Blocked`.

One commit, two legal outcomes; no two-transaction gap. Crash mid-tx commits
neither → `completing` persists for fenced retry. Thrown fault → worker's fenced
retry; CAS-0 → controlled `blocked`, not silent retry. Idempotent: success moves
the run out of `completing` permanently.

`MarkRunCompletionRetryAsync` — fenced `(completing, claim_id)`: clears
`completion_claim_id` (so the next claim mints a fresh identity), sets
`next_completion_attempt_at_utc`/`last_error`; `attempt_count` ≥
`MaxRunCompletionAttempts` (new bounded `EtlOptions` value, ~8 — open question) →
`blocked COMPLETION_ATTEMPTS_EXHAUSTED`.

### 6.4 Crash/fault matrix

| State at crash/fault | Durable evidence | Recovery |
|---|---|---|
| After `BeginEntity`, before first batch | `running`, `extracting` | startup → `blocked INTERRUPTED_NO_CHECKPOINT`; pending batches fenced (§7/§8 caveat) |
| After last batch, before `CompleteEntity` | `running`, `extracting` | same — finals never re-derived |
| After seal | `uploading` + manifest | normal upload path |
| After claim, before POST | `completing` + dead claim + payload | recovery clears `completion_claim_id` → reclaim mints new identity, replays **identical** body (uncertain, §5.3) |
| After POST, before finalize | same | identical replay → finalize |
| Mid-finalize | tx uncommitted | `completing` persists; retry re-runs the same atomic outcome |
| After finalize | `succeeded` + ack evidence | terminal; **only now** may A05a retention act (key = `succeeded`) |
| ERP non-2xx/timeout | `completing` + claim | fenced retry, identical body |

---

## 7. Failure semantics — fail_run only

`fail_run` is the only implemented policy (as-built; FR-ETL-014 is future). No
`partial_success` is ever written; complete-payload `status` is always
`'succeeded'`.

`FailEtlRunAsync(runId, error)` — one tx, guarded `WHERE status='running'`:

- run → `failed`; still-`extracting` entities → `failed`;
- pending batches of the run (`ready/retry_waiting/uploading`) → `dead_letter`,
  `last_error='RUN_FAILED'` — **with the explicit limit that a status flip cannot
  cancel an HTTP upload already in flight and cannot recall remote effects**: a
  batch mid-POST still lands at ERP (its local `AcknowledgeBatchAsync` then
  fails closed on the `status='uploading'` guard and stays `dead_letter` —
  remote effect applied, local evidence preserved). Fencing bounds *future local
  dispatch*; it is not remote quiescence (§9).
- job → **`blocked`, never `finished`** — the unresolved retention guard
  (`SqliteAgentStore.cs:325`) and the unresolved-work marker hold until explicit
  manual resolution (§9.2). Job `finished` is written only by successful
  finalize.

`BlockEtlRunAsync(runId, code, message)` — same shape to `blocked`
(`DOMAIN_CHANGED`, `DOMAIN_UNKNOWN`, `SOURCE_NAMESPACE_UNCONFIGURED`,
`LEGACY_UNRESOLVABLE`, `INTERRUPTED_NO_CHECKPOINT`, seal/claim conflicts).

---

## 8. Recovery — startup-only, never live takeover

`RecoverAsync` additions land **only at cutover** (F1 touches nothing, §11):

```sql
UPDATE etl_runs SET completion_claim_id=NULL, completion_claim_owner_id=NULL,
  completion_claim_acquired_at_utc=NULL
WHERE status='completing' AND completion_claim_id IS NOT NULL;
UPDATE etl_runs SET status='blocked', finalize_conflict_code='INTERRUPTED_NO_CHECKPOINT',
  finalize_conflict_message='Extraction interrupted before seal; manual resolution or A04 resume required.',
  updated_at_utc=$now, row_version=row_version+1 WHERE status='running';
UPDATE etl_batches SET status='dead_letter', last_error='RUN_INTERRUPTED'
WHERE status IN ('ready','retry_waiting','uploading')
  AND run_id IN (SELECT run_id FROM etl_runs WHERE status='blocked'
                 AND finalize_conflict_code='INTERRUPTED_NO_CHECKPOINT');
```

- Assumes **exclusive host** — the same single-agent-per-DB precondition the
  existing `uploading→ready` reset already relies on (`SqliteAgentStore.cs:31`).
  Claim-ID clearing is dead-process recovery before readiness, **not** live
  takeover; no time-based stealing of completion claims is added, and nothing
  here is safe while another live agent holds a claim.
- Fencing batches of interrupted runs prevents *further local dispatch*; a batch
  that was mid-POST at crash may already have remote effect — status cannot undo
  it (§7 caveat applies identically).
- `pending` job-runs untouched (A03 dispatch owns them).

---

## 9. Ownership, unresolved work, and the honest cutover boundary

### 9.1 What this slice proves vs. cannot prove

**Provable in storage alone (real-SQLite tests, isolated from legacy writers):**
generation-CAS correctness (ABA, same-cursor commits, absent-vs-present row);
all-or-nothing finalize incl. conflict-atomicity in one commit; seal exactness
and post-seal immutability; claim-ID fencing; payload byte identity; recovery
preserving evidence; legacy block preserving rows; fail_run fencing.

**Not provable / not claimed:**

- **CAS protects local watermarks only.** It cannot undo remote effects of stale
  or in-flight batches; ERP-side dedup is unproven.
- **The original batch upload loop is NOT asserted safe** (status-only claim,
  `SqliteAgentStore.Etl.cs:42-60`; unvalidated ACK `status`,
  `EtlBatchUploadWorker.cs:47-49`; crash between ERP ACK and
  `AcknowledgeBatchAsync` relies on unproven batchId dedup).
- **Entity ownership through upload** is A03/A04 scope — not built here.
- **SQL status does not prove remote quiescence** — see §9.2.

### 9.2 Unresolved-work gate — corrected semantics (still not an approved cutover)

Candidate mechanism (recorded for A03/A04 design, **not** an authorization): at
most one *unresolved* run per agent, where **unresolved** means
`status IN ('running','uploading','completing')` OR
`(status IN ('failed','blocked') AND resolved_at_utc IS NULL)`. A failed or
blocked run therefore **holds the gate** until an operator verifies quiescence
and sets `resolved_at_utc` via a narrow manual-resolution API
(`MarkEtlRunResolvedAsync(runId, note)` — guarded `status IN ('failed','blocked')`
+ `resolved_at_utc IS NULL` + local precondition `NOT EXISTS` non-terminal
batches of the run; remote quiescence cannot be proven by SQL — the operator
attestation is the boundary). Pending job-runs and job ordering remain
**undelivered** (A03 dispatch scope).

Because fencing cannot recall in-flight remote effects and the gate cannot prove
remote quiescence, **F2 remains blocked on a safe ownership + upload-fencing
mechanism** (A03/A04 leases/fencing or a root-approved equivalent). The run-gate
is a component candidate, not the missing ownership proof — **no interim
production cutover is approved or claimed by this document.**

---

## 10. Legacy runs — block/quarantine, never fabricate

- `uploading` legacy run reaching the claim path → `blocked
  LEGACY_UNRESOLVABLE` + diagnostics (derived chain values may appear in
  `finalize_conflict_message` as *evidence only* — never committed). It then
  holds the §9.2 gate until manual resolution — intended fail-closed behavior.
- `running` legacy run → `blocked INTERRUPTED_NO_CHECKPOINT` at recovery;
  pending batches fenced (§8 caveat).
- Batch graphs cannot prove original watermark generation or frozen definitions
  → no materialized bases/finals, ever. Manual resolution only, per documented
  runbook (inspect evidence → decide re-extract / explicit reset → resolve).
- `pending` runs untouched.

---

## 11. Stages — bounded, exact limits

| Stage | Content | Behaviour change |
|---|---|---|
| **F1 — dark storage** | Migration + all new store APIs + their real-SQLite tests. **No worker callsites, no `RecoverAsync` changes, no `IErpClient` changes, no retirement of v5 APIs.** | **Zero.** |
| **F2 — cutover** | Worker diffs (§4.4, §6.2); seal/fail/block wiring; `RegisterBatch` guards; `RecoverAsync` additions; `IErpClient` raw-body overload; **atomic retirement/fencing of EVERY legacy bypass** (`GetRunsReadyToCompleteAsync`, `CommitWatermarkAsync`, `MarkEtlRunExtractedAsync`, `CompleteEtlRunAsync`, unguarded `RegisterBatch`) in the same change; ownership mechanism per §9.2 gate. | Switches production to sealed+atomic finalize — **not currently approved** (§9.2). |
| **F3 — drain/cleanup** | All pre-F2 runs terminal; remove retired APIs. | Housekeeping. |

**F1 mixed-writer limitation (prominent):** the §3/§4/§6 guarantees do **not**
exist while legacy writers coexist — `CommitWatermarkAsync` upserts without
bumping `generation`, and `RegisterBatchAsync` is unguarded. Therefore: **no
production use of the new APIs until F2 fences every bypass in one change.** F1
may optionally add *additive* compatibility guards (e.g., a generation-bump
trigger on `watermarks`) only if separately reviewed for v5-writer
compatibility; otherwise F1 tests keep the new APIs isolated and this limitation
is the stated contract.

**F2 prerequisites:** (a) A05a readiness+retention landed (`succeeded`-keyed);
(b) an approved ownership + upload-fencing mechanism — **absent today**;
(c) a configured stable `source_namespace` (§3.2) and a verified
domain-establishment/reset procedure for `unknown` rows (§3.3) — **absent
today**; (d) §12 fault tests green; (e) manual-resolution runbook for
`blocked`/`failed` runs and jobs.

---

## 12. Fault tests required (real SQLite, isolated new APIs until F2)

1. `created_at_utc` ties → durable `final_watermark` wins; ordering never
   consulted.
2. **ABA**: R1 captures (X, gen g); R2 commits X→Y; R3 commits Y→X; R1 finalize →
   `GENERATION_MISMATCH`, zero writes, `blocked`.
3. **Same-cursor commit**: R2 commits cursor X under gen g+1 → R1 CAS fails on
   generation despite identical text.
4. **Absent vs NULL vs vanished**: absent expected → INSERT commits; NULL-valued
   row with `same` fp → UPDATE commits; row created after absent-capture →
   `ROW_UNEXPECTEDLY_PRESENT`, no insert; present-row deleted (simulated) →
   `ROW_VANISHED`, no insert — an INSERT can never fire with a non-NULL base.
5. **Domain identity**: identical cursor text under different fp → `changed` →
   `blocked DOMAIN_CHANGED`; **NULL-fp row → `unknown` → `blocked
   DOMAIN_UNKNOWN`, row preserved, no adoption**; absent row → commits and
   establishes fp; missing `source_namespace` → `SOURCE_NAMESPACE_UNCONFIGURED`.
6. **Late RegisterBatch**: after `done`, after seal, into `completing` →
   rejected, no counters.
7. **Mutation after claim**: `Begin`/`CompleteEntity`/`RegisterBatch`/`Seal`/
   `Fail`/`Block` against `completing`/`succeeded` → all rejected.
8. **Seal exactness**: missing/extra entity row, non-`done`, count mismatch →
   refuse; malformed `requested_entities_json` (`[]`, `{}`, `[null]`, `["a","a"]`,
   `["a",""]`) → refuse; malformed final (`'{}'`, `'[1]'`, `'"x"'`, bad date,
   `(null,null)`) → refuse at write AND at seal/claim/finalize recheck.
9. **Claim readiness**: pending batch → not claimable; expected-vs-acked
   mismatch → not claimable; `claim_id` uniqueness across reclaim generations.
10. **Ambiguous legacy** → `blocked LEGACY_UNRESOLVABLE`, all rows preserved,
    zero watermark writes, gate held until `resolved_at_utc`.
11. **Crash mid-finalize** (fault injection at k-th CAS): neither outcome
    committed; retry converges to exactly one.
12. **Conflict atomicity**: CAS-0 on entity 2/3 → one commit writes
    `blocked`+conflict and zero watermark changes.
13. **Claim fencing**: two claimants → one claim; stale `claim_id` finalize/
    retry → `ClaimLost`; reused/superseded owner with old `claim_id` → no writes.
14. **Payload wire bytes**: real `ErpClient` + capturing handler → request bytes
    == stored `complete_payload_json` across retries and simulated restarts.
15. **Fail_run fencing**: run `failed`, pending batches `dead_letter`, job
    `blocked` (retention guard holds), zero watermark writes; **gate still held**
    until `MarkEtlRunResolvedAsync`; test also documents that a batch mid-flight
    at failure keeps remote effect (asserted locally only as `dead_letter`).
16. **Gate**: second run refused while any `running/uploading/completing` or
    unresolved `failed/blocked` run exists; released only after finalize or
    `resolved_at_utc`.
17. **Recovery**: dead `completing` claim → `claim_id` cleared, payload/status
    intact; `running` at startup → `blocked` + batches fenced; startup-only.
18. **Retention seam** (with A05a): acknowledged batches of a non-`succeeded`
    run undeletable; post-`succeeded` deletion cannot alter committed cursors.
19. **Rollback drill**: v6 DB with non-terminal v6 runs + v5 binary → unsupported
    (§13); drill verifies detection, not downgrade function.

---

## 13. Backward compatibility / rollback

- Additive schema only; existing rows unchanged (`generation=1`,
  `domain_fingerprint=NULL` ⇒ `unknown` domain ⇒ fail-closed per §3.3).
- **Re-upgrade is the supported path.** Restoring a pre-upgrade backup silently
  discards post-backup commands, outbox results, spool batches, and remote
  effects already delivered — a no-loss rollback is **not** available that way.
  Backup restore is disaster recovery only, and is a prerequisite for it that
  **all** affected state (commands/outbox/spool/ERP-side effects) be reconciled
  by a documented procedure first; it is never a routine supported path.
- **Forbidden:** v5 binary on a v6 database with non-terminal v6 runs (blind
  upserts bypass every new invariant), including any `completing→uploading`
  flip to re-enable the old route.
- `blocked`/`failed` runs and their jobs resolve only via the documented manual
  procedure (§9.2); no endpoint invented.

---

## 14. Open questions / recorded dependencies

1. **Source namespace** (blocking): configuration item + provenance for a stable
   non-secret 1C source identity; without it no `domain_fingerprint` can be
   computed and F2 cannot start.
2. **Verified domain-establishment/reset procedure** (blocking): operator flow
   that legitimately re-baselines `unknown`/`changed` watermark rows (explicit
   evidence + new generation+fp), plus the equivalent for `DOMAIN_CHANGED`.
3. **Ownership + upload fencing** (blocking): A03/A04 leases and upload fencing;
   §9.2 gate is a candidate component only.
4. `MaxRunCompletionAttempts` bound value.
5. `etl_run_entities` retention horizon for terminal runs.
6. Pending-job vs scheduled-run ordering under the unresolved-work gate once A03
   dispatch exists.
7. Whether additive generation-bump triggers are wanted for F1 mixed-writer
   hardening — requires independent compatibility review; default is isolated
   APIs + the §11 limitation statement.

---

*End of revision 3. Nothing above is implemented, built, or tested. Invariants:
sealed output is immutable; every watermark write sits behind a
presence+generation+cursor+domain CAS inside ONE transaction committing either
the full success set or the conflict set; unknown domains and unprovable states
fail closed to `blocked` + preserved evidence + manual resolution — never to
claimed progress, never to invented adoption. F1 is strictly dark; F2 remains
blocked on the §11 prerequisites. Root review required before implementation.*


## Root review disposition (2026-09-24)

Accepted as a planning baseline after two mandatory revision rounds; not accepted as an implemented guarantee or production cutover. A05a is now accepted at checkpoint-380 (380/380), commit `6b3c784275f9d87a8bff23577a9d2b0c7fdecc5d`. The next implementation slice is bounded F1 storage with populated-v5 upgrade tests and explicit mixed-writer limitations. No worker/recovery wiring is authorized by this design alone: implementation must first satisfy the ownership, upload-fencing, source-identity and legacy-domain gates listed above.

Conservative definition/mode fingerprinting may block ordinary full-to-incremental transitions; before cutover, resolve that through an explicit tested domain-transition policy rather than silently reusing cursors. This document does not request new user permission; these are technical acceptance gates for the orchestrator within the already authorized remediation work.
