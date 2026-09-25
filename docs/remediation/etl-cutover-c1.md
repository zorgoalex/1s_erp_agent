# C1 — ETL cutover to the durable path

Date: 2026-09-26. No migration. Preconditions merged: O1, O2, O3, F1, R1, S1, H1, A08, D1.

This slice replaces the legacy ETL pipeline (`OnecEtlWorker`, `EtlBatchUploadWorker`,
`EtlTrigger`). The durable path is wired, and every §9 bypass is fenced in the same
change. No interim mode runs both paths.

## Workers (`src/ErpOnecAgent.Service/Workers/Etl/`)

### `EtlExtractionWorker`

A pass is skipped unless all of these hold:

- ETL is enabled and `CanExtract` holds;
- disk admission (A08) passes; a probe error fails closed;
- the spool is below `MaxSpoolBytes`;
- the source identity guard (S1) matches.

Then the pass does the following.

1. **Schedule (O3).** On the schedule interval it ensures the `incremental` run. The
   manifest contains only entities with a committed watermark. The others are reported as
   `ETL_BASELINE_REQUIRED` until an explicit `start_full_sync` or `reload_entity`
   establishes them (D1 policy: an incremental read never establishes a domain).
2. **Claim.** It claims the next manual job (O1), otherwise the next due scheduled run,
   under the shared overlap priority.
3. **Extract each frozen entity.**
   - It re-checks `CanExtract` at the entity boundary (B3).
   - `Begin` runs with the binding's `source_namespace`. A refusal blocks the run with
     `BEGIN_<REASON>`.
   - OData is read from the committed base to the safety-lag upper bound.
   - Spool batches are written under the disk reserve.
   - Each batch goes through `RegisterGuardedEtlBatchAsync`, then entity completion runs.
     Every entity ends with at least one batch; an empty window gets an explicit
     zero-row batch.
4. **After-identity check.** Up to 3 retries on `Unavailable`. If the source is no longer
   the same, the run is blocked `SOURCE_IDENTITY_CHANGED`; otherwise it is sealed.

Failure mapping:

| Condition | Result |
|---|---|
| Disk reserve | Block `DISK_RESERVE` |
| Spool quota | Block `SPOOL_LIMIT` |
| Store refused registration, entity completion or seal | Block `BATCH_REGISTRATION_REFUSED`, `ENTITY_COMPLETION_REFUSED` or `SEAL_REFUSED` |
| Any other exception (including OData) | `failed` |
| Shutdown | The claim is kept; startup recovery blocks the run `INTERRUPTED_NO_CHECKPOINT` |

A block is fenced by the extraction claim, so it cannot touch a run whose claim was lost.

### `EtlUploadWorker`

1. Claim a due batch through the O2 send ledger.
2. **Precheck.** Nothing has been sent at this point. The worker streams the spool file's
   SHA-256 and compares it with the registered value. A failure is recorded as
   `precheck_failed` with backoff from 5 s to 5 min. A missing file is recorded as
   `SPOOL_FILE_MISSING: …`. Shutdown during the precheck records the attempt as
   precheck-failed (due now) and rethrows, so it never stays `admitted`.
3. **Send.** `UploadBatchWithEvidenceAsync` streams the file.
   - Any exception after the call has started is an unknown outcome: the batch is
     quarantined and the run blocked. It is never re-sent.
   - An ACK is validated by the store against the claimed attempt. The evidence is the
     SHA-256 of the exact response body bytes plus the real HTTP status.
4. **Ledger writes after the call.** Ack, Fail and Retry get up to 3 attempts. The store
   methods are idempotent per attempt: a retry after a write that committed but threw
   returns `AlreadyObserved` or `ClaimLost` and writes nothing. If all attempts fail, the
   worker logs `ETL_BATCH_OUTCOME_UNRECORDED` (Critical). The batch then stays `uploading`
   and the run stalls until the next service start, which orphans the attempt and
   quarantines the batch. It is never re-sent.
5. A pass returns the number of batches actually claimed, so an unclaimable due row does
   not spin the loop.

### `EtlCompletionWorker`

1. `GetDueRunCompletions` → `TryClaimRunCompletion`, bounded by
   `EtlOptions.MaxRunCompletionAttempts`: default 20, valid range 1..100.
2. `CompleteEtlRunRawAsync` sends the stored payload byte-identically (H1).
3. It then calls `FinalizeEtlRunAsync` (F1). A failed send is retried with backoff from
   10 s to 10 min. Exhausting the attempts blocks the run.

## Startup (`BootstrapService`)

The steps run in this order.

1. `RecoverAsync`. The legacy `uploading → ready` reset is removed (bypass #7).
2. `BlockLegacyEtlRunsAsync`: runs without ownership bindings in running, uploading or
   completing state are blocked `LEGACY_UNRESOLVED`, and their pending batches are fenced.
   It runs first, so an interrupted legacy run is reported as legacy.
3. `RecoverInterruptedEtlRunsAsync`:
   - admitted attempts become `orphaned`;
   - `uploading` batches, including those of a just-blocked legacy run, are quarantined
     `UPLOAD_OUTCOME_UNKNOWN`, and durable runs owning them are blocked;
   - `running` runs are blocked `INTERRUPTED_NO_CHECKPOINT`.

The spool quota (`MaxSpoolBytes`) counts the upload backlog, not `quarantine/`. Quarantined
files still count against the disk reserve.
4. `.tmp` spool files are quarantined.
5. Ready spool files that no batch row references are moved to quarantine.
6. `BlockRunsWithMissingSpoolFilesAsync`: a ready or retry_waiting batch whose file is
   missing is dead-lettered (`quarantine_code` `RUN_BLOCKED`, `last_error`
   `SPOOL_FILE_MISSING`; migration 008 constrains `quarantine_code`), and its run is blocked
   `SPOOL_FILE_MISSING`.

In every case ownership and evidence are retained for R1.

## Administrative commands

`start_full_sync` and `reload_entity` are accepted through
`AcceptEtlJobAndCompleteCommandAsync`: the durable job and the accepted command result are
written in one transaction. The RAM `EtlTrigger` is gone (bypass #8).

Refusals:

| Case | Result |
|---|---|
| Unknown entity | `ENTITY_UNKNOWN` |
| Type/mode mismatch | `ENTITY_SELECTION_INVALID` |
| `reconcile_*` | `ETL_MODE_UNSUPPORTED` |

## Fenced legacy APIs (§9)

The legacy store methods moved from `IAgentStore` to `ILegacyEtlStore`, which no
production type depends on:

- `CreateEtlRunAsync`
- `RegisterBatchAsync`
- `MarkEtlRunExtractedAsync`
- `GetPendingBatchesAsync`
- `MarkBatchRetryAsync`
- `AcknowledgeBatchAsync`
- `GetRunsReadyToCompleteAsync`
- `CommitWatermarkAsync`
- the store's `CompleteEtlRunAsync`

The legacy workers and `EtlTrigger` are deleted. `EtlBatchAckStatusTests` exercised the
deleted worker and is removed; the O2 tests cover its invariant (only a valid
`acknowledged` ACK confirms a batch). `IErpClient.CompleteEtlRunAsync(object)` remains on
the client interface but has no production caller.

## Upgrade of an existing database

- Legacy in-flight runs (running, uploading or completing) are blocked
  `LEGACY_UNRESOLVED` on the first start. Their in-flight `uploading` batches are
  quarantined `UPLOAD_OUTCOME_UNKNOWN`.
- Legacy watermarks have no fingerprint, so they yield `BEGIN_DOMAIN_UNKNOWN` on first use.

The operator procedure is in `docs/etl-runbook.md`: R1 resolution, then a D1 domain reset,
then a new baseline.

## Review

An independent review of the production code found issues, all fixed:

- an upload hot loop;
- refused register, complete or seal only logged;
- the spool-quota precheck was lost;
- cancellation during the precheck left the attempt `admitted`;
- no `SPOOL_FILE_MISSING` reconciliation;
- the ACK hash was taken over a re-serialized ACK with the status hard-coded to 200;
- no retry of ledger writes after the ERP call;
- a disk probe exception was not handled.

Known limits, left open:

- An OData failure that outlasts the page retry (A10c: transient errors are retried per
  page, bounded) fails the run, and it needs R1 `retry`.
- The completion scan takes 4 due runs per pass, in order.
- `IErpClient.CompleteEtlRunAsync(object)` is not removed from the interface.

## Tests

- Existing tests adapted: the A07 constructors take `DynamicConfigurationState`. The O2
  legacy-recover test now asserts that `uploading` is not reset.
- Behaviour tests:
  - `EtlPipelineC1Tests` (P1–P11): end-to-end manual and scheduled runs, identity, upload
    uncertainty, precheck, completion byte identity, disk, pause, startup recovery,
    administrative commands, fencing;
  - `EtlC1ReviewFixTests` (R1–R9): the review fixes.

## Verification

Release, main and a clean LF clone: 754/754 integration and 243/243 unit. Evidence is in
`local-data/remediation-2026-09-26/c1-root/final`. A second independent review of the
post-review fixes and the tests found no production defect; its should-fix items are
applied (the `ETL_BATCH_OUTCOME_UNRECORDED` critical log, a spool quota without
`quarantine/`, stricter R5, R6 and P9 tests). Devin's R8 test found the migration 008
CHECK on `quarantine_code`; missing-file reconciliation now stays within it.
