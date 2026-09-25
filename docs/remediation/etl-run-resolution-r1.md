# R1 — attested manual resolution of failed/blocked ETL runs (migration 010) — DARK storage slice

Date: 2026-09-26. Baseline: main `1ff842e` (O3, 711/711). Design:
[etl-ownership-upload-design.md](etl-ownership-upload-design.md) §8.

## Why

Before R1, nothing wrote `etl_runs.resolved_at_utc`. Any failed or blocked run therefore
held its entity ownership, and for a scheduled run its schedule key (O3), forever. R1 is
the only attested exit from that state.

## Schema (`010_etl_run_resolutions.sql`, LF, additive)

`etl_run_resolutions` holds one immutable record per resolved run.

Columns:
- `resolution_id`, `resolved_at_utc`;
- `operator_id`, `decision` (`abandon|retry|rebaseline`), `remote_verification`;
- `workers_quiesced` (must be 1);
- `prior_status` (`failed|blocked`), `prior_conflict_code`;
- `ownership_released`, `batches_fenced`.

Constraints: CHECKs on every field, and a foreign key to `etl_runs`.

LF SHA-256: `AED834B63AC9051690F9DEFDD253A0BD003B026416BDF93934C536AC464DFE20`. Files
001–009 are byte-identical.

## API: `IAgentStore.ResolveEtlRunAsync(request, now)`

**Durable preconditions**, checked in one `BEGIN IMMEDIATE` transaction:
- the run is failed or blocked and not yet resolved;
- no send attempt of its batches is `admitted`;
- no batch is `uploading`;
- no extraction claim and no completion claim;
- its job is not pending, deferred or running.

The request must attest worker quiescence and state what was verified at ERP; otherwise
the call throws `ArgumentException`.

**Effect**, in ONE commit:
- the record is written and `resolved_at_utc` is set;
- remaining pre-acknowledgement batches are fenced (`dead_letter`, `RUN_BLOCKED` /
  `RUN_RESOLVED`);
- epoch-bound active ownership is released (`manual_release`). Foreign rows and re-acquired
  rows never move.

What does NOT change: the job stays blocked, watermarks are untouched, and all evidence
stays immutable. A resolved scheduled run releases its schedule key.

**Outcomes:**
- `Resolved(record)`;
- `AlreadyResolved(record, SameRequest)`, with zero writes;
- `Refused(reason)`, with zero writes. Reasons: `RunNotFound`, `RunNotResolvable`,
  `AdmittedSendAttempt`, `BatchInFlight`, `LiveExtractionClaim`, `LiveCompletionClaim`,
  `LiveJobDispatch`.

## Evidence

Evidence lives in `repo_1c-agent/local-data/remediation-2026-09-26/etl-resolution-r1-root/`
and the Devin worktree `local-data`.

**Tests (Devin, to the orchestrator's scenario list):**
- 29 R1 behaviour tests, all RED on the contract stub (`NotImplementedException`); no
  pre-existing test regressed (666/695 integration + 48 unit);
- migration v9→v10 tests with the populated v9 fixture;
- schema version pins 9→10.

After merging onto the implementation, one test failed because of its own setup: the
first deferral set `available_at` 5 minutes ahead, and the immediate re-claim was
correctly `NotClaimable`. The deferral time was corrected; the store was not changed.

**Independent review** (fresh agent) found no correctness defect: the in-transaction
preconditions, the effects, the epoch-bound release, concurrency and O3 key release all
hold. Changes made from its findings:
- `prior_conflict_code` was added to the record for audit;
- `AlreadyResolved` now reports `SameRequest`;
- the limits below were documented.

## Limits

- **Legacy in-flight sends are not durably visible.** A legacy ledger-less send whose batch
  a block already dead-lettered cannot be seen by the store. Until C1 fences the legacy
  writers, this case rests on the `workers_quiesced` attestation.
- **Some legacy-failed runs are unresolvable.** A run set `failed` by the legacy
  `CompleteEtlRunAsync` while still holding claims or a running job is refused
  (`LiveExtractionClaim`/`LiveCompletionClaim`/`LiveJobDispatch`). This fails closed. The
  C1 cutover removes that writer; such rows need recovery first.
- **`rebaseline` is only recorded.** Enforcement is D1 (the `BaselineRequired` rule and
  the domain reset).
- **Evidence may be mislabelled.** Batches reset `uploading→ready` by legacy `RecoverAsync`
  before cutover are fenced `RUN_BLOCKED/RUN_RESOLVED` instead of
  `UPLOAD_OUTCOME_UNKNOWN`. That writer is removed in C1.
