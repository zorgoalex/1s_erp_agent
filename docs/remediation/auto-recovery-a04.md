# A04a — automatic recovery of an interrupted extraction

Date: 2026-09-29. No migration. Option A, chosen by the user on 2026-09-29.

## Why not resume mid-entity

Resuming a read in the middle of an entity is not safe on this 1C. It was checked on the
test UNF with `local-data/e2e-2026-09-28/keyset-probe.py`:

- `Ref_Key gt guid'…'` returns **500** on every position;
- `$orderby Ref_Key` is not the lexical GUID order;
- a `$skip` resume shifts when rows are inserted in between.

At current volumes a full re-read of all 9 entities takes 3–4 s. So the unit of recovery is
the run.

## Change

`IAgentStore.AutoRecoverInterruptedRunsAsync(maxChain, now)` runs at startup in
`BootstrapService`, after the C1 recovery that blocks interrupted runs. It takes every
unresolved run with the `INTERRUPTED_NO_CHECKPOINT` code, one commit per run.

**Manual run** (`start_full_sync` / `reload_entity` job):
- R1 resolution as the system operator: `agent:auto-recovery`, decision `retry`. The same
  preconditions apply as in manual R1.
- The same work is queued as a new pending run + job in the same commit. It keeps the mode,
  the frozen entity definitions, the configuration version and the source generation.
- The job gets a synthetic command id: no ERP command stands behind it, and no result is
  ever sent for it.
- Its immutable `acceptance_result_json` records the lineage:
  `recovery.recoveryOf`, `recoveryAttempt` and `originCommandId`.

**Scheduled run:**
- R1 resolution only. The schedule key is released, and the scheduler creates the next run
  as usual.

**Chain limit:** `Etl:MaxAutoRecoveries`, default 3; 0 disables auto-recovery.
- When a run was already the last recovery of its chain, it stays blocked for a manual R1.
- Log code: `ETL_RUN_AUTO_RECOVERY_LIMIT`.

**Not auto-recovered:**
- runs with an unknown send outcome (`UPLOAD_OUTCOME_UNKNOWN`);
- runs R1 refuses (`ETL_RUN_AUTO_RECOVERY_REFUSED`);
- runs blocked with any other code.

A recovery run that meets a closed source generation is blocked `RUN_GENERATION_CLOSED` by
ERP. It is not `INTERRUPTED`, so it is not auto-recovered (ERP condition, `to-onec/0049`).

The R1 core now runs inside the caller's transaction (`ResolveInTransactionAsync`). Manual R1
and auto-recovery share it; the manual R1 behaviour is unchanged (38 R1 tests pass).

**Logs:**
- `ETL_RUN_AUTO_RECOVERED RunId NewRunId Attempt Max`;
- `ETL_RUN_AUTO_RESOLVED` for a scheduled run;
- `ETL_RUN_AUTO_RECOVERY_LIMIT`;
- `ETL_RUN_AUTO_RECOVERY_REFUSED`.

## A04b — closing the interrupted run at ERP

Migration **016** (`etl_run_interruption_notices`, schema v16) adds a queue of closing
completes. It was agreed with ERP in `to-onec/0049`.

**Queueing.** In the auto-recovery commit, a closing `complete` v2 is queued when ERP
acknowledged at least one batch of the interrupted run. The body is stored as the exact bytes
to send:
- `partial_success`;
- **every** entity of the run `failed` with `RUN_INTERRUPTED`, so ERP publishes none of them;
- `batchesAcknowledged` = exactly the run's acknowledged batches;
- the same identity and generation labels as a normal `complete`;
- no new fields.

**When ERP knows the run.** The notice is queued when ERP knows the run. The live test
(`to-onec/0051`) showed that ERP opens a manual run from the `start_full_sync` result, which
carries `data.runId`, before any batch. So ERP knows the run when either holds:
- it acknowledged a batch;
- the command result that carries the `runId` was delivered (`results_outbox` is
  `acknowledged`).

A recovery run's synthetic job has no result, so for it only an ACK counts.

**The body** lists every **frozen** entity of the run. An entity never begun is listed with
zeros: `readScope` from its definition, `rowsRead`/`batchesCreated` 0.

**The identity** comes from the run. A run interrupted before its first read has no namespace,
so it takes the current binding (`OneC:SourceBinding`), which `BootstrapService` passes in.
Without a binding nothing is queued.

**Sending.** `EtlInterruptionNoticeWorker` sends the stored bytes through the normal H1
`complete` call and handles the answers:

| Answer | Result |
|---|---|
| 2xx | `sent` (`ETL_INTERRUPTED_RUN_CLOSED`) |
| coded 409/422 | `refused`, never retried; ERP abandons the run after 24 h, the safe fallback |
| 503 / transport | retried with 10 s … 10 min backoff, at most 20 attempts, then `exhausted` |

ERP closes the run at once instead of abandoning it after 24 h. The per-entity alerts it raises
close themselves when the recovery run finishes those entities.

## Tests

`EtlAutoRecoveryA04Tests` (integration, 4):
- an interrupted manual run is resolved (system operator, `retry`, ownership released) and
  re-queued with the same mode, entities, configuration version and generation. The new job
  is claimable, and a second call finds nothing;
- the chain stops at the limit, and the last run stays `INTERRUPTED_NO_CHECKPOINT`,
  unresolved;
- a run with an admitted send (`UPLOAD_OUTCOME_UNKNOWN`) is never auto-recovered;
- an interrupted scheduled run is resolved and its schedule key is free again.

A04b tests (`EtlAutoRecoveryA04Tests`, 7 more):
- with one acknowledged batch the closing body is queued with exactly the agreed properties,
  `partial_success`, `batchesAcknowledged` 1 and both entities `failed RUN_INTERRUPTED`;
- without an ACK nothing is queued;
- the worker sends the exact stored bytes once:
  - 200 → `sent`;
  - 409/422 with a code → `refused`, no retry;
  - 503 or an uncoded 409 → `pending` with a future retry.

Schema pins moved 15→16 in the migration tests.

After the live test (+2):
- a run known only from the command result is closed with zeros, every frozen entity and the
  binding's identity;
- without a binding it is recovered but not closed.

Full suite: 983 integration + 325 unit.
