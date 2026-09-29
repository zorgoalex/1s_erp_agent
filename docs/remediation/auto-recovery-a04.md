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

## Open: A04b

The interrupted run stays open at ERP until ERP's 24 h `abandoned` timer. ERP agreed
(`to-onec/0049`) that the agent may close it right away with `complete` v2:
- `partial_success`, every entity `failed` with `RUN_INTERRUPTED`;
- `batchesAcknowledged` exactly the acknowledged count;
- 503 is retried, 409 is not retried.

That needs a durable send queue and comes as a separate slice.

## Tests

`EtlAutoRecoveryA04Tests` (integration, 4):
- an interrupted manual run is resolved (system operator, `retry`, ownership released) and
  re-queued with the same mode, entities, configuration version and generation. The new job
  is claimable, and a second call finds nothing;
- the chain stops at the limit, and the last run stays `INTERRUPTED_NO_CHECKPOINT`,
  unresolved;
- a run with an admitted send (`UPLOAD_OUTCOME_UNKNOWN`) is never auto-recovered;
- an interrupted scheduled run is resolved and its schedule key is free again.

Full suite: 974 integration + 325 unit.
