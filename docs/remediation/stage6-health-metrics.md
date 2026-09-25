# Stage 6 — health and metrics

Date: 2026-09-26. No migration, no heartbeat contract change.

## What changed

- **`QueueMetrics`** (store) adds three values:
  - `OldestPendingCommandAtUtc`: the earliest `received_at_utc` of commands in queued,
    retry_waiting, unknown_result or executing;
  - `OldestPendingResultAtUtc`: the earliest `created_at_utc` of results awaiting
    delivery (pending, sending, retry_waiting), read through `ix_results_pending`;
  - `EtlRunsUnresolved`: runs in `blocked` or `failed` state without an
    `etl_run_resolutions` record. R1 does not change the run status, so the `NOT EXISTS`
    check is required.

  All timestamps are written as UTC round-trip strings, so `MIN` is chronological.
- **Health state.** `EvaluateHealth` now returns `(State, Reason)`. The order of checks is:
  1. incompatible version;
  2. maintenance;
  3. storage (reasons `SQLITE_LIMIT`, `SPOOL_LIMIT`, `DISK_RESERVE`);
  4. 1C offline;
  5. `degraded`, with one of these reasons:
     - `ONEC_COMMAND_API_UNAVAILABLE`
     - `ONEC_ODATA_UNAVAILABLE`
     - `CLOCK_DRIFT`
     - `ETL_RUNS_UNRESOLVED`
     - `ETL_LAG`
  6. healthy.

  The heartbeat keeps sending only the state string.
- **ETL lag.** Lag means no successful run for 3 schedule intervals since the last success
  in this process. It is checked only when all of these hold:
  - ETL is enabled;
  - extraction is allowed (not `PauseEtl` and no local pause);
  - at least one enabled entity runs on schedule.
- **`AGENT_HEALTH` log.** The heartbeat contract has no field for queue ages or
  unresolved runs, so the full picture is logged locally when the state or reason
  changes, and every 15 minutes. The level is Warning unless the state is healthy. Fields:
  - state and reason;
  - mode and local ETL pause;
  - pending commands and results, with their oldest ages;
  - ETL batches pending and unresolved runs;
  - dead letters;
  - last ETL success;
  - free disk;
  - ERP clock offset.

## Notes

- After an upgrade, failed or blocked runs from before R1 make the agent `degraded`
  (`ETL_RUNS_UNRESOLVED`) until each is resolved with `--etl-resolve-run`. This is
  intended: such runs hold entity ownership and schedule keys.
- The existing `ResultsPending` count still scans `results_outbox`. This was the case
  before this change, and no rows are ever deleted from that table. A partial index is a
  candidate for a later migration.

## Review

An independent review found no must-fix issues. It confirmed:

- the DI wiring;
- that the SQL is correct and `NOT EXISTS` is needed;
- that the timestamp ordering is chronological;
- the order of the health checks.

Its should-fix items are applied:

- ETL lag no longer fires during `PauseEtl` or the local pause;
- ETL lag no longer fires when no entity is scheduled;
- the result-age query uses only the pending-delivery statuses;
- a reason code is added.

## Tests

`HealthMetricsTests` (8):

- empty store;
- oldest command and result, including a finished command and delivered, dead-letter
  results that do not count;
- unresolved runs counted until resolution;
- `degraded` for unresolved runs and for ETL lag (only after a first success, and only
  with a limit);
- storage outranks ETL;
- reason codes.

RED on the pre-fix build: 4 tests.
