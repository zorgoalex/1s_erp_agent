# L2 — no lease at full capacity

Date: 2026-09-29. No migration. Found by the first live `integration_probe` batch on stage
(agent-bridge `to-onec/0044`), agreed with ERP (`to-erp/0044`, `to-onec/0045`).

## Problem

Four probes were queued at once. Each executed in 0.34–0.57 s, but they reached the agent
25 s apart.

The agent does send the next lease right after intake, but by then the command is already
executing. With `MaxConcurrency = 1` the lease therefore reports `currentLoad` 1/1. ERP
hands out no command while `executing >= capacity` and holds such a lease for the whole
long poll (25 s). So a queued batch cost one long poll per command.

## Change

- **The load counts accepted but not started commands.** It is the number of commands
  executing in this process plus `IAgentStore.CountReadyUnclaimedCommandsAsync`: stored
  commands that an execution pass would select right now, not claimed by an executor.
  - The count uses the same predicate as the selection (`ReadyNowPredicate`, shared in
    SQL).
  - Rows waiting for their `notBefore` or retry time are not counted, so they can never
    hold leasing back.
- **No lease at full capacity.** When the load is `>= MaxConcurrency`, the lease worker
  does not lease. It waits for `CommandWorkSignals.Capacity`, which the execution worker
  pulses after the execution scope is released. If no pulse comes, the worker re-checks
  after 1 s (`CapacityRecheck`).
- **The lease reports that load** as `currentLoad.executing`.

ERP needed no change.

Expected result: a queued batch runs at execution speed plus network, about 0.5 s per probe
instead of 25 s.

## Tests

`L2CommandCapacityTests` (integration, 5). The fake ERP applies ERP's rule: at full load it
gives no command and holds the lease for 3 s.

| Test | What it checks |
|---|---|
| batch of 4 with capacity 1 | Finishes faster than a single hold. No lease reports full load. |
| accepted-but-not-started command fills the only slot | No lease for 1.5 s. After the execution worker frees the slot, a lease with load 0/1 goes out within 2 s. |
| two stored commands, capacity 4 | The lease reports 2/4. |
| command with `notBefore` in an hour | Does not hold leasing back. |
| `CountReadyUnclaimedCommandsAsync` | Counts only ready, unclaimed rows. |

**Sabotage check:** with the previous `CommandLeaseWorker`, tests 1–3 fail and 4–5 pass.
Tests 4 and 5 guard against regressions.

L1 tests still pass.
