# L1 — command latency: wake-ups instead of polling pauses

Date: 2026-09-28. No migration. Agreed with ERP over agent-bridge (`to-onec/0008`,
`to-erp/0003` §6).

## Before

Every stage of the command path added a polling pause:

| Stage | Pause |
|---|---|
| Stored command → execution | the execution worker polled the queue every 250 ms (0–250 ms added) |
| Result → `PUT result` | the delivery worker polled the outbox every 500 ms (0–500 ms added) |
| Empty long poll → next `lease` | a fixed 250 ms pause |

## Change

- **`CommandWorkSignals`** is a DI singleton with two signals, `Commands` and `Results`.
  - SQLite stays the only source of truth: a signal only says "look now".
  - Each worker keeps its old timed re-check (250 ms / 500 ms) as a fallback. Work that
    becomes ready by time or state (retry due, not-before, mode change) therefore keeps
    exactly the old latency, and a missing signal never loses work.
- **Lease worker.** After intake it pulses both signals: the stored command wakes execution,
  and a rejected command's stored result wakes delivery.
- **Execution worker.** It waits on `Commands` instead of sleeping. After every pass it
  pulses `Results`.
- **Delivery worker.** It waits on `Results` instead of sleeping.
- **Empty lease.**
  - The next lease goes out at once.
  - A minimum cycle of 250 ms, measured from the start of the lease request, protects
    against an endpoint that answers empty early: such an endpoint gets at most ~4 requests
    per second.
  - A conforming ERP holds the request for `LongPollSeconds`, so it sees no pause.
- **`WorkSignal`** is a coalescing auto-reset signal for exactly one consumer.
  - A pulse that arrives while nobody waits is kept, so no wake-up is lost between an empty
    query and the wait.
  - The fallback timer is cancelled as soon as a pulse wins.

Result: the agent adds milliseconds plus the SQLite writes; the rest is 1C time.

## Review

An independent review found no must-fix issues. It confirmed:
- no lost wake-ups;
- DI gives all three workers the one registered instance;
- every writer of the result outbox is covered by a pulse or runs before the workers start.

Applied should-fix items:
- the fallback timer is cancelled when a pulse wins, so short cycles leave no live timers;
- the doc comment now says "exactly one consumer";
- a test checks that the host registers the shared signals;
- the latency bound was relaxed to 200 ms, which still separates old from new.

Left as is (nit): the execution worker pulses delivery even after a pass that wrote nothing.
Delivery then runs one extra empty query.

## Tests

`L1CommandLatencyTests` (11):
- **Signal:**
  - a pulse before the wait is kept, and several pulses coalesce into one;
  - a pulse wakes a waiter long before its timeout;
  - cancellation ends the wait;
  - 2000 pulse/wait cycles complete promptly.
- **Wiring:**
  - DI resolves the registered instance over the default;
  - `Program.cs` registers the signals;
  - intake wakes execution and delivery;
  - a finished command wakes delivery.
- **Timing:**
  - after a held empty poll, the median gap to the next lease is < 100 ms (old: ≥ 250 ms);
  - an endpoint that answers empty at once makes 2–10 calls in 1.5 s (floor guard only;
    the old code passes this too);
  - stored command → executed → result sent: median of 5 < 200 ms (old: median ≈ 290 ms).

Sabotage check: with the pulses disabled and the fixed pause restored, 6 of the first 8 tests
fail. The cancellation test and the floor guard pass by design.
