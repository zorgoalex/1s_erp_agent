# A07 time and load — ERP clock, command expiry, lease load, uptime

Date: 2026-09-26. No migration, no contract change.

## Defects

- Command expiry (`expiresAtUtc`) is set by ERP but was compared with the local clock
  only. An agent whose clock lagged ERP could execute a command that ERP already
  considered expired.
- `SessionStartResponse.ServerTimeUtc` was ignored, so clock drift went undetected.
- The lease request always sent `LeaseLoad(Executing: 0, …)`.
- Uptime in the heartbeat was a difference of wall-clock times, which a clock correction
  distorts.

## Implementation

### ERP clock estimate (`AgentRuntimeState`)

- **Measurement.** At each accepted handshake the offset is measured as
  `serverTime − (sentAt + RTT/2)`. Its error bound (the uncertainty) is `RTT/2`.
- **Anchoring.** The ERP time is anchored on the monotonic clock (`Stopwatch`) and carried
  forward from there. `ErpClockOffset` is always "ERP estimate − current local clock", so a
  later Windows clock correction shows up at once and the offset never goes stale.
- **Rejected samples.** A sample is ignored, and the previous estimate kept, when:
  - the RTT is above 2 s (retries or congestion make it too imprecise);
  - the offset is larger than a day (a broken ERP clock);
  - `ServerTimeUtc` is missing.

  An ignored sample is logged as `ERP_CLOCK_SAMPLE_IGNORED`.
- **Expiry.** `ExpiryNow(local)` returns `max(local, ERP estimate + uncertainty)`: a
  command is expired as soon as either clock says so. `CommandExecutionService` takes it as
  an optional `expiryNow` delegate. The default is the local clock, so existing callers
  keep their behaviour.
- **Drift.** `ClockDriftExceeds(threshold)` compares `|offset| − uncertainty` with the
  threshold, so a wide error band does not raise a false alarm. Above
  `Agent:MaxClockDriftSeconds` (default 30), the handshake logs `CLOCK_DRIFT` and the
  heartbeat health becomes `degraded`.

### Lease load

`CommandExecutionWorker` wraps each processing pass in `state.BeginCommandExecution()`.
The lease request sends `LeaseLoad(state.ExecutingCommands, MaxConcurrency)`.
"Executing" means local execution slots in use; this includes status-lookup passes, which
also call 1C. The queued local backlog is not included.

### Uptime

`AgentRuntimeState.Uptime` is measured with a `Stopwatch`.

## Open

- **`notBeforeUtc`** still uses the local clock (`GetReadyCommandsAsync` and the claim
  guard). The conservative bound would be `min(local, ERP estimate − uncertainty)`. Whether
  not-before is a safety rule in the ERP contract is not confirmed. If it is, it needs a
  separate `$notBeforeNow` parameter; `next_attempt_at_utc` must stay on local time.
- **Lease renewal (CD-R9)** is not in the confirmed contract.

## Review

An independent review confirmed:

- the offset math and sign;
- the handling of a missing server time;
- the scope lifetime;
- that other expiry checks (the ETL acceptance SQL guard) are harmless.

It found three should-fix items, which are addressed:

- the stored offset went stale after a clock correction; the estimate is now anchored on
  the monotonic clock;
- retries inflated the uncertainty; samples with an RTT above 2 s are dropped and drift is
  judged net of the uncertainty;
- `notBeforeUtc` still uses local time; this is recorded as open (see above).

## Tests

`A07ClockAndLoadTests` (13) and one new `CommandExecutionTests` case cover:

- expiry with an unknown clock, with ERP ahead of the local clock, and with ERP behind it;
- rejected samples: a slow one and an implausible one;
- drift judged net of the uncertainty;
- a missing server time;
- a delayed handshake;
- the `CLOCK_DRIFT` log, and its absence for a small offset;
- the execution counter;
- the lease load;
- health `degraded` on drift;
- monotonic uptime;
- a command expired on the ERP clock that is not executed although the local clock lags.

RED on the pre-fix build: 7 tests.
