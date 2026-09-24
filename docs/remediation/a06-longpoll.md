# A06 — Long-poll lease pipeline: single attempt, nested budgets, no auto-retry

Scope: `agents/worktrees/a06` only. No main-repo (`repo_1c-agent`) modification, no `AgentOptions` /
`ErpOptions` change, no schema/contract/worker change. Baseline 57 tests preserved and green.

## Problem

The long-poll lease pipeline (`POST commands/lease`, a hold of up to `LongPollSeconds`, default 25s)
had one real production defect, plus two bugs introduced by the intermediate A06 work-in-progress.

**Original production defect:**

1. **Flat / standard 10s attempt timeout.** The attempt timeout, the total budget and the
   `HttpClient` timeout were all derived from the same value, and the effective cap on one long-poll
   hold was the *standard* **10s attempt timeout** (`AddStandardResilienceHandler`'s default attempt
   timeout carried over as the mental model). That 10s cap aborts a legitimate 25s hold. The circuit
   breaker was also outside the attempt timeout, so it never observed a `TimeoutRejectedException`
   and could not open on hung/slow polls.

**Intermediate implementation bugs (introduced during the A06 work-in-progress, NOT original
production defects):**

2. **Invalid Polly retry configuration.** `ConfigureLongPollPipeline` called
   `AddRetry(... MaxRetryAttempts = 0 ...)`. `MaxRetryAttempts` must be `>= 1`; `0` is an invalid
   Polly `RetryStrategyOptions` and would throw at pipeline build/first use. Even a *valid* retry
   strategy is wrong here: the lease POST is non-idempotent, so a transient 503/timeout could
   silently replay it and double-lease a command. (Fix: remove the retry strategy entirely — single
   attempt, no automatic lease POST replay.)
3. **Test-only constructor exposure.** `ErpLongPollResilienceTests` built `ErpClient` with the
   internal 4-arg constructor, which would have required exposing that constructor to tests.
4. **Duplicate registration + dead artifacts.** `AddErpApi` inlined a copy of the long-poll
   registration instead of reusing `AddErpLongPollClient`; an unnecessary 9th test project
   (`ErpOnecAgent.Service.IntegrationTests`, zero `.cs` sources) was added to the solution; and
   `ErpOnecAgent.Service` carried `InternalsVisibleTo` entries for test assemblies.

## Changes

| Area | File | Change |
|---|---|---|
| Pipeline | `src/ErpOnecAgent.Infrastructure/ErpApi/ErpClientRegistration.cs` | `ConfigureLongPollPipeline`: **retry strategy removed entirely** (single attempt; no automatic lease POST replay). Distinct nested budgets with `attempt < total < HttpClient`. Strategy order `AddTimeout(total)` → `AddCircuitBreaker` → `AddTimeout(attempt)` so the attempt timeout is the innermost cap and its `TimeoutRejectedException` is what the breaker observes. |
| Budgets | same file | Replaced `LongPollBudget` with three explicit helpers: `LongPollAttemptTimeout = hold + margin(15)`, `LongPollTotalTimeout = attempt + margin`, `LongPollHttpClientTimeout = total + margin`. `HttpClient.Timeout` now uses `LongPollHttpClientTimeout`. |
| Registration | same file | `AddErpApi` now delegates the long-poll half to `AddErpLongPollClient(services)` (duplicate inline copy removed). `AddErpLongPollClient` uses an explicit `AddTypedClient` factory so the holder ctor is never reflected over. |
| Test surface | same file | `LongPollErpClient` ctor changed `public` → `internal` (constructed only by DI; no test-only exposure). `ErpClient` 4-arg ctor stays `internal` and is **not** exposed. |
| Solution | `ErpOnecAgent.sln` | Removed the unnecessary `ErpOnecAgent.Service.IntegrationTests` project entry + its 4 config rows. Restored the **original 8 projects** (Domain, Contracts, Application, Infrastructure, Service, UnitTests, IntegrationTests, MockServer). |
| Project | `src/ErpOnecAgent.Service/ErpOnecAgent.Service.csproj` | Removed the unnecessary `InternalsVisibleTo` ItemGroup (UnitTests / IntegrationTests / Service.IntegrationTests). |
| Tests | `tests/ErpOnecAgent.UnitTests/ErpLongPollResilienceTests.cs` | Rewritten to resolve `IErpClient` through the **real** `ServiceCollection.AddErpApi` production registration, overriding only the named primary handlers for `Erp` and `ErpLongPoll`. Budget test asserts the strict `attempt < total < HttpClient` nesting. |

### The new production pipeline (single attempt)

```
HttpClient.Timeout = LongPollHttpClientTimeout  (hold + 45s; hold=25s -> 70s)
└─ AddTimeout(total)      = LongPollTotalTimeout    (hold + 30s -> 55s)
   └─ AddCircuitBreaker   (transient predicate, sees the attempt TimeoutRejectedException)
      └─ AddTimeout(attempt) = LongPollAttemptTimeout (hold + 15s -> 40s)
         └─ primary handler (the actual lease POST — sent exactly once)
```

## Tests (real production registration)

All in `tests/ErpOnecAgent.UnitTests/ErpLongPollResilienceTests.cs`, driving `AddErpApi` →
`IErpClient` with only the transport stubbed:

| Test | Expectation |
|---|---|
| `Long_poll_hold_beyond_attempt_timeout_is_not_cut_off_and_is_not_retried` | 25s-configured budget, full 25s hold (> the old standard 10s cap): returns `hasCommand=false`, elapsed >= 24s (full-hold lower bound), `probe.Requests == 1` |
| `Long_poll_budget_is_strictly_ordered_attempt_total_httpclient` | `attempt=40s`, `total=55s`, `HttpClient=70s`; strict `attempt < total < HttpClient` and `attempt > hold`; scales with `LongPollSeconds` |
| `Lease_post_is_never_re_sent_on_503` | single 503 -> `HttpRequestException`, `probe.Requests == 1` (no replay) |
| `Cancellation_of_a_hung_long_poll_propagates_promptly` | cancel a 60s hang -> `OperationCanceledException` in < 5s, `probe.Requests == 1` |
| `Lease_is_routed_through_the_long_poll_channel_with_agent_headers` | `IErpClient.LeaseCommandAsync` uses the `ErpLongPoll` channel (1 request) and never the `Erp` channel (0); asserts path `/api/integration/1c-agents/v1/commands/lease`, `POST`, and `X-Agent-Id`/`X-Site-Id`/`X-Request-Id`/`X-Correlation-Id` headers |

## Runtime red captured (step 5) — not just a compile error

To prove the tests genuinely detect the pre-fix behavior, the **actual production helper**
`LongPollAttemptTimeout` was temporarily reverted to the original hard `TimeSpan.FromSeconds(10)`
and the same test was run. This is a **runtime** failure, not a compile error:

| Test | Result with original 10s attempt restored |
|---|---|
| `Long_poll_hold_beyond_attempt_timeout_is_not_cut_off_and_is_not_retried` | **Failed** — `Polly.Timeout.TimeoutRejectedException : The operation didn't complete within the allowed timeout of '00:00:10'.` |
| `Long_poll_budget_is_strictly_ordered_attempt_total_httpclient` | **Failed** — `Assert.Equal() Failure: Values differ. Expected: 00:00:40, Actual: 00:00:10` |
| `Lease_post_is_never_re_sent_on_503` | Passed (unaffected by timeout) |
| `Lease_is_routed_through_the_long_poll_channel_with_agent_headers` | Passed |
| `Cancellation_of_a_hung_long_poll_propagates_promptly` | Passed |

Red run summary: **2 failed / 3 passed / 5**, `TimeoutRejectedException` logged. The temporary
10s edit was then reverted (`LongPollAttemptTimeout` restored to `hold + margin`); no `TEMP-RED-CAPTURE`
marker remains in the source.

## Evidence

| Item | Path |
|---|---|
| **Final 25s test log (UTF-8)** | `local-data/a06/final25/final25-test.log` |
| **Final 25s build log (UTF-8)** | `local-data/a06/final25/final25-build.log` |
| **Final 25s TRX** | `local-data/a06/final25/TestResults/` (`final25_*.trx`) |
| Runtime red log (TimeoutRejected) | `local-data/a06/red/red-test.log` |
| Runtime red TRX | `local-data/a06/red/TestResults/red_net10.0_20260924015953.trx` |
| Green test log | `local-data/a06/green/green-test.log` |
| Green TRX (integration) | `local-data/a06/green/TestResults/green_net10.0_20260924020041.trx` (27, 0 failed) |
| Green TRX (unit) | `local-data/a06/green/TestResults/green_net10.0_20260924020053.trx` (35, 0 failed) |
| Locked-mode restore | `local-data/a06/restore.log` |
| Release `-t:Rebuild` (0 warn / 0 err) | `local-data/a06/build.log` |

All logs/TRX are isolated under `local-data/a06` (this worktree), not the main repo. The **final25**
run is the authoritative evidence for the corrected 25s-hold / 24s-assertion test.

## Test counts

| Run | Unit | Integration | Total |
|---|---|---|---|
| Original baseline | 30 | 27 | 57 |
| A06 final Release | 35 | 27 | **62 passed / 0 failed** |

Delta: +5 unit tests (the A06 long-poll suite). The original 57 baseline tests are preserved and
green. Full Windows Release `-t:Rebuild` = 0 warnings / 0 errors across the original 8 projects.

## Verification commands (Windows, native `repo_1c-agent/.dotnet/dotnet.exe`)

```powershell
.\.dotnet\dotnet.exe restore .\ErpOnecAgent.sln --locked-mode                    # 8 projects, lock files unchanged
.\.dotnet\dotnet.exe build   .\ErpOnecAgent.sln -c Release -t:Rebuild --no-restore | Tee-Object local-data\a06\final25\final25-build.log
.\.dotnet\dotnet.exe test    .\ErpOnecAgent.sln -c Release --no-build --no-restore `
    --logger "trx;LogFilePrefix=final25" --results-directory local-data\a06\final25\TestResults `
    | Tee-Object local-data\a06\final25\final25-test.log
```

## Limitations / explicitly open

- **No automatic lease POST retry, by design.** A transient 503/timeout on `commands/lease` is
  surfaced to `CommandLeaseWorker` and the worker schedules the next poll. If a caller needs
  immediate in-call retry, it must be added at the worker layer with idempotency, not here.
- The ordinary `Erp` channel keeps `AddStandardResilienceHandler` with
  `options.Retry.DisableForUnsafeHttpMethods()`, so lease/result POSTs and PUTs are still never
  replayed there either (unchanged from prior intent).
- The circuit breaker is scoped to the long-poll pipeline only (`FailureRatio 0.5`,
  `MinimumThroughput 10`, `SamplingDuration 60s`, `BreakDuration 30s`). It now observes the attempt
  `TimeoutRejectedException`, but opening it on a genuinely slow ERP is intentional back-pressure.
- `ErpClient`'s 4-arg constructor remains `internal` and is used only by the production
  `AddErpApi` typed-client factory. It is deliberately **not** exposed to tests; the routing test
  resolves `IErpClient` from `ServiceCollection` instead.
- `AgentOptions` / `ErpOptions` unchanged (verified: `LongPollSeconds = 25`,
  `RequestTimeoutSeconds = 60` defaults intact).
- No live ERP/1C; the transport is a stub (`ProbeHandler`) on the named channels only — the entire
  resilience pipeline and DI wiring under test is the real production code.
- The worktree is not a git repository (mirrors the repo baseline), so no commit/revision is recorded.

There is **no** retry strategy anywhere in this pipeline, so `POST commands/lease` is issued exactly
once per `LeaseCommandAsync` call. The first failure is surfaced to the caller (`HttpRequestException`
for 5xx, `TimeoutRejectedException`/`OperationCanceledException` for timeouts); the next poll is
scheduled by the worker, never by an automatic replay.