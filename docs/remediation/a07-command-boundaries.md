# A07b — B2/B7 command resolution/admission split (bounded slice, implemented)

Date: 2026-09-24. Baseline: checkpoint-278, worktree `a07-command-boundaries`. Scope: the bounded B2/B7 slice of `a07-boundaries-design.md` §5, under the **Root disposition** there and subsequent root narrowing. **This document does not claim A07 (or stage 2) is complete.** B1/B3/B4/B6 remain open per design §6.

## Implemented local policy

1. Status lookup of already-sent **business** commands is permitted whenever `IsReady`, under every command restriction (`PauseCommands`/`Maintenance`/`Disabled`/handshake maintenance). `Drain` and a local ETL pause alone still admit all work.
2. **ANY** POST to 1C — fresh execution **and** the `NotFound` same-id retry — and every **fresh administrative callback** is denied while `CanExecuteCommands` is false.
3. A sent-evidence administrative row resolves only through the existing **local quarantine** — zero `IOnecCommandClient` calls, zero callback re-runs.
4. Sent-work filtering is applied in SQL **before `LIMIT`** — no post-fetch filter, no empty-batch spin.

## Changes

| File | Change |
|---|---|
| `src/ErpOnecAgent.Service/Runtime/AgentRuntimeState.cs` | `AgentModeSnapshot.CanResolveCommandResults => IsReady` (explicit resolution boundary). |
| `src/ErpOnecAgent.Application/Abstractions/Persistence.cs` | `IAgentStore.GetDueSentCommandsAsync` — sent-work-only sibling of the ready query. |
| `src/ErpOnecAgent.Infrastructure/Persistence/Sqlite/SqliteAgentStore.Commands.cs` | Shared `GetReadyCommandsCoreAsync(limit, now, sentOnly)`: identical `status IN ('queued','retry_waiting','unknown_result')`, `not_before`/`next_attempt` due filters, same-`ordering_key` `NOT EXISTS` head-of-line guard, `ORDER BY priority DESC, received_at_utc, queue_sequence LIMIT`; sent-only adds `AND (status='unknown_result' OR attempt_count>0 OR post_attempt_count>0 OR first_sent_at_utc IS NOT NULL)` in SQL before LIMIT. Full query unchanged. |
| `src/ErpOnecAgent.Application/Commands/CommandExecutionService.cs` | `ProcessAsync` gains optional `Func<bool>? mayStartNewWork` (default `true`, legacy callers unrestricted). Sent-evidence predicate made consistent in snapshot and live routing: `unknown_result` OR `attempt_count>0` OR `post_attempt_count>0` OR `first_sent_at_utc` non-null. Never-sent path: `admit()` evaluated inside the owned pass **before** expiry/admin/fresh paths — denial leaves the row untouched (not expired, not dead-lettered). `ExecuteFreshAsync` re-evaluates `admit()` **immediately before** `ClaimPostAttemptAsync` (the claim itself stamps send evidence — a post-claim check would fabricate it); this also covers POSTs reached after an async admin callback returning false and the NotFound re-POST. NotFound denied: closes only the claimed lookup attempt `unknown_result`/`NOT_FOUND` with deferred text, owner-aware `MarkUnknownResultAsync` backoff, **no POST budget consumed**; a later pass looks up again before retrying. |
| `src/ErpOnecAgent.Service/Workers/CommandExecutionWorker.cs` | Loop gates on `IsReady`; selects full query when `CanExecuteCommands`, else the sent-only query when `CanResolveCommandResults`; empty batch keeps the 250 ms delay (no outer `CanExecuteCommands` skip, no spin). Passes `() => state.Snapshot.CanExecuteCommands` as `mayStartNewWork`. |

**Not atomicity**: `mayStartNewWork` is a documented admission **decision point**, not an atomic mode+network transaction — a restriction landing after the check can race one in-flight call, which resolves via the unknown-result machinery. In-flight calls are never cancelled. Owner fencing, terminal immutability, separate lookup/POST budgets, backoff, ordering and uncertainty handling are unchanged; no schema, migration, contract, package, or other-worker changes.

## Test evidence

**RED (against unchanged baseline, real SQLite + real `CommandExecutionWorker`)** — `A07CommandBoundaryRedTests`, 2 tests, both failed with runtime assertion failures (see `docs/remediation/a07-command-boundaries-red.md`):

- `PauseCommands_still_resolves_already_sent_business_command_via_lookup` — zero `GetStatusAsync` in the bounded window: single loop gate blocked resolution of sent work.
- `Mode_restriction_between_fetch_and_claim_suppresses_fresh_post` — POST issued (`ExecuteCalls == 1`) after `Normal → PauseCommands` injected at the claim: gate evaluated once per iteration. The original runtime RED used the bounded POST-observation window. The later strengthened version waits for real `ReleaseCommandExecutionClaimAsync` completion via the proxy and was verified GREEN; no separate baseline RED run of that strengthened version was recorded.
- RED TRX: `local-data/remediation-2026-09-24/a07-command-boundaries/red/a07-boundaries-red-20260924-223255.trx`.

**GREEN + matrix** — `A07CommandBoundaryTests` (21 cases in the final version, including the later fresh-admin test) + the 2 strengthened regression tests: 23 new cases pass in the final full suite. The earlier targeted run had 22 cases before that addition.

| Coverage | Tests |
|---|---|
| Sent business row resolved under `PauseCommands`/`Maintenance`/`Disabled`/handshake maintenance (worker) | 4 |
| Never-sent row denied under `PauseCommands`/`Maintenance`/`Disabled`/handshake (still `queued`, zero 1C calls, bounded poll barrier) | 4 |
| `Drain` and local-ETL-pause-alone still admit fresh POSTs | 2 |
| `NotFound` under pause: audited `unknown_result`/`NOT_FOUND` close, backoff retained, zero POST budget → lift → lookup again → same-id re-POST | 1 |
| `MaxConcurrency+1` blocked fresh rows (distinct keys) ahead of due `unknown_result` — no starvation | 1 |
| Same-key active predecessor still blocks the sent row in both queries | 1 |
| All-fresh restricted queue polls on 250 ms cadence (bounded query count, zero 1C) | 1 |
| Restriction landing during async admin callback (`false`) suppresses the POST; row untouched, claim released | 1 |
| Fresh admin callback denied under pause (no side effect, still `queued`) | 1 |
| Sent admin row → local dead_letter quarantine, zero `IOnecCommandClient` calls, callback never re-runs | 1 |
| `retry_waiting` post-evidence and `first_sent_at_utc`-only evidence route to lookup even with admission denied | 2 |
| `!IsReady` issues zero command queries/1C calls | 1 |
| Expired never-sent row under denied admission stays `queued`, expires after lift | 1 |

**Full suite** (Release, `-t:Rebuild`, locked restore): **301/301 passed** — IntegrationTests **262/262**, UnitTests **39/39**.
TRX: `local-data/remediation-2026-09-24/a07-command-boundaries/a07-boundaries-full-integration-20260924-224647.trx`, `a07-boundaries-full-unit-20260924-224659.trx`.

**Existing test adjusted**: `A07ModeStateTests.Resume_admin_command_does_not_clear_remote_pause` previously executed a *fresh* `resume_etl` under `PauseCommands` — behaviour now correctly denied (fresh admin callbacks are new work). Its original intent (resume clears only the durable local pause, remote restriction persists) is preserved under `PauseEtl`, which does not gate commands.

## Limitations

- The admission check is a per-pass decision point; a mode flip between the final check and the network call can still race one in-flight POST (resolved via unknown-result machinery; never cancelled).
- The OPEN sub-policy on whether `MaxResolutionAgeHours` should exclude restricted time is **not decided** — counters, backoff and the age budget are unchanged.
- [OPEN] items per design §6 remain: B1 long-poll admission point, B3 extraction pause boundary (blocked on A04/A05), B4 `CanCompleteEtlRuns` wiring, B6 §30.2 compatibility (heartbeat decoupling, `incompatible_version`, explicit-rejection latch), B9 durable triggers, B10 heartbeat/lease-clock backlog.
- No claim that A07 is complete; all [PROPOSED] contract items await owner confirmation and [LOCAL-POLICY] items remain subject to root review.

## Independent root acceptance — 24.09.2026 22:50

Root reviewed guards/query ordering, strengthened pass-completion barrier, changed existing admin fixture and added denied-admin coverage. Merged with accepted ETL foundation using three-way merge of Persistence.cs. Native Release -t:Rebuild0 warnings/errors; full solution39unit+316integration=355/355, no failures/skips. Source manifest remained stable. Independent logs and separate unique-prefix TRX: local-data/remediation-2026-09-24/a07-command-boundaries-root/. Agent evidence copied to sibling a07-command-boundaries-devin/. Historical shared-name full TRX was overwritten by the unit assembly; use final separate integration/unit TRX and root artifacts, not that intermediate file. No full A07/stage2 closure.
