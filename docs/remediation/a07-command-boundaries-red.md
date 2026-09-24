# A07b B2/B7 — runtime RED evidence (resolution/admission split)

Date: 2026-09-24. Baseline: checkpoint-278 worktree `a07-command-boundaries`, **no production changes** — this file records failing reproductions only and does not claim any fix.

## Scope

Per root narrowing: two runtime regression reproductions against the unchanged baseline, real migrated SQLite + real `CommandExecutionWorker` + fake `IOnecCommandClient`, bounded waits only (2 s observe window, 5 s stop bound), gates released in `finally` before `StopAsync`. File: `tests/ErpOnecAgent.IntegrationTests/A07CommandBoundaryRedTests.cs` (2 tests, both RED on baseline).

## RED results

| Test | Reproduces | Baseline failure (actual assertion) |
|---|---|---|
| `PauseCommands_still_resolves_already_sent_business_command_via_lookup` | **B2**: ready + `PauseCommands`; business command seeded with real POST evidence (`ClaimPostAttemptAsync` → `MarkUnknownResultAsync`, due) must still be status-resolved — lookup is allowed once ready; only POST admission is denied | `Sent-evidence business command must be status-looked-up while CanExecuteCommands is false` — zero `GetStatusAsync` within the bounded window: the single `CanExecuteCommands` gate at `CommandExecutionWorker.cs:56` skips the whole loop, so resolution of already-sent work is blocked together with fresh admission |
| `Mode_restriction_between_fetch_and_claim_suppresses_fresh_post` | **B7**: worker fetches a fresh due row under `Normal`; `SetRemoteMode(PauseCommands)` lands while the pass is parked at `TryAcquireCommandExecutionClaimAsync` (store `DispatchProxy` gate); released claim then runs the pass | `Fresh POST was issued after CanExecuteCommands became false` — `ExecuteAsync` ran (`ExecuteCalls == 1`): the baseline evaluates `CanExecuteCommands` once per loop iteration and never re-checks inside the owned pass before the POST claim path |

Result: **2 tests, 2 failed** (deterministic runtime assertion failures, not compilation or fixture errors).

## Evidence

- TRX (final run): `local-data/remediation-2026-09-24/a07-command-boundaries/red/a07-boundaries-red-20260924-223255.trx`
- Earlier TRX `a07-boundaries-red-20260924-223242.trx` is superseded — test 2 had a fixture defect (`DispatchProxy` base must be non-sealed), fixed in-file; the final run is the valid evidence.
- Command: `dotnet test tests/ErpOnecAgent.IntegrationTests -c Release --filter FullyQualifiedName~A07CommandBoundaryRedTests` with native SDK `repo_1c-agent/.dotnet/dotnet.exe` (10.0.400), locked restore.
- No hangs: test 1 exits after the 2 s observe bound; test 2's claim gate is released in `finally` before `StopAsync`.

## What remains open

Implementation of the split (sent-only ready query before LIMIT, `CanResolveCommandResults`, in-pass `mayStartNewWork` guard incl. NotFound retry denial) is **not** done — awaiting root assignment. Broader B1/B3/B4/B6 items remain open per `a07-boundaries-design.md` §6.
