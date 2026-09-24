# Cancellation state guard

Date: 2026-09-24.

## Scope

This slice adds only the local SQLite state-machine guard required by TZFR-CMD018. It does not implement a cancellation transport, command-type policy, authorization rule, or a new public API. FR-CMD-018 remains open beyond this store invariant.

## Store invariant

`SqliteAgentStore.CompleteLocallyCoreAsync` applies the existing active-row and claim-owner predicate for every local completion. When `localStatus` is `Cancelled`, the same atomic update additionally requires:

- `status` is `queued` or `retry_waiting`;
- `attempt_count` and `post_attempt_count` are both zero; and
- `first_sent_at_utc` is null.

`executing` and `unknown_result` are therefore never cancellation-eligible, even when counters are zero. A queued or retry-waiting row with any persisted send evidence is refused conservatively. The existing owner predicate still requires an unclaimed row for the ownerless overload or the exact current owner for an owner-aware overload. A successful update still writes the result outbox and closes only the supplied attempt identity in the same transaction.

The ownerless cancellation and execution-claim transactions serialize on the row. If cancellation commits first, a later claim and POST are rejected. If the claim commits first, ownerless cancellation cannot clear the claim; an exact owner may still cancel before any POST attempt is claimed. Once a POST claim has committed, its counters and first-send evidence prevent cancellation, regardless of the later network result.

The method is not an authorization boundary. Callers remain responsible for accepting only an authorized cancellation instruction and applying command-type permission policy before invoking `CompleteLocallyAsync`.

## Regression coverage

`tests/ErpOnecAgent.IntegrationTests/CancellationStateGuardTests.cs` uses real migrated SQLite and covers:

- unclaimed never-sent queued and retry-waiting cancellation, including future `NotBeforeUtc`;
- exact-owner cancellation before POST and ownerless refusal while claimed;
- exact-owner refusal after a possible POST, recovered unknown POST, NotFound resolution, and a zero-counter unknown row;
- queued stale evidence with durable `first_sent_at_utc`;
- nonexistent, terminal, and duplicate cancellation no-ops;
- cancellation racing an execution claim plus POST, with TCS-gated concurrent operations; and
- unchanged normal successful completion.

Refusal assertions compare structural arrays containing every persisted command, outbox, and attempt row, including outbox and attempt identities.

## Evidence

All evidence is under `local-data/remediation-2026-09-24/cancellation-state/`.

- Runtime RED before the production fix: 15 focused tests executed, 5 failed and 10 passed, 0 skipped. The failures were the five cancellation-after-send/unknown/evidence cases. Console: `red/test-final.log`; TRX: `red/TestResults/cancellation-state-red-final_net10.0_20260924181555.trx`.
- Targeted GREEN: 15/15 passed, 0 failed, 0 skipped. Console: `targeted/test.log`; TRX: `targeted/TestResults/cancellation-state-targeted_net10.0_20260924181619.trx`.
- Native Windows Release Rebuild: 0 warnings, 0 errors. Console: `final/rebuild.log`.
- Full native Windows Release tests: 39/39 unit and 181/181 integration, 220 total, 0 failed, 0 skipped. Console: `final/full-test.log`; TRX: `final/TestResults/cancellation-state-full_net10.0_20260924181700.trx` (integration) and `final/TestResults/cancellation-state-full_net10.0_20260924181719.trx` (unit).
- Touched-file `dotnet format --verify-no-changes`: passed. Console: `final/format-verify-touched.log`.

## Change boundary

Production changes are limited to the cancellation predicate in `src/ErpOnecAgent.Infrastructure/Persistence/Sqlite/SqliteAgentStore.Commands.cs`. No migration, API, dependency, expiry rule, other local outcome, claim-release path, or command-type allowlist was changed.

## Independent acceptance

Orchestrator reviewed the atomic cancellation predicate and15realSQLite tests, including5runtimeRED violations and gatedcancel/claim race. Independent native Release Rebuild0warnings/errors,39unit+181integration=220/220, zero skipped, source hashes stable. Evidence `local-data/remediation-2026-09-24/cancellation-state/independent-review/`. Accepted in isolated worktree; main merge pending A07. This is only local state protection; ERP cancel transport and cancellable-type authorization remain unimplemented/unapproved.
