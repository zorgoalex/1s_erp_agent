# Administrative execution claim

Date: 2026-09-24.

## Scope and result

Administrative command routing now runs only after the same durable per-pass execution claim has been acquired and while that claim is held. The live claim state is refreshed before routing. Already-sent normal commands still resolve through status lookup before retry, while an uncertain or sent known administrative command is quarantined locally without callback replay or 1C access. Never-sent expired commands still expire before any administrative callback.

The callback API is now `Func<CommandEnvelope, string, CancellationToken, Task<bool>>`; the explicit owner is used by the worker for both successful and guarded error completion. There is no ownerless administrative callback bypass.

## Changed production paths

- `src/ErpOnecAgent.Application/Commands/AdministrativeCommandRouting.cs:3` is the single list of the worker's existing local administrative command types.
- `src/ErpOnecAgent.Application/Commands/CommandExecutionService.cs:75` acquires the claim before the callback, refreshes `StoredCommand` from `ExecutionClaim`, quarantines uncertain administrative rows before generic sent-state resolution, checks never-sent expiry, and invokes the callback under `try/finally`.
- `src/ErpOnecAgent.Application/Commands/CommandExecutionService.cs:150` uses the fixed-message `SaveAdministrativeUnknownResultAsync` helper for both callback exceptions and persisted uncertainty. It writes owner-aware `dead_letter` with `ADMINISTRATIVE_EXECUTION_UNKNOWN`, `details.outcomeUnknown=true`, and a manual-investigation explanation; it never writes `Exception.Message` to the outbound result.
- `src/ErpOnecAgent.Service/Workers/CommandExecutionWorker.cs:66` accepts the explicit owner, uses owner-aware administrative completion, and uses the centralized administrative-type guard in the process-wide catch. The business-failure log remains conditional on an applied completion.
- Existing administrative side effects, successful result payloads, ACK/replay guards, and normal command A02 lookup behavior were not broadened.

## Regression coverage

`tests/ErpOnecAgent.IntegrationTests/AdministrativeClaimTests.cs` uses real migrated SQLite and fake 1C clients. It covers:

- stale terminal snapshots;
- future retry backoff and `not_before` schedules;
- same-ordering-key predecessor blocking;
- concurrent gated administrative passes with exactly one side effect;
- owner-aware success and result outbox persistence with zero POST/lookup calls;
- never-sent administrative expiry with no side effect;
- thrown and cancelled callbacks releasing only their pass claim;
- accepted callbacks not falling through to 1C when guarded completion is refused;
- a failed administrative callback not being routed to 1C on a subsequent pass;
- persisted `unknown_result` rows for both `pause_etl` and `start_full_sync` being quarantined without lookup/POST;
- stale queued snapshots whose live row is unknown, and stale snapshots with stronger sent evidence, being quarantined conservatively;
- a fault-injected failure of administrative result persistence followed by the simulated worker `MarkUnknownResultAsync` fallback, with no subsequent 1C access;
- fixed administrative-unknown result text that contains manual-investigation guidance but not the callback exception message.

The existing normal unknown-result and expiry regressions remain in the full suite.

## Initial-slice evidence

All paths below are under `local-data/remediation-2026-09-24/state-guards/administrative-claim/`.

- Runtime RED before production edits: 10 tests executed, 1 passed, 9 failed, 0 skipped. The failures reproduced pre-claim administrative calls, two administrative side effects under concurrent passes, null callback owners, and missing claim observation. Console: `administrative-claim-runtime-red-20260924T201000Z.log`. TRX: `TestResults/administrative-claim-runtime-red-20260924T201000Z_net10.0_20260924174043.trx`.
- Targeted GREEN: 11/11 passed. Console: `administrative-claim-targeted-final-20260924T214500Z.log`. TRX: `TestResults/administrative-claim-targeted-final-20260924T214500Z_net10.0_20260924174714.trx`.
- Native Windows Release Rebuild: 0 warnings, 0 errors. Console: `administrative-claim-rebuild-final-20260924T214000Z.log`.
- Full native Windows Release tests: 39/39 unit and 150/150 integration, 189/189 total, 0 failed. Console: `administrative-claim-full-final-20260924T215000Z.log`. TRX: `TestResults/administrative-claim-full-final-20260924T215000Z_net10.0_20260924174724.trx` and `TestResults/administrative-claim-full-final-20260924T215000Z_net10.0_20260924174744.trx`.
- Touched-file formatting verification passed with no output: `administrative-claim-format-verify-touched-20260924T215500Z.log`.
- Solution-wide formatting verification still reports only pre-existing findings in `src/ErpOnecAgent.Infrastructure/ErpApi/ErpClientRegistration.cs` and `tests/ErpOnecAgent.UnitTests/ErpLongPollResilienceTests.cs`: `administrative-claim-format-verify-solution-20260924T213000Z.log`.

The original slice reached 150 integration tests; the follow-up uncertainty regressions bring the final integration count to 155 from the 139-test baseline, with the unit baseline unchanged at 39.

## Follow-up dispatch-safety evidence

- Runtime RED before the follow-up production fix: 16 tests executed, 10 passed, 6 failed, 0 skipped. The failures showed persisted unknown administrative rows reaching status lookup/POST, stale evidence reaching 1C, wrapper fallback reaching 1C, and the raw callback exception message in the outbound result. Console: `administrative-unknown-runtime-red-final-20260924T225000Z.log`. TRX: `TestResults/administrative-unknown-runtime-red-final-20260924T225000Z_net10.0_20260924175226.trx`.
- Follow-up targeted GREEN: 16/16 passed. Console: `administrative-unknown-targeted-20260924T231000Z.log`. TRX: `TestResults/administrative-unknown-targeted-20260924T231000Z_net10.0_20260924175301.trx`.
- Follow-up native Windows Release Rebuild: 0 warnings, 0 errors. Console: `administrative-unknown-rebuild-final-20260924T233000Z.log`.
- Follow-up full native Windows Release tests: 39/39 unit and 155/155 integration, 194/194 total, 0 failed. Console: `administrative-unknown-full-final-20260924T234000Z.log`. TRX: `TestResults/administrative-unknown-full-final-20260924T234000Z_net10.0_20260924175338.trx` and `TestResults/administrative-unknown-full-final-20260924T234000Z_net10.0_20260924175357.trx`.
- Follow-up touched-file formatting verification passed with no output: `administrative-unknown-format-verify-touched-20260924T235000Z.log`.

The follow-up adds five integration test cases to the administrative class; the final integration count is 155 from the 139-test baseline.

## Remaining constraints

- This does not provide exactly-once administrative effects across a process crash. The durable claim fences concurrent/live passes, but an in-memory ETL trigger or mode change can still have the existing A03 crash gap.
- Cancellation releases the claim through the existing cancellation path and does not create a new cancel protocol. A later pass re-enters the administrative callback; no 1C retry is caused by the cancellation wrapper. A larger design is needed for durable reconciliation of an administrative side effect whose outcome is unknown at cancellation.
- If owner-aware failure persistence is refused after owner loss, the unclaimed-only worker fallback cannot modify a different live owner. Failure of all persistence, and crash/cancellation around administrative effects, still require durable reconciliation in later work.
- Durable ETL jobs, mode persistence, payload-conflict changes, external APIs, migrations, deployment, and real ERP/1C integration remain outside this bounded slice. A07 pause persistence remains open.
- No service-project test reference was added; worker wiring is covered by the production Release build and the public Application integration tests.

## Final orchestrator correction

Removed the proposed early administrative return in the worker exception wrapper. When persisting ADMINISTRATIVE_EXECUTION_UNKNOWN itself throws, the existing unclaimed-only MarkUnknownResult fallback must still run; otherwise the queued row can immediately execute its administrative effect again. A successfully persisted terminal result rejects that fallback as a no-op, and a different live owner is protected by the store predicate. The existing simulated-wrapper regression now matches production control flow; a subsequent claimed administrative unknown row is quarantined without 1C or callback replay. Failure of all durable writes, process crash, and cancellation across a side effect still require the later durable administrative/ETL work; no exactly-once claim is made.

Independent merged acceptance: native Release Rebuild0warnings/errors,39unit+166integration=205/205, sources stable during run. Evidence `local-data/remediation-2026-09-24/state-guards/combined-admin-order-retention/`. Includes16administrative,5queueallocation,6retention regressions above checkpoint178.
