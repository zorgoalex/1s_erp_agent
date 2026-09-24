# Result-delivery state guards

Date: 2026-09-24.

## Scope

This is the bounded stage-1 result-delivery transition slice. It changes only the store contract and implementation for `AcknowledgeResultAsync` and `MarkResultRetryAsync`, the result-delivery worker logging around those calls, and focused regression coverage. It does not change the ERP/1C wire contract, network claims, retry policy, schema, migrations, payload-conflict handling, administration, cancellation, or ETL behavior.

## Transition semantics

`AcknowledgeResultAsync` now returns `Task<bool>`. It applies only when both durable sides match:

- `results_outbox.status` is `pending`, `retry_waiting`, or `sending`;
- `commands_inbox.status` is `result_pending`, or `completed` for the already-accepted explicit duplicate-replay path;
- the command has a persisted local result (`result_status` and `result_json`).

The guarded command update and outbox update commit in one SQLite transaction. A missing command, active `queued`/`executing` command, missing result, missing outbox, non-deliverable outbox, or already acknowledged outbox returns `false` without mutation. A first ACK and an explicit replay ACK return `true`; the latter refreshes the command and outbox confirmation timestamps. A repeated ACK of an acknowledged outbox is a complete no-op, including command row version, outbox timestamps, and attempt audit.

`MarkResultRetryAsync` now returns `Task<bool>`. It updates only an unacknowledged `pending`/`retry_waiting`/`sending` outbox whose command is locally resolved. It increments only the outbox attempt count and stores the requested error/backoff. It does not reopen an acknowledged outbox, mutate the command, result payload/hash/result ID, or execution audit/state.

`ResultDeliveryWorker` keeps the existing outgoing result PUT and retry-delay calculation. Positive acknowledgment and retry warning logs are emitted only when the corresponding store transition returns `true`; a no-op emits no positive confirmation.

## Regression coverage

`tests/ErpOnecAgent.IntegrationTests/ResultDeliveryGuardsTests.cs` uses a real migrated SQLite database through `SqliteTestDatabase` and public store APIs. It contains 14 test cases covering:

- first ACK, including a `sending` outbox;
- queued and executing commands without a result;
- a deliverable outbox attached to an active command;
- duplicate ACK timestamp/row-version/audit immutability;
- explicit duplicate replay ACK timestamp refresh;
- late retry after ACK;
- retry count, error, backoff, identity, and command/audit preservation;
- active-command retry rejection;
- nonexistent IDs; and
- SQLite-trigger failure injection proving the ACK command/outbox pair rolls back atomically.

The existing `CommandExecutionTests.Completed_row_is_never_reopened_by_lookup_claim` setup was strengthened to create a valid local result/outbox before ACK. Its A01/A02 lookup-immutability assertions were not broadened.

## Evidence

Baseline checkpoint-164: 39 unit + 125 integration = 164.

- RED before the production change: 14 total, 5 passed, 9 failed. Evidence: `local-data/remediation-2026-09-24/state-guards/result-delivery/red/result-delivery-red-20260924T122851Z.log`.
- Targeted GREEN: `ResultDeliveryGuardsTests` 14/14, 0 failed, 0 skipped. TRX: `local-data/remediation-2026-09-24/state-guards/result-delivery/targeted/TestResults/result-delivery-targeted-final-20260924T123003Z_net10.0_20260924173123.trx`; log: `local-data/remediation-2026-09-24/state-guards/result-delivery/targeted/targeted-final-20260924T123003Z.log`.
- Native Windows Release rebuild: `./.dotnet/dotnet.exe build ErpOnecAgent.sln -c Release -t:Rebuild --no-restore` — 0 warnings, 0 errors. Log: `local-data/remediation-2026-09-24/state-guards/result-delivery/final/rebuild-20260924T123003Z.log`.
- Native Windows SDK/OS evidence: `local-data/remediation-2026-09-24/state-guards/result-delivery/final/dotnet-info-20260924T123003Z.txt`.
- Native Windows full unit tests: 39/39 passed, 0 failed, 0 skipped. TRX: `local-data/remediation-2026-09-24/state-guards/result-delivery/final/TestResults/result-delivery-unit-final-20260924T123003Z_net10.0_20260924173030.trx`; log: `local-data/remediation-2026-09-24/state-guards/result-delivery/final/unit-final-20260924T123003Z.log`.
- Native Windows full integration tests: 139/139 passed, 0 failed, 0 skipped. TRX: `local-data/remediation-2026-09-24/state-guards/result-delivery/final/TestResults/result-delivery-integration-final-20260924T123003Z_net10.0_20260924173040.trx`; log: `local-data/remediation-2026-09-24/state-guards/result-delivery/final/integration-final-20260924T123003Z.log`.

Final count: 178 passed / 0 failed / 0 skipped (39 unit + 139 integration), 14 cases above checkpoint-164.

Touched-file `dotnet format --verify-no-changes` passed; evidence: `local-data/remediation-2026-09-24/state-guards/result-delivery/final/format-verify-touched-20260924T123003Z.log`. A solution-wide format check still reports only pre-existing findings in `src/ErpOnecAgent.Infrastructure/ErpApi/ErpClientRegistration.cs` and `tests/ErpOnecAgent.UnitTests/ErpLongPollResilienceTests.cs`; it is recorded at `local-data/remediation-2026-09-24/state-guards/result-delivery/final/format-verify-solution-20260924T123003Z.log`.

No migration file was changed. SHA-256 evidence: `local-data/remediation-2026-09-24/state-guards/result-delivery/final/migration-sha256-20260924T123003Z.log`:

- `001_initial.sql`: `6bb8ec130ca7fdd303c08bef28c117452613d768ba9a995d627f9d4e31f7959a`
- `002_retry_budgets.sql`: `eb4d22e6a0b747b89fc09b8b9b2857c3ad893f1f15b9010ba38cdf6447ca3590`
- `003_ordering_claims.sql`: `0bf8e5122b6d094c6e65e0781a72b967fce361130bebe63f9d9ca47f3aac44cb`

## Changed files

- `src/ErpOnecAgent.Application/Abstractions/Persistence.cs`
- `src/ErpOnecAgent.Infrastructure/Persistence/Sqlite/SqliteAgentStore.Commands.cs`
- `src/ErpOnecAgent.Service/Workers/ResultDeliveryWorker.cs`
- `tests/ErpOnecAgent.IntegrationTests/ResultDeliveryGuardsTests.cs`
- `tests/ErpOnecAgent.IntegrationTests/CommandExecutionTests.cs`
- `docs/remediation/result-delivery-guards.md`

## Remaining constraints

- All checks are local; no real ERP, 1C, credentials, certificates, or network result endpoint was used.
- The outgoing ERP result PUT and existing retry delay remain unchanged; no new network claim or generation was added.
- Administrative/cancel, ETL, payload-conflict, and broader state-machine fencing remain outside this slice.
- External ERP/1C delivery semantics and contract acceptance remain open.
- The solution-wide formatting findings listed above are pre-existing and were not broadened.

## Independent review

Orchestrator reviewed production SQL, worker wiring, fixture adaptation, and actual initial console output. Independent native Release Rebuild: 0 warnings/errors; full run 39 unit + 139 integration = 178/178, no skips. Evidence: `local-data/remediation-2026-09-24/state-guards/result-delivery/independent-review/`.

RED qualification: the original 14-case run had 9 failures, but the atomicity failure was a test snapshot array reference-equality bug, not a demonstrated production atomicity defect. Eight other cases show actual forbidden mutations in the raw console. `original-red-console.log` preserves primary output; the earlier red summary file is not raw tool output. Final corrected atomicity regression passes and protects the updated transaction order. No claim that all nine initial failures were application defects.
