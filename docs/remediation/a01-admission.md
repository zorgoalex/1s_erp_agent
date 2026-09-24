# A01 — Atomic invalid-command admission and supported payload versions

Scope: `repo_1c-agent` code/tests only. No contract/plan/ETL/mode changes.

## Problem

`CommandLeaseWorker` validated a leased command but ignored the result until after
`StoreCommandAsync` queued the row and the remote ACK completed. Invalid hash / ID /
payload therefore entered `commands_inbox` as `queued` (executable). `CommandExecutionWorker`
could pick the row up and call 1C. Post-ACK `CompleteLocallyAsync(DeadLetter)` was the only
rejection, so a delayed or failing ACK left an executable invalid row. `CommandValidator`
accepted any positive `payloadVersion` (only v1 is supported) and threw on missing/null
payload or null hash instead of rejecting.

## Changes

| Area | File | Change |
|---|---|---|
| Validator | `src/ErpOnecAgent.Application/Commands/CommandValidator.cs` | Allowlist `payloadVersion == 1` (`UNSUPPORTED_PAYLOAD_VERSION` otherwise). Reject `Undefined`/`Null` payload (`INVALID_PAYLOAD`). Reject null/empty `payloadHash` (`PAYLOAD_HASH_MISMATCH`) before hashing. No throw paths. |
| Store API | `src/ErpOnecAgent.Application/Abstractions/Persistence.cs`, `Domain/Commands/CommandModels.cs` | New `IAgentStore.AdmitCommandAsync(command, receivedAtUtc, validation, ct)` and `StoreCommandOutcome.Rejected`. Original `StoreCommandAsync` signature preserved. |
| Store impl | `src/ErpOnecAgent.Infrastructure/Persistence/Sqlite/SqliteAgentStore.Commands.cs` | `AdmitCommandAsync` is a single SQLite transaction: duplicate/conflict check → if `!validation.IsValid` insert `result_pending`/`result_status=dead_letter` + `results_outbox` pending (`Rejected`) → else insert `queued` (`Stored`). Invalid commands never reach an executable status. Null-safe payload/hash persistence. |
| Intake | `src/ErpOnecAgent.Application/Commands/CommandIntakeService.cs` (new) | Deserialize → `CommandValidator.Validate` → `AdmitCommandAsync` (terminal before ACK) → `AcknowledgeReceivedAsync`. ACK errors returned in `AckError` after durable admission. |
| Worker | `src/ErpOnecAgent.Service/Workers/CommandLeaseWorker.cs` | Uses `CommandIntakeService` (validated admission). Logs `COMMAND_REJECTED`/`COMMAND_RECEIVED`/`COMMAND_DUPLICATE`/`COMMAND_PAYLOAD_CONFLICT`. Rethrows `AckError` to keep ERP backoff. Removed post-ACK `CompleteLocallyAsync` rejection path. |

## Tests (added first, red captured)

Unit (`tests/ErpOnecAgent.UnitTests/CommandTests.cs`):
- `Validator_rejects_unknown_positive_payload_version` (v2 → `UNSUPPORTED_PAYLOAD_VERSION`)
- `Validator_accepts_supported_payload_version_v1`
- `Validator_rejects_missing_payload_without_throwing`
- `Validator_rejects_null_payload_without_throwing`
- `Validator_rejects_null_hash_without_throwing`
- `Validator_rejects_empty_command_id_without_throwing`

Integration store (`tests/ErpOnecAgent.IntegrationTests/SqliteStoreTests.cs`):
- `Admit_stores_valid_command_as_executable_queued`
- `Admit_rejects_invalid_hash_atomically_with_terminal_outbox_and_no_executable_row`
- `Admit_rejects_unknown_payload_version_without_executable_row`
- `Admit_rejects_null_payload_without_executable_row`
- `Admit_of_invalid_command_is_idempotent_on_redelivery`

Worker-level intake (`tests/ErpOnecAgent.IntegrationTests/CommandIntakeTests.cs`, new):
- `Failing_ACK_after_invalid_hash_admission_leaves_no_executable_row_and_zero_onec_calls`
- `Delayed_ACK_does_not_expose_executable_invalid_row_before_ack_completes`
- `Failing_ACK_after_unknown_payload_version_admission_leaves_no_executable_row`
- `Failing_ACK_after_null_payload_admission_leaves_no_executable_row`
- `Valid_command_admission_still_queues_before_ack`

Worker-level tests use a testable application service (`CommandIntakeService`) instead of a
Service project reference (Service targets `win-x64` self-contained).

## Evidence

| Item | Path |
|---|---|
| Red run (12 failed: 4 unit + 8 integration) | `local-data/remediation-2026-09-24/a01/red-test.log` |
| Red TRX | `local-data/remediation-2026-09-24/a01/TestResults/dmina_DB_2026-09-24_00_46_01_net10.0.trx`, `...00_46_53_net10.0[1].trx` |
| Green run | `local-data/remediation-2026-09-24/a01/green-test.log`, `final-test.log` |
| Green TRX | `local-data/remediation-2026-09-24/a01/TestResults/dmina_DB_2026-09-24_00_47_45_net10.0.trx`, `...00_47_45_net10.0[1].trx` |
| Release build (0 warn / 0 err) | `local-data/remediation-2026-09-24/a01/build.log` |

Red evidence of the A01 race (before fix):
- `Delayed_ACK_...` → `Assert.Empty() Failure: Collection was not empty` (executable invalid row while ACK in flight)
- `Failing_ACK_after_invalid_hash_...` → outcome mismatch + non-empty ready queue (invalid row executable after ACK failure)

## Test counts

| Run | Unit | Integration | Total |
|---|---|---|---|
| Baseline | 12 passed | 5 passed | 17 passed |
| Red (tests first) | 4 failed / 14 passed / 18 | 8 failed / 7 passed / 15 | 12 failed / 21 passed / 33 |
| Final Release | 18 passed | 15 passed | **33 passed / 0 failed** |

Delta vs baseline: +16 tests (6 unit validator, 5 store admit, 5 worker intake).

## Limitations

- Supported payload version allowlist is the constant `1`; adding versions requires code change (by design).
- Deserialization failures (malformed JSON, non-GUID `commandId`) still throw before admission; no terminal row is written because there is no reliable `commandId`. They never create executable state.
- `StoreCommandAsync` (original overload) remains unvalidated for existing callers/tests; the worker uses `AdmitCommandAsync` only.
- `AdmitCommandAsync` trusts the supplied `ValidationResult` (caller validates via `CommandValidator`); it is not re-validated inside SQLite.
- Duplicate redelivery of a rejected command returns `Duplicate` and keeps the original terminal outbox row (no second result).
- No live ERP/1C was exercised; intake tests use fakes. No schema migration was added (existing `commands_inbox`/`results_outbox` columns suffice).
