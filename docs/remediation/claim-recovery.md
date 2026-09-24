# Startup recovery of abandoned command claims

Date: 2026-09-24.

## Scope and defect

`SqliteAgentStore.RecoverAsync` cleared the durable per-pass claim only while converting `executing` rows to `unknown_result`. A process could die after claiming a due `queued`, `retry_waiting`, or `unknown_result` row, including after the lookup claim had persisted future backoff. Because production disables time takeover, the stale owner survived every restart and the row could not be claimed again.

The bounded production change is confined to `SqliteAgentStore.RecoverAsync`. After the unchanged `executing` conversion, recovery now clears both claim fields and increments `row_version` only for claimed `queued`, `retry_waiting`, and `unknown_result` rows. It does not change their status, send evidence, counters, attempts, payload/result, errors, `not_before_utc`, or `next_attempt_at_utc`. Unclaimed rows are not updated, so a second recovery is a no-op. Terminal commands are excluded, and existing result/ETL outbox recovery is unchanged.

A claimed never-sent `queued` row remains `queued` with no attempt, no `first_sent_at_utc`, and zero send/lookup counters. A claim alone therefore does not route it through potentially-sent lookup semantics or bypass A02 expiry for never-sent commands. Existing `executing` rows still become `unknown_result` because their durable POST evidence means the send outcome may be unknown.

## Regression coverage

New file: `tests/ErpOnecAgent.IntegrationTests/CommandClaimRecoveryTests.cs`. Every scenario uses a real migrated SQLite database and the public store acquisition, POST/lookup claim, scheduling, completion, acknowledgment, and recovery APIs. No network client, sleep, or external endpoint is used.

| Test | Verified behavior |
|---|---|
| `Queued_claim_crash_before_send_recovers_without_creating_send_evidence` | Exact command/attempt/outbox snapshot changes only claim fields and `row_version`; expired never-sent semantics remain intact; a due row reacquires with `DateTimeOffset.MinValue` as the no-time-takeover boundary. |
| `Retry_waiting_claim_releases_and_preserves_prior_send_and_schedule` | Prior POST evidence, completed attempt, error, counters, first-send timestamp, and due retry schedule remain exact; the recovered row reacquires without time takeover. |
| `Unknown_result_lookup_claim_releases_without_bypassing_future_backoff` | Public lookup claim leaves its open lookup attempt, incremented lookup budget, prior POST evidence, and future backoff exact. Acquisition now is rejected and succeeds at the persisted future timestamp, without waiting. |
| `Executing_claim_recovers_to_unknown_result_and_preserves_first_send_evidence` | Existing conversion to `unknown_result` remains; first-send timestamp, start time, POST evidence, attempt, counters, payload, and absence of a result are preserved; the due row reacquires. |
| `Terminal_commands_results_and_outboxes_remain_exactly_unchanged` | `result_pending`/pending and `completed`/acknowledged command, result, outbox, and attempt snapshots are field-for-field equal across recovery. |

Each test also invokes `RecoverAsync` a second time and compares complete command, attempt, and outbox snapshots, proving no duplicate attempt, counter, result, schedule, or row-version mutation. Red and green focused runs used the same five tests against the same production path.

## Runtime evidence

All commands used the native Windows SDK from the main repository through PowerShell scripts whose working directory was this isolated worktree.

| Run | Result | Evidence |
|---|---:|---|
| Original-production RED, focused | 5 total: 3 failed, 2 passed | `local-data/remediation-2026-09-24/claim-recovery/red-test.log`; `red/TestResults/claim-recovery-red_net10.0_20260924162342.trx` |
| Fixed focused GREEN | 5 passed, 0 failed | `local-data/remediation-2026-09-24/claim-recovery/green-test.log`; `green/TestResults/claim-recovery-green_net10.0_20260924162410.trx` |
| Full Release `-t:Rebuild --no-restore` | 0 warnings, 0 errors | `local-data/remediation-2026-09-24/claim-recovery/rebuild.log` |
| Full unit tests, `--no-build --no-restore` | 39 passed, 0 failed | `unit-test.log`; `final/TestResults/claim-recovery-final-unit_net10.0_20260924162543.trx` |
| Full integration tests, `--no-build --no-restore` | 87 passed, 0 failed | `integration-test.log`; `final/TestResults/claim-recovery-final-integration_net10.0_20260924162550.trx` |

Final count: **126 passed / 0 failed** (39 unit + 87 integration), five tests above the 39-unit/82-integration checkpoint baseline.

The first final integration run found a test-only assumption that snapshot array order matched insertion order; the test was changed to select rows by command ID. The authoritative final integration TRX above is the subsequent clean 87/87 run.

## Migration integrity

No schema, API, migration, or queue-sequence change was made. Read-only SHA-256 comparison with the main-repository baseline produced identical pairs:

- `001_initial.sql`: `6bb8ec130ca7fdd303c08bef28c117452613d768ba9a995d627f9d4e31f7959a`
- `002_retry_budgets.sql`: `eb4d22e6a0b747b89fc09b8b9b2857c3ad893f1f15b9010ba38cdf6447ca3590`
- `003_ordering_claims.sql`: `0bf8e5122b6d094c6e65e0781a72b967fce361130bebe63f9d9ca47f3aac44cb`

## Remaining limitations

- Recovery remains startup-only by design. No live age-based takeover, timer, or concurrent recovery of a running owner's claim was added.
- The tests model a crash at durable pre-network boundaries and construct a new store instance over the same database; they do not kill an operating-system process or contact a real 1C endpoint.
- Future `next_attempt_at_utc` and `not_before_utc` remain enforced by the existing atomic acquisition guard. Recovery releases ownership without making a row due.
- Other command mutators, owner conditioning, ordering/deduplication, administrative behavior, and outbox redesign remain outside this bounded change.
