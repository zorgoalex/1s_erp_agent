# Dead-letter retention

Date: 2026-09-24.

## Requirement and defect

TZ section 15.5 requires a dead letter to remain in SQLite until manual closure. A local command outcome is persisted as `result_status='dead_letter'`; ERP result acknowledgement changes `status` to `completed` but preserves `result_status`. Before this change, `CleanupAsync` selected every old acknowledged `completed` command regardless of `result_status`, then deleted its acknowledged outbox row and attempts. The operational dead-letter metric consequently fell to zero after retention cleanup even though the dead letter had not been manually closed.

## Bounded implementation

Production changes are limited to the three command-related deletions in `SqliteAgentStore.CleanupAsync`:

- attempts are eligible only when the parent command's `result_status` is not `dead_letter`;
- an acknowledged outbox row is eligible only when its parent command's `result_status` is not `dead_letter`;
- a command is eligible only when its `result_status` is not `dead_letter`.

The existing ERP-ACK cutoff and the guard that retains a completed command while any outbox row is not acknowledged remain unchanged. Pending, retry-waiting, and sending replay states therefore retain command and attempt history as before. Ordinary acknowledged success, business failure, cancellation, and expiry rows remain eligible for the configured cutoff. No ETL path, worker, migration, index, external contract, or accepted queue-allocation behavior was changed.

Because no supported manual-closure API or durable closure state exists, the conservative policy is to retain every command whose `result_status='dead_letter'`, including an acknowledged outbox row and all execution attempts. This can grow SQLite storage indefinitely. Operators have no supported product action to close such a command; direct production-database mutation is not an operational procedure. Designing and accepting a manual-close path remains a separate slice. This bounded fix does not claim stage 1 complete.

## Runtime coverage

`tests/ErpOnecAgent.IntegrationTests/DeadLetterRetentionTests.cs` adds six real-SQLite cases using the fixture-owned `SqliteTestDatabase` connection pool. The suite covers:

- locally completed dead letter, old ERP result ACK, cleanup, exact command/outbox/attempt history retention, and unchanged dead-letter metrics;
- rejected admission dead letter with old ERP result ACK and cleanup;
- locally completed dead letter without ERP result ACK;
- explicit duplicate replay followed by late cleanup;
- repeated cleanup, scoped-pool restart, recovery, and another cleanup;
- positive control proving ordinary acknowledged success is still purged.

Snapshots compare every persisted command and outbox field and compare each attempt record by deterministic attempt number; they do not use snapshot-array equality. Restart uses `SqliteTestDatabase.ClearPoolAsync` for the fixture's own connection string and does not call `SqliteConnection.ClearAllPools`.

## Evidence

All evidence was produced with the native Windows SDK at `D:/WORK/CNC_Milling/WORK_CNC/SOFT/1C/1C-agent/repo_1c-agent/.dotnet/dotnet.exe` from this isolated worktree.

- Runtime RED before the production edit: 6 total, 3 passed and 3 failed, 0 skipped. The acknowledged local dead letter, rejected admission dead letter, and repeated-cleanup/restart cases found no surviving command row. Console: `local-data/remediation-2026-09-24/dead-letter-retention/red/test.log`. TRX: `local-data/remediation-2026-09-24/dead-letter-retention/red/TestResults/dead-letter-red_net10.0_20260924174807.trx`.
- Targeted GREEN: 6 passed, 0 failed, 0 skipped. Console: `local-data/remediation-2026-09-24/dead-letter-retention/targeted/test.log`. TRX: `local-data/remediation-2026-09-24/dead-letter-retention/targeted/TestResults/dead-letter-targeted_net10.0_20260924174828.trx`.
- Touched C# formatting verification passed with no findings. Console: `local-data/remediation-2026-09-24/dead-letter-retention/final/format-verify-touched.log`. The solution-wide check reproduced only the pre-existing findings in `ErpClientRegistration.cs` and `ErpLongPollResilienceTests.cs`, which are outside this task's immutable scope. Console: `local-data/remediation-2026-09-24/dead-letter-retention/final/format-verify-solution.log`.
- Native Release Rebuild: 0 warnings, 0 errors. Console: `local-data/remediation-2026-09-24/dead-letter-retention/final/rebuild.log`.
- One full solution test run: 39/39 unit and 150/150 integration, 189 passed, 0 failed, 0 skipped. The six added integration cases raise the accepted 183-test checkpoint to 189. Console: `local-data/remediation-2026-09-24/dead-letter-retention/final/full-test.log`. TRX: `local-data/remediation-2026-09-24/dead-letter-retention/final/TestResults/dead-letter-final_net10.0_20260924174935.trx` (integration) and `dead-letter-final_net10.0_20260924174954.trx` (unit).

No live ERP, 1C, external database, network endpoint, deployment, credential, certificate, or migration was used.

## Changed files

- `src/ErpOnecAgent.Infrastructure/Persistence/Sqlite/SqliteAgentStore.cs`
- `tests/ErpOnecAgent.IntegrationTests/DeadLetterRetentionTests.cs`
- `docs/remediation/dead-letter-retention.md`

## Independent review

Orchestrator reviewed the three deletion predicates and all six real-SQLite regressions. Independent Release Rebuild:0 warnings/errors; full39unit+150integration=189/189, zero skipped. Evidence `local-data/remediation-2026-09-24/dead-letter-retention/independent-review/`. Accepted and merged into main with queue allocation and administrative correction; combined independent205/205.
