# Queue-sequence allocation

Date: 2026-09-24.

## Scope and implementation

This bounded change fixes durable `queue_sequence` allocation for `StoreCommandAsync` and `AdmitCommandAsync`, including rejected admissions. It changes no migration, index, ordering query, head-of-line policy, claim policy, retry policy, or existing test.

`SqliteAgentStore` no longer caches a process-local counter. Each new admission reads `COALESCE(MAX(queue_sequence),0)+1` from `commands_inbox` in the same existing serialized SQLite transaction that inserts the command or terminal rejection row. Allocation and insert therefore commit or roll back together, and independent store instances cannot allocate from stale per-instance state. The obsolete fields and counter helper were removed.

The invariant is uniqueness and stable relative order among retained rows. Existing rows are never renumbered. Values are not a permanent global history: numbers of removed rows may be reused while lower-numbered rows remain; after cleanup removes every row, allocation starts again at `1`. The change prevents new collisions but does not renumber or otherwise repair duplicates already persisted by an earlier build.

## Regression coverage

`tests/ErpOnecAgent.IntegrationTests/QueueSequenceAllocationTests.cs` adds five real-SQLite cases using `SqliteTestDatabase` scoped connection pools. Every factory is owned by the fixture, and no global pool clearing is used. The cases cover:

- deterministic A/B/A admission with equal receive timestamps and one ordering key, including result completion, ERP acknowledgement, one ready successor, and mutually exclusive successor claims;
- mixed `StoreCommandAsync` and valid/rejected `AdmitCommandAsync` entries;
- a failed insert transaction followed by a successful insert, proving the failed row is absent, retained rows remain unchanged, and successor ordering is preserved;
- allocation from new store instances without renumbering retained rows;
- eight independent stores released through a no-sleep start gate, proving concurrent retained values are unique.

## Evidence

All commands used the native Windows SDK at `D:/WORK/CNC_Milling/WORK_CNC/SOFT/1C/1C-agent/repo_1c-agent/.dotnet/dotnet.exe` from this isolated worktree.

- Runtime RED before the production edit: 5 total, 2 passed, 3 failed, 0 skipped. The A/B/A case persisted `1,2,2`; after the first result and ERP ACK, both equal-sequence successors were ready and both claims succeeded. The rollback case persisted `1,3`, and the mixed Store/Admit case persisted `1,2,2,3`. Console: `local-data/remediation-2026-09-24/queue-sequence/red/test.log`. TRX: `local-data/remediation-2026-09-24/queue-sequence/red/TestResults/queue-sequence-red_net10.0_20260924174054.trx`.
- Targeted GREEN: 5 passed, 0 failed, 0 skipped. Console: `local-data/remediation-2026-09-24/queue-sequence/targeted/test.log`. TRX: `local-data/remediation-2026-09-24/queue-sequence/targeted/TestResults/queue-sequence-targeted_net10.0_20260924174137.trx`.
- Native Release rebuild: 0 warnings, 0 errors. Log: `local-data/remediation-2026-09-24/queue-sequence/final/rebuild.log`.
- Full unit suite: 39 passed, 0 failed, 0 skipped. Console: `local-data/remediation-2026-09-24/queue-sequence/final/unit-test.log`. TRX: `local-data/remediation-2026-09-24/queue-sequence/final/TestResults/queue-sequence-final-unit_net10.0_20260924174224.trx`.
- Full integration suite: 144 passed, 0 failed, 0 skipped. Console: `local-data/remediation-2026-09-24/queue-sequence/final/integration-test.log`. TRX: `local-data/remediation-2026-09-24/queue-sequence/final/TestResults/queue-sequence-final-integration_net10.0_20260924174205.trx`.
- Final count: 183 passed / 0 failed / 0 skipped (39 unit + 144 integration), exactly five cases above the checkpoint-178 baseline.

Migrations were not edited. SHA-256 evidence is recorded in `local-data/remediation-2026-09-24/queue-sequence/final/migration-sha256.log`:

- `001_initial.sql`: `6bb8ec130ca7fdd303c08bef28c117452613d768ba9a995d627f9d4e31f7959a`
- `002_retry_budgets.sql`: `eb4d22e6a0b747b89fc09b8b9b2857c3ad893f1f15b9010ba38cdf6447ca3590`
- `003_ordering_claims.sql`: `0bf8e5122b6d094c6e65e0781a72b967fce361130bebe63f9d9ca47f3aac44cb`

## Limitations

- Production DI supplies a singleton `SqliteAgentStore`; the multi-instance tests intentionally go beyond that wiring. They do not prove multiprocess deployment safety.
- Admission is serialized with its allocation and insert, but this slice does not establish safe concurrent use of `RecoverAsync` with admission or claim operations.
- No ERP, 1C, credentials, certificates, external database, or network service was used.
- SQLite `MAX(queue_sequence)+1` is intentionally not backed by a new unique index or migration, and pre-existing duplicate values are not repaired.

## Changed files

- `src/ErpOnecAgent.Infrastructure/Persistence/Sqlite/SqliteAgentStore.cs`
- `src/ErpOnecAgent.Infrastructure/Persistence/Sqlite/SqliteAgentStore.Commands.cs`
- `tests/ErpOnecAgent.IntegrationTests/QueueSequenceAllocationTests.cs`
- `docs/remediation/queue-sequence-allocation.md`

## Reviewer clarification

Two RED cases demonstrate duplicate retained sequences and simultaneous same-key claims. The third original RED asserted gapless allocation after rollback; gaplessness is not a required invariant. Reviewer changed that test to verify no failed row persists, existing sequence remains unchanged, successor is strictly later, and ordering remains enforced. Removed-row numbers may be reused even when lower-numbered rows remain; only uniqueness and order of retained rows are promised. Independent validation below covers this final revision.

Independent final native Release Rebuild 0 warnings/errors, 39 unit +144 integration =183/183, zero skipped. Evidence `local-data/remediation-2026-09-24/queue-sequence/independent-review/`. Accepted and merged into main after administrative review; combined independent205/205.
