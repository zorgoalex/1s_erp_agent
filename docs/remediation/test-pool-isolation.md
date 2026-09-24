# Fixture-scoped SQLite pool isolation

Date: 2026-09-24.

## Scope

This is a test-infrastructure-only correction. It does not change production behavior, `SqliteConnectionFactory` pooling or shared-cache settings, the Microsoft.Data.Sqlite version, migrations, test assertions, or xUnit parallelization. No production source file, package version, migration, test assertion, or assembly test setting was changed.

No `AGENTS.md` exists in `repo_1c-agent` or its `1C-agent` parent, so repository conventions were taken from `.editorconfig` and the existing test style.

## Failure evidence and mechanism

The supplied independent run remains unchanged at:

- `local-data/remediation-2026-09-24/state-guards/combined-scheduling-recovery/test.log`
- `local-data/remediation-2026-09-24/state-guards/combined-scheduling-recovery/TestResults/independent-combined_net10.0_20260924164143.trx`

It reported 96 passed and 1 failed integration tests. `CommandOrderingClaimFencingTests.Concurrent_lookup_passes_of_one_command_make_exactly_one_status_call` failed before reaching its test body while `SqliteMigrator.ExecuteAsync` prepared a migration statement. The native `SQLitePCL.sqlite3` safe handle had already been disposed. The same run passed all 39 unit tests. The TRX timestamps show multiple fixture classes active concurrently when the 1 ms fixture-initialization failure occurred.

The integration fixtures used unique temporary database roots, but nine of them called static `SqliteConnection.ClearAllPools()`. That API empties every Microsoft.Data.Sqlite pool in the test process, not pools below the calling fixture's root. Because the production factory retains `Pooling = true` and `Cache = Shared`, a teardown or simulated restart in one xUnit class could therefore cross the process-wide pool registry while another class was using a different database. The supplied disposed-handle failure and its overlap are consistent with that scope escape.

The defect was not deterministically reproduced after the supplied failure. The evidence used is the preserved runtime failure, the process-wide call sites, and the parallel TRX schedule; the fix was not based on rerunning until green.

## Fix

`tests/ErpOnecAgent.IntegrationTests/SqliteTestDatabase.cs` now owns one unique fixture root and records every `SqliteConnectionFactory` created below it. Its targeted cleanup opens a connection from the actual factory, calls `SqliteConnection.ClearPool(connection)`, and disposes that helper connection. This uses the factory's actual matching connection string and clears only the associated pool.

Fixture disposal clears every pool registered by that fixture before recursively deleting its temporary directory. This includes the additional `legacy.db` factory created by `CommandExecutionTests`. Simulated restart points in the migration, ordering, execution, and claim-recovery fixtures use the same targeted operation. Each xUnit test instance still has its own unique root, and parallel execution and the existing gated concurrency regressions remain unchanged.

The nine fixture files changed are:

- `tests/ErpOnecAgent.IntegrationTests/SqliteStoreTests.cs`
- `tests/ErpOnecAgent.IntegrationTests/SchedulingOwnerFencingTests.cs`
- `tests/ErpOnecAgent.IntegrationTests/RetryBudgetMigrationTests.cs`
- `tests/ErpOnecAgent.IntegrationTests/OrderingClaimsMigrationTests.cs`
- `tests/ErpOnecAgent.IntegrationTests/CommandOrderingClaimGuardTests.cs`
- `tests/ErpOnecAgent.IntegrationTests/CommandOrderingClaimFencingTests.cs`
- `tests/ErpOnecAgent.IntegrationTests/CommandIntakeTests.cs`
- `tests/ErpOnecAgent.IntegrationTests/CommandExecutionTests.cs`
- `tests/ErpOnecAgent.IntegrationTests/CommandClaimRecoveryTests.cs`

The shared helper is `tests/ErpOnecAgent.IntegrationTests/SqliteTestDatabase.cs`. A search of `tests/` found no remaining `ClearAllPools` call.

## Native Windows validation

All validation used the repository's native Windows `.dotnet/dotnet.exe` through `powershell.exe`. Evidence is under `local-data/remediation-2026-09-24/test-pool-isolation/`.

| Validation | Result | Evidence |
|---|---:|---|
| All nine affected fixture classes | 97 passed, 0 failed, 0 skipped | `targeted-fixtures_20260924T164533006.log`; `TestResults/targeted-fixtures_net10.0_20260924T164533006.trx` |
| Consecutive integration suite 1 | 97 passed, 0 failed, 0 skipped | `integration-suite-1_20260924T164549718.log`; matching TRX |
| Consecutive integration suite 2 | 97 passed, 0 failed, 0 skipped | `integration-suite-2_20260924T164557005.log`; matching TRX |
| Consecutive integration suite 3 | 97 passed, 0 failed, 0 skipped | `integration-suite-3_20260924T164604113.log`; matching TRX |
| Full solution Release rebuild | 0 warnings, 0 errors | `release-rebuild_20260924T164621819.log` |
| Full solution tests | 39 unit + 97 integration = 136 passed, 0 failed, 0 skipped | `full-solution_20260924T164627474.log`; the two uniquely prefixed `full-solution_20260924T164627474_*.trx` files |

The first targeted invocation stopped before executing tests because the analyzer required the new instance-independent helper method to be static (`CA1822`). It was corrected to static. That compile-only attempt is retained as `targeted-fixtures_20260924T164519985.log` and is not counted as runtime evidence. A no-change `dotnet format --verify-no-changes` check limited to the ten touched C# files then exited successfully.

After the correction, the targeted run, three consecutive full-integration runs, and the final full-solution integration run amount to five fresh 97-test integration processes: 485 integration test executions, all passed. The final solution run adds 39 unit executions: 524 post-fix test executions in the authoritative runtime evidence, all passed. The combined expected count is 136 tests, not the older 131 count recorded in the scheduling-owner document.

## Statistical limits

- Five clean 97-test integration executions after the fix provide useful repeat evidence but cannot prove an intermittent parallel race is impossible.
- The work validates the current local Windows/.NET 10 process behavior. It does not establish behavior on other operating systems, provider builds, production databases, or external ERP/1C systems.
- The fix was not tested with an operating-system process kill, forced stress loop, or production deployment. The concurrency tests remain genuinely concurrent and xUnit class parallelization remains enabled.
- Successful targeted cleanup demonstrates that fixture-owned handles permit temporary-directory deletion in these runs; it is not a formal leak detector.

## Independent reviewer validation

The orchestrator independently rebuilt the full merged solution (0 warnings/errors) and passed 39 unit + 97 integration =136/136 with unchanged source hashes during execution. Evidence: `local-data/remediation-2026-09-24/test-pool-isolation/independent-review/`. The exact low-level trigger remains an inference; the preserved failure is not claimed as a deterministic provider-bug reproduction.
