# A07a — Composed mode state and durable local ETL pause

Date: 2026-09-24. Scope: bounded A07a mode-source composition only. This report does not claim that all of A07 is complete.

## Result

The runtime no longer uses `AgentRuntimeState.Mode` as a mutable single source of truth. `AgentModeSnapshot` composes three independent inputs under one lock:

- local ETL pause intent, persisted in the existing `agent_state` table under `local_etl_paused`;
- accepted remote `AgentMode` from a validated dynamic configuration;
- handshake maintenance restriction.

A fresh state is not ready and exposes `Disabled` with all worker permissions denied. Readiness is published only after SQLite initialization, integrity checking, recovery, and restoration of both saved sources complete. Handshake maintenance updates only the handshake input. A non-maintenance handshake therefore cannot clear a remote pause or local pause. Configuration 304/null leaves the restored snapshot unchanged.

Remote mode parsing rejects unknown names and all numeric strings, including defined numeric values such as `0`. Other remote configuration validation and version rollback checks remain in `DynamicConfigurationState.Apply`; validation occurs before command types, entities, interval, or mode inputs are published.

`EffectiveMode` is display/heartbeat compatibility only. Its deterministic precedence is `Disabled > Maintenance > Drain > PauseCommands > PauseEtl > Normal`; local pause participates at the `PauseEtl` level. Worker permissions are not taken from this single displayed value and instead intersect every applicable restriction.

## Policy matrix

`Yes*` means the operation is also conditional on bootstrap readiness, no local ETL pause for extraction, and no handshake maintenance restriction.

| Remote mode | Lease new commands | Execute accepted commands | Extract | Deliver results | Upload batches / complete runs |
|---|---:|---:|---:|---:|---:|
| `Normal` | Yes | Yes | Yes* | Yes | Yes |
| `PauseEtl` | Yes | Yes | No | Yes | Yes |
| `PauseCommands` | No | No | Yes* | Yes | Yes |
| `Drain` | No | Yes | Yes* | Yes | Yes |
| `Maintenance` | No | No | No | Yes | Yes |
| `Disabled` | No | No | No | Yes | Yes |

Additional source rules:

- local pause blocks extraction only; it does not block leasing, command execution, result delivery, batch upload, or run completion;
- handshake maintenance blocks leasing, execution, and extraction, but does not block result delivery or batch upload/completion;
- local pause plus remote `PauseCommands` blocks both command paths and extraction; clearing only the local pause leaves the remote command restriction intact;
- `Drain` stops admission while allowing already accepted commands and result delivery.

## Persistence and administration

`LocalEtlPauseController` serializes asynchronous pause/resume operations. It writes the canonical `{"paused":true|false}` value through `IAgentStore.SetStateAsync` before changing the in-memory source and before the administrative success result is written. A persistence exception leaves the previous source and durable value unchanged. The existing owner-aware administrative unknown-result path and the worker’s existing unclaimed `MarkUnknownResultAsync` fallback remain intact.

A missing local state key restores `false`. Malformed JSON, a non-boolean value, an object with an invalid shape, or another invalid persisted representation fails bootstrap rather than silently selecting `Normal`.

## Startup sequence

`BootstrapService.StartAsync` now performs this sequence before it returns:

1. acquire the instance lock and perform existing certificate/secret checks;
2. initialize SQLite, run `IntegrityCheckAsync`, and run `RecoverAsync`;
3. quarantine temporary spool files;
4. restore and validate local pause state;
5. load the active remote snapshot, deserialize it, and apply its validated version/configuration;
6. set the runtime ready bit;
7. log `AGENT_STARTED` and allow the remaining hosted services to proceed.

All production worker loops have a readiness guard. The late active-snapshot restore was removed from `ConfigurationWorker`; its null/304 path is intentionally a no-op. No schema or migration was added.

## Changed files

Production:

- `src/ErpOnecAgent.Service/Runtime/AgentRuntimeState.cs`
- `src/ErpOnecAgent.Service/Runtime/DynamicConfigurationState.cs`
- `src/ErpOnecAgent.Service/Runtime/ErpSessionManager.cs`
- `src/ErpOnecAgent.Service/Runtime/LocalEtlPauseController.cs` (new)
- `src/ErpOnecAgent.Service/Runtime/BootstrapService.cs`
- `src/ErpOnecAgent.Service/Program.cs`
- `src/ErpOnecAgent.Service/Workers/ConfigurationWorker.cs`
- `src/ErpOnecAgent.Service/Workers/CommandLeaseWorker.cs`
- `src/ErpOnecAgent.Service/Workers/CommandExecutionWorker.cs`
- `src/ErpOnecAgent.Service/Workers/OnecEtlWorker.cs`
- `src/ErpOnecAgent.Service/Workers/ResultDeliveryWorker.cs`
- `src/ErpOnecAgent.Service/Workers/EtlBatchUploadWorker.cs`
- `src/ErpOnecAgent.Service/Workers/HeartbeatWorker.cs`
- `src/ErpOnecAgent.Service/Workers/HealthMonitorWorker.cs`
- `src/ErpOnecAgent.Service/Workers/MaintenanceWorker.cs`
- Historical audit probe restored unchanged by orchestrator; adapted copy retained only as `local-data/remediation-2026-09-24/a07-mode-state/review/adapted-historical-probe-Program.cs.txt`.

Tests and test graph:

- `tests/ErpOnecAgent.IntegrationTests/ErpOnecAgent.IntegrationTests.csproj` (direct Service project reference so tests exercise production worker/runtime classes);
- `tests/ErpOnecAgent.IntegrationTests/packages.lock.json` (dependency graph updated for that reference; no package versions were changed);
- `tests/ErpOnecAgent.IntegrationTests/A07ModeStateRedTests.cs`;
- `tests/ErpOnecAgent.IntegrationTests/A07ModeStateTests.cs`.

Report:

- `docs/remediation/a07-mode-state.md`.

No SQLite store implementation, migration, schema version, conflict documentation, plan, or orchestration file was changed.

## Evidence and exact counts

All commands used the native Windows SDK at `./.dotnet/dotnet.exe`.

| Check | Result | Evidence |
|---|---:|---|
| Runtime RED before production fix | 0 passed / 2 failed / 2 total | `local-data/remediation-2026-09-24/a07-mode-state/red/test.log`; `red/a07-mode-state-red.trx` |
| A07 targeted GREEN | 20 passed / 0 failed / 0 skipped | `local-data/remediation-2026-09-24/a07-mode-state/targeted-final/test-authoritative.log`; `targeted-final/TestResults/a07-mode-state-targeted-authoritative.trx` |
| Native Release Rebuild | 0 warnings / 0 errors | `local-data/remediation-2026-09-24/a07-mode-state/final/rebuild-authoritative.log` |
| Full unit tests | 39 passed / 0 failed / 0 skipped | `local-data/remediation-2026-09-24/a07-mode-state/final/unit-test-authoritative.log`; `final/TestResults/a07-mode-state-unit-authoritative.trx` |
| Full integration tests | 186 passed / 0 failed / 0 skipped | `local-data/remediation-2026-09-24/a07-mode-state/final/integration-test-authoritative.log`; `final/TestResults/a07-mode-state-integration-authoritative.trx` |
| Full total | 225 passed / 0 failed / 0 skipped | 39 unit + 186 integration |
| Locked restore | exit 0 | `local-data/remediation-2026-09-24/a07-mode-state/final/restore-locked.log` |
| Touched-file format verification | exit 0 | `local-data/remediation-2026-09-24/a07-mode-state/final/format-verify-authoritative.log` |

The RED failures were runtime assertion failures against the old production classes: `PauseCommands` became `Normal` after a normal handshake, and numeric mode `999` was accepted. The initial restore lock mismatch was resolved by reevaluating only the changed test project dependency graph; the locked restore above is green and package versions were preserved.

A solution-wide format verification still reports only the pre-existing findings in `src/ErpOnecAgent.Infrastructure/ErpApi/ErpClientRegistration.cs` and `tests/ErpOnecAgent.UnitTests/ErpLongPollResilienceTests.cs`; no touched A07 file is reported.

## Review follow-up

The concurrency regression now uses a `DispatchProxy` around the real migrated SQLite store. The first `SetStateAsync(true)` completes the real write, signals a TCS, and remains blocked before the controller publishes runtime state; the second `SetAsync(false)` is invoked while that gate is held. The test records that the second call has not entered persistence before release, then verifies both calls finish with SQLite and runtime at `false`. No sleep or timeout is used for ordering; `WaitAsync` is only a bounded hang guard.

Coverage also now includes local resume while handshake maintenance is active, and an actual `CommandExecutionWorker` pause callback whose result is absent while persistence is gated and succeeds after release.

| Review check | Result | Evidence |
|---|---:|---|
| A07 targeted review GREEN | 22 passed / 0 failed / 0 skipped | `local-data/remediation-2026-09-24/a07-mode-state/review/space-bunny-20260924-green/test.log`; `review/space-bunny-20260924-green/TestResults/a07-mode-review-20260924T183500_net10.0_20260924182726.trx` |
| Initial review attempt | 20 passed / 2 failed / 22 total | `review/space-bunny-20260924/test.log`; the failure was the test proxy initially declared `sealed`, which `DispatchProxy` rejects; corrected before GREEN |

No production code was changed in this follow-up, and no full solution run was repeated. The prior authoritative full result remains 39 unit + 186 integration = 225 passed. The earlier solution-level `a07-mode-state-final.trx` was overwritten by the two-project invocation and is not used as evidence; the separate unit and integration TRX files above remain authoritative.

## Remaining A07b and integration limits

A07b remains open for the full safe-page/long-running-operation boundary matrix, complete upload/complete-run mode matrix, and any broader atomic activation/reconciliation work. This slice does not add network cancellation, lease renewal, clock-drift policy, durable ETL jobs, extraction resume, or re-POST policy changes.

No real ERP or 1C was contacted. Tests use fake external clients, real migrated temporary SQLite, and fake certificate/secret dependencies. No Windows service installation, certificate store access, DPAPI/user-secret operation, deployment, or real integration acceptance was performed.

## Independent combined acceptance

Orchestrator reviewed the production changes, deterministic persistence overlap tests, unchanged existing package versions, and restored historical audit probe. After merging payload-conflict migration004 and cancellation state guard: native locked restore exit0; Release Rebuild0 warnings/errors; 39 unit +219 integration =258/258, no skips. Source hashes stable throughout. Evidence: `local-data/remediation-2026-09-24/a07-mode-state/combined-conflict-cancel`. Full A07b remains open.
