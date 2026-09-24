# Configuration activation consistency — durable acceptance before runtime publication

Date: 2026-09-24. Scope: bounded configuration activation slice after A07a. This report does not claim that all of A07/A07b is complete. Work continued from `agents/worktrees/configuration-activation-wip` on top of checkpoint-258 (39 unit + 219 integration); the previously unaccepted edits were preserved as the starting point.

## Root-review defects fixed

1. `ConfigurationWorker.Apply` changed memory before `ActivateConfigSnapshotAsync`, so a durable failure left a rejected configuration effective. The worker now validates, durably accepts and activates, and only then publishes prepared runtime state.
2. `SaveConfigSnapshotAsync` `ON CONFLICT DO UPDATE status` could demote a currently active (or superseded) same-version row to `validated`/`rejected`; a failed replay could destroy the restart active source. Status transitions are now explicit and never demote `active`/`superseded` rows.
3. `ActivateConfigSnapshotAsync` superseded the old active unconditionally even for a missing, rejected, or stale target, and could leave no valid active. Activation is now atomic inside one transaction: the previous active is preserved on any rejection.
4. Bootstrap trusted the persisted active body without validating it. `BootstrapService` now verifies the stored canonical hash and rejects ambiguous bodies before deserializing, and fails closed.
5. Canonical-hash vs case-insensitive deserializer ambiguity (second review): `PayloadHasher` sorts property names ordinally, while `RemoteAgentConfiguration` deserializes case-insensitively with last-wins binding. A stored `rejected` row `{"Mode":"Normal","mode":"999",...}` and an incoming `{"mode":"999","Mode":"Normal",...}` share one canonical hash, yet parse to different modes. Pre-fix, the incoming body passed `Prepare`, `AcceptAndActivate` promoted the stored ambiguous row to `active`, and the post-commit re-parse of the persisted body threw — leaving an invalid v8 active in the database with memory still on v7 and a fail-closed restart. Fixed below.

## Design

Durable acceptance happens before runtime publication:

- `DynamicConfigurationState.Prepare` produces a complete immutable `DynamicConfigurationSnapshot` (frozen allowlist, deep-frozen entity `Select`/`KeyFields`, interval, parsed mode) and rejects rollback versions before any write.
- `JsonAmbiguityGuard.EnsureUnambiguous` (new, `ErpOnecAgent.Domain.Common`) recursively rejects any JSON object containing property names that differ only by case — i.e., inputs whose canonical hash cannot determine a unique case-insensitive deserialization. It is enforced at every semantic boundary: the worker input check before `Prepare`, the store `ValidateSnapshot` used by `validated` saves, `AcceptAndActivateConfigSnapshotAsync` input, and `ActivateConfigSnapshotAsync` target validation; `AcceptAndActivate` additionally validates the stored row itself before promoting it; the worker re-checks the persisted body before publishing; `BootstrapService` checks the persisted body before restore. `PayloadHasher` is unchanged; `rejected` evidence rows bypass validation deliberately. An unambiguous replay whose canonical form equals the stored row still passes (hash equality implies the same property-name multiset, hence the same deserialization).
- `IAgentStore.AcceptAndActivateConfigSnapshotAsync` performs accept-and-activate in a single SQLite transaction: it validates input hash/body, inserts or verifies the row (same version requires identical hash and equivalent canonical body, and the stored row must itself pass `ValidateSnapshot`), rejects versions older than the latest accepted (`MAX(version)` over `active`/`superseded`) or than the current active, supersedes only the old active, marks the target `active`, commits with `CancellationToken.None`, and returns the persisted row. A same-version/hash replay is an idempotent no-op returning the active row.
- The worker passes `CancellationToken.None` to the durable call, re-validates the persisted payload hash and shape, re-prepares the persisted body, and publishes. Publication ordering: the store call is the only awaited step after the last cancellation check; once it returns, only synchronous code runs before `Publish`, so a stopping cancellation arriving after the durable commit cannot skip publication. The production worker deliberately passes CancellationToken.None after preparation; stopping cancellation during that call does not abort activation. A genuine storage failure before commit rolls back the transaction.
- `Publish` writes all snapshot fields and `runtime.SetRemoteMode` inside the configuration lock, so overlapping publishes cannot leave snapshot version/mode disagreeing with runtime remote mode; a stale publish is rejected by the rollback check under the same lock.
- `SaveConfigSnapshotAsync` keeps evidence semantics: `rejected`/`validated` writes on a missing row insert; `validated` input is always fully validated first; on an existing row the methods verify hash (and body for `validated`) and can only move `validated`↔`rejected`; `active`/`superseded` rows are never demoted and a changed hash or changed body is rejected with `InvalidDataException`.
- `ActivateConfigSnapshotAsync` (administrative/test path) now rejects atomically: missing row, non-`validated` status, corrupted or ambiguous stored body/hash, older-than-accepted, or same-version conflicting body all throw before any `superseded` update, so the previous active survives every failure.
- Read-check-write serialization uses a process-lifetime `SemaphoreSlim` (standard primitive, no homemade async lock). Production calls are additionally sequential: the only runtime caller of activation is the single hosted `ConfigurationWorker` loop, and `BootstrapService` publishes once before readiness. Activation-through-publication ordering therefore holds by construction on the production route; this is single-writer source ownership, not a general multi-writer coordinator.
- Configuration readers take one snapshot per decision: `ConfigurationWorker` and `CommandLeaseWorker` read `dynamicConfiguration.Snapshot` once per iteration; `OnecEtlWorker` reads `Snapshot.IntervalMinutes`/`Snapshot.Entities` once per scheduling/selection decision. Local ETL pause and handshake maintenance sources are untouched and remain independent inputs to `AgentModeSnapshot`.
- Bootstrap validates the stored canonical hash and rejects ambiguous persisted bodies before trusting the body; malformed JSON, hash mismatch, ambiguity, or invalid content fails closed before readiness.

## Changed files

Production:

- `src/ErpOnecAgent.Application/Abstractions/Persistence.cs` — added `AcceptAndActivateConfigSnapshotAsync` (returns the persisted `ConfigSnapshot`).
- `src/ErpOnecAgent.Domain/Common/JsonAmbiguityGuard.cs` (new) — recursive case-insensitive duplicate-property rejection for JSON documents.
- `src/ErpOnecAgent.Infrastructure/Persistence/Sqlite/SqliteAgentStore.cs` — status-transition-safe `SaveConfigSnapshotAsync`, atomic `AcceptAndActivateConfigSnapshotAsync` (validates stored row before promotion), guarded `ActivateConfigSnapshotAsync`, shared row/hash/body validation helpers including ambiguity rejection, `SemaphoreSlim` write gate.
- `src/ErpOnecAgent.Service/Runtime/DynamicConfigurationState.cs` — `Prepare`/`Publish` split, immutable snapshot record, atomic mode publication, rollback rejection.
- `src/ErpOnecAgent.Service/Runtime/BootstrapService.cs` — stored-hash and ambiguity validation before restore; publish through `Prepare`/`Publish`.
- `src/ErpOnecAgent.Service/Workers/ConfigurationWorker.cs` — validate (hash, ambiguity, `Prepare`) → durable accept/activate → verify persisted payload → publish; rejected evidence saved with `CancellationToken.None`; version sent from one snapshot.
- `src/ErpOnecAgent.Service/Workers/CommandLeaseWorker.cs` — one snapshot per iteration.
- `src/ErpOnecAgent.Service/Workers/OnecEtlWorker.cs` — one snapshot per scheduling/selection decision.

Tests:

- `tests/ErpOnecAgent.IntegrationTests/ConfigurationActivationTests.cs` (new, 20 tests) — real migrated temporary SQLite plus the real `ConfigurationWorker`/`BootstrapService`/`DynamicConfigurationState`/`LocalEtlPauseController` with faked `IErpClient`, `ISpoolStore`, `ISecretStore`, and `DispatchProxy` store fakes with TCS gates.
- `tests/ErpOnecAgent.IntegrationTests/A07ModeStateTests.cs`, `tests/ErpOnecAgent.IntegrationTests/SqliteStoreTests.cs` — real canonical hashes (the store now validates stored hash/body).

No schema, migration, package, plan, or orchestration change; no historical audit file modified.

## Test coverage

Worker level (real worker + real store, faked ERP): activation failure keeps previous DB active and memory state; same-version replay idempotent; valid new version preserves local pause and handshake maintenance; 304/null no-op; stopping cancellation after durable commit cannot skip publish (TCS gate asserts DB active at 51 while memory still 50, cancels, releases, observes publish; gate released in `finally` before bounded `StopAsync`); ambiguous case-alias replay cannot promote a rejected row (root-finding repro). Store level: rejected replay does not demote active; missing/rejected/older/conflicting/ambiguous activation targets preserve previous active; same-version changed hash cannot mutate the original active row; same-version replay idempotent; canonical reorder/whitespace replay accepted; concurrent accept/activate from two store instances on the same database, started behind a shared TCS barrier via `Task.Run`, converges to exactly one `active` row at the highest version. State level: corrupted persisted hash fails closed at bootstrap; ambiguous persisted body fails closed at bootstrap; restart restores the persisted activation before readiness; overlapping publishes stay coherent (stress coverage, see below); prepared snapshots are immutable and independent of source lists.

## Evidence and exact counts

All commands used the native Windows SDK `D:/WORK/CNC_Milling/WORK_CNC/SOFT/1C/1C-agent/repo_1c-agent/.dotnet/dotnet.exe` with this worktree as cwd.

| Check | Result | Evidence |
|---|---:|---|
| Runtime RED against original implementation (authoritative) | 6 failed / 4 passed / 10 total | `local-data/remediation-2026-09-24/configuration-activation/red/test.log`; `red/TestResults/configuration-activation-red_net10.0_20260924183708.trx` |
| Runtime RED, ambiguity defect (pre-fix) | 3 failed / 5 total | `local-data/remediation-2026-09-24/configuration-activation/red-ambiguity/test.log`; `red-ambiguity/TestResults/configuration-activation-red-ambiguity_net10.0_20260924214205.trx` |
| Focused final (this file) | 20 passed / 0 failed / 0 skipped | `final/focused-test.log`; `final/TestResults/configuration-activation-focused_net10.0_20260924214303.trx` |
| Touched-path regression (SqliteStoreTests + A07ModeStateTests) | 35 passed / 0 failed / 0 skipped | console run, same build |
| Locked restore | exit 0 | `local-data/remediation-2026-09-24/configuration-activation/final/restore-locked.log` |
| Release rebuild | 0 warnings / 0 errors | `local-data/remediation-2026-09-24/configuration-activation/final/rebuild.log` |
| Full unit tests (pre-follow-up state) | 39 passed / 0 failed / 0 skipped (includes ~25 s long-poll test) | `final/unit-test.log`; `final/TestResults/configuration-activation-unit_net10.0_20260924213053.trx` |
| Full integration tests (pre-follow-up state) | 235 passed / 0 failed / 0 skipped | `final/integration-test.log`; `final/TestResults/configuration-activation-integration_net10.0_20260924213107.trx` |
| Touched-file format verification (post-follow-up) | exit 0 | `final/format-verify.log` |

Notes on evidence:

- The authoritative first-slice RED is the `..._20260924183708.trx` run (6 failed / 4 passed). The sibling `..._20260924183651.trx` was an earlier attempt with a fixture issue (7/3) and is not cited as evidence. The RED run was produced earlier in this slice against the checkpoint-258 production sources (the test file copied into the main tree, sources unchanged): `Assert.Throws` found no exception for rollback/missing/rejected targets, the rejected replay demoted `active` to `rejected`, the corrupted stored hash was trusted, and the worker held version 8 in memory after the durable activation failure — runtime assertion failures, not compile errors.
- The ambiguity RED ran against the then-current (pre-fix) implementation in this worktree: the filter `~Ambiguous` matched 5 tests; the 3 new defect reproductions failed at runtime (`active` became 8 after promoting the ambiguous rejected row; the ambiguous stored `validated` row was activated without exception; bootstrap became ready on an ambiguous persisted body), while the 2 unrelated pre-existing `ambiguous`-named tests passed.
- The full-suite numbers (39 unit + 235 integration) were produced before the reviewer follow-up changes in this round. After the ambiguity fix and test corrections, the focused run is 20/20 in the new file plus 35/35 on the touched store/A07 paths; the root reviewer runs the authoritative full rebuild/test before merge.

## Reviewer follow-up resolution

- The ~70-line hand-rolled `ConfigurationGate` (cancellable async semaphore with waiter lifecycle) was deleted. Read-check-write config transactions are now serialized by a process-lifetime static `SemaphoreSlim`; SQLite `busy_timeout=30000` remains the fallback boundary. Making the store `IDisposable` for the semaphore was rejected: it propagated CA1001 into 18 accepted test classes, which is out of this slice's bounds. A static field is not instance-owned, so no analyzer suppression was needed.
- `DynamicConfigurationState.Publish` now performs the field update and `runtime.SetRemoteMode` under the same lock, so overlapping publishes cannot split version/mode from runtime remote mode. `Overlapping_publishes_converge_to_coherent_snapshot_and_remote_mode` races 64 publish pairs and asserts coherence after every pair; this is stress coverage, not a deterministic forced interleaving — the fix is by construction (single lock covers both writes plus the version-order check).
- `Worker_publishes_after_durable_commit_even_when_stopping_is_cancelled` uses a `DispatchProxy` TCS gate between durable commit and publish, cancels the start token while gated, and verifies publication still happens; the gate is released in `finally` before `StopAsync` (which is also bounded by `WaitAsync`) so a failed assertion cannot hang the test.
- `Concurrent_accept_and_activate_converges_to_single_highest_active` now uses two `Task.Run` operations behind a shared TCS start barrier against two `SqliteAgentStore` instances sharing one database file (exercising the shared static gate), and asserts `COUNT(active)=1` at version 7 in either completion order.
- Activation-through-publication serialization is single-writer source ownership: the only runtime activation caller is the one hosted `ConfigurationWorker` loop (sequential per iteration), `BootstrapService` publishes once before readiness, the store gate serializes durable read-check-write, and `Publish` rejects out-of-order versions under lock. No general multi-writer coordinator is claimed.
- Canonical-hash/deserializer ambiguity is closed by rejecting case-insensitive duplicate property names at every validation boundary (worker input, store `ValidateSnapshot` for `validated`/accept/activate paths, stored-row validation before promotion, worker persisted-body check, bootstrap restore). `PayloadHasher` is unchanged, so command/wire hashing is untouched; ordinary same-version canonical reorder/whitespace replay remains accepted.

## Remaining limits

- A07b remains open for the wider worker safe-boundary matrix (safe-page/long-running-operation boundaries, upload/complete-run mode matrix, and any broader reconciliation work); this slice covers configuration activation consistency only.
- The write gate is per-process; cross-process exclusion on the same database file relies on the existing single-instance lock, as before.
- `AcceptAndActivateConfigSnapshotAsync` trusts caller-side `Prepare` for semantic validation; the store itself enforces hash/body/ambiguity integrity and version ordering only.
- A `rejected` row may be re-promoted by a later same-hash `validated` save/accept. This is intentional: a valid configuration may be rejected after a transient persistence failure and later legitimately accepted with the same hash. Promotion is still gated by stored-row validation, hash/body equivalence, and version ordering.
- No real ERP or 1C was contacted; no service installation, secrets, or network endpoints were used. Tests use fake external clients and real migrated temporary SQLite. No raw payload logging was added.

## Independent acceptance — 24.09.2026 21:51

Root reviewed and merged the final changes after checking each destination against checkpoint-258. Native Release `-t:Rebuild --no-restore`: 0 warnings/errors. Full solution tests: 39 unit + 239 integration = **278/278**, no failures or skips. Sources remained byte-identical during build/test. Evidence: `local-data/remediation-2026-09-24/configuration-activation-root/` (rebuild.log, full-test.log, two unique TRX files, source manifest). Agent evidence preserved separately under `configuration-activation-devin/`; historical worktree-relative paths above refer to that copied evidence tree. This accepts configuration activation only; broader A07 remains open.
