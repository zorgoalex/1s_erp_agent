# A07b B6 — Compatibility rejection latch and session-independent heartbeat

Date: 2026-09-24 (rev. 2 — root review corrections 2026-09-25: malformed `minimumAgentVersion` fail-closed validation, disclosed test corrections). Baseline: checkpoint-380 (c2c04f9). Scope: bounded B6 slice only — explicit compatibility rejection latching, §30.2 heartbeat decoupling, and `incompatible_version` visibility. **This report does not claim that A07 (or stage 2) is complete.** B1 admission point, B3 extraction pause boundary, B4 permission wiring, B8/B9/B10 items, remain open. The local policy now blocks never-sent POSTs while explicit compatibility rejection is latched; external agreement of that policy remains separate.

## Result

### Compatibility rejection latch (`AgentRuntimeState` / `AgentModeSnapshot`)

`AgentModeSnapshot` gains `CompatibilityRejected`, a fourth mode input evaluated under the existing `_modeGate` lock:

- `CanLeaseCommands`, `CanExecuteCommands`, and `CanExtract` additionally require `!CompatibilityRejected`.
- `CanResolveCommandResults`, `CanDeliverResults`, `CanUploadBatches`, `CanCompleteEtlRuns` are unchanged (`IsReady` only) — already-sent status lookup, saved result delivery, and batch upload/run completion continue under the latch per the chosen LOCAL policy and the A07a matrix.
- `IsReady`, the persisted local ETL pause, the validated remote `AgentMode`, and handshake maintenance remain independent inputs; rejection never modifies them.

Two new transitions (both under the same lock):

- `LatchCompatibilityRejection()` — sets the latch only; touched by no other input.
- `ApplyCompatibleHandshake(maintenanceMode)` — publishes one accepted version-compatible response coherently: clears ONLY the compatibility latch AND applies that response's maintenance flag atomically, so no snapshot ever shows a transient lifting of all restrictions. Local pause and remote mode are never touched.

### Session manager (`ErpSessionManager.GetSessionAsync`)

- `Accepted=false` → latch + `InvalidOperationException` (unchanged throw semantics for callers' retry paths).
- `minimumAgentVersion` is contract-required (`erp-agent-api.openapi.yaml:89` `required: […, minimumAgentVersion, …]`, `type: string`; non-nullable DTO field) — an absent/null/empty/whitespace/garbage value can never establish compatibility, so it fails closed as a protocol error (`InvalidDataException`) that neither latches a rejection nor clears an existing one and caches no session. This closes a fail-open gap where `Version.TryParse` returning false fell through to `ApplyCompatibleHandshake`, clearing a latched rejection on an unverifiable response.
- An unparseable local assembly version is likewise a hard failure before any publish (environment fault, not a response rejection).
- Parsed `minimumAgentVersion` > running binary → latch + `InvalidOperationException`.
- `Accepted=true` with `SessionId == Guid.Empty` → `InvalidDataException` **before** any state publish — protocol-invalid, distinguished from explicit version rejection; it neither latches nor clears and the session is never cached as valid.
- Accepted + compatible + valid → `ApplyCompatibleHandshake(response.MaintenanceMode)` then cache session.
- `StartSessionAsync` throwing (network/timeout) → no state touched: transient failures neither invent nor clear the latch.
- `Invalidate()` clears the session cache only — never the latch, never the maintenance flag.

### Heartbeat (`HeartbeatWorker`)

- The `GetSessionAsync` prerequisite is removed: `HeartbeatRequest` carries `agentId` and no `sessionId`, so the heartbeat never waits on or calls a failed/hung handshake. The `sessions` constructor parameter is retained — a failed heartbeat POST still calls `Invalidate()` so a dead session is re-handshaken by the lease/config path.
- `State` reports `incompatible_version` while `CompatibilityRejected` is latched, with priority **above** `maintenance`/`storage_critical`/`offline_onec`/`degraded` (documented choice: while latched it is the only state that explains why admission is off and that an operator action — agent upgrade — is required; the other states become observable again after a compatible handshake).

### Recovery and deadlock-freedom

`ConfigurationWorker` still calls `GetSessionAsync` every minute while `IsReady`; `CommandLeaseWorker` cannot reach the handshake at all while `CanLeaseCommands` is false, so the blocked lease path can never hold the session semaphore — the config retry is the recovery driver and cannot deadlock. The legitimate lease handshake is untouched. The latch is **in-memory only**: it is intentionally not durable across restart (disclosed limitation — a rejected agent that restarts returns to the unknown/baseline admitted-work policy until the next handshake response).

Test-honesty note: `Blocked_lease_does_not_starve_handshake_retry_and_recovers_after_accept` drives the retry with a **manual** `sessions.GetSessionAsync` call standing in for the `ConfigurationWorker` path — it does not run the real `ConfigurationWorker` or its one-minute timing. It proves the latch blocks lease admission without blocking the handshake, and that an accepted response recovers the real lease worker; it does not prove the production polling cadence.

## Policy notes

- Unknown-before-first-handshake keeps the baseline admitted-work policy: absence of a session alone adds no gate.
- Fresh-POST gate from accepted A07b B2/B7 is preserved verbatim: under the latch the real `CommandExecutionWorker` selects only the sent-work query (`GetDueSentCommandsAsync`); `mayStartNewWork` stays `CanExecuteCommands`, so zero POSTs while `GetStatusAsync` resolution continues. The decision-point/non-atomic-network caveat is unchanged — a restriction landing after the in-pass check can still race one in-flight POST, resolved by the existing unknown-result machinery.
- `incompatible_version` visibility uses only the existing heartbeat `State` field — no invented external contract, no schema or persistence change.

## Changed files

Production:

- `src/ErpOnecAgent.Service/Runtime/AgentRuntimeState.cs` (snapshot input + gates + `ApplyCompatibleHandshake`/`LatchCompatibilityRejection`)
- `src/ErpOnecAgent.Service/Runtime/ErpSessionManager.cs` (latch on explicit rejection, coherent accept publish, `Guid.Empty` protocol guard)
- `src/ErpOnecAgent.Service/Workers/HeartbeatWorker.cs` (session prerequisite removed, `incompatible_version` state)

Tests (new):

- `tests/ErpOnecAgent.IntegrationTests/A07CompatibilityHeartbeatRedTests.cs` (runtime reproductions, failed on baseline)
- `tests/ErpOnecAgent.IntegrationTests/A07CompatibilityHeartbeatTests.cs` (slice matrix, green after fix)

Report: `docs/remediation/a07-compatibility-heartbeat.md`.

No SQLite store, migration, schema, ETL, contract, plan, or orchestration file was changed. No other worker was touched.

## Evidence and exact counts

All commands used the native Windows SDK at `D:/WORK/CNC_Milling/WORK_CNC/SOFT/1C/1C-agent/repo_1c-agent/.dotnet/dotnet.exe` (10.0.400), Release configuration, on this worktree only. dotnet exit codes were captured directly (not pipe exit).

| Check | Result | Evidence |
|---|---:|---|
| Runtime RED before production fix | 0 passed / 2 failed / 2 total | `local-data/remediation-2026-09-24/a07-compatibility-heartbeat/red/test.log`; `red/a07-compat-red.trx`; build log `red/build.log` |
| A07 compat targeted GREEN (15 = 13 slice + 2 former-RED, pre-review code) | 15 passed / 0 failed / 0 skipped | `local-data/remediation-2026-09-24/a07-compatibility-heartbeat/targeted.log`; `a07-compat-targeted.trx` |
| Locked restore (pre-review) | exit 0 | `final/restore-locked.log` |
| Native Release `-t:Rebuild --no-restore` (pre-review) | 0 warnings / 0 errors, exit 0 | `final/rebuild.log` |
| Full unit tests (pre-review) | 39 passed / 0 failed / 0 skipped | `final/unit-test.log`; `final/a07-compat-unit.trx` |
| Full integration tests (pre-review) | 356 passed / 0 failed / 0 skipped | `final/integration-test.log`; `final/a07-compat-integration.trx` |
| Pre-review full total | 395 passed / 0 failed / 0 skipped | 39 unit + 356 integration |
| **Review RED on pre-review impl** (malformed `minimumAgentVersion` fail-open) | 0 passed / 6 failed / 6 total | `review-red/test.log`; `review-red/a07-compat-reviewred.trx`; `review-red/build.log` |
| **Review compat GREEN (post-fix)** | 21 passed / 0 failed / 0 skipped | `review-green/test.log`; `review-green/a07-compat-review.trx` |
| Locked restore (post-review) | exit 0 | `review-final/restore-locked.log` |
| Native Release `-t:Rebuild --no-restore` (post-review) | 0 warnings / 0 errors, exit 0 | `review-final/rebuild.log` |
| Full unit tests (post-review) | 39 passed / 0 failed / 0 skipped | `review-final/unit-test.log`; `review-final/a07-compat-r2-unit.trx` |
| Full integration tests (post-review) | 362 passed / 0 failed / 0 skipped | `review-final/integration-test.log`; `review-final/a07-compat-r2-integration.trx` |
| **Post-review full total** | **401 passed / 0 failed / 0 skipped** | 39 unit + 362 integration |

The baseline RED failures were runtime assertions against the unchanged workers: the explicit-rejection heartbeat was never delivered (the worker awaited `GetSessionAsync` before sending) and the heartbeat waited on a hung handshake indefinitely. The review-RED failures were all "no exception thrown": a malformed required `minimumAgentVersion` on an accepted response fell through `Version.TryParse` to `ApplyCompatibleHandshake`, cleared the latch, and cached the session.

## Review corrections disclosed

- **RED test scenario correction (post-baseline):** the original baseline RED run proved heartbeat suppression with the worker driving the handshake itself. After the fix the heartbeat is session-independent, so `Explicit_handshake_rejection_still_delivers_heartbeat_with_incompatible_version` was amended to explicitly drive the rejection through `sessions.GetSessionAsync` (the lease/config-equivalent session path) before starting the heartbeat worker. The original baseline RED evidence stands as runtime proof of the gap; the amended setup is a corrected scenario for the post-fix world, not the identical original test.
- **Malformed-minimum fail-open:** added during root review — validation of the contract-required `minimumAgentVersion` (and the local version) now precedes compare/publish; `Guid.Empty` remains a protocol error preserving the latch. Review RED (6/6 fail on pre-review impl) and post-fix GREEN are separate evidence folders above.
- **Result-delivery assertion race:** `Saved_result_delivery_continues_under_rejection` asserted outbox emptiness immediately after the fake ERP ACK returned, racing the worker's subsequent `store.AcknowledgeResultAsync` write. It now polls the durable `results_outbox` transition within the bounded window.
- All counts above were re-verified against the saved console logs and TRX files.

## Test coverage map (minimum-required matrix)

- Explicit rejection → heartbeat still delivered: `A07CompatibilityHeartbeatRedTests.Explicit_handshake_rejection_still_delivers_heartbeat_with_incompatible_version` (real `HeartbeatWorker` + fake `IErpClient` + real migrated SQLite + real `AgentMetricsCollector`).
- Accepted-but-min-high rejection latch: `Accepted_response_with_minimum_above_current_latches_rejection`; explicit `Accepted=false` latch + permission split: `Explicit_rejection_latches_compatibility_and_denies_admission_only`.
- Mode-source non-interference: `Rejection_preserves_local_pause_and_remote_mode`; compatible handshake clears only the latch + applies its maintenance flag without touching local/remote: `Compatible_handshake_clears_only_rejection_and_applies_its_maintenance_flag`, `Compatible_handshake_without_maintenance_clears_rejection_and_flag`.
- Transient failure preserves/does not invent latch: `Transient_handshake_failure_neither_latches_nor_clears_rejection`.
- Invalidate preserves latch (cache-only): `Invalidate_clears_only_the_session_cache_not_the_rejection`.
- No handshake call from heartbeat even when the endpoint hangs: `Heartbeat_never_calls_or_waits_on_the_session_handshake` (asserts `StartSessionCalls == 0`); former-RED `Heartbeat_does_not_wait_on_a_hung_session_handshake`.
- `incompatible_version` priority over maintenance: `Heartbeat_reports_incompatible_version_above_maintenance_state`.
- Protocol-invalid vs version rejection: `Empty_session_id_is_protocol_invalid_not_a_version_rejection` (`Guid.Empty` never cached as a valid session); malformed `minimumAgentVersion` after a latched rejection preserves latch + caches no session, valid accept afterward clears (`Malformed_minimum_in_accepted_response_preserves_latch_and_never_caches_session`, theory over null/empty/whitespace/garbage); malformed on first handshake is a protocol error that invents no rejection (`Malformed_minimum_on_first_handshake_is_protocol_error_not_rejection`).
- Blocked lease cannot starve compatibility recovery: `Blocked_lease_does_not_starve_handshake_retry_and_recovers_after_accept` (real `CommandLeaseWorker` resumes leasing only after the accepted handshake clears the latch; retry driven by a manual `GetSessionAsync` standing in for `ConfigurationWorker`, not the real config timing).
- Real worker under latch — zero POST, status lookup continues, fresh row untouched: `Sent_business_command_resolves_under_latch_while_fresh_post_stays_denied` (real `CommandExecutionWorker` over real migrated SQLite).
- Saved result delivery under rejection (real worker): `Saved_result_delivery_continues_under_rejection` (real `ResultDeliveryWorker`, `results_outbox` row ACKed to fake ERP).

All synchronization uses TCS barriers + bounded `Task.WhenAny`/`WaitAsync` hang guards and `try/finally` worker cleanup; no sleeps are used for race ordering.

## Remaining limits

A07 remains partial: B1 long-poll admission point, B3 mid-extraction pause boundary (blocked on A04/A05), B4 `CanCompleteEtlRuns` wiring, B8 admin semantics beyond local policy, B9 durable triggers (A03), B10 heartbeat-shape/lease-clock backlog, remain open. Explicit rejection now gates never-sent locally-queued POSTs under the root-selected local policy; external agreement is not claimed. The latch is in-memory (not durable across restart). No real ERP or 1C was contacted; tests use fake external clients, real migrated temporary SQLite, and fake certificate/spool/secret dependencies. No service installation, deployment, secrets, external systems, commits, or pushes were performed.


## Independent root acceptance (2026-09-25)

The orchestrator reviewed the three production files, both test files, original two runtime RED assertions and six review RED assertions. After transferring this slice onto checkpoint-380, a fresh full Release `-t:Rebuild --no-restore` passed with zero warnings/errors; the full solution test returned exit 0 with 362 integration + 39 unit = 401/401, no skips. Source hashes were stable. Evidence relative to repository: `local-data/remediation-2026-09-25/a07-compatibility-root/` (`build.log`, `test.log`, separate TRX files in `trx/`). The correction to the former RED test setup and the manual session-recovery stand-in remain explicitly bounded as described above.
