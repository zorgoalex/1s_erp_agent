# O1 — ETL ownership + extraction fencing (DARK)

Remediation slice implementing the bounded **O1** layer of
`docs/remediation/etl-ownership-upload-design.md` (revision 3, root disposition
normative) on top of the F1 finalize slice (`a05-finalize-storage.md`).

**Status: independently accepted as a DARK storage slice on 25 September 2026.
O1 is not wired into workers, RecoverAsync, the ERP client or production dispatch.
This is not a production cutover; see root acceptance below.**

## Scope delivered

- Migration `007_etl_ownership.sql` (additive): `etl_entity_ownership`
  (lifetime ownership, positive `ownership_epoch`, no TTL/stale takeover),
  `etl_run_ownership_bindings` (immutable expected-epoch bindings), extraction
  claim columns on `etl_runs`, dispatch/deferral/claim-counter columns on
  `etl_jobs`. `SqliteMigrator.CurrentSchemaVersion = 7`; migrations 001–006 and
  their checksums unchanged.
- Typed O1 contracts in `Abstractions/EtlOwnership.cs`
  (`EtlJobClaim`, `EtlJobClaimOutcome`, `EtlJobDeferralReason`,
  `EtlDispatchableJob`, `EtlPendingRunQuarantine`, `EtlJobDispatchPage`) and new
  rejection members (`ExtractionClaimLost`, `OwnershipSetMismatch`) in
  `EtlFinalize.cs`.
- `TryClaimEtlJobAsync`: probe-guard write, frozen job-identity consistency
  check, elder-manifest quarantine/hold, elder-overlap admission reservation,
  all-entity ownership acquisition + immutable bindings under a SAVEPOINT with
  full rollback on any conflict, fresh `extractionClaimId` mint, committed-claim
  counter increment — one transaction per outcome.
- `GetDispatchableEtlJobsAsync`: the SAME complete typed frozen-identity
  validation the claim transaction applies — manifest typed-validity AND
  ordered typed entity-code equality via the deterministic per-connection
  SQLite scalar `etl_job_manifest_consistent` (calls the existing C# helpers;
  malformed JSON can never throw inside the query) — plus frozen mode and
  configuration-version equality, elder-overlap and ownership-availability
  predicates, all evaluated in SQL **before** `LIMIT`; corrupt pending runs are
  surfaced as quarantine diagnostics (including mode/config drift); a busy,
  deferred or corrupt head cannot starve disjoint eligible work. Enumeration is
  strictly read-only.
- Guarded quarantine transitions: `CommitJobManifestBlockAsync` returns affected
  row counts; the pending-run quarantine and any still-blockable job transition
  are expected-one — a zero-row guard loss or swallowed write (RAISE(IGNORE))
  throws and rolls back the whole claim transaction, never a partial
  quarantine. Already-terminal jobs legitimately transition zero rows.
- Extraction claim fence: `BeginEtlEntityExtractionAsync`,
  `RegisterGuardedEtlBatchAsync`, `CompleteEtlEntityExtractionAsync`,
  `SealEtlRunExtractionAsync`, `FailEtlRunAsync`, `BlockEtlRunAsync` all require
  the exact extraction claim GUID; no old overload bypass exists.
- `RunOwnershipSetPredicate` (shared const): typed nonempty unique string
  manifest AND exact set equality (manifest == immutable bindings == active
  ownership rows at bound positive epochs) in both directions, using
  `NOT EXISTS` only — `NOT IN`/NULL semantics can never hide extras. Malformed
  JSON, `null`, `[]`, `[null]`, `[1]`, `"{}"`, blank or duplicate entries all
  fail closed with zero writes.
- Touched-entity arm (`EntityOwnershipPredicate`): Register AND Complete require
  the mutated entity itself to carry an epoch-bound active ownership row +
  binding — a stray injected `extracting` row cannot accept writes.
- Completion/finalize: `VerifyClaimReadinessAsync` evaluates the exact ownership
  gate **before** the transient un-ACK check — corrupt ownership blocks
  `OWNERSHIP_SET_MISMATCH` and `CommitBlockedRunAsync` dead-letters pending
  batches; intact ownership + in-flight uploads remain transient. Finalize
  releases ownership inside the same savepoint as the watermark CAS writes;
  `released != sealed_entity_count` is a controlled `OWNERSHIP_RELEASE_MISMATCH`
  rollback-then-block; a thrown SQL error rolls back the whole transaction with
  the completing claim preserved for a convergent retry.
- Conservative inert evidence (`RunInertPredicate`, shared verbatim between the
  elder read and the direct self-claim path): lifecycle timestamps, errors,
  conflict fields, counters, extraction/completion claims, attempt policy, seal,
  payload, batches, entity rows, bindings, ownership rows, and ALL job-side
  history (non-`pending` status, `claim_attempt_count`, dispatch diagnostics,
  deferral fields) count as prior effects. The diagnostic job block preserves dispatch and claim evidence. It replaces
  the deferral message and clears the next eligibility time; its blocked status
  itself prevents a later never-started classification. Unresolved effects keep the pending run's admission hold
  forever; provably-inert corrupt evidence is quarantined (job + run blocked).
- `RecoverInterruptedEtlRunsAsync` clears dead extraction claims on running runs
  while retaining ownership/bindings; `CommitBlockedRunAsync` clears the
  extraction claim defensively and dead-letters pending batches. Legacy
  `RecoverAsync` untouched.

## Out of scope (unchanged)

No `008_etl_send_attempts.sql`, no `009_etl_scheduled_runs.sql`, no send/upload
ledger APIs, no scheduled-run APIs, no worker/client/source-config changes, no
manual ownership release API (`manual_release` remains a reserved reason only),
no production cutover. Legacy §9 bypass writers are documented as remaining
open until the atomic cutover change.

## Evidence (`local-data/remediation-2026-09-25/etl-ownership-o1/`)

| Step | Command | Exit | Result |
|---|---|---|---|
| baseline | `dotnet restore --locked-mode` | 0 | `baseline/restore.log` |
| baseline | `dotnet build -t:Rebuild --no-restore -c Release` | 0 | `baseline/rebuild.log` |
| baseline | `dotnet test` (full, unchanged worktree) | 0 | 470 int + 39 unit = **509** (`baseline/test.log`, TRX) |
| runtime RED | RED suite vs unchanged impl | 1 | 9/9 runtime failures (`red/`, `red/exit-code.log`) |
| F1+RED adapted | filtered run | 0 | 111/111 (`green/`) |
| probes vs draft | `dotnet test --filter ReviewProbes` | 1 | **5 failed / 13 passed** — real RED on the draft (`review-final/probes-prefixed-draft.*`) |
| fix iteration | filtered O1/probes/migration/RED | 1 then 0 | `o1-fix1` (3 test-expectation fixes) → `o1-fix2` 62/62 |
| ProbeE root RED | root's independent run vs final draft | 1 | **2 failed / 1 passed** (`root-enumeration-red/`, root-owned, preserved) |
| my ProbeE/fault RED | `--filter ProbeE\|ProbeD\|Swallowed_elder\|Mode_drift` vs same draft | 1 | **4 failed / 3 passed** (`probeE-red-draft.*`) |
| post-fix | filtered probes/O1/migration/RED | 0 | 69/69 (`o1-enum-fix.*`) |
| locked restore | `dotnet restore ErpOnecAgent.sln --locked-mode` | 0 | `review-final/restore-locked-final2.log` |
| release rebuild | `dotnet build ErpOnecAgent.sln -c Release -t:Rebuild --no-restore` | 0 | `review-final/rebuild-release-final2.log` |
| full suite | `dotnet test ErpOnecAgent.sln -c Release --no-build` | 0 | **539 int + 39 unit = 578, 0 failed** (`full-final4.log`, TRX `full-final4_*`) |
| format | `dotnet format whitespace --verify-no-changes --include <touched>` | 0 | `review-final/format-verify.log`, `format-verify2.log`, `format-verify3.log` |

Intermediate counts (571, then 572) were superseded as required tests were
added; the earlier green runs (`full-final*.log`) remain in evidence.

All commands used the native SDK `repo_1c-agent/.dotnet/dotnet.exe` (10.0.400);
exit codes captured directly (no `tee`/`tail` masking); unique TRX
`LogFilePrefix` per run.

## Root probe findings — disposition

Independent read-only probes (18 checks) were copied unchanged into
`tests/ErpOnecAgent.IntegrationTests/ReviewProbes/` and run against the draft
before any fix: exit 1, **5 failed / 13 passed** — matching the confirmed
defects:

- **A (2 fail)**: `VerifyClaimReadinessAsync` returned `Transient` on un-ACKed
  batches before the ownership gate → sealed unowned run stayed `uploading` with
  a dispatchable batch. Fixed by evaluating `RunOwnershipSetHoldsAsync` before
  the transient branch; corrupted set now blocks `OWNERSHIP_SET_MISMATCH` and
  dead-letters the pending batch in one transaction. Owned+un-ACKed still
  transient (positive control passes).
- **B (2 fail)**: the diagnostic job block cleared
  `dispatch_owner_id`/`dispatch_claimed_at_utc`; the next claim pass then saw
  the corrupt elder as inert and released its hold. Fixed: the diagnostic block
  preserves all prior-history fields, and the shared `RunInertPredicate` now
  counts non-`pending` job status, deferral fields, `claim_attempt_count`,
  dispatch diagnostics, counters, errors and finish/resolve timestamps as
  effects. The younger claim stays `ElderManifestInvalid` after the direct
  elder block.
- **D (1 fail)**: a stray injected `extracting` row for an unowned entity could
  register a batch. Fixed: `EntityOwnershipPredicate` requires epoch-bound
  membership of the touched entity in Register and Complete; stray rows
  classify `EntityNotExtracting` with zero writes.
- **C (9 pass)**: malformed/empty/null/[null]/duplicate/scalar manifests reject
  all four extraction mutations with `OwnershipSetMismatch` and zero writes;
  no raw `json_each` exception path reproduces.
- **E (root's second-round probes, 2 fail / 1 pass on the prior draft — real
  RED)**: `entities_json=[{"entityCode":"clients"}]` passed the code-only SQL
  check while failing the typed claim check (candidate AND quarantined), and
  `configuration_version` drift omitted the candidate but diagnostics ignored
  mode/config. Fixed: the eligibility query calls the same typed helper
  (`etl_job_manifest_consistent` scalar + mode/config equality), and the
  diagnostics scan and elder read now apply the identical frozen-identity rule.
  The lossy GROUP_CONCAT comparison was removed.
- **Transition integrity**: `CommitJobManifestBlockAsync` ignored affected row
  counts — a swallowed (RAISE(IGNORE)) quarantine write committed a partial
  quarantine and let the younger claim defer/commit on top. Fixed: expected-one
  pending-run transition and expected-one blockable-job transition; violations
  throw, rolling back the whole claim transaction (proved by
  `Swallowed_elder_quarantine_write_aborts_the_whole_claim_transaction`).
- **ProbeD counterpart**: `Stray_extracting_entity_row_cannot_complete_for_unowned_entity`
  proves the Complete arm of the touched-entity gate with a counter-matching
  stray row and a full zero-write snapshot.

## RED-honesty notes

- The original runtime-RED run (9/9 failures against the unchanged
  implementation) is preserved under `red/` and was not overwritten.
- `EtlOwnershipRedTests` retains the same scenarios on the new signatures:
  unclaimed/legacy-seeded runs with real persisted entity/batch/counter state
  reject `ExtractionClaimLost` with zero writes — the seeded `extracting`/`done`
  rows prove the closed hole, not a trivially-absent entity.
- Two intentional classification precedences are recorded: entity-state
  (`EntityNotExtracting`) outranks claim classification on entity completion;
  not-mutable (`RunNotRunningOrAlreadySealed`/`RunNotRunning`) outranks claim
  loss on sealed/terminal runs.
- `ReleaseOwnershipAsync` in the O1 suite is SQL fixture corruption injection
  for the released-row/epoch-ABA scenario — not the (unimplemented) attested
  manual-resolution path.

## Known limitations

- O1 is dark: nothing calls `TryClaimEtlJobAsync`/`GetDispatchableEtlJobsAsync`
  in production; workers still use legacy unowned paths (the documented §9
  bypasses). Fencing them is a separate cutover change, not this slice.
- No manual release API exists; unresolved-effect holds are permanent until a
  future attested resolution slice.
- `RecoverInterruptedEtlRunsAsync` is an explicit host-only API; it is not
  invoked by `RecoverAsync` or any startup path.
- Jobless (legacy/scheduled) run capture is rejected until O3 supplies a claim
  path for them.


## Root acceptance — 25 September 2026

Reviewed the production changes, preserved F1 assertions and real-SQLite failure evidence; the initial drafts were not accepted on their green counts alone. Additional independent probes exposed ownership/ACK ordering, erased admission-hold evidence, stray-entity mutation and incomplete pre-LIMIT identity validation. Root also reproduced the last two enumeration failures against the proposed 572-test result before the final correction.

Transferred 24 reviewed files to main. The new, previously unpublished migration 007 was normalized to LF before verification so its compiled resource bytes agree with Git (core.autocrlf=input). Migrations 001–006 stayed byte-identical to the accepted local baseline. Minor contract/comment wording was corrected to distinguish job blocking from the retained pending-run admission hold.

Independent native Windows locked restore, Release Rebuild and full tests all exited 0: **539 integration + 39 unit = 578/578**, no skips, no build warnings/errors. Source hashes stayed stable throughout the run. Evidence: `local-data/remediation-2026-09-25/etl-ownership-o1-root/` (logs, TRX, exit-codes.json).

Remaining gates: O2 send-attempt ledger and unknown-outcome handling; O3 scheduled frozen identity; source-domain/full-to-incremental policy; spool/checkpoints and legacy resolution; simultaneous retirement/fencing of all legacy writers. No service deployment or ERP/1C acceptance was performed.

A separate migration portability issue was found during Git review: existing local 002/003/005/006 files have CRLF while published Git blobs have LF, affecting byte-based ledger checksums between builds. Their bytes and existing ledgers were not rewritten in O1. Cross-checkout database compatibility and a strict historical-checksum policy are added to the remediation plan before cutover.
