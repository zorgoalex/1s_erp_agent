# A05a · ETL retention + completion readiness

Date: 2026-09-24. Audit item: A05 (P1) of `spec_1c-agent/reviews/architecture-audit-2026-09-23.md`; ETL design root review disposition of `docs/remediation/etl-durable-design.md` (bounded slice only).

## Requirement and defect

`GetAcknowledgedBatchesAsync` selected every aged `acknowledged` batch for spool deletion without checking the parent run. During a long outage of the ERP run-completion endpoint, `MaintenanceWorker` deleted the file and marked the row `deleted`; that row then blocked `GetRunsReadyToCompleteAsync` (its `NOT EXISTS ... status<>'acknowledged'` predicate) until `CleanupAsync` purged it — after which the run completed with watermarks reconstructed from nothing. Readiness itself was also negative ("no non-acknowledged batch") and checked counters, the requested-entity manifest, entity coverage, and batch-row presence in no way; the readiness read and the watermark read were separate statements with no shared snapshot. `CompleteEtlRunAsync` could overwrite any status, including reopening `succeeded`, so retention eligibility was not monotonic.

## Bounded implementation

Production changes are limited to `SqliteAgentStore.Etl.cs` and the batch purge inside `SqliteAgentStore.CleanupAsync`:

- `GetAcknowledgedBatchesAsync` offers a batch file for deletion only when its parent run is `succeeded` (conservative: failed/partial/cancelled/unfinished runs keep evidence; their cleanup is deferred manual policy).
- `MarkBatchDeletedAsync` applies the same parent-succeeded guard independently of the selection path.
- `CleanupAsync`'s `etl_batches` purge deletes `deleted` rows only when the parent run is `succeeded`; legacy/manually marked `deleted` rows of unresolved runs are preserved. The unresolved-`etl_jobs` guards on command cleanup are unchanged.
- `GetRunsReadyToCompleteAsync` fails closed inside ONE read transaction shared by the manifest read, batch read, and watermark collection: run must be `uploading`; `batches_created` must be positive; actual batch rows must equal `batches_created` exactly; every batch must be `acknowledged` (creating/ready/uploading/retry_waiting/dead_letter/deleted all block); `batches_acknowledged` must equal `batches_created`; `requested_entities_json` must be a valid non-empty array of unique non-blank strings; the set of covered entities must equal the requested set exactly (each requested entity has >=1 batch, no unexpected entity); every batch's `watermark_to_json` must deserialize to a non-null `EtlCursor` (this is not complete semantic cursor validation). An unprovable run stays `uploading` for a later manual policy — it is never completed.
- `CompleteEtlRunAsync` never reopens a `succeeded` run, and a run may enter `succeeded` only from a non-terminal status, keeping retention eligibility monotonic. The failed-extraction path is preserved.

No migrations (001–005 verified bytewise identical), no new durable-job dispatch, no watermark worker-loop atomicity change, no recovery/lease work, no interface change.

## Runtime coverage

`tests/ErpOnecAgent.IntegrationTests/RetentionReadinessTests.cs` adds 13 test methods (25 executions) on the real migrated SQLite (`SqliteTestDatabase`), real `FileSpoolStore`, and a fake `IErpClient`:

- aged acknowledged batches of `running`, `uploading`, and `failed` runs are not offered for deletion;
- acknowledged batches of a `succeeded` run are offered, marked `deleted`, and purged (positive control);
- direct `MarkBatchDeletedAsync` refuses a batch of an unresolved run;
- `CleanupAsync` preserves legacy `deleted` rows of `uploading` and `failed` runs;
- runs with one or all batch rows missing are not ready;
- theory: a batch in `creating`/`ready`/`uploading`/`retry_waiting`/`dead_letter`/`deleted` blocks readiness;
- tampered `batches_acknowledged` / `batches_created` counters block readiness;
- missing requested entity and unexpected (stowaway) entity block readiness;
- theory: NULL / `[]` / non-array / malformed / blank / non-string / NULL-element / duplicate manifest blocks readiness;
- zero-selected-entities run is not ready (pending policy);
- multi-entity multi-batch run with an explicit zero-row acknowledged entity batch is ready;
- fake ERP `CompleteEtlRunAsync` failure keeps the run `uploading`: the acknowledged spool file is not offered and still exists; after ERP success the file is deleted, marked `deleted`, and purged;
- a `succeeded` run cannot be reopened by a later `CompleteEtlRunAsync(Failed)`.

`SqliteStoreTests.Spool_writes_atomic_gzip_ndjson_and_run_becomes_completable_after_ack` was updated to the corrected contract: it now asserts the acknowledged batch is NOT offered while the run is `uploading`, then drives the documented completion path (commit watermarks + `CompleteEtlRunAsync(Succeeded)`) and asserts the offer/mark/purge flow. No test was weakened to pass.

## Evidence

Devin evidence paths below are relative to the workspace root (`1C-agent/`), not the repository/worktree. All evidence was produced with the native Windows SDK at `D:/WORK/CNC_Milling/WORK_CNC/SOFT/1C/1C-agent/repo_1c-agent/.dotnet/dotnet.exe` from this isolated worktree.

- Runtime RED before the production edit (new suite only): 25 total, 8 passed, 17 failed, 0 skipped. Console: `local-data/remediation-2026-09-24/a05-retention-readiness/red/test.log`. TRX: `.../red/TestResults/retention-readiness-red_net10.0_20260924231611.trx`.
- Targeted GREEN: 25 passed, 0 failed, 0 skipped. Console: `local-data/remediation-2026-09-24/a05-retention-readiness/targeted/test.log`. TRX: `.../targeted/TestResults/retention-readiness-targeted_net10.0_20260924232047.trx`.
- Locked restore (`--locked-mode`), full non-incremental Release rebuild: 0 warnings, 0 errors (8 projects). Console: `local-data/remediation-2026-09-24/a05-retention-readiness/final/restore.log`, `final/rebuild.log`.
- One full solution test run: 341/341 integration and 39/39 unit — 380 passed, 0 failed, 0 skipped (checkpoint355 + 25 new). Console: `local-data/remediation-2026-09-24/a05-retention-readiness/final/full-test.log`. TRX: `final/TestResults/a05-retention-readiness-final_net10.0_20260924232314.trx` (integration) and `a05-retention-readiness-final_net10.0_20260924232333.trx` (unit).

No live ERP, 1C, external database, network endpoint, deployment, credential, certificate, service install, or commit was used.

## Remaining limitations (explicitly deferred)

- Final-cursor ordering is still the pre-existing provisional `created_at_utc` last-writer-per-entity; `created_at` ties are NOT guaranteed safe and no `MAX`-based ordering was invented. Durable final cursors per run and the definitive ordering are A05b design scope.
- ERP-complete → per-entity `CommitWatermarkAsync` → `CompleteEtlRunAsync(Succeeded)` is still a sequence of separate writes, not one SQLite transaction (A05's atomicity gap). Root disposition also requires any expected-base CAS mismatch to roll back ALL watermark changes; that CAS does not exist yet.
- `CompleteEtlRunAsync` guards only `succeeded` (no reopen, no terminal→succeeded). Other terminal-state transitions (e.g. failed→cancelled) remain unguarded; the guard is silent by design since the interface returns no outcome.
- Runs that fail the closed readiness checks stay `uploading` indefinitely; there is no automatic resolution path — blocked/degraded states await the manual policy and the A05b finalize protocol.
- `GetPendingBatchesAsync` still uploads batches of `failed` runs (root disposition item 3 — failed runs keep entity ownership while uploads are in flight; quiesce/fence not implemented here).
- Evidence retention for unresolved runs can grow SQLite/spool usage until the manual policy exists.

## Changed files

- `src/ErpOnecAgent.Infrastructure/Persistence/Sqlite/SqliteAgentStore.Etl.cs`
- `src/ErpOnecAgent.Infrastructure/Persistence/Sqlite/SqliteAgentStore.cs`
- `tests/ErpOnecAgent.IntegrationTests/RetentionReadinessTests.cs` (new)
- `tests/ErpOnecAgent.IntegrationTests/SqliteStoreTests.cs`
- `docs/remediation/a05-retention-readiness.md`


## Independent root acceptance

The orchestrator reviewed all four production/test changes and reproduced the new suite against a separate checkpoint-355 copy: 17 failed / 8 passed, runtime assertions (restore/build exit 0; test exit 1). After transfer into the main repository, a fresh Release `-t:Rebuild --no-restore` completed with zero warnings/errors and the full solution test returned exit 0: 341 integration + 39 unit = 380/380, no skips. Source hashes were stable during verification. Evidence relative to repository: `local-data/remediation-2026-09-24/a05-retention-readiness-root/`, with `red-test.log`, `green-build.log`, `green-test.log` and separate TRX files in `red/` and `green/`.

The fake ERP + real spool scenario invokes store methods directly; it does not run `EtlBatchUploadWorker` or `MaintenanceWorker`. No BackgroundService scheduling/concurrency guarantee is claimed by that test. Direct success transitions still do not prove atomic watermark commit; full A05 remains open.
