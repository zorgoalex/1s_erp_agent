# ETL batch acknowledgement — status validation

Date: 2026-09-25. Worktree: `agents/worktrees/etl-ack-status`, baseline `main28750d8` (504 tests).
Model: swe-2-high. Native Windows SDK: `D:/WORK/CNC_Milling/WORK_CNC/SOFT/1C/1C-agent/repo_1c-agent/.dotnet/dotnet.exe` (10.0.400), cwd = this worktree.

## Scope

Narrow real-worker regression + minimal fix: `EtlBatchUploadWorker` accepted any ERP batch
acknowledgement whose `batchId`/`checksumValid`/`rowsAccepted` matched, regardless of `status`.
The ERP contract (`contracts/erp-agent-api.openapi.yaml`, schema `BatchAck`) requires
`status: acknowledged` (`const`); the `BatchAcknowledgement` DTO carries `Status` as a plain
string, so `"failed"`, `"pending"`, empty and JSON `null` all deserialize and were treated as a
positive acknowledgement — bumping `batches_acknowledged`, releasing run completion and
committing the watermark for a batch ERP may have rejected.

Bounded: status validation only. No persistence/migration/F1/other-worker/HTTP-contract/global-
option changes; no ownership or remote-dedup guarantee is added or implied. The only production
file touched is `src/ErpOnecAgent.Service/Workers/EtlBatchUploadWorker.cs`.

## Test design (`tests/ErpOnecAgent.IntegrationTests/EtlBatchAckStatusTests.cs`)

Real `EtlBatchUploadWorker` + real migrated temporary SQLite
(`SqliteAgentStore.InitializeAsync`) + real `FileSpoolStore` (genuine gzip batch file + SHA-256)
+ fake `IErpClient` returning a matching id/checksum/rows acknowledgement with a scripted
`status`. Determinism: a `DispatchProxy` `IAgentStore` pass-through raises `BatchSettled` when
the worker's terminal batch write (`AcknowledgeBatchAsync` or `MarkBatchRetryAsync`) returns,
and `SecondSweep` when the second `GetPendingBatchesAsync` call begins — by construction the
entire first loop iteration (including any run-completion delivery and watermark commit) has
finished. The second sweep does NOT reach the store: it is parked on the worker's stopping token
(review fix — otherwise a delayed test continuation could let a due retry re-upload while
assertions run), and `StopAsync` unwinds the park. No sleeps mask races; all waits bounded
(5 s observe, 5 s stop). `UploadCalls==1` is asserted on both paths.

## Runtime RED (unchanged baseline / reverted guard)

Theory `Non_acknowledged_status_is_not_accepted_batch_stays_retryable`:

| status | Result | Failure |
|---|---|---|
| `"failed"` | RED | `Expected: "retry_waiting" / Actual: "acknowledged"` — invalid ack accepted |
| `"pending"` | RED | same |
| `""` | RED | same |
| `null` (as the DTO permits) | RED | same |

Fact `Acknowledged_status_completes_run_and_commits_watermark` passes on baseline (no false
positive). Result: **5 tests, 4 failed, 1 passed**.

Two RED evidences exist (identical outcome):

- **Initial** (pre-review test revision): TRX
  `D:/WORK/CNC_Milling/WORK_CNC/SOFT/1C/1C-agent/local-data/remediation-2026-09-25/etl-batch-ack-status/red/TestResults/etl-ack-status-red.trx`.
  The `dotnet test` exit code was NOT captured for that run — the console pipeline
  (`tee`/`tail`) masked it (wrapper exit 0); RED stands on TRX outcomes, not on a claimed
  exit code.
- **Review re-run** (revised tests against the temporarily reverted guard, exit code captured
  unmasked): TRX
  `D:/WORK/CNC_Milling/WORK_CNC/SOFT/1C/1C-agent/agents/worktrees/etl-ack-status/local-data/remediation-2026-09-25/etl-batch-ack-status/review/red/TestResults/review-red_net10.0_20260925012150.trx`,
  `dotnet test` exit code **1**, 4 failed / 1 passed.

## Fix

One condition added to the existing mismatch guard in `EtlBatchUploadWorker.UploadBatchAsync`
(`EtlBatchUploadWorker.cs:47`):

```csharp
if (acknowledgement.BatchId != batch.BatchId
    || !string.Equals(acknowledgement.Status, "acknowledged", StringComparison.Ordinal)
    || !acknowledgement.ChecksumValid
    || acknowledgement.RowsAccepted != batch.RowCount)
    throw new InvalidDataException("ERP ETL acknowledgement does not match the uploaded batch.");
```

- Exact-ordinal `"acknowledged"` per the contract `const`; `null`/empty/`"failed"`/`"pending"`/
  wrong-case all reject.
- The `InvalidDataException` flows into the **existing** catch → `MarkBatchRetryAsync`
  (existing retry policy, unchanged): batch returns to `retry_waiting`, `attempt_count+1`,
  persisted backoff, `last_error` recorded, spool evidence preserved, no ack counter, no run
  completion, no watermark.
- Id/checksum/row checks and the positive `"acknowledged"` path are unchanged.

## Verified post-fix assertions

Each invalid-status case asserts after the parked-sweep barrier: batch `retry_waiting`,
`attempt_count=1`, `last_error` non-empty, `batches_acknowledged=0`, run still `uploading`,
exactly one upload (`UploadCalls==1`), `CompleteEtlRunAsync` never called, committed watermark
still `NULL`, spool file present, and a future-due `GetPendingBatchesAsync` re-offers the batch
(retryable under existing policy). The positive case asserts `acknowledged` batch,
`batches_acknowledged=1`, `UploadCalls==1`, exactly one completion delivery, run `succeeded`,
committed watermark equal to the batch `watermark_to`, and `LastEtlSuccessAtUtc` set.

## Commands / exit codes / counts

`dotnet` = `D:/WORK/CNC_Milling/WORK_CNC/SOFT/1C/1C-agent/repo_1c-agent/.dotnet/dotnet.exe`
(10.0.400), cwd = worktree. Exit codes below are real process exits (no pipeline masking);
the initial baseline/RED round used `tee`/`tail` pipes whose displayed exit was the wrapper's —
those rows are marked accordingly and their results stand on the log/TRX files.

| Round | Step | Command | Exit | Result |
|---|---|---|---|---|
| initial | restore | `dotnet restore ErpOnecAgent.sln --locked-mode` | 0 | clean |
| initial | build | `dotnet build ErpOnecAgent.sln -c Release --no-restore` | 0 | 0 warn / 0 err |
| initial | full test | `dotnet test ErpOnecAgent.sln -c Release --no-build` | masked (log: pass) | 504/504 (465 int + 39 unit) |
| initial | RED | `dotnet test tests/ErpOnecAgent.IntegrationTests -c Release --no-build --filter FullyQualifiedName~EtlBatchAckStatusTests` | masked (TRX: 4 Failed / 1 Passed) | 4 failed / 1 passed |
| initial | targeted post-fix | same filter | 0 | 5/5 |
| initial | full post-fix | `dotnet test ErpOnecAgent.sln -c Release --no-build` | masked (log: pass) | 509/509 |
| review | build reverted guard | `dotnet build ErpOnecAgent.sln -c Release -t:Rebuild --no-restore` | 0 | 0 warn / 0 err |
| review | RED re-run | same filter, `LogFilePrefix=review-red` | **1** | 4 failed / 1 passed |
| review | rebuild (fix restored) | `dotnet build ErpOnecAgent.sln -c Release -t:Rebuild --no-restore` | 0 | 0 warn / 0 err, all 8 projects compiled |
| review | full test | `dotnet test ErpOnecAgent.sln -c Release --no-build`, `LogFilePrefix=review-final` | 0 | **509/509** (470 int + 39 unit) |
| review | targeted | same filter, `LogFilePrefix=review-targeted` | 0 | 5/5 |

## Evidence

- Initial round (preserved): `D:/WORK/CNC_Milling/WORK_CNC/SOFT/1C/1C-agent/local-data/remediation-2026-09-25/etl-batch-ack-status/{baseline,red,final}/` + `REPORT.md` (corrected).
- Review round (this tree): `D:/WORK/CNC_Milling/WORK_CNC/SOFT/1C/1C-agent/agents/worktrees/etl-ack-status/local-data/remediation-2026-09-25/etl-batch-ack-status/review/{red,final}/` + `REPORT.md` — real exit codes, per-assembly TRX via `LogFilePrefix`.

No commit/push; no live ERP/1C touched; no secrets involved.

## Limits / what this does not cover

- Validates the acknowledgement **status field only**. Ownership fencing of the batch row and
  remote/deduplication guarantees on the ERP side are out of scope.
- `BatchAcknowledgement.Status` remains a plain `string` in the DTO (contract/HTTP unchanged);
  enforcement lives at the worker evaluation point.
- Retry exhaustion/dead-letter policy for batches is pre-existing and unchanged.


## Root acceptance — 2026-09-25

Root reviewed the one-line production diff, real-worker/SQLite/spool test and corrected second-sweep cancellation barrier. Re-run RED evidence confirms four invalid statuses accepted by the original guard, exit 1; positive case passed. After merging only three files into main, independent locked restore + Release Rebuild passed (zero warnings/errors), full suite **470 integration + 39 unit = 509/509**, zero failures/skips, exit 0; source manifest unchanged during verification. Evidence: `local-data/remediation-2026-09-25/etl-batch-ack-status-root/` under repo_1c-agent. Accepted only as ACK-status validation; legacy retry/ownership/completion limitations remain.
