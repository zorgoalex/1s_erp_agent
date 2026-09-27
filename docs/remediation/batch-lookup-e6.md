# E6 — ERP batch lookup for R1 evidence

Date: 2026-09-28. No migration. Agreed with ERP (spec В-4; `to-onec/0001`): `GET etl/batches/{batchId}`
returns `200` with the original ACK, or `404` when ERP has not stored the batch.

## Change

- **`IErpClient.GetBatchStatusAsync(batchId)`** is read-only and uses the ordinary channel.
  - `200` gives `Stored` with the ACK. An ACK for a different `batchId` is rejected as
    `InvalidDataException`.
  - `404` gives `NotStored`.
  - Any other status throws `ErpApiException`.
  - The default interface member throws `NotSupportedException`, so the test doubles stay as
    they are.
- **`IAgentStore.GetUnacknowledgedRunBatchesAsync(runId)`** lists the run's batches that are
  neither acknowledged nor deleted, with their local status, quarantine code, number of send
  attempts and last attempt outcome.
- **CLI `--etl-check-batches <runId>`** prints one line per batch and a summary line with
  ready-made `--verification` text.
  - Exit code: `0` when ERP answered for every batch, `2` when a lookup failed.
  - It writes nothing locally and needs no instance lock.
  - The upload path never uses the lookup: an uncertain outcome still quarantines the batch,
    and nothing is resent automatically.
- **Runbook:** step 2 of the blocked/failed run procedure now uses the command.

The R1 resolution itself is unchanged: the operator still attests `--verification` and
`--workers-stopped`.

## Tests

`E6BatchLookupTests` (5):
- `200` and `404` both give an answer;
- an ACK for another batch and an error status are rejected;
- the store lists only the run's unacknowledged batches, with their last outcome;
- the CLI reports stored, not-stored and failed answers, sets exit code 2 and leaves every
  batch row unchanged;
- with nothing to check, the CLI makes no ERP call; an invalid run id is refused.
