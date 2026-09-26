# Partial ETL runs — skip a failed entity, continue the others

Date: 2026-09-26. Migration **012** (`012_etl_partial_runs.sql`, additive, LF checksum
`7A597CDA…`); schema version 12. User decision: "skip the failed entity and continue the
others, but a log is mandatory".

## Behaviour

- **Entity-level failures** are failures in reading the entity from 1C or in building its
  cursor. The entity is marked `failed` with a code, and the run continues with the next
  entity:

  | Code | Cause |
  |---|---|
  | `ODATA_HTTP_<status>`, `ODATA_TRANSPORT`, `ODATA_TIMEOUT` | the OData request failed after the A10c page retries |
  | `ODATA_LIMIT` | an A10 size limit was hit |
  | `ODATA_JSON` | the response is not valid JSON |
  | `ODATA_CURSOR` | the cursor could not be built from a row |
  | `ODATA_READ` | any other read error |
  | `DOMAIN_CHANGED`, `DOMAIN_UNKNOWN`, `BASELINE_REQUIRED` | the entity was refused at Begin |

  In every case:
  - the failed entity's watermark is not committed, so the next run retries it
    automatically from its previous cursor;
  - batches it registered before failing are still uploaded and acknowledged, because they
    may already be in flight;
  - its ownership is released at finalize together with the others.
- **Run-level failures** stay as before: spool, disk reserve, spool limit, store refusals,
  cancellation and the source-identity after-check block or fail the whole run.
- **All entities failed:** the run fails (`ALL_ENTITIES_FAILED`) and needs R1, as before.
- **Completion payload.**
  - A run with a skipped entity sends `status:"partial_success"` (as in CD-ETL-3 / CD-P-1),
    `entitiesFailed` and `entities[]`.
  - `entities[]` is sorted by entity name, one element per entity:
    `{entity, status: done|failed, rowsRead, batchesCreated, errorCode, errorMessage}`.
    `errorMessage` is at most 512 characters, with any URL query removed.
  - A run with no failed entity sends the unchanged 6-property body.
  - The stored body is validated exactly and replayed byte-identically (H1).
- **Run status.** The run is `succeeded` and the job `finished`. No new status was added,
  so schedule keys, retention and R1 are unchanged.
- **Logs:**

  | Event | Level | Content |
  |---|---|---|
  | `ETL_ENTITY_FAILED` | Error | run, entity, code, message, batches and rows already registered |
  | `ETL_ENTITY_FAILURE_REFUSED` | Warning | the run is then blocked `ENTITY_FAILURE_REFUSED` |
  | `ETL_RUN_SEALED` | Information | done and failed counts, failed entity names |
  | `ETL_RUN_PARTIAL` | Warning | number and names of failed entities |
  | `ETL_RUN_FAILED` | Error | `Code=ALL_ENTITIES_FAILED` |
- **Health.** `QueueMetrics.EtlEntitiesFailing` counts entities that were skipped in their
  latest finalized run. While it is above zero, health is `degraded` with reason
  `ETL_ENTITIES_FAILING`, so an entity that fails on every run is not silent.

## Store

- `FailEtlEntityExtractionAsync` runs under the extraction fence. It freezes the entity's
  expected batch count at the number of batches already registered.
- Begin records its domain and baseline refusals as failed rows with codes. Before this
  change, BaselineRequired wrote nothing.
- Seal and readiness accept a `failed` row only if it has a failure code (`IsSkippedFailure`).
  A run-termination or recovery `failed` row never has a code, so it still blocks.
- The seal writes `sealed_failed_entity_count`. The CAS loop in finalize skips failed
  entities. The outcome is `Finalized(EntitiesFailed, FailedEntities)`.

## Review

An independent review found no must-fix issues. It confirmed:
- no finalize happens while any batch is unacknowledged or dead-lettered;
- the ownership release count is unchanged;
- a skipped row cannot be spoofed;
- payload determinism holds;
- the Begin change is safe for the O3 filter, R1, retention and the D1 reset.

Its should-fix items are applied:
- `partial_success` instead of `partial`;
- the failing-entity health signal;
- entity names in `ETL_RUN_PARTIAL`;
- updated interface documentation.

## Tests

- **`EtlPartialRunsTests`** (Devin, 25 tests, S1–S13, all pass with no production defect
  found) covers:
  - the migration;
  - a one-of-three failure end to end;
  - failure after registered batches;
  - failure-code mapping;
  - run-level spool limit;
  - all entities failed;
  - scheduled retry from the old cursor;
  - fence;
  - seal validation;
  - byte-identical replay and tamper detection;
  - domain rejection skip;
  - recovery;
  - health.
- **`HealthMetricsTests`:** the failing-entity metric.
- **Adapted tests:**
  - migration counts and the checksum catalog now go up to 12;
  - migration upgrade snapshots use an explicit pre-012 column list for `etl_run_entities`;
  - D1 and O3 BaselineRequired tests now expect a failed row with a code instead of
    "zero writes".
