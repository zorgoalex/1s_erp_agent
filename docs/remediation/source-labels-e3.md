# E3 — source identity, generation token, always-v2 completion

Date: 2026-09-28. Migration 013, schema v13. Agreed with ERP over agent-bridge:
`to-erp/0003`, `0004`, `0007`; `to-onec/0005`, `0009`, `0010`. Design:
`agents/runs/2026-09-27/e3-design.md`.

## What changed

- **Migration `013_etl_source_labels.sql`** adds four columns to `etl_runs`:
  - `source_namespace`: the namespace used at the run's first entity Begin;
  - `source_generation`: the ERP token frozen when the run is created;
  - `legacy_source_identity`: set to 1 for runs that began reading before 013;
  - `complete_payload_shape`: 1 for bodies stored before 013, 2 for v2.

  A trigger makes both labels immutable: the namespace can be set once, and the generation
  only on insert.
- **Generation token**
  - `configuration.sourceGeneration` is opaque to the agent. It must be 1..128 printable
    ASCII characters without spaces, because it is sent as a header.
  - It is frozen from one configuration snapshot when a run is created, both for
    `start_full_sync` / `reload_entity` acceptance and for scheduled runs. A later
    configuration never relabels an existing run.
  - If a restored configuration carries an unusable token, the token is dropped with
    `CONFIG_SOURCE_GENERATION_IGNORED` and the service still starts.
- **Namespace**
  - Begin accepts only the canonical `1c-identity:v1:{databaseId}:{exportEpoch}:{test|production}`
    (`SourceNamespaceInvalid`).
  - The first Begin records it, including when that first entity is refused as
    `BASELINE_REQUIRED` or `DOMAIN_*`.
  - A later Begin with a different namespace gets `SourceNamespaceMismatch`, writes nothing,
    and blocks the run.
  - Legacy runs keep pre-E3 behaviour: nothing is recorded or compared.
  - Seal fails closed when a post-013 run with entity rows has no namespace.
- **Batches.** `X-Source-Namespace` and `X-Source-Generation` are sent, each only when set.
  They go through a new `IErpClient.UploadBatchWithEvidenceAsync(batch, labels, …)` overload.
  The 3-argument call sends no labels.
- **`complete` is always v2** (shape 2). The root has, in order: `runId`, `status`
  (`succeeded` | `partial_success`), `mode`, `sourceIdentity`, `sourceGeneration`, `rowsRead`,
  `batchesCreated`, `batchesAcknowledged`, `completedAtUtc`, `entitiesFailed`, `entities[]`.
  - `sourceIdentity` and `sourceGeneration` appear only when the run has them.
  - Each entity has `entity`, `status`, `readScope`, `rowsRead`, `batchesCreated`, `errorCode`
    and `errorMessage`.
  - `readScope` is `full` when the mode is not incremental or the entity has no
    `updatedAtField`, and `delta` otherwise. It describes the query, not verified
    completeness. Per the 28.09 decision, ERP only diagnoses missing keys and never deletes.
  - The validator dispatches on the shape. Shape 1 bodies replay byte for byte, as before.
- **`session/start` and heartbeat** carry `sourceIdentity`. It is `null` without a valid
  binding, which ERP treats as absent.
- The capability `etl.source-labels.v1` is added.
- **`MaxConcurrentBatchUploads` default 2 → 1** (agreed; also in `appsettings.json`). Include
  this in the release note.

## Upgrade behaviour (confirmed by ERP, `to-onec/0009`, `0010`)

| Run | Batch headers | `complete` |
|---|---|---|
| Began reading before 013 (legacy) | none | v2, without identity and generation |
| Created after 013, before ERP sent a token | namespace only | v2 with identity |
| Created with a token | namespace and generation | v2 with both |

ERP accepts all three when its source is `bound`. Otherwise it answers `409`, which blocks the
run without retries; resolution goes through R1.

## Review

An independent review found no must-fix issues.

Applied should-fix items:
- the restored-token fallback, so the service is not stopped by an old configuration;
- the mixed case, namespace without a token, confirmed with ERP.

Nits left as they are:
- the trigger does not protect `legacy_source_identity` or `complete_payload_shape`; only
  out-of-band tampering can change them, and a changed shape blocks the run as
  `SEAL_VIOLATED`;
- a "full" read still bounds `updatedAtField` by the SafetyLag upper bound.

## Tests

- **`EtlFinalizeStorageTests.E3_*`** (29):
  - namespace recording, mismatch and zero writes;
  - non-canonical namespaces;
  - the first-entity refusal;
  - legacy runs;
  - trigger immutability and the CHECKs;
  - seal failing closed;
  - the exact v2 body with identity;
  - a scheduled incremental run with a frozen token, `delta` and `full` scopes;
  - due batches carrying labels;
  - invalid tokens;
  - eight shape and tamper cases blocking `SEAL_VIOLATED` on reclaim;
  - byte-for-byte replay of a pre-013 body.
- **`A07ModeStateTests.E3_*`** (8): bootstrap drops an unusable restored token; token
  validation and publication; upload concurrency is 1.
- **`E3SourceLabelsWireTests`** (7):
  - batch headers present or absent;
  - the 3-argument upload sends no labels;
  - the default interface overload reaches a 3-argument test double;
  - `session/start` identity and capability, and `null` without a binding.
- **Existing tests:**
  - namespace constants made canonical;
  - migration bookkeeping moved from v12 to v13.
