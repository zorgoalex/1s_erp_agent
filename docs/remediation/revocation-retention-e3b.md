# E3b — retention and revocation of sensitive entities

Date: 2026-09-28. No migration. For `counterparty_phones`: agreed over agent-bridge
(`to-erp/0031` proposal, `to-onec/0030` ERP decision: all three points, a blocked run instead
of a partial completion).

## Change

1. **`deleteBatchAfterAck`** (entity field, remote and `appsettings`, default `false`).
   - For such an entity the upload worker deletes the batch file right after ERP's ACK
     (`ETL_BATCH_FILE_DELETED_AFTER_ACK`), instead of after `AcknowledgedBatchRetentionDays`.
   - The batch row stays `acknowledged` with its metadata (ids, row count, hashes). Finalize
     and R1 read rows, never files.
   - The decision comes from the run's **frozen** definition (`etl_run_entities`), never from
     the live configuration.
   - `false` is not serialized: existing definition JSON is byte-identical.
   - The flag is also **excluded from the domain fingerprint**
     (`EtlDomainFingerprint.RetentionProperties`), so toggling it never forces a new baseline.
2. **Revocation through configuration** — `EtlRetentionWorker`, a sweep once a minute.
   - It acts only under an ERP configuration (version > 0). The local `appsettings` list is
     never grounds for revocation.
   - Every unfinished run (`pending`/`running`/`paused`/`uploading`/`completing`) is checked.
     If its frozen entities include a sensitive entity that the active configuration no longer
     lists, the run is blocked `ENTITY_REVOKED` in one transaction:
     - its pending batches are fenced `dead_letter`/`RUN_BLOCKED`;
     - the job is blocked;
     - ownership is retained for R1 (`BlockRunsWithRevokedEntitiesAsync`).
   - Then **every** remaining file of that entity is deleted, whatever the batch status.
     Runs are blocked first, so no path can still need them.
   - The same sweep catches up the file of an acknowledged sensitive batch that the uploader
     could not delete, for example after a crash or a sharing violation.
   - The log carries counts only (`ETL_SENSITIVE_FILES_DELETED`).
3. **`409 ENTITY_REVOKED` on a batch** (revocation raced a send). `FailClaimedBatchSendAsync`
   takes a `refusalCode`:
   - the attempt is `rejected_ack` with `last_error` `ENTITY_REVOKED: …`;
   - the batch is `dead_letter`/`RUN_BLOCKED`;
   - the run is blocked `ENTITY_REVOKED`.

   There is no resend. The file is deleted at once (`ETL_BATCH_FILE_DELETED_REVOKED`). Other
   409/422 codes stay `UPLOAD_OUTCOME_UNKNOWN`, and so does `ENTITY_REVOKED` on a status other
   than 409.

OpenAPI 1.5.0: `deleteBatchAfterAck`, and `ENTITY_REVOKED` on `POST etl/batches`.

## Edge cases

- **A file is open by an in-flight upload when the sweep deletes it.** Windows refuses the
  delete (`ETL_BATCH_FILE_DELETE_FAILED`), and the next sweep retries it. The upload's outcome
  lands on the dead fence of the blocked run: evidence only, never a resend.
- **Extraction is under way when the run is blocked.** The extraction claim is cleared, so the
  next batch registration fails. A file already written but not registered is quarantined by
  the startup reconciliation (C1) and is not deleted by the sweep. ERP does not allow a
  revocation while batches are pending (`etlBatchesPending > 0`), so this window is narrow.

## Tests

- **`EtlC1ReviewFixTests`** (integration, 5 new, 2 new theory rows):
  - the sensitive file is deleted after the ACK, its row stays, and an ordinary entity keeps
    its file;
  - `ENTITY_REVOKED`: the run is blocked, the attempt is `rejected_ack`, the file is deleted and
    nothing is resent; the sibling file goes at the next sweep;
  - revocation blocks both the sealed and the pending run that include the entity, deletes the
    file and leaves other runs alone; a second sweep does nothing;
  - the local configuration (version 0) never revokes, and a still-configured ready batch keeps
    its file;
  - the sweep catches up an acknowledged file left behind;
  - `409 BATCH_CONFLICT` and `422 ENTITY_REVOKED` are still `UPLOAD_OUTCOME_UNKNOWN`.
- **`EtlEntityFilterTests`** (unit): the flag is serialized only when set, round-trips and
  keeps the fingerprint.
- **`RemoteEntityNamingTests`:** the remote flag binds (`true`/`false`/absent), and so does the
  `appsettings` flag.
