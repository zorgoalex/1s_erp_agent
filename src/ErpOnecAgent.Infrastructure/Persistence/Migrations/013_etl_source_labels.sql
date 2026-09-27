-- 013_etl_source_labels.sql
-- E3 (agreed with ERP, agent-bridge to-erp/0003, to-onec/0009). 001-012 are immutable.
-- Additive only.
--
-- etl_runs.source_namespace — '1c-identity:v1:{databaseId}:{exportEpoch}:{environment}'
--   recorded at the run's FIRST entity Begin (the namespace the reads were fingerprinted
--   under); sent as X-Source-Namespace on every batch and as sourceIdentity in complete.
-- etl_runs.source_generation — the opaque configuration.sourceGeneration token frozen at
--   run creation; sent as X-Source-Generation and as sourceGeneration. NULL when the
--   configuration had no token then.
-- etl_runs.legacy_source_identity — 1 for runs that began reading before 013: the
--   namespace they used is unrecoverable and is never backfilled from the current binding.
--   Such runs send neither the header nor the field (ERP accepts or refuses with 409).
-- etl_runs.complete_payload_shape — 1 for bodies stored before 013 (legacy 6-property /
--   partial 8-property, replayed byte for byte), 2 for the always-v2 body.
--
-- Labels are immutable at the storage level: the namespace is set once, the generation only
-- on INSERT.

ALTER TABLE etl_runs ADD COLUMN source_namespace TEXT NULL CHECK(source_namespace IS NULL OR source_namespace LIKE '1c-identity:v1:%');
ALTER TABLE etl_runs ADD COLUMN source_generation TEXT NULL CHECK(source_generation IS NULL OR length(source_generation) BETWEEN 1 AND 128);
ALTER TABLE etl_runs ADD COLUMN legacy_source_identity INTEGER NULL CHECK(legacy_source_identity IS NULL OR legacy_source_identity = 1);
ALTER TABLE etl_runs ADD COLUMN complete_payload_shape INTEGER NULL CHECK(complete_payload_shape IS NULL OR complete_payload_shape IN (1, 2));

UPDATE etl_runs SET legacy_source_identity = 1
WHERE EXISTS (SELECT 1 FROM etl_run_entities e WHERE e.run_id = etl_runs.run_id);

UPDATE etl_runs SET complete_payload_shape = 1 WHERE complete_payload_json IS NOT NULL;

CREATE TRIGGER IF NOT EXISTS trg_etl_runs_source_labels_immutable
BEFORE UPDATE OF source_namespace, source_generation ON etl_runs
WHEN (OLD.source_namespace IS NOT NULL AND NEW.source_namespace IS NOT OLD.source_namespace)
  OR (NEW.source_generation IS NOT OLD.source_generation)
BEGIN
    SELECT RAISE(ABORT, 'etl_runs source labels are immutable');
END;
