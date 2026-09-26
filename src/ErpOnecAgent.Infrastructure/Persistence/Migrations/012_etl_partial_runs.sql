-- 012_etl_partial_runs.sql
-- Partial ETL runs (user decision 2026-09-26: skip a failed entity, continue the others).
-- 001-011 are immutable. Additive only.
--
-- etl_run_entities.failure_code / failure_message / failed_at_utc — an entity that failed
-- at the source (OData read, cursor) or at Begin (domain changed/unknown, baseline
-- required) is marked 'failed' WITH a failure code; only such rows may be sealed. A
-- 'failed' row without a code (run termination, interruption) still blocks the seal.
-- The failed entity's watermark is never committed, so the next run retries it.
--
-- etl_runs.sealed_failed_entity_count — frozen at seal; NULL on runs sealed before 012
-- means zero.

ALTER TABLE etl_run_entities ADD COLUMN failure_code TEXT NULL CHECK(failure_code IS NULL OR length(trim(failure_code)) > 0);
ALTER TABLE etl_run_entities ADD COLUMN failure_message TEXT NULL;
ALTER TABLE etl_run_entities ADD COLUMN failed_at_utc TEXT NULL;
ALTER TABLE etl_runs ADD COLUMN sealed_failed_entity_count INTEGER NULL CHECK(sealed_failed_entity_count IS NULL OR sealed_failed_entity_count >= 0);
