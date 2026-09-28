-- 015_etl_snapshot_at.sql
-- E3a (agreed with ERP, agent-bridge to-onec/0022, to-erp/0025). 001-014 are immutable. Additive only.
--
-- etl_run_entities.snapshot_at_utc — when the agent started reading the entity from 1C, written
--   once at Begin for an entity that proceeds to extraction; sent in complete entities[] as
--   snapshotAtUtc. NULL for rows written before 015 and for entities refused at Begin.
-- etl_entity_snapshot_clock — the last snapshot time issued per entity. A new one is
--   max(now, last + 1 ms), so a later read of the same entity always carries a strictly greater
--   snapshotAtUtc even if the host clock steps back (ERP replaces a snapshot only with a newer one).

ALTER TABLE etl_run_entities ADD COLUMN snapshot_at_utc TEXT NULL;

CREATE TABLE IF NOT EXISTS etl_entity_snapshot_clock (
    entity_name TEXT PRIMARY KEY,
    last_snapshot_at_utc TEXT NOT NULL
);
