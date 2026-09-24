-- 005_durable_etl_jobs.sql
-- Durable manual ETL job acceptance storage (bounded storage foundation).
-- 001-004 are immutable (checksums 1-4). Additive only: a new etl_jobs table and
-- nullable/defaulted etl_runs columns; every existing row keeps its data.
--
-- etl_jobs — one row per accepted manual ETL request:
--   * command_id is the manual command identity: one durable job per command ever
--     (NOT NULL, UNIQUE). There is deliberately NO foreign key to commands_inbox:
--     the job outlives normal command cleanup, while CleanupAsync keeps
--     inbox/attempts/result/outbox rows a non-terminal job still references.
--   * run_id is the stable run identity fixed at acceptance (UNIQUE), referencing
--     the associated pending etl_runs row created in the same transaction.
--   * mode is bounded to the two authorized manual modes; reconcile modes are not
--     storable here and can never masquerade as a full/entity reload job.
--   * entities_json freezes the fully resolved entity definitions and
--     configuration_version freezes the config version they were resolved against;
--     a later configuration change never alters an accepted job.
--   * command_payload_hash is the canonical hash read from the saved inbox row and
--     acceptance_result_json is the exact stored acceptance result; both are
--     immutable acceptance evidence so a repeated commandId replays the original
--     result identity even if the inbox row is later gone.
--   * Only 'pending' is produced in this stage (no dispatch, claims, extraction or
--     completion exist yet); the CHECK bounds the documented job lifecycle domain.
--
-- etl_runs additive pending-run metadata:
--   * configuration_version — frozen config version of the accepted run.
--   * created_at_utc — acceptance time; existing rows backfill from started_at_utc.
--   * updated_at_utc — last durable run-row update; deterministic backfill.
--   * row_version — optimistic-concurrency counter, project convention.
-- started_at_utc stays NULL while a run is pending; claim-time resolved bounds are
-- later-stage columns and are intentionally not added here.
CREATE TABLE IF NOT EXISTS etl_jobs (
    job_id TEXT PRIMARY KEY,
    command_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    mode TEXT NOT NULL,
    entities_json TEXT NOT NULL,
    configuration_version INTEGER NOT NULL,
    status TEXT NOT NULL,
    command_payload_hash TEXT NOT NULL,
    acceptance_result_json TEXT NOT NULL,
    created_at_utc TEXT NOT NULL,
    updated_at_utc TEXT NOT NULL,
    row_version INTEGER NOT NULL DEFAULT 1,
    UNIQUE(command_id),
    UNIQUE(run_id),
    CHECK(mode IN ('bootstrap_full','entity_reload')),
    CHECK(status IN ('pending','deferred','running','blocked','finished','cancelled')),
    FOREIGN KEY(run_id) REFERENCES etl_runs(run_id)
);
CREATE INDEX IF NOT EXISTS ix_etl_jobs_pending ON etl_jobs(status, created_at_utc, job_id);

ALTER TABLE etl_runs ADD COLUMN configuration_version INTEGER NULL;
ALTER TABLE etl_runs ADD COLUMN created_at_utc TEXT NULL;
ALTER TABLE etl_runs ADD COLUMN updated_at_utc TEXT NULL;
ALTER TABLE etl_runs ADD COLUMN row_version INTEGER NOT NULL DEFAULT 1;

UPDATE etl_runs
SET created_at_utc = started_at_utc
WHERE created_at_utc IS NULL AND started_at_utc IS NOT NULL;

UPDATE etl_runs
SET updated_at_utc = COALESCE(finished_at_utc, started_at_utc)
WHERE updated_at_utc IS NULL;
