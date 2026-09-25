-- 009_etl_scheduled_runs.sql
-- Scheduled ETL runs with frozen identity and dedup (bounded O3 slice of the ETL
-- ownership + owner-fenced upload design, revision 3, §3.3/§4.4).
-- 001-008 are immutable (checksums 1-8). Additive only: two nullable etl_runs
-- columns and one partial unique index; every existing row keeps its data.
--
-- etl_runs additive columns:
--   * schedule_key — the schedule a jobless run belongs to (e.g. 'incremental').
--     NULL for job runs and for legacy runs. Scheduled work never becomes an
--     etl_jobs row (etl_jobs.command_id stays NOT NULL UNIQUE).
--   * resolved_entities_json — the full frozen effective entity definitions of a
--     scheduled run, captured at creation together with configuration_version. For a
--     run without a job this is the only authority Begin compares a caller-supplied
--     definition against; names alone are never enough.
--
-- ux_etl_runs_schedule_active — at most one active or unresolved run per schedule
-- key. Fail closed: EVERY status holds the key except 'succeeded', 'cancelled', and a
-- failed/blocked run that was explicitly resolved (resolved_at_utc, 006). Active
-- states, paused/partial results, unresolved failures and any unknown status all keep
-- it, so a fault can never mint a successor queue behind unresolved work.
ALTER TABLE etl_runs ADD COLUMN schedule_key TEXT NULL;
ALTER TABLE etl_runs ADD COLUMN resolved_entities_json TEXT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS ux_etl_runs_schedule_active ON etl_runs(schedule_key)
WHERE schedule_key IS NOT NULL
  AND status NOT IN ('succeeded','cancelled')
  AND NOT (status IN ('failed','blocked') AND resolved_at_utc IS NOT NULL);
