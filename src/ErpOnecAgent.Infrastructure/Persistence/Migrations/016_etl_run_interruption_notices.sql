-- 016_etl_run_interruption_notices.sql
-- A04b (agreed with ERP, agent-bridge to-onec/0049). 001-015 are immutable. Additive only.
--
-- etl_run_interruption_notices — one closing complete (v2, status partial_success, every entity
--   failed RUN_INTERRUPTED, batchesAcknowledged = the run's acknowledged batches) for a run that
--   the A04 auto-recovery resolved after ERP had acknowledged at least one of its batches. It is
--   written in the same commit as the recovery; payload_json is the exact byte-stable body sent
--   to POST etl/runs/{run_id}/complete. ERP closes the run at once instead of abandoning it after
--   24 h. 'refused' (a coded 409/422: never retried; ERP then abandons the run itself) and
--   'exhausted' are terminal like 'sent'.
CREATE TABLE IF NOT EXISTS etl_run_interruption_notices (
    run_id TEXT PRIMARY KEY,
    payload_json TEXT NOT NULL,
    status TEXT NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    next_attempt_at_utc TEXT NULL,
    last_error TEXT NULL,
    created_at_utc TEXT NOT NULL,
    updated_at_utc TEXT NOT NULL,
    CHECK(status IN ('pending','sent','refused','exhausted')),
    CHECK(attempt_count >= 0),
    FOREIGN KEY(run_id) REFERENCES etl_runs(run_id)
);
CREATE INDEX IF NOT EXISTS ix_etl_run_interruption_notices_due ON etl_run_interruption_notices(status, next_attempt_at_utc);
