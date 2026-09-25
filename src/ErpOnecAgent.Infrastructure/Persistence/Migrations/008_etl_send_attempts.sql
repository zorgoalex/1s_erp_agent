-- 008_etl_send_attempts.sql
-- Durable per-batch send-attempt ledger + batch send-fence/bound/quarantine
-- columns (bounded O2 slice of the ETL ownership + owner-fenced upload design,
-- revision 3; orchestrator decisions D1-D3 approved 2026-09-25).
-- 001-007 are immutable (checksums 1-7). Additive only: one new table plus
-- nullable/defaulted columns on etl_batches; every existing row keeps its data.
--
-- etl_batch_send_attempts — durable admitted-attempt ledger (design §3.2):
--   * attempt_id is the send-attempt execution-fence identity — a fresh GUID
--     minted inside the claim commit; owner_id is dispatcher diagnostics only
--     and is never the fence.
--   * 'admitted' is the only pre-network state: it proves a send was ADMITTED
--     under this identity, never that bytes left the process. 'precheck_failed'
--     is the trusted worker attestation that the network call was never invoked
--     — the only outcome a bounded NEW attempt may follow. 'acknowledged',
--     'rejected_ack', 'unknown' and 'orphaned' are terminal: an attempt is never
--     re-armed, and an uncertain outcome is never auto-replayed under unproven
--     ERP dedup.
--   * ack_payload_hash + ack_observed_at_utc + ack_valid carry the FIRST recorded
--     ACK observation (apply or matched-late evidence); ack_valid is the result of
--     the store's field validation of that body (1 valid, 0 invalid). An exact
--     replay preserves it and a conflicting later observation is reported
--     explicitly — never an in-place overwrite of evidence.
--   * Re-admission is decided by the LEDGER, not the batch status: a batch may be
--     claimed again only while every prior attempt is 'precheck_failed'. A batch
--     made due again by any other writer after an admitted/uncertain/terminal
--     attempt is quarantined, never re-sent.
--   * UNIQUE(batch_id,attempt_no) orders admissions per batch; the partial
--     UNIQUE index enforces at most one live 'admitted' attempt per batch at the
--     storage level.
CREATE TABLE IF NOT EXISTS etl_batch_send_attempts (
    attempt_id TEXT PRIMARY KEY,
    batch_id TEXT NOT NULL,
    attempt_no INTEGER NOT NULL,
    owner_id TEXT NOT NULL,
    admitted_at_utc TEXT NOT NULL,
    finished_at_utc TEXT NULL,
    outcome TEXT NOT NULL,
    http_status INTEGER NULL,
    ack_payload_hash TEXT NULL,
    ack_observed_at_utc TEXT NULL,
    ack_valid INTEGER NULL,
    last_error TEXT NULL,
    UNIQUE(batch_id, attempt_no),
    CHECK(attempt_no > 0),
    CHECK(ack_valid IS NULL OR ack_valid IN (0,1)),
    CHECK(outcome IN ('admitted','precheck_failed','acknowledged','rejected_ack','unknown','orphaned')),
    FOREIGN KEY(batch_id) REFERENCES etl_batches(batch_id)
);
CREATE INDEX IF NOT EXISTS ix_etl_batch_send_attempts_batch ON etl_batch_send_attempts(batch_id);
CREATE UNIQUE INDEX IF NOT EXISTS ux_etl_batch_send_attempts_admitted ON etl_batch_send_attempts(batch_id) WHERE outcome='admitted';

-- etl_batches additive send-fence/bound/quarantine columns:
--   * send_attempt_id — the admitted attempt the 'uploading' batch is fenced to
--     (== etl_batch_send_attempts.attempt_id); retained afterwards purely as the
--     evidence pointer of the last live attempt. Never cleared in place.
--   * upload_max_attempts — durable bound on ADMITTED sends (every ledger row
--     starts 'admitted'), persisted by the first claim; every later claim must
--     present the identical policy limit — identical-value enforcement, exactly
--     like etl_runs.completion_max_attempts (D1).
--   * quarantine_code — stable machine code on a 'dead_letter' row (D3):
--     UPLOAD_OUTCOME_UNKNOWN | ACK_INVALID | UPLOAD_ATTEMPTS_EXHAUSTED |
--     RUN_BLOCKED | RUN_FAILED | SEND_LEDGER_LOST. Every dead_letter written by the
--     new path carries a code; NULL on pre-008 rows means legacy/unknown — never
--     backfilled.
--     last_error stays human/diagnostic text; no new batch status value exists.
--   * row_version — optimistic-concurrency counter, project convention.
ALTER TABLE etl_batches ADD COLUMN send_attempt_id TEXT NULL;
ALTER TABLE etl_batches ADD COLUMN upload_max_attempts INTEGER NULL;
ALTER TABLE etl_batches ADD COLUMN quarantine_code TEXT NULL
    CHECK(quarantine_code IS NULL OR quarantine_code IN ('UPLOAD_OUTCOME_UNKNOWN','ACK_INVALID','UPLOAD_ATTEMPTS_EXHAUSTED','RUN_BLOCKED','RUN_FAILED','SEND_LEDGER_LOST'));
ALTER TABLE etl_batches ADD COLUMN row_version INTEGER NOT NULL DEFAULT 1;
