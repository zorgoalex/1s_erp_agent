-- 010_etl_run_resolutions.sql
-- Attested manual resolution of failed/blocked ETL runs (design §8, slice R1).
-- 001-009 are immutable (checksums 1-9). Additive only: one new table.
--
-- etl_run_resolutions — one immutable record per resolved run, written in the SAME
-- transaction that sets etl_runs.resolved_at_utc, fences the run's remaining
-- pre-acknowledgement batches and releases its lifetime ownership ('manual_release').
--   * operator_id / remote_verification — who resolved the run and what they verified at
--     ERP (remote quiescence and reconciliation). The store records the attestation; it
--     never derives or invents ERP proof.
--   * workers_quiesced — the operator's attestation that dispatchers and upload workers
--     were stopped AND drained under the exclusive-maintenance procedure. The store checks
--     only the durable preconditions (no admitted send attempt, no in-flight batch, no live
--     extraction/completion/dispatch fence); it cannot observe processes.
--   * decision — what may happen next: 'abandon' (nothing is retried), 'retry' (a new
--     command/job or schedule tick may redo the work from the committed watermarks), or
--     'rebaseline' (the next run for these entities must be a new full baseline).
--   * prior_status / prior_conflict_code — the run's terminal status and the
--     finalize_conflict_code it was blocked with (NULL when none), kept for audit.
--   * ownership_released / batches_fenced — exact counts written by the resolution commit.
-- Limit (until the §9 legacy bypasses are fenced at cutover): a legacy batch taken by the
-- ledger-less GetPendingBatchesAsync path is dead-lettered when its run blocks, so a
-- possibly running legacy POST is covered only by the workers_quiesced attestation.
-- Resolution never commits watermarks and never marks a job 'finished'; original run,
-- batch, attempt and job evidence stays immutable.
CREATE TABLE IF NOT EXISTS etl_run_resolutions (
    run_id TEXT PRIMARY KEY,
    resolution_id TEXT NOT NULL UNIQUE,
    resolved_at_utc TEXT NOT NULL,
    operator_id TEXT NOT NULL,
    decision TEXT NOT NULL,
    remote_verification TEXT NOT NULL,
    workers_quiesced INTEGER NOT NULL,
    prior_status TEXT NOT NULL,
    prior_conflict_code TEXT NULL,
    ownership_released INTEGER NOT NULL,
    batches_fenced INTEGER NOT NULL,
    CHECK(decision IN ('abandon','retry','rebaseline')),
    CHECK(workers_quiesced = 1),
    CHECK(prior_status IN ('failed','blocked')),
    CHECK(length(trim(operator_id)) > 0 AND length(trim(remote_verification)) > 0),
    CHECK(ownership_released >= 0 AND batches_fenced >= 0),
    FOREIGN KEY(run_id) REFERENCES etl_runs(run_id)
);
