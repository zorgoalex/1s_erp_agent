-- 007_etl_ownership.sql
-- Durable per-entity lifetime ownership + immutable per-run ownership bindings +
-- the run-level extraction execution fence + etl_jobs dispatch/deferral columns
-- (bounded O1 slice of the ETL ownership design, revision 3).
-- 001-006 are immutable (checksums 1-6). Additive only: two new tables plus
-- nullable/defaulted columns on etl_jobs and etl_runs; every existing row keeps
-- its data.
--
-- etl_entity_ownership — LIFETIME ownership, NOT a lease: at most one owner per
-- entity, ever. Acquired atomically for the run's entire manifest at claim time;
-- held through extraction, upload, completing, failure and restart; released only
-- inside the successful finalize commit ('finalized') or an attested manual
-- resolution commit ('manual_release', reserved — no release API exists in O1).
--   * ownership_epoch is +1 on every (re)acquisition — a positive ABA token; a
--     released row is re-acquirable only through the released_at_utc IS NOT NULL
--     guarded update, and an active row can never be taken over. There is no TTL,
--     no expiry predicate and no stale-takeover path — ever.
CREATE TABLE IF NOT EXISTS etl_entity_ownership (
    entity_name TEXT PRIMARY KEY,
    owner_run_id TEXT NOT NULL,
    owner_job_id TEXT NULL,
    ownership_epoch INTEGER NOT NULL,
    acquired_at_utc TEXT NOT NULL,
    released_at_utc TEXT NULL,
    release_reason TEXT NULL,
    updated_at_utc TEXT NOT NULL,
    row_version INTEGER NOT NULL DEFAULT 1,
    CHECK(ownership_epoch > 0),
    CHECK(release_reason IS NULL OR release_reason IN ('finalized','manual_release')),
    FOREIGN KEY(owner_run_id) REFERENCES etl_runs(run_id),
    FOREIGN KEY(owner_job_id) REFERENCES etl_jobs(job_id)
);
CREATE INDEX IF NOT EXISTS ix_etl_entity_ownership_owner ON etl_entity_ownership(owner_run_id, released_at_utc);

-- etl_run_ownership_bindings — immutable per-run/entity record of the exact epoch
-- the claim committed. Written inside the same claim transaction that acquires the
-- ownership row and immutable thereafter: no UPDATE path exists, a binding mismatch
-- is evidence and is never repaired in place. expected_epoch is always positive
-- (insert=1, reacquire=old+1, read back in-transaction — never assumed). Every
-- ownership gate checks owner_run_id AND released_at_utc IS NULL AND
-- ownership_epoch = bindings.expected_epoch, so a released-then-reacquired row
-- (epoch moved) fails closed even for the same owner_run_id.
CREATE TABLE IF NOT EXISTS etl_run_ownership_bindings (
    run_id TEXT NOT NULL,
    entity_name TEXT NOT NULL,
    expected_epoch INTEGER NOT NULL,
    acquired_at_utc TEXT NOT NULL,
    PRIMARY KEY(run_id, entity_name),
    CHECK(expected_epoch > 0),
    FOREIGN KEY(run_id) REFERENCES etl_runs(run_id)
);

-- etl_jobs additive dispatch/deferral columns:
--   * dispatch_owner_id / dispatch_claimed_at_utc — diagnostics of the claiming
--     pass only (cleared on deferral); never a fence.
--   * claim_attempt_count — COMMITTED claims only (deferrals never count).
--   * deferral_code / deferral_message — 'busy_entity' | 'queued_overlap' |
--     'elder_manifest_invalid' plus a human-readable diagnostic.
--   * available_at_utc — next dispatch evaluation for a 'deferred' job.
ALTER TABLE etl_jobs ADD COLUMN dispatch_owner_id TEXT NULL;
ALTER TABLE etl_jobs ADD COLUMN dispatch_claimed_at_utc TEXT NULL;
ALTER TABLE etl_jobs ADD COLUMN claim_attempt_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE etl_jobs ADD COLUMN deferral_code TEXT NULL;
ALTER TABLE etl_jobs ADD COLUMN deferral_message TEXT NULL;
ALTER TABLE etl_jobs ADD COLUMN available_at_utc TEXT NULL;

-- etl_runs additive extraction execution fence (run-level so jobless/scheduled
-- runs reuse the identical mechanism in O3):
--   * extraction_claim_id — a fresh GUID minted by a committed run claim; REQUIRED
--     by every extraction mutation API. It is the fence identity, never the
--     caller's owner string. Cleared at seal, fail/block and dead-process
--     recovery — never by ownership release.
--   * extraction_claim_owner_id / extraction_claim_acquired_at_utc — diagnostics
--     only, never the fence.
ALTER TABLE etl_runs ADD COLUMN extraction_claim_id TEXT NULL;
ALTER TABLE etl_runs ADD COLUMN extraction_claim_owner_id TEXT NULL;
ALTER TABLE etl_runs ADD COLUMN extraction_claim_acquired_at_utc TEXT NULL;
