-- 011_watermark_domain_resets.sql
-- Attested watermark domain reset (slice D1). 001-010 are immutable. Additive only.
--
-- watermark_domain_resets — one immutable archive row per reset. The reset removes the
-- entity's watermarks row in the SAME transaction, so the entity's next extraction sees an
-- absent base: only a full baseline (bootstrap_full/entity_reload) may establish the new
-- domain, while an incremental read is refused (BaselineRequired). This is the only exit
-- from DOMAIN_CHANGED (source namespace or definition changed, e.g. a new exportEpoch after
-- a backup restore) and from DOMAIN_UNKNOWN (a legacy row without a fingerprint).
--   * prior_* — the removed row verbatim (committed and extracting cursor text,
--     generation, fingerprint, last run, updated-at).
--   * operator_id / reason — who reset the domain and why (attested, not derived).
-- A reset is refused while the entity is owned by an active run (lifetime ownership,
-- 007), and it is CAS-guarded on the observed generation. It never touches ERP data;
-- re-sending a baseline is the operator's decision.
CREATE TABLE IF NOT EXISTS watermark_domain_resets (
    reset_id TEXT PRIMARY KEY,
    entity_name TEXT NOT NULL,
    reset_at_utc TEXT NOT NULL,
    operator_id TEXT NOT NULL,
    reason TEXT NOT NULL,
    prior_committed_cursor_json TEXT NULL,
    prior_extracting_cursor_json TEXT NULL,
    prior_generation INTEGER NOT NULL,
    prior_domain_fingerprint TEXT NULL,
    prior_last_run_id TEXT NULL,
    prior_updated_at_utc TEXT NULL,
    CHECK(length(trim(operator_id)) > 0 AND length(trim(reason)) > 0),
    CHECK(prior_generation > 0)
);
CREATE INDEX IF NOT EXISTS ix_watermark_domain_resets_entity ON watermark_domain_resets(entity_name, reset_at_utc);
