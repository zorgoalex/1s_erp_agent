-- 006_etl_finalize.sql
-- Durable per-entity bases/finals, sealed extraction output, and atomic run finalize
-- (bounded F1 DARK storage slice of the A05 finalize design, revision 3).
-- 001-005 are immutable (checksums 1-5). Additive only: one new table plus
-- nullable/defaulted columns on watermarks and etl_runs; every existing row keeps
-- its data.
--
-- watermarks additive columns:
--   * generation — bumped on EVERY new-path finalize commit; it is the ABA token:
--     cursor-text equality can never prove "unchanged" (X->Y->X and legitimate
--     same-cursor commits both defeat a value CAS). NOTE: the legacy
--     CommitWatermarkAsync upsert does not bump generation, so generation CAS only
--     detects writers that bump it — the new APIs stay dark until every legacy
--     bypass is retired/fenced atomically at cutover (F1 mixed-writer limitation).
--   * domain_fingerprint — fingerprint of (source namespace + frozen entity
--     definition + entity + query mode) that produced the cursor. NULL on
--     pre-existing rows means unknown domain: fail closed, never adopt.
ALTER TABLE watermarks ADD COLUMN generation INTEGER NOT NULL DEFAULT 1;
ALTER TABLE watermarks ADD COLUMN domain_fingerprint TEXT NULL;

-- etl_run_entities — one row per entity captured for one run. expected_base_*
-- stores the RAW watermarks column values observed inside the capture transaction
-- (never a re-serialization); base_row_present distinguishes an absent row from a
-- present row with NULL values — different CAS shapes and domain dispositions.
-- final_watermark_json is written explicitly at entity completion and survives
-- batch deletion; no ORDER BY/batch_no/MAX participates in finalization.
-- status: extracting|done|failed ('pending' reserved for A04); domain_status:
-- absent|same|changed|unknown.
CREATE TABLE IF NOT EXISTS etl_run_entities (
    run_id TEXT NOT NULL,
    entity_name TEXT NOT NULL,
    entity_definition_json TEXT NOT NULL,
    domain_fingerprint TEXT NOT NULL,
    status TEXT NOT NULL,
    base_row_present INTEGER NOT NULL,
    expected_base_generation INTEGER NULL,
    expected_base_cursor_json TEXT NULL,
    expected_base_domain_fingerprint TEXT NULL,
    domain_status TEXT NOT NULL,
    watermark_from_json TEXT NULL,
    snapshot_upper_bound_json TEXT NULL,
    final_watermark_json TEXT NULL,
    expected_batch_count INTEGER NULL,
    rows_read INTEGER NOT NULL DEFAULT 0,
    batches_created INTEGER NOT NULL DEFAULT 0,
    last_error TEXT NULL,
    created_at_utc TEXT NOT NULL,
    updated_at_utc TEXT NOT NULL,
    row_version INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY(run_id, entity_name),
    CHECK(status IN ('pending','extracting','done','failed')),
    CHECK(domain_status IN ('absent','same','changed','unknown')),
    FOREIGN KEY(run_id) REFERENCES etl_runs(run_id)
);
CREATE INDEX IF NOT EXISTS ix_etl_run_entities_status ON etl_run_entities(run_id, status);

-- etl_runs additive seal/claim/finalize columns:
--   * sealed_* — extraction seal: frozen manifest counts; NULL = mutable/unsealed.
--   * completion_claim_id — NEW unpredictable GUID per successful claim; it is the
--     exact claim identity (the fence), not a timestamp. owner/acquired are
--     diagnostics only and are never used for fencing or time-based stealing.
--   * complete_payload_json — verbatim wire body, written once at first claim and
--     replayed byte-identical on every reclaim.
--   * completion_max_attempts — the durable completion-attempt bound, persisted by
--     the first successful claim; every later claim/retry must present the identical
--     policy limit and claim admission refuses once completion_attempt_count reaches
--     it, so crash+recovery cycles can never mint unbounded completion sends.
--   * completion_acknowledged_at_utc — evidence a completion was finalized after
--     the ERP call; diagnostic, never a retention/cleanup key.
--   * finalize_conflict_* / resolved_at_utc — conflict diagnostics and the manual
--     resolution marker (NULL = unresolved).
ALTER TABLE etl_runs ADD COLUMN sealed_at_utc TEXT NULL;
ALTER TABLE etl_runs ADD COLUMN sealed_entity_count INTEGER NULL;
ALTER TABLE etl_runs ADD COLUMN sealed_expected_batch_count INTEGER NULL;
ALTER TABLE etl_runs ADD COLUMN completion_claim_id TEXT NULL;
ALTER TABLE etl_runs ADD COLUMN completion_claim_owner_id TEXT NULL;
ALTER TABLE etl_runs ADD COLUMN completion_claim_acquired_at_utc TEXT NULL;
ALTER TABLE etl_runs ADD COLUMN completion_attempt_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE etl_runs ADD COLUMN completion_max_attempts INTEGER NULL;
ALTER TABLE etl_runs ADD COLUMN next_completion_attempt_at_utc TEXT NULL;
ALTER TABLE etl_runs ADD COLUMN complete_payload_json TEXT NULL;
ALTER TABLE etl_runs ADD COLUMN completion_acknowledged_at_utc TEXT NULL;
ALTER TABLE etl_runs ADD COLUMN finalize_conflict_code TEXT NULL;
ALTER TABLE etl_runs ADD COLUMN finalize_conflict_message TEXT NULL;
ALTER TABLE etl_runs ADD COLUMN resolved_at_utc TEXT NULL;
