-- 014_etl_read_completeness.sql
-- V1: verified completeness of full reads. 001-013 are immutable. Additive only.
--
-- etl_run_entities.read_completeness — the agent's verdict on a full read (readScope "full"):
--   'verified'    — $count before == $count after == rows read == rows of an independent
--                   key-only pass, and the key multisets of both passes are equal;
--   'unverified'  — the check ran and failed, or could not run (reason below);
--   'not_checked' — verification is switched off (Etl:VerifyFullReads = false).
--   NULL for delta reads, failed entities and rows written before 014; such items carry no
--   completeness fields in the completion body.
-- etl_run_entities.read_completeness_reason — a stable code for 'unverified' / 'not_checked'.
-- Both are written once, with the entity's completion, and never change.

ALTER TABLE etl_run_entities ADD COLUMN read_completeness TEXT NULL
    CHECK(read_completeness IS NULL OR read_completeness IN ('verified','unverified','not_checked'));
ALTER TABLE etl_run_entities ADD COLUMN read_completeness_reason TEXT NULL
    CHECK(read_completeness_reason IS NULL OR length(read_completeness_reason) BETWEEN 1 AND 64);
