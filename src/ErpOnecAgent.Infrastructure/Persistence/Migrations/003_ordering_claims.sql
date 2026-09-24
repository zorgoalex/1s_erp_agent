-- 003_ordering_claims.sql
-- Deterministic command ordering + durable fresh-execution claim guard (A09 ordering/metrics slice).
-- 001_initial.sql and 002_retry_budgets.sql are immutable (checksums 1 and 2). Non-destructive:
-- every column is additive and every existing row keeps its data (explicit backfill below).
--
-- queue_sequence
--   * Stable tie-break for the required per-ordering-key queue semantics: received_at_utc alone is
--     NOT a total order (two commands can share the exact same receive timestamp), which let two
--     heads of one ordering key be ready at once.
--   * Durable monotonic admission order (MAX(queue_sequence)+1 at insert), so the total order
--     (priority DESC, received_at_utc, queue_sequence) is deterministic across restarts and never
--     reorders rows whose timestamps differ. Backfilled from received_at_utc, command_id for rows
--     admitted before this migration (deterministic, no data loss).
--
-- exec_claim_owner_id / exec_claim_acquired_at_utc
--   * Durable claim identity for one FRESH execution pass (POST). Claim acquisition is atomic
--     (guarded UPDATE ... RETURNING on the active states) so two stale ready snapshots or two
--     concurrent claimants can never both execute the same command/key. The owner id identifies
--     exactly one claim generation: a stale in-memory snapshot from another pass cannot release or
--     complete a newer claim.
--   * Recovered claim_pending rows are intentionally left unclaimable through that stale snapshot
--     (no lease expiry re-opens them during the pass) while RecoverAsync re-schedules them normally,
--     so nothing is lost after restart and backoff/schedule semantics are preserved.
--   * Scoped to fresh execution only: the status-lookup resolve-before-retry path keeps its accepted
--     atomic lookup claim and unknown_result reclaim semantics (FR-CMD-012) unchanged.
ALTER TABLE commands_inbox ADD COLUMN queue_sequence INTEGER NOT NULL DEFAULT 0;
ALTER TABLE commands_inbox ADD COLUMN exec_claim_owner_id TEXT NULL;
ALTER TABLE commands_inbox ADD COLUMN exec_claim_acquired_at_utc TEXT NULL;

UPDATE commands_inbox
SET queue_sequence = (
    SELECT rn FROM (
        SELECT command_id,
               ROW_NUMBER() OVER (ORDER BY received_at_utc, command_id) AS rn
        FROM commands_inbox
    ) ordered
    WHERE ordered.command_id = commands_inbox.command_id
)
WHERE queue_sequence = 0;
