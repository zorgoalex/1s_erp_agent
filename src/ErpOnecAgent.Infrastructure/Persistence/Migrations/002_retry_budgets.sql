-- 002_retry_budgets.sql
-- Non-destructive retry-budget backfill. 001_initial.sql is immutable (checksum 1).
-- first_sent_at_utc (send age is evidence-based, never status-based):
--   * rows with command_attempts use the earliest command_attempts.started_at_utc
--     (not the latest overwritten commands.started_at), including retry_waiting/retry-driven and
--     terminal rows that were previously sent (A02: retry_waiting may already have been sent);
--   * conservative fallback (started_at_utc) only for potentially-sent rows without attempt rows;
--   * never-sent queued/retry_waiting rows keep NULL (age budget starts at first real POST claim).
-- Prior send = attempt records (command_attempts) or attempt_count > 0; state alone never implies it.
ALTER TABLE commands_inbox ADD COLUMN first_sent_at_utc TEXT NULL;
ALTER TABLE commands_inbox ADD COLUMN lookup_attempt_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE commands_inbox ADD COLUMN post_attempt_count INTEGER NOT NULL DEFAULT 0;

UPDATE commands_inbox
SET first_sent_at_utc = (
    SELECT MIN(ca.started_at_utc)
    FROM command_attempts ca
    WHERE ca.command_id = commands_inbox.command_id
)
WHERE first_sent_at_utc IS NULL
  AND EXISTS (SELECT 1 FROM command_attempts ca WHERE ca.command_id = commands_inbox.command_id);

UPDATE commands_inbox
SET first_sent_at_utc = started_at_utc
WHERE first_sent_at_utc IS NULL
  AND attempt_count > 0
  AND started_at_utc IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM command_attempts ca WHERE ca.command_id = commands_inbox.command_id);

UPDATE commands_inbox
SET post_attempt_count = attempt_count
WHERE post_attempt_count = 0 AND attempt_count > 0;

ALTER TABLE command_attempts ADD COLUMN attempt_kind TEXT NOT NULL DEFAULT 'post';
UPDATE command_attempts SET attempt_kind = 'post' WHERE attempt_kind IS NULL OR attempt_kind = '';