CREATE TABLE IF NOT EXISTS command_payload_conflicts (
    event_id TEXT PRIMARY KEY,
    command_id TEXT NOT NULL,
    event_code TEXT NOT NULL,
    severity TEXT NOT NULL,
    original_declared_hash_fingerprint TEXT NOT NULL,
    incoming_declared_hash_fingerprint TEXT NOT NULL,
    original_command_status TEXT NOT NULL,
    first_seen_at_utc TEXT NOT NULL,
    last_seen_at_utc TEXT NOT NULL,
    occurrence_count INTEGER NOT NULL,
    UNIQUE(command_id, original_declared_hash_fingerprint, incoming_declared_hash_fingerprint),
    CHECK(event_code='COMMAND_PAYLOAD_CONFLICT'),
    CHECK(severity='critical'),
    CHECK(length(original_declared_hash_fingerprint)=64),
    CHECK(length(incoming_declared_hash_fingerprint)=64),
    CHECK(occurrence_count>=1)
);

CREATE INDEX IF NOT EXISTS ix_command_payload_conflicts_recent
ON command_payload_conflicts(last_seen_at_utc DESC, command_id);
