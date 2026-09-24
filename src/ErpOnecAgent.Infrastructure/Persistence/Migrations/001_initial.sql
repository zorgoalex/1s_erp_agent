CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    checksum TEXT NOT NULL,
    applied_at_utc TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS commands_inbox (
    command_id TEXT PRIMARY KEY,
    command_type TEXT NOT NULL,
    payload_version INTEGER NOT NULL,
    priority INTEGER NOT NULL,
    ordering_key TEXT NULL,
    correlation_id TEXT NULL,
    payload_json TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at_utc TEXT NOT NULL,
    received_at_utc TEXT NOT NULL,
    not_before_utc TEXT NULL,
    expires_at_utc TEXT NULL,
    started_at_utc TEXT NULL,
    finished_at_utc TEXT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    next_attempt_at_utc TEXT NULL,
    result_status TEXT NULL,
    result_json TEXT NULL,
    external_ref TEXT NULL,
    external_number TEXT NULL,
    last_error_code TEXT NULL,
    last_error_message TEXT NULL,
    erp_acknowledged_at_utc TEXT NULL,
    row_version INTEGER NOT NULL DEFAULT 1
);

CREATE INDEX IF NOT EXISTS ix_commands_queue ON commands_inbox(status, next_attempt_at_utc, priority DESC, received_at_utc);
CREATE INDEX IF NOT EXISTS ix_commands_ordering ON commands_inbox(ordering_key, status, received_at_utc);
CREATE INDEX IF NOT EXISTS ix_commands_expiry ON commands_inbox(expires_at_utc);

CREATE TABLE IF NOT EXISTS command_attempts (
    attempt_id TEXT PRIMARY KEY,
    command_id TEXT NOT NULL,
    attempt_no INTEGER NOT NULL,
    started_at_utc TEXT NOT NULL,
    finished_at_utc TEXT NULL,
    request_hash TEXT NOT NULL,
    http_status INTEGER NULL,
    outcome TEXT NULL,
    error_code TEXT NULL,
    error_message TEXT NULL,
    duration_ms INTEGER NULL,
    FOREIGN KEY(command_id) REFERENCES commands_inbox(command_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_command_attempt ON command_attempts(command_id, attempt_no);

CREATE TABLE IF NOT EXISTS results_outbox (
    result_id TEXT PRIMARY KEY,
    command_id TEXT NOT NULL UNIQUE,
    payload_json TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    status TEXT NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    next_attempt_at_utc TEXT NULL,
    created_at_utc TEXT NOT NULL,
    sent_at_utc TEXT NULL,
    acknowledged_at_utc TEXT NULL,
    last_error TEXT NULL,
    FOREIGN KEY(command_id) REFERENCES commands_inbox(command_id)
);
CREATE INDEX IF NOT EXISTS ix_results_pending ON results_outbox(status, next_attempt_at_utc);

CREATE TABLE IF NOT EXISTS etl_runs (
    run_id TEXT PRIMARY KEY,
    mode TEXT NOT NULL,
    requested_entities_json TEXT NULL,
    status TEXT NOT NULL,
    started_at_utc TEXT NULL,
    finished_at_utc TEXT NULL,
    rows_read INTEGER NOT NULL DEFAULT 0,
    batches_created INTEGER NOT NULL DEFAULT 0,
    batches_acknowledged INTEGER NOT NULL DEFAULT 0,
    error_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT NULL
);
CREATE INDEX IF NOT EXISTS ix_etl_runs_status ON etl_runs(status, started_at_utc);

CREATE TABLE IF NOT EXISTS etl_batches (
    batch_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    entity_name TEXT NOT NULL,
    schema_version INTEGER NOT NULL,
    file_path TEXT NOT NULL,
    status TEXT NOT NULL,
    row_count INTEGER NOT NULL,
    watermark_from_json TEXT NULL,
    watermark_to_json TEXT NULL,
    sha256 TEXT NOT NULL,
    compressed_size INTEGER NOT NULL,
    uncompressed_size INTEGER NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    next_attempt_at_utc TEXT NULL,
    created_at_utc TEXT NOT NULL,
    acknowledged_at_utc TEXT NULL,
    last_error TEXT NULL,
    FOREIGN KEY(run_id) REFERENCES etl_runs(run_id)
);
CREATE INDEX IF NOT EXISTS ix_etl_batches_pending ON etl_batches(status, next_attempt_at_utc);
CREATE INDEX IF NOT EXISTS ix_etl_batches_run ON etl_batches(run_id, entity_name);

CREATE TABLE IF NOT EXISTS watermarks (
    entity_name TEXT PRIMARY KEY,
    committed_cursor_json TEXT NULL,
    extracting_cursor_json TEXT NULL,
    last_run_id TEXT NULL,
    updated_at_utc TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS agent_state (
    key TEXT PRIMARY KEY,
    value_json TEXT NOT NULL,
    updated_at_utc TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS config_snapshots (
    config_version INTEGER PRIMARY KEY,
    config_json TEXT NOT NULL,
    config_hash TEXT NOT NULL,
    received_at_utc TEXT NOT NULL,
    activated_at_utc TEXT NULL,
    status TEXT NOT NULL
);

