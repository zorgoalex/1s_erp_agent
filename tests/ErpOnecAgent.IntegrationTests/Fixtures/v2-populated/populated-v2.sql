-- v2-populated fixture: immutable pre-003 database state, exactly 001_initial.sql + 002_retry_budgets.sql
-- semantics (a real v2 database). Applied on top of 001+002 (schema) + the correct v2 schema_migrations
-- ledger rows. Exercises the 003_ordering_claims backfill: queue_sequence assigned deterministically by
-- (received_at_utc, command_id) for pre-existing rows, and exec_claim_* left NULL (no in-flight claims
-- survive a migration; RecoverAsync handles those at restart).
--
-- Two groups share EQUAL receive timestamps so the queue_sequence tie-break (not received_at) resolves
-- their total order:
--   Group 1  received 2026-09-10T07:00:00.0000000+00:00  (order:200)
--     00000000-...-000000000a01  queued   (smaller command_id -> queue_sequence 1, head)
--     00000000-...-000000000a02  queued   (larger  command_id -> queue_sequence 2, successor)
--   Group 2  received 2026-09-10T08:00:00.0000000+00:00  (order:200 as well, later receive)
--     00000000-...-000000000b01  queued   (smaller command_id -> queue_sequence 3)
--     00000000-...-000000000b02  queued   (larger  command_id -> queue_sequence 4)
-- Plus a terminal completed row (808) to prove migration never rewrites terminal data.
INSERT INTO commands_inbox(command_id,command_type,payload_version,priority,ordering_key,correlation_id,payload_json,payload_hash,status,created_at_utc,received_at_utc,not_before_utc,expires_at_utc,started_at_utc,finished_at_utc,attempt_count,next_attempt_at_utc,result_status,result_json,external_ref,external_number,last_error_code,last_error_message,erp_acknowledged_at_utc,row_version)
VALUES
('00000000-0000-0000-0000-000000000a01','create_customer_order',1,100,'order:200',NULL,'{"amount":1}','hash-a01','queued','2026-09-10T07:00:00.0000000+00:00','2026-09-10T07:00:00.0000000+00:00',NULL,NULL,NULL,NULL,0,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,1),
('00000000-0000-0000-0000-000000000a02','create_customer_order',1,100,'order:200',NULL,'{"amount":2}','hash-a02','queued','2026-09-10T07:00:00.0000000+00:00','2026-09-10T07:00:00.0000000+00:00',NULL,NULL,NULL,NULL,0,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,1),
('00000000-0000-0000-0000-000000000b01','create_customer_order',1,100,'order:200',NULL,'{"amount":3}','hash-b01','queued','2026-09-10T08:00:00.0000000+00:00','2026-09-10T08:00:00.0000000+00:00',NULL,NULL,NULL,NULL,0,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,1),
('00000000-0000-0000-0000-000000000b02','create_customer_order',1,100,'order:200',NULL,'{"amount":4}','hash-b02','queued','2026-09-10T08:00:00.0000000+00:00','2026-09-10T08:00:00.0000000+00:00',NULL,NULL,NULL,NULL,0,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,1),
('00000000-0000-0000-0000-000000000808','create_customer_order',1,30,'order:808',NULL,'{"amount":8}','hash-808','completed','2026-09-10T07:00:00.0000000+00:00','2026-09-10T06:00:00.0000000+00:00',NULL,NULL,'2026-09-11T12:00:00.0000000+00:00','2026-09-11T12:00:05.0000000+00:00',1,NULL,'succeeded_local','{"status":"succeeded"}','ref-808','A-808',NULL,NULL,'2026-09-11T12:05:00.0000000+00:00',6);

INSERT INTO results_outbox(result_id,command_id,payload_json,payload_hash,status,attempt_count,next_attempt_at_utc,created_at_utc,sent_at_utc,acknowledged_at_utc,last_error)
VALUES
('00000000-0000-0000-0000-000000000808-r','00000000-0000-0000-0000-000000000808','{"status":"succeeded"}','hash-res-808','acknowledged',1,NULL,'2026-09-11T12:00:05.0000000+00:00','2026-09-11T12:04:00.0000000+00:00','2026-09-11T12:05:00.0000000+00:00',NULL);
