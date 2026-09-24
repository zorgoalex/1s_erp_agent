-- v1-populated fixture: immutable pre-002 database state, exactly 001_initial.sql semantics.
-- Applied on top of 001_initial.sql (schema) + the correct v1 schema_migrations ledger row.
-- Deterministic timestamps (2026-09-10..20) make first-send age assertions exact.
--
-- Command rows:
--   101 queued         never sent (no attempts)              -> first_sent_at_utc must stay NULL
--   202 retry_waiting  never sent (no attempts)              -> first_sent_at_utc must stay NULL
--   303 retry_waiting  previously sent (3 attempts,          -> earliest attempt (2026-09-10T08:00Z),
--                      started_at_utc overwritten 09-16)        far older than started_at_utc
--   404 unknown_result sent, 2 attempts,                    -> earliest attempt older than started_at_utc
--                      started_at_utc overwritten 09-18
--   505 executing      send evidence without attempt rows   -> conservative fallback = started_at_utc
--   606 unknown_result send evidence without attempt rows   -> conservative fallback = started_at_utc
--   707 result_pending sent terminal row, 1 attempt         -> earliest attempt preserved (terminal-safe)
--   808 completed      sent terminal row + acknowledged     -> preserved untouched (A01/A02)
INSERT INTO commands_inbox(command_id,command_type,payload_version,priority,ordering_key,correlation_id,payload_json,payload_hash,status,created_at_utc,received_at_utc,not_before_utc,expires_at_utc,started_at_utc,finished_at_utc,attempt_count,next_attempt_at_utc,result_status,result_json,external_ref,external_number,last_error_code,last_error_message,erp_acknowledged_at_utc,row_version)
VALUES
('00000000-0000-0000-0000-000000000101','create_customer_order',1,100,'order:101',NULL,'{"amount":1}','hash-101','queued','2026-09-10T07:00:00.0000000+00:00','2026-09-10T07:00:00.0000000+00:00',NULL,NULL,NULL,NULL,0,'2026-09-10T07:00:00.0000000+00:00',NULL,NULL,NULL,NULL,NULL,NULL,NULL,1),
('00000000-0000-0000-0000-000000000202','create_customer_order',1,90,'order:202',NULL,'{"amount":2}','hash-202','retry_waiting','2026-09-10T07:00:00.0000000+00:00','2026-09-10T07:00:00.0000000+00:00',NULL,NULL,NULL,NULL,0,'2026-09-12T09:00:00.0000000+00:00',NULL,NULL,NULL,NULL,'ONEC_TECHNICAL','queue full, never sent',NULL,3),
('00000000-0000-0000-0000-000000000303','create_customer_order',1,80,'order:303',NULL,'{"amount":3}','hash-303','retry_waiting','2026-09-10T07:00:00.0000000+00:00','2026-09-10T07:00:00.0000000+00:00',NULL,NULL,'2026-09-16T15:00:00.0000000+00:00',NULL,3,'2026-09-16T16:00:00.0000000+00:00',NULL,NULL,NULL,NULL,'ONEC_TECHNICAL','technical error after send',NULL,7),
('00000000-0000-0000-0000-000000000404','create_customer_order',1,70,'order:404',NULL,'{"amount":4}','hash-404','unknown_result','2026-09-10T07:00:00.0000000+00:00','2026-09-10T07:00:00.0000000+00:00',NULL,NULL,'2026-09-18T12:00:00.0000000+00:00',NULL,2,'2026-09-18T13:00:00.0000000+00:00',NULL,NULL,NULL,NULL,'STATUS_PENDING','1C result is still unknown.',NULL,5),
('00000000-0000-0000-0000-000000000505','create_customer_order',1,60,'order:505',NULL,'{"amount":5}','hash-505','executing','2026-09-10T07:00:00.0000000+00:00','2026-09-10T07:00:00.0000000+00:00',NULL,NULL,'2026-09-15T10:00:00.0000000+00:00',NULL,1,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,2),
('00000000-0000-0000-0000-000000000606','create_customer_order',1,50,'order:606',NULL,'{"amount":6}','hash-606','unknown_result','2026-09-10T07:00:00.0000000+00:00','2026-09-10T07:00:00.0000000+00:00',NULL,NULL,'2026-09-14T10:00:00.0000000+00:00',NULL,1,'2026-09-14T11:00:00.0000000+00:00',NULL,NULL,NULL,NULL,'STATUS_PENDING','1C result is still unknown.',NULL,3),
('00000000-0000-0000-0000-000000000707','create_customer_order',1,40,'order:707',NULL,'{"amount":7}','hash-707','result_pending','2026-09-10T07:00:00.0000000+00:00','2026-09-10T07:00:00.0000000+00:00',NULL,NULL,'2026-09-12T12:00:00.0000000+00:00','2026-09-12T12:00:05.0000000+00:00',1,NULL,'succeeded_local','{"status":"succeeded"}','ref-707','A-707',NULL,NULL,NULL,4),
('00000000-0000-0000-0000-000000000808','create_customer_order',1,30,'order:808',NULL,'{"amount":8}','hash-808','completed','2026-09-10T07:00:00.0000000+00:00','2026-09-10T07:00:00.0000000+00:00',NULL,NULL,'2026-09-11T12:00:00.0000000+00:00','2026-09-11T12:00:05.0000000+00:00',1,NULL,'succeeded_local','{"status":"succeeded"}','ref-808','A-808',NULL,NULL,'2026-09-11T12:05:00.0000000+00:00',6);

INSERT INTO command_attempts(attempt_id,command_id,attempt_no,started_at_utc,finished_at_utc,request_hash,http_status,outcome,error_code,error_message,duration_ms)
VALUES
('00000000-0000-0000-0000-000000000303-a','00000000-0000-0000-0000-000000000303',1,'2026-09-10T08:00:00.0000000+00:00','2026-09-10T08:00:01.0000000+00:00','hash-303',502,'unknown_result','ONEC_TECHNICAL','boom',1000),
('00000000-0000-0000-0000-000000000303-b','00000000-0000-0000-0000-000000000303',2,'2026-09-12T08:00:00.0000000+00:00','2026-09-12T08:00:01.0000000+00:00','hash-303',502,'unknown_result','ONEC_TECHNICAL','boom',1000),
('00000000-0000-0000-0000-000000000303-c','00000000-0000-0000-0000-000000000303',3,'2026-09-14T08:00:00.0000000+00:00','2026-09-14T08:00:01.0000000+00:00','hash-303',502,'unknown_result','ONEC_TECHNICAL','boom',1000),
('00000000-0000-0000-0000-000000000404-a','00000000-0000-0000-0000-000000000404',1,'2026-09-13T09:00:00.0000000+00:00','2026-09-13T09:00:01.0000000+00:00','hash-404',202,'unknown_result',NULL,NULL,1000),
('00000000-0000-0000-0000-000000000404-b','00000000-0000-0000-0000-000000000404',2,'2026-09-15T09:00:00.0000000+00:00','2026-09-15T09:00:01.0000000+00:00','hash-404',202,'unknown_result',NULL,NULL,1000),
('00000000-0000-0000-0000-000000000707-a','00000000-0000-0000-0000-000000000707',1,'2026-09-10T09:00:00.0000000+00:00','2026-09-10T09:00:01.0000000+00:00','hash-707',200,'succeeded_local',NULL,NULL,1000);

INSERT INTO results_outbox(result_id,command_id,payload_json,payload_hash,status,attempt_count,next_attempt_at_utc,created_at_utc,sent_at_utc,acknowledged_at_utc,last_error)
VALUES
('00000000-0000-0000-0000-000000000707-r','00000000-0000-0000-0000-000000000707','{"status":"succeeded"}','hash-res-707','pending',1,'2026-09-12T12:01:00.0000000+00:00','2026-09-12T12:00:05.0000000+00:00',NULL,NULL,NULL),
('00000000-0000-0000-0000-000000000808-r','00000000-0000-0000-0000-000000000808','{"status":"succeeded"}','hash-res-808','acknowledged',1,NULL,'2026-09-11T12:00:05.0000000+00:00','2026-09-11T12:04:00.0000000+00:00','2026-09-11T12:05:00.0000000+00:00',NULL);
