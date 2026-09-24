-- v4-populated fixture: immutable pre-005 database state, exactly 001_initial.sql +
-- 002_retry_budgets.sql + 003_ordering_claims.sql + 004_command_payload_conflicts.sql semantics
-- (a real v4 database). Applied on top of 001-004 (schema) + the correct v1-v4 schema_migrations
-- ledger rows. Exercises that 005_durable_etl_jobs is purely additive: every command, attempt,
-- outbox, ETL run/batch, watermark, state, snapshot and conflict row is preserved byte-for-byte,
-- and the new etl_runs columns backfill deterministically.
--
-- Command rows cover every unfinished state plus terminal rows:
--   j101 start_full_sync  queued          never sent (manual ETL type, no job yet in v4)
--   j202 reload_entity    retry_waiting   never sent
--   j303 customer order   unknown_result  sent twice (post + lookup attempts)
--   j404 customer order   executing       in-flight post attempt (unfinished attempt row)
--   j505 customer order   result_pending  local success awaiting ERP ACK
--   j606 customer order   completed       ERP acknowledged
--   j707 customer order   result_pending  dead_letter result awaiting delivery
INSERT INTO commands_inbox(command_id,command_type,payload_version,priority,ordering_key,correlation_id,payload_json,payload_hash,status,created_at_utc,received_at_utc,not_before_utc,expires_at_utc,started_at_utc,finished_at_utc,attempt_count,next_attempt_at_utc,result_status,result_json,external_ref,external_number,last_error_code,last_error_message,erp_acknowledged_at_utc,row_version,first_sent_at_utc,lookup_attempt_count,post_attempt_count,queue_sequence,exec_claim_owner_id,exec_claim_acquired_at_utc)
VALUES
('00000000-0000-0000-0000-000000000101','start_full_sync',1,100,NULL,NULL,'{"entities":["clients","orders"]}','hash-j101','queued','2026-09-10T07:00:00.0000000+00:00','2026-09-10T07:00:00.0000000+00:00',NULL,NULL,NULL,NULL,0,'2026-09-10T07:00:00.0000000+00:00',NULL,NULL,NULL,NULL,NULL,NULL,NULL,1,NULL,0,0,1,NULL,NULL),
('00000000-0000-0000-0000-000000000202','reload_entity',1,90,NULL,NULL,'{"entity":"clients"}','hash-j202','retry_waiting','2026-09-10T07:00:00.0000000+00:00','2026-09-10T07:00:00.0000000+00:00',NULL,NULL,NULL,NULL,0,'2026-09-12T09:00:00.0000000+00:00',NULL,NULL,NULL,NULL,'ONEC_TECHNICAL','queue full, never sent',NULL,3,NULL,0,0,2,NULL,NULL),
('00000000-0000-0000-0000-000000000303','create_customer_order',1,80,'order:303',NULL,'{"amount":3}','hash-j303','unknown_result','2026-09-10T07:00:00.0000000+00:00','2026-09-10T07:00:00.0000000+00:00',NULL,NULL,'2026-09-18T12:00:00.0000000+00:00',NULL,2,'2026-09-18T13:00:00.0000000+00:00',NULL,NULL,NULL,NULL,'STATUS_PENDING','1C result is still unknown.',NULL,5,'2026-09-13T09:00:00.0000000+00:00',1,1,3,NULL,NULL),
('00000000-0000-0000-0000-000000000404','create_customer_order',1,70,'order:404',NULL,'{"amount":4}','hash-j404','executing','2026-09-10T07:00:00.0000000+00:00','2026-09-10T07:00:00.0000000+00:00',NULL,NULL,'2026-09-19T10:00:00.0000000+00:00',NULL,1,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,2,'2026-09-19T10:00:00.0000000+00:00',0,1,4,NULL,NULL),
('00000000-0000-0000-0000-000000000505','create_customer_order',1,60,'order:505',NULL,'{"amount":5}','hash-j505','result_pending','2026-09-10T07:00:00.0000000+00:00','2026-09-10T07:00:00.0000000+00:00',NULL,NULL,'2026-09-12T12:00:00.0000000+00:00','2026-09-12T12:00:05.0000000+00:00',1,NULL,'succeeded_local','{"status":"succeeded","document":{"ref":"ref-505"}}','ref-505','A-505',NULL,NULL,NULL,4,'2026-09-12T12:00:00.0000000+00:00',0,1,5,NULL,NULL),
('00000000-0000-0000-0000-000000000606','create_customer_order',1,50,'order:606',NULL,'{"amount":6}','hash-j606','completed','2026-09-10T07:00:00.0000000+00:00','2026-09-10T07:00:00.0000000+00:00',NULL,NULL,'2026-09-11T12:00:00.0000000+00:00','2026-09-11T12:00:05.0000000+00:00',1,NULL,'succeeded_local','{"status":"succeeded","document":{"ref":"ref-606"}}','ref-606','A-606',NULL,NULL,'2026-09-11T12:05:00.0000000+00:00',6,'2026-09-11T12:00:00.0000000+00:00',0,1,6,NULL,NULL),
('00000000-0000-0000-0000-000000000707','create_customer_order',1,40,'order:707',NULL,'{"amount":7}','hash-j707','result_pending','2026-09-10T07:00:00.0000000+00:00','2026-09-10T07:00:00.0000000+00:00',NULL,NULL,'2026-09-12T12:00:00.0000000+00:00','2026-09-12T12:00:05.0000000+00:00',1,NULL,'dead_letter','{"status":"dead_letter","error":{"code":"ONEC_BUSINESS_ERROR"}}',NULL,NULL,'ONEC_BUSINESS_ERROR','rejected by 1C',NULL,4,'2026-09-12T12:00:00.0000000+00:00',0,1,7,NULL,NULL);

INSERT INTO command_attempts(attempt_id,command_id,attempt_no,started_at_utc,finished_at_utc,request_hash,http_status,outcome,error_code,error_message,duration_ms,attempt_kind)
VALUES
('00000000-0000-0000-0000-000000000303-a','00000000-0000-0000-0000-000000000303',1,'2026-09-13T09:00:00.0000000+00:00','2026-09-13T09:00:01.0000000+00:00','hash-j303',502,'unknown_result','ONEC_TECHNICAL','boom',1000,'post'),
('00000000-0000-0000-0000-000000000303-b','00000000-0000-0000-0000-000000000303',2,'2026-09-15T09:00:00.0000000+00:00','2026-09-15T09:00:01.0000000+00:00','hash-j303',202,'unknown_result',NULL,NULL,1000,'lookup'),
('00000000-0000-0000-0000-000000000404-a','00000000-0000-0000-0000-000000000404',1,'2026-09-19T10:00:00.0000000+00:00',NULL,'hash-j404',NULL,NULL,NULL,NULL,NULL,'post'),
('00000000-0000-0000-0000-000000000505-a','00000000-0000-0000-0000-000000000505',1,'2026-09-12T12:00:00.0000000+00:00','2026-09-12T12:00:05.0000000+00:00','hash-j505',200,'succeeded_local',NULL,NULL,5000,'post'),
('00000000-0000-0000-0000-000000000606-a','00000000-0000-0000-0000-000000000606',1,'2026-09-11T12:00:00.0000000+00:00','2026-09-11T12:00:05.0000000+00:00','hash-j606',200,'succeeded_local',NULL,NULL,5000,'post'),
('00000000-0000-0000-0000-000000000707-a','00000000-0000-0000-0000-000000000707',1,'2026-09-12T12:00:00.0000000+00:00','2026-09-12T12:00:05.0000000+00:00','hash-j707',422,'business_failed_local','ONEC_BUSINESS_ERROR','rejected by 1C',5000,'post');

INSERT INTO results_outbox(result_id,command_id,payload_json,payload_hash,status,attempt_count,next_attempt_at_utc,created_at_utc,sent_at_utc,acknowledged_at_utc,last_error)
VALUES
('00000000-0000-0000-0000-000000000505-r','00000000-0000-0000-0000-000000000505','{"status":"succeeded","document":{"ref":"ref-505"}}','hash-res-505','pending',1,'2026-09-12T12:01:00.0000000+00:00','2026-09-12T12:00:05.0000000+00:00','2026-09-12T12:00:30.0000000+00:00',NULL,'timeout'),
('00000000-0000-0000-0000-000000000606-r','00000000-0000-0000-0000-000000000606','{"status":"succeeded","document":{"ref":"ref-606"}}','hash-res-606','acknowledged',1,NULL,'2026-09-11T12:00:05.0000000+00:00','2026-09-11T12:04:00.0000000+00:00','2026-09-11T12:05:00.0000000+00:00',NULL),
('00000000-0000-0000-0000-000000000707-r','00000000-0000-0000-0000-000000000707','{"status":"dead_letter","error":{"code":"ONEC_BUSINESS_ERROR"}}','hash-res-707','pending',0,'2026-09-12T12:00:05.0000000+00:00','2026-09-12T12:00:05.0000000+00:00',NULL,NULL,NULL);

-- Legacy v4 runs in every lifecycle position the foundation must not touch.
INSERT INTO etl_runs(run_id,mode,requested_entities_json,status,started_at_utc,finished_at_utc,rows_read,batches_created,batches_acknowledged,error_count,last_error)
VALUES
('00000000-0000-0000-0000-0000000000e1','incremental','["clients"]','running','2026-09-19T10:00:00.0000000+00:00',NULL,10,1,0,0,NULL),
('00000000-0000-0000-0000-0000000000e2','bootstrap_full','["clients","orders"]','uploading','2026-09-18T08:00:00.0000000+00:00',NULL,42,1,1,0,NULL),
('00000000-0000-0000-0000-0000000000e3','incremental','["clients"]','succeeded','2026-09-11T06:00:00.0000000+00:00','2026-09-11T06:10:00.0000000+00:00',7,1,1,0,NULL);

INSERT INTO etl_batches(batch_id,run_id,entity_name,schema_version,file_path,status,row_count,watermark_from_json,watermark_to_json,sha256,compressed_size,uncompressed_size,attempt_count,next_attempt_at_utc,created_at_utc,acknowledged_at_utc,last_error)
VALUES
('00000000-0000-0000-0000-000000000b01','00000000-0000-0000-0000-0000000000e1','clients',1,'spool/e1-b1.gz','ready',10,NULL,'{"updatedAtUtc":"2026-09-19T09:59:00.0000000+00:00","sourceId":"A10"}','hash-e1b1',1000,5000,0,'2026-09-19T10:00:01.0000000+00:00','2026-09-19T10:00:01.0000000+00:00',NULL,NULL),
('00000000-0000-0000-0000-000000000b02','00000000-0000-0000-0000-0000000000e2','clients',1,'spool/e2-b1.gz','acknowledged',42,NULL,'{"updatedAtUtc":"2026-09-18T07:59:00.0000000+00:00","sourceId":"A42"}','hash-e2b1',2000,9000,1,'2026-09-18T08:00:01.0000000+00:00','2026-09-18T08:00:01.0000000+00:00','2026-09-18T08:01:00.0000000+00:00',NULL),
('00000000-0000-0000-0000-000000000b03','00000000-0000-0000-0000-0000000000e3','clients',1,'spool/e3-b1.gz','acknowledged',7,NULL,'{"updatedAtUtc":"2026-09-11T05:59:00.0000000+00:00","sourceId":"A7"}','hash-e3b1',900,4000,1,'2026-09-11T06:00:01.0000000+00:00','2026-09-11T06:00:01.0000000+00:00','2026-09-11T06:01:00.0000000+00:00',NULL);

INSERT INTO watermarks(entity_name,committed_cursor_json,extracting_cursor_json,last_run_id,updated_at_utc)
VALUES
('clients','{"updatedAtUtc":"2026-09-11T05:59:00.0000000+00:00","sourceId":"A7"}',NULL,'00000000-0000-0000-0000-0000000000e3','2026-09-11T06:10:00.0000000+00:00');

INSERT INTO agent_state(key,value_json,updated_at_utc)
VALUES
('local_etl_pause','{"paused":false}','2026-09-19T09:00:00.0000000+00:00');

INSERT INTO config_snapshots(config_version,config_json,config_hash,received_at_utc,activated_at_utc,status)
VALUES
(3,'{"mode":"Normal","etlEntities":[]}','cfghash-3','2026-09-17T00:00:00.0000000+00:00','2026-09-17T00:00:05.0000000+00:00','active');

INSERT INTO command_payload_conflicts(event_id,command_id,event_code,severity,original_declared_hash_fingerprint,incoming_declared_hash_fingerprint,original_command_status,first_seen_at_utc,last_seen_at_utc,occurrence_count)
VALUES
('00000000-0000-0000-0000-000000000cf1','00000000-0000-0000-0000-000000000606','COMMAND_PAYLOAD_CONFLICT','critical','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb','completed','2026-09-12T00:00:00.0000000+00:00','2026-09-12T01:00:00.0000000+00:00',2);
