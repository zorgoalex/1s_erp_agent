-- v9-populated fixture: immutable pre-010 database state, exactly 001_initial.sql +
-- 002_retry_budgets.sql + 003_ordering_claims.sql + 004_command_payload_conflicts.sql +
-- 005_durable_etl_jobs.sql + 006_etl_finalize.sql + 007_etl_ownership.sql +
-- 008_etl_send_attempts.sql + 009_etl_scheduled_runs.sql semantics (a real v9
-- database). Applied on top of 001-009 (schema) + the correct v1-v9
-- schema_migrations ledger rows seeded by the test (ledger entries are NOT in this
-- file).
-- Builds on the v8 fixture coverage (every v8 row kept byte-identical, NULL
-- schedule_key/resolved_entities_json on all of them), extended with the
-- R1-upgrade-critical states:
--   * scheduled runs in every key-holding position: 'pending', 'running' with a
--     live extraction claim + ownerless (owner_job_id NULL) ownership, 'blocked'
--     unresolved (still holding its schedule key with retained ownership), and
--     'succeeded' (key released, ownership released 'finalized') — each carrying
--     schedule_key and the frozen resolved_entities_json;
--   * a 'failed' manual-job run and a 'blocked' manual-job run, each with retained
--     lifetime ownership + immutable epoch bindings, blocked jobs and fenced
--     dead_letter batch/send-attempt evidence — the exact unresolved states
--     ResolveEtlRunAsync (R1) exists to attest over.
--
-- 010 must stay additive: every pre-existing row survives byte-identical and
-- etl_run_resolutions is created EMPTY.

BEGIN TRANSACTION;

-- Command rows cover every unfinished state plus terminal rows (same coverage as the
-- v6 fixture):
--   j101 start_full_sync  queued          never sent (manual ETL type)
--   j202 reload_entity    retry_waiting   never sent
--   j303 customer order   unknown_result  sent twice (post + lookup attempts)
--   j404 customer order   executing       in-flight post attempt (unfinished attempt row)
--   j505 customer order   result_pending  local success awaiting ERP ACK
--   j606 customer order   completed       ERP acknowledged
--   j707 customer order   result_pending  dead_letter result awaiting delivery
--   j808 start_full_sync  result_pending  accepted job a05 (failed run f5)
--   j909 reload_entity    result_pending  accepted job a06 (blocked run f6)
INSERT INTO commands_inbox(command_id,command_type,payload_version,priority,ordering_key,correlation_id,payload_json,payload_hash,status,created_at_utc,received_at_utc,not_before_utc,expires_at_utc,started_at_utc,finished_at_utc,attempt_count,next_attempt_at_utc,result_status,result_json,external_ref,external_number,last_error_code,last_error_message,erp_acknowledged_at_utc,row_version,first_sent_at_utc,lookup_attempt_count,post_attempt_count,queue_sequence,exec_claim_owner_id,exec_claim_acquired_at_utc)
VALUES
('00000000-0000-0000-0000-000000000101','start_full_sync',1,100,NULL,NULL,'{"entities":["clients","orders"]}','hash-j101','queued','2026-09-10T07:00:00.0000000+00:00','2026-09-10T07:00:00.0000000+00:00',NULL,NULL,NULL,NULL,0,'2026-09-10T07:00:00.0000000+00:00',NULL,NULL,NULL,NULL,NULL,NULL,NULL,1,NULL,0,0,1,NULL,NULL),
('00000000-0000-0000-0000-000000000202','reload_entity',1,90,NULL,NULL,'{"entity":"clients"}','hash-j202','retry_waiting','2026-09-10T07:00:00.0000000+00:00','2026-09-10T07:00:00.0000000+00:00',NULL,NULL,NULL,NULL,0,'2026-09-12T09:00:00.0000000+00:00',NULL,NULL,NULL,NULL,'ONEC_TECHNICAL','queue full, never sent',NULL,3,NULL,0,0,2,NULL,NULL),
('00000000-0000-0000-0000-000000000303','create_customer_order',1,80,'order:303',NULL,'{"amount":3}','hash-j303','unknown_result','2026-09-10T07:00:00.0000000+00:00','2026-09-10T07:00:00.0000000+00:00',NULL,NULL,'2026-09-18T12:00:00.0000000+00:00',NULL,2,'2026-09-18T13:00:00.0000000+00:00',NULL,NULL,NULL,NULL,'STATUS_PENDING','1C result is still unknown.',NULL,5,'2026-09-13T09:00:00.0000000+00:00',1,1,3,NULL,NULL),
('00000000-0000-0000-0000-000000000404','create_customer_order',1,70,'order:404',NULL,'{"amount":4}','hash-j404','executing','2026-09-10T07:00:00.0000000+00:00','2026-09-10T07:00:00.0000000+00:00',NULL,NULL,'2026-09-19T10:00:00.0000000+00:00',NULL,1,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,2,'2026-09-19T10:00:00.0000000+00:00',0,1,4,NULL,NULL),
('00000000-0000-0000-0000-000000000505','create_customer_order',1,60,'order:505',NULL,'{"amount":5}','hash-j505','result_pending','2026-09-10T07:00:00.0000000+00:00','2026-09-10T07:00:00.0000000+00:00',NULL,NULL,'2026-09-12T12:00:00.0000000+00:00','2026-09-12T12:00:05.0000000+00:00',1,NULL,'succeeded_local','{"status":"succeeded","document":{"ref":"ref-505"}}','ref-505','A-505',NULL,NULL,NULL,4,'2026-09-12T12:00:00.0000000+00:00',0,1,5,NULL,NULL),
('00000000-0000-0000-0000-000000000606','create_customer_order',1,50,'order:606',NULL,'{"amount":6}','hash-j606','completed','2026-09-10T07:00:00.0000000+00:00','2026-09-10T07:00:00.0000000+00:00',NULL,NULL,'2026-09-11T12:00:00.0000000+00:00','2026-09-11T12:00:05.0000000+00:00',1,NULL,'succeeded_local','{"status":"succeeded","document":{"ref":"ref-606"}}','ref-606','A-606',NULL,NULL,'2026-09-11T12:05:00.0000000+00:00',6,'2026-09-11T12:00:00.0000000+00:00',0,1,6,NULL,NULL),
('00000000-0000-0000-0000-000000000707','create_customer_order',1,40,'order:707',NULL,'{"amount":7}','hash-j707','result_pending','2026-09-10T07:00:00.0000000+00:00','2026-09-10T07:00:00.0000000+00:00',NULL,NULL,'2026-09-12T12:00:00.0000000+00:00','2026-09-12T12:00:05.0000000+00:00',1,NULL,'dead_letter','{"status":"dead_letter","error":{"code":"ONEC_BUSINESS_ERROR"}}',NULL,NULL,'ONEC_BUSINESS_ERROR','rejected by 1C',NULL,4,'2026-09-12T12:00:00.0000000+00:00',0,1,7,NULL,NULL),
('00000000-0000-0000-0000-000000000808','start_full_sync',1,95,NULL,NULL,'{"entities":["employees"]}','hash-j808','result_pending','2026-09-21T08:00:00.0000000+00:00','2026-09-21T08:00:00.0000000+00:00',NULL,NULL,'2026-09-21T08:00:01.0000000+00:00','2026-09-21T08:00:02.0000000+00:00',1,NULL,'succeeded_local','{"status":"succeeded","completedAtUtc":"2026-09-21T08:00:02.0000000+00:00","data":{"accepted":true,"runId":"00000000-0000-0000-0000-0000000000f5","mode":"bootstrap_full"},"warnings":[],"resultVersion":1}',NULL,NULL,NULL,NULL,NULL,1,NULL,0,1,8,NULL,NULL),
('00000000-0000-0000-0000-000000000909','reload_entity',1,85,NULL,NULL,'{"entity":"assets"}','hash-j909','result_pending','2026-09-21T09:00:00.0000000+00:00','2026-09-21T09:00:00.0000000+00:00',NULL,NULL,'2026-09-21T09:00:01.0000000+00:00','2026-09-21T09:00:02.0000000+00:00',1,NULL,'succeeded_local','{"status":"succeeded","completedAtUtc":"2026-09-21T09:00:02.0000000+00:00","data":{"accepted":true,"runId":"00000000-0000-0000-0000-0000000000f6","mode":"entity_reload"},"warnings":[],"resultVersion":1}',NULL,NULL,NULL,NULL,NULL,1,NULL,0,1,9,NULL,NULL);

INSERT INTO command_attempts(attempt_id,command_id,attempt_no,started_at_utc,finished_at_utc,request_hash,http_status,outcome,error_code,error_message,duration_ms,attempt_kind)
VALUES
('00000000-0000-0000-0000-000000000303-a','00000000-0000-0000-0000-000000000303',1,'2026-09-13T09:00:00.0000000+00:00','2026-09-13T09:00:01.0000000+00:00','hash-j303',502,'unknown_result','ONEC_TECHNICAL','boom',1000,'post'),
('00000000-0000-0000-0000-000000000303-b','00000000-0000-0000-0000-000000000303',2,'2026-09-15T09:00:00.0000000+00:00','2026-09-15T09:00:01.0000000+00:00','hash-j303',202,'unknown_result',NULL,NULL,1000,'lookup'),
('00000000-0000-0000-0000-000000000404-a','00000000-0000-0000-0000-000000000404',1,'2026-09-19T10:00:00.0000000+00:00',NULL,'hash-j404',NULL,NULL,NULL,NULL,NULL,'post'),
('00000000-0000-0000-0000-000000000505-a','00000000-0000-0000-0000-000000000505',1,'2026-09-12T12:00:00.0000000+00:00','2026-09-12T12:00:05.0000000+00:00','hash-j505',200,'succeeded_local',NULL,NULL,5000,'post'),
('00000000-0000-0000-0000-000000000606-a','00000000-0000-0000-0000-000000000606',1,'2026-09-11T12:00:00.0000000+00:00','2026-09-11T12:00:05.0000000+00:00','hash-j606',200,'succeeded_local',NULL,NULL,5000,'post'),
('00000000-0000-0000-0000-000000000707-a','00000000-0000-0000-0000-000000000707',1,'2026-09-12T12:00:00.0000000+00:00','2026-09-12T12:00:05.0000000+00:00','hash-j707',422,'business_failed_local','ONEC_BUSINESS_ERROR','rejected by 1C',5000,'post'),
('00000000-0000-0000-0000-000000000808-a','00000000-0000-0000-0000-000000000808',1,'2026-09-21T08:00:01.0000000+00:00','2026-09-21T08:00:02.0000000+00:00','hash-j808',200,'succeeded_local',NULL,NULL,1000,'post'),
('00000000-0000-0000-0000-000000000909-a','00000000-0000-0000-0000-000000000909',1,'2026-09-21T09:00:01.0000000+00:00','2026-09-21T09:00:02.0000000+00:00','hash-j909',200,'succeeded_local',NULL,NULL,1000,'post');

INSERT INTO results_outbox(result_id,command_id,payload_json,payload_hash,status,attempt_count,next_attempt_at_utc,created_at_utc,sent_at_utc,acknowledged_at_utc,last_error)
VALUES
('00000000-0000-0000-0000-000000000505-r','00000000-0000-0000-0000-000000000505','{"status":"succeeded","document":{"ref":"ref-505"}}','hash-res-505','pending',1,'2026-09-12T12:01:00.0000000+00:00','2026-09-12T12:00:05.0000000+00:00','2026-09-12T12:00:30.0000000+00:00',NULL,'timeout'),
('00000000-0000-0000-0000-000000000606-r','00000000-0000-0000-0000-000000000606','{"status":"succeeded","document":{"ref":"ref-606"}}','hash-res-606','acknowledged',1,NULL,'2026-09-11T12:00:05.0000000+00:00','2026-09-11T12:04:00.0000000+00:00','2026-09-11T12:05:00.0000000+00:00',NULL),
('00000000-0000-0000-0000-000000000707-r','00000000-0000-0000-0000-000000000707','{"status":"dead_letter","error":{"code":"ONEC_BUSINESS_ERROR"}}','hash-res-707','pending',0,'2026-09-12T12:00:05.0000000+00:00','2026-09-12T12:00:05.0000000+00:00',NULL,NULL,NULL),
('00000000-0000-0000-0000-000000000808-r','00000000-0000-0000-0000-000000000808','{"status":"succeeded","completedAtUtc":"2026-09-21T08:00:02.0000000+00:00","data":{"accepted":true,"runId":"00000000-0000-0000-0000-0000000000f5","mode":"bootstrap_full"},"warnings":[],"resultVersion":1}','hash-res-808','pending',0,NULL,'2026-09-21T08:00:02.0000000+00:00',NULL,NULL,NULL),
('00000000-0000-0000-0000-000000000909-r','00000000-0000-0000-0000-000000000909','{"status":"succeeded","completedAtUtc":"2026-09-21T09:00:02.0000000+00:00","data":{"accepted":true,"runId":"00000000-0000-0000-0000-0000000000f6","mode":"entity_reload"},"warnings":[],"resultVersion":1}','hash-res-909','pending',0,NULL,'2026-09-21T09:00:02.0000000+00:00',NULL,NULL,NULL);

-- Runs: the v8 lifecycle coverage (running e1, uploading e2, succeeded e3, pending
-- e4, completing e5, blocked e6 — all WITHOUT a schedule key) plus the v9 states:
--   f1 scheduled 'pending'     holds key 'sched-nightly'
--   f2 scheduled 'running'     holds key 'sched-hourly' + extraction claim + ownership
--   f3 scheduled 'blocked'     holds key 'sched-weekly' UNRESOLVED + retained ownership
--   f4 scheduled 'succeeded'   released key 'sched-monthly' + finalized ownership
--   f5 manual 'failed'         unresolved, job a05 blocked, ownership retained
--   f6 manual 'blocked'        unresolved, job a06 blocked, ownership retained
INSERT INTO etl_runs(run_id,mode,requested_entities_json,status,started_at_utc,finished_at_utc,rows_read,batches_created,batches_acknowledged,error_count,last_error,configuration_version,created_at_utc,updated_at_utc,row_version,sealed_at_utc,sealed_entity_count,sealed_expected_batch_count,completion_claim_id,completion_claim_owner_id,completion_claim_acquired_at_utc,completion_attempt_count,completion_max_attempts,next_completion_attempt_at_utc,complete_payload_json,completion_acknowledged_at_utc,finalize_conflict_code,finalize_conflict_message,resolved_at_utc,extraction_claim_id,extraction_claim_owner_id,extraction_claim_acquired_at_utc,schedule_key,resolved_entities_json)
VALUES
('00000000-0000-0000-0000-0000000000e1','entity_reload','["payments"]','running','2026-09-19T10:00:00.0000000+00:00',NULL,10,1,0,0,NULL,NULL,'2026-09-19T10:00:00.0000000+00:00','2026-09-19T10:00:00.0000000+00:00',1,NULL,NULL,NULL,NULL,NULL,NULL,0,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'00000000-0000-0000-0000-000000000cc0','worker-old','2026-09-19T10:00:00.0000000+00:00',NULL,NULL),
('00000000-0000-0000-0000-0000000000e2','bootstrap_full','["clients","orders"]','uploading','2026-09-18T08:00:00.0000000+00:00',NULL,42,2,1,0,NULL,NULL,'2026-09-18T08:00:00.0000000+00:00','2026-09-18T08:00:00.0000000+00:00',1,'2026-09-18T08:05:00.0000000+00:00',2,2,NULL,NULL,NULL,0,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL),
('00000000-0000-0000-0000-0000000000e3','incremental','["products"]','succeeded','2026-09-11T06:00:00.0000000+00:00','2026-09-11T06:10:00.0000000+00:00',7,1,1,0,NULL,NULL,'2026-09-11T06:00:00.0000000+00:00','2026-09-11T06:10:00.0000000+00:00',1,NULL,NULL,NULL,NULL,NULL,NULL,0,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL),
('00000000-0000-0000-0000-0000000000e4','bootstrap_full','["clients"]','pending',NULL,NULL,0,0,0,0,NULL,3,'2026-09-20T07:00:00.0000000+00:00','2026-09-20T07:00:00.0000000+00:00',1,NULL,NULL,NULL,NULL,NULL,NULL,0,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL),
('00000000-0000-0000-0000-0000000000e5','bootstrap_full','["shipments"]','completing','2026-09-19T09:00:00.0000000+00:00',NULL,7,1,1,0,NULL,3,'2026-09-19T09:00:00.0000000+00:00','2026-09-19T09:10:00.0000000+00:00',3,'2026-09-19T09:05:00.0000000+00:00',1,1,'00000000-0000-0000-0000-000000000cc1','owner-7','2026-09-19T09:09:00.0000000+00:00',1,8,'2026-09-19T09:20:00.0000000+00:00','{"runId":"00000000-0000-0000-0000-0000000000e5","status":"succeeded","rowsRead":7,"batchesCreated":1,"batchesAcknowledged":1,"completedAtUtc":"2026-09-19T09:09:00.0000000+00:00"}',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL),
('00000000-0000-0000-0000-0000000000e6','entity_reload','["inventory"]','blocked','2026-09-18T11:00:00.0000000+00:00','2026-09-18T11:15:00.0000000+00:00',60,1,0,1,'A batch send outcome was unknowable.',NULL,'2026-09-18T11:00:00.0000000+00:00','2026-09-18T11:15:00.0000000+00:00',3,NULL,NULL,NULL,NULL,NULL,NULL,0,NULL,NULL,NULL,NULL,'OWNERSHIP_SET_MISMATCH','binding drift detected at seal',NULL,NULL,NULL,NULL,NULL,NULL),
('00000000-0000-0000-0000-0000000000f1','incremental','["warehouses"]','pending',NULL,NULL,0,0,0,0,NULL,7,'2026-09-21T00:00:00.0000000+00:00','2026-09-21T00:00:00.0000000+00:00',1,NULL,NULL,NULL,NULL,NULL,NULL,0,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'sched-nightly','[{"entityCode":"warehouses","oDataPath":"Catalog_Warehouses","keyField":"Ref_Key","updatedAtField":"UpdatedAt","deletedField":"DeletionMark","select":["Ref_Key","UpdatedAt","DeletionMark"],"syncMode":"incremental","pageSize":500,"overlapMinutes":10,"schemaVersion":1,"oDataVersion":3,"enabled":true,"keyFields":null,"updatedAtEdmType":"Edm.DateTimeOffset"}]'),
('00000000-0000-0000-0000-0000000000f2','incremental','["returns"]','running','2026-09-21T01:00:00.0000000+00:00',NULL,15,1,0,0,NULL,7,'2026-09-21T01:00:00.0000000+00:00','2026-09-21T01:00:00.0000000+00:00',1,NULL,NULL,NULL,NULL,NULL,NULL,0,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'00000000-0000-0000-0000-000000000cc2','scheduler-1','2026-09-21T01:00:00.0000000+00:00','sched-hourly','[{"entityCode":"returns","oDataPath":"Document_Returns","keyField":"Ref_Key","updatedAtField":"Date","deletedField":"Posted","select":["Ref_Key","Date","Posted"],"syncMode":"incremental","pageSize":500,"overlapMinutes":10,"schemaVersion":1,"oDataVersion":3,"enabled":true,"keyFields":null,"updatedAtEdmType":"Edm.DateTimeOffset"}]'),
('00000000-0000-0000-0000-0000000000f3','incremental','["price_lists"]','blocked','2026-09-21T02:00:00.0000000+00:00','2026-09-21T02:30:00.0000000+00:00',4,1,0,1,'Interrupted scheduled extraction: no durable checkpoint.',7,'2026-09-21T02:00:00.0000000+00:00','2026-09-21T02:30:00.0000000+00:00',2,NULL,NULL,NULL,NULL,NULL,NULL,0,NULL,NULL,NULL,NULL,'INTERRUPTED_NO_CHECKPOINT','recovery blocked the interrupted run',NULL,NULL,NULL,NULL,'sched-weekly','[{"entityCode":"price_lists","oDataPath":"Catalog_PriceLists","keyField":"Ref_Key","updatedAtField":"UpdatedAt","deletedField":"DeletionMark","select":["Ref_Key","UpdatedAt","DeletionMark"],"syncMode":"incremental","pageSize":500,"overlapMinutes":10,"schemaVersion":1,"oDataVersion":3,"enabled":true,"keyFields":null,"updatedAtEdmType":"Edm.DateTimeOffset"}]'),
('00000000-0000-0000-0000-0000000000f4','incremental','["currencies"]','succeeded','2026-09-21T03:00:00.0000000+00:00','2026-09-21T03:10:00.0000000+00:00',9,1,1,0,NULL,7,'2026-09-21T03:00:00.0000000+00:00','2026-09-21T03:10:00.0000000+00:00',3,'2026-09-21T03:05:00.0000000+00:00',1,1,NULL,NULL,NULL,1,8,NULL,'{"runId":"00000000-0000-0000-0000-0000000000f4","status":"succeeded","rowsRead":9,"batchesCreated":1,"batchesAcknowledged":1,"completedAtUtc":"2026-09-21T03:09:00.0000000+00:00"}','2026-09-21T03:10:00.0000000+00:00',NULL,NULL,NULL,NULL,NULL,NULL,'sched-monthly','[{"entityCode":"currencies","oDataPath":"Catalog_Currencies","keyField":"Ref_Key","updatedAtField":"UpdatedAt","deletedField":"DeletionMark","select":["Ref_Key","UpdatedAt","DeletionMark"],"syncMode":"incremental","pageSize":500,"overlapMinutes":10,"schemaVersion":1,"oDataVersion":3,"enabled":true,"keyFields":null,"updatedAtEdmType":"Edm.DateTimeOffset"}]'),
('00000000-0000-0000-0000-0000000000f5','bootstrap_full','["employees"]','failed','2026-09-21T08:10:00.0000000+00:00','2026-09-21T08:20:00.0000000+00:00',3,1,0,1,'extraction failed',3,'2026-09-21T08:00:00.0000000+00:00','2026-09-21T08:20:00.0000000+00:00',2,NULL,NULL,NULL,NULL,NULL,NULL,0,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL),
('00000000-0000-0000-0000-0000000000f6','entity_reload','["assets"]','blocked','2026-09-21T09:10:00.0000000+00:00','2026-09-21T09:40:00.0000000+00:00',22,1,0,1,'A batch send outcome was unknowable.',3,'2026-09-21T09:00:00.0000000+00:00','2026-09-21T09:40:00.0000000+00:00',3,'2026-09-21T09:20:00.0000000+00:00',1,1,NULL,NULL,NULL,0,NULL,NULL,NULL,NULL,'UPLOAD_OUTCOME_UNKNOWN','send outcome unknown for batch 00000000-0000-0000-0000-000000000b10',NULL,NULL,NULL,NULL,NULL,NULL);

INSERT INTO etl_run_entities(run_id,entity_name,entity_definition_json,domain_fingerprint,status,base_row_present,expected_base_generation,expected_base_cursor_json,expected_base_domain_fingerprint,domain_status,watermark_from_json,snapshot_upper_bound_json,final_watermark_json,expected_batch_count,rows_read,batches_created,last_error,created_at_utc,updated_at_utc,row_version)
VALUES
('00000000-0000-0000-0000-0000000000e1','payments','{"entityCode":"payments"}','fp-e1-payments','extracting',1,4,'{"updatedAtUtc":"2026-09-18T05:59:00.0000000+00:00","sourceId":"P4"}','fp-e1-payments','same','{"updatedAtUtc":"2026-09-18T05:59:00.0000000+00:00","sourceId":"P4"}',NULL,NULL,1,10,0,NULL,'2026-09-19T10:00:00.0000000+00:00','2026-09-19T10:00:00.0000000+00:00',1),
('00000000-0000-0000-0000-0000000000e2','clients','{"entityCode":"clients"}','fp-e2-clients','done',1,4,'{"updatedAtUtc":"2026-09-17T05:59:00.0000000+00:00","sourceId":"A4"}','fp-e2-clients','same','{"updatedAtUtc":"2026-09-17T05:59:00.0000000+00:00","sourceId":"A4"}',NULL,'{"updatedAtUtc":"2026-09-18T07:59:00.0000000+00:00","sourceId":"A42"}',2,42,2,NULL,'2026-09-18T08:00:00.0000000+00:00','2026-09-18T08:05:00.0000000+00:00',1),
('00000000-0000-0000-0000-0000000000e2','orders','{"entityCode":"orders"}','fp-e2-orders','done',0,NULL,NULL,NULL,'absent',NULL,NULL,'{"updatedAtUtc":"2026-09-18T07:58:00.0000000+00:00","sourceId":"O1"}',0,0,0,NULL,'2026-09-18T08:00:00.0000000+00:00','2026-09-18T08:05:00.0000000+00:00',1),
('00000000-0000-0000-0000-0000000000e5','shipments','{"entityCode":"shipments"}','fp-e5-shipments','done',0,NULL,NULL,NULL,'absent',NULL,NULL,'{"updatedAtUtc":"2026-09-19T08:59:00.0000000+00:00","sourceId":"O7"}',1,7,1,NULL,'2026-09-19T09:00:00.0000000+00:00','2026-09-19T09:05:00.0000000+00:00',1),
('00000000-0000-0000-0000-0000000000e6','inventory','{"entityCode":"inventory"}','fp-e6-inventory','done',1,2,'{"updatedAtUtc":"2026-09-17T10:59:00.0000000+00:00","sourceId":"I2"}','fp-e6-inventory','same','{"updatedAtUtc":"2026-09-17T10:59:00.0000000+00:00","sourceId":"I2"}',NULL,'{"updatedAtUtc":"2026-09-18T10:59:00.0000000+00:00","sourceId":"I9"}',1,60,1,'RUN_BLOCKED','2026-09-18T11:00:00.0000000+00:00','2026-09-18T11:15:00.0000000+00:00',3),
('00000000-0000-0000-0000-0000000000f2','returns','{"entityCode":"returns"}','fp-f2-returns','extracting',0,NULL,NULL,NULL,'absent',NULL,NULL,NULL,1,15,1,NULL,'2026-09-21T01:00:00.0000000+00:00','2026-09-21T01:00:00.0000000+00:00',1),
('00000000-0000-0000-0000-0000000000f3','price_lists','{"entityCode":"price_lists"}','fp-f3-prices','failed',0,NULL,NULL,NULL,'absent',NULL,NULL,NULL,1,4,1,'INTERRUPTED_NO_CHECKPOINT','2026-09-21T02:00:00.0000000+00:00','2026-09-21T02:30:00.0000000+00:00',2),
('00000000-0000-0000-0000-0000000000f4','currencies','{"entityCode":"currencies"}','fp-f4-currencies','done',0,NULL,NULL,NULL,'absent',NULL,NULL,'{"updatedAtUtc":"2026-09-21T02:59:00.0000000+00:00","sourceId":"C9"}',1,9,1,NULL,'2026-09-21T03:00:00.0000000+00:00','2026-09-21T03:05:00.0000000+00:00',1),
('00000000-0000-0000-0000-0000000000f5','employees','{"entityCode":"employees"}','fp-f5-employees','failed',0,NULL,NULL,NULL,'absent',NULL,NULL,NULL,1,3,1,'extraction failed','2026-09-21T08:10:00.0000000+00:00','2026-09-21T08:20:00.0000000+00:00',2),
('00000000-0000-0000-0000-0000000000f6','assets','{"entityCode":"assets"}','fp-f6-assets','done',0,NULL,NULL,NULL,'absent',NULL,NULL,'{"updatedAtUtc":"2026-09-21T09:19:00.0000000+00:00","sourceId":"F9"}',1,22,1,'RUN_BLOCKED','2026-09-21T09:10:00.0000000+00:00','2026-09-21T09:40:00.0000000+00:00',3);

-- Batches: the v8 set plus a fenced dead_letter RUN_FAILED batch of the failed run
-- and a dead_letter UPLOAD_OUTCOME_UNKNOWN batch of the blocked run.
INSERT INTO etl_batches(batch_id,run_id,entity_name,schema_version,file_path,status,row_count,watermark_from_json,watermark_to_json,sha256,compressed_size,uncompressed_size,attempt_count,next_attempt_at_utc,created_at_utc,acknowledged_at_utc,last_error,send_attempt_id,upload_max_attempts,quarantine_code,row_version)
VALUES
('00000000-0000-0000-0000-000000000b01','00000000-0000-0000-0000-0000000000e1','payments',1,'spool/e1-b1.gz','ready',10,NULL,'{"updatedAtUtc":"2026-09-19T09:59:00.0000000+00:00","sourceId":"P10"}','hash-e1b1',1000,5000,0,'2026-09-19T10:00:01.0000000+00:00','2026-09-19T10:00:01.0000000+00:00',NULL,NULL,NULL,NULL,NULL,1),
('00000000-0000-0000-0000-000000000b02','00000000-0000-0000-0000-0000000000e2','clients',1,'spool/e2-b1.gz','acknowledged',42,NULL,'{"updatedAtUtc":"2026-09-18T07:59:00.0000000+00:00","sourceId":"A42"}','hash-e2b1',2000,9000,1,'2026-09-18T08:00:01.0000000+00:00','2026-09-18T08:00:01.0000000+00:00','2026-09-18T08:01:00.0000000+00:00',NULL,'00000000-0000-0000-0000-000000000d02',5,NULL,3),
('00000000-0000-0000-0000-000000000b03','00000000-0000-0000-0000-0000000000e3','products',1,'spool/e3-b1.gz','acknowledged',7,NULL,'{"updatedAtUtc":"2026-09-11T05:59:00.0000000+00:00","sourceId":"A7"}','hash-e3b1',900,4000,1,'2026-09-11T06:00:01.0000000+00:00','2026-09-11T06:00:01.0000000+00:00','2026-09-11T06:01:00.0000000+00:00',NULL,NULL,NULL,NULL,1),
('00000000-0000-0000-0000-000000000b05','00000000-0000-0000-0000-0000000000e5','shipments',1,'spool/e5-b1.gz','acknowledged',7,NULL,'{"updatedAtUtc":"2026-09-19T08:59:00.0000000+00:00","sourceId":"O7"}','hash-e5b1',800,3500,1,'2026-09-19T09:00:01.0000000+00:00','2026-09-19T09:00:01.0000000+00:00','2026-09-19T09:01:00.0000000+00:00',NULL,NULL,NULL,NULL,1),
('00000000-0000-0000-0000-000000000b06','00000000-0000-0000-0000-0000000000e2','clients',1,'spool/e2-b2.gz','uploading',30,NULL,'{"updatedAtUtc":"2026-09-18T07:59:00.0000000+00:00","sourceId":"A43"}','hash-e2b2',1500,7000,1,NULL,'2026-09-18T08:02:00.0000000+00:00',NULL,NULL,'00000000-0000-0000-0000-000000000d06',5,NULL,2),
('00000000-0000-0000-0000-000000000b07','00000000-0000-0000-0000-0000000000e6','inventory',1,'spool/e6-b1.gz','dead_letter',60,NULL,'{"updatedAtUtc":"2026-09-18T10:59:00.0000000+00:00","sourceId":"I9"}','hash-e6b1',3000,12000,0,NULL,'2026-09-18T11:05:00.0000000+00:00',NULL,'timeout after 30s — response never arrived','00000000-0000-0000-0000-000000000d07',5,'UPLOAD_OUTCOME_UNKNOWN',4),
('00000000-0000-0000-0000-000000000b08','00000000-0000-0000-0000-0000000000e1','payments',1,'spool/e1-b2.gz','retry_waiting',8,NULL,'{"updatedAtUtc":"2026-09-19T09:58:00.0000000+00:00","sourceId":"P8"}','hash-e1b2',400,1600,0,'2026-09-19T10:05:00.0000000+00:00','2026-09-19T10:01:00.0000000+00:00',NULL,'dns precheck failed','00000000-0000-0000-0000-000000000d08',5,NULL,2),
('00000000-0000-0000-0000-000000000b09','00000000-0000-0000-0000-0000000000f5','employees',1,'spool/f5-b1.gz','dead_letter',3,NULL,'{"updatedAtUtc":"2026-09-21T08:19:00.0000000+00:00","sourceId":"E3"}','hash-f5b1',300,1200,0,NULL,'2026-09-21T08:15:00.0000000+00:00',NULL,'RUN_FAILED',NULL,NULL,'RUN_FAILED',2),
('00000000-0000-0000-0000-000000000b10','00000000-0000-0000-0000-0000000000f6','assets',1,'spool/f6-b1.gz','dead_letter',22,NULL,'{"updatedAtUtc":"2026-09-21T09:19:00.0000000+00:00","sourceId":"F9"}','hash-f6b1',900,3600,0,NULL,'2026-09-21T09:25:00.0000000+00:00',NULL,'timeout after 30s — response never arrived','00000000-0000-0000-0000-000000000d09',5,'UPLOAD_OUTCOME_UNKNOWN',3);

-- The send-attempt ledger (008): the v8 evidence plus the 'unknown' uncertain
-- outcome attempt of the blocked f6 batch.
INSERT INTO etl_batch_send_attempts(attempt_id,batch_id,attempt_no,owner_id,admitted_at_utc,finished_at_utc,outcome,http_status,ack_payload_hash,ack_observed_at_utc,ack_valid,last_error)
VALUES
('00000000-0000-0000-0000-000000000d02','00000000-0000-0000-0000-000000000b02',1,'uploader-7','2026-09-18T08:00:02.0000000+00:00','2026-09-18T08:01:00.0000000+00:00','acknowledged',200,'ackhash-b02','2026-09-18T08:01:00.0000000+00:00',1,NULL),
('00000000-0000-0000-0000-000000000d06','00000000-0000-0000-0000-000000000b06',1,'uploader-7','2026-09-18T08:02:01.0000000+00:00',NULL,'admitted',NULL,NULL,NULL,NULL,NULL),
('00000000-0000-0000-0000-000000000d07','00000000-0000-0000-0000-000000000b07',1,'uploader-7','2026-09-18T11:05:01.0000000+00:00','2026-09-18T11:05:31.0000000+00:00','unknown',NULL,NULL,NULL,NULL,'timeout after 30s — response never arrived'),
('00000000-0000-0000-0000-000000000d08','00000000-0000-0000-0000-000000000b08',1,'uploader-7','2026-09-19T10:01:01.0000000+00:00','2026-09-19T10:01:02.0000000+00:00','precheck_failed',NULL,NULL,NULL,NULL,'dns precheck failed'),
('00000000-0000-0000-0000-000000000d09','00000000-0000-0000-0000-000000000b10',1,'uploader-9','2026-09-21T09:25:01.0000000+00:00','2026-09-21T09:25:31.0000000+00:00','unknown',NULL,NULL,NULL,NULL,'timeout after 30s — response never arrived');

-- Durable jobs: the v8 set plus the blocked jobs of the failed f5 and blocked f6
-- runs (terminate blocks the job, never finishes it).
INSERT INTO etl_jobs(job_id,command_id,run_id,mode,entities_json,configuration_version,status,command_payload_hash,acceptance_result_json,created_at_utc,updated_at_utc,row_version,dispatch_owner_id,dispatch_claimed_at_utc,claim_attempt_count,deferral_code,deferral_message,available_at_utc)
VALUES
('00000000-0000-0000-0000-000000000a01','00000000-0000-0000-0000-000000000101','00000000-0000-0000-0000-0000000000e4','bootstrap_full','[{"entityCode":"clients","oDataPath":"Catalog_Clients","keyField":"Ref_Key","updatedAtField":"UpdatedAt","deletedField":"DeletionMark","select":["Ref_Key","UpdatedAt","DeletionMark"],"syncMode":"incremental","pageSize":500,"overlapMinutes":10,"schemaVersion":1,"oDataVersion":3,"enabled":true,"keyFields":null,"updatedAtEdmType":"Edm.DateTimeOffset"}]',3,'pending','hash-j101','{"commandId":"00000000-0000-0000-0000-000000000101","status":"succeeded","completedAtUtc":"2026-09-20T07:00:00.0000000+00:00","data":{"accepted":true,"runId":"00000000-0000-0000-0000-0000000000e4","mode":"bootstrap_full"},"warnings":[],"resultVersion":1}','2026-09-20T07:00:00.0000000+00:00','2026-09-20T07:00:00.0000000+00:00',1,NULL,NULL,0,NULL,NULL,NULL),
('00000000-0000-0000-0000-000000000a02','00000000-0000-0000-0000-000000000606','00000000-0000-0000-0000-0000000000e3','entity_reload','[{"entityCode":"products","oDataPath":"Catalog_Products","keyField":"Ref_Key","updatedAtField":"UpdatedAt","deletedField":"DeletionMark","select":["Ref_Key","UpdatedAt","DeletionMark"],"syncMode":"incremental","pageSize":500,"overlapMinutes":10,"schemaVersion":1,"oDataVersion":3,"enabled":true,"keyFields":null,"updatedAtEdmType":"Edm.DateTimeOffset"}]',2,'finished','hash-j606','{"commandId":"00000000-0000-0000-0000-000000000606","status":"succeeded","completedAtUtc":"2026-09-11T06:00:00.0000000+00:00","data":{"accepted":true,"runId":"00000000-0000-0000-0000-0000000000e3","mode":"entity_reload"},"warnings":[],"resultVersion":1}','2026-09-11T06:00:00.0000000+00:00','2026-09-11T06:10:00.0000000+00:00',2,'dispatcher-old','2026-09-11T06:00:00.0000000+00:00',1,NULL,NULL,NULL),
('00000000-0000-0000-0000-000000000a03','00000000-0000-0000-0000-000000000202','00000000-0000-0000-0000-0000000000e6','entity_reload','[{"entityCode":"inventory","oDataPath":"Document_Inventory","keyField":"Ref_Key","updatedAtField":"Date","deletedField":"Posted","select":["Ref_Key","Date","Posted"],"syncMode":"incremental","pageSize":500,"overlapMinutes":10,"schemaVersion":1,"oDataVersion":3,"enabled":true,"keyFields":null,"updatedAtEdmType":"Edm.DateTimeOffset"}]',3,'blocked','hash-e6','{}','2026-09-18T10:59:00.0000000+00:00','2026-09-18T11:15:00.0000000+00:00',3,'dispatcher-old','2026-09-18T11:00:00.0000000+00:00',1,NULL,NULL,NULL),
('00000000-0000-0000-0000-000000000a04','00000000-0000-0000-0000-000000000404','00000000-0000-0000-0000-0000000000e1','entity_reload','[{"entityCode":"payments","oDataPath":"Document_Payments","keyField":"Ref_Key","updatedAtField":"Date","deletedField":"Posted","select":["Ref_Key","Date","Posted"],"syncMode":"incremental","pageSize":500,"overlapMinutes":10,"schemaVersion":1,"oDataVersion":3,"enabled":true,"keyFields":null,"updatedAtEdmType":"Edm.DateTimeOffset"}]',3,'running','hash-e1','{}','2026-09-19T09:59:00.0000000+00:00','2026-09-19T10:00:00.0000000+00:00',2,'worker-old','2026-09-19T10:00:00.0000000+00:00',1,NULL,NULL,NULL),
('00000000-0000-0000-0000-000000000a05','00000000-0000-0000-0000-000000000808','00000000-0000-0000-0000-0000000000f5','bootstrap_full','[{"entityCode":"employees","oDataPath":"Catalog_Employees","keyField":"Ref_Key","updatedAtField":"UpdatedAt","deletedField":"DeletionMark","select":["Ref_Key","UpdatedAt","DeletionMark"],"syncMode":"incremental","pageSize":500,"overlapMinutes":10,"schemaVersion":1,"oDataVersion":3,"enabled":true,"keyFields":null,"updatedAtEdmType":"Edm.DateTimeOffset"}]',3,'blocked','hash-j808','{"commandId":"00000000-0000-0000-0000-000000000808","status":"succeeded","completedAtUtc":"2026-09-21T08:00:02.0000000+00:00","data":{"accepted":true,"runId":"00000000-0000-0000-0000-0000000000f5","mode":"bootstrap_full"},"warnings":[],"resultVersion":1}','2026-09-21T08:00:00.0000000+00:00','2026-09-21T08:20:00.0000000+00:00',2,'dispatcher-2','2026-09-21T08:10:00.0000000+00:00',1,NULL,NULL,NULL),
('00000000-0000-0000-0000-000000000a06','00000000-0000-0000-0000-000000000909','00000000-0000-0000-0000-0000000000f6','entity_reload','[{"entityCode":"assets","oDataPath":"Catalog_FixedAssets","keyField":"Ref_Key","updatedAtField":"Date","deletedField":"Posted","select":["Ref_Key","Date","Posted"],"syncMode":"incremental","pageSize":500,"overlapMinutes":10,"schemaVersion":1,"oDataVersion":3,"enabled":true,"keyFields":null,"updatedAtEdmType":"Edm.DateTimeOffset"}]',3,'blocked','hash-j909','{"commandId":"00000000-0000-0000-0000-000000000909","status":"succeeded","completedAtUtc":"2026-09-21T09:00:02.0000000+00:00","data":{"accepted":true,"runId":"00000000-0000-0000-0000-0000000000f6","mode":"entity_reload"},"warnings":[],"resultVersion":1}','2026-09-21T09:00:00.0000000+00:00','2026-09-21T09:40:00.0000000+00:00',3,'dispatcher-2','2026-09-21T09:10:00.0000000+00:00',1,NULL,NULL,NULL);

-- Lifetime ownership (007): the v8 rows plus the scheduled/manual unresolved runs —
-- 'returns' held by running f2 and 'price_lists' retained by blocked f3 (scheduled:
-- owner_job_id NULL), 'currencies' released by succeeded f4 ('finalized'), and
-- 'employees'/'assets' retained by the failed f5 and blocked f6 manual runs.
INSERT INTO etl_entity_ownership(entity_name,owner_run_id,owner_job_id,ownership_epoch,acquired_at_utc,released_at_utc,release_reason,updated_at_utc,row_version)
VALUES
('clients','00000000-0000-0000-0000-0000000000e2',NULL,1,'2026-09-18T08:00:00.0000000+00:00',NULL,NULL,'2026-09-18T08:00:00.0000000+00:00',1),
('orders','00000000-0000-0000-0000-0000000000e2',NULL,1,'2026-09-18T08:00:00.0000000+00:00',NULL,NULL,'2026-09-18T08:00:00.0000000+00:00',1),
('payments','00000000-0000-0000-0000-0000000000e1',NULL,1,'2026-09-19T10:00:00.0000000+00:00',NULL,NULL,'2026-09-19T10:00:00.0000000+00:00',1),
('shipments','00000000-0000-0000-0000-0000000000e5',NULL,1,'2026-09-19T09:00:00.0000000+00:00',NULL,NULL,'2026-09-19T09:00:00.0000000+00:00',1),
('inventory','00000000-0000-0000-0000-0000000000e6','00000000-0000-0000-0000-000000000a03',1,'2026-09-18T11:00:00.0000000+00:00',NULL,NULL,'2026-09-18T11:00:00.0000000+00:00',1),
('products','00000000-0000-0000-0000-0000000000e3','00000000-0000-0000-0000-000000000a02',1,'2026-09-11T06:00:00.0000000+00:00','2026-09-11T06:10:00.0000000+00:00','finalized','2026-09-11T06:10:00.0000000+00:00',2),
('returns','00000000-0000-0000-0000-0000000000f2',NULL,1,'2026-09-21T01:00:00.0000000+00:00',NULL,NULL,'2026-09-21T01:00:00.0000000+00:00',1),
('price_lists','00000000-0000-0000-0000-0000000000f3',NULL,1,'2026-09-21T02:00:00.0000000+00:00',NULL,NULL,'2026-09-21T02:00:00.0000000+00:00',1),
('currencies','00000000-0000-0000-0000-0000000000f4',NULL,1,'2026-09-21T03:00:00.0000000+00:00','2026-09-21T03:10:00.0000000+00:00','finalized','2026-09-21T03:10:00.0000000+00:00',2),
('employees','00000000-0000-0000-0000-0000000000f5','00000000-0000-0000-0000-000000000a05',1,'2026-09-21T08:10:00.0000000+00:00',NULL,NULL,'2026-09-21T08:10:00.0000000+00:00',1),
('assets','00000000-0000-0000-0000-0000000000f6','00000000-0000-0000-0000-000000000a06',1,'2026-09-21T09:10:00.0000000+00:00',NULL,NULL,'2026-09-21T09:10:00.0000000+00:00',1);

-- Immutable ownership bindings (007): the exact epoch contract committed at claim.
INSERT INTO etl_run_ownership_bindings(run_id,entity_name,expected_epoch,acquired_at_utc) VALUES
('00000000-0000-0000-0000-0000000000e2','clients',1,'2026-09-18T08:00:00.0000000+00:00'),
('00000000-0000-0000-0000-0000000000e2','orders',1,'2026-09-18T08:00:00.0000000+00:00'),
('00000000-0000-0000-0000-0000000000e1','payments',1,'2026-09-19T10:00:00.0000000+00:00'),
('00000000-0000-0000-0000-0000000000e5','shipments',1,'2026-09-19T09:00:00.0000000+00:00'),
('00000000-0000-0000-0000-0000000000e6','inventory',1,'2026-09-18T11:00:00.0000000+00:00'),
('00000000-0000-0000-0000-0000000000e3','products',1,'2026-09-11T06:00:00.0000000+00:00'),
('00000000-0000-0000-0000-0000000000f2','returns',1,'2026-09-21T01:00:00.0000000+00:00'),
('00000000-0000-0000-0000-0000000000f3','price_lists',1,'2026-09-21T02:00:00.0000000+00:00'),
('00000000-0000-0000-0000-0000000000f4','currencies',1,'2026-09-21T03:00:00.0000000+00:00'),
('00000000-0000-0000-0000-0000000000f5','employees',1,'2026-09-21T08:10:00.0000000+00:00'),
('00000000-0000-0000-0000-0000000000f6','assets',1,'2026-09-21T09:10:00.0000000+00:00');

INSERT INTO watermarks(entity_name,committed_cursor_json,extracting_cursor_json,last_run_id,updated_at_utc,generation,domain_fingerprint)
VALUES
('clients','{"updatedAtUtc":"2026-09-11T05:59:00.0000000+00:00","sourceId":"A7"}',NULL,'00000000-0000-0000-0000-0000000000e3','2026-09-11T06:10:00.0000000+00:00',3,'fp-e3-clients');

INSERT INTO agent_state(key,value_json,updated_at_utc)
VALUES
('local_etl_pause','{"paused":false}','2026-09-19T09:00:00.0000000+00:00');

INSERT INTO config_snapshots(config_version,config_json,config_hash,received_at_utc,activated_at_utc,status)
VALUES
(3,'{"mode":"Normal","etlEntities":[]}','cfghash-3','2026-09-17T00:00:00.0000000+00:00','2026-09-17T00:00:05.0000000+00:00','active');

INSERT INTO command_payload_conflicts(event_id,command_id,event_code,severity,original_declared_hash_fingerprint,incoming_declared_hash_fingerprint,original_command_status,first_seen_at_utc,last_seen_at_utc,occurrence_count)
VALUES
('00000000-0000-0000-0000-000000000cf1','00000000-0000-0000-0000-000000000606','COMMAND_PAYLOAD_CONFLICT','critical','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb','completed','2026-09-12T00:00:00.0000000+00:00','2026-09-12T01:00:00.0000000+00:00',2);

COMMIT;
