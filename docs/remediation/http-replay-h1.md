# H1 — byte-identical ETL completion replay; redacted ERP errors

Date: 2026-09-26. No migration.

## What was added

**`IErpClient.CompleteEtlRunRawAsync(runId, completePayloadJson)`** sends the F1 stored
`complete_payload_json` as the exact UTF-8 bytes, never re-serialized:
- no BOM;
- `Content-Type: application/json; charset=utf-8`;
- `Content-Length` set; no chunking and no request compression in the pipeline;
- `Idempotency-Key` = run id.

Every fenced retry of one completion therefore presents identical body bytes and the same
dedup identity. Per-request `X-Request-Id`/`X-Correlation-Id` stay fresh: they are
transport correlation, not payload identity. ERP must deduplicate on `Idempotency-Key`,
which is still unproven.

Invalid UTF-16, such as a lone surrogate, throws instead of being silently replaced. The
interface method has a default body that throws `NotSupportedException`, so hand-written
fakes fail closed.

**`ErpClient` error redaction** (a stage-6 item): a non-success ERP response no longer
copies up to 2 KB of the body into the exception message, which is logged and may be
persisted, for example as a completion retry error. Only the status and the body size are
reported.

## Evidence

The tests (`ErpClientCompletionReplayTests`) cover:
- identical bytes and dedup headers across two sends of a deliberately non-canonical
  payload;
- non-success status mapping;
- refusal of an empty payload without sending;
- fail-closed fakes;
- invalid UTF-16;
- redaction of the error body.

Result: 8/8.

**Independent review** (fresh agent): no blockers. It confirmed the pipeline does not
alter request bodies: `AutomaticDecompression` affects responses only, the POST is not
replayed by the resilience handler, and `ByteArrayContent` sets the length. Two review
findings were fixed: strict encoding and error-body leakage. It also found that the
legacy worker still called the object overload. That caller is replaced in C1, which is
the only intended caller of the raw method.

## Limits

- **No caller yet:** nothing calls the method until C1 lands.
- **ERP dedup is not verified.**
