# E2 — agreed ERP error codes

Date: 2026-09-27. No migration. Agreed with ERP over agent-bridge (`to-onec/0002`,
`to-onec/0003`, `to-erp/0002`).

## Reading the code

- **Exception.** Every non-2xx ERP answer raises `ErpApiException : HttpRequestException`,
  which carries the status, `ApiError.code` and `Retry-After`. Existing handling keeps working.
- **What is read.** At most the first 4 KiB of the body, streamed through `Utf8JsonReader`.
  The code is the first **top-level** string property `code` of the form `[A-Z0-9_]{1,64}`.
  A tail cut off inside a later property is fine, so a code followed by a long `message` is
  still found.
- **Time limit.** The read is bounded by 5 s: after `ResponseHeadersRead` no HTTP timeout
  applies, so a stalled body would otherwise hang the caller.
- **Nothing kept.** The body is never logged or stored. The exception message holds only
  the status and the code.

## Behaviour

| Call | Answer | Agent |
|---|---|---|
| `PUT commands/{id}/result` | `409 RESULT_CONFLICT` | Stops delivery. The outbox row becomes `dead_letter` and stays as evidence. Logs `RESULT_CONFLICT` (Critical). The row counts as a dead letter, not as pending. |
| `POST etl/runs/{id}/complete` | `422 BATCH_PAYLOAD_INVALID`, `409 SOURCE_IDENTITY_MISMATCH`, `409 RUN_GENERATION_CLOSED` | No retries. The run is blocked with the code under its exact completion claim (`BlockRunCompletionAsync`). Watermarks are untouched; resolution goes through R1. |
| `POST etl/batches` | `503 BATCH_NOT_STORED_RETRYABLE` | ERP attests nothing was stored, so the attempt is ledger-proven unsent: outcome `precheck_failed`, reason `REMOTE_NOT_STORED`. The same batch, with the same `batchId` and `Idempotency-Key`, is sent again after `max(Retry-After, backoff)`, capped at 15 min, within the durable attempt bound (5). Exhausting the bound blocks the run as `UPLOAD_ATTEMPTS_EXHAUSTED`. |
| any other non-2xx or timeout after sending | — | Unchanged: unknown outcome, batch quarantined, no resend. |

The same code under a different status does not match. For example, `500 RESULT_CONFLICT`
is an ordinary failure.

## Review

An independent review found one must-fix issue: a code at the start of a body longer than
4 KiB was lost, because a truncated document failed to parse. It is fixed with the streaming
reader.

Its should-fix items are applied:
- a time bound on reading the error body;
- a cap on `Retry-After`;
- a result conflict no longer counted as pending, and counted as a dead letter;
- a failed store write no longer stops the delivery worker.

The review confirmed:
- reusing `precheck_failed` is safe for every O2 invariant;
- a 503 reaches the agent through the transfer channel with its code intact.

## Tests

- **`ErpApiErrorTests`:**
  - code and `Retry-After` extraction;
  - malformed or missing codes;
  - a code at the start of a long body;
  - top-level-only extraction;
  - the 4 KiB bound;
  - the status/code pairing.
- **`ErpTransferResilienceTests`:** the code survives the real transfer channel.
- **`EtlC1ReviewFixTests` (E2_\*):**
  - not stored → retried, not quarantined;
  - other failures still quarantine;
  - the attempt bound is exactly 5, then `UPLOAD_ATTEMPTS_EXHAUSTED`;
  - `Retry-After` is capped.
- **`ErpErrorCodeStoreTests`:**
  - result conflict: store transition, metrics, and the delivery worker (it stops only on the
    agreed conflict);
  - completion block fenced by the claim;
  - the permanent-refusal predicate.
