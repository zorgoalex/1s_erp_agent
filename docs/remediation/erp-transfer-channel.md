# ERP transfer channel — batch upload and run completion

Date: 2026-09-26. No migration. Found while preparing the ERP Agent API specification.

## Defect

ETL batch upload (up to 100 MB gzip) and run completion went through the ordinary ERP
pipeline, `AddStandardResilienceHandler` with a 10 s attempt timeout and a 30 s total.
A legitimately slow upload was cut off after it had been sent. That is an **unknown
outcome**: the batch was quarantined, the run blocked, and an operator had to step in. The
long poll had the same class of bug earlier (A06).

## Fix

- New named client `ErpTransfer` (`ErpClientRegistration.AddErpTransferClient`):
  - one attempt only, with a timeout of `Erp:TransferTimeoutSeconds` (default 300, validated
    30..3600);
  - `HttpClient.Timeout` = that value + 15 s;
  - `ResponseContentRead`, so a response that stalls after its headers is bounded by the
    same budget;
  - responses larger than 1 MB are refused.
- `ErpClient` sends `UploadBatchWithEvidenceAsync` and `CompleteEtlRunRawAsync` through it.
  The unused `CompleteEtlRunAsync(object)` now delegates to the raw call.
- Removed `AddErpClient`, an unused registration that would route uploads through the
  10 s pipeline.
- The completion claim hold is `max(5 min, transfer budget + 1 min)`, so a claim can never
  expire and be re-sent while its own call is still in flight.
- ERP must acknowledge a 100 MB batch within the budget. At the default 300 s that needs
  about 2.7 Mbit/s of sustained uplink; raise `TransferTimeoutSeconds` for slower links.

## Review

An independent review found one must-fix and several should-fix items, all addressed:

- must-fix: the ACK body read was unbounded after the headers arrived (fixed with
  `ResponseContentRead` plus the size cap);
- the completion claim could expire during the call (claim hold now derived from the
  transfer budget);
- the budget test was not specific (now asserts `TimeoutRejectedException` in under 8 s);
- a test was added for a stalled ACK body;
- the unused registration was removed.

Left open: each channel's primary handler reloads the client certificate on every handler
rotation, and the loaded certificates are never disposed; caching the certificate is a
follow-up.

## Tests

`ErpTransferResilienceTests` (unit, real production DI registration, transports stubbed):
- a 12 s upload is not cut off;
- a 12 s completion is not cut off;
- a 503 is sent exactly once;
- routing goes through the transfer channel;
- the transfer budget fires;
- a stalled ACK body is bounded;
- the budget ordering holds.

`EtlCompletionClaimHoldTests` (integration) checks the claim hold against the transfer
budget.

RED on the pre-fix build: the upload, completion and routing tests failed; the first two
were cut off at 10 s.
