# A10c — bounded OData page retry

Date: 2026-09-26. No migration.

## Problem

A transient 1C failure while reading a page failed the whole extraction run. The failure
could be a dropped connection, a timeout, or a 408, 429 or 5xx response. On the durable
path (C1), a failed run needs a manual R1 `retry`.

Two more problems:

- A stalled response body could hang the read: `HttpClient.Timeout` ends at the response
  headers when `ResponseHeadersRead` is used.
- A repeating `@odata.nextLink` looped forever. The pre-fix run grew a test host to about
  24 GB before it was killed.

The OData client also carried `AddStandardResilienceHandler`. That handler retries
408/429/5xx on its own and applies a 10 s per-attempt timeout and a 30 s total timeout,
which cuts off long 1C queries.

## Rule

One retry layer owns OData reads: `OnecODataClient` retries a whole page.

- The page is parsed completely (`JsonDocument`) before any of its rows is yielded, and
  the read is idempotent. A retry therefore never duplicates or skips rows. In `$skip`
  mode the next skip comes from the parsed page, so a retry repeats the same skip.
- Retried: connection failures (`HttpRequestException` without status, except
  `SecureConnectionError` and `ConfigurationLimitExceeded`), 408/429/500/502/503/504, an
  `IOException` while reading the body, and a timeout.
- A timeout is an `OperationCanceledException` while the caller's token is not
  cancelled. Each attempt, body included, is bounded by `HttpClient.Timeout` through a
  linked `CancellationTokenSource`.
- Not retried:
  - A10 limit violations (`InvalidDataException`);
  - malformed JSON;
  - other statuses (400/401/403/404 and so on);
  - TLS failures;
  - caller cancellation.
- `EtlOptions.ODataPageRetries`: default 3, valid 0..10; 0 disables the retry.
- `EtlOptions.ODataRetryBaseDelayMilliseconds`: default 1000, valid 0..60000. The delay
  doubles per attempt, capped at 60 s.
- The OData HTTP client no longer has `AddStandardResilienceHandler`. The health client
  keeps it.
- Loop guard: the initial page URI and every continuation URI are recorded as absolute
  URIs. A repeat throws `InvalidDataException` before the page is read again.

## Known limits

- `Retry-After` is not honoured.
- A server whose `$skiptoken` changes on every page without ever ending is not detected.
  The spool quota bounds that run.
- The extraction claim has no time lease, so a long retry holds the entity longer but
  cannot lose the claim.

## Review

The independent review found the second retry layer. With both layers a page could
be attempted 16 times, and Polly's `TimeoutRejectedException` escaped the new retry. It
also found:

- the first page was missing from the loop guard;
- TLS failures were retried;
- test gaps: a mid-body failure, `$skip` mode, and a self-referencing first page.

All of these are fixed.

## Tests

`tests/ErpOnecAgent.UnitTests/OnecODataRetryTests.cs`, 24 tests:

- each transient status is retried and each permanent status is not;
- a network failure and a timeout are retried;
- a stalled body times out and is retried;
- a mid-body `IOException` is retried without duplicating rows;
- retries are bounded, and 0 retries disables them;
- limit violations, malformed JSON and caller cancellation are not retried;
- continuation-page and `$skip`-page retries repeat the same URI;
- a repeating `nextLink` and a self-referencing first page are refused;
- TLS failures are classified as not transient.

RED on the pre-fix build:

- 10 retry tests failed;
- the repeating-`nextLink` test never terminated.

Evidence is in `local-data/remediation-2026-09-26/a10c-root/`.
