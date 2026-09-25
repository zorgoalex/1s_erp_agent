# Stage 5/6 bundle — OData limits (A10b), certificate expiry, ETL operator CLI, runbook, database presence guard

Date: 2026-09-26. No migration.

## OData page and row limits (A10b)

- `EtlOptions.MaxODataPageBytes` (default 64 MB, validated 1 MB..256 MB) and
  `MaxODataRowBytes` (default 8 MB, at least 64 KB and not above the page limit).
- A declared `Content-Length` above the page limit is refused before the body is read.
- Every page body is read through `BoundedReadStream`, which wraps the already
  decompressed stream (`AutomaticDecompression`), so compressed responses are bounded by
  their decompressed size.
- Every record is checked in UTF-8 bytes (`JsonMarshal.GetRawUtf8Value`), not UTF-16
  characters: Cyrillic text cannot exceed the limit twice over.
- Exceeding either limit throws `InvalidDataException`; the run fails closed.
- Peak memory is a small multiple of the page limit, because the page is parsed as one
  `JsonDocument` (parse buffer plus token metadata). That is why the upper bound is 256 MB.
  A streaming reader would remove the multiple; it is not part of this bundle.
- Open: there is no cap on the number of pages, so a server that keeps returning the same
  `nextLink` loops until the spool limit stops the run.

## Certificate expiry

- `CertificateExpiryPolicy.CrossedThreshold` maps the remaining time to 30/14/7/3/1 days,
  or 0 when expired.
- `HeartbeatWorker` logs one entry per crossed threshold per process:
  - `CERTIFICATE_EXPIRING` at 30 and 14 days: Warning;
  - at 7, 3 and 1 days: Error;
  - `CERTIFICATE_EXPIRED`: Critical.
- The expiry date is read with `CertificateLoader.TryReadNotAfter`, which skips the
  validity check. Otherwise the validity-checked load would throw for an expired
  certificate and hide `CERTIFICATE_EXPIRED`.
- A certificate with no threshold crossed resets the sequence, so a renewed certificate
  gets its own warnings without a restart.

## Operator CLI (R1/D1)

- `--etl-resolve-run <runId> --decision abandon|retry|rebaseline --operator <id>
  --verification "<text>" --workers-stopped`
- `--etl-reset-domain <entity> --generation <N> --operator <id> --reason "<text>"`

Both commands:

- take the service's single-instance lock before migrations or any other database access,
  so they fail while the service runs;
- cannot be combined in one call.

Exit codes:

| Code | Meaning |
|---|---|
| `0` | Done, or already resolved with the same request |
| `1` | Argument or precondition error, for example the lock is held |
| `2` | The store refused (reason printed) |
| `3` | Already resolved with a different request |
| `4` | The database is missing although agent state exists |

## Database presence guard

- After the first successful initialization, a marker `agent.db.initialized` is written
  next to the database. `--migrate` writes it too.
- On service start, and before any CLI command other than `--migrate`, the guard checks
  the `SqliteConnectionFactory` database path. If the database is missing but the marker,
  maintenance backups (`backups/agent-*.db`) or spool files exist, the start or command is
  refused.
- This stops a lost database from being silently recreated empty (for example by
  `--integrity-check`) and then passing the start check.
- Creating a new database stays an explicit operator decision: restore a backup, or run
  `--migrate`.
- In-process test hosts that inject a store without the factory skip the guard.

## Runbook

`docs/etl-runbook.md` (Russian) covers:

- blocked and failed codes;
- R1 resolution;
- D1 domain reset;
- source identity verdicts;
- disk and spool;
- a lost database;
- certificates.

The start-time recovery it describes (orphaning admitted attempts, blocking interrupted
runs) arrives with the C1 cutover; the runbook says so.

## Review

An independent review found these issues, all fixed:

- CLI commands recreated a lost database before any check;
- the row limit counted characters, not bytes;
- `CERTIFICATE_EXPIRED` could never be logged;
- the page limit's upper bound was too high;
- the maintenance commands ran migrations before taking the lock;
- runbook exit codes and refusals were incomplete.

The review nits are fixed as well:

- the resolve switch now has a default;
- combining the two commands is rejected;
- the certificate warning resets after renewal.

## Verification

Worktree and clean LF clone, Release: 726/726 integration and 219/219 unit.
Evidence is in `local-data/remediation-2026-09-26/stage5-6-root/final`.
