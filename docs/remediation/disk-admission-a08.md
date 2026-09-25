# A08 — ETL never consumes the command disk reserve; A07 B4 permission wiring

Date: 2026-09-26. No migration.

## Rule

ETL writing must keep `StorageOptions.MinimumReservedBytesForCommands` of the data
volume's ACTUAL free space intact. The spool quota alone is not enough: other files can
fill the disk. The reserve protects SQLite, command results and the outbox.

## Implementation

- **`IDiskSpaceProbe`.** `DriveInfoDiskSpaceProbe` reports free space for the nearest
  existing directory. On Windows it uses `GetDiskFreeSpaceEx`, which also covers UNC
  shares and folder mount points.
- **`EtlDiskAdmission.Allows(free, reserve, projected)`** is strict (`free - projected >
  reserve`).
- **`IsDiskFull`** recognizes, per platform:
  - Windows: `ERROR_DISK_FULL` 112, `ERROR_HANDLE_DISK_FULL` 39 and
    `ERROR_DISK_QUOTA_EXCEEDED` 1295 as Win32 HRESULTs;
  - elsewhere: ENOSPC 28 and EDQUOT 122.
- **`FileSpoolStore`** is constructed with a probe and a reserve. It checks before the
  batch file is created, projecting batch headroom (min(64 MB, max batch)), and while
  writing:
  - every 256 rows;
  - every 4 MB of payload;
  - before any single row of 4 MB or more, projecting that row's size;
  - once the file passes the compressed-size cap.

  A refusal, an unknown free-space state (probe exception) or a real disk-full IOException
  raises `EtlDiskReserveException`. The temp file is removed; a failed removal never masks
  the original error, and a leftover `.tmp` is quarantined at the next start. Nothing is
  registered, and no ready or acknowledged data is ever deleted. Default constructor
  arguments (no probe, reserve 0) keep the previous behaviour for existing callers.
- **Legacy `OnecEtlWorker`** skips a pass while the reserve plus headroom is not free.
  Probe errors fail closed without stopping the host. C1 replaces this worker with the
  durable pipeline, which blocks the run with `DISK_RESERVE`.
- **A07 B4:** the legacy upload worker gates run completion on `CanCompleteEtlRuns`
  instead of the upload permission. The two are identical today, so behaviour does not
  change.

## Evidence

`EtlDiskAdmissionTests`, 23 tests:
- strictness of `Allows`;
- disk-full codes per platform;
- preflight refusal with no file;
- a mid-batch drop leaving no file;
- a single large row;
- the byte-volume trigger;
- probe failure;
- the normal path;
- the no-probe path;
- a negative reserve;
- the real probe.

**Independent review** (fresh agent): no blockers. Four should-fix findings were all
fixed:
1. A probe exception could stop the host.
2. A single huge row, or a sub-256-row batch, was not checked mid-write.
3. Cleanup and disposal errors could mask the original error.
4. Disk-full codes collided with Linux errno values.

## Limits

- **Deliberate disk filling was not run.** The plan forbids filling the working disk; an
  isolated small volume is a stage-8 acceptance item.
- **SQLite growth is not counted.** SQLite/WAL growth from ETL's own registrations is not
  charged against the reserve; the reserve itself is the margin.
- **Buffered bytes are not counted.** Up to about 64 KB of stream buffers are not yet on
  disk at check time.
