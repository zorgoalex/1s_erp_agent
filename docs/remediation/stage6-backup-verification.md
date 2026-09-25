# Stage 6 — verified backups

Date: 2026-09-26. No migration.

## Defects in the daily maintenance backup

1. **The backup was never verified, and rotation ran right after it.** A broken copy
   could push the good backups out of retention.
2. **The live database's integrity was checked only after the backup.** A corrupt
   database was copied, older good backups were rotated away, and destructive cleanup still
   ran on the corrupt database.
3. **The pooled destination connection kept every backup file open.** Rotation later in
   the same process failed with `IOException`. In a long-running service backups piled up
   and the maintenance cycle kept failing.
4. **Rotation ordered by file-system creation time.** Copy tools can change that time.
5. **Found in review: the copy inherits WAL mode.** Every verification left `-wal`/`-shm`
   sidecars that nothing ever removed.

## Behaviour now (`MaintenanceWorker.RunCycleAsync` / `RunBackupAsync`)

1. Delete leftover `agent-*.db.tmp*` files (this agent's own interrupted copies).
2. Run `PRAGMA integrity_check` on the live database. If it fails:
   - log `SQLITE_INTEGRITY_FAILED` (Critical);
   - take no backup;
   - skip cleanup and checkpoint;
   - keep the existing backups for the restore procedure in `docs/operations.md`.
3. Write the online backup to `agent-<ts>.db.tmp`. The destination connection is not
   pooled, and the copy is switched to `journal_mode=DELETE` so it is a single
   self-contained file. A restored copy is switched back to WAL by the migrator.
4. Check the copy with `SqliteBackupVerifier`: read-only, unpooled, `integrity_check`, and
   a non-empty `schema_migrations`. If it fails, log `BACKUP_VERIFICATION_FAILED` and
   delete the copy; existing backups stay.
5. Rename the copy to `agent-<ts>.db`.
6. Rotate: keep the newest `BackupRetentionCount` backups (at least 1), comparing
   timestamped names `agent-yyyyMMdd-HHmmss.db`. Other `agent-*.db` files, such as an
   operator's copy, are left alone. A file that cannot be deleted produces a
   `BACKUP_ROTATION_FAILED` warning and does not abort the cycle.

## Review

An independent review reproduced the WAL-sidecar defect with the SQLite engine. It is
fixed, and a test that lists every file in the backup directory would fail without the
fix (verified). The review's should-fix items are also applied:

- a locked file no longer aborts the cycle;
- a test covers skipping cleanup when the database is corrupt;
- a test covers the stale `.tmp` sweep;
- rotation uses only timestamped names.

## Tests

`MaintenanceBackupTests` (8):

- a verified single-file backup;
- rotation by name, not file time;
- a corrupt live database, where old backups are kept;
- an invalid copy that is discarded without displacing a good one;
- the verifier rejects a non-database file;
- the stale `.tmp`/sidecar sweep and foreign names;
- cleanup skipped for a corrupt database and run for a healthy one.

RED on the pre-fix build: 4 tests (file locks, verification, ordering).
