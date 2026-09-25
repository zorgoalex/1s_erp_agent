# Migration checksum portability — pinned compatibility catalog for published migrations 001-007

Date: 2026-09-25; **revision 2 (2026-09-25)** — root clean-LF-checkout review
(7 pass/1 fail focused run,
`repo_1c-agent/local-data/remediation-2026-09-25/migration-checksum-root/clean-review-focused.log`)
found the CRLF-ledger test seeded build-dependent resource checksums instead of
the pinned historical values (now seeded explicitly so it is a real
reverse-direction regression on any checkout), and that
`IsAcceptedStoredChecksum` consulted `resourceChecksum` only for unlisted
versions (helper now enforces the resource pin itself; production caller order
already checked first, so this is contract consistency, not a proven bypass).
TRX overwrite observed in the first full run is fixed by `LogFilePrefix`.
Worktree: `agents/worktrees/migration-checksum-portability`,
baseline main `62e559d` (578 tests). Model: swe-2-high. Native Windows SDK:
`D:/WORK/CNC_Milling/WORK_CNC/SOFT/1C/1C-agent/repo_1c-agent/.dotnet/dotnet.exe`
(10.0.400), Release builds/tests from this worktree. Evidence:
`local-data/remediation-2026-09-25/migration-checksum-portability/`.

## Problem

`SqliteMigrator` recorded and verified `SHA256(UTF8(decoded resource text))` of
the embedded migration `.sql`. The published git blobs are LF, while this
local checkout contains CRLF bytes for 002/003/005/006 (the specific
line-ending origin of that historical mix — e.g. `core.autocrlf` — is not
proven), so the *same published migration* produces different ledger checksums
on different checkouts: a database migrated by one build fails `ApplyAsync` on
the other with `Checksum mismatch for applied migration NNN`. Separately, the
applied-version check verified only `checksum`; the ledger `name` column was
never compared, so a renamed ledger row for a correct checksum was silently
accepted.

## Policy implemented (exact)

`MigrationChecksumCatalog` (`src/.../Persistence/Sqlite/MigrationChecksumCatalog.cs`,
internal) is a small explicit pinned catalog keyed by exact `version` +
`fileName` for the already-published migrations 001-007. Each entry lists the
canonical published checksum (git-blob LF form, verified via read-only
`git show HEAD:...` against `repo_1c-agent` at `62e559d`) plus the approved
historical variant checksums (the exact CRLF checkout bytes observed for that
same SQL — `hash-evidence.txt`).

1. **Resource pin.** For a cataloged version the embedded resource must hash to
   one of that version's approved checksums *and* the resource file name must
   equal the cataloged name; any other bytes (meaningful SQL drift, renamed
   file, foreign migration) fail closed before any ledger read. No semantic
   normalization — only the literal pinned hashes are admitted.
2. **Stored checksum.** For a cataloged version a pre-existing ledger row is
   accepted iff `name` equals the resource file name (fail-closed name
   validation, new) *and* `checksum` is one of that version's approved hashes.
   The matching historical checksum is **retained unchanged** — no ledger
   upgrade, no data rewrite, no `applied_at_utc` touch.
3. **New records.** A migration applied now records the canonical published
   (LF) checksum for cataloged versions 1-7.
4. **Unlisted versions** keep the strict pre-existing contract: recorded and
   accepted checksum = raw hash of the embedded resource bytes.
5. **Checkout policy.** Root `.gitattributes` pins
   `src/ErpOnecAgent.Infrastructure/Persistence/Migrations/*.sql text eol=lf`
   so future checkouts embed the published bytes; it does not restage or
   rewrite the existing 001-007 files, and migrations 001-007 SQL, existing
   ledger checksums/names/applied_at_utc, and all durable data are untouched.
   No schema 008 (reserved for O2).

Rejection paths unchanged in shape: wrong hash, a hash borrowed from another
migration, a hash of drifted SQL, or a mismatched name all throw
`InvalidOperationException` and leave the seeded ledger rows and populated work
rows logically unchanged (row-for-row equality asserted by the tests below).
Note `ApplyAsync` unconditionally runs its startup PRAGMAs and
`CREATE TABLE IF NOT EXISTS schema_migrations` and commits each migration in
its own transaction, so a rejection does not imply database-file-byte identity
or whole-batch atomicity — the tests compare logical ledger/data rows of a
populated valid v6 database, not file bytes.

## Admitted hash pairs (SHA-256, hex upper)

| ver | file | canonical published (LF, git blob) | approved historical (CRLF checkout) |
|-----|------|------------------------------------|-------------------------------------|
| 1 | 001_initial.sql | `6BB8EC13…F7959A` | — (LF only) |
| 2 | 002_retry_budgets.sql | `95DA7777…192F04` | `EB4D22E6…CA3590` |
| 3 | 003_ordering_claims.sql | `C9F6EF47…238981B` | `0BF8E512…AAC44CB` |
| 4 | 004_command_payload_conflicts.sql | `E75CAF3E…C23A6AE` | — (LF only) |
| 5 | 005_durable_etl_jobs.sql | `2D30F711…212657` | `38BB53A4…911A34` |
| 6 | 006_etl_finalize.sql | `D1B20CE2…A6FB3` | `1634CFEB…D3166A` |
| 7 | 007_etl_ownership.sql | `8F4DBACB…8B8658` | — (LF only) |

Full hashes: `local-data/remediation-2026-09-25/migration-checksum-portability/hash-evidence.txt`
(git blob digests from `git -C repo_1c-agent show HEAD:...`; CRLF digests from
this worktree's files, CR/LF byte counts recorded; no BOM in any file).

## Runtime evidence

### RED on pre-fix code (real SQLite, no compile-fail counted)

`red-portability-001/002.trx` + `red-test-run*.log` — new
`MigrationChecksumPortabilityTests` on the unmodified migrator: 3 runtime
failures —
`Published_LF_ledger_is_accepted_by_this_build_and_preserved_byte_for_byte`
(InvalidOperationException "Checksum mismatch for applied migration
002_retry_budgets.sql" — the exact reported defect),
`Fresh_apply_records_canonical_published_checksums_for_001_007` (recorded CRLF
hashes), `Wrong_ledger_name_for_applied_version_is_rejected_and_leaves_
database_unchanged` (no exception — name never validated).

### GREEN after fix

`green-portability-002.trx` (8/8), `green-catalog-003.trx` (8/8 unit,
including the helper-drift fail-closed contract test — its RED is
`red-helper-drift-001.trx`/`red-helper-drift.log`),
`green-migrations-001.trx` (24/24 migration review tests).
Full Release suite after `--locked-mode` restore and `--no-restore` rebuild
(`rev2-restore.log`, `rev2-build.log`): `rev2-full_net10.0_*.trx` —
two unique per-project TRX files via `LogFilePrefix` (the earlier single
`LogFileName` run overwrote the integration TRX with the unit one — observed,
corrected): 547 integration + 47 unit = 594 total (baseline 578 + 16 new),
0 failed, 0 skipped (`rev2-full-test-run.log`).

### Actual different-checkout/build resource bytes

`lf-scenario/run-lf-scenario.ps1` (scripted outside product tests) stages
throwaway copies of `src/` under `%TEMP%/mcp-lf-scenario`, normalizes the
migration `.sql` files to LF or leaves them CRLF, builds the MigratorHarness
console app against each variant, and runs real SQLite seed+migrate cycles.
Results in `lf-scenario-results/` (`lf-scenario-run.log`: ALL SCENARIO LEGS
PASSED):

- `lf-fixed` — LF embedded resources (verified: resource dump equals the
  published digests) + ledger seeded with CRLF checksums → `ApplyAsync`
  succeeds; ledger rows 1-6 retain the CRLF checksums byte-for-byte; v7 row
  appended with the canonical published checksum.
- `lf-prefix` — same LF build with the baseline migrator (baseline
  `SqliteMigrator.cs` preserved under `baseline/`) → fails with the checksum
  mismatch (the RED leg).
- `crlf-fixed` — CRLF embedded resources + LF-seeded ledger → succeeds, LF
  rows retained, v7 canonical.
- `crlf-prefix` — CRLF embedded + LF ledger on baseline → fails (RED leg).

Root can independently reproduce on a clean checkout; the in-worktree product
tests cover the CRLF direction (this worktree's 002/003/005/006 are CRLF).

## Tests

- `tests/.../MigrationChecksumPortabilityTests.cs` (8, integration): LF-ledger
  on this (CRLF) build + populated v6 upgrade — ledger retained byte-for-byte,
  unfinished work preserved, idempotent rerun; CRLF-ledger retained; fresh
  apply records canonical published checksums; unknown hash, other-migration
  hash, drifted-SQL hash, and wrong ledger name each rejected with the seeded
  ledger and populated work rows unchanged; populated v7 idempotence with
  inserted unfinished work.
- `tests/.../MigrationChecksumCatalogTests.cs` (8, unit): catalog pins,
  canonical resolution for both variants, fail-closed drift/rename,
  helper-level rejection of a drifted `resourceChecksum` for a cataloged
  version, strict raw-hash contract for unlisted versions, and a sweep
  asserting every embedded 001-007 resource of *this* build is
  cataloged+approved (resource-drift coverage without any public test-only
  injection API; `InternalsVisibleTo ErpOnecAgent.UnitTests` added to
  Infrastructure).
- Baseline migration review tests updated where they asserted the recorded
  checksum of *freshly applied* versions: those now expect the canonical
  published checksum (`PublishedChecksum` = LF form); seeded ledger rows are
  still asserted byte-for-byte with their seeded (CRLF) checksums, which is
  itself the "existing legacy raw preserved" requirement. No skips added.

## Alternatives considered

Global normalization (hash `sql.Replace("\r\n","\n")`) or ignoring checksums
would accept arbitrary changed SQL byte patterns silently — rejected per root
policy. Normalizing ledger rows on read (rewriting stored checksums) mutates
the durable ledger — rejected. The pinned catalog is the smallest change that
admits only the two known-byte-shape variants of exactly the already-published
SQL.

## Limitations

- The catalog pins `SHA256(UTF8(decoded text))` — the pre-existing decoder
  semantics (`StreamReader` UTF-8, BOM stripped) are unchanged. The observed
  001-007 files carry no BOM; a BOM-prefixed or differently-encoded file that
  still decodes to identical text would produce an approved hash — there is no
  raw-file-encoding integrity claim. A file whose decoded text differs in any
  byte (content edit, different line-ending mix, real encoding change visible
  in the text) is rejected — intentionally.
- No general semantic normalization exists: any real content change to a
  published migration still fails closed, as does a renamed file.
- Historical ledgers written by checkouts with yet other byte shapes (none
  observed in this repo's history) would need an explicit new catalog entry,
  added only after the same blob-vs-file verification.
- The harness scenario builds are ephemeral (`%TEMP%/mcp-lf-scenario`); product
  tests intentionally do not build second assemblies.


## Root acceptance — 2026-09-25

Accepted after independent review and two native Windows verification runs: main with unchanged historical SQL bytes, and a clean Git checkout of base 62e559d with all seven SQL resources LF plus the reviewed patch. Each completed locked restore, Release Rebuild (0 warnings/errors), and full tests: 547 integration + 47 unit = 594 passed, 0 failed/skipped. Evidence in repo local-data/remediation-2026-09-25/migration-checksum-root/: main-final-*.log, clean-final-*.log, separate timestamped TRX and exit-code JSON. Source manifests were stable through testing. All catalog literals independently matched published Git LF or observed historical file hashes; SQL 001-007 remained byte-identical to the local baseline.

Root first reproduced one test failure in the clean LF checkout; the corrected historical-ledger seed now uses pinned values and passes there. Existing seeded-ledger assertions were retained; only newly-applied checksum expectations changed in three older migration tests. No worker activation or ERP deployment occurred. O1 remains DARK; O2/008 send-attempt ledger is the next implementation slice.
