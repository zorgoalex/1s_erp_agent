# V1 — verified completeness of full reads

Date: 2026-09-28. Migration 014, schema v14.

## Why

`readScope: "full"` only says how an entity was queried: without a date filter from the last
watermark. It does not prove that the rows ERP received are the whole entity set. A read can be
short or inconsistent for several reasons:
- rights changed or the publication changed;
- rows changed between pages, and `$skip` paging shifted;
- rows changed after the snapshot bound, which a full read still filters by `updatedAtField`.

The pilot decision (28.09, both users) is that missing keys are only diagnosed. Automatic
deletion comes back only with a separate verified-completeness signal. V1 is that signal. It
does not switch deletion on.

## Rule

For every entity read with readScope `full`, when `Etl:VerifyFullReads` is on (the default):
1. `GET {set}/$count` of the whole set, without a filter, before the data pass.
2. The data pass as before. Each row's key is added to an order-independent digest: the
   SHA-256 of each key, first 128 bits summed modulo 2^128. Memory stays constant.
3. A second, independent key-only pass: `$select` = key fields, `$orderby` = keys, no
   `$filter`, paged to the end. Its keys go into a second digest.
4. `$count` again.

The entity is **`verified`** only if count before = count after = rows in the data pass = rows
in the key pass, and both digests are equal. Otherwise it is **`unverified`** with a reason:

| Reason | Meaning |
|---|---|
| `COUNT_UNSUPPORTED`, `COUNT_FAILED` | `$count` is not available or failed |
| `KEY_MISSING` | a data row had no value in a key field |
| `KEY_PASS_UNSUPPORTED`, `KEY_PASS_FAILED` | the key pass is not available or failed |
| `COUNT_CHANGED` | the set changed during the read |
| `ROWS_NOT_EQUAL_COUNT` | the data pass differs from `$count`: rows changed after the snapshot bound, or paging skipped or repeated rows |
| `KEY_PASS_COUNT_MISMATCH` | the key pass differs from `$count` |
| `KEY_SET_MISMATCH` | same counts, but different keys (for example, a duplicate replaced a missing row) |

With `Etl:VerifyFullReads = false`, a full entity gets `not_checked` / `VERIFICATION_DISABLED`.
Delta reads and failed entities get no verdict.

The check never fails an entity: the read, batches and watermark behave as before, and only
the verdict differs. Log: `ETL_ENTITY_COMPLETENESS`.

**What `verified` does not prove.** It covers the key set, not every field value. The data
pass and the key pass are separate requests, so it is strong evidence, not a transactional
snapshot. That is the same limit the extension contract (`readonly-agent-v1.md`) states for
its two passes.

## Wire and storage

- Migration 014 adds `etl_run_entities.read_completeness` and `read_completeness_reason`.
  Both are written once, with the entity's completion.
- `complete` v2: every entity item with a verdict carries two more properties,
  `completeness` and `completenessReason`. Items without a verdict are byte for byte as in E3.
- The v2 validator requires both properties exactly when the durable row has a verdict. A
  changed verdict in a stored body is `SEAL_VIOLATED`, and an honest reclaim replays the same
  bytes.
- OpenAPI: `RunCompletionEntityV2.completeness` and `completenessReason`.

## Load

Per full entity: two `$count` requests and one key-only pass. The key-only pass has the same
number of pages, but each page carries only the keys.

## Tests

- `ReadCompletenessV1Tests` (unit, 12):
  - digest: order independence, missing, extra and duplicate keys, composite keys and JSON
    types, a missing or null key;
  - `$count` parsing, including a BOM and CRLF, and invalid bodies and statuses;
  - the key pass URL: key-only `$select`, `$orderby`, no `$filter`, paging;
  - verdict validity.
- `EtlPipelineC1Tests.V1_*` (7): the real pipeline with `verified`, `COUNT_CHANGED`,
  `KEY_SET_MISMATCH`, `ROWS_NOT_EQUAL_COUNT` and `COUNT_UNSUPPORTED`; switched off gives
  `not_checked`; an empty set is verified.
- `EtlFinalizeStorageTests.V1_*` (3):
  - an invalid verdict is refused;
  - an honest reclaim replays the same bytes;
  - a tampered `completeness` or `completenessReason`, or removed completeness properties,
    is `SEAL_VIOLATED`.
- Existing tests: migration bookkeeping v13 → v14.
