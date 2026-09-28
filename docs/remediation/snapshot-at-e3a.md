# E3a — snapshotAtUtc per entity

Date: 2026-09-28. Migration 015, schema v15. Agreed with ERP (agent-bridge `to-onec/0022`,
`to-erp/0025`).

## Change

- **What.** Every entity that proceeds to extraction gets `snapshot_at_utc` at Begin: the time
  the agent starts reading it from 1C, UTC, millisecond precision (`…T…:…:….fffZ`). ERP replaces
  a stored snapshot (stock, phones) only with a newer one.
- **Monotonic per entity.** `etl_entity_snapshot_clock` keeps the last value issued for each
  entity. A new value is `max(now, last + 1 ms)`, compared and stored at millisecond precision.
  A later read therefore always carries a strictly greater time, even after a backward clock
  step.
- **Wire.** The value goes into `complete` v2 `entities[]` as `snapshotAtUtc`, after the
  optional completeness fields. It is omitted when absent: entities refused at Begin and bodies
  stored before v15. Bodies built before v15 replay byte for byte.
- **Validation.** The validator requires `snapshotAtUtc` exactly when the durable row has it,
  with the same value. A tampered or removed value is `SEAL_VIOLATED`.
- **OpenAPI** 1.3.0.

## Tests

`EtlFinalizeStorageTests.E3a_*`:
- the format, and the value carried into `complete`;
- strict increase after a backward clock step;
- no value for an entity refused at Begin;
- a tampered or removed value is `SEAL_VIOLATED`.

Migration bookkeeping v14 → v15.
