# E4 — heartbeat: state reason and configuration versions

Date: 2026-09-28. No migration. Agreed with ERP (spec В-9 and В-10; `to-onec/0001`, `0002`).

## Change

Heartbeat gains four optional fields (additive):

| Field | Value |
|---|---|
| `stateReason` | The health reason code behind `state`, e.g. `CLOCK_DRIFT`, `ETL_RUNS_UNRESOLVED` or `ONEC_ODATA_UNAVAILABLE`. It is `null` when the agent is healthy. |
| `activeConfigVersion` | The active remote configuration version, or `null` before any configuration is active. |
| `rejectedConfigVersion` | The last remote configuration the agent refused, while it is newer than the active one. |
| `rejectedReason` | A stable code for that refusal. |

**Codes for `rejectedReason`:**
- `CONFIG_JSON_INVALID`;
- `CONFIG_HASH_MISMATCH`;
- `CONFIG_VERSION_ROLLBACK`;
- `CONFIG_INVALID`: validation failed, including an invalid `sourceGeneration` or an ambiguous body;
- `CONFIG_APPLY_FAILED`: a store failure.

Exception text is never sent, because it can carry configuration details.

**How the refusal is tracked:**
- The refusal is kept in memory by `DynamicConfigurationState.RecordRejection`.
- It stops being reported once a version at least as new becomes active.
- After a restart, ERP offers the same configuration again within one poll. The agent then
  refuses it again and reports it again.
- A transport failure while fetching is not a refusal.

## Tests

- `A07CompatibilityHeartbeatTests.E4_*`:
  - heartbeat carries all four fields, and the Web JSON names are checked;
  - a healthy heartbeat sends no reason and no versions;
  - a rejection is reported only while it is newer than the active version;
  - the reason mapping gives stable codes.
- `ConfigurationActivationTests.Worker_ambiguous_case_alias_replay_cannot_promote_rejected_snapshot`
  now also checks that the worker records the refusal for heartbeat.
