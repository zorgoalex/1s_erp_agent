# D1 — full → incremental domain transition, BaselineRequired, attested domain reset (migration 011)

Date: 2026-09-26. DARK store behaviour.

## Problem

The F1 domain fingerprint hashed the query mode, so the first incremental run after a
`bootstrap_full` baseline hit `DOMAIN_CHANGED` and blocked. There was also no exit at all
from `DOMAIN_CHANGED` (for example a new `exportEpoch` after a backup restore) or from
`DOMAIN_UNKNOWN` (legacy rows without a fingerprint).

## Changes

**`EtlDomainFingerprint`** now hashes { source namespace, entity, entire frozen
definition, cursor class } instead of the mode. `bootstrap_full`, `entity_reload` and
`incremental` all share the cursor class `watermark-cursor/v1`: they produce the same
(UpdatedAtUtc, SourceId) cursor, and a full read covers a superset of an incremental read.
Any other mode stays distinct and is rejected by `Begin` anyway.

What still changes the domain:
- any definition change, including `UpdatedAtField`;
- a source change, including a new `exportEpoch`.

**`Begin` BaselineRequired.** An incremental read with no watermark row is refused
(`BaselineRequired`) with zero writes; the fence bump is rolled back too. The first
watermark of an entity, and the first after a reset, always comes from an explicit
baseline.

**`ResetEtlWatermarkDomainAsync`** and `011_watermark_domain_resets.sql` (LF SHA-256
`6ABF179B5A13EE6928B72116099BCC9402584BE82062D107DE10B28B77BE2BEA`) — an attested reset:
- It works under a generation CAS, and only while no active ownership exists for the
  entity. Ownership is held from claim until finalize or R1 resolution, and both leave the
  run terminal, so no run that captured the old base can finalize afterwards; this also
  rules out ABA on the restarted generation.
- ONE transaction archives the row verbatim (committed and extracting cursor, generation,
  fingerprint, last run, updated-at, operator, reason) and deletes it.
- Refusals (`WatermarkMissing`, `GenerationMismatch`, `EntityOwned`) write nothing.
- Invalid requests throw.

## Evidence

Evidence lives in `repo_1c-agent/local-data/remediation-2026-09-26/domain-transition-d1-root/`.

**Tests (Devin, to the orchestrator's list):**
- schema pins 10→11;
- the populated v10 fixture and a v10→v11 migration test;
- `EtlDomainTransitionD1Tests` (fingerprint, epoch change → R1 resolution → reset → new
  baseline, legacy NULL fingerprint, refusals, stale owner, concurrent resets);
- `EtlDomainTransitionD1BaselineTests` (full → incremental continuation end to end,
  BaselineRequired zero writes);
- two O3 tests moved onto a seeded baseline, plus a new BaselineRequired test for a
  scheduled incremental.

Devin stopped mid-debug. Seven of his tests read every string column as null because a
helper never set `CommandText`. The orchestrator found and fixed that test bug.

**RED on pre-D1 main (behavioural):** the continuation test got `Rejected(DomainChanged)`,
and the no-baseline test got `Begun`. Both failed there and pass after D1.

**Independent review** (fresh agent) found no blocker; mode-independence, BaselineRequired
placement and reset safety were verified. Changes made from its findings:
- `prior_extracting_cursor_json` added, so the archive really is verbatim;
- zero-write assertions now include the run's `row_version`;
- the resolved-run completion outcome is pinned to `NotClaimed`;
- design doc note (§3.2).

The review's key operational finding was that a missing baseline for one entity stalls a
whole scheduled run. That is handled in C1: the scheduled manifest contains only entities
with an established baseline, and entities without one are reported.

Final result: 709 integration + 168 unit = 877/877 on main `f0ba3a2` + D1. The main
transfer is verified separately.

## Cutover preconditions (recorded)

- No F1 watermark rows written before D1 may remain: their fingerprints hashed the mode
  and now read as `DOMAIN_CHANGED`. The F1 path was DARK, so none exist in production.
  If one does exist, reset it.
- A fresh deployment, or a newly enabled entity, needs an explicit
  `start_full_sync`/`reload_entity` baseline before scheduled incremental runs include it.
