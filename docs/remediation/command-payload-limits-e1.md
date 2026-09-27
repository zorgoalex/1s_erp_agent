# E1 — command payload limits and a Content-Length body to 1C

Date: 2026-09-27. No migration. Agreed with ERP over agent-bridge (`to-onec/0002`,
`to-erp/0002`) and aligned with the 1C extension's BSL JSON parser
(`repo_1c_extension/src-designer/CommonModules/ERPIntegration_Protocol/Ext/Module.bsl`).

## Limits (checked at intake, before the hash)

`CommandPayloadPolicy.Check` rejects the payload with the codes below.

| Rule | Limit | Code |
|---|---|---|
| Canonical payload size | ≤ 61 440 bytes (60 KiB), measured on canonical bytes, not raw text: escaped Cyrillic is 6 bytes per character | `PAYLOAD_TOO_LARGE` |
| Depth | ≤ 31, payload root = 0 | `INVALID_PAYLOAD` |
| Values | ≤ 4 089, scalars included, property names excluded | `INVALID_PAYLOAD` |
| Properties per object | ≤ 128 | `INVALID_PAYLOAD` |
| Names | no duplicate or case-duplicate names | `INVALID_PAYLOAD` |
| Strings | no unpaired surrogates, in values or names | `INVALID_PAYLOAD` |

Why these numbers:
- **Depth and values.** 1C parses the whole request body: the envelope root is at depth 0 and
  the payload at depth 1. The envelope has 7 non-payload nodes: the root and 6 fields,
  counting a null `correlationId`. So payload depth 31 and 4 089 values are exactly 1C's
  32 / 4 096.
- **Size.** With the envelope, the body stays within 1C's 65 536 bytes.
- **Settings.** `Commands:MaxPayloadBytes` now defaults to 61 440. A larger value, such as an
  older 1 MiB override, still starts: the effective limit is clamped to 61 440.

`PayloadHasher.cs` and `JsonAmbiguityGuard.cs` are byte-pinned snapshots in the extension
contract and are not changed. `CommandPayloadPolicy.CanonicalBytes` repeats the hasher's writer;
a test asserts both produce identical bytes.

## Body to 1C

`OnecCommandClient` sends a `ByteArrayContent` (`application/json; charset=utf-8`) with
`Content-Length`. `JsonContent` streamed the body as `Transfer-Encoding: chunked`, which the
extension was never tested with.

## Review

An independent review found no must-fix issues. It confirmed:
- the depth and node arithmetic against the BSL parser;
- that the body cannot exceed 64 KiB.

Its should-fix items are applied:
- no startup failure for older configurations above 61 440 (the value is clamped instead);
- an end-to-end test of the real serialized body.

Nit, documented: case-duplicate names use .NET `OrdinalIgnoreCase`, while 1C uses `ВРег`.

Commands already queued under the old 1 MiB limit are not re-checked on dispatch. This matters
only during the upgrade.

## Tests

`CommandPayloadPolicyTests` cover:
- ambiguous names;
- unpaired surrogates;
- depth 31/32, values 4089/4090, properties 128/129;
- canonical vs raw size;
- the exact limit;
- limit before hash;
- clamping;
- canonical bytes equal to the hasher.

`OnecCommandClientTests` cover:
- Content-Length rather than chunked;
- the worst-case body (maximum bytes with escaped text, maximum depth, maximum values)
  measured against 1C's rules.

RED on the pre-fix build: 11 tests.
