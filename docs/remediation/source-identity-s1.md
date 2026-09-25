# S1 — 1C source identity binding and ETL source namespace

Date: 2026-09-26. No migration.

Sources:
- decision: [source-identity-vs-source-namespace-2026-09-25](../../../spec_1c-agent/reviews/source-identity-vs-source-namespace-2026-09-25.md) §4 (format agreed by the user);
- extension contract: `repo_1c_extension` identity-server-report 2026-09-25 (GET identity, extension 0.3.0+).

## What was added

- **`OneC:SourceBinding`** (`OnecSourceBindingOptions`) holds the expected `DatabaseId`,
  `ExportEpoch`, `Environment` (`test|production`) and `ODataEndpoint`. It is configured
  explicitly after the source is verified and never adopted from a server response.
  Startup validation rejects a binding in any of these cases:
  - it is invalid;
  - `ODataBaseUrl` differs from the bound endpoint after normalization;
  - the OData root and `CommandApiBaseUrl` do not belong to the same 1C publication (same
    scheme, host and port, and the same prefix before `/odata/` and `/hs/`).
- **`OnecSourceBinding.SourceNamespace`** =
  `1c-identity:v1:{databaseId}:{exportEpoch}:{environment}` (lowercase UUIDs). A new
  `exportEpoch` (backup restore, rotation) is a new cursor domain by design.
- **`OnecIdentityClient`** (`GET {CommandApiBaseUrl}/identity`, read-only) classifies every
  response:

  | Response | Result |
  |---|---|
  | 200 ready, strict case-sensitive contract | `Ready` |
  | 503 `uninitialized`/`blocked` identity body | `NotReady` |
  | 503 storage failure (`STORAGE_UNAVAILABLE`), 503 proxy page, other statuses, transport errors, timeouts, credential failures | `Unavailable` |
  | Any 200 contract violation, including duplicate or case-variant names and oversized bodies | `Malformed` |

  The whole call (headers and body) is bounded by the client timeout. Only caller
  cancellation propagates.
- **`SourceIdentityVerifier` / `SourceIdentityGuard`** check the binding and endpoint, then
  scope, database, epoch and environment. A missing or invalid binding costs no network
  call. `SameSource(before, after)` is the extraction before/after rule. The guard resolves
  a fresh typed client per call, so HttpClient handlers keep rotating.

The guard is not yet used by any worker. C1 wires it before claim and before seal.

## Evidence

Evidence lives in `repo_1c-agent/local-data/remediation-2026-09-26/source-identity-s1-root/`
and the Devin worktree `local-data`.

**Tests (Devin, to the orchestrator's scenario list): 100 unit cases.** They cover:
- binding, normalization and verifier;
- client parsing and classification;
- the guard;
- options validation.

They passed on the first implementation; Devin found no production defect.

**Independent review** (fresh agent) found no blocker. It raised 5 should-fix items:
1. The extension's real storage-failure 503 and proxy 503 pages were classified
   `Malformed`.
2. Credential failures escaped as exceptions.
3. A stalled body had no time bound.
4. The singleton guard pinned its HttpClient.
5. An after-check retry is needed; that one is handled in C1.

All were fixed. The review-driven tests (`OnecIdentityClientReviewTests`) show on the
pre-fix build: 4 runtime failures (three credential cases, proxy 503) and the
stalled-body test hanging indefinitely. The hung test host was stopped after its command
line was verified. After the fix: **168/168 unit** tests pass in the slice worktree.

## Limits

- **Identity is an administrative guarantee, not cryptographic proof.** A byte clone with
  the same UUIDs is not detected, and the before/after reads are not a transactional
  snapshot with OData.
- **Extension 0.3.1 is not installed in the original test УНФ**, so the endpoint is not
  available there yet.
