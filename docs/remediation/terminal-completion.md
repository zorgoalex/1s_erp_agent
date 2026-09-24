# Terminal completion state guard

Date: 2026-09-24.

`CompleteLocallyAsync` now performs its first terminal transition only from `queued`, `retry_waiting`, `executing`, or `unknown_result`. The guarded command update is the transaction gate: only a changed row can write `results_outbox` or close `command_attempts`. A terminal or nonexistent command commits an empty transaction and returns `false`; an applied completion returns `true`.

The store API now returns `Task<bool>`. Command-execution success/business-failure hooks and the worker business-failure log run only for an applied transition. Administrative behavior, claim acquisition/release, A01/A02, A06, ordering, ETL, and migrations were not broadened.

## Runtime evidence

- Red: 6 focused real-SQLite tests, 5 failed and 1 passed. The failures reproduced result/outbox overwrite, acknowledged-outbox corruption, nonexistent-ID FK failure, duplicate mutation, audit closure, and false success notification.
- Green: 7 focused tests passed, including both success and business-failure notification suppression.
- Release rebuild: 0 warnings, 0 errors.
- Final tests: 39/39 unit and 82/82 integration (121 total; checkpoint baseline 114).
- Immutable migrations `001_initial.sql`, `002_retry_budgets.sql`, and `003_ordering_claims.sql` are byte-identical to checkpoint-114.
- Logs and unique TRX files: `local-data/remediation-2026-09-24/state-guards/terminal-completion/`.
- Targeted formatting verification passed. Solution-wide formatting still reports pre-existing whitespace/final-newline findings in `ErpClientRegistration.cs`, `SqliteAgentStore.cs`, and `ErpLongPollResilienceTests.cs`, outside this bounded change.

## Owner-fencing status and remaining gaps

- `ClaimPostAttemptAsync` and `ClaimLookupAttemptAsync` still do not accept or predicate the per-pass owner token.
- `CompleteLocallyAsync` is intentionally state-guarded but not owner-conditioned; a direct legacy caller may first complete an active row, though it can no longer overwrite a terminal result.
- `MarkUnknownResultAsync` and `ScheduleRetryAsync` now require the exact owner or an explicitly unclaimed row; their owner-fence semantics and evidence are in [`scheduling-owner.md`](scheduling-owner.md).
- Administrative actions still execute before claim acquisition; a stale administrative snapshot may cause its bounded side effect before a terminal no-op completion. This remains outside the terminal-completion fix.
