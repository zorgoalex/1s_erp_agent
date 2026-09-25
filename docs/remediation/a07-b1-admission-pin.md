# A07 B1 — long-poll admission point, pinned

Date: 2026-09-26. Tests only; no production change.

Decision (the conservative interim in `a07-boundaries-design.md`, row B1): a command is
admitted when the lease poll is issued. The mode can change while the poll is held, to
`PauseCommands`, `Drain`, `Maintenance` or `Disabled`, or into handshake maintenance. A
command that ERP returns afterwards is still:

- stored durably (`commands_inbox`, `queued`);
- acknowledged once as `received`.

It stays unexecuted while the restriction holds. No new poll is issued, because
`CanLeaseCommands` is false in all four modes, including `Drain`. This cannot lose work,
and it needs no contract change: there is no release or NACK route.

`tests/ErpOnecAgent.IntegrationTests/A07LongPollAdmissionTests.cs` (written by Devin) has
6 tests:

- one per mode;
- handshake maintenance;
- a Normal-mode control in which polling continues.

The tests run the real `CommandLeaseWorker` over SQLite, synchronised with TCS barriers.
Absence of a second poll is checked in a 1.5 s observation window.

If a post-poll re-check is ever adopted (reject and don't ACK), these tests must change
together with a confirmed ERP lease-expiry reclaim.
