# FR-CMD-003 — durable local payload-conflict events

Дата: 24.09.2026.  
Статус: локальный ограниченный срез этапа 1; внешний wire-контракт не согласован, FR-CMD-003 целиком не закрыт.

## Реализованная семантика

`StoreCommandAsync` и `AdmitCommandAsync` при одинаковом `commandId` и другом объявленном `payloadHash` в той же admission-транзакции, до возврата outcome и ACK `received`:

- сохраняют исходную `commands_inbox` строку, её authoritative `payload_hash`, результат, outbox и attempt audit неизменными;
- вставляют или обновляют строку `command_payload_conflicts` с `COMMAND_PAYLOAD_CONFLICT`, severity `critical`, commandId и текущим статусом исходной команды;
- записывают UTC `first_seen_at_utc`, `last_seen_at_utc` и `occurrence_count`;
- используют переданный в admission `receivedAtUtc`, а stale redelivery не уменьшает `last_seen_at_utc`;
- агрегируют только одинаковую пару commandId + original declared-hash fingerprint + incoming declared-hash fingerprint через UNIQUE key; разные incoming hash-строки получают отдельные строки;
- сохраняют только SHA-256 UTF-8 fingerprints (64 hex-символа) declared original/incoming hash-строк. Это fingerprints строк объявленного hash, а не вычисленные payload hashes; исходная authoritative hash-строка команды остаётся неизменной;
- не сохраняют payload, `requestedBy`, raw incoming JSON, error text или иные пользовательские данные;
- не создают FK к `commands_inbox`, поэтому ordinary command cleanup не удаляет critical evidence.

Ошибка записи event propagates из admission и запрещает ACK через `CommandIntakeService`. Matching-hash duplicate остаётся `Duplicate` и event не создаёт. Внешние ACK/status/409 не менялись.

## Миграция и API

Новая additive migration `004_command_payload_conflicts.sql` создаёт только dedicated table и recent-read index. `SqliteMigrator.CurrentSchemaVersion` увеличен с 3 до 4. Миграции 001/002/003 не изменены.

`IAgentStore.GetCommandPayloadConflictEventsAsync(int limit, ...)` — минимальный typed bounded read API: newest-first, строгий `LIMIT`, `limit > 0`, без general event framework, outbound API или diagnostics changes.

## Runtime coverage

`PayloadConflictEventTests` и `PayloadConflictMigrationTests` добавляют 16 real-SQLite test cases:

- оба public store/admit paths и unchanged originals в `queued`, `executing`, `result_pending`, `completed`;
- same-hash duplicates без event;
- агрегация count/first/last, stale timestamp и отдельные distinct pairs;
- persistence после restart и bounded typed read;
- trigger-injected event insert failure: rollback/throw, original state unchanged, zero ERP `received` ACK;
- ordinary cleanup удаляет command/outbox/attempts, но оставляет event;
- invalid/large attacker hash strings и payload/user markers не попадают в event, остаются только fixed fingerprints;
- production migrator обновляет заполненную v3 БД с 8 commands, 6 attempts, 2 outbox rows, проверяет checksums 001–004, отсутствие FK и idempotent rerun.

## Evidence

Все команды выполнены native Windows SDK из isolated worktree.

- Runtime RED до production: 1 total, 0 passed, 1 failed. Реальный SQLite вернул `PayloadConflict`, но runtime assert `sqlite_master` не нашёл event table. Console: `local-data/remediation-2026-09-24/payload-conflict/red/console.log`; TRX: `local-data/remediation-2026-09-24/payload-conflict/red/red.trx`.
- Targeted: 16 passed, 0 failed, 0 skipped. Console: `local-data/remediation-2026-09-24/payload-conflict/targeted/console.log`; TRX: `local-data/remediation-2026-09-24/payload-conflict/targeted/targeted.trx`.
- Native Release Rebuild: 0 warnings, 0 errors. Console: `local-data/remediation-2026-09-24/payload-conflict/final/rebuild.log`.
- Full unit suite: 39 passed, 0 failed, 0 skipped. Console: `local-data/remediation-2026-09-24/payload-conflict/final/unit-test.log`; TRX: `local-data/remediation-2026-09-24/payload-conflict/final/final-unit.trx`.
- Full integration suite: 166 passed, 0 failed, 0 skipped. Console: `local-data/remediation-2026-09-24/payload-conflict/final/integration-test.log`; TRX: `local-data/remediation-2026-09-24/payload-conflict/final/final-integration.trx`.
- Final total: 205 passed / 0 failed / 0 skipped (39 unit + 166 integration), 16 cases выше checkpoint 189.
- Migration SHA-256: `local-data/remediation-2026-09-24/payload-conflict/final/migration-sha256.log`. 001–003 совпадают с immutable baseline; 004 = `e75caf3ef711be74a91e15be8fc467330b83c8f943c7c1626ddb38f91c23a6ae`.

## Ограничения

- Retention/configuration для `command_payload_conflicts` остаётся stage 6. События консервативно сохраняются и могут занимать SQLite disk; разные attacker-controlled incoming hash-строки legitimately создают разные bounded-per-pair aggregates.
- Локальный API не является каналом отчёта ERP и не заменяет ручную security triage.
- Внешний HTTP 409 для intake, `ackStatus`, hash выбора в ACK и любые wire-изменения не реализованы и не согласованы; фактический intake ACK/status contract не менялся.
- Реальные ERP, 1C, credentials, certificates, external DB, network services, deployment и commits не использовались.
- Этот срез не заявляет полное выполнение FR-CMD-003 или этапа 1.

## Изменённые файлы

- `src/ErpOnecAgent.Infrastructure/Persistence/Migrations/004_command_payload_conflicts.sql`
- `src/ErpOnecAgent.Infrastructure/Persistence/Sqlite/SqliteMigrator.cs`
- `src/ErpOnecAgent.Infrastructure/Persistence/Sqlite/SqliteAgentStore.Commands.cs`
- `src/ErpOnecAgent.Application/Abstractions/Persistence.cs`
- `tests/ErpOnecAgent.IntegrationTests/PayloadConflictEventTests.cs`
- `tests/ErpOnecAgent.IntegrationTests/PayloadConflictMigrationTests.cs`
- `tests/ErpOnecAgent.IntegrationTests/CommandExecutionTests.cs` — только schema-version assertion 3→4
- `docs/remediation/contract-decisions.md`
- `docs/remediation/duplicate-result-replay.md`
- `docs/remediation/payload-conflict-events.md`

## Независимая проверка

Оркестратор проверил SQL-транзакцию, fingerprints, отсутствие FK-cascade,16регрессий и заполненный v3→v4 upgrade. Собственные Rebuild0warnings/errors,39unit+166integration=205/205 в изолированной копии; C#/SQL/csproj стабильны. Все001–003 SHA256 совпадают с checkpoint205 основного дерева. Evidence `local-data/remediation-2026-09-24/payload-conflict/independent-review/`. Локальный срез принят, перенос в main ожидает завершения A07. original_command_status фиксируется при первом наблюдении пары и не обновляется при агрегации.
