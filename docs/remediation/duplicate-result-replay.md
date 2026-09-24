# FR-CMD-003 — replay сохранённого результата duplicate

Дата: 24.09.2026.  
Статус: локальный ограниченный срез этапа 1 и reviewer-correction для `sending` проверены; независимая root-проверка ожидается, внешний контракт не согласован, FR-CMD-003 целиком не закрыт.

## 1. Реализованная семантика

`StoreCommandAsync` и `AdmitCommandAsync` разрешают существующий `commandId` в той же SQLite-транзакции, которая должна завершиться до ACK `received`.

Для совпадающего `payloadHash`:

- при `commands_inbox.result_json IS NOT NULL` сохранённый результат возвращается в `results_outbox`;
- `acknowledged` outbox атомарно переводится в `pending`, а `next_attempt_at_utc` становится равен времени replay;
- существующие `result_id`, `payload_json`, `payload_hash`, `created_at_utc`, `attempt_count`, `sent_at_utc`, `acknowledged_at_utc` и `last_error` сохраняются;
- уже `pending`/`retry_waiting` outbox не меняется: повторный lease не создаёт строку, не сбрасывает backoff и не увеличивает счётчик;
- `commands_inbox` остаётся `completed`, его `result_status`, `row_version`, audit-счётчики, send-счётчики, `queue_sequence`, claim-поля и timestamps не меняются; successor с тем же `ordering_key` не блокируется;
- active `queued`/`executing` duplicate без `result_json` не меняет строку команды;
- hash-конфликт остаётся `PayloadConflict`: исходная команда, результат и outbox не изменяются, replay не создаётся; durable local event записывается отдельным последующим срезом согласно [payload-conflict-events.md](payload-conflict-events.md).

Повторный lease не является источником result JSON. В replay используется только ранее сохранённый `commands_inbox.result_json`; входящий duplicate не может пересоздать `completedAtUtc`, payload или иные поля результата.

## 2. Отсутствующий outbox

Выбран инвариант: `commands_inbox.result_json` — авторитетный durable result, а `results_outbox` — его единственная доставляемая проекция (`UNIQUE(command_id)`).

Если сохранённая terminal-команда ещё существует, но outbox отсутствует, matching duplicate безопасно восстанавливает projection:

- только из точных сохранённых строк `result_json`;
- с новым `result_id`, поскольку удалённую исходную identity восстановить нельзя;
- с `payload_hash`, вычисленным из UTF-8 байтов сохранённого `result_json`;
- без данных из incoming duplicate.

Если команда уже удалена retention-операцией, reconstruction невозможна и не выполняется. Новых схем и миграций для этого не требуется.

## 3. Cleanup и timestamps ACK

Cleanup больше не определяет удаление команды только старым `commands_inbox.erp_acknowledged_at_utc`. Guards удаления `command_attempts` и `commands_inbox` используют консервативный критерий `results_outbox.status <> 'acknowledged'`: команда, attempts и outbox сохраняются для любого неподтверждённого состояния, включая `pending`, `retry_waiting` и `sending`, даже если command ACK старше cutoff. `RecoverAsync` возвращает `sending` в `pending`. Новый delivery ACK переводит outbox в `acknowledged`; только после этого применяется retention cutoff.

`commands_inbox.erp_acknowledged_at_utc` и `results_outbox.acknowledged_at_utc/sent_at_utc` означают время последнего подтверждённого доставленного результата. Они сохраняются во время replay как история последнего подтверждения и не делают replay выполненным. Новый `AcknowledgeResultAsync` заменяет их новым временем подтверждения. Текущая обязанность доставки определяется outbox status, а не возрастом исторического ACK.

ETL retention и иные cleanup-пути не изменялись.

## 4. Изменённые файлы

- `src/ErpOnecAgent.Infrastructure/Persistence/Sqlite/SqliteAgentStore.Commands.cs` — атомарное duplicate-resolution и replay;
- `src/ErpOnecAgent.Infrastructure/Persistence/Sqlite/SqliteAgentStore.cs` — узкая защита command/attempts/outbox от cleanup при активном replay;
- `tests/ErpOnecAgent.IntegrationTests/DuplicateResultReplayTests.cs` — 11 test methods / 12 test cases на изолированных `SqliteTestDatabase`;
- `docs/remediation/contract-decisions.md` — только локальные as-built формулировки CD-ACK-3/CD-RS-3 и индекс;
- `local-data/remediation-2026-09-24/duplicate-result-replay/`, включая `sending-review/` — evidence.

Миграции `001_initial.sql`, `002_retry_budgets.sql`, `003_ordering_claims.sql` не изменялись; migration 004 не создавалась. `ClearAllPools` не использовался.

## 5. Проверки

Baseline checkpoint-152: 39 unit + 113 integration = 152/152.

### RED до production-изменения

Класс `DuplicateResultReplayTests`: 3 passed / 8 failed / 11 total. Неизменённый store не requeue-ил acknowledged outbox, не восстанавливал отсутствующую projection и cleanup удалял pending replay вместе с командой и attempts.

- TRX: `local-data/remediation-2026-09-24/duplicate-result-replay/red/TestResults/duplicate-result-replay-red-final_net10.0_20260924171431.trx`
- log: `local-data/remediation-2026-09-24/duplicate-result-replay/red/test-final.log`

### Targeted после исправления

`DuplicateResultReplayTests`: 11/11 passed.

- TRX: `local-data/remediation-2026-09-24/duplicate-result-replay/targeted/TestResults/duplicate-result-replay-targeted_net10.0_20260924171540.trx`
- log: `local-data/remediation-2026-09-24/duplicate-result-replay/targeted/test.log`

### Промежуточный Windows Release прогон 163/163 (до замечания ревью)

Команда: `.\.dotnet\dotnet.exe build .\ErpOnecAgent.sln -c Release -t:Rebuild --no-restore`.

Результат: 0 warnings / 0 errors.

- log: `local-data/remediation-2026-09-24/duplicate-result-replay/final/rebuild.log`
- SDK: `local-data/remediation-2026-09-24/duplicate-result-replay/final/dotnet-info.txt`

Команды: `dotnet test <assembly> -c Release --no-build --no-restore`, отдельный TRX prefix на assembly.

| Assembly | Passed | Failed | Skipped | TRX |
|---|---:|---:|---:|---|
| Unit | 39 | 0 | 0 | `final/TestResults/duplicate-result-replay-unit_net10.0_20260924171647.trx` |
| Integration | 124 | 0 | 0 | `final/TestResults/duplicate-result-replay-integration_net10.0_20260924171628.trx` |
| **Total** | **163** | **0** | **0** | |

- integration log: `local-data/remediation-2026-09-24/duplicate-result-replay/final/integration-test.log`
- unit log: `local-data/remediation-2026-09-24/duplicate-result-replay/final/unit-test.log`
- migration hashes: `local-data/remediation-2026-09-24/duplicate-result-replay/final/migration-sha256.txt`

### Reviewer correction — `sending` boundary

Reviewer обнаружил, что guards cleanup учитывали только `pending`/`retry_waiting`, хотя `RecoverAsync` уже восстанавливает `sending`. Добавлена real-SQLite регрессия: после replay outbox через raw SQL переводится в `sending`, cleanup выполняется с cutoff старше command ACK, затем store пересоздаётся и вызывается `RecoverAsync`.

- RED до SQL-fix: 0 passed / 1 failed. Cleanup пытался удалить `commands_inbox` при существующем `sending` outbox и откатывал транзакцию по FK; TRX: `local-data/remediation-2026-09-24/duplicate-result-replay/sending-review/red/TestResults/sending-review-red_net10.0_20260924171953.trx`.
- Минимальный fix: оба cleanup guards используют `o.status <> 'acknowledged'`; production API, схема и миграции не менялись.
- Targeted после fix: `DuplicateResultReplayTests` 12/12; TRX: `local-data/remediation-2026-09-24/duplicate-result-replay/sending-review/targeted/TestResults/sending-review-targeted_net10.0_20260924172012.trx`.
- Recovery-проверка подтверждает: `sending → pending`, exact result JSON/hash/id и attempt history сохранены, команда и attempt остаются доступны после restart, результат снова доставляем.
- Финальный Windows Release Rebuild: 0 warnings / 0 errors; log: `local-data/remediation-2026-09-24/duplicate-result-replay/sending-review/final/rebuild.log`.
- Финальные tests: 39 unit + 125 integration = **164/164**, 0 failed/skipped; TRX: `sending-review/final/TestResults/sending-review-unit_net10.0_20260924172050.trx`, `sending-review/final/TestResults/sending-review-integration_net10.0_20260924172031.trx`.

## 6. Границы

- Проверки локальные: реальные ERP/1C, credentials, сертификаты и сетевые сервисы не использовались.
- Result delivery ACK проверен через публичный store API; сетевой ERP result endpoint не вызывался.
- `ackStatus`, HTTP 409 и новые wire-поля не добавлялись.
- Долговременное хранение, ETL retention и общие cleanup-политики не расширялись.
- Conflict-контур FR-CMD-003 не завершён этим срезом: durable локальное critical evidence
  реализовано позднее в [payload-conflict-events.md](payload-conflict-events.md), но внешний
  отчёт о payload conflict и согласованный conflict-409 остаются открытыми.
- Внешняя семантика ERP/1C и приёмка replay владельцами не согласованы.

## Независимая приёмка

Оркестратор проверил итоговый diff и выполнил отдельный Release Rebuild (0warnings/errors),39unit+125integration=164/164. Hash C# исходников во время проверки не менялись. Доказательства: `local-data/remediation-2026-09-24/duplicate-result-replay/independent-review/`. Принят локальный replay-срез; весь FR-CMD-003 и этап1 не закрыты.
