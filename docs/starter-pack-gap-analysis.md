# Сопоставление со starter-pack — 2026-08-30

Сравнение выполнено с документами и исходниками `ErpOnecAgent_CSharp_Windows_Starter_v0.1.zip`. При конфликте чернового `API_CONTRACTS.md` с основным ТЗ приоритет оставлен за `TZ_Local_1C_Agent_CSharp_Windows_v1.0.md`.

## Добавлено из starter-pack

- OData v3/v4: `value`, `d.results`, `@odata.nextLink`, `odata.nextLink`, `__next` и `$skip` fallback.
- Автоматическое включение key/update/deletion полей в `$select`.
- Заголовок `OData-Version` и корректное форматирование дат v3/v4.
- Раздельная проверка OData `$metadata` и командного health endpoint 1С.
- CPU, SQLite и spool metrics в heartbeat; конфигурируемые интервалы health/heartbeat.
- `RunOnStartup`, safety lag, управляемая параллельность upload и per-entity `enabled`/`oDataVersion`.
- `MaxSqliteBytes`, `MaxBatchCompressedBytes`, период maintenance, WAL checkpoint и `PRAGMA optimize`.
- Атомарный перевод выбранных ETL batch в `uploading` до сетевой отправки.
- Запрет HTTP redirects/cookies и проверка certificate revocation для ERP/1С handlers.
- Отдельный безопасный скрипт смены DPAPI credentials и `.editorconfig`.
- Корректная классификация `business_failed` при HTTP 200 от 1С.

## Уже было реализовано или было сильнее starter-pack

- Модульные Domain/Contracts/Application/Infrastructure/Service проекты.
- Durable command inbox/result outbox, attempts, ordering, expiry, deduplication и crash recovery.
- Atomic NDJSON/GZip spool с требуемым envelope `sourceId/sourceUpdatedAt/deleted/data`.
- ETL runs/batches/watermarks, commit только после ERP ACK.
- mTLS, DPAPI, Windows Service, virtual service account и ACL.
- Remote configuration snapshots, diagnostics ZIP, OpenAPI, contract mock, integration tests, SBOM и hash manifest.

## Намеренно не перенесено

Черновой `API_CONTRACTS.md` starter-pack предлагает `POST /sessions`, `POST /commands/{id}/result` и `POST /commands`. Основное ТЗ требует соответственно:

- `POST /session/start`;
- идемпотентный `PUT /commands/{commandId}/result`;
- `POST /erp-integration/v1/commands/execute`.

Текущая реализация сохранена по основному ТЗ. Для ETL также сохранён допустимый основным ТЗ вариант gzip NDJSON + metadata headers вместо обязательного только multipart.
