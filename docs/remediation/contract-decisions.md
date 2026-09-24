# Этап 0 — решения по контрактам (contract decisions)

Дата: 24.09.2026 (ревизия 2 — по замечаниям ревью).
Статус: зафиксировано для реализации. Основной план и реестр A01–A11 не изменялись.
Артефакты работы: этот файл и [`contracts/onec-command-api.openapi.yaml`](../../contracts/onec-command-api.openapi.yaml)
(в этой стадии изменялись только они). Исходники, mock, `contracts/erp-agent-api.openapi.yaml`
и внешние ERP/1С не изменялись и не вызывались.

## 0. Маркеры статуса

- **[TZ]** — требование ТЗ v1.0 (владельческий документ-основание).
- **[AS-BUILT]** — текущий код/DTO/mock (файл указан).
- **[HISTORICAL]** — поведение baseline до remediation-исправлений; к текущему коду неприменимо,
  приведено как история расхождений.
- **[PROPOSED]** — предложение этапа 0 (минимальное; обратная совместимость — где доказуема).
  **Локальная реализация (код агента, mock, тесты, дизайн) авторизована — полный remediation
  по указанию пользователя.** Подтверждённый контракт с владельцем требуется для внешнего
  развёртывания и интеграции с реальной ERP/1С. Предложение НЕ считается принятым/согласованным.
- **[UNDEFINED]** — не определено исходными документами; локально реализуется только поверх
  [PROPOSED] с явной пометкой; внешний контракт — после определения.

Статус «согласовано владельцем» не присваивается ни одному пункту: подтверждений владельцев
ERP/1С нет (для ERP — см. [инвентаризацию](erp-integration-inventory.md): Agent API в архиве
исходников отсутствует, согласовывать не с чем).

## 1. Источники

- ТЗ v1.0 (`spec_1c-agent/docs/TZ_...v1.0.md`), план remediation, аудит 23.09.2026.
- Starter `API_CONTRACTS.md` — см. §2 (расхождения маршрутов устранены в самом документе).
- DTO: `Contracts/OneC/OnecContracts.cs`, `Contracts/Erp/ErpContracts.cs`, `Domain/.../CommandModels.cs`, `EtlModels.cs`.
- Клиенты: `Infrastructure/OneC/OnecCommandClient.cs`, `OnecHealthClient.cs`, `OnecAuthentication.cs`, `ErpApi/ErpClient.cs`.
- Текущий приём команд: `Application/Commands/CommandIntakeService.cs`,
  `SqliteAgentStore.AdmitCommandAsync` (`Infrastructure/Persistence/Sqlite/SqliteAgentStore.Commands.cs`);
  исполнение: `Application/Commands/CommandExecutionService.cs`, `Service/Workers/*`;
  hash: `Domain/Common/PayloadHasher.cs`.
- Remediation-доказательства: [a01-admission.md](a01-admission.md), [a02-expiry.md](a02-expiry.md);
  отсутствие ERP Agent API — [erp-integration-inventory.md](erp-integration-inventory.md).
- Mock: `tools/ErpOnecAgent.MockServer/Program.cs`; тест клиента: `tests/.../OnecCommandClientTests.cs`.
- Информация по расширению 1С — позже по указанию пользователя.

## 2. Канонические маршруты

Исторически starter-API_CONTRACTS.md предлагал `POST /sessions`, `POST .../result`,
`POST /commands`, а схема §13.1 ТЗ показывала commit watermark до complete-run.
Оба расхождения **устранены в исходных документах**: API_CONTRACTS.md приведён к канону
`POST /session/start`, `PUT .../result`, `POST .../commands/execute`; §13.1 — к порядку
FR-ETL-005. Канон совпадает с реализацией:

| # | Канон | Статус | Комментарий |
|---|---|---|---|
| CD-R1 | `POST /session/start` | [AS-BUILT][TZ §17.3] | |
| CD-R2 | `PUT /commands/{commandId}/result` | [AS-BUILT][TZ §17.7] | PUT — идемпотентный повтор |
| CD-R3 | `POST .../commands/execute` | [AS-BUILT][TZ §18.1] | |
| CD-R4 | `GET .../commands/{commandId}` | [AS-BUILT][TZ §18.1] | status lookup |
| CD-R5 | База 1С: `/hs/erp-integration/v1` | [AS-BUILT] | ТЗ §18.1 без `hs`; пути клиента относительны, URL задаётся `OneC:CommandApiBaseUrl` |
| CD-R6 | База ERP: `/api/integration/1c-agents/v1` | [AS-BUILT][TZ §17.1] | |
| CD-R7 | `POST /etl/batches`: gzip NDJSON + заголовки | [AS-BUILT] | starter-multipart устранён вместе с исправлением документа |
| CD-R8 | `POST /etl/runs/{runId}/complete` | [AS-BUILT][TZ §17.10] | |
| CD-R9 | `POST /commands/{commandId}/lease/renew` | маршрут в ERP OpenAPI; клиент не реализует; тела не определены | [UNDEFINED→PROPOSED] §3.2 |
| CD-R10 | Cancel instruction | канала нет ни в одном источнике | [UNDEFINED→PROPOSED] §6 |
| CD-R11 | `GET .../capabilities` | обязателен (ТЗ §18.1), DTO нет | [UNDEFINED→PROPOSED] §5 |
| CD-R12 | `POST /heartbeat`, `GET /configuration` (+304) | [AS-BUILT][TZ §17.8/§17.11] | |

`contracts/erp-agent-api.openapi.yaml` для ERP-контура не изменялся; замечания §12.2 —
к отдельной правке.

## 3. ACK получения, lease renewal, конфликт payload vs исходный результат

### 3.1 ACK (`POST /commands/{commandId}/received`) — текущее состояние

- **[AS-BUILT] A01 закрыт.** Приём: `CommandIntakeService.IntakeAsync` →
  `CommandValidator.Validate` → `AdmitCommandAsync` (одна SQLite-транзакция) → ACK.
  `AdmitCommandAsync` завершает транзакцию до ACK: `Stored` (queued), `Rejected` (невалидная:
  `result_pending`+`result_status=dead_letter`+`results_outbox` в той же транзакции,
  исполняемого статуса нет), `Duplicate`, `PayloadConflict` (только durable
  conflict-evidence aggregate). Ошибка ACK — `AckError`, повтор/backoff ERP; принятая
  работа уже durable. A01 использует текущую схему; локальное durable-evidence
  конфликта реализовано миграцией 004
  ([payload-conflict-events.md](payload-conflict-events.md)).
- **[HISTORICAL] baseline до A01:** отклонение фиксировалось после ACK
  (`CompleteLocallyAsync`), невалидная команда могла получить исполняемый статус —
  воспроизведено аудитом. К текущему коду неприменимо; упоминания «post-ACK rejection»
  в старых отчётах относятся к baseline.
- **[AS-BUILT] hash в ACK:** для всех исходов ACK уходит с hash **входящего** конверта
  (`CommandIntakeService.cs:35`), включая `PayloadConflict`/`Rejected`.
- **CD-ACK-1 [PROPOSED, не принято]:** в ACK передавать hash **сохранённого исходного**
  конверта (при конфликте — исходный, не конфликтующий). Не реализовано; требует
  подтверждения ERP (отсутствует).
- **CD-ACK-2 [AS-BUILT]:** допуск/отклонение одной транзакцией до ACK — реализовано
  (A01, `AdmitCommandAsync`).
- **CD-ACK-3 [AS-BUILT, локально]:** повторный `received` при `Duplicate` идемпотентен.
  При совпадающем hash сохранённый результат replay-ится в outbox до ACK `received`;
  команда не возвращается в исполнение. Локальная реализация: CD-RS-3 и
  [duplicate-result-replay.md](duplicate-result-replay.md).
- **CD-ACK-4 [PROPOSED, не принято]:** добавочное поле `ackStatus`
  (`received|duplicate|payload_conflict`) в теле `received` — единственный предложенный
  канал отчёта о конфликте. **Не принято; ERP отсутствует
  ([erp-integration-inventory.md](erp-integration-inventory.md)).** Старая/чужая ERP
  может не принимать новое поле — проверка обязательна до внешнего внедрения.

### 3.2 Lease renewal

- **CD-LS-1 [PROPOSED, не принято]:** тело запроса `{leaseId}`; ответ 200 —
  `{leaseId, leaseExpiresAtUtc}`; 409 — lease потерян.
- **CD-LS-2 [PROPOSED, консервативное] 204 без нового expiry:** агент **не выдумывает
  продлённый срок** произвольным локальным таймаутом. Консервативное поведение до
  явного capability/контракта: **не начинать новые POST-исполнения** команды; продолжать
  status lookup и доставку уже сохранённого результата (идемпотентные операции).
  Владение подтверждается только явным `leaseExpiresAtUtc` от ERP либо согласованным
  контрактом (не согласован). Неподтверждение/потеря lease не отменяет уже совершённый
  бизнес-эффект и не заменяет идемпотентность 1С (FR-CMD-004).
- **CD-LS-3 [PROPOSED]:** 404/405 на renew **не доказывают** отсутствие фичи — 404 может
  означать отсутствие ресурса (lease/команда не найдены). Без явного сигнала capability
  трактовать как «renewal не подтверждён» → CD-LS-2.

### 3.3 Конфликт payload (тот же commandId, иной hash)

- **CD-PC-1 [AS-BUILT, локально]:** исходная команда, результат, outbox и audit
  неизменны при `PayloadConflict`; конфликтующий конверт не исполняется. До возврата
  outcome и ACK `received` та же admission-транзакция durable-записывает или обновляет
  локальное critical evidence `COMMAND_PAYLOAD_CONFLICT` в `command_payload_conflicts`.
  Записываются только commandId, статус исходной команды, UTC first/last seen, счётчик и
  фиксированные SHA-256 UTF-8 fingerprints объявленных original/incoming hash-строк;
  payload, `requestedBy`, raw JSON и error text не сохраняются. Одинаковая hash-пара
  агрегируется, разные incoming fingerprints разделяются. Evidence не имеет FK и
  переживает обычный cleanup команды. Детали:
  [payload-conflict-events.md](payload-conflict-events.md).
- **CD-PC-2 [AS-BUILT локально / PROPOSED внешне]:** durable локальное событие
  конфликта реализовано; внешний отчёт ERP через `ackStatus` (CD-ACK-4) остаётся
  непринятым, поскольку ERP API отсутствует.
- **CD-PC-3 [PROPOSED, не принято]:** 409 в ответе на `received` (есть в ERP OpenAPI) —
  сигнал ERP о расхождении hash; агент не повторяет ACK, продолжает с исходной командой.
  В текущем локальном intake внешний status/ACK conflict-контракт не изменён. Семантика
  требует подтверждения ERP.
- **CD-PC-4 [AS-BUILT]:** 409 от 1С → `dead_letter` c `COMMAND_PAYLOAD_CONFLICT`
  (`CommandExecutionService`).

## 4. Повтор результата (duplicate result)

- **CD-RS-1 [TZ FR-CMD-016/017]:** `PUT result` повторяется до ACK; ERP принимает повтор
  **идентичного** результата и снова отвечает 204; `completed` — только после ACK.
- **CD-RS-2 [PROPOSED, не принято]:** тот же commandId с **иным** содержимым результата —
  409 + `ApiError{code:"RESULT_CONFLICT"}`, first-write-wins; агент — в `dead_letter`,
  ручной разбор. **Новая 409 может сломать старых клиентов:** существующий агент
  обрабатывает не-2xx как ошибку доставки и повторяет PUT (`ResultDeliveryWorker`) —
  внедрять поэтапно после согласования с ERP.
- **CD-RS-3 [AS-BUILT, локальная реализация; внешне не согласовано]:** при повторной
  выдаче команды с тем же ID/hash сохранённый `commands_inbox.result_json` атомарно
  возвращается в `results_outbox` до ACK `received`. Существующие identity/JSON/hash
  outbox сохраняются; `pending`/`retry_waiting` и backoff не сбрасываются; команда
  остаётся `completed`, audit/send/ordering не меняются. Cleanup guards для команды и
  attempts удерживают любой outbox со статусом, отличным от `acknowledged` (включая
  `pending`, `retry_waiting`, `sending`) до нового delivery ACK; `RecoverAsync` возвращает
  `sending` в `pending`. Timestamps ACK хранят время последнего подтверждения. Если
  outbox отсутствует, он восстанавливается только из
  сохранённого `result_json`, без данных duplicate. Локальные replay-инвариант и durable
  payload-conflict evidence FR-CMD-003 реализованы; внешний отчёт/409 и полный контракт
  остаются открыты. Доказательства: [duplicate-result-replay.md](duplicate-result-replay.md),
  [payload-conflict-events.md](payload-conflict-events.md).
- **CD-RS-4 [AS-BUILT] wire-формат результата** (`CommandExecutionService.cs:75,85`;
  `CommandExecutionWorker.cs:80,87`):
  - `status:"succeeded"` + `document` (passthrough из 1С) + `warnings` + `resultVersion`;
  - `status:"expired"|"dead_letter"|"business_error"` + `error{code,message,retryable,details:{}}`
    (валидационный `dead_letter` из `AdmitCommandAsync` — без `details`; получатель обязан
    считать `details` необязательным);
  - административные команды: `status:"succeeded"` + `data` (не `document`);
  - `cancelled` зарезервирован (§6), `business_failed` — словарь статусов **ответа 1С**,
    не wire-статус результата агента (на проводе всегда `business_error`).

## 5. 1С HTTP-контракт

Полная спецификация — `contracts/onec-command-api.openapi.yaml`.

- **CD-1C-1 [AS-BUILT] сериализация:** System.Text.Json `JsonSerializerDefaults.Web`:
  camelCase, порядок record-DTO, `null` выводится явно для nullable-членов; `Guid`/`int`
  (`commandId`, `payloadVersion`, `resultVersion`) не-null; `DateTimeOffset` сохраняет
  фактический offset (`+05:00`…; для UTC — `+00:00`, не `Z`); GUID — `D`, нижний регистр;
  `resultVersion` при отсутствии → 1. Игнорирование неизвестных свойств проверено для
  агента (STJ); для серверов 1С/ERP — **не проверялось**, не заявлять.
- **CD-1C-2 [AS-BUILT] статусы/коды:** `OnecCommandClient.SendAsync` **общий** для
  `POST execute` и `GET status` — классификация идентична (таблицы в OpenAPI):
  словари `succeeded|completed|created` / `processing|executing|pending` / `not_found` /
  `business_failed|failed|rejected|validation_error`; иная строка → `ONEC_UNKNOWN_STATUS`;
  2xx требует тела, **кроме 202** (тело опционально); отсутствующий/`null` status в 2xx
  (кроме 202) — **не легитимный ответ**, клиент деградирует в сбой/неопределённый исход;
  400/401/403/422 → бизнес-ошибка; 409 → `PayloadConflict`; 404 → `NotFound` (до парсинга
  тела); прочие не-2xx и транспорт/таймаут → технические (`ONEC_TRANSPORT_ERROR`,
  `ONEC_TIMEOUT` — «результат неизвестен»).
- **CD-1C-3 [TZ FR-CMD-011]:** журнал `commandId → DocumentRef` обязателен; без него
  автоматический повтор POST запрещён; повтор — только с исходным ID (FR-CMD-012).
- **CD-1C-4 [AS-BUILT]:** `requestedAtUtc` = `CommandEnvelope.CreatedAtUtc` (время
  создания команды ERP), не время вызова (`OnecCommandClient.cs:16`).
- **CD-1C-5 [UNDEFINED→PROPOSED]:** `CapabilitiesResponse` — по фактуре mock
  (`{commandTypes, versions}`); DTO нет. Требует подтверждения владельца 1С для внешнего
  контракта; локально использовать как есть.
- **CD-1C-6 [AS-BUILT + PROPOSED]:** 409-тело mock — `{error:{code,message,retryable}}`
  (частичный `OnecCommandResponse`); [PROPOSED, не принято] стандарт — полный
  `OnecCommandResponse`. При отсутствии `error.code` клиент подставляет
  `COMMAND_PAYLOAD_CONFLICT`.
- **CD-1C-7 [AS-BUILT]:** HTTP Basic из DPAPI-секрета (`OnecAuthentication`).
- **CD-1C-8 [AS-BUILT]:** `payloadHash` = base64(SHA-256(канонический JSON: компактно,
  ключи объектов Ordinal, строки по `Utf8JsonWriter`)) — `PayloadHasher.cs`; единый для
  ERP ↔ агент ↔ 1С. Примеры hash в OpenAPI вычислены по нему (закрепить тестом позже).
- **CD-1C-9 [AS-BUILT]:** `payload`, `details`, `document` в DTO — `JsonElement`
  (**произвольный JSON**: объект, массив, скаляр, null), не объектные структуры.
  Объектные бизнес-формы (payload по типу команды; `DocumentSummary`
  `{type,ref,number,date,posted}` из mock/FR-CMD-015; `details:{}`) — наблюдаемая/
  [PROPOSED] форма, типами C# не обеспечена; в OpenAPI помечены отдельно. Агент
  не интерпретирует payload и не передаёт `requestedBy` в 1С.
- **CD-1C-10 [PROPOSED, локально]:** сверять `commandId` ответа с запросом (as-built не
  сверяет).

## 6. Отмена команды — интерфейс не определён

FR-CMD-018 требует cancel instruction от ERP; канала нет ни в ERP OpenAPI, ни в клиентах,
ни в mock. Локальная реализация предложений ниже авторизована; внешний контракт — после
подтверждения ERP.

- **CD-CN-1 [PROPOSED, не принято]:** транспорт — добавочное `cancelCommandIds: uuid[]`
  в ответе `POST /commands/lease` (long-poll сохраняет outbound-only; новых endpoint нет).
- **CD-CN-2 [PROPOSED]:** отмена — только до передачи в 1С и только для типов с признаком
  `cancellable` (реестр удалённой конфигурации; default `false`, fail-closed). Уже
  исполняемая в 1С не прерывается; фактический результат доставляется штатно.
- **CD-CN-3 [PROPOSED]:** подтверждение отмены — `PUT result` со `status:"cancelled"`
  (значение уже в enum ERP OpenAPI). Повтор cancel финальной команды — идемпотентный
  повтор результата.
- **CD-CN-4 [PROPOSED]:** `Queued → Cancelled` только атомарным claim (общая работа с
  A09/этап 1); для `Executing`/`UnknownResult` — no-op + событие; прерывание вызова 1С
  запрещено фронтом решений (FR-CMD-018).

## 7. Durable ETL: «принято» vs «завершено»

- **CD-ETL-1 [PROPOSED]:** `succeeded` административной команды (`start_full_sync`,
  `reload_entity`) = **только принятие**: `data:{accepted:true, runId, mode}`; ответ
  формируется после атомарной фиксации (одна транзакция: `etl_jobs(command_id)` +
  завершение команды + outbox). Повтор команды — тот же runId, не второй job.
- **CD-ETL-2 [PROPOSED — отложение, не отклонение]:** при `pause`/`disabled`/
  backpressure/переполнении канала **принятое** durable-задание **откладывается** в очереди
  с явной причиной и исполняется позже (план, этап 3: «сохранять работу ожидающей с явной
  причиной»); результат приёма остаётся `accepted`. Отклонение легитимно только **до**
  принятия (валидация: неизвестный тип, нет `entity` и т.п.). Текущий дефект as-built:
  переполнение RAM-канала даёт отказной результат `business_error` c
  `ETL_TRIGGER_QUEUE_FULL` (`CommandExecutionWorker.cs:61-70`) — отклонение принятой
  работы против плана; заменяется durable-очередью (этап 3). Wire-статус отказа —
  `business_error` (не `business_failed`, см. CD-RS-4).
- **CD-ETL-3 [AS-BUILT]:** «завершено» — только жизненный циклом run
  (`pending→running→uploading→completing→succeeded|partial_success|failed|cancelled`)
  и сигналами `POST /etl/runs/{runId}/complete` + `heartbeat.etl`. Успех приёма не означает
  завершения; завершение не означает успеха каждой сущности (§8).
- **CD-ETL-5 [AS-BUILT]:** `pause_etl`/`resume_etl`/`collect_diagnostics`/
  `run_connectivity_test`/`rotate_certificate_hint` (ТЗ §17.12;
  `CommandExecutionWorker.TryExecuteAdministrativeAsync`) — `succeeded` = локальный эффект
  применён; к run не относится.

## 8. Batch ACK → complete run → commit watermark; частичный успех

- **CD-ORD-1 [TZ FR-ETL-005] нормативный порядок:** (1) окно прочитано; (2) **все** batch
  подтверждены (`acknowledged`, `checksumValid`, `rowsAccepted == row_count`); (3) complete
  run принят ERP (204, идемпотентно); (4) **одной транзакцией** — commit watermark(ов)
  полностью подтверждённых сущностей + финализация run. [HISTORICAL] схема §13.1 показывала
  commit до complete — расхождение **устранено**, §13.1 приведена к порядку FR-ETL-005.
  Авария до шага 4 → прежний committed watermark; повтор complete/commit безопасен.
- **CD-B-1 [PROPOSED, локально]:** агент обязан валидировать ACK (batchId, status,
  checksum, rows) — в baseline и текущем коде уже проверяются `BatchId`, `ChecksumValid` и `RowsAccepted`;
  непроверенным остаётся поле `Status` (`EtlBatchUploadWorker`). Негативный ACK → batch в `dead_letter`/quarantine, run не
  продвигается.
- **CD-B-2:** повтор `batchId` — прежний ACK (FR-ETL-011). [PROPOSED, не принято] тот же
  `batchId` с иным checksum → 409 `BATCH_CONFLICT`, quarantine; новая409 — см. предостережение
  CD-RS-2.
- **CD-B-3 [AS-BUILT→PROPOSED]:** заголовки фактически шлются (Idempotency-Key,
  X-Content-SHA256 — hash сжатого файла, X-Batch-Id, X-Run-Id, X-Entity, X-Schema-Version,
  X-Row-Count, X-Agent-Id и др.); не хватает FR-ETL-008: `X-Watermark-From/To`,
  `X-Uncompressed-Size`, `X-Created-At` — добавить заголовками (старая ERP может игнорировать;
  проверить).
- **CD-P-1 [TZ FR-ETL-016]:** политика `fail_run|continue_other_entities|retry_entity`;
  `partial_success` — часть сущностей подтверждена, часть провалена окончательно.
  **Неподтверждённый участок не попадает в committed watermark** — commit только по
  сущностям со всеми подтверждёнными batch.
- **CD-P-2 [PROPOSED]:** форма complete-summary (в ERP OpenAPI сейчас `object`) —
  `runId, mode, status, started/finishedAtUtc, entities[{entity, rows, batches, outcome,
  watermarkFrom/To}]`. Агент уже шлёт объект summary; **принимает ли/отвергает ли его
  существующая ERP — не проверено (ERP отсутствует)** — проверить до внедрения формы.
- **CD-P-3 [PROPOSED + открытый вопрос]:** commit пустого окна — только при доказанной
  полноте чтения (граница снимка на старте run); верхняя граница даты — не snapshot без
  гарантий источника. Для источников без гарантий вопрос открыт (§10).
- **CD-P-4 [PROPOSED]:** cleanup не трогает данные незавершённого run до локального commit
  (A05, этап 4).

## 9. Индекс решений

| ID | Решение | Статус |
|---|---|---|
| CD-R1..R8, R12 | Канонические маршруты = реализация | [AS-BUILT][TZ] |
| CD-R9/R10/R11 | renew / cancel / capabilities | [UNDEFINED→PROPOSED] |
| CD-ACK-2 | Атомарный допуск до ACK | [AS-BUILT] (A01; conflict-evidence — migration 004) |
| CD-ACK-1, CD-ACK-4, CD-PC-3 | hash исходного в ACK, `ackStatus`, conflict-409 | [PROPOSED, не принято] (ERP отсутствует) |
| CD-PC-2 | Durable локальное critical evidence; внешний `ackStatus` | [AS-BUILT локально][PROPOSED внешне] |
| CD-LS-1..3 | renew-тела; 204 без expiry → стоп новых POST, lookup/доставка продолжаются; 404≠отсутствие фичи | [PROPOSED, не принято] |
| CD-PC-1 | Исходная команда/результат неизменны при конфликте | [AS-BUILT] |
| CD-RS-1 | Идемпотентный повтор PUT | [TZ] |
| CD-RS-2, CD-B-2 | `RESULT_CONFLICT`/`BATCH_CONFLICT` 409 (может сломать старых клиентов) | [PROPOSED, не принято] |
| CD-RS-3 | Повторная доставка сохранённого результата при `Duplicate`; cleanup удерживает любой неподтверждённый replay, включая `sending` | [AS-BUILT, локально]; внешний conflict/reporting открыты |
| CD-1C-1..4, 6a, 7..9 | Фактический контракт 1С (DTO/статусы/hash/Basic) | [AS-BUILT] |
| CD-1C-5, 1C-6b, 1C-10 | capabilities-схема; стандарт 409-тела; сверка commandId | [PROPOSED] |
| CD-CN-1..4 | Отмена (`cancelCommandIds`, `cancellable`) | [UNDEFINED→PROPOSED] |
| CD-ETL-1 | accepted vs completed | [PROPOSED] |
| CD-ETL-2 | Отложение durable-задания вместо отклонения; wire `business_error` | [PROPOSED] (as-built отклоняет — этап 3) |
| CD-ORD-1 | extract → ACK всех → complete → commit | [TZ] (§13.1 исправлена) |
| CD-B-1, CD-B-3 | Валидация ACK; метаданные FR-ETL-008 | [PROPOSED] |
| CD-P-1..4 | partial_success; summary; пустое окно; cleanup | [TZ]+[PROPOSED] |

## 10. Открытые вопросы и внешние зависимости

Локальные исправления (этапы 1–6) от них не зависят; внешнее развёртывание — зависит.

1. **ERP:** Agent API отсутствует в архиве исходников
   ([erp-integration-inventory.md](erp-integration-inventory.md)). Подтвердить:
   `ackStatus` (CD-ACK-4), renew-тела (CD-LS-1), `cancelCommandIds` (CD-CN-1),
   `RESULT_CONFLICT`/`BATCH_CONFLICT` (CD-RS-2/CD-B-2), тело complete-summary (CD-P-2),
   приёмку внезапных 409 старыми клиентами.
2. **1С** (информация по расширению позже): журнал идемпотентности, status lookup,
   `CapabilitiesResponse`, спецификации payload/результатов по типам, часовой пояс
   `Edm.DateTime` (A10).
3. Политика `cancellable` по типам; семантика пустого окна/snapshot (CD-P-3).

## 11. Совместимость и миграции

- Аддитивные необязательные поля — конвенция [PROPOSED] (ТЗ §30.2), но: игнорирование
  неизвестных полей проверено **только для агента** (System.Text.Json); поведение ERP/1С
  не проверено и не заявляется.
- **Новая 409 может сломать старых клиентов** (CD-RS-2/CD-B-2): существующий агент на
  не-2xx повторяет доставку — внедрение поэтапное, после согласования и тестов.
- **Тело complete-summary:** факт, что агент шлёт объект, не доказывает, что существующая
  ERP его принимает/игнорирует — проверить до внедрения CD-P-2.
- **Откат бинарника НЕ объявляется безопасным** только на основании аддитивных колонок:
  durable jobs, изменённое состояние очередей и новая логика приёма требуют явных
  rollback-тестов (обновление → работа → откат → проверка очередей). Без тестов откат —
  непроверенный риск.
- Миграции: A01/A02 закрыты без отдельных миграций; локальное durable payload-conflict
  evidence добавлено миграцией 004 с checksum и обновлением schema version. Retention/
  cleanup этой таблицы остаётся stage 6. Для предложений остаются: `cancel_*`, durable
  `etl_jobs(command_id)`, итоговые cursors run (A05) — последовательные миграции с
  checksum и проверкой БД с незавершённой работой.
- Mock валиден для реализованного подмножества; только локальная разработка/тесты,
  не production.
  Ветки renew/cancel/conflict/отрицательного ACK покрыть тестами с подменёнными клиентами
  (этапы 1–4); доработка mock — отдельной задачей.

## 12. Сверка refs/schema/DTO и ограничения валидации

### 12.1 `onec-command-api.openapi.yaml` ↔ DTO (ручная сверка)

| Схема OpenAPI | DTO (`OnecContracts.cs`) | Статус |
|---|---|---|
| ExecuteCommandRequest (7 полей) | ExecuteCommandRequest(...) | совпадает (порядок record) |
| OnecCommandResponse (6 полей) | OnecCommandResponse(..., ResultVersion = 1) | совпадает |
| `payload`/`document`/`details` — произвольный JSON | `JsonElement`/`JsonElement?` | совпадает (as-built) |
| DocumentSummary `{type,ref,number,date,posted}` | отсутствует в DTO | [PROPOSED] бизнес-форма (mock/FR-CMD-015), помечена в YAML |
| ApiErrorBody (4 поля) | ApiErrorBody(...) | совпадает |
| OnecHealthResponse (5 полей) | OnecHealthResponse(...) | совпадает |
| CapabilitiesResponse | отсутствует в DTO | [PROPOSED] по mock |
| ErrorEnvelope | отсутствует | фактура mock 409 |
| basic auth | OnecAuthentication | совпадает |

Статусы/коды в описаниях — дословно по `OnecCommandClient.SendAsync` (общему для GET/POST).

### 12.2 `erp-agent-api.openapi.yaml` (просмотрено, не изменялось)

1. `GET /configuration` — без схемы при наличии DTO `RemoteConfigurationResponse`.
2. `lease/renew` — без тел и семантики потери lease (CD-R9, CD-LS-*).
3. `etl/batches` — не описаны `X-Schema-Version`, `X-Row-Count`, `Content-Encoding: gzip`.
4. `CommandResult.status` содержит `cancelled` (зарезервирован, §6).
5. `ReceivedAck`≡`CommandReceivedRequest`, `BatchAck`≡`BatchAcknowledgement` — поля совпадают.

### 12.3 Валидация и ограничения

Локально, без установки (python3 + PyYAML 6.0.1): `yaml.safe_load` — синтаксис OK;
все локальные `$ref` разрешаются; базовая структура OpenAPI 3.1 — без расхождений
(результат повторной проверки — в отчёте этапа).

**Ограничения:** `openapi_spec_validator`/`spectral`/`redocly` отсутствуют (установка
запрещена заданием) → семантическая валидация OpenAPI 3.1/JSON Schema **не выполнялась**;
соответствие сериализации — по исходникам DTO/клиента/mock, не генерацией из кода;
`payloadHash`-примеры вычислены внешне — закрепить тестом; поведение сторонних серверов
в части игнорирования полей и принятия новых тел — не проверялось (ERP отсутствует,
расширение 1С — позже).
