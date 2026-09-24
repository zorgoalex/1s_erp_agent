# Этап 0 — базовая точка и подготовка (baseline)

Дата: 24.09.2026.
Статус: этап 0 выполнен в объёме инвентаризации, сборки, тестов, статической ревалидации и фиксации контрактных решений. Производственные исходники не изменялись.
Основание: [план ремедиации](../../../spec_1c-agent/plans/remediation-plan-2026-09-23.md) §4, [аудит A01–A11](../../../spec_1c-agent/reviews/architecture-audit-2026-09-23.md).

## 1. Среда и SDK

| Параметр | Значение |
|---|---|
| Хост выполнения команд | WSL2 (Linux 6.18.33.2-microsoft-standard-WSL2), запуск Windows PE через interop / `powershell.exe` |
| ОС сборки | Windows 10 Домашняя 10.0.19045 (x64), RID `win-x64` |
| .NET SDK | 10.0.400 (локальный `repo_1c-agent/.dotnet`, host 10.0.11, MSBuild 18.9.6) |
| Runtimes | Microsoft.NETCore.App 10.0.11, Microsoft.AspNetCore.App 10.0.11, Microsoft.WindowsDesktop.App 10.0.11 |
| global.json | `10.0.400`, `rollForward: latestPatch`, `allowPrerelease: false` |
| Глобальные SDK | Не устанавливались; в WSL Linux-`dotnet` отсутствует |

Контаминация obj/bin Windows↔Linux исключена: Linux-тулчейна нет. Существующие `obj/` были созданы Windows-сборкой по старому пути `D:\Project\1C-agent\...`; репозиторий перенесён в `D:\WORK\CNC_Milling\WORK_CNC\SOFT\1C\1C-agent\repo_1c-agent`, поэтому `restore` регенерировал assets под текущий путь. Файлы не удалялись.

**Git: репозиторий отсутствует.** `git status` в корне workspace и в `repo_1c-agent` — «not a git repository». Ревизия/коммит зафиксировать невозможно; снапшот базовой точки выполняет оркестратор. `git init` не выполнялся.

## 2. Инвентаризация

- Solution `ErpOnecAgent.sln` — 8 проектов:
  - `src/`: ErpOnecAgent.Domain, .Application, .Contracts, .Infrastructure, .Service (win-x64);
  - `tests/`: ErpOnecAgent.UnitTests, ErpOnecAgent.IntegrationTests;
  - `tools/`: ErpOnecAgent.MockServer.
- TFM `net10.0`, `TreatWarningsAsErrors=true`, lock-файлы NuGet (`Directory.Build.props`, `Directory.Packages.props` — 13 pinned `PackageVersion`).
- Исходники: 50 `.cs` в `src/` + `tests/` (без obj/bin).
- Контракты: `contracts/erp-agent-api.openapi.yaml` (OpenAPI 3.1); клиентские маршруты — `src/ErpOnecAgent.Infrastructure/ErpApi/ErpClient.cs`, `OneC/OnecCommandClient.cs`, `OneC/OnecHealthClient.cs`, `OneC/OnecODataClient.cs`.
- Тесты: 16 методов (11 unit-классовых имён, один Theory с 2 кейсами; 5 integration) — см. §3.
- Диагностический стенд аудита: `local-data/audit-2026-09-23/probes/` (AuditProbes.csproj + Program.cs, 13 сценариев), запускаемый, результаты `probe-results.json`.
- Прочее: `installer/powershell/*.ps1` (8 скриптов), `installer/wix/Package.wxs` (каркас), `.github/workflows/ci.yml`, `artifacts/publish/win-x64` (устаревшая публикация), `local-data/e2e/` (пустые data/logs/secrets/spool).
- ERP-снапшот: `spec_1c-agent/erp/repo/refine-mvp_for_mfg-app-main.zip` (архив, маршруты Agent API в нём ранее не обнаружены — подтверждается `docs/api-contracts.md`).

## 3. Baseline restore/build/test (Windows, из WSL)

Команды (рабочий каталог `D:\WORK\CNC_Milling\WORK_CNC\SOFT\1C\1C-agent\repo_1c-agent`, `dotnet` = `.\.dotnet\dotnet.exe`):

| Команда | Код выхода | Результат |
|---|---|---|
| `dotnet restore ErpOnecAgent.sln --locked-mode` | 0 | 8 проектов восстановлены (~20 c) |
| `dotnet build ErpOnecAgent.sln -c Release --no-restore` | 0 | 0 предупреждений, 0 ошибок (10,02 c) |
| `dotnet test ErpOnecAgent.sln -c Release --no-build --no-restore --logger "trx;LogFilePrefix=baseline" --results-directory local-data\remediation-2026-09-24\baseline\TestResults` | 0 | Unit 12/12, Integration 5/5; всего 17/17 |
| `dotnet run --project local-data\audit-2026-09-23\probes\AuditProbes.csproj -c Release -- local-data\remediation-2026-09-24\baseline\probe-results.json` | 0 | 13/13 сценариев воспроизведены |

Тесты (по TRX): unit — CommandTests (4), EtlTests (2), OnecCommandClientTests (1), OnecODataClientTests (4 + 1 кейс Theory); integration — SqliteStoreTests (5: идемпотентность/конфликт, snapshot-атомарность, recovery executing→unknown_result, транзакционный outbox, spool+run completion).

Доказательства (`repo_1c-agent/local-data/remediation-2026-09-24/baseline/`):
- `restore.log`, `build.log`, `test.log`, `probes-run.log`;
- `TestResults/baseline_net10.0_20260924002339.trx` (unit), `TestResults/baseline_net10.0_20260924002340.trx` (integration);
- `probe-results.json`, `dotnet-info.txt`, `windows-os.txt`.

Прохождение 17 штатных тестов воспроизводимо и, как зафиксировано аудитом, не закрывает дефекты: соответствующих проверок инвариантов в suite нет.

## 4. Ревалидация A01–A11 (статически + пробы)

Все дефекты **подтверждены в текущем коде**. Пробы 24.09.2026 дали результаты, идентичные аудиту 23.09.2026 (включая 17/16 переполнение, A,B,C,D,C,D пагинацию, `PauseCommands → Normal`, attempt 10 s / total 30 s / long poll 25 s, publishedHasKeyFields=false vs currentHasKeyFields=true).

| ID | Подтверждённое место (текущие строки) | Ключевой факт пробы |
|---|---|---|
| A01 P1 | `src/ErpOnecAgent.Service/Workers/CommandLeaseWorker.cs:51` (валидация) → `:52` (безусловный store `queued`) → `:59` (ACK) → `:62-66` (отклонение после ACK; при сбое ACK не выполняется); `src/ErpOnecAgent.Application/Commands/CommandValidator.cs:20` — отвергает только `PayloadVersion <= 0`, allowlist версий отсутствует | `validatorAccepts:false`, `postCalls:1` |
| A02 P1 | `src/ErpOnecAgent.Service/Workers/CommandExecutionWorker.cs:44` — expiry проверяется раньше ветки `UnknownResult` (`:50`) | `statusCalls:0`, `reportedStatus:"expired"` |
| A03 P1 | `src/ErpOnecAgent.Service/Runtime/EtlTrigger.cs:9-11` — bounded(16) `DropWrite`; `src/ErpOnecAgent.Service/Workers/OnecEtlWorker.cs:28-30` — проигравший `ReadAsync` не отменяется; `CommandExecutionWorker.cs:91,111` — результат админ-команды сохраняется после записи только в RAM; `OnecEtlWorker.cs:33-34` — уже прочитанный запрос отбрасывается при pause/limit | `accepted:17,delivered:16`; `totalExtractions:1` из 2 |
| A04 P1 | `src/ErpOnecAgent.Infrastructure/Persistence/Sqlite/SqliteAgentStore.cs:16-27` — `RecoverAsync` не восстанавливает незавершённые runs; `OnecEtlWorker.cs:48-50` — всегда новый run; `src/ErpOnecAgent.Infrastructure/Spool/FileSpoolStore.cs:43` — файл в `ready` до `RegisterBatchAsync` (`SqliteAgentStore.Etl.cs:19`), пара не атомарна | `runStatus:"running"`, `watermarkRows:"0"` |
| A05 P1 | `SqliteAgentStore.Etl.cs:87-93` — выбор ACK batch без проверки завершённости run; `:114` — `deleted` блокирует условие завершения; `:122` — итоговые watermarks пересобираются из удаляемых строк | `beforeWatermarks:1 → afterWatermarks:0` |
| A06 P1 | `src/ErpOnecAgent.Service/Program.cs:54-64` — `AddStandardResilienceHandler()` без настройки таймаутов | attempt 10 s, total 30 s, long poll 25 s |
| A07 P1 | `src/ErpOnecAgent.Service/Runtime/ErpSessionManager.cs:27` — handshake без maintenance безусловно ставит `Normal` | `PauseCommands → Normal` |
| A08 P1 | `OnecEtlWorker.cs:34` и `FileSpoolStore.cs:23-24,42` — ограничение только по размеру spool; `MinimumReservedBytesForCommands` не используется как запрет записи ETL (по путям использования; живое заполнение диска не выполнялось) | — (статика) |
| A09 P2 | `CommandExecutionWorker.cs:59` — лимит lookup по счётчику POST, lookup счётчик не увеличивает; `:53` — ветка `NotFound` без своего лимита; `SqliteAgentStore.Commands.cs:152` — фиксированная задержка 2 с вместо retry policy; `Commands.cs:56` — строго `<` по `received_at_utc` (ничья допускает 2 команды); `SqliteAgentStore.Etl.cs:171` — метрика `status='dead_letter'`, а `Commands.cs:106` пишет `status='result_pending'` + `result_status` | 5 lookup при лимите 2 (`AttemptCount=1`); `runnableSameKey:2`; `reportedCount:0` при наличии dead_letter |
| A10 P2 | `src/ErpOnecAgent.Infrastructure/OneC/OnecODataClient.cs:33-38` — после continuation возврат к `$skip` без учёта continuation-страниц; `OnecEtlWorker.cs:63` — `reconcile_keys`/`reconcile_totals` = тот же full-read; `window_reload` отсутствует; страница целиком в `JsonDocument` (`:30`), лимита байт ответа нет; `FormatDate` (`:86-96`) — `Edm.DateTime` без offset трактуется как UTC | `rows:[A,B,C,D,C,D]` |
| A11 P2 | `artifacts/publish/win-x64/ErpOnecAgent.Domain.dll` — нет `KeyFields`; в исходниках есть (`src/ErpOnecAgent.Domain/Etl/EtlModels.cs:23`) | published=false vs current=true |

Перенос сценариев стенда в поддерживаемые тестовые проекты (с заменой reflection на рабочие интерфейсы, где возможно) — работа этапов 1–5; стенд пригоден как источник регрессий (проверен, запускается, воспроизводит).

## 5. Контракты: состояние, предлагаемые локальные решения, внешние вопросы

Фактические маршруты кода сверены с `contracts/erp-agent-api.openapi.yaml`, ТЗ v1.0 (§17–18) и черновиками starter-pack (`API_CONTRACTS.md`, `starter_note.md`).

### 5.1. Расхождения черновиков с ТЗ (предлагаемое локальное решение — ТЗ v1.0, код уже соответствует)

| Вопрос | Черновики starter-pack | ТЗ v1.0 | Код агента | Решение |
|---|---|---|---|---|
| Сессия ERP | `POST /sessions` | `POST /session/start` (§17.3) | `ErpClient.cs:18` ✓ | ТЗ: `/session/start` |
| Отправка результата | `POST /commands/{id}/result` | `PUT /commands/{id}/result` (§17.7, идемпотентность) | `ErpClient.cs:27` ✓ | ТЗ: `PUT` |
| Вызов 1С | `POST /commands` | `POST /erp-integration/v1/commands/execute` (§18.1) | `OnecCommandClient.cs:17` ✓ | ТЗ: `commands/execute` от configured base |
| Статус 1С | `GET /commands/{id}` | `GET /erp-integration/v1/commands/{commandId}` | `OnecCommandClient.cs:24` ✓ | ТЗ |

Остальные маршруты ERP (lease, received, heartbeat, etl/batches, etl/runs/{runId}/complete, configuration?currentVersion) совпадают во всех источниках и в mock (`tools/ErpOnecAgent.MockServer/Program.cs:28-44`).

### 5.2. Подтверждённые пробелы «контракт есть — кода нет»

- `POST /commands/{commandId}/lease/renew` — в OpenAPI и ТЗ §17.6 есть; в `IErpClient`/`ErpClient` отсутствует (подтверждает замечание аудита про lease renewal).
- `GET /erp-integration/v1/capabilities` — в ТЗ §18.1 и mock есть; агент не вызывает.
- Метаданные batch неполные относительно FR-ETL-008: заголовками batch передаются `Idempotency-Key, X-Content-SHA256, X-Batch-Id, X-Run-Id, X-Entity, X-Schema-Version, X-Row-Count` (`ErpClient.cs:32-38`); идентичность агента доступна транспортно как общий заголовок `X-Agent-Id` каждого запроса (`ErpClient.cs:81`), но не как поле метаданных batch. Не передаются вовсе: `watermark_from/to`, `uncompressed_size`, `compressed_size`, `created_at`.
- §13.1 ТЗ (mermaid) фиксирует commit watermark до `Complete etl_run` — противоречит FR-ETL-005 (commit после подтверждения завершения run ERP). План §4 предписывает опираться на FR-ETL-005; правка §13.1 — задача этапа 4/документационная, в этапе 0 не выполнялась.

### 5.3. Предлагаемая локальная модель семантики (для этапов 1–4; не является согласованием с владельцами)

1. ACK получения отправляется только после атомарного решения «допущена/отклонена» (FR-CMD-002/008); отклонение завершается локальным результатом для доставки ERP, а не открытием исполнения.
2. Lease после локального сохранения — подтверждение владения, не гарантия уникальности (FR-CMD-004); renewal по контракту §17.6 реализуется в этапе 2.
3. Повторный результат для уже завершённой команды — повторная доставка сохранённого результата (FR-CMD-003).
4. Конфликт `commandId`/hash — устойчивый conflict-результат и критическое событие, без изменения исходной команды (FR-CMD-003, `409 command_payload_conflict`).
5. Отмена — только до передачи в 1С, транзакционно с захватом исполнения.
6. Административное ETL-задание: «принято» = durable job в SQLite в той же транзакции, что и результат команды; «выгрузка завершена» — отдельное событие run.

### 5.4. Внешние нерешённые вопросы (владельцы ERP/1С; согласования НЕ получены)

1. Реализация ERP Agent API (в предоставленном снапшоте ERP модуль `/api/integration/1c-agents/v1` не обнаружен): transactional outbox, lease/renewal, идемпотентный приём результата, batch ACK/upsert, complete run, configuration, RBAC/аудит. Ответственный и сроки — открыты.
2. Расширение 1С (BSL): журнал commandId/hash, конкурентная дедупликация, атомарность «документ + результат», status lookup, health/capabilities, версии payload. В материалах не найдено; владелец — открыт.
3. Подтверждение семантики §5.3 (пп. 1–6) владельцами, включая форму `409` и коды ошибок ACK/result/batch.
4. Часовой пояс `Edm.DateTime` без offset на стороне 1С; допустимость `Date`/`Period` как даты изменения — по образцам реальной базы.
5. Политика потери lease и необходимость renewal на реальной ERP; поведение empty run и partial success при complete.
6. URLs тестовых ERP/1С, credentials, сертификаты, версии платформы/конфигурации 1С, согласованные payload первого документа и ETL mappings (вход этапа 7).

## 6. Внешние блокеры этапа 0

- Нет git-репозитория — ревизия не фиксируется; базовый снапшот — на оркестраторе.
- Репозиторий перенесён (`D:\Project\1C-agent` → текущий путь): старые NuGet-assets перегенерированы restore; побочных эффектов не выявлено, сборка/тесты/пробы зелёные.
- Реальные ERP/1С, сертификаты, ACL, установка службы — вне границ этапа 0 и не выполнялись.
- `spec_1c-agent/windowsdesktop-runtime-10.0.11-win-x64.exe` — рантайм уже присутствует в локальном `.dotnet`; установка не требовалась.

## 7. Рекомендуемый первый срез реализации (вход в этап 1)

**Атомарный допуск команды (A01) + allowlist версий payload.** Наименьшая и самая рискованная граница «валидация → очередь»:

1. Регрессионный тест из пробы `invalid_command_executes_before_validation_handling` в поддерживаемый тестовый проект: невалидная команда (hash/версия вне allowlist) + задержанный/падающий ACK → 0 вызовов 1С, результат отклонения сохранён для доставки.
2. Изменение `CommandLeaseWorker`/`IAgentStore`: единая транзакция «допустить или отклонить» до появления исполняемого состояния; `CommandValidator` — allowlist поддерживаемых версий из конфигурации.
3. Затем по плану этапа 1: дедупликация/конфликт (A09-часть), expiry/unknown_result (A02), retry-политика, ordering.

Стартовая проверочная команда среды (проверена 24.09.2026, exit 0 на всех шагах):

```powershell
.\.dotnet\dotnet.exe restore .\ErpOnecAgent.sln --locked-mode
.\.dotnet\dotnet.exe build .\ErpOnecAgent.sln -c Release --no-restore
.\.dotnet\dotnet.exe test .\ErpOnecAgent.sln -c Release --no-build --no-restore
```

## 8. Решение этапа 0

Критерии выполнены в части подготовки: воспроизводимая базовая сборка и тесты зафиксированы (§3), актуальные дефекты подтверждены с текущими позициями кода и пробами (§4), локальная модель контрактов предложена (§5.1–5.3), внешние неопределённости перечислены без присвоения согласования (§5.4). Открытые внешние вопросы блокируют только этапы 7–8 и финальную фиксацию контрактов; локальные исправления этапов 1–6 разблокированы.
