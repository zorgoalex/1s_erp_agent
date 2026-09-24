# Инвентаризация ERP-исходников для интеграции Agent API

Дата: 24.09.2026.
Метод: чтение архива `spec_1c-agent/erp/repo/refine-mvp_for_mfg-app-main.zip` напрямую через Python `zipfile` без распаковки (3192 файла). Секреты, `.env`, токены и пароли не извлекались и не приводятся: файлов `.env*` в архиве нет; `backend/env.example.md` содержит только имена переменных (значения не цитируются). Статус: обзор завершён; **интеграция не реализована и не согласована** — документ планировочный для этапа 7.
Основание: [план ремедиации §11 (этап 7)](../../../spec_1c-agent/plans/remediation-plan-2026-09-23.md), [ТЗ §17–18](../../../spec_1c-agent/starter-pack/TZ_Local_1C_Agent_CSharp_Windows_v1.0.md).

## 1. Состав архива

`refine-mvp_for_mfg-app-main/` — монорепозиторий производственного ERP/CRM (не только NestJS):

| Каталог | Роль | Ключевые факты |
|---|---|---|
| `src/` | Frontend | React 18 + Refine + Antd + Vite (`package.json`: `front_refine_v1`), ~765 файлов страниц |
| `api/` | Vercel serverless (legacy-контур) | `api/_lib/`: `db.ts` (Hasura GraphQL admin), `jwt.ts`, `password.ts`, `roles.ts`, `verify-token.ts`, `rate-limit.ts`, `auth0Token.ts` |
| `backend/` | **Основной backend — NestJS 11** | `backend/package.json`: `@nestjs/common|^11.1.19`, `@nestjs/platform-express`, `@nestjs/swagger`, `pg`, `redis`, `zod`; 1167 файлов в `backend/src` |
| `backend/db/migrations/` | PostgreSQL-миграции | 168 SQL-файлов, нумерация `001..145` (+ спецфайлы `034_preflight/rollback/verify`), большинство с `.test.ts` |
| `backend/contracts/` | OpenAPI backend | `04-api-contract.openapi.yaml` (ERP/CRM Backend API, path-versioning `/api/v1`) |
| `ops/` | Эксплуатация | `apply-migrations.sh` (ledger-раннер миграций), `apply-hasura-metadata.sh`, systemd, docker-compose, backup/restore |
| `tests/`, `e2e/` | Playwright | e2e + stage-canary набор (~60 spec) |
| `vitest.*.config.ts` | Vitest | unit + 8 интеграционных конфигов |
| `cnc-telegram-worker/`, `glm-ocr-runner/`, `ocr-service/` | Python-сервисы | отдельные Docker-образы |
| `docs/` | Документация | `configuration-and-auth.md`, `development-and-testing.md`, `deployment-and-operations.md`, `security-manual-checklist-2026-05-17.md` |

## 2. NestJS-модули (`backend/src/modules/`, регистрируются в `backend/src/app.module.ts`)

27 предметных модулей: `audit`, `auth`, `bazis`, `bazis-cut`, `client-phones`, `cnc-telegram`, `crm-sync`, `cut`, `deadlines`, `doweling`, `export-templates`, `groups`, `health`, `labels`, `notifications`, `notifications-engine`, `order-realtime`, `orders`, `org`, `payments`, `production-actions`, `profile`, `projects`, `sheet-materials`, `status-automation`, `users`, `vlm`.

Сквозная инфраструктура:
- `backend/src/database/` — `DatabaseService` (pg Pool, `query()`, `transaction()`, таймауты, telemetry), `database.types.ts` (`DatabaseClient`, `TransactionClient`).
- `backend/src/permissions/` — RBAC (см. §3), включая политики доступа к orders/payments/users.
- `backend/src/common/` — `errors/api-error.filter.ts`, `logging/redaction.ts` (+ redaction coverage test), `audit/audit.service.ts`, `request-id/`, `request-context/`.
- `backend/src/rate-limit/`, `backend/src/performance/`, `backend/src/config/` (`api-prefix.ts`, `cors.ts`, `env.validation.ts`, `swagger.ts`).

Каркас приложения: `backend/src/main.ts` — Express-платформа, global prefix `/api/v1` (`api-prefix.ts`, исключения для `health/live`, `health/ready`), Swagger, CORS, `ApiErrorFilter`, feature-флаги (`BACKEND_ENABLE_*`) на модули/лимиты body parser. Пример интеграционного callback-маршрута: `main.ts` монтирует `${API_PREFIX}/integrations/bitrix24` с отдельным лимитом 256 kb.

## 3. Auth и RBAC

- Аутентификация (backend): JWT bearer — `backend/src/modules/auth/http/access-token.middleware.ts`; refresh-сессии с rotation/reuse-detection (`auth_sessions` в миграции `001_backend_stage1_additive.sql`: `token_family_id`, статусы `active|revoked|expired|reuse_detected`); bcrypt (`adapters/bcrypt-password-verifier.ts`); аудит входов (`adapters/pg-auth-audit-repository.ts`).
- SSO: `backend/src/modules/auth/workos/` (WorkOS: `workos-api.client.ts`, `workos-auth.controller.ts`, множественные идентичности через `pg-user-identity-repository.ts`).
- RBAC: `@RequirePermissions('perm.name')` + `PermissionsGuard` (`backend/src/permissions/permissions.guard.ts`) + `PermissionsService`; в OpenAPI — расширение `x-permission(s)`; роли/иерархия и матрица: `permissions/policies/role-policies.ts`, `order-access.policy.ts`, `payment-access.policy.ts`, `user-access.policy.ts`, `visibility/order-visibility-filter.ts`. Legacy-роли продублированы в `api/_lib/roles.ts` (admin/superadmin/manager/operator/top_manager/worker/viewer).
- Защита секретов: шифрование OAuth-токенов Bitrix24 (`BITRIX24_APP_TOKEN_ENCRYPTION_KEY` в `env.example.md`), `common/logging/redaction.ts`, `api/_lib/rate-limit.ts`, `refresh-cookie.ts` (httpOnly).

## 4. База данных и миграции

- PostgreSQL, доступ через сырой `pg` Pool (`backend/src/database/database.service.ts`): `DATABASE_URL`, pool min/max, `DATABASE_QUERY_TIMEOUT_MS`, опциональный SSL; транзакции `database.transaction(handler)`.
- Параллельно существует Hasura (GraphQL) поверх той же БД — для frontend/legacy `api/` (`api/_lib/db.ts`: `hasuraAdminQuery` с admin secret; `ops/hasura/` — метаданные). Схема БД legacy: `users`, `roles`, `orders`, `clients`, …; в `orders`/`clients` уже есть колонки `ref_key_1c` (в `orders_view` миграции 004 выставляются как `order_ref_key_1c`, `client_ref_key_1c`) — ссылки на объекты 1С в схеме предусмотрены.
- Миграции: `backend/db/migrations/NNN_name.sql`, следующий свободный номер — **146**. В backend **встроенного раннера нет**: `ops/apply-migrations.sh` — ordered ledger-раннер (`schema_migrations` + checksum drift; режимы `dry-run|status|apply|baseline|mark-applied|auto|probe`; `auto` умеет доводку восстановленного prod-dump). Миграции тестируются парными `.test.ts` (vitest).

## 5. Существующие outbox/идемпотентность/lease-паттерны (переиспользуемы для Agent API)

| Паттерн | Где | Суть |
|---|---|---|
| Outbox-таблица | `backend/db/migrations/002_deadline_engine.sql` | `public.outbox_events`: `event_type`, `aggregate_type`, `aggregate_id`, `payload_json`, статусы `pending|processing|processed|failed`, `attempts`, `next_attempt_at`, `locked_at`, `locked_by` |
| Outbox idempotency | `backend/db/migrations/004_production_actions_audit_outbox.sql` | `outbox_events.idempotency_key` + unique index |
| Command idempotency | там же | `command_idempotency_keys`: PK `idempotency_key`, `command_name`, `request_hash`, `response_json`, статусы `processing|completed|failed` — готовый образец «одна команда — один эффект» |
| Outbox enqueue | `backend/src/modules/deadlines/ports/outbox.port.ts` + `adapters/pg-outbox-port.ts` | простой insert события |
| Outbox relay (claim/lock/retry) | `backend/src/modules/notifications-engine/`: `ports/outbox-repository.port.ts`, `adapters/pg-outbox-repository.ts` (`FOR UPDATE SKIP LOCKED`, `markProcessed`, `markRetry` c `maxAttempts`→`failed`), `application/outbox-relay.service.ts` (транзакция на событие, экспоненциальный backoff, cap 3600 c), `application/outbox-relay-scheduler.service.ts`, `http/outbox-relay.controller.ts` (`POST outbox-relay/process-now`, `process-scheduled` при `relayOwner=external`) |
| Lease/writer lock | `backend/src/modules/crm-sync/adapters/pg-crm-sync-outbox-repository.ts` | `crm_sync_writer_lock`: `acquireWriterLock(leaseMs)` (ON CONFLICT + истечение lease), `heartbeatWriterLock` — готовый образец продления lease |
| Third-party integration | `backend/src/modules/crm-sync/` + `main.ts` Bitrix24 callback | приём внешнего события → валидация токена → durable enqueue (аналог «принять и не потерять») |

## 6. Тестовая инфраструктура

- Unit: vitest (`vitest.config.ts`, node-окружение; тестовые JWT-секреты задаются в `env` конфига).
- Интеграционные: отдельные конфиги `vitest.{deadline,notification-engine,cut,workos-multilink,production-actions,migration-auto-integration,sheet-materials,workos...}` — include `*.integration.ts` рядом с адаптерами (реальная PostgreSQL, судя по `*.integration.ts` в `adapters/` и `ops/migration-auto.integration.ts`).
- Миграции: `.test.ts` при каждом SQL (например `004_production_actions_audit_outbox.test.ts`).
- E2E/stage-canary: Playwright (`playwright.config.ts`, `playwright.frontend-ci.config.ts`, `tests/*.spec.ts` c флагами `*_STAGE_CANARY` и `PLAYWRIGHT_SKIP_WEB_SERVER` — проверка по реальным env `backend-test`).
- Smoke/ops: `scripts/smoke-*.js`, `scripts/stage-cutover-smoke.js`, `ops/smoke-vps.sh`, `ops/run-vps-tests.sh`, `ops/apply-migrations.test.ts`.
- CI: `.github/workflows/frontend-quality.yml` (frontend; отдельного backend-workflow в архиве не видно).

## 7. Маршруты Agent API — подтверждено отсутствие (по содержимому кода)

Полнотекстовый поиск по всем 3192 файлам архива (двоичный, по байтовым маркерам):

| Маркер | Совпадений |
|---|---|
| `1c-agents` | 0 |
| `session/start` | 0 |
| `commands/lease` | 0 |
| `commands/execute` | 0 |
| `etl/batches` | 0 |
| `etl/runs` | 0 |
| `lease/renew` | 0 |
| `X-Agent-Id` | 0 |
| `agent_id` | 0 |

Упоминания `heartbeat` (2 файла) — **не** Agent API: это `GET /api/v1/cnc-telegram/worker-logs/health` (heartbeat Telegram-worker) в `backend/contracts/04-api-contract.openapi.yaml` и python-клиент `cnc-telegram-worker/cnc_telegram_worker/erp_client.py`. Упоминания `1С` (54 файла) — это колонки `ref_key_1c`/свойства `refKey1c` (orders, clients, client-phones, sheet-materials, order-snapshots) и base64-совпадения в `package-lock.json`; кода интеграции с агентом/1С там нет. В `backend/contracts/04-api-contract.openapi.yaml` тегов/путей Agent API нет (есть Auth, Orders, Payments, Production Actions, Groups, Deadlines, Notifications, Users, VLM, Labels, Health, Bitrix24). Вывод аудита 23.09.2026 подтверждён: **модуль `/api/integration/1c-agents/v1` в снапшоте ERP отсутствует**.

## 8. Архитектурный план интеграции для этапа 7 (предложение, не согласование)

Размещение: новый модуль NestJS в `backend/` (а не Vercel `api/` и не Hasura) — консистентно с ролью backend как «источника истины для транзакций и прав» (декларация в `04-api-contract.openapi.yaml`). Базовый URL агента по ТЗ §17 — `/api/integration/1c-agents/v1/*`; в backend сейчас global prefix `/api/v1`. Локально предлагается смонтировать контроллер агента по маршруту из ТЗ (через исключение из global prefix, как для health) и считать это решение временным до подтверждения владельцем (внешний вопрос, см. §9).

Минимальный набор файлов к реализации (следуя ports/adapters и нумерации миграций):

1. `backend/db/migrations/146_onec_agent_api.sql` + `146_onec_agent_api.test.ts`:
   - `onec_agent_sessions` (sessionId, agentId, siteId, версии, capabilities, serverTimeUtc, maintenanceMode);
   - `onec_commands` (command_id PK, command_type, payload_version, payload_hash, payload_json, priority, ordering_key, correlation_id, expires/not_before, status-машина, attempt_count, next_attempt_at) — модель «принять или отклонить» атомарно;
   - `onec_command_results` (результат для идемпотентной доставки; `result_status` для dead_letter — урок A09), `onec_command_attempts` (POST и status lookup раздельно);
   - `onec_etl_runs`, `onec_etl_batches` (все поля FR-ETL-008), `onec_etl_watermarks` (committed/extracting курсоры — урок A05);
   - переиспользовать `command_idempotency_keys` и `outbox_events` для событий ручного разбора/уведомлений, не плодить новые паттерны.
2. `backend/src/modules/onec-agent/` (по образцу `notifications-engine`):
   - `onec-agent.module.ts` (+ регистрация в `app.module.ts`, env-флаг `BACKEND_ENABLE_ONEC_AGENT`);
   - `http/onec-agent.controller.ts` — `session/start`, `commands/lease`, `commands/{id}/received`, `commands/{id}/lease/renew`, `PUT commands/{id}/result`, `heartbeat`, `etl/batches`, `etl/runs/{id}/complete`, `configuration` (все — под mTLS-идентичностью агента, см. §9);
   - `application/onec-command.service.ts` (атомарный допуск/дедуп/конфликт/expiry/ordering), `onec-result.service.ts` (идемпотентный PUT результата), `onec-etl.service.ts` (ACK batch по id+status+checksum+rows, complete run идемпотентно);
   - `ports/*.ts` + `adapters/pg-onec-*.ts` на `DatabaseService`;
   - аренда: `onec-agent-lease.repository.ts` по образцу `crm_sync_writer_lock` (acquire + heartbeat + expiry);
   - аутентификация агента: guard/middleware «сертификат ↔ агентский идентификатор» (место для решения о mTLS-termination).
3. Контракт: смержить локальный канон `repo_1c-agent/contracts/erp-agent-api.openapi.yaml` с `backend/contracts/` (отдельный файл `05-onec-agent.openapi.yaml` или новый тег в `04-…`) — требует решения владельца о версионировании.
4. Тесты: unit `.test.ts` рядом с файлами; интеграционные `*.integration.ts` + `vitest.onec-agent-integration.config.ts` (реальная временная PostgreSQL, по образцу `vitest.notification-engine-integration.config.ts`); сценарии kill/restart и contract tests по OpenAPI — этап 7 по плану.
5. Бизнес-связка `create_customer_order` → модуль `orders` (в схеме уже есть `orders.ref_key_1c`/`clients.ref_key_1c` для возврата Ref_Key/номера). ETL mappings к отчётам — отдельное незакрытое соглашение (владелец, §9).

## 9. Нерешённые вопросы к владельцам и тестовому стенду

1. Владелец и репозиторий поставки ERP Agent API: этот архив — канонический backend (отдельный репо с CI) или целевое внедрение в производственный деплой? Кто принимает code review/security review (план §11 требует «согласованный репозиторий ERP»).
2. Маршрутизация: сохранить базу агента `/api/integration/1c-agents/v1` (ТЗ §17) или вложить в `/api/v1/...` (текущий global prefix backend)? Ответ фиксирует контракт `erp-agent-api.openapi.yaml` и `ErpClient` BaseUrl.
3. mTLS: где терминируется клиентский сертификат агента (reverse proxy? Kestrel/Express?) и как сопоставлять сертификат с `agentId`/`X-Agent-Id` (в ТЗ §21 mTLS обязателен; в архиве механизмов mTLS-маппинга нет — см. §7).
4. RBAC-модель агента: машинная роль/permission set (агент не является пользователем `users`?) — как вписывается в `PermissionsService`/`RequirePermissions`.
5. Тестовая среда: доступ к PostgreSQL (`DATABASE_URL` для `backend-test`), Hasura-инстанс, способ применения миграций (`ops/apply-migrations.sh baseline/auto` для существующего `erp_test`), учётные данные тестовой ERP.
6. Семантики из [stage0-baseline.md §5.3–5.4](stage0-baseline.md): ACK получения, поведение при повторе результата, `409 command_payload_conflict`, отмена, принятие административного ETL-задания, потеря/продление lease (в backend есть образец writer-lock lease — предложить его контракт владельцам), empty run/partial success.
7. `Edm.DateTime` без offset на стороне 1С, составные ключи и deletion marks — по образцам реальной конфигурации 1С (владелец 1С).
8. Первый документ `create_customer_order` (payload) и пилотный ETL-справочник/mappings — без этого этап 7 не стартует по плану §11.

## 10. Вывод

Backend для Agent API есть (NestJS 11, PostgreSQL, RBAC, outbox/идемпотентность/lease-образцы, миграционный ledger, тестовая культура), но **готового модуля Agent API нет** — подтверждено полнотекстовым поиском по архиву. Реализация возможна минимальным модулем поверх существующих паттернов (§8); внешние решения (пункты маршрутизации, mTLS, RBAC-роль агента, payload/mappings) перечислены в §9 и владельцами **не подтверждены**. Статус внешней интеграции: не начата.
