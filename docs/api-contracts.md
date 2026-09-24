# API contracts

Канонический контракт ERP: [`contracts/erp-agent-api.openapi.yaml`](../contracts/erp-agent-api.openapi.yaml).

ERP обязана атомарно создавать integration command вместе с бизнес-транзакцией, выдавать lease, идемпотентно принимать `PUT result`, проверять `batchId + checksum` и выполнять upsert ETL по source key. mTLS middleware должна сопоставлять сертификат с `X-Agent-Id` до controller.

В snapshot ERP из ТЗ используется NestJS 11 и PostgreSQL через `DatabaseService`; готового модуля `/api/integration/1c-agents/v1` в архиве не обнаружено. Поэтому этот репозиторий поставляет точный OpenAPI и runnable mock, но не изменяет архив ERP. Production-модуль ERP и его PostgreSQL outbox/inbox migration внедряются в отдельном репозитории ERP с его CI и security review.

Mock запускается на loopback:

```powershell
.\.dotnet\dotnet.exe run --project .\tools\ErpOnecAgent.MockServer
```

Mock не реализует mTLS и запрещён для production.

