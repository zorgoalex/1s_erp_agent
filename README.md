# ErpOnecAgent

Локальная Windows-служба на C#/.NET 10 для надёжного двустороннего обмена ERP ↔ 1С без входящих соединений из интернета.

Реализовано:

- HTTPS long polling ERP → агент и mTLS-аутентификация сертификатом `LocalMachine\My`;
- durable inbox/result outbox на SQLite WAL с миграциями, backup API и crash recovery;
- дедупликация `commandId + payloadHash`, ordering key, expiry, retry и `unknown_result` status-check;
- вызов версионированного HTTP-сервиса 1С с обязательной сквозной идемпотентностью;
- полный и инкрементальный OData ETL, overlap, bounded batches, UTF-8 NDJSON + GZip и атомарный spool;
- watermark только после ACK всех batch и ACK завершения run;
- heartbeat, certificate/disk/queue metrics, JSON-логи и обезличенный diagnostics ZIP;
- DPAPI-хранилище учётных данных 1С, Windows Service scripts, WiX source, SBOM/manifest/signing pipeline;
- контрактный mock ERP + 1С и unit/integration tests.

Быстрые команды:

```powershell
.\.dotnet\dotnet.exe restore .\ErpOnecAgent.sln --locked-mode
.\.dotnet\dotnet.exe build .\ErpOnecAgent.sln -c Release --no-restore
.\.dotnet\dotnet.exe test .\ErpOnecAgent.sln -c Release --no-build --no-restore
```

Документация: [установка](docs/installation.md), [конфигурация](docs/configuration.md), [эксплуатация](docs/operations.md), [контракты](docs/api-contracts.md), [доработка 1С](docs/1c-contracts.md), [сравнение со starter-pack](docs/starter-pack-gap-analysis.md), [локальная приёмка](docs/acceptance-report.md), [границы готовности](docs/production-readiness.md).

Важно: конкретные payload handlers документов и ETL mappings нельзя зафиксировать без параметров раздела 39 ТЗ. Транспортное ядро работает с версионированными envelope и передаёт payload без бизнес-преобразований; площадочные обработчики 1С должны быть реализованы и приняты для фактической конфигурации 1С.
