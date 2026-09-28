# Конфигурация

Bootstrap-настройки находятся в `appsettings.json`. Секрет 1С хранится отдельно в DPAPI blob под ACL service account.

- `Agent.AgentId`, `SiteId` — стабильные уникальные идентификаторы агента и площадки.
- `Agent.DataDirectory` — локальный NTFS-каталог.
- `Agent.HeartbeatIntervalSeconds`, `HealthCheckIntervalSeconds` — независимые интервалы телеметрии и проверки обоих endpoint 1С.
- `Erp.BaseUrl` — только HTTPS; базовый API добавляется автоматически.
- `Erp.ClientCertificateThumbprint` — thumbprint сертификата `LocalMachine\My`.
- `OneC.ODataBaseUrl`, `CommandApiBaseUrl` — локальные endpoints 1С.
- `Commands.SupportedTypes` — allowlist; неизвестный тип fail-closed уходит в dead letter.
- `Etl.Entities` — явные поля и стратегия каждой сущности; выгрузки `select *` нет.
  - `DeleteBatchAfterAck` (по умолчанию `false`) — для чувствительных сущностей (телефоны контрагентов): файл пакета удаляется сразу после ACK ERP, остаются только метаданные. Если такая сущность пропадёт из конфигурации ERP (отзыв), незавершённые run с ней блокируются `ENTITY_REVOKED`, а все оставшиеся файлы сущности удаляются. Флаг не входит в fingerprint домена: его смена не требует новой базовой выгрузки. Подробности: `docs/remediation/revocation-retention-e3b.md`.
- `Etl.SafetyLagSeconds` — отступ верхней границы окна от текущего времени; `RunOnStartup` управляет немедленным запуском после старта.
- `Etl.MaxConcurrentBatchUploads` — независимый предел параллельной доставки batch.
- `Storage.MaxSpoolBytes`, `MaxSqliteBytes`, `MaxBatchCompressedBytes` — backpressure и диагностические пределы локального хранения.

Пример ETL-сущности:

```json
{
  "entityCode": "counterparties",
  "odataPath": "Catalog_Контрагенты",
  "keyField": "Ref_Key",
  "updatedAtField": null,
  "deletedField": "DeletionMark",
  "select": [
    "Ref_Key",
    "DataVersion",
    "DeletionMark",
    "Code",
    "Description",
    "НаименованиеПолное",
    "ВидКонтрагента",
    "Покупатель",
    "Поставщик"
  ],
  "syncMode": "manual_only",
  "pageSize": 500,
  "overlapMinutes": 10,
  "schemaVersion": 1,
  "oDataVersion": 3,
  "enabled": true
}
```

Клиент автоматически включает `keyField`/`keyFields`, `updatedAtField` и `deletedField` в `$select`, поддерживает OData v3/v4, составные ключи, `Edm.DateTime`/`Edm.DateTimeOffset`, `@odata.nextLink`, `odata.nextLink`, `d.results`, `__next` и безопасный `$skip` fallback.

Для сущности без даты изменения чтение выполняется полным ограниченным страницами проходом с идемпотентным upsert в ERP. Сущности `manual_only` автоматически исключаются из почасового запуска и доступны через ручной полный импорт или `reload_entity`.

Для составного ключа сохраняется совместимый `keyField`, а полный ключ задаётся отдельно, например: `"keyFields": ["Ref_Key", "LineNumber"]`. Для временного поля OData v3 типа `Edm.DateTime` укажите `"updatedAtEdmType": "Edm.DateTime"`.

Фактические рекомендации для исследованной базы 1С приведены в [odata-dataset-mapping.md](odata-dataset-mapping.md).

### Учётные записи 1С

Службе нужны две учётные записи 1С. Каждая хранится в DPAPI под своим именем секрета:

| Настройка | Для чего | Роли 1С |
|---|---|---|
| `OneC:CredentialSecretName` (по умолчанию `onec-main`) | OData, `health`, `identity` | только чтение: `ERPIntegration_ODataRead` |
| `OneC:CommandCredentialSecretName` (например, `onec-command`) | `commands/execute` и `commands/{id}` | `ERPIntegration_CommandWrite` + `БазовыеПраваБСП` |

Если `CommandCredentialSecretName` не задан, команды идут под учётной записью OData, как до E5. При запуске служба тогда пишет предупреждение `ONEC_SINGLE_CREDENTIAL`. Эта учётная запись должна иметь право записи, поэтому для рабочей установки задайте отдельную.

Перед запуском:

```powershell
ErpOnecAgent.exe --store-onec-credential
ErpOnecAgent.exe --store-onec-credential --purpose command
ErpOnecAgent.exe --validate-config
ErpOnecAgent.exe --test-erp
ErpOnecAgent.exe --test-onec
```

### Сертификат агента для mTLS

ERP принимает самоподписанный клиентский сертификат агента: ERP регистрирует его отпечаток.
Сертификат создаётся на машине агента в PowerShell, запущенном от администратора:

```powershell
.\installer\powershell\new-agent-certificate.ps1 -AgentId <agentId> -OutputDirectory C:\temp\agent-cert -GrantServiceAccess
```

Что делает скрипт:
- создаёт ключ RSA 3072 без права экспорта в `LocalMachine\My`, назначение — проверка
  подлинности клиента, срок — 2 года;
- с `-GrantServiceAccess` даёт виртуальной учётной записи службы `NT SERVICE\ErpOnecAgent`
  право чтения закрытого ключа;
- пишет **только публичный** сертификат `agent-<agentId>.pem` и выводит отпечатки.

Дальше:
- отправьте в ERP файл `.pem`, а также `agentId` и `siteId`;
- в `Erp:ClientCertificateThumbprint` укажите выведенный отпечаток SHA-1.

Закрытый ключ машину не покидает.
