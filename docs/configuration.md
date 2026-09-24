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

Перед запуском:

```powershell
ErpOnecAgent.exe --store-onec-credential
ErpOnecAgent.exe --validate-config
ErpOnecAgent.exe --test-erp
ErpOnecAgent.exe --test-onec
```
