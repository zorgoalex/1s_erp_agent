# Эксплуатация и восстановление

Поддерживаемые команды CLI:

```text
--console
--validate-config
--test-erp
--test-onec
--migrate
--integrity-check
--collect-diagnostics
--store-onec-credential
--version
```

При старте агент применяет checksum-verified миграции, запускает `PRAGMA integrity_check`, переводит оборванные `executing` в `unknown_result`, возвращает `sending/uploading` в durable очереди и помещает `.tmp` spool в quarantine.

При недоступности ERP результаты и ETL batches остаются на диске до ACK. При недоступности 1С команды остаются в очереди; POST не повторяется вслепую — сначала выполняется `GET status(commandId)`.

Ежесуточное обслуживание использует SQLite backup API, проверяет integrity, хранит ограниченное число backup, удаляет только ACK-данные старше retention. Dead letter автоматически не удаляется.

Диагностический ZIP не включает payload или секреты и содержит версию, обезличенную конфигурацию, integrity и queue summary:

```powershell
.\installer\powershell\collect-diagnostics.ps1
```

Восстановление:

1. Остановить службу.
2. Сохранить повреждённую БД и spool для расследования.
3. Проверить backup отдельной копией через `--integrity-check`.
4. Восстановить `agent.db` только при остановленной службе; не копировать открытые WAL/SHM.
5. Запустить службу и подтвердить повторную доставку result/batch без дублей.

