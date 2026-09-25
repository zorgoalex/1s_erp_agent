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
--etl-resolve-run <runId> --decision abandon|retry|rebaseline --operator <id> --verification "<text>" --workers-stopped
--etl-reset-domain <entity> --generation <N> --operator <id> --reason "<text>"
```

Команды ETL работают только при остановленной службе; порядок действий — в `docs/etl-runbook.md`.

При старте агент по порядку:
- проверяет, что база не потеряна;
- применяет checksum-verified миграции;
- запускает `PRAGMA integrity_check`;
- переводит оборванные `executing` в `unknown_result`;
- возвращает `sending` результатов в durable очередь.

Для ETL при старте:
- незавершённая попытка отправки пакета помечается `orphaned`. Пакет уходит в карантин, run блокируется, повторной отправки нет;
- прерванное извлечение блокирует run с кодом `INTERRUPTED_NO_CHECKPOINT`;
- run старого конвейера блокируются с кодом `LEGACY_UNRESOLVED`;
- `.tmp` и готовые файлы spool без записи в базе помещаются в quarantine;
- неотправленный пакет, файл которого пропал из spool, выводится из работы, а его run блокируется с кодом `SPOOL_FILE_MISSING`.

Если запись исхода отправки в базу не удалась, в журнале появляется `ETL_BATCH_OUTCOME_UNRECORDED` (Critical). Run стоит до перезапуска службы; после перезапуска пакет уходит в карантин без повторной отправки.

Заблокированные run разбираются вручную (`docs/etl-runbook.md`).

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
5. Запустить службу и подтвердить повторную доставку результатов без дублей. ETL-пакеты с неизвестным исходом отправки не отправляются повторно автоматически: их run блокируются и разбираются по `docs/etl-runbook.md`.

