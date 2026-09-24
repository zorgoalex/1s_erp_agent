# Отчёт локальной приёмки — обновлено 2026-08-30

## Проверено

| Gate | Результат |
|---|---|
| `dotnet format --verify-no-changes` | PASS |
| Release build, warnings/errors | PASS, `0/0` |
| Unit tests | PASS, `10/10` |
| SQLite/integration tests | PASS, `5/5` |
| NuGet vulnerability audit, including transitive dependencies | PASS, известных уязвимых пакетов не найдено |
| PowerShell parser | PASS, `8/8` scripts |
| Self-contained `win-x64` publish | PASS, версия `1.0.0` |
| SHA-256 release manifest | PASS, `261/261` файлов |
| CycloneDX SBOM | PASS, spec `1.6`, `72` components |

## Контрактный E2E

На локальном стенде выполнен полный маршрут:

`mock ERP → lease/ACK → durable SQLite inbox → агент → mock 1С → result outbox → mock ERP`.

Команда `7baf95f7-75eb-48ca-beaa-2b97403c22c3` типа `create_customer_order` завершилась статусом `succeeded`; ERP получил тип, ссылку, номер, дату и признак проведения документа.

После интеграции starter-pack маршрут повторно проверен командой `1ab621a2-c93e-444b-9c5a-5f255529fc32`: статус `succeeded`, документ `CustomerOrder`, результат версии `1` доставлен ERP. Раздельные health-check OData и command API также вернули `true`.

Дополнительно E2E выявил и подтвердил исправление загрузки `appsettings.json` относительно каталога executable. Это критично для запуска Windows Service из рабочего каталога `System32`. Пустой ответ mock lease также ограничен защитной задержкой, исключающей busy-poll.

## Что этот отчёт не подтверждает

- Артефакты локальной сборки не подписаны owner-controlled code-signing certificate.
- Production MSI не собран и не подписан; для MVP проверен PowerShell installer, WiX authoring оставлен как production-заготовка.
- Не выполнялся запуск на реальной тестовой базе 1С: конфигурация, payload handlers и OData mappings не предоставлены.
- Не выполнялся запуск против production ERP: в предоставленном ERP-архиве отсутствует реализация Agent API, к которой приложен OpenAPI-контракт.
- Не выполнены site acceptance, нагрузочное испытание и семидневный soak.

До закрытия этих пунктов решение является проверенным транспортным ядром/MVP, но не подписанным production-релизом для конкретной площадки.
