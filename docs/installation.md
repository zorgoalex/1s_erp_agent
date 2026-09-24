# Установка

## Предварительные условия

- поддерживаемый Windows Server x64 или Windows 11 Pro/Enterprise x64;
- сертификат mTLS с private key в `LocalMachine\My`, выданный доверенным корпоративным CA;
- исходящий TCP 443 к ERP и доступ к локальным OData/HTTP endpoints 1С;
- отдельные малопривилегированные учётные данные 1С;
- локальный NTFS-каталог, не SMB/OneDrive/Dropbox/временный диск.

## Сборка

```powershell
.\installer\powershell\build-release.ps1
```

Без `-SigningCertificateThumbprint` результат помечается как unsigned и не считается production-релизом. Подпись выполняется только в owner-controlled окружении с доступом к code-signing key.

## Установка PowerShell

1. Скопируйте и заполните `appsettings.json`, не помещая в него пароль 1С.
2. Запустите elevated PowerShell:

```powershell
.\installer\powershell\install-agent.ps1 `
  -PublishDirectory .\artifacts\publish\win-x64 `
  -ConfigPath .\src\ErpOnecAgent.Service\appsettings.json
```

Скрипт регистрирует delayed-auto service `ErpOnecAgent` под virtual service account, применяет ACL, миграции и интерактивно сохраняет пароль через DPAPI. Пароль не передаётся аргументом командной строки.

Для последующей безопасной смены учётных данных остановите службу, выполните:

```powershell
.\installer\powershell\set-onec-credentials.ps1
```

и снова запустите службу. Скрипт принимает пароль только интерактивно и не помещает его в командную строку.

## Сертификат

Импорт и ACL private key выполняет администратор по политике PKI. В конфиг записывается только thumbprint. Проверка:

```powershell
Get-ChildItem Cert:\LocalMachine\My\THUMBPRINT
```

## Обновление

Переведите агент в `Drain`, дождитесь пустой executing-очереди, остановите службу, создайте backup, выполните repair новой публикацией и проверьте `--test-erp`, `--test-onec`, heartbeat. ProgramData и SQLite не заменяются.

## Удаление

```powershell
.\installer\powershell\uninstall-agent.ps1
```

Данные сохраняются. Их необратимое удаление требует одновременно `-RemoveData -ConfirmDataRemoval`.
