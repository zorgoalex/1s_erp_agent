# Troubleshooting

- `certificate not found` — проверить StoreLocation `LocalMachine`, thumbprint без невидимых символов, private key и ACL service account.
- `401/403 ERP` — проверить привязку сертификата к `agent_id`, цепочку CA, EKU и отзыв; не отключать проверку TLS.
- `1C credential is not configured` — выполнить интерактивный `--store-onec-credential` из elevated консоли.
- повторяющийся `unknown_result` — проверить обязательный журнал идемпотентности в 1С и endpoint `GET commands/{commandId}`; ручной повтор с новым ID запрещён.
- растёт spool — проверить ERP batch ACK, checksum и свободное место. Неподтверждённые файлы вручную не удалять.
- `SQLITE_INTEGRITY_FAILED` — остановить службу, сохранить evidence, проверить backup и следовать runbook восстановления.
- `.tmp` в quarantine после crash — это незавершённые файлы; они не отправляются ERP.

