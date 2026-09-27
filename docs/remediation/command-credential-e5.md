# E5 — separate 1C credential for commands

Date: 2026-09-28. No migration.

## Why

Before E5, one 1C credential served both OData reads and business commands. The OData
service user is meant to be read-only (`ERPIntegration_ODataRead`), and business commands
need `ERPIntegration_CommandWrite` + `БазовыеПраваБСП`. With a single credential, either
commands fail or the reading user gets write rights.

## Change

- **`OneC:CommandCredentialSecretName`** (optional). When set:
  - `commands/execute` and `commands/{id}` run under it;
  - OData, `health` and `identity` keep `OneC:CredentialSecretName`.
- **When it is not set,** commands fall back to the single credential, as before E5. The
  service logs a warning `ONEC_SINGLE_CREDENTIAL` at start.
- **Checks.** The service start and `--validate-config` require both secrets. A missing
  command secret stops the start with its name.
- **CLI:** `--store-onec-credential --purpose read|command`. `command` requires
  `CommandCredentialSecretName` to be configured.
- **Installer:** `set-onec-credentials.ps1 -Purpose read|command`.
- **Diagnostics** report `separateCommandCredential`.
- **Docs:** `docs/configuration.md` has a section on the two credentials.

## Tests

- `OnecCredentialSplitE5Tests` (unit, 4):
  - read and command credentials are applied to their own calls;
  - commands fall back to the single credential when no command secret is set;
  - a missing command secret fails the call and names the secret;
  - `OnecCommandClient` sends both execute and status under the command credential.
- `A07ModeStateTests.E5_*`: bootstrap refuses to start without the configured command
  credential.
