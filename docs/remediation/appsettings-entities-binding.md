# Defect: Etl:Entities from appsettings.json were silently dropped

Date: 2026-09-28. Found by the first read-only E2E (`e2e-20260928-02`, `pass1`). No migration.

## Symptom

`start_full_sync` ended with `business_error NO_ENTITIES` although `appsettings.json` listed two
entities. The agent had no local entities at all. Only entities from ERP's remote
configuration ever worked, because that path deserializes with System.Text.Json.

## Cause

`EtlEntityDefinition` is a positional record. The configuration binder builds it through its
constructor, and it **drops the array element without an error** when any constructor
parameter without a default is missing or `null` in the configuration. Examples:
- `"updatedAtField": null` — the documented shape of every catalog;
- `"deletedField": null`.

Before E2E, no test bound `Etl:Entities` from JSON. Every test built `EtlOptions` in code.

## Fix

- `EtlEntityConfiguration.Read` (in `ErpOnecAgent.Service/Runtime`) binds each element into a
  mutable settings class whose every property may be null, then builds the record.
  `Program.cs` sets `EtlOptions.Entities` from it in `PostConfigure`, before validation.
- **An incomplete element is no longer dropped.** It is kept with empty required values, so
  the startup validation (`One or more ETL entities are invalid.`) stops the service.
- **Defaults** of the absent optional values match the record's: schema 1, OData v3, enabled,
  `Edm.DateTimeOffset`, `EtlOptions` page size and overlap. `SyncMode` defaults to
  `incremental`.

## Tests

`EtlEntityConfigurationBindingTests` (5), all binding from JSON through the production path:
- a catalog with `updatedAtField: null`;
- a catalog without the key;
- a dated entity with `Edm.DateTime` and `deletedField: null`;
- an incomplete entity is kept;
- the E2E shape with PascalCase keys and two catalogs.
