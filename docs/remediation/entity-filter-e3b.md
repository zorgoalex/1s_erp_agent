# E3b — static entity filter

Date: 2026-09-28. No migration. For `counterparty_phones` (agent-bridge `to-onec/0021`,
`to-erp/0025`; user decision: all phones, only rows with `Тип = Телефон`).

## Change

- **`EtlEntityDefinition.Filter`** (optional, remote `filter`) holds an OData `$filter`
  expression over the entity set. It applies to:
  - the data read: `(filter) and <date bounds>`, with the filter parenthesised so an `or`
    inside cannot escape the bounds;
  - `$count?$filter=filter`;
  - the V1 key pass.

  This way `completeness: verified` refers to the filtered set.
- **The filter is part of the definition,** so it is part of the domain fingerprint: changing
  it is a domain change.
- **A null filter is not serialized.** Every existing definition JSON, and every existing
  domain fingerprint, stays byte-identical. Otherwise the upgrade would have reported
  `DOMAIN_CHANGED` for every entity.
- **Validation:** 1..512 characters, not blank, no control characters. The filter is sent
  URL-escaped, so it cannot add query options. Remote and local (`appsettings`) definitions
  bind it.
- Verified on the test base: `Catalog_Контрагенты_КонтактнаяИнформация/$count?$filter=Тип eq 'Телефон'`
  → 76 of 78 rows.
- OpenAPI 1.4.0.

## Tests

- **`EtlEntityFilterTests`** (unit, 4):
  - a definition without a filter serializes exactly as before;
  - the filter round-trips and changes the fingerprint;
  - the filter applies to the data read, `$count` and the key pass;
  - date bounds sit next to the parenthesised filter.
- **`RemoteEntityNamingTests`:** the filter validation (accepted, null, empty, blank,
  control character, too long).
