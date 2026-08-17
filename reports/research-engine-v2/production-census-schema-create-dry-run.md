# Production Census Schema Create — Dry-Run

**Status:** `production_census_schema_created_ready_for_census_write_approval`
**Dry-run pass:** true
**Base:** `appCCU…foLk` (platform_alt)
**Token:** `patEUo…14eb`
**ALLOW_PRODUCTION_CENSUS_SCHEMA_CREATE:** true
**Zero record writes:** true

## Tables to create


## Fields to create (0)

| Table | Field | Type |
| --- | --- | --- |

## Linked-table dependencies

```json
[
  {
    "from_table": "Hotel Property Brand Affiliations",
    "field": "Hotel Property Census",
    "to_table": "Hotel Property Census",
    "create_order": "after_census"
  },
  {
    "from_table": "Hotel Property Source Evidence",
    "field": "Hotel Property Census",
    "to_table": "Hotel Property Census",
    "create_order": "after_census"
  },
  {
    "from_table": "Hotel Property Steward Review",
    "field": "Hotel Property Census",
    "to_table": "Hotel Property Census",
    "create_order": "after_census"
  }
]
```

## Conflicts

_None_

## Preflight

```json
{
  "pat": {
    "ok": true
  },
  "mvp_base": {
    "ok": true,
    "masked": "appvtn…INP6"
  },
  "not_sandbox": {
    "ok": true
  },
  "schema_read": {
    "ok": true,
    "table_count": 22
  },
  "schema_write": {
    "ok": true,
    "status": 422,
    "detail": "schema write endpoint reachable (validation error on empty body expected)"
  }
}
```
