# Production Census Schema Validation

**Status:** `production_census_schema_validation_pass`
**Base:** `appCCU…foLk`

## Checks

- **schema_read:** PASS
- **four_tables_exist:** PASS
- **required_fields_exist:** PASS
- **field_types_compatible:** PASS
- **zero_records:** PASS
- **legacy_stub_present_unchanged_presence:** PASS
- **brand_explorer_tables_present:** PASS
- **frozen_vic_untouched:** PASS
- **frozen_62_untouched:** PASS

```json
[
  {
    "id": "schema_read",
    "pass": true
  },
  {
    "id": "four_tables_exist",
    "pass": true,
    "missing": []
  },
  {
    "id": "required_fields_exist",
    "pass": true,
    "missing": []
  },
  {
    "id": "field_types_compatible",
    "pass": true,
    "mismatches": []
  },
  {
    "id": "zero_records",
    "pass": true,
    "issues": []
  },
  {
    "id": "legacy_stub_present_unchanged_presence",
    "pass": true,
    "tables": [
      {
        "name": "Hotel Census",
        "exists": true,
        "field_count": 105,
        "note": "Not modified by schema create (presence-only check)"
      },
      {
        "name": "Verified Independent Hotel Census",
        "exists": true,
        "field_count": 18,
        "note": "Not modified by schema create (presence-only check)"
      },
      {
        "name": "Independent Hotel Source Candidates",
        "exists": true,
        "field_count": 24,
        "note": "Not modified by schema create (presence-only check)"
      },
      {
        "name": "Independent Hotel Source Evidence",
        "exists": true,
        "field_count": 10,
        "note": "Not modified by schema create (presence-only check)"
      }
    ]
  },
  {
    "id": "brand_explorer_tables_present",
    "pass": true,
    "detail": {
      "Brand Setup - Brand Basics": {
        "exists": true,
        "id": "tbl1x6S7I7JwTcRdV",
        "field_count": 81
      },
      "Brand Setup - Brand Explorer Presentation": {
        "exists": true,
        "id": "tblZyZF4JsRkNeCbU",
        "field_count": 28
      }
    },
    "note": "Schema create does not modify these tables"
  },
  {
    "id": "frozen_vic_untouched",
    "pass": true,
    "vic": {
      "file_count": 29,
      "aggregate_sha256": "013a62f3f739cef81412467ecee3b56e5440be0e8c81d6b55ae9675705270a1e"
    },
    "expected_freeze": "c1cb244a95d7311b4ab2cf31d4988685879ef492f4f6420710633267d0effda3"
  },
  {
    "id": "frozen_62_untouched",
    "pass": true,
    "artifacts": [
      {
        "path": "reports/brand-explorer-62-active-public-full-baseline.json",
        "exists": true,
        "size": 189579,
        "mtime_ms": 1785894190880.195,
        "sha256": "87e2870b719327167d39564b45039227e37b1d3be518705da290c3469e50f8b7"
      },
      {
        "path": "reports/brand-explorer-62-active-public-full-baseline.md",
        "exists": true,
        "size": 29101,
        "mtime_ms": 1785894190884.1685,
        "sha256": "cb05db4571e291b8ea332f82d081d6265290396538536cd8c4ae0f7765a2e56f"
      },
      {
        "path": "docs/data-intelligence/brand-explorer-62-active-public-full-baseline.md",
        "exists": true,
        "size": 29101,
        "mtime_ms": 1785894190885.1675,
        "sha256": "cb05db4571e291b8ea332f82d081d6265290396538536cd8c4ae0f7765a2e56f"
      },
      {
        "path": "lib/partner-intelligence/brand-explorer-62-active-public-full-baseline.js",
        "exists": true,
        "size": 76327,
        "mtime_ms": 1785915102595.374,
        "sha256": "7bc5f0d6cca57b7aad0f141f8b56d3528e6fbb634327ce3945b182360966ac7c"
      }
    ]
  }
]
```
