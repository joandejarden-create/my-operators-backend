# Production Census Schema v1.1.3 — Address & Coordinate Provenance Fields

**Status:** `production_census_schema_v113_coordinate_provenance_added_ready_for_provider_decision`  
**Mode:** apply  
**Generated:** 2026-08-05T15:21:46.259Z  
**Apply executed:** true

## 1. Executive summary

- Fields created: **7**
- Fields already existing: **0**
- Field count: **101 → 108**
- Census records: **666**
- Validation pass: **true**
- Provenance populated: **no (expected)**

## 2. Fields created

```json
[
  {
    "name": "Address Confidence",
    "type": "singleSelect",
    "id": "fldSx8vdU9jS4ZKA4",
    "options": [
      "High",
      "Medium",
      "Low",
      "Hold"
    ]
  },
  {
    "name": "Address Source URL",
    "type": "url",
    "id": "fldJQrjMEUdlzTi8O",
    "options": null
  },
  {
    "name": "Coordinate Source Type",
    "type": "singleSelect",
    "id": "fld4wWVNu9nkMhX1V",
    "options": [
      "official_coordinates",
      "official_address_geocode",
      "existing_source",
      "structured_data_extraction",
      "embedded_map_extraction",
      "blocked_low_confidence",
      "blocked_no_official_address",
      "steward_review"
    ]
  },
  {
    "name": "Coordinate Confidence",
    "type": "singleSelect",
    "id": "fldNPRyVrUOw9CSIF",
    "options": [
      "High",
      "Medium",
      "Low",
      "Hold"
    ]
  },
  {
    "name": "Geocode Provider",
    "type": "singleSelect",
    "id": "fldAUUeHqAMemDUu6",
    "options": [
      "Mapbox",
      "Google",
      "Official Page",
      "Existing Source",
      "Manual Review",
      "None"
    ]
  },
  {
    "name": "Geocode Method",
    "type": "singleSelect",
    "id": "fldsruuAw0aG1z4AC",
    "options": [
      "official_coordinates",
      "official_address_geocode",
      "structured_data_extraction",
      "embedded_map_extraction",
      "manual_review",
      "none"
    ]
  },
  {
    "name": "Geocode Reviewed Date",
    "type": "date",
    "id": "fldhdB4jEAVG7nq6X",
    "options": null
  }
]
```

## 3. Fields already existing

```json
[]
```

## 4. Final Census field count

- Before: 101
- After: 108
- Expected if all 7 created from 101: 108

## 5. Census validation

```json
{
  "record_count": 666,
  "field_count": 108,
  "expected_field_count": 108,
  "duplicate_identity_keys": 0,
  "duplicate_field_names": [],
  "provenance_fields_present": [
    "Address Confidence",
    "Address Source URL",
    "Coordinate Source Type",
    "Coordinate Confidence",
    "Geocode Provider",
    "Geocode Method",
    "Geocode Reviewed Date"
  ],
  "provenance_fields_missing": [],
  "provenance_populated_counts": {
    "Address Confidence": 0,
    "Address Source URL": 0,
    "Coordinate Source Type": 0,
    "Coordinate Confidence": 0,
    "Geocode Provider": 0,
    "Geocode Method": 0,
    "Geocode Reviewed Date": 0
  },
  "provenance_any_populated": false,
  "human_review_true": 4,
  "coords_filled": 132,
  "description_filled": 0,
  "amenities_filled": 215,
  "owner_filled": 0,
  "operator_filled": 0,
  "rooms_filled": 0,
  "opening_filled": 0,
  "renovation_filled": 0,
  "affiliation_start_filled": 0,
  "zero_zero": 0,
  "pass": true
}
```

## 6. Brand Explorer safety result

```json
{
  "touched": false,
  "writes": 0,
  "gates": [
    {
      "label": "active_universe_sot",
      "ok": true,
      "exit_code": 0,
      "command": "npm run brand-explorer-active-universe-source-of-truth -- --dry-run"
    },
    {
      "label": "global_active_semantic",
      "ok": true,
      "exit_code": 0,
      "command": "npm run brand-explorer-global-active-semantic-audit -- --dry-run --fresh"
    },
    {
      "label": "pvql_quiet",
      "ok": true,
      "exit_code": 0,
      "command": "node scripts/brand-explorer-quiet-sequential-pvql.mjs"
    },
    {
      "label": "momentum_evidence",
      "ok": true,
      "exit_code": 0,
      "command": "npm run test:brand-explorer-recent-momentum-evidence-quality"
    },
    {
      "label": "mandatory_release_gates",
      "ok": true,
      "exit_code": 0,
      "command": "npm run test:brand-explorer-mandatory-release-gates"
    }
  ],
  "all_pass": true,
  "active_universe": 62,
  "summary": {
    "active_universe": 62,
    "pvql": "PASS",
    "momentum": "PASS",
    "mandatory_gates": "PASS",
    "semantic": "PASS"
  }
}
```

## 7. Learning ledger update

```json
{
  "entry_id": "census-schema-v113-provenance-fields-applied",
  "ledger_entries": 22,
  "audit_status": "dealality_batch_learning_system_ready",
  "process_actually_learned": true
}
```

## 8. Recommended next step

Founder provider/storage decision (Mapbox Permanent recommended) → re-run address-geocode dry-run → approved apply with provenance writes.


