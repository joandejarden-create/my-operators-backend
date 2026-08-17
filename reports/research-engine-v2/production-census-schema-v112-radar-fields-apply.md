# Production Census Schema v1.1.2 — Radar/Public Display Fields Apply

**Status:** `production_census_schema_v112_radar_fields_added_ready_for_radar_readiness_classification`
**Mode:** apply
**Generated:** 2026-08-05T12:35:55.867Z

## 1. Executive summary

- Apply executed: true
- Fields created: 6
- Fields already existing: 0
- Field count: 95 → 101
- Validation pass: true
- Ready for Radar readiness classification: true

## 2. Fields found before apply

```json
{
  "radar_present": [],
  "radar_missing": [
    "Radar Display Status",
    "Radar Display Reason",
    "Radar Geography Status",
    "Public Census Eligibility",
    "Public Display Confidence",
    "Public Display Review Status"
  ]
}
```

## 3. Fields created

```json
[
  {
    "name": "Radar Display Status",
    "type": "singleSelect",
    "id": "fldJqFaunHrRjw4eg",
    "options": [
      "Public Map Eligible",
      "Public List Eligible",
      "Internal Only",
      "Hold"
    ]
  },
  {
    "name": "Radar Display Reason",
    "type": "multilineText",
    "id": "fldHu1uAq8MVHV61e",
    "options": null
  },
  {
    "name": "Radar Geography Status",
    "type": "singleSelect",
    "id": "fldqZ73gZqAFrCcvj",
    "options": [
      "Coordinates Available",
      "City-Level Only",
      "Address Available No Coordinates",
      "Geography Insufficient",
      "Hold"
    ]
  },
  {
    "name": "Public Census Eligibility",
    "type": "singleSelect",
    "id": "fldVgYyN0HKXiaLmx",
    "options": [
      "Eligible",
      "Eligible With Limits",
      "Not Eligible",
      "Hold"
    ]
  },
  {
    "name": "Public Display Confidence",
    "type": "singleSelect",
    "id": "fldy1k4LDRaIF9CpD",
    "options": [
      "High",
      "Medium",
      "Low",
      "Hold"
    ]
  },
  {
    "name": "Public Display Review Status",
    "type": "singleSelect",
    "id": "fld4WuHANP3xJCwRU",
    "options": [
      "Auto-Classified",
      "Needs Review",
      "Approved",
      "Hold"
    ]
  }
]
```

## 4. Fields already existing

```json
[]
```

## 5. Final field count

- Before: 95
- After: 101

## 6. Census validation

```json
{
  "record_count": 666,
  "field_count": 101,
  "expected_field_count": 101,
  "duplicates": 0,
  "radar_fields_present": [
    "Radar Display Status",
    "Radar Display Reason",
    "Radar Geography Status",
    "Public Census Eligibility",
    "Public Display Confidence",
    "Public Display Review Status"
  ],
  "radar_fields_missing": [],
  "radar_populated_counts": {
    "Radar Display Status": 0,
    "Radar Display Reason": 0,
    "Radar Geography Status": 0,
    "Public Census Eligibility": 0,
    "Public Display Confidence": 0,
    "Public Display Review Status": 0
  },
  "enrichment_not_started": 666,
  "human_review_true": 4,
  "production_use_ok": 666,
  "description_filled": 0,
  "amenities_filled": 0,
  "owner_filled": 0,
  "operator_filled": 0,
  "rooms_filled": 0,
  "opening_filled": 0,
  "renovation_filled": 0,
  "affiliation_start_filled": 0,
  "zero_zero": 0,
  "renames_present": {
    "Last Reviewed Date": true,
    "Resort / Leisure Flag": true,
    "Extended Stay Flag": true
  },
  "old_names_absent": {
    "Last Verified Date": true,
    "Resort Amenities Flag": true,
    "Extended Stay Amenity Flag": true
  },
  "pass": true
}
```

## 7. Brand Explorer safety result

```json
{
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
  "semantic": {
    "activeCount": 62,
    "severityTotals": {
      "critical": 0,
      "high": 0,
      "medium": 0,
      "low": 0
    },
    "freezeDecision": "ready_to_freeze_62_semantic_qa_clean"
  },
  "summary": {
    "active_universe": 62,
    "semantic_c_h_m": "0/0/0",
    "pvql": "PASS",
    "momentum": "PASS",
    "mandatory_gates": "PASS"
  },
  "expected": {
    "active_universe": 62,
    "semantic_c_h_m": "0/0/0",
    "pvql": "PASS",
    "momentum": "PASS",
    "mandatory_gates": "PASS"
  }
}
```

## 8. Whether Radar/public readiness classification can run next

true

## Next

Run Radar/public readiness classification (separate task) or proceed with first enrichment lane under the frozen v1.1.1 contract.

