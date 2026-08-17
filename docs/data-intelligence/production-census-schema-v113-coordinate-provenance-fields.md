# Production Census Schema v1.1.3 — Address & Coordinate Provenance Fields

**Status:** `production_census_schema_v113_coordinate_provenance_added_ready_for_provider_decision`  
**Generated:** 2026-08-05T15:21:46.259Z  
**Apply executed:** true  
**Base:** Deal Capture Platform (`appCCU…foLk`)  
**Table:** Hotel Property Census (`tbl9aY5ijiuIzzWam`)

## 1. Executive summary

Schema-only create of coordinate/address provenance fields. No record population. No Brand Explorer writes.

| Metric | Value |
| --- | ---: |
| Fields created | 7 |
| Fields already existing | 0 |
| Field count | 101 → 108 |
| Census records | 666 |
| Validation pass | true |
| BE gates all_pass | true |

## 2. Fields created

- **Address Confidence** (`singleSelect` · `fldSx8vdU9jS4ZKA4`)
- **Address Source URL** (`url` · `fldJQrjMEUdlzTi8O`)
- **Coordinate Source Type** (`singleSelect` · `fld4wWVNu9nkMhX1V`)
- **Coordinate Confidence** (`singleSelect` · `fldNPRyVrUOw9CSIF`)
- **Geocode Provider** (`singleSelect` · `fldAUUeHqAMemDUu6`)
- **Geocode Method** (`singleSelect` · `fldsruuAw0aG1z4AC`)
- **Geocode Reviewed Date** (`date` · `fldhdB4jEAVG7nq6X`)

## 3. Fields already existing

- (none)

## 4. Final Census field count

**108**

## 5. Census validation

- Records: 666
- Held (Human Review Required): 4
- Coords filled: 132
- Provenance populated: false
- Owner/operator/rooms/dates filled: 0/0/0/dates=0

## 6. Brand Explorer safety

all_pass: **true**

## 7. Learning ledger

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

## Apply command (reference)

```bash
ALLOW_PRODUCTION_CENSUS_SCHEMA_V113=1 \
CONFIRM_SCHEMA_ONLY_NO_RECORD_WRITES=1 \
CONFIRM_NO_BRAND_EXPLORER_WRITES=1 \
npm run research-engine-v2:production-census-schema-v113-coordinate-provenance -- --apply \
  --confirm-add-coordinate-provenance-fields-only \
  --confirm-no-record-writes \
  --confirm-no-brand-explorer-writes \
  --confirm-no-field-deletes \
  --confirm-no-field-renames \
  --confirm-no-field-population
```
