# Production Census Schema v1.1.2 — Radar/Public Display Fields

**Status:** `production_census_schema_v112_radar_fields_added_ready_for_radar_readiness_classification`  
**Generated:** 2026-08-05T12:35:55.867Z  
**Apply executed:** true  
**Base:** Deal Capture Platform (`appCCU…foLk`)  
**Table:** Hotel Property Census (`tbl9aY5ijiuIzzWam`)

## What was added

Schema-only create. Fields are blank on all 666 records.

| Field | Type | Airtable field ID | Options |
| --- | --- | --- | --- |
| Radar Display Status | singleSelect | `fldJqFaunHrRjw4eg` | Public Map Eligible · Public List Eligible · Internal Only · Hold |
| Radar Display Reason | multilineText | `fldHu1uAq8MVHV61e` | — |
| Radar Geography Status | singleSelect | `fldqZ73gZqAFrCcvj` | Coordinates Available · City-Level Only · Address Available No Coordinates · Geography Insufficient · Hold |
| Public Census Eligibility | singleSelect | `fldVgYyN0HKXiaLmx` | Eligible · Eligible With Limits · Not Eligible · Hold |
| Public Display Confidence | singleSelect | `fldy1k4LDRaIF9CpD` | High · Medium · Low · Hold |
| Public Display Review Status | singleSelect | `fld4WuHANP3xJCwRU` | Auto-Classified · Needs Review · Approved · Hold |

## Counts

- Field count: **95 → 101**
- Census records: **666** (unchanged)
- Validation pass: **true**
- Radar fields populated: **0 / 6**

## Safety

- No record population of Radar fields
- No enrichment writes (descriptions / amenities / owner / operator / rooms / dates remain empty)
- No Brand Explorer writes
- No field deletes or renames
- Brand Explorer all_pass: **true** (universe 62 · semantic 0/0/0 · PVQL PASS · momentum PASS · mandatory gates PASS)

## Apply command

```bash
ALLOW_PRODUCTION_CENSUS_SCHEMA_V112=1 \
CONFIRM_SCHEMA_ONLY_NO_RECORD_WRITES=1 \
CONFIRM_NO_BRAND_EXPLORER_WRITES=1 \
npm run research-engine-v2:production-census-schema-v112-radar-fields -- --apply \
  --confirm-add-radar-public-fields-only \
  --confirm-no-record-writes \
  --confirm-no-brand-explorer-writes \
  --confirm-no-field-deletes \
  --confirm-no-field-renames
```

## Reports

- `reports/research-engine-v2/production-census-schema-v112-radar-fields-apply.md`
- `reports/research-engine-v2/production-census-schema-v112-radar-fields-apply.json`

## Next

Run Radar/public readiness classification (separate task) or proceed with first enrichment lane under the frozen v1.1.1 contract.
