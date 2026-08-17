# Full LATAM Census Autopilot Controller v3

**Status:** `production_census_full_latam_autopilot_v3_partial_source_remaining`
**Objective:** `full-latam-census-autopilot-v3`
**Census mode:** `field-completion-only`
**Region:** CALA
**Write target:** Hotel Property Census (`tbl9aY5ijiuIzzWam`)
**Airtable writes:** no
**Brand Setup / Brand Explorer writes:** false
**Founder gate between passes:** false

## Scorecard before → after

| Metric | Before % | After % | Before n | After n |
| --- | ---: | ---: | ---: | ---: |
| Total records | — | — | 1224 | 1224 |
| Clean Core | 81.9 | 81.9 | 1003 | 1003 |
| State / Region | 83.2 | 83.2 | 1018 | 1018 |
| Market | 63.7 | 63.7 | 780 | 780 |
| Hotel URL | 80 | 80 | 979 | 979 |
| Address | 30.6 | 30.6 | 375 | 375 |
| Address High | 26.5 | 26.5 | 324 | 324 |
| Lat/Long | 31 | 31 | 379 | 379 |
| Phone | 28.6 | 28.6 | 350 | 350 |
| Rooms | 9.5 | 9.5 | 116 | 116 |
| Complete Census v1 | 1.1 | 1.1 | 14 | 14 |
| Governance Hold | 31.4 | 31.4 | 384 | 384 |
| Data Quality Hold | 10.5 | 10.5 | 129 | 129 |

## Writes

- Records updated: 0
- Records inserted: 0
- Inserts by parent: {}
- Fields written: —
- Passes run: 3
- Safety stops: none

## Top recommended actions (final)

1. **Latitude** via `mapbox_permanent` — 840 records, expected High ~588
2. **Longitude** via `mapbox_permanent` — 840 records, expected High ~588
3. **Address** via `official_json_ld` — 632 records, expected High ~221
4. **Phone** via `official_property_page` — 632 records, expected High ~221
5. **Official Property URL** via `official_parent_directory` — 235 records, expected High ~47
6. **Rooms / Keys** via `official_factsheet` — 829 records, expected High ~166
7. **Source URL** via `official_parent_directory` — 235 records, expected High ~47
8. **Address Source URL** via `official_property_page` — 788 records, expected High ~276
9. **Phone** via `official_parent_directory` — 224 records, expected High ~45
10. **Address** via `official_parent_directory` — 133 records, expected High ~27

## Command to continue

```bash
ALLOW_CENSUS_AUTOPILOT_APPLY=1 \
CONFIRM_WRITE_TO_PRODUCTION_CENSUS=1 \
CONFIRM_NO_BRAND_EXPLORER_WRITES=1 \
CONFIRM_NO_OWNER_OPERATOR_WRITES=1 \
npm run census:autopilot -- --region CALA --scope official-parent-inventory --mode mission \
  --objective full-latam-census-autopilot-v3 \
  --census-mode field-completion-only \
  --strategy highest-yield-safe \
  --run-until-complete \
  --max-passes 10 \
  --batch-size 100 \
  --confirm-safe-writes \
  --confirm-write-to-production-census \
  --confirm-no-brand-explorer-writes \
  --confirm-no-owner-operator \
  --confirm-no-date-writes \
  --confirm-no-recent-momentum \
  --confirm-no-company-validation \
  --confirm-webhound-not-production-source \
  --enable-production-writes
```
