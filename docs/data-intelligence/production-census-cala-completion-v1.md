# CALA Census Completion Mission v1

**Status:** `production_census_cala_completion_v1_partial_source_remaining`
**Objective:** `cala-census-completion-v1`
**Write target:** Hotel Property Census (`tbl9aY5ijiuIzzWam`)
**Airtable writes:** no
**Brand Setup writes:** false
**Brand Explorer writes:** false
**Inserts:** 0
**Updates:** 0
**Runtime ms:** 1525151

## Inventory

| Metric | Before | After |
| --- | ---: | ---: |
| Total records | 1224 | 1224 |
| Clean Core | 1003 | 1003 |
| Excluded from Clean Core | 221 | 221 |
| Unknown brands | 113 | 113 |
| Dirty partner labels | 16 | 16 |
| Human Review | 170 | 170 |
| Unknown City | 6 | 6 |
| Canonical blank | 3 | 3 |
| State / Region complete | 1018 | 1018 |

## Geography

| Metric | Before | After |
| --- | ---: | ---: |
| Continent complete | 1224 | 1224 |
| Sub-Continent complete | 1224 | 1224 |
| Market complete | 780 | 780 |
| Submarket complete | 200 | 200 |

## Level 2

| Metric | Before | After |
| --- | ---: | ---: |
| Address complete | 400 | 400 |
| Address Confidence High | 324 | 324 |
| Address Source URL complete | 325 | 325 |
| Lat/Long complete | 379 | 379 |
| Mapbox eligible | 89 | 89 |
| Est. Mapbox requests | 89 | 89 |
| Phone complete | 350 | 350 |
| Rooms complete | 116 | 116 |

## Readiness

| Metric | Before | After |
| --- | ---: | ---: |
| Map Ready | 203 | 203 |
| Contact Ready | 223 | 223 |
| Size Ready | 79 | 79 |
| Complete Census v1 | 14 | 14 |
| Needs Source Lookup | 5 | 5 |
| Needs Steward Review | 46 | 46 |
| Duplicate Risk | 170 | 170 |
| Not Usable Yet / below Clean Core | 221 | 221 |

## Dirty partner labels (parked — not force-mapped)

- Count: 16
- Classification: dirty_partner_label / excluded_from_clean_core / steward_review_required
- Brand Setup not modified

## Brand Setup promotion pack (read-only)

- **Test Brand X** ×3 · Accor · — · `steward_review_for_brand_setup_promotion`

## Operations

- Records updated: 0
- Records inserted: 0
- Dirty partners parked (writes): 0
- Fields written: (none)
- Phases completed: 8
- Passes / runtime: see run_dir
- Safety stops: 0
- Run dir: `C:\Dev\deal-capture-proxy\reports\research-engine-v2\autopilot\2026-08-07T17-05-05_CALA-cala-census-completion-v1-mission`

## Safety

- Hotel Property Census only
- Brand Setup / Brand Explorer untouched
- No owner/operator/date / Recent Momentum / Company Validated / Brand Verified
- No weak brand inference; dirty labels parked
- No Mapbox on dirty identity; phone/rooms official-only

## Next recommended action

Official sources exhausted for some Level 2 fields — dirty partner labels remain parked; do not invent address/phone/rooms/coords or promote Brand Setup automatically.
