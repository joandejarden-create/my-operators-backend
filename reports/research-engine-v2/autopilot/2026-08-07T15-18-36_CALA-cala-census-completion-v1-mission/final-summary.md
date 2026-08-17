# CALA Census Completion Mission — Final Summary

**Status:** `production_census_cala_completion_v1_partial_source_remaining`  
**Objective:** `cala-census-completion-v1`  
**Generated:** 2026-08-07T15:43:06.105Z  
**Airtable writes:** yes  
**Inserts:** 0  
**Updates:** 57  
**Run dir:** `C:\Dev\deal-capture-proxy\reports\research-engine-v2\autopilot\2026-08-07T15-18-36_CALA-cala-census-completion-v1-mission`

## Before → After

| Metric | Before | After |
|--------|-------:|------:|
| Total records | 1224 | 1224 |
| Clean Core | 1003 | 1023 |
| Below Clean Core | 221 | 201 |
| Continent complete | 1224 | 1224 |
| Sub-Continent complete | 1224 | 1224 |
| Market complete | 780 | 780 |
| Submarket complete | 200 | 200 |
| State / Region complete | 1018 | 1018 |
| Address complete | 400 | 400 |
| Address Confidence High | 324 | 324 |
| Address Source URL complete | 325 | 325 |
| Lat/Long complete | 374 | 374 |
| Mapbox eligible | 92 | 92 |
| Phone complete | 350 | 350 |
| Rooms complete | 116 | 116 |
| Complete Census v1 | 14 | 21 |
| Map Ready | 200 | 219 |
| Contact Ready | 223 | 238 |
| Size Ready | 79 | 86 |
| Blocked missing address | 711 | 712 |
| Blocked dirty identity | 169 | 199 |
| Blocked source access | 0 | 0 |
| Coord blocked dirty identity | 41 | 17 |
| Steward remaining | 100 | 26 |
| Source lookup remaining | 5 | 5 |
| Duplicate risk remaining | 116 | 170 |
| Est. Mapbox requests | 92 | 92 |
| Est. Mapbox cost USD | 0.46 | 0.46 |

## Soft targets met

- address_complete: yes
- lat_long_complete: yes
- phone_complete: yes
- rooms_complete: yes
- complete_census_v1: no

## Fields written

- Human Review Required

## Queues executed

- brand_normalization
- parent_company_normalization
- core_identity_quality
- core_identity_source_lookup
- key_field_completion
- clean_core_classification
- market_geography_completion
- address_confirmation
- coordinate_completion
- phone_number_enrichment
- rooms_keys

## Phases

- **Reconfirm Brand / Core Identity**: updates=57
- **Clean Core Classification**: updates=0
- **Market Geography**: updates=0
- **Address Completion (Clean Core)**: updates=0
- **Coordinate Completion (Clean Core + High Address)**: updates=0
- **Phone Completion (Clean Core / official only)**: updates=0
- **Rooms Completion (Clean Core / official only)**: updates=0
- **Final Readiness Classification**: updates=0

## Safety stops

- (none)

## Next recommended action

Official sources exhausted for some Level 2 fields — dirty partner labels remain parked; do not invent address/phone/rooms/coords or promote Brand Setup automatically.
