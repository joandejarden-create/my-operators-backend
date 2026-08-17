# Complete CALA Census Mission — Final Summary

**Status:** `production_census_complete_census_v1_mission_partial_source_remaining`  
**Objective:** `complete-census-v1`  
**Generated:** 2026-08-06T22:32:36.460Z  
**Airtable writes:** no  
**Inserts:** 0  
**Updates:** 0  
**Run dir:** `C:\Dev\deal-capture-proxy\reports\research-engine-v2\autopilot\2026-08-06T22-23-30_CALA-complete-census-v1-mission`

## Before → After

| Metric | Before | After |
|--------|-------:|------:|
| Total records | 907 | 907 |
| Clean Core | 865 | 865 |
| Below Clean Core | 42 | 42 |
| Continent complete | 903 | 903 |
| Sub-Continent complete | 903 | 903 |
| Market complete | 461 | 461 |
| Submarket complete | 38 | 38 |
| State / Region complete | 43 | 43 |
| Address complete | 196 | 196 |
| Address Confidence High | 195 | 195 |
| Address Source URL complete | 195 | 195 |
| Lat/Long complete | 243 | 243 |
| Mapbox eligible | 0 | 0 |
| Phone complete | 0 | 0 |
| Rooms complete | 5 | 5 |
| Complete Census v1 | 0 | 0 |
| Map Ready | 194 | 194 |
| Contact Ready | 0 | 0 |
| Size Ready | 0 | 0 |
| Blocked missing address | 671 | 671 |
| Blocked dirty identity | 42 | 42 |
| Blocked source access | 0 | 0 |
| Coord blocked dirty identity | 38 | 38 |
| Steward remaining | 28 | 28 |
| Source lookup remaining | 7 | 7 |
| Duplicate risk remaining | 7 | 7 |
| Est. Mapbox requests | 0 | 0 |
| Est. Mapbox cost USD | 0 | 0 |

## Soft targets met

- address_complete: no
- lat_long_complete: no
- phone_complete: no
- rooms_complete: no
- complete_census_v1: no

## Fields written

- (none)

## Queues executed

- core_identity_quality
- key_field_completion
- market_geography_completion
- address_confirmation
- coordinate_completion
- phone_number_enrichment
- rooms_keys
- clean_core_classification

## Phases

- **Reconfirm Clean Core + Geography**: updates=0
- **State / Region Completion (Clean Core)**: updates=0
- **Address Completion (Clean Core)**: updates=0
- **Coordinate Completion (Clean Core + High Address)**: updates=0
- **Phone Completion (Clean Core / official only)**: updates=0
- **Rooms Completion (Clean Core / official only)**: updates=0
- **Final Readiness Classification**: updates=0

## Safety stops

- (none)

## Next recommended action

Official sources exhausted for some Level 2 fields — steward/source-lookup remaining; do not invent address/phone/rooms/coords.
