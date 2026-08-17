# CALA Census Completion Mission — Final Summary

**Status:** `production_census_cala_completion_v1_partial_source_remaining`  
**Objective:** `cala-census-completion-v1`  
**Generated:** 2026-08-07T09:45:59.729Z  
**Airtable writes:** yes  
**Inserts:** 0  
**Updates:** 20  
**Run dir:** `C:\Dev\deal-capture-proxy\reports\research-engine-v2\autopilot\2026-08-07T09-31-22_CALA-cala-census-completion-v1-mission`

## Before → After

| Metric | Before | After |
|--------|-------:|------:|
| Total records | 1110 | 1110 |
| Clean Core | 790 | 1007 |
| Below Clean Core | 320 | 103 |
| Continent complete | 1087 | 1106 |
| Sub-Continent complete | 1087 | 1106 |
| Market complete | 629 | 637 |
| Submarket complete | 46 | 46 |
| State / Region complete | 43 | 43 |
| Address complete | 196 | 196 |
| Address Confidence High | 195 | 195 |
| Address Source URL complete | 195 | 195 |
| Lat/Long complete | 262 | 262 |
| Mapbox eligible | 0 | 0 |
| Phone complete | 21 | 21 |
| Rooms complete | 5 | 5 |
| Complete Census v1 | 0 | 0 |
| Map Ready | 97 | 184 |
| Contact Ready | 1 | 9 |
| Size Ready | 0 | 0 |
| Blocked missing address | 693 | 823 |
| Blocked dirty identity | 101 | 101 |
| Blocked source access | 0 | 0 |
| Coord blocked dirty identity | 19 | 19 |
| Steward remaining | 239 | 22 |
| Source lookup remaining | 7 | 7 |
| Duplicate risk remaining | 74 | 74 |
| Est. Mapbox requests | 0 | 0 |
| Est. Mapbox cost USD | 0 | 0 |

## Soft targets met

- address_complete: no
- lat_long_complete: no
- phone_complete: no
- rooms_complete: no
- complete_census_v1: no

## Fields written

- City
- Continent
- Sub-Continent
- Market

## Queues executed

- brand_normalization
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

- **Reconfirm Brand / Core Identity**: updates=1
- **Clean Core Classification**: updates=0
- **Market Geography**: updates=19
- **Address Completion (Clean Core)**: updates=0
- **Coordinate Completion (Clean Core + High Address)**: updates=0
- **Phone Completion (Clean Core / official only)**: updates=0
- **Rooms Completion (Clean Core / official only)**: updates=0
- **Final Readiness Classification**: updates=0

## Safety stops

- (none)

## Next recommended action

Official sources exhausted for some Level 2 fields — dirty partner labels remain parked; do not invent address/phone/rooms/coords or promote Brand Setup automatically.
