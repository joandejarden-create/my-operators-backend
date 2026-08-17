# CALA Census Completion Mission — Final Summary

**Status:** `production_census_cala_completion_v1_partial_source_remaining`  
**Objective:** `cala-census-completion-v1`  
**Generated:** 2026-08-07T10:42:01.569Z  
**Airtable writes:** yes  
**Inserts:** 0  
**Updates:** 13  
**Run dir:** `C:\Dev\deal-capture-proxy\reports\research-engine-v2\autopilot\2026-08-07T10-24-32_CALA-cala-census-completion-v1-mission`

## Before → After

| Metric | Before | After |
|--------|-------:|------:|
| Total records | 1110 | 1110 |
| Clean Core | 790 | 1007 |
| Below Clean Core | 320 | 103 |
| Continent complete | 1106 | 1106 |
| Sub-Continent complete | 1106 | 1106 |
| Market complete | 637 | 638 |
| Submarket complete | 46 | 46 |
| State / Region complete | 115 | 122 |
| Address complete | 216 | 216 |
| Address Confidence High | 215 | 215 |
| Address Source URL complete | 215 | 215 |
| Lat/Long complete | 262 | 267 |
| Mapbox eligible | 20 | 15 |
| Phone complete | 109 | 116 |
| Rooms complete | 5 | 5 |
| Complete Census v1 | 0 | 0 |
| Map Ready | 97 | 189 |
| Contact Ready | 89 | 104 |
| Size Ready | 0 | 0 |
| Blocked missing address | 673 | 803 |
| Blocked dirty identity | 101 | 101 |
| Blocked source access | 0 | 0 |
| Coord blocked dirty identity | 19 | 19 |
| Steward remaining | 239 | 24 |
| Source lookup remaining | 7 | 5 |
| Duplicate risk remaining | 74 | 74 |
| Est. Mapbox requests | 20 | 15 |
| Est. Mapbox cost USD | 0.1 | 0.075 |

## Soft targets met

- address_complete: no
- lat_long_complete: no
- phone_complete: yes
- rooms_complete: no
- complete_census_v1: no

## Fields written

- Market
- Latitude
- Longitude
- Coordinate Source Type
- Coordinate Confidence
- Geocode Provider
- Geocode Method
- Geocode Reviewed Date
- Phone

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

- **Reconfirm Brand / Core Identity**: updates=0
- **Clean Core Classification**: updates=0
- **Market Geography**: updates=1
- **Address Completion (Clean Core)**: updates=0
- **Coordinate Completion (Clean Core + High Address)**: updates=5
- **Phone Completion (Clean Core / official only)**: updates=7
- **Rooms Completion (Clean Core / official only)**: updates=0
- **Final Readiness Classification**: updates=0

## Safety stops

- (none)

## Next recommended action

Official sources exhausted for some Level 2 fields — dirty partner labels remain parked; do not invent address/phone/rooms/coords or promote Brand Setup automatically.
