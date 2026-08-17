# CALA Census Completion Mission — Final Summary

**Status:** `production_census_cala_completion_v1_partial_source_remaining`  
**Objective:** `cala-census-completion-v1`  
**Generated:** 2026-08-07T11:40:24.657Z  
**Airtable writes:** yes  
**Inserts:** 0  
**Updates:** 12  
**Run dir:** `C:\Dev\deal-capture-proxy\reports\research-engine-v2\autopilot\2026-08-07T11-27-14_CALA-cala-census-completion-v1-mission`

## Before → After

| Metric | Before | After |
|--------|-------:|------:|
| Total records | 1205 | 1205 |
| Clean Core | 885 | 1009 |
| Below Clean Core | 320 | 196 |
| Continent complete | 1106 | 1201 |
| Sub-Continent complete | 1106 | 1201 |
| Market complete | 638 | 744 |
| Submarket complete | 46 | 142 |
| State / Region complete | 230 | 230 |
| Address complete | 250 | 331 |
| Address Confidence High | 249 | 249 |
| Address Source URL complete | 249 | 249 |
| Lat/Long complete | 362 | 366 |
| Mapbox eligible | 47 | 43 |
| Phone complete | 240 | 286 |
| Rooms complete | 23 | 37 |
| Complete Census v1 | 0 | 1 |
| Map Ready | 181 | 198 |
| Contact Ready | 183 | 191 |
| Size Ready | 18 | 20 |
| Blocked missing address | 657 | 768 |
| Blocked dirty identity | 196 | 194 |
| Blocked source access | 0 | 0 |
| Coord blocked dirty identity | 20 | 18 |
| Steward remaining | 146 | 22 |
| Source lookup remaining | 5 | 5 |
| Duplicate risk remaining | 169 | 169 |
| Est. Mapbox requests | 47 | 43 |
| Est. Mapbox cost USD | 0.235 | 0.215 |

## Soft targets met

- address_complete: no
- lat_long_complete: yes
- phone_complete: yes
- rooms_complete: no
- complete_census_v1: no

## Fields written

- Canonical Property Name
- Market
- Submarket
- Latitude
- Longitude
- Coordinate Source Type
- Coordinate Confidence
- Geocode Provider
- Geocode Method
- Geocode Reviewed Date
- Radar Geography Status
- Radar Display Status
- Radar Display Reason
- Public Display Review Status

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

- **Reconfirm Brand / Core Identity**: updates=2
- **Clean Core Classification**: updates=0
- **Market Geography**: updates=6
- **Address Completion (Clean Core)**: updates=0
- **Coordinate Completion (Clean Core + High Address)**: updates=4
- **Phone Completion (Clean Core / official only)**: updates=0
- **Rooms Completion (Clean Core / official only)**: updates=0
- **Final Readiness Classification**: updates=0

## Safety stops

- (none)

## Next recommended action

Official sources exhausted for some Level 2 fields — dirty partner labels remain parked; do not invent address/phone/rooms/coords or promote Brand Setup automatically.
