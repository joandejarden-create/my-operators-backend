# HBX Content API Smoke Test v1

**Status:** `production_census_hbx_content_api_smoke_test_v1_complete`
**Objective:** `hbx-content-api-smoke-test-v1`
**Generated:** 2026-08-09T14:04:58.887Z
**Client:** `hbx-content-api-client-v1`
**HBX env:** `test`
**Content base:** `https://api.test.hotelbeds.com/hotel-content-api/1.0`

## No-write confirmation

- Airtable writes: **0**
- Hotel Property Census writes: **0**
- Brand Explorer writes: **0**
- Brand Setup writes: **0**
- `ENABLE_HBX_CENSUS_WRITES`: **0**
- `ENABLE_HBX_INSERTS`: **0**
- Future write target (not used): Hotel Property Census (`tbl9aY5ijiuIzzWam`)

## Auth

- Success: **true**
- API key fingerprint: `5421…(len=32)`
- Signature fingerprint (truncated): `671a8348…(len=64)`
- Secrets logged: **false**

## Endpoints tested

- **A_status_auth** `GET /hotel-api/1.0/status` → status **200** ok=true (1638ms)
- **B_hotels_sample** `GET /hotel-content-api/1.0/hotels?fields=all&language=ENG&from=1&to=10&useSecondaryLanguage=false` → status **200** ok=true (935ms)
- **C_countries** `GET /hotel-content-api/1.0/locations/countries?fields=all&language=ENG&from=1&to=5` → status **200** ok=true (136ms)
- **C_destinations** `GET /hotel-content-api/1.0/locations/destinations?fields=all&language=ENG&from=1&to=5` → status **200** ok=true (109ms)
- **C_categories** `GET /hotel-content-api/1.0/types/categories?fields=all&language=ENG&from=1&to=5` → status **200** ok=true (110ms)
- **C_chains** `GET /hotel-content-api/1.0/types/chains?fields=all&language=ENG&from=1&to=5` → status **200** ok=true (146ms)
- **C_accommodations** `GET /hotel-content-api/1.0/types/accommodations?fields=all&language=ENG&from=1&to=5` → status **403** ok=false (132ms)
- **C_facilities** `GET /hotel-content-api/1.0/types/facilities?fields=all&language=ENG&from=1&to=5` → status **403** ok=false (117ms)
- **D_hotel_details** `GET /hotel-content-api/1.0/hotels/1/details?fields=all&language=ENG&useSecondaryLanguage=false` → status **200** ok=true (278ms)

## Sample hotels

- Count: **10**
- Codes: 1, 2, 5, 7, 8, 9, 10, 11, 12, 13
- Names (truncated): `Ohtels Villa Dorada`, `htop Jadhe`, `HG Lomo Blanco`, `Servatur Don Miguel Adults Only`, `Servatur Waikiki`, `Barcelo Fuerteventura Mar`, `Natural Park`, `H10 Salou Princess`, `Hotel Lloret Santa Rosa by Pierre & Vacances`, `Rentalmar Arquus`

## Capability snapshot

| Capability | Present? |
|------------|----------|
| Address | yes |
| Coordinates | yes |
| Phone | yes |
| Website | yes |
| Rooms/Keys (total count) | no |
| Room types array only | yes |
| Brand/chain | yes |
| Descriptions | yes |
| Facilities | yes |
| Images | yes |

## Field availability matrix

| Field | Present in HBX? | Response path | Sample value | Census use recommendation |
|-------|-----------------|---------------|--------------|---------------------------|
| Hotelbeds hotel code / ID | yes | code | 1 | `write_safe_high` |
| Hotel name | yes | name.content | Ohtels Villa Dorada | `write_safe_high` |
| Accommodation type | yes | accommodationTypeCode | H | `write_medium_internal` |
| Category / star rating | yes | categoryCode | 3EST | `write_medium_internal` |
| Chain / brand | yes | chainCode | OHTEL | `candidate_only` |
| Country | yes | countryCode | ES | `write_safe_high` |
| State / Region | yes | stateCode | 43 | `write_medium_internal` |
| Destination | yes | destinationCode | SAL | `candidate_only` |
| Zone | yes | zoneCode | 10 | `candidate_only` |
| City | yes | city.content | SALOU | `write_safe_high` |
| Address | yes | address.content | Carrer Del Vendrell, 11 | `write_medium_internal` |
| Postal code | yes | postalCode | 43840 | `write_medium_internal` |
| Latitude | yes | coordinates.latitude | 41.068407 | `license_policy_needed` |
| Longitude | yes | coordinates.longitude | 1.152529 | `license_policy_needed` |
| Phone | yes | phones | array(len=4) | `write_medium_internal` |
| Phone (property PHONEHOTEL) | yes | phones[phoneType=PHONEHOTEL].phoneNumber | +34977385511 | `write_medium_internal` |
| Email | yes | email | comercial@ohtels.es | `candidate_only` |
| Website / web URL | yes | web | http://www.ohtels.es/ | `write_medium_internal` |
| Description | yes | description.content | This hotel is located about 150 metres from the fine sandy beach. The lively centre of Cambrils is approximately 10 km a | `license_policy_needed` |
| Facilities / amenities | yes | facilities | array(len=67) | `candidate_only` |
| Images | yes | images | array(len=61) | `license_policy_needed` |
| Rooms / room types | yes | rooms | array(len=23) | `unsupported` |
| Total rooms / keys | no | — | — | `unsupported` |
| Segments / tags | yes | segmentCodes | array(len=3) | `candidate_only` |
| Terminals / interest points | yes | terminals | array(len=2) | `candidate_only` |
| Last update date | yes | lastUpdate | 2026-06-08 | `write_medium_internal` |
| Active / issues | yes | ranking | 31 | `candidate_only` |

## Recommended Census write policy

**Posture:** `read_only_discovery_lane_candidate`

HBX is promising as a discovery/enrichment source. Keep ENABLE_HBX_CENSUS_WRITES=0 until dry-run ingest + license review for coordinates/images/descriptions. Prefer identity + address/city/country as Medium internal; prefer phones[].phoneType=PHONEHOTEL (reject PHONEBOOKING); Mapbox for coords after validated address unless HBX coordinate storage is licensed; Rooms/Keys NOT supported from rooms[] (room-type catalog only — no roomsNumber in sample).

### Keep flags

```
ENABLE_HBX_CENSUS_WRITES=0
ENABLE_HBX_INSERTS=0
```

### Next dry-run ingest scope

- CALA countries only (filter countryCode)
- from/to pagination batches of 100–500
- dedupe vs Hotel Property Census by name|country + Hotelbeds code key
- candidate review pack only — no apply
- phone filter: PHONEHOTEL only; drop PHONEBOOKING
- license review: coordinates, images, descriptions storage rights
- rooms: do not derive Rooms/Keys from rooms[] length; seek alternate approved sources

## Notes

- (none)
