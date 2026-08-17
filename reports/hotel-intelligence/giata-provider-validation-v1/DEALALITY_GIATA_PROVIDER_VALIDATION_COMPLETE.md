# DEALALITY_GIATA_PROVIDER_VALIDATION_COMPLETE

## 1. Safety

```
Airtable writes: 0
Census writes: 0
Brand Explorer writes: 0
Automatic merges: 0
Schema changes: 0
Migrations: 0
Secrets exposed: 0
```

ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES=0 · ENABLE_HBX_CENSUS_WRITES=0

## 2. GIATA Account

```
Product: GIATA Drive — Open Content Link (Hotel Directory subset)
Base: https://giatadrive.com/api/v1
Auth: Bearer API key (Authorization: Bearer <GIATA_DRIVE_API_KEY>)
Credential status: API key present
Reachable: true
Entitlement: GIATA_DRIVE_OPEN_CONTENT_LINK_OK
Response format: application/json
Global Open Content properties: 4604
```

GIATA_DRIVE_USERNAME/PASSWORD present but Basic auth rejected on Drive API; MultiCodes/MHG returned 401 with these credentials

### Existing integration audit
{
  "marker": "GIATA_EXISTING_INTEGRATION_AUDIT_COMPLETE",
  "credential_variables_present": [
    "GIATA_DRIVE_API_KEY",
    "GIATA_DRIVE_USERNAME",
    "GIATA_DRIVE_PASSWORD"
  ],
  "existing_GIATA_client": false,
  "existing_provider_adapter": false,
  "existing_MCP_integration": false,
  "existing_scripts": false,
  "existing_tests": false,
  "existing_documentation": "lib/research-engine-v2/external-hotel-source-registry.js references GIATA MultiCodes/Hotel Guide as licensed stub only"
}

## 3. Actual Account Capability Matrix

| Field | Status |
| --- | --- |
| giata_hotel_id | CONFIRMED_SUPPORTED |
| hotel_name | CONFIRMED_SUPPORTED |
| alternate_names | NOT_SUPPORTED |
| former_names | NOT_SUPPORTED |
| city | CONFIRMED_SUPPORTED |
| destination | CONFIRMED_SUPPORTED |
| state_region | CONFIRMED_SUPPORTED |
| country | CONFIRMED_SUPPORTED |
| country_iso | CONFIRMED_SUPPORTED |
| street_address | CONFIRMED_SUPPORTED |
| postal_code | CONFIRMED_SUPPORTED |
| latitude | CONFIRMED_SUPPORTED |
| longitude | CONFIRMED_SUPPORTED |
| chain_brand | CONFIRMED_SUPPORTED |
| parent_company | NOT_SUPPORTED |
| hotel_category_star | CONFIRMED_SUPPORTED |
| property_type | NOT_SUPPORTED |
| active_inactive_status | NOT_SUPPORTED |
| website | CONFIRMED_SUPPORTED |
| phone | CONFIRMED_SUPPORTED |
| total_property_room_count | SUPPORTED_BUT_NOT_ENTITLED |
| room_types | CONFIRMED_SUPPORTED |
| construction_year | NOT_SUPPORTED |
| renovation_year | NOT_SUPPORTED |
| supplier_ids | SUPPORTED_BUT_NOT_ENTITLED |
| booking_id | SUPPORTED_BUT_NOT_ENTITLED |
| hotelbeds_id | SUPPORTED_BUT_NOT_ENTITLED |
| expedia_id | SUPPORTED_BUT_NOT_ENTITLED |
| other_provider_ids | NOT_SUPPORTED |
| descriptions | CONFIRMED_SUPPORTED |
| facts_amenities | CONFIRMED_SUPPORTED |
| images | CONFIRMED_SUPPORTED |

## 4. Room Count Verdict

```
GIATA_TOTAL_PROPERTY_ROOM_COUNT: SUPPORTED_BUT_NOT_ENTITLED
```

roomTypes[] = catalog of room type names/codes/views — NOT total physical keys. No roomCount/totalRooms field in Open Content property payload.

## 5. Country Discovery

```
GIATA_COUNTRY_DISCOVERY_SUPPORTED: YES
```

Filters confirmed: countryCode (ISO alpha-2), after (incremental), property id

## 6–10. Controlled Country Tests

### Brazil (BR)
- Dealality: 494
- GIATA Open Content: 15
- Existing matches: 1 · New: 14 · Ambiguous: 0
- City 100% · Coords 100% · API calls 16

### Mexico (MX)
- Dealality: 2181
- GIATA Open Content: 42
- Existing matches: 14 · New: 19 · Ambiguous: 9
- City 100% · Coords 100% · API calls 43

### Dominican Republic (DO)
- Dealality: 654
- GIATA Open Content: 22
- Existing matches: 12 · New: 2 · Ambiguous: 8
- City 100% · Coords 95.5% · API calls 23

### Paraguay (PY)
- Dealality: 0
- GIATA Open Content: 0
- Existing matches: 0 · New: 0 · Ambiguous: 0
- City null% · Coords null% · API calls 1

### Turks and Caicos Islands (TC)
- Dealality: 0
- GIATA Open Content: 3
- Existing matches: 0 · New: 3 · Ambiguous: 0
- City 100% · Coords 100% · API calls 4

### Bonaire (BQ)
- Dealality: 0
- GIATA Open Content: 1
- Existing matches: 0 · New: 1 · Ambiguous: 0
- City 100% · Coords 100% · API calls 2

### Brazil opportunity
{
  "dealality": 494,
  "estimated_known_universe": 5336,
  "giata_brazil_active": 15,
  "already_in_dealality": 1,
  "probable_new": 14,
  "ambiguous": 0,
  "cvent_hold_pool": 5165,
  "verdict": "GIATA Open Content Brazil set is tiny vs Cvent hold pool — cannot replace Cvent for Brazil scale; useful as high-quality identity anchors for the few branded properties present."
}

### Zero-coverage opportunity
[
  {
    "country": "Paraguay",
    "dealality": 0,
    "giata_active": 0,
    "existing": 0,
    "new_candidates": 0,
    "potential_coverage_note": "No Open Content properties for this ISO in entitlement"
  },
  {
    "country": "Turks and Caicos Islands",
    "dealality": 0,
    "giata_active": 3,
    "existing": 0,
    "new_candidates": 3,
    "potential_coverage_note": "Could seed 3 high-quality shells (still far from full national universe)"
  },
  {
    "country": "Bonaire",
    "dealality": 0,
    "giata_active": 1,
    "existing": 0,
    "new_candidates": 1,
    "potential_coverage_note": "Could seed 1 high-quality shells (still far from full national universe)"
  }
]

## 11–12. External ID / Supplier mapping

- GIATA ID: **CONFIRMED_SUPPORTED** as persistent external id on `dhl_` graph
- Supplier mapping: **NO** — No supplier/channel mapping objects in Drive Open Content property payloads. MultiCodes (mapping product) not entitled (401).

## 13–15. Roles / efficiency / incremental

Recommended roles: SECONDARY_UNIVERSE_DISCOVERY, IDENTITY_VALIDATION, EXTERNAL_ID_GRAPH, GEO_ENRICHMENT, BRAND_ENRICHMENT
Not useful as: PRIMARY_UNIVERSE_DISCOVERY, ROOM_COUNT, DEDUPLICATION

API efficiency: {"api_calls":91,"controlled_records":83,"global_list_size":4604,"hotels_per_list_call":4604,"detail_calls_required":true,"avg_country_latency_note":"list ~80–800ms; detail ~100–700ms each","quota_signals_observed":"none in response headers","candidate_new_per_detail_call":0.46987951807228917}

```
GIATA_INCREMENTAL_UPDATE_SUPPORTED: YES
```

## 16–18. Verdict

**Overall:** `GIATA_HIGH_VALUE_COMPLEMENTARY_PROVIDER`

**Highest-value next step (do not execute):** Add a read-only Hotel Intelligence provider adapter for GIATA Drive Open Content that attaches giataId + geo/address/brand evidence to existing dhl_ hotels and stages NEW_CANDIDATE shells only for zero/near-zero countries where Open Content returns rows — do not treat Drive as CALA universe SoT.
