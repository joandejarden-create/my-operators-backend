# DEALALITY_GIATA_TEST_PRODUCTS_VALIDATION_COMPLETE

**Non-negotiable:** `TEST_SAMPLE_NOT_VALID_FOR_GEOGRAPHIC_COVERAGE`

MultiCodes TEST / MHG TEST return random hotels — not usable for CALA, country, or production-scale yield estimates.

## Auth fix applied

GIATA activation mail specifies Basic-auth username:

`giata|aohospitalityadvisors.com`

Env usernames without `|` caused the earlier 401s. Validation script now resolves `user|company` (activation-mail fallback when env lacks pipe).

Update `.env` to store the pipe form explicitly (see `.env.example`).

---

## 1. Safety

```text
Airtable writes: 0
Census writes: 0
Brand Explorer writes: 0
Automatic merges: 0
Canonical writes: 0
Schema changes: 0
Migrations: 0
Secrets exposed: false
```

## 2. MultiCodes Connectivity

```text
MULTICODES_TEST_CONNECTIVITY
credentials_present: true
reachable: true
HTTP_status: 200
authenticated: true
response_format: xml
sanitized_error: null
auth: HTTP Basic (user|company)
```

## 3. MHG Connectivity

```text
MHG_TEST_CONNECTIVITY
credentials_present: true
reachable: true
HTTP_status: 200
authenticated: true
response_format: xml
sanitized_error: null
auth: HTTP Basic (user|company)
```

## 4. Random TEST Data Warning

**Confirmed:** `TEST_SAMPLE_NOT_VALID_FOR_GEOGRAPHIC_COVERAGE`

Sample size: 15 MultiCodes properties + 15 MHG properties (schema only).

## 5. MultiCodes Capability Matrix

| Field | Status |
| --- | --- |
| giata_id | CONFIRMED_SUPPORTED |
| name | CONFIRMED_SUPPORTED |
| alternate_names | NOT_OBSERVED |
| former_names | NOT_OBSERVED |
| city | CONFIRMED_SUPPORTED |
| country | CONFIRMED_SUPPORTED |
| address | CONFIRMED_SUPPORTED |
| coordinates | CONFIRMED_SUPPORTED |
| brand/chain | CONFIRMED_SUPPORTED |
| category | CONFIRMED_SUPPORTED |
| inactive status | NOT_OBSERVED |
| supplier mappings | CONFIRMED_SUPPORTED |
| room_count | NOT_OBSERVED |

`MULTICODES_EXTERNAL_ID_GRAPH_VALUE: HIGH`

## 6. Supplier Mapping Matrix (`MULTICODES_SUPPLIER_MAPPING_MATRIX`)

| supplier/provider | mapping field/path | sample count | persistent ID | usable crosswalk | entitled in TEST |
| --- | --- | ---: | --- | --- | --- |
| AIC Travelgroup (`aic_travelgroup`, gds) | `propertyCodes/provider[@providerCode]/code/value` | 15/15 | yes | yes | yes |
| GoGlobal (`goglobal`, gds) | same | 15/15 | yes | yes | yes |
| Restel (`restel`, gds) | same | 15/15 | yes | yes | yes |
| Hotelbeds | — | 0 | — | — | NOT_OBSERVED in this TEST sample |
| Booking.com | — | 0 | — | — | NOT_OBSERVED |
| Expedia | — | 0 | — | — | NOT_OBSERVED |
| Agoda / Amadeus / Sabre / Travelport | — | 0 | — | — | NOT_OBSERVED |

Path: `property/propertyCodes/provider[@providerCode,@providerType]/code/value`

## 7. MultiCodes Identity / Crosswalk Verdict

```text
MULTICODES_HIGH_VALUE_IDENTITY_CROSSWALK
```

Evidence: stable numeric `giataId`, name/geo/address/chain, plus multi-provider codes on every sampled property. Does **not** replace `dhl_`.

## 8. MHG Capability Matrix (`MHG_TEST_CAPABILITY_MATRIX`)

| Field | Status |
| --- | --- |
| name | CONFIRMED_SUPPORTED |
| city / country | CONFIRMED_SUPPORTED |
| address / postal / lat / lng on item shell | NOT_OBSERVED (geo richer on MultiCodes) |
| room count / total keys | CONFIRMED_SUPPORTED (`num_rooms_total`) |
| room types catalog | NOT_OBSERVED |
| brand / chain | NOT_OBSERVED in fact names sampled |
| star/category | CONFIRMED_SUPPORTED |
| phone | CONFIRMED_SUPPORTED |
| website | NOT_OBSERVED |
| descriptions | CONFIRMED_SUPPORTED |
| facts / amenities | CONFIRMED_SUPPORTED |
| images | CONFIRMED_SUPPORTED (endpoint OK; some properties empty) |
| opening / renovation year | CONFIRMED_SUPPORTED |
| status | NOT_OBSERVED |

## 9. MHG Room Count Semantic Analysis

| Candidate | Path | Meaning |
| --- | --- | --- |
| **`num_rooms_total`** | `factsheet/sections/section/facts/fact[@name=num_rooms_total]/value` | **TOTAL PROPERTY ROOM COUNT** (`typeHint=int`) |
| `num_rooms_single` / `num_rooms_double` | same facts section | Inventory breakdown — not total keys alone |
| `room_*` facts | `room_facilities` section | Amenity flags (bath, TV, …) — NOT keys |
| `facility_roomservice` | facilities | Boolean amenity — NOT keys |
| Text “N rooms” | `texts/en` paragraphs | Narrative corroboration of total |

## 10. MHG Room Count Verdict

```text
MHG_TOTAL_PROPERTY_ROOM_COUNT_CONFIRMED
```

```text
field/path: fact[@name=num_rooms_total]
sample_presence_rate: 1.0 (15/15)
sample_numeric_values: 6,7,12,19,42,60,69,71,74,75,88,96,97,111,123,182,185,195,314 …
```

Firewall: map **only** `num_rooms_total` → `room_count`. Never map room facility facts or room-type catalogs.

## 11. MultiCodes vs MHG Roles

| Product | Role |
| --- | --- |
| MultiCodes | identity, dedupe, supplier ID crosswalk, chain/geo anchors |
| MHG | rich content + **confirmed total keys** + descriptions/amenities/images |

## 12. Drive vs MultiCodes vs MHG

| | Drive | MultiCodes | MHG |
| --- | --- | --- | --- |
| Auth | Bearer API key | HTTP Basic `user\|company` | HTTP Basic `user\|company` |
| Format | JSON | XML | XML |
| Country filter | Yes (Open Content) | Spec yes; TEST random | Spec yes; TEST random |
| Universe today | ~4.6k Open Content | TEST only (random) | TEST only (random) |
| Supplier IDs | No | Yes (TEST: AIC/GoGlobal/Restel) | No |
| Total keys | No | No | **Yes (`num_rooms_total`)** |
| Production roles (Drive unchanged) | SECONDARY discovery / geo / brand enrich | IDENTITY + CROSSWALK | ROOM_COUNT + RICH_CONTENT |

## 13. Recommended MCP Provider Structure

```text
GIATA_PROVIDER_STRUCTURE_RECOMMENDATION
giata_drive
giata_multicodes
giata_mhg
```

Do not collapse into one generic `giata` adapter.

## 14. Production Potential (no geo extrapolation)

**MultiCodes:** IDENTITY_CORE, SUPPLIER_CROSSWALK, DEDUPLICATION  

**MHG:** ROOM_COUNT_SOURCE, RICH_CONTENT_SOURCE, GEO_SOURCE (via city/country + MultiCodes geo), CONTACT_SOURCE  

```text
IF_PRODUCTION_MHG_CALA_COVERAGE_IS_SUFFICIENT:
MHG could become a candidate primary/fallback room-count provider.
```

Dealality gap ~5,765 missing room counts — recovery rate unknown until production entitlement.

Supplier crosswalk can reduce Dealality↔bedbank matching **if** production includes Hotelbeds/Booking/Expedia codes (not present in this TEST sample).

`GIATA_ID_FORMAT_CONSISTENT: YES` (numeric across Drive / MultiCodes / MHG).

## 15. Highest-Value Next Step

```text
REQUEST_PRODUCTION_GIATA_ENTITLEMENT
```

(Not executed.) TEST schema value is proven; geographic/CALA usefulness requires production coverage.

---

### Existing audit (unchanged)

`GIATA_TEST_PRODUCTS_EXISTING_CAPABILITY_AUDIT` — Drive client/adapter ALREADY_EXISTS; MultiCodes/MHG clients MISSING; XML parsers NEEDS_EXTENSION.

### API / quota (this run)

| Product | calls | successes | errors | avg latency |
| --- | ---: | ---: | ---: | ---: |
| MultiCodes | 16 | 16 | 0 | ~145 ms |
| MHG | 62 | 58 | 4 | ~50 ms |

No rate-limit headers observed.

Artifacts: `reports/hotel-intelligence/giata-test-products-validation-v1/`  
Script: `node scripts/hotel-intelligence-giata-test-products-validation.mjs`
