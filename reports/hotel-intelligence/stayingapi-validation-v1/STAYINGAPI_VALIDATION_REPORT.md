# StayingAPI Provider Validation

`DEALALITY_STAYINGAPI_PROVIDER_VALIDATION_COMPLETE`

Frozen sample: `hotel-intelligence-cala-validation-v1` (400 hotels).  
Safety: `ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES=0` — 0 Airtable / Census / Brand Explorer writes.

## 1. Existing StayingAPI Status

```text
Previously connected: yes (research-engine-v2)
Existing implementation: lib/research-engine-v2/providers/staying-api/*
Existing MCP: no (no Cursor StayingAPI MCP server)
Existing credentials configuration: STAYINGAPI_KEY (.env / .env.example); plan Free/Sandbox; rate_limit_rpm=30
Previously used by Hotel Intelligence MCP: no (wired in this task behind provider abstraction)
```

## 2. StayingAPI Capability Matrix

| Field | Classification |
| --- | --- |
| hotel name | SUPPORTED |
| alternate name | UNKNOWN |
| address | SUPPORTED |
| city | SUPPORTED |
| state/region | SUPPORTED |
| postal code | SUPPORTED |
| country | SUPPORTED |
| country code | UNKNOWN |
| latitude / longitude | SUPPORTED |
| room count / total keys | NOT_SUPPORTED |
| room types | SUPPORTED_INDIRECTLY (≠ total keys) |
| brand | NOT_SUPPORTED |
| parent company | NOT_SUPPORTED |
| star rating | SUPPORTED_INDIRECTLY |
| website | SUPPORTED_INDIRECTLY (listing URL) |
| phone | NOT_SUPPORTED |
| photos | SUPPORTED_INDIRECTLY |
| description | UNKNOWN |
| amenities | SUPPORTED |
| review score | SUPPORTED_INDIRECTLY |
| review count | UNKNOWN |
| Booking.com hotel ID | SUPPORTED |
| availability / room rates | SUPPORTED_INDIRECTLY |
| currency | UNKNOWN |
| check-in/out | SUPPORTED_INDIRECTLY |
| property type | SUPPORTED |
| status | NOT_SUPPORTED |

`STAYINGAPI_TOTAL_PROPERTY_ROOM_COUNT: NOT_SUPPORTED`  
Evidence: `STAYINGAPI_ROOMS_CAPABILITY=NOT_SUPPORTED` field-firewall; bedrooms/occupancy/room types must never map to Rooms/Keys.

## 3. Room Count Verdict

**STAYINGAPI_NOT_A_ROOM_COUNT_SOURCE**

## 4. MCP Integration

```text
Provider added: stayingapi (behind Hotel Intelligence provider interface)
Files changed:
  - lib/hotel-intelligence/providers/stayingapi.js
  - lib/hotel-intelligence/providers/* registry / orchestration / confidence
  - scripts/hotel-intelligence-stayingapi-validation.mjs
  - scripts/test-hotel-intelligence-stayingapi.mjs
  - package.json scripts
  - .env.example (HOTEL_INTELLIGENCE_STAYINGAPI=0)
Tools affected: hotel_enrich (multi-provider; hotelbeds failure isolated from stayingapi)
Tests: test:hotel-intelligence-stayingapi — pass
Regressions: none observed in adapter unit test; live enrich produced 0 census field recovery
```

## 5. 10-Hotel Controlled Test

| Metric | Value |
| --- | ---: |
| hotels | 10 |
| successful lookups (matched) | 0 |
| no_match | 2 |
| failed (mostly 429 rate_limited) | 8 |
| avg latency | ~48s |
| fields recovered | 0 |

**STOP signal honored for bulk:** Free-plan rate limits + credit ceiling prevented a clean full-400 enrich. Continued only under credit cap; still 0 exact/high matches.

## 6. Frozen 400-Hotel CALA Test

| Metric | Value |
| --- | ---: |
| sample | 400 (same frozen seed) |
| enrich_attempted | 15 (credit-capped) |
| enrich_matched_exact_or_high | 0 |
| credit_limited | true |
| credits charged (tracker) | 5 |
| ending credits available | 185 (started 225) |

## 7. Field Recovery

| Field | Missing Before | Candidate Found | High Confidence | Conflict | Still Missing | Recovery % |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| address_line_1 | 299 | 0 | 0 | 0 | 299 | 0% |
| latitude | 351 | 0 | 0 | 0 | 351 | 0% |
| longitude | 351 | 0 | 0 | 0 | 351 | 0% |
| room_count | 375 | 0 | 0 | 0 | 375 | 0% |
| brand_name | 104 | 0 | 0 | 0 | 104 | 0% |
| parent_company_name | 95 | 0 | 0 | 0 | 95 | 0% |
| website | 38 | 0 | 0 | 0 | 38 | 0% |
| phone | 295 | 0 | 0 | 0 | 295 | 0% |

## 8. Coordinate Recovery

```text
sample_missing_coords: 351
coords_found: 0
coords_high_confidence: 0
coords_conflicts: 0
coords_still_missing: 351
distance buckets (when both present): all 0
```

## 9. Room Count Recovery

```text
STAYINGAPI_ROOM_COUNT_TEST
total_property_room_count_supported: NOT_SUPPORTED
method: field-firewall + adapter always null room_count
hotels_tested: 15
room_counts_returned: 0
room_counts_high_confidence: 0
room_type_records_returned: n/a (not treated as keys)
availability_records_returned: n/a
```

## 10. Cross-Provider Linkage

```text
Dealality ↔ StayingAPI: 0
Dealality ↔ Booking.com: 0
Dealality ↔ Hotelbeds: 0 (this run; HBX not used)
Hotelbeds ↔ StayingAPI: 0
```

## 11. Provider Efficiency

```text
total StayingAPI calls: 15
successful calls: 4
failed calls: 10
rate-limited calls: present (429 on controlled set)
quota events: 2
average latency: ~43s
hotels enriched (matched): 0
calls per enriched hotel: n/a
scale: free 225 credits cannot cover 5,956 hotels at ~1 search/hotel
cost USD: UNKNOWN
```

## 12. HotelAPI.co Free Audit

Source: https://docs.hotelapi.co/free-hotel-api (docs only; not integrated)

`HOTELAPI_CO_FREE_CAPABILITY_MATRIX`

| Field | Classification |
| --- | --- |
| hotel name | SUPPORTED |
| persistent hotel ID | SUPPORTED (`hotelId`) |
| address | NOT_SUPPORTED (not in free response docs) |
| coordinates | NOT_SUPPORTED |
| room count / total keys | NOT_SUPPORTED |
| brand | NOT_SUPPORTED |
| website | NOT_SUPPORTED |
| phone | NOT_SUPPORTED |
| rates | SUPPORTED (vendor prices; random future dates) |
| availability | SUPPORTED_INDIRECTLY (price presence only; no pax/dates) |

`HOTELAPI_CO_FREE_CENSUS_VALUE: LOW`  
Reason: Free API returns city-limited hotel names + IDs + OTA rate snippets (30 hotels/city, no check-in control). Not a census fill source for rooms/geo/brand/contact.

## 13. Provider Comparison

| Provider | Identity | Address | Coordinates | Total Rooms | Brand | Phone | Website | Rates | Current Census Value |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Dealality Census | Strong (SoT) | Partial | Partial (5,120 missing live) | Partial (5,765 missing) | Partial | Partial | Partial | N/A | PRIMARY |
| Hotelbeds | Strong (codes linked) | Strong (content) | Strong | Strong (when content) | Limited | Limited | Limited | Yes | HIGH when LIVE quota |
| StayingAPI | Weak in this CALA run (0 match) | Documented yes / observed 0 | Documented yes / observed 0 | NOT_SUPPORTED | NOT_SUPPORTED | NOT_SUPPORTED | Indirect URL | Indirect | LOW for census now |
| HotelAPI.co Free | Name+ID only | No | No | No | No | No | No | Yes | LOW |

## 14. Safety

```text
Airtable writes: 0
Census writes: 0
Brand Explorer writes: 0
Automatic merges: 0
Migrations: 0
Secrets exposed: no
ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES=0
```

## 15. Recommendation

**DO_NOT_USE_STAYINGAPI_FOR_CENSUS**

Adapter remains available behind Hotel Intelligence for future paid-plan / better identity experiments, but free-plan CALA validation did not produce trusted census field recovery.

### Provider roles (evidence-based)

| Provider | Roles |
| --- | --- |
| Dealality Census | PRIMARY_IDENTITY, VALIDATION |
| Hotelbeds | ROOM_COUNT, GEO, ADDRESS, SECONDARY_IDENTITY, VALIDATION (when quota live) |
| StayingAPI | NOT_CURRENTLY_USEFUL for census (documented GEO/ADDRESS/Booking ID potential; live 0 recovery; not rooms) |
| HotelAPI.co Free | RATE_INTELLIGENCE only; NOT_CURRENTLY_USEFUL for census |

## 16. Highest-Value Next Step

**Restore LIVE Hotelbeds content quota and re-run frozen-sample `hotel_enrich` for rooms + coordinates + address evidence — do not burn StayingAPI free credits on bulk census.**
