# SerpApi Provider Validation

`DEALALITY_SERPAPI_PROVIDER_VALIDATION_COMPLETE`

Frozen sample: `hotel-intelligence-cala-validation-v1` (400 hotels).  
Safety: `ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES=0` — 0 Airtable / Census / Brand Explorer writes.

## 1. Existing SerpApi Status

```text
Previously connected: yes (research-engine-v2 + census-autopilot)
Existing implementation: lib/research-engine-v2/providers/serpapi-google-hotels/*
Existing MCP: no (no Cursor SerpApi MCP server)
Existing credentials configuration: SERPAPI_KEY (.env / .env.example)
Previously used by Hotel Intelligence MCP: no (wired in this task)
Prior census usage: serpapi-benchmark-v1; autopilot cache/eligibility/rights; gap-closure backfills
Cursor MCP SerpApi: none
```

## 2. Capability Matrix (Google Hotels + existing client)

| Field | Classification |
| --- | --- |
| hotel name | SUPPORTED |
| address | SUPPORTED |
| city / country | SUPPORTED_INDIRECTLY (parsed from address) |
| latitude / longitude | SUPPORTED |
| phone | SUPPORTED |
| website | SUPPORTED_INDIRECTLY (non-Google link when present) |
| Google Hotels property_token | SUPPORTED |
| Google Place ID / data_id / CID | NOT_SUPPORTED in current client (Maps engine not wired) |
| hotel class / star | SUPPORTED / SUPPORTED_INDIRECTLY |
| review score / count | SUPPORTED |
| amenities / description / photos | SUPPORTED / SUPPORTED_INDIRECTLY |
| check-in/out | SUPPORTED |
| brand / parent company | NOT_SUPPORTED |
| **total property room count / keys** | **NOT_SUPPORTED** |
| room types | SUPPORTED_INDIRECTLY (≠ keys) |
| rates / availability | SUPPORTED / SUPPORTED_INDIRECTLY |
| nearby places | SUPPORTED_INDIRECTLY (details) |

`SERPAPI_TOTAL_PROPERTY_ROOM_COUNT: NOT_SUPPORTED`

### Endpoint roles

| Endpoint | Census value per request |
| --- | --- |
| **google_hotels search** | Best first call — often returns direct_property with name/address/GPS/phone/token |
| google_hotels property_details | Second call when search list is thin |
| google_maps | Strong for place_id/CID/phone/website in docs — **not used** this run (avoid duplicate spend) |

## 3. Room Count Verdict

**SERPAPI_NOT_A_ROOM_COUNT_SOURCE**

## 4. MCP Integration

```text
Provider added: serpapi
Key file: lib/hotel-intelligence/providers/serpapi.js
Also: registry, confidence, map_hotel_intelligence_fields, service hotel_enrich, MCP meta
Tests: test:hotel-intelligence-serpapi
Env: HOTEL_INTELLIGENCE_SERPAPI=0 (default off)
```

## 5. 10-Hotel Controlled Test

| Metric | Result |
| --- | ---: |
| Lookups | 10 |
| Exact/High | 3 |
| Failed API | 0 |
| Avg latency | ~2.6s |
| Account delta | 9 searches |
| Fields when matched | address, coords, phone, website |

**Proceeded to full 400** — no unexpected quota/cost behavior.

## 6. Frozen 400-Hotel CALA Test

| Metric | Result |
| --- | ---: |
| Attempted | 400/400 |
| Exact/High matches | 129 (32.3%) |
| property_tokens linked | 129 |
| Account search delta | 380 |
| Runtime | ~18 min |

## 7. Field Recovery (full 400)

| Field | Missing Before | Candidate Found | High Conf | Conflict | Still Missing | Recovery % |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| address_line_1 | 299 | 129 | 83 | 25 | 216 | 27.8% |
| latitude | 351 | 129 | 106 | 0 | 245 | 30.2% |
| longitude | 351 | 129 | 106 | 0 | 245 | 30.2% |
| room_count | 375 | 0 | 0 | 0 | 375 | 0% |
| brand_name | 104 | 0 | 0 | 0 | 104 | 0% |
| parent_company_name | 95 | 0 | 0 | 0 | 95 | 0% |
| website | 38 | 126 | 0 | 36 | 33 | 13.2% |
| phone | 295 | 123 | 4 | 16 | 216 | 26.8% |

Note: phone base confidence is 0.82 (probable), so “high conf” understates useful phone recovery (79 phones recovered from missing).

## 8. Coordinate Recovery

```text
sample_missing_coords: 351
coords_found: 106 (high-confidence missing→candidate)
coords_still_missing: 245
coords_conflicts (distance buckets on overlap): see 05-summary.json
```

## 9. Room Count Recovery

```text
SERPAPI_NOT_A_ROOM_COUNT_SOURCE — 0 candidates
```

## 10. Cross-Provider Linkage

```text
Dealality ↔ SerpApi property_token: 129
Google Place ID / CID / data_id: 0 (Maps not wired)
Dealality ↔ Hotelbeds: census-linked codes only (this run did not call HBX)
```

## 11. Provider Efficiency

```text
plan: Production Plan
monthly_search_allowance: 15000
remaining_at_start: ~13499
account_delta: 380 searches for 400 hotels (~0.95/hotel; some cache)
rate_limit: 3000/hour
estimated_calls_per_hotel: ~1 (details rarely needed when direct_property)
scale 5956 hotels: ~5600–9000 searches (within monthly plan if paced)
```

## 12. BEST_PROVIDER_BY_FIELD

```text
hotel_identity: dealality_census → fallback serpapi, hotelbeds
address: serpapi → fallback hotelbeds, dealality_census
coordinates: serpapi → fallback hotelbeds, dealality_census
room_count: hotelbeds → fallback official_site, dealality_census
brand: dealality_census → fallback brand_directory
parent_company: dealality_census → fallback brand_directory
phone: serpapi → fallback hotelbeds, dealality_census
website: dealality_census → fallback serpapi, hotelbeds
rates: hotelbeds → fallback serpapi, hotelapi_co_free
```

## 13. Provider Comparison

| Provider | Identity | Address | Coordinates | Total Rooms | Brand | Phone | Website | Rates | External IDs | Census Role |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Dealality Census | Strong | Partial | Partial | Partial | Partial | Partial | Partial | — | dhl_/rec | PRIMARY |
| Hotelbeds | Strong | Strong* | Strong* | Strong* | Limited | Limited | Limited | Yes | HBX codes | ROOM_COUNT + content |
| StayingAPI | Weak (0 match) | Doc yes | Doc yes | No | No | No | Indirect | Indirect | Booking ID | NOT_CURRENTLY_USEFUL |
| SerpApi | Good (32% Exact/High) | Strong when matched | Strong when matched | No | No | Strong when matched | Partial | Yes | property_token | GEO/ADDRESS/PHONE/ID |
| HotelAPI.co Free | Name+ID | No | No | No | No | No | No | Yes | hotelId | RATE only / LOW |

\*when LIVE content quota available

## 14. Safety

```text
Airtable writes: 0
Census writes: 0
Brand Explorer writes: 0
Automatic merges: 0
Migrations: 0
Secrets exposed: no
```

## 15. Recommendation

**USE_SERPAPI_AS_COMPLEMENTARY_PROVIDER**

Own: identity corroboration, address, coordinates, phone, Google Hotels `property_token`.  
Do not own: Rooms/Keys, brand, parent company.

## 16. Highest-Value Next Step

**Enable SerpApi complementary enrich for census gaps (geo/address/phone) behind `HOTEL_INTELLIGENCE_SERPAPI=1` with evidence-only staging; keep Hotelbeds LIVE as the rooms path — do not auto-accept or write Airtable yet.**
