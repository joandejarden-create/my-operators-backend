# Bypass Audit

| File | Kind | Sample | Intentional? |
|------|------|--------|--------------|
| `lib/group-demand-intelligence/research-orchestrator.js` | YOTEL_HOTEL_ID | `recrPQcZg7SFARRb2` | YES |
| `lib/group-demand-intelligence/research-orchestrator.js` | YOTEL_SECOND_GEN_FLAG | `isYotel` | YES |
| `lib/group-demand-intelligence/demand-campaigns/campaign-decomposition-orchestrator.js` | YOTEL_SECOND_GEN_FLAG | `YOTEL_SECOND_GEN` | YES |
| `lib/group-demand-intelligence/demand-campaigns/yotel-second-generation-p0.js` | YOTEL_SECOND_GEN_FLAG | `YOTEL_SECOND_GEN` | YES |
| `lib/group-demand-intelligence/demand-campaigns/yotel-second-generation-p0.js` | NYC_LOGIC | `New York` | REVIEWED/FIXED_SHARED |
| `lib/group-demand-intelligence/demand-campaigns/yotel-ten-generators.js` | YOTEL_HOTEL_ID | `recrPQcZg7SFARRb2` | YES |
| `lib/group-demand-intelligence/demand-campaigns/yotel-ten-generators.js` | NYC_LOGIC | `New York` | REVIEWED/FIXED_SHARED |
| `lib/group-demand-intelligence/market-opportunity-graph/market-first-discovery-nyc-v1.js` | NYC_LOGIC | `NYC` | YES |
| `lib/group-demand-intelligence/market-opportunity-graph/market-first-discovery-nyc-v1.js` | ENGLISH_US_DEFAULT | `hl: "en"` | REVIEWED/FIXED_SHARED |
| `lib/group-demand-intelligence/hidden-demand/source-family-catalog-v2.js` | HOTEL_ID_ALLOWLIST | `hotelId === "rec` | REVIEWED/FIXED_SHARED |
| `lib/group-demand-intelligence/discovery-expansion-v3/market-languages.js` | YOTEL_HOTEL_ID | `recrPQcZg7SFARRb2` | YES |
| `lib/group-demand-intelligence/discovery-expansion-v3/market-languages.js` | NYC_LOGIC | `New York` | REVIEWED/FIXED_SHARED |

## Counts

- Total hits scanned: **12**
- YOTEL-only logic (content/flag, intentional): **6**
- NYC-only logic (market-first canary file): **4**
- English-default assumptions: **1**

## Fixes applied this pass

1. Research orchestrator campaign decomposition is **hotel-agnostic** (runs for any hotel with visible campaigns; YOTEL still ensures P0 campaigns).
2. Campaign `hotelContext` / thesis lines no longer hardcode YOTEL Geneva for non-YOTEL hotels.
3. Radisson added to `MARKET_LANGUAGE_PROFILES`; locale blocks on AC/RAD/YOTEL configs.
4. `buildGdiDiscoveryQueries` shared helper + ten-bases provenance enrichment.
5. Apify in Ten Bases is **opt-in only** (`enableApify === true`).
6. Multilingual buyer/lodging normalizers shared (no language sidecar).
