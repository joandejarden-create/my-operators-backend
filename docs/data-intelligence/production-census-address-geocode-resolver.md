# Production Census Address-First Geocode Resolver

**Status:** `production_census_address_geocode_needs_provider_or_terms_decision`  
**Contract:** `production-census-address-geocode-resolver-v1`  
**Generated:** 2026-08-05T14:29:23.100Z  
**Apply executed:** false (dry-run only)

## Executive summary

Address-first Census coordinate lane: confirm official hotel address, use official lat/lng when present, otherwise geocode property name + street address via `GEOCODING_PROVIDER` (mapbox | google | none). Webhound remains closed.

| Metric | Value |
| --- | ---: |
| Scanned | 666 |
| Active missing coordinates | 293 |
| Official address found | 78 |
| Official coordinates | 0 |
| Geocoder requests | 40 |
| Proposed | 34 |
| Provider | google |
| Est. cost (USD) | 0.2 |
| Webhound writes | 0 |
| Brand Explorer gates | PASS |

## Provider / terms

- Google Geocoding API: do not permanently store lat/long in Airtable unless Dealality's Maps Platform terms allow storage/display for this use case. Set GOOGLE_GEOCODE_STORAGE_TERMS_REVIEWED=1 only after legal/founder review.
- GOOGLE_GEOCODE_STORAGE_TERMS_REVIEWED is not set — dry-run may propose coords for review, but apply must not proceed until terms are confirmed.
- Do not use the public Nominatim/OSM endpoint for bulk production geocoding.

## Schema v1.1.3

Supporting provenance fields are **not** in the live schema. Recommended add (separate task): Address Confidence, Address Source URL, Coordinate Source Type, Coordinate Confidence, Geocode Provider, Geocode Method, Geocode Reviewed Date. Dry-run report captures these until approved.

## Commands

```bash
npm run research-engine-v2:production-census-address-geocode-resolver -- --dry-run
GEOCODING_PROVIDER=mapbox MAPBOX_PERMANENT_GEOCODING=1 npm run research-engine-v2:production-census-address-geocode-resolver -- --dry-run
```

## Next step

Founder decision: (1) enable Mapbox permanent geocoding (MAPBOX_ACCESS_TOKEN + MAPBOX_PERMANENT_GEOCODING=1), or (2) confirm Google storage terms (GOOGLE_GEOCODE_STORAGE_TERMS_REVIEWED=1) if Google remains the provider. Then re-run dry-run. Do not apply until terms are confirmed. Optional: approve schema v1.1.3 provenance fields.
