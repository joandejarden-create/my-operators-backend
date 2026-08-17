# Production Census — Marriott Property URL + Unknown City Backfill

**Status:** `production_census_marriott_property_url_city_backfill_partial_remaining`  
**Generated:** 2026-08-07T16:40:54.628Z  
**Write target:** Deal Capture Platform → Hotel Property Census (`tbl9aY5ijiuIzzWam`)  
**Airtable writes:** no  
**Inserts:** 0  

## Before → After

| Metric | Before | After |
|--------|-------:|------:|
| Clean Core (all Census) | 1023 | 1023 |
| Below Clean Core | 201 | 201 |
| Marriott Unknown city | 3 | 3 |
| Coordinate blocked (dirty identity) | 17 | 17 |

## Applied

- Records fixed: 0
- Fields written: (none)
- Property URLs found: —
- Source URLs replaced: —
- Cities written: —
- Canonical written: —
- Stewarded: —

## Blocked source patterns

- marriott_country_hotel_sitemap_as_source_url
- akamai_blocked_property_page_json_ld
- hotel_name_only_city_inference_forbidden

## Examples

- (none)

## Next recommended action

Steward remaining Marriott Unknown cities without slug/IATA High city; optionally harvest property-page locality when Akamai allows. Keep address/Mapbox paused.
