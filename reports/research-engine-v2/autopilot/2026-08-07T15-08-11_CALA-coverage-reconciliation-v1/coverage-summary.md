# Production Census Coverage Reconciliation v1

**Status:** `production_census_coverage_reconciliation_v1_partial_missing_remaining`
**Objective:** `coverage-reconciliation-v1`
**Region:** CALA
**Parent company:** Marriott
**Brand filter:** Sheraton
**Write target:** Hotel Property Census (`tbl9aY5ijiuIzzWam`)
**Airtable writes:** no (controlled)

## Summary

- Official inventory count: **3**
- Census inventory count (scoped brand): **1**
- Exact matches: **1**
- Probable matches: **0**
- Missing High: **1**
- Missing steward: **1**
- Duplicate risks: **0**
- Source blocked: **0**
- Source insufficient: **0**
- Inserted: **0**
- Stewarded (held): **1**

## Official sources used

- Marriott: Official country hotel-sitemaps (MARSHA + property URL); HQV not used for discovery
- Sheraton destination page (secondary / JS shell — not sole SoT): https://sheraton.marriott.com/es-XM/destinos-hotel/
- Primary Marriott SoT: country hotel-sitemaps (marriott_country_hotel_sitemap)

## Brand rollup

| Brand | Official | Census | Missing | Coverage % | Action |
| --- | ---: | ---: | ---: | ---: | --- |
| Sheraton | 3 | 1 | 2 | 33.3 | insert_high_confidence_missing |

## Missing hotels (sample)

- **Sheraton Missing High** (Sheraton, Bogotá, Colombia) — `missing_high_confidence` — https://www.marriott.com/en-us/hotels/bogsh-sheraton-bogota/overview/ — MARSHA/code: BOGSH
- **Sheraton Weak** (Sheraton, ?, Mexico) — `missing_needs_steward` — no URL — MARSHA/code: WEAK1

## Safety

- Hotel Property Census only
- Brand Setup / Brand Explorer untouched
- No owner/operator/date / Recent Momentum / Company Validated writes
- No fuzzy auto-insert; no hotel-name-only insert
- No lat/long/phone/rooms on coverage inserts
