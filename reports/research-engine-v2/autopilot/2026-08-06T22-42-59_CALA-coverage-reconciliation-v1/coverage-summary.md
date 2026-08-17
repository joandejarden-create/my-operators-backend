# Production Census Coverage Reconciliation v1

**Status:** `production_census_coverage_reconciliation_v1_complete`
**Objective:** `coverage-reconciliation-v1`
**Region:** CALA
**Parent company:** Marriott
**Brand filter:** Sheraton
**Write target:** Hotel Property Census (`tbl9aY5ijiuIzzWam`)
**Airtable writes:** no (controlled)

## Summary

- Official inventory count: **9**
- Census inventory count (scoped brand): **9**
- Exact matches: **9**
- Probable matches: **0**
- Missing High: **0**
- Missing steward: **0**
- Duplicate risks: **0**
- Source blocked: **0**
- Source insufficient: **0**
- Inserted: **0**
- Stewarded (held): **0**

## Official sources used

- Marriott: Official country hotel-sitemaps (MARSHA + property URL); HQV not used for discovery
- VIC_evidence: official_family_directory_adapter
- Sheraton destination page (secondary / JS shell — not sole SoT): https://sheraton.marriott.com/es-XM/destinos-hotel/
- Primary Marriott SoT: country hotel-sitemaps (marriott_country_hotel_sitemap)

## Brand rollup

| Brand | Official | Census | Missing | Coverage % | Action |
| --- | ---: | ---: | ---: | ---: | --- |
| Sheraton | 9 | 9 | 0 | 100 | coverage_complete |

## Missing hotels (sample)


## Safety

- Hotel Property Census only
- Brand Setup / Brand Explorer untouched
- No owner/operator/date / Recent Momentum / Company Validated writes
- No fuzzy auto-insert; no hotel-name-only insert
- No lat/long/phone/rooms on coverage inserts
