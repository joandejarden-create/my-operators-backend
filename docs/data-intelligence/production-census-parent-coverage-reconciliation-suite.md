# Production Census Parent Coverage Reconciliation Suite

**Status:** `production_census_parent_coverage_reconciliation_suite_partial_steward_remaining`
**Region:** CALA
**Write target:** Hotel Property Census (`tbl9aY5ijiuIzzWam`)
**Generated:** 2026-08-06T23:00:32.870Z

## Overall

| Metric | Value |
| --- | ---: |
| Hotel Property Census before (suite start) | 942 |
| Hotel Property Census after | 1077 |
| Suite-wave inserts (942→1077) | 135 |
| Total High inserts (incl. Marriott wave) | 167 |
| Missing High remaining | 0 |
| Steward remaining | 25 |
| Parents complete | Hilton, Choice, Wyndham, Preferred |
| Parents partial | Marriott, IHG, Accor |
| Parents blocked | — |

## By parent

| Parent | Official | Exact | Inserted | Missing High | Steward | Dup | Source blocked | Status |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| Marriott | 397 | 376 | 32 | 0 | 21 | 0 | 0 | partial_missing_remaining |
| Hilton | 158 | 158 | 16 | 0 | 0 | 0 | 0 | complete |
| IHG | 231 | 228 | 34 | 0 | 3 | 0 | 0 | partial_missing_remaining |
| Choice | 78 | 78 | 0 | 0 | 0 | 0 | 0 | complete |
| Accor | 77 | 76 | 27 | 0 | 1 | 0 | 0 | partial_missing_remaining |
| Wyndham | 80 | 80 | 58 | 0 | 0 | 0 | 0 | complete |
| Preferred | 61 | 61 | 0 | 0 | 0 | 0 | 0 | complete |

## Official source patterns

- **Marriott:** marriott_cala_country_sitemap (official country hotel-sitemaps / MARSHA)
- **Hilton:** hilton_cala_locations (official Hilton locations directory)
- **IHG:** ihg_cala_destination_directory (official IHG destination directory)
- **Choice:** choice_cala_regional (official Choice regional hotel cards)
- **Accor:** accor_cala_continent_catalog (official Accor continent catalog)
- **Wyndham:** wyndham_cala_property_sitemap (official Wyndham property sitemap)
- **Preferred:** preferred_directory (official Preferred Hotels directory)

## Largest brand gaps (remaining steward / missing)

### Marriott
- Autograph Collection: official=23 census=19 missing=4 (high=0, steward=4) → steward_review
- Marriott Bonvoy — Brand Unconfirmed: official=9 census=4 missing=4 (high=0, steward=4) → steward_review
- Design Hotels: official=29 census=26 missing=3 (high=0, steward=3) → steward_review
- The Luxury Collection: official=8 census=6 missing=2 (high=0, steward=2) → steward_review
- JW Marriott: official=12 census=10 missing=2 (high=0, steward=2) → steward_review

### IHG
- holidayinn: official=67 census=7 missing=2 (high=0, steward=2) → steward_review
- crowneplaza: official=13 census=4 missing=1 (high=0, steward=1) → steward_review

### Accor
- Sofitel: official=3 census=2 missing=1 (high=0, steward=1) → steward_review

## Safety

- Hotel Property Census only
- Brand Setup / Brand Explorer untouched
- old Census / VIC not write targets
- No address / lat-long / phone / rooms on coverage inserts
- No owner/operator/date / Recent Momentum / Company Validated
- No fuzzy or hotel-name-only inserts

## Notes

- Marriott High inserts (32) applied in prior wave; suite baseline Census=942.
- Remaining-parent suite wave inserted Hilton+IHG+Accor+Wyndham = 135 (942→1077).
- Choice and Preferred had 0 High missing at controlled time.
- Steward remaining: Marriott 21, IHG 3, Accor 1 — no fuzzy/name-only inserts.
