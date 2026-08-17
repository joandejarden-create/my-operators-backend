# CALA Parent × Country Adapter Matrix

**Version:** `production-census-cala-region-config-v3`  
**Priority countries:** Mexico · Dominican Republic · Costa Rica · Colombia · Panama

## Summary

| Readiness | Count |
| --- | ---: |
| supported | 20 |
| needs_adapter | 0 (priority set) |
| blocked | 0 |

## Matrix (priority)

| Parent | Mexico | DR | Costa Rica | Colombia | Panama |
| --- | --- | --- | --- | --- | --- |
| Marriott | supported | supported | supported | supported | supported |
| IHG | supported | supported | supported | supported | supported |
| Hilton | supported | supported | supported | supported | supported |
| Choice | supported | supported | supported | supported | supported |

## Source URL patterns

| Parent | Pattern |
| --- | --- |
| Marriott | `marriott.com/en-us/hotel-sitemap/{country}-hotel-sitemap` |
| IHG | `ihg.com/destinations/us/en/{country}-hotels` |
| Hilton | `hilton.com/en/locations/{country}/` (+ Mexico brand subpages) |
| Choice | `choicehotels.com/en-uk/{country}/regional-hotels` |

## Known gaps (outside priority)

- Choice: Jamaica, Bolivia, US Virgin Islands — regional Hotel JSON empty (sitemap-only)
- Broader CALA radar countries: URL patterns often exist but not live-reprobed in this sprint

Machine JSON: `reports/research-engine-v2/production-census-cala-parent-country-adapter-matrix.json`
