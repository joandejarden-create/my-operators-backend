# CALA Parent × Country Adapter Matrix

Companion to multi-parent discovery adapters.

**Machine JSON:** `reports/research-engine-v2/production-census-cala-parent-country-adapter-matrix.json`  
**Coverage module:** `lib/research-engine-v2/production-census-cala-region-config.js` (v3)

## Priority countries (all supported × 4 parents)

Mexico · Dominican Republic · Costa Rica · Colombia · Panama → **20 / 20 supported**

| Parent | Official discovery pattern |
| --- | --- |
| Marriott | `/en-us/hotel-sitemap/{country}-hotel-sitemap` |
| IHG | `/destinations/us/en/{country}-hotels` |
| Hilton | `/en/locations/{country}/` (Mexico: brand subpages) |
| Choice | `/en-uk/{country}/regional-hotels` |

## Partial / needs_adapter (non-priority examples)

- Choice Jamaica / Bolivia / USVI — regional JSON empty
