# Marriott CALA Discovery Adapter (Autopilot)

> Listing discovery only. HQV GraphQL is enrichment-only and must not gate discovery.  
> Machine report: `reports/research-engine-v2/production-census-marriott-discovery-adapter.json`

## Status

`production_census_marriott_discovery_adapter_ready_for_insert_review`

## Production target

- Base: Deal Capture Platform  
- Table: Hotel Property Census  
- Table ID: `tbl9aY5ijiuIzzWam`

## Adapter

- Module: `lib/research-engine-v2/census-autopilot-marriott-discovery-adapter.js`
- Wired in: `census-autopilot-source-discovery.js`
- Coverage: `production-census-cala-region-config.js` (v2)

### Official source

`https://www.marriott.com/en-us/hotel-sitemap/{country-slug}-hotel-sitemap`

### Do not use

- Deprecated `*.sitemap-hotels.xml` (404)
- OTAs / Webhound as Census SoT
- VIC as write target
- HQV as discovery dependency

## Priority countries (supported)

Mexico · Dominican Republic · Costa Rica · Colombia · Panama

## Controlled run highlights

| Run | Discovered | Existing | New | Inserts (bundle) |
| --- | ---: | ---: | ---: | ---: |
| Marriott Mexico | 301 | 301 | 0 | 0 |
| Marriott CALA ×5 | 398 | 301 | 93 | 65 |
| Active Brand Setup | 290 | 262 | 28 | 28 |

No Airtable writes. Approval bundles only.

## Next

Founder insert review → then Hilton/Choice non-Mexico → IHG Autopilot wiring.
