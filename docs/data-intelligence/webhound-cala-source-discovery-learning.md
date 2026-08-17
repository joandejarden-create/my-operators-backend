# Webhound CALA Source Discovery Learning

> Learning-only sidecar. **Not** a production Census population process.  
> Full machine report: `reports/research-engine-v2/webhound-cala-source-discovery-learning.json`

## Status

`webhound_cala_source_discovery_learning_complete_ready_for_adapter_build`

## Recommendation

**B — Build Marriott discovery adapter next** (wire existing country hotel-sitemap extractors into Autopilot `source_discovery`).

Webhound preferred IHG for enrichment friction; Dealality prioritizes Marriott listing wiring because extractors and live sitemap counts are already proven, and HQV is enrichment-only.

## Constraints honored

- No Airtable writes
- No Census inserts / enrichment apply
- No Brand Explorer / Brand Setup writes
- No old Census
- No owner/operator/developer/date / Recent Momentum / Company Validated / Brand Verified
- VIC = evidence only

## Session

- URL: https://webhound.ai/session/485549eb-4541-44fd-86dd-5c7919b839b0
- Budget: $5 · completed
- Companion probe: `reports/research-engine-v2/webhound-cala-source-discovery-code-probe.json`

## Official discovery sources (keep)

| Parent | Pattern |
| --- | --- |
| Marriott | `/en-us/hotel-sitemap/{country}-hotel-sitemap` + master hotel-sitemaps XML |
| IHG | `/destinations/us/en/{country}-hotels` + `/bin/sitemapindex.xml` hoteldetail XMLs |
| Hilton | `/en/locations/{country}/` (+ Mexico brand location pages already wired) |
| Choice | `/en-uk/{country}/regional-hotels` (+ Mexico placeId when required) |

## Do not implement

- Marriott deprecated `*.sitemap-hotels.xml` (404)
- OTA / Wikipedia / news as discovery SoT
- Webhound bulk population
- HQV as a gate for Marriott **listing** discovery

## Next adapter order (Dealality)

1. Marriott CALA country sitemap → Autopilot
2. Hilton + Choice non-Mexico country parameterization
3. IHG destination / hoteldetail wiring
4. Optional later Webhound: HQV signature harvest hardening only (5–10 cases)

## Data contract snapshot

- **Tables:** none written
- **Field maps:** none changed
- **Expected output:** learning JSON/MD + ledger entries only

## Regression checklist

- Confirm no apply scripts were run for this task
- Re-run `npm run test:census-autopilot` and `npm run dealality:batch-learning-audit` after ledger update
- Autopilot Mexico Hilton/Choice discovery behavior unchanged until a separate adapter-build task
