# Multi-Parent CALA Discovery Adapters

> Autopilot `source_discovery` for Marriott, IHG, Hilton, and Choice across priority CALA countries.  
> Full report: `reports/research-engine-v2/production-census-multi-parent-cala-discovery-adapters.json`

## Status

`production_census_multi_parent_cala_discovery_adapters_ready_for_insert_review`

## Production target

Hotel Property Census · `tbl9aY5ijiuIzzWam` · Deal Capture Platform

## Wired adapters

| Parent | Module |
| --- | --- |
| Marriott | `census-autopilot-marriott-discovery-adapter.js` |
| IHG | `census-autopilot-ihg-cala-discovery-adapter.js` |
| Hilton | `census-autopilot-hilton-cala-discovery-adapter.js` |
| Choice | `census-autopilot-choice-cala-discovery-adapter.js` |

## Controlled discovery (no apply)

| Parent | Discovered | Existing | New inserts (bundle) |
| --- | ---: | ---: | ---: |
| Marriott | 398 | 301 | 93 |
| IHG | 231 | 194 | 37 |
| Hilton | 158 | 102 | 56 |
| Choice | 78 | 50 | 28 |
| Active Brand Setup | 353 | 262 | 91 |

## Do not

- Apply without founder confirms  
- Use Webhound / OTAs / VIC as Census SoT  
- Gate Marriott listing on HQV  
- Use Choice regional `placeId` URLs as Address Source URL  

## Next

Founder insert review → optional Choice sitemap-only country follow-up.
