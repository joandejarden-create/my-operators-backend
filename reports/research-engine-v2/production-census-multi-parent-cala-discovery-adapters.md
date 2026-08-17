# Multi-Parent CALA Discovery Adapters

**Status:** `production_census_multi_parent_cala_discovery_adapters_ready_for_insert_review`  
**Airtable writes:** false · **Webhound:** learning input only · **HQV for discovery:** false  
**Target:** Hotel Property Census (`tbl9aY5ijiuIzzWam`)

## Sprint outcome

All four parent families are wired into Autopilot `source_discovery` for priority CALA countries (Mexico, Dominican Republic, Costa Rica, Colombia, Panama).

| Parent | Adapter | Pattern |
| --- | --- | --- |
| Marriott | `census-autopilot-marriott-discovery-adapter.js` | Country hotel-sitemap |
| IHG | `census-autopilot-ihg-cala-discovery-adapter.js` | Destination `/…/{country}-hotels` |
| Hilton | `census-autopilot-hilton-cala-discovery-adapter.js` | Mexico brand pages + non-MX `locations/{country}/` |
| Choice | `census-autopilot-choice-cala-discovery-adapter.js` | Multi-country regional JSON-LD |

## Controlled runs (no apply)

| Run | Discovered | Existing | New | Insert bundle |
| --- | ---: | ---: | ---: | ---: |
| Marriott CALA | 398 | 301 | 93 | **93** |
| IHG CALA | 231 | 194 | 37 | **37** |
| Hilton CALA | 158 | 102 | 56 | **56** |
| Choice CALA | 78 | 50 | 28 | **28** |
| Active Brand Setup | 353 | 262 | 91 | **91** |

Parent-run insert total (if all applied separately): **214** High candidates (overlaps possible across Active Setup).

### Run folders

- Marriott: `autopilot/2026-08-05T23-51-33_CALA-source-discovery`
- IHG: `autopilot/2026-08-05T23-51-50_CALA-source-discovery`
- Hilton: `autopilot/2026-08-05T23-52-06_CALA-source-discovery`
- Choice: `autopilot/2026-08-05T23-52-31_CALA-source-discovery`
- Active Setup: `autopilot/2026-08-05T23-52-53_CALA-source-discovery`

## Matrix

Priority 5 × 4 parents = **20 supported** (see companion matrix docs).

Outside priority: Choice Jamaica/Bolivia/USVI remain sitemap-only (`needs_adapter`).

## Rules honored

- Hotel Property Census only; VIC evidence-only
- Brand Setup / Brand Explorer read-only / untouched
- No owner/operator/dates / Recent Momentum / Company Validated / Brand Verified
- No fuzzy auto-insert; approval-bundle-bound
- Choice regional URL ≠ Address Source URL (property-level only)
- Marriott HQV not required for listing
- Webhound not invoked

## Recommended next

1. Founder review Active Setup approval bundle (91) or per-parent bundles  
2. Apply only with confirm flags  
3. Optional: Choice sitemap-only country adapters later  

## Change impact

- **Impact:** Medium (discovery read-path + insert proposals)  
- **Rollback:** Flip family `ready: false` in coverage classifiers / revert Autopilot discovery branches  
- **Regression:** Mexico Hilton/Choice still work; Marriott Mexico 301 exact matches  
