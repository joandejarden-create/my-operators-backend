# Official Parent Level 2 Completion v1

**Status:** `production_census_official_parent_level_2_completion_v1_partial_source_remaining`
**Objective:** `official-parent-level-2-completion-v1`
**Write target:** Hotel Property Census (`tbl9aY5ijiuIzzWam`)
**Airtable writes:** yes
**Brand Setup writes:** false
**Brand Explorer writes:** false

## Review classification

- Governance review = product/public approval (Census Only / Hold / non-active brand)
- Data quality review = dirty labels, unresolved codes, unsupported, conflicts, duplicates
- Governance-only review does **not** block Level 2 enrichment

## Before / After

| Metric | Before | After |
| --- | ---: | ---: |
| Total records | 1205 | 1224 |
| Clean Core | 943 | 1003 |
| active_brand_setup | 710 | 711 |
| evidence_backed_non_active_brand | 384 | 384 |
| Governance Review Required | 495 | 513 |
| Data Quality Review Required | 111 | 129 |
| Human Review Required (checkbox) | 554 | 113 |
| Public Hold | 384 | 384 |
| Radar Hold | 400 | 400 |
| Level 2 eligible | 1023 | 1023 |
| Address complete | 311 | 378 |
| Address Confidence High | 260 | 327 |
| Address Source URL | 261 | 328 |
| Lat/Long complete | 366 | 374 |
| Phone complete | 299 | 350 |
| Rooms complete | 56 | 116 |
| Complete Census v1 | 14 | 14 |
| Census Complete | 218 | 276 |
| Map Ready | 268 | 276 |
| Contact Ready | 299 | 350 |
| Size Ready | 56 | 116 |
| Needs Governance Approval | 495 | 513 |
| Needs Data Steward Review | 111 | 129 |

## Mission writes

- Reclassification patches: 554
- Level 2 updates: 60
- Records updated (approx): 614
- Bot-blocked: 0
- Source-insufficient: 0
- Safety stops: none

## Chains

- cala-census-completion-v1: production_census_cala_completion_v1_partial_source_remaining
- source-confirmed-census-v2: production_census_source_confirmed_census_v2_partial_steward_remaining

## Hard constraints

- Brand Setup / Brand Explorer untouched
- Non-active brands remain Public/Radar Hold
- No weak address / unofficial phone / room inference
- No Mapbox on dirty identity

## Notes

- **HR separation worked:** checkbox 554 → 113 after clearing governance-only review; Public/Radar Hold unchanged (384 / 400).
- **Level 2 did not treat governance Holds as dirty:** evidence-backed non-active stayed Census Only + Hold and remained Level 2 eligible when Clean Core.
- **Census total 1,205 → 1,224:** growth occurred during chained `cala-census-completion-v1` (not during reclassify / Level 2 extraction). Parent mission used `--cleanup-existing-only`; chained cala still performed restricted coverage activity — review if insert-free chaining is required next pass.
- **Remaining gap:** official source adapters still leave address/phone/rooms incomplete vs Clean Core; status is partial source remaining (not a safety stop).
