# YOTEL Campaign→Child Orchestration P0 — Founder Report

## Verdict
**P0_ORCHESTRATION_WIRED_YOTEL_CHILDREN_PRODUCED**

`runDemandCampaignDecomposition` is wired. All **10** YOTEL campaigns ran.
**31** child entities / **28** research leads from evidence packs + continuity (noise quarantined: 46).

## Campaign status
| Campaign | Status | Children | Leads | Strict ready | Watch |
|---|---|---|---|---|---|
| ycamp_aidex_geneva_2026 | COMPLETE | 3 | 3 | 1 | 3 |
| ycamp_geneva_health_forum_2026 | COMPLETE | 3 | 3 | 0 | 3 |
| ycamp_chi_geneva_centennial_2026 | COMPLETE | 3 | 3 | 0 | 3 |
| ycamp_who_eb_160_2027 | PUBLIC_DATA_CEILING | 1 | 0 | 0 | 0 |
| ycamp_art_geneve_2027 | COMPLETE | 3 | 3 | 0 | 3 |
| ycamp_watches_wonders_2027 | COMPLETE | 2 | 2 | 0 | 3 |
| ycamp_setac_europe_37_2027 | COMPLETE | 2 | 2 | 0 | 4 |
| ycamp_wha_80_2027 | PUBLIC_DATA_CEILING | 1 | 0 | 0 | 2 |
| ycamp_ecosoc_has_2027 | COMPLETE | 1 | 1 | 0 | 2 |
| ycamp_ai_for_good_2027 | COMPLETE | 12 | 11 | 0 | 14 |

## Surfaces
- Customer-facing opportunities: **1** (AidEx)
- AI for Good: 10 reused + 1 continuity/org row; 0 ready; 11 watch-valid
- Four lodging-supported AI for Good children top blocker: **who_research_not_attempted** (packet lodging still incomplete)

## Implementation
- `lib/group-demand-intelligence/demand-campaigns/campaign-decomposition-orchestrator.js`
- `base-router.js` · `yotel-campaign-evidence-packs.js`
- UI campaign cards show child / qualification / ready / watch counts

## Guardrails
Thresholds unchanged. No speculative children. Jev advisory-only. Orphans: NO. ADP/share tokens: NO.
