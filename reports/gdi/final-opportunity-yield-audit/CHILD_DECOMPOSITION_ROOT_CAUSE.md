# Child Decomposition Root Cause

## Intended path
Demand Generator → ten-bases / participant decomposers → child entities → research leads → complete packets → candidates → canonical opportunities

## Actual path today
1. YOTEL 10 generators registered in `demand-campaigns.json` via `buildYotelTenGeneratorCampaigns` / `upsertDemandCampaigns`.
2. UI/API expose campaigns via `listVisibleDemandCampaigns`.
3. `runGroupDemandResearch` (`research-orchestrator.js`) does **not** call `runTenBasesForHotel` or `decomposePublishedEventDemand`.
4. Ten-bases decomposers exist only under `lib/group-demand-intelligence/ten-bases-of-demand-v1/` and are invoked by **report script** `scripts/gdi-ten-bases-of-demand-v1-2026-10-04.mjs`.
5. Campaign seed hardcodes `childDecompositionState: CHILD_DECOMPOSITION_NOT_YET_RUN`.
6. No scheduler / eligibility gate even attempts decomposition for these campaign IDs.

## Classification
**NOT INVOKED / NOT CONNECTED TO CURRENT-CYCLE ORCHESTRATOR** (report-only decomposers).

Not blocked by readiness thresholds. Not a missing Airtable write of children that already ran — decomposition never ran for these 10 campaign IDs.

## Exact root cause
`demand-campaigns` visibility layer is disconnected from `ten-bases-of-demand-v1` decomposers and from `runGroupDemandResearch`.
