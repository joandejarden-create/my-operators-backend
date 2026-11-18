# Demand qualification blockers

## Standards preserved

- `CONFIRMED_DEMAND` / `RELIABLE_INFERRED_DEMAND` may qualify.
- `SPECULATIVE_DEMAND` stays research continuation.
- No RFP / room-night volume requirement.
- No manufactured room-night estimates.
- Contact eligibility gates unchanged.

## Defect corrected

**Construction / GC project workforce** evidence (e.g. “general contractor … construction and staged opening/move-ins”) was failing `classifyDemandMechanismEvidence` as `generated_thesis_wording_is_not_independent_evidence` even when the same wording lived in independent `evidence[]` statements. Trigger regexes covered office openings and manufacturing but omitted construction GC / staged move-ins.

**Fix:** extend verified commercial trigger + inherent lodging mechanism + project-deployment hints; include `title` / `programmeName` in independent statement collection; hydrate `guestroomDemandConfidence` from demand thesis when eligibility already accepts RELIABLE/CONFIRMED rooms demand.

**Not weakened:** Speculative orgs without a verified local trigger still fail. Celebrity / unrelated names still rejected.

## Per-opportunity blocker diagnosis

| Org | Stored demand | After rescore | Root cause class | Precise blocker |
| --- | --- | --- | --- | --- |
| M.C. Dean, Inc. | RELIABLE_INFERRED_DEMAND / RESEARCH_CONTINUATION | RELIABLE_INFERRED_DEMAND / RESEARCH_CONTINUATION | GENUINE_GEOGRAPHY_MISMATCH | HOTEL_FIT_NOT_DEFENSIBLE_FOR_SALES_READY |
| True Anomaly, Inc. | RELIABLE_INFERRED_DEMAND / RESEARCH_CONTINUATION | RELIABLE_INFERRED_DEMAND / RESEARCH_CONTINUATION | GENUINE_GEOGRAPHY_MISMATCH | HOTEL_FIT_NOT_DEFENSIBLE_FOR_SALES_READY; MISSING_CONTACT_ROUTE |
| PCI Government Services | RELIABLE_INFERRED_DEMAND / RESEARCH_CONTINUATION | RELIABLE_INFERRED_DEMAND / RESEARCH_CONTINUATION | MISSING_EVIDENCE_OR_ROUTE | BUYER_RESPONSIBILITY_UNKNOWN; MISSING_CONTACT_ROUTE |
| GuidePoint Security | RELIABLE_INFERRED_DEMAND / SALES_READY | RELIABLE_INFERRED_DEMAND / SALES_READY | ELIGIBILITY_SALES_READY_DISPOSITION_HELD_WATCHLIST_BY_DESIGN | Commercial eligibility is SALES_READY, but V41 disposition reconcile keeps WATCHLIST (no automatic customer promotion). Publication review still blocked by copy |
| KnowBe4 | RELIABLE_INFERRED_DEMAND / RESEARCH_CONTINUATION | RELIABLE_INFERRED_DEMAND / RESEARCH_CONTINUATION | GENUINE_GEOGRAPHY_MISMATCH | HOTEL_FIT_NOT_DEFENSIBLE_FOR_SALES_READY |
| The Whiting-Turner Contracting Company | SPECULATIVE_DEMAND / RESEARCH_CONTINUATION | RELIABLE_INFERRED_DEMAND / SALES_READY | FALSE_NEGATIVE_FIXED_BY_DEMAND_SYNTHESIS | Commercial eligibility is SALES_READY, but V41 disposition reconcile keeps WATCHLIST (no automatic customer promotion). Publication review still blocked by copy |

## Category coverage supported by existing evidence

| Category | Supported among six? | Notes |
| --- | --- | --- |
| Group guestrooms | Partial | Inferred from HQ openings / project workforce — not confirmed blocks |
| Corporate travel | Partial | Facilities / travel managers (GuidePoint, M.C. Dean) |
| Meetings and training | Partial | Customer engagement / training language (GuidePoint, KnowBe4) |
| Project / workforce lodging | Yes (WT after fix) | Grandview Bethesda GC + move-ins |
| Extended stay | Weak | PCI relocation segment; opening may be past-closed |
| Catering / private events | Weak | Opening celebrations — not hotel catering confirmed |
| Preferred lodging partnerships | No | Not evidenced in these six |
