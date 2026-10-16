# Segment depth rules (evidence-based — no card quotas)

Source: `DEPTH_INADEQUACY_RULES` + `E2E_SEGMENT_TAXONOMY` in `full-e2e-contract-v1.js`.

## Taxonomy (primary segments)

- Associations / conferences / government  
- Corporate meetings / training / project teams  
- Scaling companies / emerging corporate accounts  
- Local meetings and catering (incl. subsegments)  
- Professional planners / DMCs / intermediaries (**coverage axis** — association contacts do not satisfy)  
- Private-event venue lodging / referral  
- Entertainment / production  
- Exhibitor / sponsor teams  
- Sports  
- Medical / academic  
- Educational / tour / specialist (where applicable)

Applicability derives from hotel profile, catchment, and supported demand use cases.  
`NOT_APPLICABLE` requires a recorded rationale and **cannot** use: research inconvenient, provider returned nothing useful, budget exhausted, no cards published yet.

## Depth inadequacy (examples)

| Rule id | Rule |
|---------|------|
| `single_broad_competing_hotels` | A single broad query returning competing hotels does not complete buyer discovery |
| `directory_only_unverified` | Directory-only results require source checking before qualification |
| `assoc_contact_not_planner_coverage` | An association contact does not establish planner coverage |
| `venue_list_not_activity` | A venue list ≠ venue activity / lodging-referral / hotel fit |
| `funding_alone_not_scaling` | Funding alone does not qualify a scaling company |
| `local_sports_calendar_not_travel` | A local sports calendar does not establish traveling demand |
| `plan_only_not_execution` | A brief or query plan is not execution |

## Brief ≠ execution

Each applicable segment needs a hotel-specific brief (geography, buyer categories, horizon, corpus/exclusions, qualification evidence, contact roles, planned depth, follow-up, budget/stop). Existing research may satisfy declared scope only when provenance, freshness, breadth, and evidence meet the contract; otherwise research the gaps. Do not require new paid calls when current evidence already satisfies scope.

Zero qualified prospects can be a valid researched outcome. Zero useful results from an inadequate search cannot be labeled completed coverage.

Freeze scope before execution. Scope changes require recorded reason + before/after.
