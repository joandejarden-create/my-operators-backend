# Design Hotels Remediation Plan (v36C)

- Draft state: **draft_applied_with_defects**
- Calibrated score: **43/100** (founder_review_required)
- Active approval: **NOT RECOMMENDED**

## Known issue areas
### design-hotels:property_example_render_not_ready:footprint.openings
- Severity: high
- Tab: Footprint & Growth | Slot: footprint.openings | Row: —
- Root cause: 2/3 row-level image match
- Fix: Match footprint.openings row by property name; attach hotel image per catalog entry
- Stage: visual_asset_materialization

### design-hotels:wrong_model_language:global
- Severity: high
- Tab: Unknown | Slot: — | Row: —
- Root cause: 3 rows
- Fix: Rewrite with affiliation_curation_platform framing; block franchise flag language
- Stage: copy_governance

## Before active approval
- Clear modal placeholders on footprint.openings
- Property example row-level image match (3/3 render-ready)
- Standards table owner-ready with affiliation-safe governance
- Loyalty KPI/proof/watchouts coverage
- Economics/fee affiliation fit (no FDD templates)
- Pass founder visual review after remediation apply