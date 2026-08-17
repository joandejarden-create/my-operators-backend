# Brand Explorer Radisson Pending Fact Stewardship Writer v28G

- Generated: 2026-07-10T07:04:34.744Z
- Brand: **Radisson by Choice** (`recywbx1YQSTCPqW1`)
- v28G exists: **yes**
- Mode: **dry-run**
- Pending facts before: **0**
- Pending facts after (projected): **0**
- Airtable modified: **no**
- Company Validated untouched: **yes**
- Dry-run clean: **no**

## Fact diagnosis
### be.footprint.geoIntro (`rec1F8M6YcWa2Lc6g`)
- Classification: **not_pending**
- Proposed action: **none**
- Rationale: Review status is Rejected — idempotent skip
- Current value: `Americas as of September 30, 2024.`
- Presentation slot: `footprint.geo_intro` (thin/missing)
- Presentation excerpt: Core Radisson under Choice Hotels is positioned as a global upscale flag with visible CALA momentum—especially Mexico, Central America, Panama, and the Caribbean—alongside airport, urban conversion, a
- Source: `recdOL9QhOIrAxYRP` (approved for Explorer: yes)
### be.overview.typicalUseCase (`recBJrPntYbQ5X0e0`)
- Classification: **not_pending**
- Proposed action: **none**
- Rationale: Review status is Rejected — idempotent skip
- Current value: `travelers worldwide.`
- Presentation slot: `overview.typical_use_case` (thin/missing)
- Presentation excerpt: Gateway cities, regional hubs, airport corridors, and secondary-market conversions where upscale affiliation and loyalty retail matter
- Source: `recLsN4M2G1z0rJBa` (approved for Explorer: yes)
### be.overview.whyValue (`reckqeeACfDkkv9A4`)
- Classification: **not_pending**
- Proposed action: **none**
- Rationale: Review status is Rejected — idempotent skip
- Current value: `value proposition.`
- Presentation slot: `overview.why_value` (thin/missing)
- Presentation excerpt: Brand on a Page — why this wins: We believe in the timelessness of hospitality. We exist to champion the enduring spirit of hospitality—innovating with purpose for evolving guest needs while holding f
- Source: `recLsN4M2G1z0rJBa` (approved for Explorer: yes)

## Summary
- Approve: **0**
- Reject / keep internal: **0**
- Hold pending: **3**

## Apply blockers
- hold:be.footprint.geoIntro
- hold:be.overview.typicalUseCase
- hold:be.overview.whyValue

## Exact apply command
```bash
npm run brand-explorer-radisson-pending-fact-stewardship-writer -- --brand radisson --apply --approve-brand-explorer-v28G-radisson-pending-fact-stewardship --founder-reviewed-radisson-fact-copy --confirm-no-company-validation-claim
```