# Brand Explorer Final QA Auditor

- Generated: 2026-07-15T08:50:24.384Z
- Mode: **dry-run**
- Airtable modified: **no**

## Design Hotels (`rec02zPClpWUTCyXM`)

### Scores
- Required Section Readiness: **38**
- Presentation Quality: **56**
- Brand Carryover Risk: **100** (higher is better)
- Source Governance: **100**
- Visual Completeness: **56**
- Overall Active Profile Readiness: **blocked** (65)

### Defects by severity
- Critical: 2
- High: 1
- Medium: 2
- Low: 0

### Required sections
- **Openings / Examples / Properties**: rendering_but_weak (0/3)
- **Recent Momentum**: rendering_but_weak (0/3)
- **Portfolio Mix**: ready (3/3)
- **Portfolio Context**: ready (1/1)
- **Standard Detail / Where Available**: founder_legal_review_needed (3/1)
- **Demand Scenario View**: ready (3/3)
- **Loyalty Program**: source_approval_needed (1/5)
- **Geographic Footprint**: rendering_but_weak (3/5)

### Recommended fix batches
- v24C_source_evidence_work
- 1) row creation writer
- 2) source capture writer
- 3) standards founder-review package
- 4) fact approval writer
- 5) brand-explorer-tribute-geographic-footprint-refinement-writer

### Carryover findings
- none

## Next command
```bash
npm run brand-explorer-complete-build -- --brand design-hotels --dry-run --target-quality active-profile
```