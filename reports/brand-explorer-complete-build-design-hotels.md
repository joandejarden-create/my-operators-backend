# Brand Explorer Complete Build — Design Hotels

- Slug: `design-hotels`
- Record: `rec02zPClpWUTCyXM`
- Input target: `design-hotels`
- Resolution source: **expansion_backlog**
- Parent company: Marriott International, Inc.
- Target quality: **active-profile**
- Readiness band: **blocked**
- Ready for active profile: **no**
- Company Validated untouched: **yes**

## Required sections
- Score: **38**
- Ready: **no**
- Sections: 3/8

## Final QA
- Status: **blocked** (62)

## Visual QA
- Score: **56**
- Defects: 5
- Next batch: **v24C_source_evidence_work**

## Governance
- Governed platform ready: **yes**
- Sources: 10 (10 approved)
- Explorer facts: 0 (0 pending)

## Blockers
- [data] Openings / Examples / Properties: Insufficient complete openings rows.
- [data] Recent Momentum: No minimum set of dated/source-backed momentum rows.
- [founder_legal_review] Standard Detail / Where Available: No approved external-display-safe standards owner table package.
- [source] Loyalty Program: Required loyalty mechanics/proof coverage incomplete.
- [data] Geographic Footprint: Regional copy still template-thin.
- [frontend] Key Watchouts: undefined
- [frontend] Standard Detail, Where Available: undefined
- [frontend] Standard Detail, Where Available: undefined

## Next writer
- **row creation writer**

## Apply safety
- Safe for apply-approved: **no**
- block: 2 critical Final QA defect(s) remain
- block: Required sections are not ready
- block: Human/founder/legal review is required

## Commands
```bash
npm run brand-explorer-complete-build -- --brand design-hotels --dry-run --target-quality active-profile
```

### Staged apply
- Standard Detail / Where Available: `npm run brand-explorer-tribute-standard-detail-table-writer -- --brand design-hotels --dry-run`
- Geographic Footprint: `npm run brand-explorer-display-content-completion-writer -- --brand design-hotels --dry-run`