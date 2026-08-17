# Brand Explorer Complete Build — avid hotels

- Slug: `avid-hotels`
- Record: `recoEarnE8T6sDjZq`
- Input target: `avid-hotels`
- Resolution source: **expansion_backlog**
- Parent company: InterContinental Hotels Group
- Target quality: **active-profile**
- Readiness band: **blocked**
- Ready for active profile: **no**
- Company Validated untouched: **yes**

## Required sections
- Score: **0**
- Ready: **no**
- Sections: 0/8

## Final QA
- Status: **blocked** (30)

## Visual QA
- Score: **0**
- Defects: 20
- Next batch: **v24B_media_asset_fix**

## Governance
- Governed platform ready: **no**
- Sources: 0 (0 approved)
- Explorer facts: 0 (0 pending)

## Blockers
- [data] Openings / Examples / Properties: Insufficient complete openings rows.
- [data] Recent Momentum: No minimum set of dated/source-backed momentum rows.
- [data] Portfolio Mix: Portfolio mix coverage below minimum.
- [data] Portfolio Context: missing_portfolio_context_row; portfolio_context_body_empty; portfolio_context_too_thin
- [founder_legal_review] Standard Detail / Where Available: No approved external-display-safe standards owner table package.
- [data] Demand Scenario View: incomplete_demand_rows:0<3
- [source] Loyalty Program: Required loyalty mechanics/proof coverage incomplete.
- [data] Geographic Footprint: Regional copy still template-thin.
- [frontend] Where This Brand Creates the Most Value: undefined
- [frontend] Where This Brand Creates the Most Value: undefined
- [frontend] Where This Brand Creates the Most Value: undefined
- [frontend] Value Creation Scenarios: undefined
- [frontend] Value Creation Scenarios: undefined
- [frontend] Value Creation Scenarios: undefined
- [frontend] Value Creation Scenarios: undefined
- [frontend] Key Watchouts: undefined
- [frontend] Standard Detail, Where Available: undefined
- [frontend] Standard Detail, Where Available: undefined
- [frontend] Portfolio Context: undefined
- [frontend] Brand Materials: undefined

## Next writer
- **row creation writer**

## Apply safety
- Safe for apply-approved: **no**
- block: 2 critical Final QA defect(s) remain
- block: Required sections are not ready
- block: Required image slots are still missing
- block: Source governance is insufficient for governed apply
- block: Human/founder/legal review is required

## Commands
```bash
npm run brand-explorer-complete-build -- --brand avid-hotels --dry-run --target-quality active-profile
```

### Staged apply
- Portfolio Context: `npm run brand-explorer-portfolio-context-ladder-mapping-repair -- --brand avid-hotels --dry-run`
- Standard Detail / Where Available: `npm run brand-explorer-tribute-standard-detail-table-writer -- --brand avid-hotels --dry-run`
- Demand Scenario View: `npm run brand-explorer-display-content-completion-writer -- --brand avid-hotels --dry-run`
- Geographic Footprint: `npm run brand-explorer-display-content-completion-writer -- --brand avid-hotels --dry-run`
- Portfolio Context: `npm run brand-explorer-portfolio-context-ladder-mapping-repair -- --brand avid-hotels --dry-run`