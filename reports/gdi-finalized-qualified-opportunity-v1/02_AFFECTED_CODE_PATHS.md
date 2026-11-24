# Affected code paths

| Path | Change |
|------|--------|
| `lib/group-demand-intelligence/workflow/gdi-finalized-qualified-opportunity-contract-v1.js` | **NEW** FQO evaluator + person-specific channel gate |
| `lib/group-demand-intelligence/publication-package/gdi-publication-quality-gate-v1.js` | Opt-in `enforceFinalizedQualifiedOpportunity` |
| `lib/group-demand-intelligence/publication-decision-model-v1.js` | Note: local STAGED uses FQO, not Sales Ready |
| `lib/group-demand-intelligence/commercial-eligibility-contract-v1.js` | Doc: SR is compatibility scoring only for local STAGED |
| `public/app.html` | Remove Research inventory tab button |
| `public/app.js` | Local default **staged**; research only with `gdiAdminResearch=1` |
| `public/js/group-demand-intelligence/app.js` | Same inventory policy; no Research UI mode without admin flag |
| `scripts/test-gdi-finalized-qualified-opportunity-v1.mjs` | Permanent positive/negative regression |
| `scripts/gdi-finalized-qualified-opportunity-portfolio-audit-v1.mjs` | Portfolio matrix generator |

## Explicitly not changed

- Existing STAGED/CURRENT pointer files  
- Airtable writers  
- Paid Exa / Agent / Surfe / Findymail  
- Hilton package activation (separate PR #592 candidacy)  
- Open Research View PRs #590 / #591 — **must not merge** as product Research browsing (contradicts this policy)
