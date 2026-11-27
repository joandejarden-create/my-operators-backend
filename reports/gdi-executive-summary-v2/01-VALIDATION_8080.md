# GDI Executive Summary V2 — :8080 validation

**Date:** 2026-11-27  
**Workspace:** `C:\Dev\dealality\ws-gdi`  
**Contract:** `gdi-executive-summary-v2`  
**Gate:** `scripts/gdi/Invoke-GdiExecutiveSummaryV2Validate.ps1` → PASS

## Screenshots

| File | Perspective |
|---|---|
| `screenshots/00-before-wyndham-exec-summary.png` | Prior narrow / unavailable state |
| `screenshots/01-wyndham-executive-overview.png` | Full-width Executive Overview |
| `screenshots/02-wyndham-sales-performance.png` | Sales Performance |
| `screenshots/03-wyndham-research-intelligence.png` | Research Intelligence (founder session) |

## Live API probes (`?preview=staged`)

| Hotel | ID | Qualified | Segments | Unspecified | Evidence URLs | Period label |
|---|---|---|---|---|---|---|
| Wyndham San Diego Bayside | `recnPB3Q02NO8Ma1o` | 26 | 5 | 0 | 22 | STAGED research as of 2026-11-27 |
| Bethesda Marriott | `recLuxvwwxID7U2B8` | 68 | 13 | 0 | 36 | STAGED research as of 2026-11-25 |
| YOTEL Geneva Lake | `recrPQcZg7SFARRb2` | 42 | 7 | 0 | 41 | STAGED research as of 2026-11-17 |

## Integrity notes

- Competitive Opportunities = **0** on Wyndham is a measured STAGED zero (Hotel∩Competitive overlap), not a mapping defect.
- Need Periods Supported = **0** where `needDateMatch` is absent on qualified rows.
- Weekly Change new/updated = **Unavailable** when comparable history is missing (not fabricated zeros).
- Research Spend / Cost per FQO = **Unavailable** (ledger join not loaded for dashboard view).
- YOTEL still shows **Other / Unclassified** rows where taxonomy was never mapped — distinct from prior Wyndham “Unspecified 26” defect.
- Research Intelligence section is emitted only when `researchIntelligenceAuthorized`; `stripResearchIntelligencePerspective` removes it for ordinary customers.
- PDF exporter intentionally not shipped; no nonfunctional download control.

## Regression

- `npm run test:gdi-executive-summary-v2` PASS
- `npm run test:gdi-executive-summary-v1` PASS (22)
- List / Tiles / filters / drawers / four-tab nav preserved (additive)
