# DEALALITY Responsive Remediation Closeout V1

**Status:** `PARTIAL — APPROVED_REMEDIATIONS_INTEGRATED_PENDING_MERGE — CROSS_VIEWPORT_VALIDATED_RANKED — AUTH_RADAR_BLOCKED — HE_NOT_VALIDATED`  
**Date:** 2026-11-28  
**Serving baseline during validation:** `http://127.0.0.1:8080` (ws-gdi `server.js`)  
**Contract:** `DEALALITY_RESPONSIVE_DESIGN_STANDARD_V1` (ADOPTED, founder-amended)

## 1. Adopted standard

Amended and adopted: `reports/dealality-codebase-health-audit-v1/responsive/DEALALITY_RESPONSIVE_DESIGN_STANDARD_V1.md`

- Overflow ≤ 2px page-level
- Five mandatory viewports
- Full-information accessibility (not title-only)
- Brand Explorer application = Tier A mobile-core
- GDI-002 in Batch A
- Evidence vocabulary: PASS / FAIL / PARTIAL_RESPONSIVE_SUPPORT / EXPECTED_DESKTOP_ONLY / NOT_VALIDATED / BLOCKED

## 2. Permanent regression

- Script: `scripts/playwright-dealality-responsive-regression-v1.mjs`
- npm: `npm run test:dealality-responsive-regression-v1`
- Evidence: `reports/dealality-responsive-regression/responsive-regression-v1.json` + `screenshots/`

Latest local run counts: **PASS 25 · NOT_VALIDATED 5 (HE) · BLOCKED 1 (auth Radar)** · FAIL 0

## 3. Finding table (observed)

| Finding ID | Product | Original Defect | Remediation | Desktop | Tablet | Mobile | Final Status |
|---|---|---|---|---|---|---|---|
| RD-001 | Radar ranked | Dense filter truncation / overflow:hidden | Wrap denser grid ≤1400; overflow visible; select titles + aria | PASS | PASS | PASS (drawer path) | PASS (ranked) |
| RD-002 | Radar ranked | Mobile select containment | Hide dense selects ≤768; More filter drawer | PASS | PASS | PASS | PASS (ranked) |
| GDI-002 | GDI | Sticky/filter intercept | Lower sticky nav z; raise presets/chips; test enabled chips (empty High remains `.no-records`) | PASS | PASS | PASS | PASS |
| GDI-001 | GDI | Property bar / mobile H-scroll | sr-only pin; toolbar wrap; property bar stack ≤768 | PASS | PASS | PASS | PASS |
| SH-001 | Deal Pulse / shell | Overlap / orphaned nav | Insights stack; off-canvas sidebar + Menu toggle; toast containment | PASS* | PASS* | PASS* | PASS (shell CSS + My Deals Menu) |
| ADP-001 | ADP | Hybrid ES grid not stacking | Higher-specificity ≤960 `1fr !important` + cache bust | PASS | PASS | PASS | PASS |
| BE-001 | BE app | Fixed 260px sidebar | Recoverable Menu + off-canvas | PASS | PASS | PASS | PASS |
| BE-002 | BE share | Filter overflow | Stack Parent/Chain Scale; contain filters | PASS | PASS | PASS | PASS |
| HE-001/002 | HE | Incomplete prior audit | HCI overflow fix included; preview content still “Opening…” | NOT_VALIDATED | NOT_VALIDATED | NOT_VALIDATED | NOT_VALIDATED |
| Auth Radar | Radar `/app#/opportunity-radar` | Gate | Memberstack session required | — | — | — | BLOCKED |

\* Deal Pulse shell validated via CSS + My Deals Menu wiring; full authenticated `/app` Deal Pulse remains Tier B / partial until founder session review.

## 4. Remaining limitations

1. Authenticated `/app#/opportunity-radar` — **BLOCKED** (needs authorized Memberstack test session).
2. Hotel Explorer loaded-content matrix — **NOT_VALIDATED** (`hotel-explorer-preview.html` remains on “Opening Bethesda Marriott…”).
3. GDI research / STAGED / CURRENT / Airtable — **unchanged** (out of scope).
4. Empty Priority chips (e.g. High (0)) intentionally use `pointer-events: none` via `.no-records` — not treated as sticky intercept.

## 5. Source changes (primary)

- `public/deal-capture-radar-with-ranked-list.html`
- `public/css/group-demand-intelligence.css`
- `public/group-demand-intelligence.html` (+ share cache bust)
- `public/js/ai-demand-positioning/ai-demand-positioning.css`
- `public/owner-ai-demand.html` / `owner-ai-demand-share.html`
- `public/brand-explorer.html`
- `public/css/brand-explorer-atelier-list-parity.css`
- `public/css/deal-workspace-shell.css`
- `public/app.html` / `public/app.css` / `public/app.js`
- `public/my-deals.html`
- `public/css/hotel-contact-intelligence.css`
- `scripts/playwright-dealality-responsive-regression-v1.mjs`
- Standard + regression reports

## 6. Publication pointers

No STAGED / CURRENT pointer changes. No Airtable writes. No paid research.
