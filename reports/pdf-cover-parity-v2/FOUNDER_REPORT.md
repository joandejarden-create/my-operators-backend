# PDF Cover Parity V2 — GDI Reuses ADP Cover System

## ADP canonical cover component

| Artifact | Path |
|----------|------|
| **ADP_COVER_COMPONENT** | `lib/dealality-report-family/dealality-report-cover-v1.js` → `renderDealalityReportCover()` |
| **ADP_COVER_TEMPLATE** | Same module — slot-based HTML (`bas-cover-page`, `bas-cover-block`, etc.) |
| **ADP_COVER_CSS** | `public/css/brand-alignment-snapshot.css` (layout) + `public/css/adp-monthly-review-report-v1.css` (`.adp-mr-pdf-host` height/print) |
| **ADP_FOOTER_COMPONENT** | `public/js/dealality-report-print-chrome.js` → `playwrightFooterTemplate()` |
| **ADP_DISCLAIMER_COMPONENT** | `bas-cover-disclaimer` inside shared cover renderer |

ADP monthly review PDF builder (`public/js/ai-demand-positioning/adp-monthly-review-report-pdf-v1.js`) now delegates to `DealalityReportCoverV1.renderAdpMonthlyReviewCover()` when available, with identical fallback markup.

## GDI before (bespoke)

- Inline cover HTML in `gdi-pdf-report-html-v1.js` with wrong confidential line, disclaimer, and slot mapping
- `gdi-pdf-extra-css-v1.js` bespoke cover height (`calc(297mm - 20mm)`) and `.gdi-pdf-cover` selectors
- Host wrapper `.gdi-pdf-report` without `.adp-mr-pdf-host`
- CSS bundle omitted `adp-monthly-review-report-v1.css`

## GDI after (shared)

- `renderGdiReportCover(data)` from shared module — **same HTML primitive as ADP**
- Host: `.adp-mr-pdf-host brand-alignment-snapshot` (identical to ADP PDF host)
- CSS: `loadDealalityPdfReportCssBundle()` includes ADP monthly review cover rules
- Body-only styles remain in `gdi-pdf-extra-css-v1.js` (no cover geometry)

## GDI content mapping (ADP slots)

| Slot | GDI content |
|------|-------------|
| Top line | DEALALITY GROUP & DEMAND INTELLIGENCE · CONFIDENTIAL · FOR RECIPIENT ONLY |
| Report title (`bas-cover-doc-type`) | GROUP & DEMAND INTELLIGENCE |
| Hotel (`h1`) | Bethesda Marriott |
| Market | Washington metropolitan area |
| Report class (`bas-cover-sub`) | COMMERCIAL DEMAND REVIEW |
| Period line | OCTOBER 2026 · REPORT DATE OCT 2, 2026 |
| Descriptor line | OPPORTUNITIES · CONTACTS · PRIORITIES · NEXT ACTIONS |
| Disclaimer | GDI commercial-intelligence disclaimer (same footprint as ADP) |

## Visual diff (Bethesda, local)

Generated with `node scripts/pdf-cover-parity-v2-qa.mjs`:

| Metric | ADP | GDI | Δ |
|--------|-----|-----|---|
| Page raster | 1191×1685 | 1191×1685 | 0 |
| Cover panel top (px) | 2 | 2 | 0 |
| Cover panel bottom (px) | 1485 | 1485 | 0 |
| Panel height (px) | 1484 | 1484 | 0 |
| Side inset L/R (px) | 0 / 1 | 0 / 1 | 0 |
| Bottom white band (px) | 199 | 199 | 0 |

Artifacts:

- `reports/pdf-cover-parity-v2/ADP_GDI_COVER_SIDE_BY_SIDE.png`
- `reports/pdf-cover-parity-v2/ADP_GDI_COVER_OVERLAY.png`
- `reports/pdf-cover-parity-v2/COVER_GEOMETRY.json`

**Overlay PASS** — panel edges, footer band, and logo baseline align; text differs by design.

## Archive versioning

- Prior archived PDFs preserved under `reports/group-demand-intelligence/pdf/recLuxvwwxID7U2B8/archive/v1–v4`
- New corrected current pointer + **v5** written (264,490 bytes)
- No archive history rewritten

## Production

Not deployed in this pass. See `PRODUCTION_PDF_QA.md`.

## FINAL VERDICT

**COVER PARITY V2 PASSES — GDI USES SHARED ADP COVER SYSTEM; GEOMETRY MATCHES ADP PAGE 1**
