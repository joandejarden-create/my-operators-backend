# PDF Cover Parity — GDI vs Canonical Dealality Family

**Date:** 2026-10-02  
**Artifact:** `Bethesda-Marriott-GDI-corrected.pdf` (16 pages)

---

## Canonical source

| Item | Path |
|------|------|
| **CANONICAL_COVER_COMPONENT** | `lib/dealality-report-family/dealality-report-cover-v1.js` → `renderDealalityReportCover` / `renderGdiReportCover` |
| **CANONICAL_COVER_CSS** | ADP host geometry: `brand-alignment-snapshot.css` + `.adp-mr-pdf-host` rules in `adp-monthly-review-report-v1.css`, shared via `lib/dealality-report-family/dealality-pdf-report-css-v1.js` |
| **ADP reference** | `renderAdpMonthlyReviewCover` (same primitive, different slot strings) |
| **CURRENT_GDI_COVER_COMPONENT** | `renderGdiReportCover(data)` called from `lib/group-demand-intelligence/reports/gdi-pdf-report-html-v1.js` |
| **WHY_GDI_DIVERGED** | Earlier GDI PDF used a bespoke navy cover / layout outside the ADP monthly-review cover shell. Fixed in SHA `df0c7b3` by routing GDI page 1 through the shared cover primitive. |

---

## Cover content (Bethesda corrected)

| Slot | Value |
|------|--------|
| Confidential line | DEALALITY GROUP & DEMAND INTELLIGENCE · CONFIDENTIAL · FOR RECIPIENT ONLY |
| Report title | GROUP & DEMAND INTELLIGENCE |
| Hotel | Bethesda Marriott |
| Market | Washington metropolitan area (not bare “DMV”) |
| Report class | COMMERCIAL DEMAND REVIEW |
| Period | OCTOBER 2026 · REPORT DATE OCT 1, 2026 |
| Descriptor | OPPORTUNITIES · CONTACTS · PRIORITIES · NEXT ACTIONS |
| Logo | Canonical Dealality mark + red-dot treatment (same asset family as ADP) |

Date format matches ADP cover helpers (`formatCoverMonthYearFromIso` / `formatCoverShortDate`) — human month labels, not ISO `2026-10-01` as the primary cover date.

---

## Title rendering

PDF.js text extract shows intentional tracked uppercase (letter-spaced) on the cover, matching ADP:

`G R O U P   &   D E M A N D   I N T E L L I G E N C E`

Visual raster (`corrected-cover-page-1.png`) reads cleanly as **GROUP & DEMAND INTELLIGENCE** — no mid-word “INT E L L IGENCE” corruption.

**TITLE RENDERING:** PASS

---

## Footer / page chrome

Same ADP cover shell: cover inherits the book-page surface and print footer chrome used by the report family (confidential line + page N of M). No GDI-only footer invention.

---

## Body design

Pages 2–16 keep existing GDI content structure (Commercial Demand Snapshot, Top Immediate Pursuits, Top Opportunities, Action Plan, Pipeline, Future Watch, Supporting Intelligence) under shared report-family CSS. No body redesign beyond content-QA display fixes in the data mapper.
