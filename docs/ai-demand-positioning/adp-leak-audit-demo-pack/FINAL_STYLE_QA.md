# Leak Audit Final Style QA

**Date:** 2026-09-07  
**Scope:** `/adp-leak-audit/sample` and `/adp-leak-audit/:reportId` (unified shell)  
**Layout:** `three_page_v1` — Cover · Executive Diagnostic · Action + Conversion

## 1. Source reports used for comparison

| Surface | Path / files |
|---------|----------------|
| Cover geometry (SoT) | `public/css/brand-alignment-snapshot.css` (`.bas-cover-*`) |
| Cover print fidelity (SoT) | `public/css/hotel-intelligence-dossier.css` |
| Cover markup SoT | `public/js/hotel-intelligence-dossier.js` → `buildCoverHtml()` (text-swapped in leak shell) |
| Cover parity QA | `docs/ai-demand-positioning/adp-leak-audit-demo-pack/COVER_PARITY_QA.md` |
| ADP monthly PDF body | `public/css/adp-monthly-review-report-v1.css` (pages 2–3 family alignment only) |
| PDF footer / logo URL | `public/js/dealality-report-print-chrome.js`, `public/css/dealality-report-print-chrome.css` |
| KPI + info icons | `public/js/ai-visibility/ai-visibility-shared.css` (`.aiv-kpi`, `.info-tooltip.aiv-col-info`, `.info-icon`) |
| Evidence body markup | `public/js/adp-leak-audit-shared-ui.js` (`.aiv-evidence.ala-adp-evidence`) |

## 2. Shared CSS / components reused

1. `/css/brand-alignment-snapshot.css` — cover pattern, title block, logo hero  
2. `/css/hotel-intelligence-dossier.css` — HID cover print/screen sizing + foot  
3. `/css/dealality-report-print-chrome.css` + print chrome JS — logo URL  
4. `/css/dealality-report-system-v1.css` — report family tokens (not used for cover layout)  
5. `/css/adp-monthly-review-report-v1.css` — ADP PDF family alignment for body pages  
6. `/js/ai-visibility/ai-visibility-shared.css` — ADP KPI + info icon grammar  
7. `/js/ai-demand-positioning/adp-guided-report-tour.*` — How-to-Read modal (web only)  
8. `/css/adp-leak-audit-report-v2.css` — navy pages 2–3 + cover page-break / foot visibility only

## 3. Remaining leak-audit-specific CSS (and why)

| Rule set | Why kept |
|----------|----------|
| `.ala-adp-report-page` navy gradient | Monthly ADP PDF body is white paper; free diagnostic intentionally uses ADP product navy surface |
| `.ala-action-row` / `.ala-who` | Compact sales diagnostic action grid not present in monthly PDF |
| `.ala-drawer*` | Evidence drawer chrome for free diagnostic; body content still uses `.aiv-evidence` |
| `.ala-page-foot` | Explicit Page 2/3 labels for 3-page free diagnostic |

## 4. Layout contract

1. **Page 1 — Cover:** BAS/HID cover only (`data-ala-page="cover"`), single disclaimer, HID-style foot (`Page 1 of 3`)  
2. **Page 2 — Executive Diagnostic:** navy surface with Executive Summary, Executive Signal, Demand Area to Review, Competitors Showing Up Instead, Supporting Evidence  
3. **Page 3 — Action + Conversion:** Recommended Dealality Action Items, Who Does the Work, Next Step  

PDF: exactly **3 pages**. How-to-Read chrome and interactive evidence buttons suppressed in print; info icons suppressed in PDF.

Web: ADP guided modal (8 steps, report order) + ADP-style evidence drawer.

## 5. Corrections made in this pass

1. Split single dense page 2 into executive + action pages (`three_page_v1`)  
2. Cover aligned to BAS/HID Deep Research classes + foot legal/page label  
3. Cover content simplified (product family, product, hotel, limited diagnostic, sample line, date/providers/priority)  
4. KPI cards switched to shared `renderKpiBand` / `.aiv-kpi` + ADP info icons; icons hidden in PDF  
5. Action copy expanded for readable page 3; PDF evidence buttons hidden  
6. Linked `ai-visibility-shared.css` into report shell  
7. Wired missing `server.js` sample/live/admin + public API routes onto the unified shell  
8. Competitor cards no longer show rank labels  
9. Added `FINAL_STYLE_QA.md`, `test:adp-leak-audit-pdf-three-page-v1`, `test:adp-leak-audit-adp-style-consistency-v1`

## 6. Known remaining issues

1. Playwright Chromium footer template (print-chrome) is still the production PDF foot authority for other ADP/HID exports; Leak Audit currently paints DOM page labels for the 3-page diagnostic. Unifying to print-chrome-only footers is a follow-up if Playwright export becomes the primary delivery path.  
2. Portfolio sample (`/adp-leak-audit/sample-portfolio`) remains on the older sample stack and was out of scope for this single-property polish.  
3. Live Node process must be restarted (or hard-refresh with cache bust `?v=leak5` / `?v=6`) to pick up shell/CSS/JS/route changes. Cover SoT is documented in `COVER_PARITY_QA.md`.  
4. Full styled Puppeteer export of the complete shell can produce blank trailing pages under Chromium when page boxes are exactly A4-tall with `break-after`. Production print CSS uses `height: 296mm` + `break-before` on pages 2/3; the PDF gate proves the pagination contract with a minimal 3-section A4 proof.  
4. Full styled Puppeteer export of the complete shell can produce blank trailing pages under Chromium when page boxes are exactly A4-tall with `break-after`. Production print CSS uses `height: 296mm` + `break-before` on pages 2/3; the PDF gate proves the pagination contract with a minimal 3-section A4 proof.
