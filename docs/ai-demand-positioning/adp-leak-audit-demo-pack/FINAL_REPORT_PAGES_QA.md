# Leak Audit Final Report Pages QA

**Date:** 2026-09-07  
**Scope:** Pages 2–3 of `/adp-leak-audit/sample` and `/adp-leak-audit/:reportId`  
**Layout:** `three_page_v1` — Cover (HID) · Executive Diagnostic · Action + Conversion

## 1. Source reports used for comparison

| Surface | Path / files |
|---------|----------------|
| Live ADP product navy UI | `public/js/ai-visibility/ai-visibility-shared.css` (`.aiv-kpi`, `.aiv-card`, `.aiv-link`, `.aiv-drawer`, `.info-tooltip`) |
| Live ADP KPI / evidence builders | `public/js/ai-demand-positioning/ai-demand-positioning.js` (`kpiCardWithInfo`, evidence drawer body) |
| ADP guided tour / How-to-Read | `public/js/ai-demand-positioning/adp-guided-report-tour.*` |
| Monthly PDF action grammar | `public/css/dealality-report-system-v1.css` (`.drs-action`, `.drs-label`) — adapted onto navy `aiv-card` |
| Hotel Intelligence cover (page 1) | `public/js/hotel-intelligence-dossier.js` + BAS/HID CSS (see `COVER_PARITY_QA.md`) |
| Leak Audit shell | `public/adp-leak-audit-report.html`, `public/css/adp-leak-audit-report-v2.css`, `public/js/adp-leak-audit-shared-ui.js`, `public/js/adp-leak-audit-report.js` |

## 2. Shared CSS / classes / components reused

1. **Page surface:** ADP navy `#080f25` / `--neutral--800` (product UI, not monthly white paper)
2. **Section chrome:** `.aiv-theme-group` + `.aiv-theme-title` + `.aiv-theme-help`
3. **KPI cards:** `.aiv-kpi-row` → `.aiv-kpi` / `.aiv-kpi-label` / `.aiv-value` / `.aiv-meta`
4. **Info icons:** `.info-tooltip.aiv-col-info` → `.info-icon` + `#aiv-info-icon` SVG sprite (same as Brand AI / ADP)
5. **Evidence drawer:** `dialog.aiv-drawer.aiv-evidence-drawer` + `.aiv-drawer-inner` + `.aiv-evidence-header` + `.aiv-evidence-body`
6. **Evidence body:** `.aiv-evidence` / `.aiv-evidence-meta*` / `.aiv-evidence-section--ai-response` / `.aiv-evidence-response`
7. **Hyperlinks:** `button.aiv-btn-text.aiv-link`
8. **Action cards:** `.drs-action.ala-adp-action.aiv-card` (DRS action fields on ADP navy cards)
9. **Who / Next:** `.aiv-card` + `.aiv-secondary-row`
10. **Footers:** `.ala-page-foot` with left legal + `Page X of 3` (DOM footers; HID cover foot on page 1)

## 3. Differences intentionally retained

1. **Navy insides vs monthly PDF white paper** — free diagnostic matches live ADP product navy, not the white monthly PDF body.
2. **3-page short form** — monthly review is multi-section; leak audit stays Cover / Executive / Action.
3. **DOM page footers** — Leak Audit does not use Playwright `dealality-report-print-chrome` pageNumber tokens for this free PDF path.
4. **No evidence links on KPI cards** — Supporting Evidence only (web).
5. **Portfolio sample** still on older shell (out of scope).

## 4. Fixes made in this pass

| Area | Fix |
|------|-----|
| Report background | Solid ADP navy `#080f25` / token surface (not custom gradient-only shell) |
| Section headers | Replaced gold uppercase `.ala-sec__h` with `.aiv-theme-title` |
| KPI cards | Reused `renderKpiBand` + live ADP card tokens; readable 22px values |
| Info icons | Exact ADP `#aiv-info-icon` sprite + `.info-icon` span; suppressed in PDF |
| Evidence cards/drawer | `aiv-card` examples + `dialog.aiv-drawer.aiv-evidence-drawer`; ADP evidence body labels |
| Hyperlinks | `aiv-btn-text aiv-link`; removed competitor-section evidence button; PDF hides links |
| Action cards | Shared `renderActionItems` → `.drs-action.aiv-card` (no custom dense grid rows) |
| Who / Next | `aiv-card` columns + ADP-tint CTA card |
| Footers/page numbers | Visible screen+print footers: confidential left, Page 2/3 of 3 right |
| PDF scaling | Raised print type sizes; kept 296mm page lock + break-before on pages 2–3 |
| Page breaks | Cover break-after; exec/actions break-before; exactly 3 page regions |
| Duplicate competitors | Only `competitorDisplacementRank.rows` (max 2); no second displacement list render |

## 5. Remaining known issues

1. Full Chromium export of the complete styled shell can still be pagination-sensitive; the PDF gate proves the 3-page contract with shell structure + CSS lock + minimal A4 proof.
2. Portfolio sample (`/adp-leak-audit/sample-portfolio`) is not yet on this unified ADP page grammar.
3. Hard-refresh required (`?v=7` / shared-ui `?v=6` / report `?v=16`) after deploy.

## 6. How to verify

1. Open `/adp-leak-audit/sample` — page 2 uses `.aiv-theme-group` sections and ADP KPI cards with circular info icons.
2. Click Supporting Evidence “View example” — opens ADP `aiv-drawer` with Evidence type / Demand area / Provider / AI response / Why it matters / Management review.
3. Print/PDF preview — 3 pages; no How-to-Read modal; no View example buttons; footers Page 1–3 of 3.
