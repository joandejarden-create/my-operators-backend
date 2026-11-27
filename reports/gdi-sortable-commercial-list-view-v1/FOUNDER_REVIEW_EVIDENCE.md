# GDI Sortable Commercial List View — Founder Review Evidence

**Status:** `GDI_SORTABLE_LIST_VIEW_COMPLETE — AWAITING_FOUNDER_REVIEW`  
**Date:** 2026-11-27  
**Branch:** `feat/gdi-sortable-commercial-list-view-20261127`  
**Base:** `origin/main` `@ d97d293`  
**Separated from:** PR #618 (Executive Summary Phase 1) — intentionally not mixed

## Acceptance checklist

| # | Criterion | Result |
|---|-----------|--------|
| 1 | Before/after List View screenshots | PASS — `screenshots/01-before-tiles.png`, `screenshots/02-after-list-table.png` |
| 2 | Deal Opportunities design parity | PASS — dark header, uppercase labels, sort indicators, eye icon, row hover, horizontal scroll, monogram identity |
| 3 | Tiles View unchanged | PASS — still `gdi-results-grid` / `brand-card` path; automated gate asserts no table in tiles mode |
| 4 | Sortable columns | PASS — header click syncs `#gdiSortSelect` (`eventStartDate` verified live) |
| 5 | Filter + sorting interoperability | PASS — High filter → 4 rows; Showing count preserved; sort applies to full filtered inventory |
| 6 | Optional columns | PASS — Columns control enables Estimated Room Nights without fabricating ADR/revenue |
| 7 | List/Tiles ID parity | PASS — exact 68/68 ID reconciliation on Bethesda CURRENT/founder preview |
| 8 | Existing drawer | PASS — eye icon opened drawer (`Greater Bethesda Chamber of Commerce`) |
| 9 | Hotel switching | PASS — property selector retained; filters/viewMode session prefs preserved |
| 10 | Responsive layout | PASS — table `min-width` + `.gdi-list-table-scroll` horizontal scroll |
| 11 | Automated regression | PASS — `npm run test:gdi-sortable-commercial-list-view-v1` |
| 12 | PowerShell validation | PASS — `scripts/validate-gdi-sortable-commercial-list-view-v1.ps1` |
| 13 | PR + IC handoff | See PR URL in completion report |

## Non-regression

- No Tiles markup/layout changes beyond shared sort option labels in the dropdown.
- No qualification / discovery / inventory / publication / drawer / nav / Exec Summary / Commercial Priorities / PDI changes.
- No paid research, enrichment, Airtable writes, or publication pointer mutation.
- Deal Opportunities page (`public/deal-opportunities.html`) untouched.

## Live browser (canonical :8080)

- URL: `http://localhost:8080/group-demand-intelligence.html?hotelId=recLuxvwwxID7U2B8&founderPreview=1`
- Inventory: Showing **68 of 68**
- List/Tiles parity: **68 = 68**, missing `[]`, unexpected `[]`
- Header background: `rgb(33, 44, 77)` (DOP-parity dark header)
- Optional column Estimated Room Nights visible after Columns toggle

## Repeatable commands

```powershell
npm run test:gdi-sortable-commercial-list-view-v1
powershell -NoProfile -File scripts/validate-gdi-sortable-commercial-list-view-v1.ps1
```

## Deal Opportunities parity assessment

| Pattern | DOP | GDI List |
|---------|-----|----------|
| Table container | `.dop-table` / `.deals-table` | `.gdi-commercial-table.deals-table` |
| Dark sticky header | Yes | Yes (`thead` sticky) |
| Uppercase labels + sort arrows | Yes | Yes |
| Identity monogram / verified logo | Yes | Monogram + verified-logo fields when present |
| Eye View Details | `ICON_VIEW` | Same SVG pattern |
| Column settings | N/A (fixed) | Compact optional Columns panel |
| Horizontal scroll | Yes | `.gdi-list-table-scroll` |

## Permanent protection

- Module: `public/js/group-demand-intelligence/gdi-commercial-list-table-v1.js`
- Gate: `npm run test:gdi-sortable-commercial-list-view-v1`
- Asserts list table path, tiles non-regression, ID parity helper, sort semantics, optional columns, shell wiring, DOP untouched
