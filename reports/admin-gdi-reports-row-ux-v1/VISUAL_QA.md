# Visual QA — GDI Reports vs AI Demand Reviews

## Method

Compared markup/CSS class reuse and live catalog payload against the Reviews tab patterns in:

- `public/css/admin-ai-demand-reviews.css`
- `public/js/admin-ai-demand-reviews.js`
- `public/app/admin/ai-demand-admin.html` (`#adaPanelReviews` vs `#adaPanelGdiReports`)

Browser walkthrough requires admin auth in the running app (`/admin/ai-demand?tab=gdi-reports`).

## Checklist

| Check | Result | Notes |
|-------|--------|-------|
| Same Admin shell / tabs | PASS | Unchanged `ai-demand-admin.html` shell |
| Same page width rhythm | PASS | Shared `.adr-shell` / panel layout |
| Summary cards | PASS | `.adr-counts` / `.adr-count` / `__n` / `__l` |
| Filter bar grid | PASS | `.adr-filters` + Apply button |
| Table wrap + scroll | PASS | `.adr-table-wrap` overflow-x |
| Table styling | PASS | `.adr-table` |
| Status chips | PASS | `.adr-badge` ok/warn/danger |
| Tiny action buttons | PASS | `.adr-btn--tiny` |
| Row density | PASS | Same button + badge language as Reviews PDF/Action columns |
| No competing top dropdown workflow | PASS | Removed |
| No separate External Client block | PASS | Removed |
| Horizontal scroll for wide columns | PASS | Inherited from Reviews |

## Live data sanity (catalog smoke)

- 6 hotels, 4 report-ready, 1 PDF ready, 5 needs PDF, 0 blocked, 2 watch-only
- Bethesda: PDF READY + GDI/ADP shares available
- Cambridge / NOW NOW: WATCH_ONLY
- Hilton / Renaissance / Waterstone: READY + PDF MISSING

## Residual visual notes

- GDI table is wider than Reviews (extra client columns) — horizontal scroll expected, same as Reviews when many action columns are present.
- Sticky hotel column not implemented (optional; Reviews also does not sticky-lock).
