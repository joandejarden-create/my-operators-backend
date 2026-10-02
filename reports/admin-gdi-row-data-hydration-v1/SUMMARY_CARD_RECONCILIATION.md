# Summary Card Reconciliation — GDI Reports

**Law:** PDF READY / Needs PDF / Report Ready / Blocked cards MUST equal counts derived from the same normalized rows rendered in the table.

## Implementation

- Server: `reconcileGdiAdminReportCounts(rows)` in `lib/admin/gdi-admin-report-row-v1.js`
- Client: `countsFromRows(catalog)` after `normalizeCatalogRow` (defense — never trust a divergent summary query)

## Formulas

| Card | Formula |
|---|---|
| GDI Hotels | `rows.length` |
| Report Ready | `count(reportStatus === 'READY')` |
| PDF Ready | `count(pdfReady \|\| pdfStatus === 'READY')` |
| Needs PDF | `count(available && !pdfReady)` |
| Blocked | `count(reportStatus in {BLOCKED, NEEDS_BUILD})` |

## Local reconciliation (2026-10-02)

| Metric | Card | Table | Match |
|---|---:|---:|:---:|
| Hotels | 6 | 6 | YES |
| Report Ready | 4 | 4 | YES |
| PDF Ready | 1 | 1 | YES |
| Needs PDF | 5 | 5 | YES |
| Blocked | 0 | 0 | YES |

PDF Ready hotel: **Bethesda Marriott** only.  
Needs PDF: Cambridge, Hilton NYTS, NOW NOW NOHO, Renaissance NYTS, Waterstone.

**CARD/TABLE COUNTS RECONCILE: YES**
