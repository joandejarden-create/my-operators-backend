# Supported row & bulk actions

## Shared behavior

- Icon primary actions + vertical-ellipsis **More** portal menu (document.body, `position: fixed`)
- Menu escapes sticky Actions stacking; flip/clamp to viewport; Escape / outside click / scroll-reposition
- Disabled actions explain why (tooltip + More why-line)
- Opening More does not change row height or select the row
- No unrestricted client URLs; uses existing admin external-client-links + share registry
- Selection uses stable IDs; pruned when filters change (does not silently expand)
- Header checkbox label: **Select this page** (not every filtered row)

## GDI Reports

**Row (icons):** View PDF / Generate|Retry PDF, Open GDI  

**More:** Download PDF, Copy GDI URL, Open/Copy ADP (when share exists), Regenerate, Report Archive  

**Bulk (selected):**
- Generate missing PDFs — confirms scope; existing `/report-pdf/generate` only; no paid research
- Export selected rows (CSV) — admin table fields only
- Clear selection

## AI Demand Reviews

**Row (icons):** View/Generate PDF, Open review  

**More:** Download PDF, Meeting mode, Regenerate, Open/Copy ADP, Action plan, Report Archive  

**Bulk:** selection + count visible; mutating bulk PDF left to existing per-row handlers

## Report Archive

**Row (icons):** View PDF (when archived)  

**More:** Download PDF, View data  

## ADP Action Plan

Toolbar Load Actions / Download CSV / View PDF / external client Open+Copy preserved (no row More menu). Preview table uses `ada-table-shell` for overflow containment.
