# Supported row & bulk actions

## Shared behavior

- Compact primary buttons + **More** menu
- Disabled actions explain why (tooltip / More why-line)
- No unrestricted client URLs; uses existing admin external-client-links + share registry
- Selection uses stable IDs; pruned when filters change (does not silently expand)

## GDI Reports

**Row:** View PDF, Generate/Retry PDF, Open GDI, Copy GDI URL, Open/Copy ADP (when share exists), Regenerate, Report Archive  

**Bulk (selected):**
- Generate missing PDFs — confirms scope; existing `/report-pdf/generate` only; no paid research
- Export selected rows (CSV) — admin table fields only
- Clear selection

## AI Demand Reviews

**Row:** View/Generate PDF, Open review, Meeting mode, Regenerate, Open/Copy ADP, Action plan, Report Archive  

**Bulk:** selection + count visible; mutating bulk PDF left to existing per-row handlers (no new mass regenerate without confirmation path on GDI)

## Report Archive

**Row:** View/Download PDF (when archived), View data  

## ADP Action Plan

Existing Load Actions / Download CSV / View PDF / external client Open+Copy preserved.
