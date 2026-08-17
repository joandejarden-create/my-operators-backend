# Operator Fit — Shortlist Architecture

**Status:** Implemented for **internal pilot** (Airtable + file fallback)  
**Date:** 2026-08-04  
**Table ID:** `tbl4D5DCK7oPFhi98`

---

## Workflow distinction

```text
System Candidate
→ Top Operator Alignment Result
→ Owner Target List (brands)
→ Operator Shortlist
→ Outreach / Operator Deal Request
→ Contacted
```

| Object | Role |
| ------ | ---- |
| Target List | Brand exploration |
| **Operator Shortlist** | Curated operator decisions + immutable snapshot |
| Operator Deal Request | Outreach / RFP — **not** shortlist |

## Persistence

- Airtable: `Operator Fit - Shortlist`  
- File fallback: `data/operator-fit/shortlist-store.json`  
- Field map: `map_operatorShortlistFields`  
- Migration: `npm run operator-fit-shortlist-migrate`  

## Non-goals

No auto ODR · no owner exposure yet · no Target List / Shortlist / Outreach collapse.
