# Operator Fit — Airtable Schema Dry Run

**Mode:** Dry-run only — **no operations executed**  
**Generated:** 2026-08-03  
**Machine-readable:** `reports/operator-fit-airtable-schema-dry-run.json`

---

## Summary

| Bucket | Count |
| ------ | ----: |
| Populate existing fields | 4 |
| Create fields (proposed) | 3 |
| Create tables (proposed) | 2 |
| Extend Case Studies | 1 |
| Explicit do-not-create | 1 |

**Applied:** `false`

---

## Operations

| ID | Operation | Table | Field | Type | Conflict | MVP | Founder approval | Rollback |
| -- | --------- | ----- | ----- | ---- | -------- | --- | ---------------- | -------- |
| op-001 | populate_existing | Platform & Markets | Active Countries | multi | Often empty | Yes | Yes | Snapshot restore |
| op-002 | populate_existing | Platform & Markets | Market Presence Type | multi | Sparse | Yes | Yes | Snapshot restore |
| op-003 | populate_existing | Commercial | Management Structures Supported | multi | 12.5% filled | Yes | Yes | Snapshot restore |
| op-004 | populate_existing | Commercial | Conversion / Reflag Experience | select/text | 0% | No | Yes | Clear values |
| op-005 | create_field | Brand Relationships | Direct Brand Management Available | select | May not exist | Yes | Yes | Hide / stop write |
| op-006 | create_field | Brand Relationships | Approval Status | select | Name collision check | Yes | Yes | Dual-read deprecate |
| op-007 | create_field | Brand Relationships | Date Verified | date | None | Yes | Yes | Ignore in adapters |
| op-008 | create_table | Operator Fit - Evidence Sources | — | table | Prefer PI if sufficient | Yes | Yes | Archive + fallback |
| op-009 | extend_fields | Case Studies | Why Comparable | long text | Check existing narrative | Yes | Yes | Ignore in readers |
| op-010 | create_table | Operator Fit - Project Responses | — | table | Must not be ODR | No | Yes | Do not create yet |
| op-011 | do_not_create | Operator Deal Requests | shortlist flags | — | Founder 2.4 | No | — | Never apply |

---

## Recommended apply order (future, after approval)

1. Export full Active-operator snapshot  
2. Taxonomy validation (`npm run operator-fit-taxonomy-validation`)  
3. Populate Active Countries + Presence Type + Management Structures (`--dry-run` then `--apply`)  
4. Evidence + Case Study comparable enrichment  
5. Brand relationship confirmation fields (only after table inventory)  
6. Re-run `npm run operator-fit-data-readiness` until Ranking Ready ≥ pipeline need  

---

## Confirmation

No Airtable schema change or record write was performed while producing this dry run.
