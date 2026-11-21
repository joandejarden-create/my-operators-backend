# Additive Airtable proposal — HIF Weekly Regional Yield Scorecard

**Status:** PROPOSAL ONLY — **do not apply** without founder approval  
**CUSTOMER_BLOCKED:** research metrics must not bypass publication restrictions  
**As of:** 2026-11-21

## Why additive

Weekly scorecards currently persist as immutable JSON under:

`reports/sec-edgar-hif-v6-weekly-scorecard/snapshots/`

and durable HIF ledgers. An Airtable table is optional for founder dashboards / Master To-Do linkage — not required for the operating scorecard to function.

## Proposed table (new)

**Name:** `HIF Weekly Coverage Yield`  
**Base:** existing Hotel Intelligence / research base (founder to confirm) — **not** customer-facing.

### Fields (additive)

| Field | Type | Notes |
|---|---|---|
| Week Id | Single line text | `YYYY-Www` primary key |
| As Of | Date | |
| Snapshot Path | URL / text | Repo-relative path to immutable JSON |
| Hotels Discovered | Number | |
| Hotels Identity Verified | Number | |
| Hotels Researched | Number | |
| Newly Discovered This Week | Number | Must not include reclassifications |
| Verified Owner Relationships | Number | |
| Verified Management Co Relationships | Number | |
| Verified Franchise Expirations | Number | |
| Verified Management Expirations | Number | |
| Historical Expirations | Number | |
| Future Expirations | Number | |
| Notice Periods Verified | Number | |
| Notice Deadlines Verified | Number | |
| Brand Conversion Targets (new) | Number | |
| Management Contract Targets (new) | Number | |
| Actionable Opportunities | Number | |
| Research Candidates | Number | |
| HOLD / Quarantined | Number | |
| Documents Processed | Number | |
| Research Cost USD | Number | |
| Track A Hotels | Number | |
| Track B Hotels | Number | |
| Overlap Deduped | Number | |
| Intensive Regions Covered | Number | Actual research only |
| Customer Blocked | Checkbox | Always true for research rows |
| Publication Status | Single select | `INTERNAL_ONLY` |

## Explicit non-goals

- No customer publication views
- No HPC auto-create from scorecard rows
- No Meta schema changes in this proposal
- No overwrite of prior week rows (append-only / week-id unique)

## Approval gate

Founder / Integration Controller must approve before any `ensure-*` or live Airtable write.
