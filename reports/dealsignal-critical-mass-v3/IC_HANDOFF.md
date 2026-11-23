# Integration Controller Handoff — Critical-Mass V3

**As of:** 2026-11-23  
**Stream:** Track A DealSignal · `DEALSIGNAL_CRITICAL_MASS_V3`  
**Publication:** CUSTOMER_BLOCKED  
**Status:** `AWAITING_INTEGRATION_CONTROLLER`

## Ask

1. Review + merge PR after CI (gitleaks / path checks).  
2. Do **not** publish CUSTOMER_BLOCKED Airtable records to customer products.  
3. Note permanent Qualified classifier module if not yet on `main` (`lib/hotel-intelligence/dealsignal/dealsignal-qualified-opportunity-classification-v1.js`).  
4. Post-merge: `dealality:post-merge-sync` for owning stream when applicable.

## Scorecard (permanent rule)

| Metric | Value |
|---|---|
| Starting open Qualified (rule) | **2** |
| Newly verified Qualified | **3** |
| Total open Qualified | **5** |
| Ideal target | 20 (not met; standards not relaxed) |
| Airtable creates (first apply) | 20 (10 inv + 10 findings) |
| Idempotent | 20 noops |
| Paid research | $0 |

## Fail-closed

No CUSTOMER_PUBLISHED · No schema expansion · No FINN volume rehash · No Qualified inflation from committed/historical.
