# DealSignal — Qualified Opportunity classification V1

**Status:** Permanent founder rule (2026-11-23)  
**Publication:** CUSTOMER_BLOCKED  
**Module:** `lib/hotel-intelligence/dealsignal/dealsignal-qualified-opportunity-classification-v1.js`  
**Cursor rule:** `.cursor/rules/dealsignal-qualified-opportunity-classification.mdc`  
**Test:** `npm run test:dealsignal-qualified-opportunity-classification-v1`

## Problem

Verified commercial events (completed reflags, MFA renewals, appointed operators, executed management replacements) were repeatedly miscounted as open **Qualified** opportunities.

## Rule

Completed or committed brand conversions, renewed agreements, retained partnerships, and awarded management contracts must **never** be counted as open Qualified Opportunities solely because they are verified commercial events.

Preserve them as **Historical / Executed / Committed / Retained** with original URLs, chronology, and owner intelligence.

## Qualified (all three required)

1. A relevant **BCT/MCT** decision exists  
2. The decision remains **unresolved**  
3. A **credible competitive engagement pathway** is evidenced  

Formal RFP is **not** required.

## Do not

- Promote historical Verified facts → Verified **open**  
- Force Early / Emerging into Qualified  
- Inflate Qualified counts for commercial critical-mass scorecards  
- Treat expiry / at-will / auto-renewal alone as Qualified  

## Examples (not Qualified)

| Event | Correct class |
|---|---|
| AC Hotel Cartagena completed reflag | Historical / Executed |
| GHL–Sonesta MFA through 2034 | Retained / Renewed |
| Completed franchise renewals | Retained / Renewed |
| Already-appointed operators | Committed / Closed |
| Executed management replacements | Historical / Executed |

## Integration Controller

Central eligibility / reader counting must apply the same non-open exclusion. Track A research assessment labels are **not** new Airtable enums by themselves.
