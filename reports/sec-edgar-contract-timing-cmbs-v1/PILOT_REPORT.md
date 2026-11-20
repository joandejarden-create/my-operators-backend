# Hotel Contract Timing Intelligence — CMBS Pilot V1

**Module:** HOTEL_CONTRACT_TIMING_INTELLIGENCE_V1 1.0.0  
**Mode:** APPLY  
**Publication:** CUSTOMER_BLOCKED  
**Merge:** DO NOT MERGE without Integration Controller approval

## Documents researched

| Source family | Count |
|---|---|
| HOTEL_REIT_SCHEDULE | 7 |
| CMBS_424B | 16 |

- Documents OK: **21** / 23 (cap 50)
- Hotels / findings extracted (unique): **57**
- Exact hotel-level contract dates (verified+probable): **57**
- Loan maturities isolated (not used as hotel timing): **0**
- Duplicate discoveries prevented: **0**
- HPC matches: **0** (auto-create: never)
- Historical dates needing revalidation: **52**

## Horizons (forward-looking)

| Window | Count |
|---|---|
| 6 months | 0 |
| 12 months | 0 |
| 24 months | 0 |
| 36 months | 0 |

## Agreement split

- Franchise: 57
- Management: 0
- Termination rights: 0

## Persistence

- Apply: true
- Created: 40
- Dry-run planned: 0
- Duplicates prevented: 0
- Read-back OK: 40
- All research rows: CUSTOMER_BLOCKED

## Yield

- Dates per 100 documents: **271.43**
- Research spend: **$0** (SEC EDGAR only)
- Source precision: **1**

## Scalability

PROMISING — public CMBS/REIT schedules can yield hotel-level franchise/management dates when tables are HTML-parsable; loan maturity isolation is mandatory.

Remaining: Expand CMBS depositor CIKs, EX-10 hotel agreement exhibits, FDD term tables (selected brands), and non-SEC REIT supplements. Revalidate historical dates before CURRENTLY_VERIFIED.

## Guarantees

- Mortgage/loan maturity is never treated as franchise or management expiration
- Portfolio-level date rows are rejected — hotel identity required
- Conditional renewal / termination-right language is preserved as DATE_MEANING, not silent expiration
- CUSTOMER_BLOCKED on all research-only persistence
- No automatic HPC hotel creation
- Registrant / CMBS issuer is not auto-classified as legal owner
