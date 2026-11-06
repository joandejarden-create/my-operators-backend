# ADP Customer-Visible Parity Contract

**Gate:** `certifyAdpCustomerVisibleParity()`  
**Module:** `adp-customer-visible-parity-certification-v1.js`

## Principle

**HTTP 200 is never sufficient.** The customer-visible report must render without blocked-report errors, grain mismatches, or JS failures.

## Surfaces

1. Admin published report  
2. Customer-facing report API  
3. External share API  
4. Full browser-rendered customer report  
5. PDF preview  
6. PDF download  
7. Provider Presence charts  
8. Scenario Presence charts  
9. Territory and competitive sections  
10. Historical report references  

## Required equality (when surface provided)

- Correct hotel (`propertyId`)
- Correct measurement period
- Correct methodology fingerprint
- Correct Consideration (≤0.05 pp)
- Provider Presence grain pass + client render not blocked
- Publication manifest SHA (when present)
- PDF artifact SHA (PDF surfaces)
- No `blockedReport` / `jsError` / missing mandatory sections

## CUSTOMER_READY

A report cannot become `CUSTOMER_READY` without:

1. Passing `certifyAdpMeasurementReconciliation`
2. Passing `certifyAdpImmutablePackage`
3. Passing `certifyAdpCustomerVisibleParity` with live surfaces (browser + share + PDF)

## Local status (this phase)

- Package-level render safety certified for 17 Group A + 21 prepared Group B packages
- Live production surface probes **not** executed (no production activation)
- Prior Hilton production block is the reference failure mode this contract prevents
