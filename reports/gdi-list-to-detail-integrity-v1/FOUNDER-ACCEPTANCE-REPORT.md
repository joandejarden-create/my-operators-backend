# GDI List→Detail Integrity — Founder Acceptance

**Hotel:** Bethesda Marriott (`recLuxvwwxID7U2B8`)  
**Date:** 2026-11-12  

## Root cause (Regulatory Information Conference)

1. List ID = detail ID = `gdi_opp_regulatory_information_conference_20260930` (same identity).  
2. Record exists in the canonical 59-row hotel store.  
3. List path (Bethesda internal commercial) correctly returned it.  
4. Detail path found the record, then `isGdiTestOrFixtureOpportunity` returned **true** solely because `customerVisible === false` (unpublished to customers ≠ synthetic fixture).  
5. `getGdiOpportunityDetail` mapped that to HTTP 404 `opportunity_not_found`.

Same defect affected **15** of **68** commercial-list rows.

## Architectural correction

- Shared resolver: `gdi-opportunity-id-resolution-v1.js` (`resolveHotelOpportunityById`, `isSyntheticGdiTestOrFixtureOpportunity`, `mayOpenAuthOpportunityDetail`).  
- Auth detail blocks **true synthetics only**; unpublished inventory remains openable.  
- Share/customer surfaces still use `isGdiTestOrFixtureOpportunity` (unpublished stays hidden from customers).  
- Human-readable drawer error; structured server logs keep the code.  
- Permanent gate: `npm run test:gdi-list-to-detail-integrity-v1`.

## Acceptance matrix

| Metric | Result |
|--------|--------|
| Default commercial list count | **68** |
| Detail requests tested | **68** |
| Successful detail requests | **68** |
| Failed detail requests | **0** |
| Cross-hotel mismatch | **0** |
| RIC detail | **PASS** |
| Forbidden commercial IDs | **0** |

Machine-readable: `00-bethesda-list-to-detail-matrix.json`.
