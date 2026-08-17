# Operator Explorer — Operationalization Wave 01 Founder Decisions

**Decision date:** 2026-08-10  
**Status:** APPROVED for operationalization + targeted enrichment Wave 01  
**Does not authorize:** Operator Fit changes, owner pilot, My Deals, mass Research graduation, Test Fixture deletion

## Approvals

| Item | Decision |
| ---- | -------- |
| Canonical universe definitions (`docs/product/operator-explorer-universe-definitions.md`) | **APPROVED** |
| Canonical resolver `lib/operator-explorer/operator-universe.js` | **APPROVED** as OE universe SoT |
| Canonical readiness `lib/operator-explorer/readiness.js` | **APPROVED** as OE readiness SoT |
| Research Record Purpose remains gated from owner-publishable Explorer | **APPROVED** |
| Internal universe dashboard as admin SoT | **APPROVED** |
| Authoritative OE Airtable views (minimum set) | **APPROVED** |
| Targeted enrichment of non-publishable real operators | **APPROVED** |
| No broad re-research of already Strong/Publishable without specific gap | **APPROVED** |

## View creation note

Airtable Metadata API does **not** support creating filtered views. Wave 01 therefore:

1. Syncs filterable `OE *` status fields onto Master from the canonical resolver  
2. Documents exact view recipes for Airtable UI creation  
3. Validates expected counts via API `filterByFormula` equivalent to each view  

## Wave 01 scope lock

- Approximately 8–12 operators  
- Assignment-first  
- Exception-based founder review  
- Webhound Track 2 merge only if `done=true` (else deferred)  
