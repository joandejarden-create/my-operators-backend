# Production Eligibility Policy (Steward)

## AUTO_WRITE_ELIGIBLE — all must pass

1. Identity Exact or High
2. No unresolved identity conflict
3. Required provenance present on material fields
4. Source rights permit persistence for those fields
5. No material contradiction
6. Priority completeness ≥ steward threshold (default 95% for full auto; field-class aware for partial)
7. No prohibited inference
8. No Cvent/legacy contamination (`cvent_used_as_production_evidence=false`)
9. Geography valid (Dealality taxonomy)
10. Required schema fields valid

## Field classes
See `15-field-write-classes.json` — only Class A/B may auto-write when rights pass; C steward; D first-party preferred; E never.

## SerpApi
Technically eligible ≠ rights eligible. Until written SerpApi clarification, SerpApi-derived fields are **staging only**.
