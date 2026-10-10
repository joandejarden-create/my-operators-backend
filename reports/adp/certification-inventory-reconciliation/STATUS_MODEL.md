# Status Model

## Dimensions (orthogonal)

1. **PROPERTY_QA_STATUS** — PASS | REVIEW | FAIL  
   Engine dry-run quality of the current period (hard/review flags). Does **not** mean period is CERTIFIED.

2. **PERIOD_CERTIFICATION_STATUS** — DRAFT | QA_REVIEW_REQUIRED | QA_FAILED | CERTIFIED | LEGACY_UNCERTIFIED | SUPERSEDED | **LEGACY_QA_PASS** (reporting)  
   Governed certification state. Stale `CERTIFIED` stamps on pre-framework manifests do **not** count as CERTIFIED.

3. **CURRENT_PERIOD_CLASS** — CERTIFICATION_ERA_OFFICIAL | LEGACY_OFFICIAL | NO_OFFICIAL_PERIOD  
   Whether the Live period was created under `adp_global_certification_engine_v1`.

4. **PUBLISH_ELIGIBILITY** — ALLOWED | BLOCKED | GRANDFATHERED  
   New official publish vs customer-read grandfathering.

5. **LEGACY_STATUS** — NOT_LEGACY | LEGACY_CURRENT | LEGACY_SUPERSEDED | NO_PERIOD

6. **COMPARABILITY_STATUS** — EXACT_COMPARABLE | COMMON_SET_COMPARABLE | DIRECTIONAL_ONLY | NOT_COMPARABLE | NOT_EVALUATED

## Rules
- Property QA PASS ≠ period CERTIFIED
- LEGACY_QA_PASS = engineWouldCertify CERTIFIED + legacy period class
- Only CERTIFICATION_ERA_OFFICIAL + CERTIFIED counts in CERTIFICATION-ERA CERTIFIED PERIOD COUNT
- Never report combined label `CERTIFIED/PASS`
