# ADP 2.0 Phase 1C / PR-2 — Attribute Selection Governance Closeout

**Scope:** Recommendation engine, min/max coverage policy, semantic model, definition/recognition validation, Option B Airtable contract.  
**No Airtable writes / schema creation / customer RG cutover / paid research.**

## Coverage policy

| Limit | Value |
|---|---|
| Minimum | 12 unique eligible concepts |
| Preferred | 15–18 |
| Maximum | 20 |
| Enforcement | Diagnostic only |

Statuses: `BELOW_MINIMUM_ATTRIBUTE_COVERAGE` · `MEASUREMENT_COVERAGE_COMPLIANT` · `ABOVE_MAXIMUM_ATTRIBUTE_COVERAGE`

## Pilot results

### Bethesda Marriott
- Selected raw: 15
- Unique eligible concepts: below minimum (shortfall reported in latest JSON)
- Status: `BELOW_MINIMUM_ATTRIBUTE_COVERAGE`
- Recommendations include ADD/VERIFY candidates (e.g. large_ballroom) — not auto-approved
- Existing selection preserved

### Spice Island Beach Resort
- Selected raw: 24 (preserved)
- Unique eligible concepts: **22** (TRUE_DUPLICATE + BROADER_NARROWER collapsed; RELATED_BUT_DISTINCT remain separate)
- Status: `ABOVE_MAXIMUM_ATTRIBUTE_COVERAGE`
- Prioritized shortlist ≤ 20 with proposed exclusions for steward review (no auto-remove)
- No invented Reality Gap scores

### YOTEL Geneva Lake
- BELOW_MINIMUM
- `airport_shuttle` / `park_and_fly`: selected, undefined, recognition missing
- Factual support: profile positioning hints only — **not** customer-enabled
- Y1 blocked pending founder

## Semantic model

Relations: TRUE_DUPLICATE · BROADER_NARROWER · RELATED_BUT_DISTINCT · DISTINCT  
Only TRUE_DUPLICATE (and default BROADER_NARROWER) collapse for unique-concept counting.

## Option B Airtable

Tables: `ADP Priority Attribute Configurations` + `ADP Priority Attribute Lines`  
Base: `appa2cE7FTRmIbB32`  
Lifecycle: DRAFT → SUGGESTED → REVIEW_REQUIRED → APPROVED → SUPERSEDED  
Fixture migration: **REVIEW_REQUIRED** (never auto-APPROVED)  
Classification: **PERSISTENCE_DESIGNED**

## Persistence / methodology

- Customer RG / historical periods / weights unchanged
- PR-3 requires separate founder auth for schema + writes

## Tests

- `npm run test:adp-attribute-selection-governance-v1`
- `npm run test:adp-verified-context-bridge-v1`
- `npm run adp-attribute-selection-governance`
