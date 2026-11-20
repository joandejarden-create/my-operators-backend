# ADP 2.0 Phase 1C / PR-2 — Attribute Selection Governance Closeout

**Scope:** Recommendation engine, min/max coverage policy, semantic model, definition/recognition validation, Option B Airtable contract.  
**No Airtable writes / schema creation / customer RG cutover / paid research.**  
**Founder corrections (pre-merge):** min/max Airtable fields from shared policy · Spice 20↔22 reconciliation · lifecycle DRAFT→SUPERSEDED with fixture → REVIEW_REQUIRED.

## Coverage policy (shared)

| Limit | Value |
|---|---|
| Minimum Eligible Required | 12 unique eligible concepts |
| Preferred Range | 15–18 |
| Maximum Eligible Allowed | 20 |
| Policy version | `ADP_MEASUREMENT_COVERAGE_POLICY_V1` |
| Enforcement | Diagnostic only (no production research blocking) |

Statuses: `BELOW_MINIMUM_ATTRIBUTE_COVERAGE` · `MEASUREMENT_COVERAGE_COMPLIANT` · `ABOVE_MAXIMUM_ATTRIBUTE_COVERAGE`

Full candidate / research inventory is **not** capped at 20. Only the measurement shortlist / approved measurement inventory must satisfy the policy.

## Semantic reconciliation (Spice Island 20 → 22)

| Rules | Unique eligible concepts |
|---|---|
| Phase 1B aggressive group collapse | **20** |
| `ADP_SEMANTIC_EQUIVALENCE_RULES_V1_1` | **22** |
| Delta | **+2** |

**Why +2:** Phase 1B collapsed every co-member of a semantic group (including RELATED pairs) into one concept. RULES_V1_1 collapses only `TRUE_DUPLICATE` and `BROADER_NARROWER`, and keeps `RELATED_BUT_DISTINCT` when independently measurable:

| Pair | Relation | Count |
|---|---|---|
| beachfront / oceanfront | TRUE_DUPLICATE | 1 concept (oceanfront collapsed) |
| spa / full_service_spa | BROADER_NARROWER | 1 concept (full_service_spa collapsed) |
| luxury / five_diamond | RELATED_BUT_DISTINCT | **2** (independently measurable) |
| meeting_space / event_space_outdoor | RELATED_BUT_DISTINCT | **2** (independently measurable) |

Coverage diagnostics and the proposed measurement shortlist use the **same** RULES_V1_1 collapse.

## Pilot results

### Bethesda Marriott
- Selected raw: 15 (preserved)
- Unique eligible concepts: below minimum (shortfall in latest JSON)
- Status: `BELOW_MINIMUM_ATTRIBUTE_COVERAGE`
- Recommendations include ADD/VERIFY candidates — not auto-approved
- Existing selection preserved

### Spice Island Beach Resort
- Selected raw: **24** (preserved; full inventory uncapped)
- Unique eligible concepts: **22** → `ABOVE_MAXIMUM_ATTRIBUTE_COVERAGE`
- Proposed measurement shortlist: **20** unique concepts (satisfies min/max)
- Proposed exclusions (4 — steward decision; raw selection not auto-removed):
  1. `oceanfront` — TRUE_DUPLICATE of `beachfront` (beachfront kept)
  2. `full_service_spa` — BROADER_NARROWER of `spa` (spa kept)
  3. `tennis_courts` — capacity under max-20 unique-concept policy
  4. `full_service` — capacity under max-20 unique-concept policy
- No invented Reality Gap scores; historical RG unchanged

### YOTEL Geneva Lake
- BELOW_MINIMUM
- `airport_shuttle` / `park_and_fly`: selected, undefined, recognition missing
- Factual support: profile positioning hints only — **not** customer-enabled
- Y1 blocked pending founder / steward verification — **no definitions activated**

## Semantic model

Relations: TRUE_DUPLICATE · BROADER_NARROWER · RELATED_BUT_DISTINCT · DISTINCT  
Conservative counting: TRUE_DUPLICATE + BROADER_NARROWER collapse. RELATED independently measurable stay separate.

## Option B Airtable

Tables: `ADP Priority Attribute Configurations` + `ADP Priority Attribute Lines`  
Base: `appa2cE7FTRmIbB32`  
Lifecycle: **DRAFT → SUGGESTED → REVIEW_REQUIRED → APPROVED → SUPERSEDED**  
Draft edits never mutate Active APPROVED; SUPERSEDED history remains retrievable.  
Fixture migration: **REVIEW_REQUIRED** (never auto-APPROVED)  
Classification: **PERSISTENCE_DESIGNED** (no schema creation / writes in this PR)

## Persistence / methodology

- Customer RG / historical periods / weights / certified research unchanged
- PR-3 requires separate founder auth for schema + writes

## Tests

- `npm run test:adp-attribute-selection-governance-v1`
- `npm run test:adp-verified-context-bridge-v1`
- `npm run adp-attribute-selection-governance`
