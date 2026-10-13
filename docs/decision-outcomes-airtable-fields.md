# Decision & Outcome — Airtable fields

**Canonical base:** `appa2cE7FTRmIbB32` (`AIRTABLE_INTELLIGENCE_BASE_ID` / `AIRTABLE_DECISION_OUTCOME_BASE_ID`)  
**Legacy mistaken base (do not write):** Deal Capture MVP `appvtnDurnMSjINP6`  
**Tables:** `Decisions`, `Decision Events`  
**Field map:** `lib/decision-outcomes/field-map.js`  
**Base resolver:** `lib/decision-outcomes/airtable-base.js`  
**Ensure:** `npm run ensure:decision-outcomes-airtable-schema`

Module-neutral. Used by GDI and ADP. Do not use `AI Demand Positioning - Published Reports` as the event store (same base may host ADP report tables + these shared tables).

## Env resolution order

1. `AIRTABLE_INTELLIGENCE_BASE_ID`
2. `AIRTABLE_DECISION_OUTCOME_BASE_ID`
3. `DECISION_OUTCOMES_AIRTABLE_BASE_ID` (compat)
4. `ADP_AIRTABLE_BASE_ID`
5. `AIRTABLE_BASE_ID` (legacy fallback — **blocked** by `WRONG_CANONICAL_AIRTABLE_BASE` when it resolves to MVP unless `DECISION_OUTCOMES_ALLOW_MVP_BASE=1`)

## Decisions

| Field | Type | Notes |
|-------|------|-------|
| decisionId | single line text | Primary; stable id |
| idempotencyKey | single line text | Creation dedupe |
| hotelId | single line text | Required |
| organizationId | single line text | Optional |
| entityId | single line text | Optional |
| productModule | single select | GDI, ADP, … |
| decisionType | single select | |
| subjectType | single select | |
| subjectId | single line text | Opportunity / finding id |
| recommendation | long text | |
| recommendationSummary | long text | |
| recommendationDate | date/time | |
| recommendationVersion | number | |
| decisionStatus | single select | Lifecycle projection summary |
| confidenceScore | number | |
| confidenceBand | single line text | |
| confidenceMethodologyVersion | single line text | |
| evidenceSnapshotSummary | long text | JSON |
| evidenceReferenceIds | long text | JSON array |
| sourceSystem | single line text | |
| schemaVersion | single line text | |
| createdAt / updatedAt | date/time | |
| createdBy / createdByRole | single line text | |
| supersedesDecisionId | single line text | Optional |

## Decision Events

| Field | Type | Notes |
|-------|------|-------|
| eventId | single line text | Primary; append-only key |
| decisionId | single line text | Links to Decision |
| hotelId | single line text | |
| organizationId | single line text | Optional |
| productModule | single select | |
| subjectId | single line text | |
| eventType | single select | VALIDATION / ACTION / OUTCOME |
| eventSubtype | single line text | e.g. CONTACTED, LOST |
| eventValue | single line text | |
| reason / note | text | |
| userId / userRole | single line text | |
| eventDate / reportedAt / createdAt | date/time | |
| financialValue / revenueValue | currency | Optional |
| roomNights | number | Optional |
| sourceSurface | single line text | |
| schemaVersion | single line text | |
| causalConfidence | single line text | Default UNKNOWN for ADP |
| measurementChange / temporalAssociation / rawEventJson | long text | JSON |

## Cutover

```bash
# Point env at appa2cE7FTRmIbB32 (see .env.example)
npm run ensure:decision-outcomes-airtable-schema -- --apply
npm run ensure:gdi-opportunities-airtable-schema -- --apply
node scripts/migrate-canonical-airtable-mvp-to-intelligence-base.mjs --apply
node scripts/verify-canonical-airtable-cutover.mjs
```

Legacy MVP rows retained as `LEGACY_MIGRATED_SOURCE` until an explicit delete task.
