# ADP Airtable Persistence Contract

**Contract:** `ADP_AIRTABLE_ECOSYSTEM_CONTRACT_V1` (existing) + permanent certification linkage  
**Status this phase:** **READ-ONLY audit / design.** No Airtable writes authorized.

## Tables in scope

| Table | Role |
|-------|------|
| AI Demand Positioning - Published Reports | Publication metadata (publication-only) |
| ADP Monitoring Periods | Period identity |
| ADP Report Snapshots | Snapshot lineage |
| ADP Period Metrics | Consideration / SP / KPIs |
| ADP Territory Metrics | Territory breakdowns |
| ADP Provider Metrics | Provider Presence |
| ADP Competitive Rankings | Competitive set |
| ADP Evidence Index | Evidence pointers |
| ADP Observation Prompt Ledger | Prompt + observation linkage (primary key) |
| ADP Report Corrections | Correction / supersession lineage |

## Rules

1. **Idempotent writes** when authorized (stable propertyId + periodId keys).
2. **Immutable historical versions** — never overwrite prior period metrics.
3. **Explicit current-version designation** separate from historical rows.
4. **Correction/supersession lineage** required when Group B corrected packages are later published.
5. **Persistence completeness checks** before `PERSISTENCE_VERIFIED`.
6. Airtable is the **historical research/measurement record**.
7. **Durable published registry** remains authoritative for customer-visible current publications.
8. Airtable **must not silently override** approved publication values.

## Full E2E integration

Stage `S9B_AIRTABLE_HISTORY_PERSISTENCE` remains gated by:

- `ADP_HISTORY_WRITES_ENABLED`
- `ADP_HISTORY_AIRTABLE_WRITE_APPLY`
- `allowGovernedProductionHistoryBase`

Permanent certification artifact (`certificationHash`) must be recorded on period/history rows when writes are authorized in a future founder-approved phase.

## This phase

- No Airtable mutations
- No backfills
- Persistence readiness = contract + gates present; production write backlog remains separate (see prior Airtable persistence assessment: INCOMPLETE live overlay)
