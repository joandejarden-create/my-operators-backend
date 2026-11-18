# Existing workflow read / write map

**Base:** `appa2cE7FTRmIbB32`  
**Production Full E2E writes:** still fail-closed (`AIRTABLE_PRODUCTION_WRITES_NOT_AUTHORIZED`).

## By workflow

| Workflow | Reads | Writes (when authorized) | Key modules |
|----------|-------|--------------------------|-------------|
| Opportunity sync / research persist | GDO | GDO upsert | `airtable-opportunity-store.js`, `gdi-airtable-sync-v1.js`, `opportunity-persistence.js` |
| Demand generator graph | DG, Programs, Signals, Fit | upsert* | `demand-generators/airtable-stores.js` |
| Private events | Venues, Signals, Venue Fit (+ GDO) | PE upserts → optional GDO | `private-events/*`, `canonical-persist-and-link-v1.js` |
| Research coverage / onboard | Targets, Runs, Target Runs (+ DG/Fit) | upsertResearch* ; onboard graph | `research-coverage/airtable-stores.js`, `onboard-hotel-research-graph.js` |
| Need-date portal | GDI profiles, need periods, history, matches, campaigns; **reads** facility Hotel Commercial Profiles for link | Portal saves + match writers | `need-date-airtable-store-v3.js`, `hotel-commercial-profile-store-v1.js`, `api/gdi-need-dates-portal.js` |
| Decisions / hotel feedback | Decisions, Events (+ FS feedback merge) | saveDecision / appendEvent | `decision-outcomes/airtable-store.js`, GDI feedback bridges |
| Hotel intelligence / AVA | Facility tables + Evidence | `applyHotelIntelligencePacket`; AVA evidence sync | `hi-airtable-store.js`, `ava-audit-airtable-sync-v1.js` |
| Canonical Full E2E | Plans destinations via contract | **Dry-run only** until founder auth | `gdi-full-e2e-airtable-persistence-contract-v1.js`, `…-dry-run-v1.js` |
| Customer publication | Packages on disk | **Never** via Airtable persistence path | `bundled-publication-packages/` |

## By table (active adapters)

See explore map / contract. Ensure scripts (schema only; not run in this audit):

- `ensure-gdi-opportunities-airtable-schema.mjs`
- `ensure-gdi-demand-generators-airtable-schema.mjs`
- `ensure-gdi-private-events-airtable-schema.mjs`
- `ensure-gdi-research-coverage-schema.mjs`
- `ensure-decision-outcomes-airtable-schema.mjs`

## Update frequency (observed)

| Layer | Cadence (inferred) |
|-------|--------------------|
| GDO | Per research sync / hotel campaigns (GDO +36 since 2026-11-13) |
| DG/Programs/Signals/Fit | Burst on onboard / PE / corporate persist; many Fit seeds older |
| Research Targets/Runs | Sparse recently (counts unchanged since prior audit) |
| Need periods / matches | Portal + provisional scenario loads (Need Periods +9; Matches +20) |
| Decisions/Events | Feedback / validation sessions |
| Facility HI / Evidence | HI research + AVA sync |

## Publication sensitivity

- GDO + research tables may contain **non-customer-approved** research — never treat as CURRENT publication.
- Need periods may be hotel-reported **or** test/provisional — customer icons must continue to ignore TEST_SCENARIO / SYNTHETIC (existing presentation guards).
- Decisions/Events and hotel feedback are confidential — never sync cross-hotel or into global DG.
