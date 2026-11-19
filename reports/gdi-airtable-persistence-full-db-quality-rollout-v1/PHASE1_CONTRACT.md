# Phase 1 — Airtable persistence contract (Sheraton reference)

**Status:** Slim fact↔source extension implemented; offline dry-run PASS (8/8). Live write/read-back pending Airtable credentials in GDI runtime.  
**Hotel:** Sheraton Guadalajara Expo `recCAGqkERSXgiku4`  
**Base:** `appa2cE7FTRmIbB32` (intelligence / GDI)

## Existing tables (no new schema)

| Role | Table | ID |
|------|-------|----|
| Research inventory (primary write for this rollout) | GDI Hotel Research Opportunities | `tblbkDj0vhQLfrssV` |
| Qualified opportunities | Group Demand Opportunities | `tblRuReslJMwsfRQj` |
| DG / Program / Signal / Fit | existing Demand* tables | see Full E2E contract |

Contacts, fact catalog, and source register remain in `researchPayloadJson` (and GDO `opportunityPayloadJson` when synced) — **no Contacts/Sources/Facts tables**.

## Slice 2A gap closed (smallest justified extension)

`gdi-research-inventory-slim-payload-v1.js` now retain:

- `commercialFactCatalog` (facts + `sourceIds` + durableRef)
- `commercialSourceRegister` (sources + `sectionSourceIds`)
- compact `sectionCitations`

Still fail-closed at 95k (no silent truncate). Publication / sales / hotel-private / telemetry keys remain stripped.

`assessSourceRegisterPersistence` accepts a slim **payload object** and can certify fact↔source retention.

## Controlled write gates

```bash
# Dry-run (default)
node -r dotenv/config scripts/gdi-sheraton-airtable-persistence-certification-v1.mjs --limit 12

# Apply (Sheraton sample only)
set GDI_RESEARCH_INVENTORY_PRODUCTION_WRITES=1
set FOUNDER_SHERATON_PERSISTENCE_APPLY=1
node -r dotenv/config scripts/gdi-sheraton-airtable-persistence-certification-v1.mjs --apply --limit 5
```

Use Integration Controller `.env` via `DOTENV_CONFIG_PATH` when `ws-gdi` has no local Airtable token (do not copy secrets into the product worktree).

## Excluded from updates

All `PUBLICATION_STRIP_KEYS` / publication-owned fields; sales-owned feedback; Surfe/provider session channels; locked published customer packages.

## Certification vocabulary

- `PROTOTYPE` — offline slim/composition only  
- `PERSISTENCE_DESIGNED` — contract + dry-run plan against live schema  
- `PERSISTENCE_VALIDATED` — write + read-back reconstructs fact graph / contacts  
- `FULL_E2E_VALIDATED` — only after database-wide certification (Phase 7)

## Inventory reconciliation (canonical identity)

Union-deduped comparison inventory: **79** opportunities  
(`CURRENT_STAGED` / local / historical Full E2E) — not interchangeable with staged-only 38 or local-only 43.
