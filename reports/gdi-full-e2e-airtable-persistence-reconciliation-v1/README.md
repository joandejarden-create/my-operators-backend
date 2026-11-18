# GDI Full E2E — Airtable Persistence & Bethesda Reconciliation V1

**Contract:** `gdi-full-e2e-airtable-persistence-contract-v1`  
**Base:** AI Demand Positioning `appa2cE7FTRmIbB32`  
**Production writes:** blocked until founder approval

## What this delivers

1. Existing-table routing inventory (no new competing tables)
2. Canonical research-object → Airtable / non-Airtable destinations
3. Idempotent dry-run persistence planner + read-back contract
4. Bethesda live Exa run `gdi_e2e_14seg_mwladic1_ff9898` reconciliation
5. Production activation / selective backfill proposal

## Key files

| Path | Role |
|---|---|
| `lib/.../gdi-full-e2e-airtable-persistence-contract-v1.js` | Table IDs + routing |
| `lib/.../gdi-full-e2e-airtable-persistence-dry-run-v1.js` | Dry-run planner |
| `lib/.../gdi-bethesda-exa-airtable-reconciliation-v1.js` | Bethesda recon |
| `scripts/test-gdi-full-e2e-airtable-persistence-reconciliation-v1.mjs` | Gate |
| `reports/.../AIRTABLE_ACTIVATION_PLAN.md` | Activation plan |

## Test

```bash
npm run test:gdi-full-e2e-airtable-persistence-reconciliation-v1
```

## Safety

- No paid research
- No production Airtable mutations
- No CURRENT publication package changes
- Do not backfill all 165 raw Bethesda candidates
