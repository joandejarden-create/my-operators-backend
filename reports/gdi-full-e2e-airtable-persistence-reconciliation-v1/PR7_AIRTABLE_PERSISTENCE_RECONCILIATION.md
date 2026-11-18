# PR7 — Mandatory Airtable persistence & Full E2E reconciliation

Integrates into the Discovery Intelligence Full E2E enhancement sequence after PR1–PR6 / founder-preview work.

## Scope

- Contract + routing to existing ADP GDI tables  
- Dry-run persistence planner + read-back outcomes  
- Bethesda Exa run reconciliation (read-only)  
- Activation / selective backfill proposal  
- Chain dry-run checkpoint  

## Out of scope

- Production Airtable mutations  
- Paid research  
- Publication CURRENT changes  
- New competing Airtable tables/bases  

## Gate

`npm run test:gdi-full-e2e-airtable-persistence-reconciliation-v1`
