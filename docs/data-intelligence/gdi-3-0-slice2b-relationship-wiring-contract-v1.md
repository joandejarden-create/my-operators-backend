# GDI 3.0 Slice 2B — Relationship wiring contract V1

**Track:** A — Research & Commercial Intelligence  
**Status:** Implementation PR (offline / $0)  
**Depends on:** Slice 2A persistence honesty (#444)

## Purpose

Wire Full E2E research opportunities to durable **Demand Generator / Partner / Program / Signal / Hotel Fit** identities using the **existing** Airtable architecture — no new tables, no production writes in this slice.

## Reused structures

| Entity | Existing table / module |
|---|---|
| Demand Generator | `Demand Generators` · `demand-generators/entities.js` |
| Demand Program | `Demand Programs` · `buildProgramEntity` + `mayUpsertDemandProgram` |
| Demand Generator Signal | `Demand Generator Signals` · `qualify-promote.js` |
| Hotel Demand Generator Fit | `Hotel Demand Generator Fit` · hotel-scoped |
| Research inventory link | typed `dgGeneratorId` / `dgProgramId` / `dgSignalId` on slim payload |

## Rules

1. Partner/planner organizations receive independent DG ids when their name differs from the primary org.  
2. Opportunities link to reusable orgs via stable `dgGeneratorId` (not opportunity-local strings alone).  
3. Market opportunity id (when present) remains distinct from hotel opportunity id.  
4. Evidence URLs and named contacts stay on the opportunity / slim payload; DG entities carry source URL refs only.  
5. Idempotent memory upserts: `CREATED` / `UPDATED` / `UNCHANGED` / `DUPLICATE_LINKED` / `DEFERRED`.  
6. Shared market / org intelligence never includes `hotelSalesFeedback` or other hotel-private keys.  
7. Programs require recurrence evidence (`mayUpsertDemandProgram`); otherwise `DEFERRED`.

## Offline certification

```bash
npm run test:gdi-slice2b-relationship-wiring-v1
```

Reports: `reports/gdi-slice2b-relationship-wiring-v1/`

## Non-goals

- Production Airtable writes  
- New Contacts / Partners / Competitor tables  
- Slice 2C default-on DG reuse  
- Portfolio UI (PDI-3)  
- Paid research  

## Coordination

Shared identity / fit changes must be coordinated with Track B (PDI-1 membership + fit classes). See `gdi-3-0-dual-track-roadmap-v1.md`.
