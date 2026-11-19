# GDI 3.0 PDI-3 — Portfolio tab preparation V1

**Track:** B — Market & Portfolio Intelligence  
**Status:** Planning + fixture prep (no implementation PR while Brief v2 is active)  
**Depends on:** PDI-2 API (#450) · Brief v2 presentation contract

## Tab placement (binding)

`Group & Demand Opportunities` · **`Portfolio Demand Intelligence`** · `Commercial Priorities`

## Architecture (no second drawer)

| Layer | Reuse |
|---|---|
| Data | PDI-2 read-only portfolio API (`/portfolio/markets|hotels|market-opportunities`) |
| Auth | Verified Memberstack + steward operator mapping (PDI-2 hardened) |
| Fit matrix | PDI-1 fit classes |
| Opportunity detail | **Existing** hotel opportunity drawer + Brief v2 narrative contract |

## Prep checklist

- [x] PDI-2 API routes + security gate merged  
- [ ] Brief v2 narrative contract shipping (current Track A PR)  
- [ ] UI tab id `portfolio` inserted left of Commercial Priorities  
- [ ] Fit matrix wired to PDI-2 market-opportunities response  
- [ ] Drawer open uses existing detail route / hash — no forked content engine  
- [ ] Live operator registry provisioned before customer enablement  

## Explicit non-goals until founder auth

- Production Airtable membership writes  
- Market-level paid Full E2E  
- Customer publication enablement  
