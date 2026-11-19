# GDI 3.0 PDI-3 — Portfolio Demand Intelligence tab

**Track:** B — Market & Portfolio Intelligence  
**Status:** Implementation PR (after Brief v2 #452 merged)  
**Depends on:** PDI-2 API (#450) · Brief v2 presentation contract (#452)

## Tab placement (binding)

`Group & Demand Opportunities` · **`Portfolio Demand Intelligence`** · `Commercial Priorities`

## Architecture (no second drawer)

| Layer | Reuse |
|---|---|
| Data | PDI-2 read-only portfolio API (`/portfolio/markets|hotels|market-opportunities`) |
| Auth | Verified Memberstack + steward operator mapping (PDI-2 hardened) |
| Fit matrix | PDI-1 fit classes via PDI-2 `hotelAssessments` |
| Opportunity detail | **Existing** `#gdiDrawer` + Brief v2 + Supporting Research Level 2 |
| Unavailable | Explicit UNAVAILABLE when live registry missing; labeled `?pdiDemo=1` DEMO only |

## Checklist

- [x] PDI-2 API routes + security gate merged  
- [x] Brief v2 narrative contract merged (#452)  
- [x] UI tab id `portfolio` inserted left of Commercial Priorities  
- [x] Fit matrix wired to PDI-2 market-opportunities response  
- [x] Drawer open uses existing `openDetail` — no forked content engine  
- [ ] Live operator registry provisioned before customer enablement  

## Explicit non-goals until founder auth

- Production Airtable membership writes  
- Market-level paid Full E2E  
- Customer publication enablement  
