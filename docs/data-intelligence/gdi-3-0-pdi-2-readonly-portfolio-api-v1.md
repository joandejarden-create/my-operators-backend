# GDI 3.0 PDI-2 — Read-only portfolio API V1

**Track:** B — Market & Portfolio Intelligence  
**Depends on:** PDI-1 (#446), Slice 2B (#448)

## Endpoints (read-only)

| Method | Path | Purpose |
|---|---|---|
| GET | `/api/group-demand-intelligence/portfolio/markets` | Markets for authorized operator |
| GET | `/api/group-demand-intelligence/portfolio/hotels?marketId=` | Explicit membership hotels |
| GET | `/api/group-demand-intelligence/portfolio/market-opportunities?marketId=` | Shared market packets + per-hotel fit + drawer links |

Optional query: `marketOpportunityId`, `hotelId`, `customerSurface=1`, `operatorId`.

## Authorization

- Requires authenticated Memberstack / session mapped in steward registry.
- **Never** authorize by company-name, notes, or fuzzy text.
- Cross-operator requests denied.
- Hotels returned only via PDI-1 membership for operator × market.
- Wrong-market hotels (e.g. Cancún under Guadalajara) denied.

## Response guarantees

- Shared public market packet only (confidential keys stripped).
- Independent hotel fit classes (PDI-1 / `evaluateCrossHotelFit`).
- Drawer links to existing hotel opportunity detail routes.
- `customerSurface=1` filters unpublished / RESEARCH_ONLY rows.

## Offline test

```bash
npm run test:gdi-pdi-2-readonly-portfolio-api-v1
```

## Non-goals

- Portfolio tab UI (PDI-3)
- Production Airtable writes / new tables
- Paid market research
- Second drawer engine
