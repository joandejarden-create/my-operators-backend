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

## Authorization (hardened)

- Requires **verified** Memberstack → `req.dealalityUser` (`gdiPortfolioReadAuth`).
- **Never** trust `x-dealality-member-id`, `x-dealality-session-id`, query `sessionId`, company-name, or notes.
- Client `operatorId` may only match steward mapping — never grant access.
- Market allowlist: missing → deny all; empty `[]` → deny all; non-empty → listed markets only.
- Cross-operator requests denied.
- Hotels only via PDI-1 membership for operator × market.

## Publication boundary

- **Customer-published is the default** for market-opportunity retrieval.
- Internal research requires steward privilege `GDI_INTERNAL_RESEARCH_READ` (not a client flag).
- Opportunities without matching `marketId` are excluded.
- Hotel publication is independent (`publishedHotelIds` / `hotelPublication`); market publish ≠ all hotels.
- Unpublished hotels withhold drawer links on the customer surface.

## Runtime isolation

- Deployed HTTP path loads **only** `data/group-demand-intelligence/portfolio-demand-intelligence/*`.
- Fixtures are offline-test only; missing live registry → **503 fail-closed**.

## Offline test

```bash
npm run test:gdi-pdi-2-readonly-portfolio-api-v1
```

## Non-goals

- Portfolio tab UI (PDI-3)
- Production Airtable writes / new tables
- Paid market research
- Second drawer engine
