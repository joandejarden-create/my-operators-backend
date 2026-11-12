# PR #220 — Final Security Gate (Sales-Team Inventory Access Control)

## Auth middleware trace

| Flag / object | Set by | When |
|---|---|---|
| `req.dealalityUser` | `middleware/requireDealalityUser.js` (after `memberstackAuth`) | Bearer Memberstack session resolves an Airtable Users row (or local demo fallback) |
| `req.gdiPilotUnauthRead` | `middleware/gdiPilotReadAuth.js` | No Bearer token **and** `isGroupDemandIntelligencePilotReadAllowed()` (local default ON / `GROUP_DEMAND_INTELLIGENCE_PILOT_READ=1` / V1 flag) |

GDI opportunity list, detail, and CSV routes use `gdiPilotReadAuth` (`server.js`).

## Hotel-scoped sales inventory guard

`assertHotelScopedSalesTeamInventoryAccess` + updated `resolveGdiInventoryAudience`:

1. **Allowlist** — `GDI_SALES_TEAM_INVENTORY_HOTEL_IDS` (default: Bethesda only). Pilot unauth cannot expand inventory for other hotels.
2. **Cross-hotel user** — `dealalityUser.allowedHotelIds` / `gdiHotelIds` / `hotelIds` that exclude Bethesda → published feed only (19).
3. **Share / external** — always published feed.
4. **Anonymous** (no pilot, no user) — published feed.
5. **Detail** — `assertOpportunityReadableForInventoryAudience` mirrors list/CSV; unpublished baseline IDs 404 under published audience.

Local Bethesda Opportunities with pilot-unauth remain **67 + 7 CMI**; published/share remain **19**. No opportunity data or publication approvals changed.

## Tests

- `npm run test:gdi-bethesda-67-inventory-access-control-v1` — PASS (16)
- `npm run test:gdi-bethesda-67-sales-team-inventory-display-v1` — PASS
- `npm run test:gdi-bethesda-67-sales-baseline-cmi-v1` — PASS
