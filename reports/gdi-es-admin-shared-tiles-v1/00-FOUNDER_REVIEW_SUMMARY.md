# GDI Executive Summary Simplification & Admin Dashboards

**Status:** `GDI_EXECUTIVE_SUMMARY_AND_ADMIN_DASHBOARDS_REFINED — AWAITING_FOUNDER_REVIEW`  
**PR:** #626 (cumulative)  
**Port:** `http://127.0.0.1:8080` from `C:\Dev\dealality\ws-gdi`

## Delivered

1. Executive Summary is a single consolidated customer dashboard — **no secondary tabs** (Executive Overview / Sales Performance / Research Intelligence selectors removed).
2. **GDI Sales Performance** and **GDI Research Intelligence** live under Admin Resources (nav + SupportAdminGate + API strip for non-founder/admin).
3. Priority Commercial Opportunities reuse **`DealalityGdiUi.opportunityTileHtml` / `opportunityCardsGridHtml`** (exact Opportunities tiles). OOTW badge is outside the tile. Max **4** tiles. View All Opportunities link present.
4. Customer Research Receipt + Research Productivity remain on Executive Summary.
5. Wyndham STAGED **26** inventory preserved; title gaps for Newstead / Wheeler / Coan remain honest.
6. Permanent shared-tile rule: `docs/ai-build-system/GDI_SHARED_OPPORTUNITY_TILE_COMPONENT_V1.md`.

## Screenshots (port 8080)

| File | Evidence |
|------|----------|
| `01-es-no-secondary-tabs.png` | ES without secondary tabs |
| `02-es-priority-shared-tiles.png` | Priority tiles (shared component, 4-col) |
| `03-opportunities-tiles.png` | Opportunities Tiles View (parity source) |
| `04-admin-resources-nav.png` | Admin Resources nav entries |
| `05-gdi-sales-performance.png` | GDI Sales Performance admin page |
| `06-gdi-research-intelligence.png` | GDI Research Intelligence admin page |
| `07-customer-denied-admin-dashboard.png` | Access restricted gate UI |

## Validation commands

```bash
npm run test:gdi-executive-summary-v2
npm run test:gdi-es-admin-shared-tiles-v1
npm run test:gdi-research-productivity-v1
```

## Founder review URLs

- ES: `http://127.0.0.1:8080/group-demand-intelligence.html?hotelId=recnPB3Q02NO8Ma1o&preview=staged` → Executive Summary tab
- Opportunities tiles: same URL → Group & Demand Opportunities → Tiles
- Admin Sales: `http://127.0.0.1:8080/app#/admin/gdi-sales-performance` or `/app/admin/gdi-sales-performance.html?hotelId=recnPB3Q02NO8Ma1o&preview=staged`
- Admin RI: `http://127.0.0.1:8080/app#/admin/gdi-research-intelligence`

## Notes

- Ensure :8080 is launched from **ws-gdi** (`node scripts/dealality-gdi-local-8080-stable-launch-v1.mjs`). A foreign `server.js` on 8080 serves stale HTML without ES.
- Customer API payloads strip `salesPerformance` + `researchIntelligence` via `stripResearchIntelligencePerspective` / `stripInternalAdminDashboards`.
- No paid research, no CURRENT publication, no merge from GDI chat.
