# GDI PDI-3 — Portfolio Demand Intelligence tab

## Placement
`Group & Demand Opportunities` · **Portfolio Demand Intelligence** · `Commercial Priorities`

## Architecture
- Data: PDI-2 read-only API (`/portfolio/markets|hotels|market-opportunities`)
- Auth: verified Memberstack (same as PDI-2)
- Drawer: existing `#gdiDrawer` + Brief v2 / Supporting Research (Level 2) — **no second engine**
- Live registry missing → explicit **UNAVAILABLE** (optional labeled `?pdiDemo=1` DEMO only)

## Tests
`npm run test:gdi-pdi-3-portfolio-tab-ui-v1` — PASS

Paid/Airtable/publication: 0
