# GDI PDI-3 — Portfolio Demand Intelligence tab (hardened)

## Placement
`Group & Demand Opportunities` · **Commercial Priorities** · How to Read This Report · `Portfolio Demand Intelligence`

## Hardening
- `?pdiDemo=1` **only** on localhost / 127.0.0.1 / `[::1]` / 0.0.0.0 — ignored on production hosts
- Fit labels aligned to full PDI-1 contract (POTENTIAL_PRIMARY_HOST, OVERFLOW_ACCOMMODATION, PREFERRED_LODGING_OPPORTUNITY, MEETING_CATERING_OPPORTUNITY, OTHER_PARTNERSHIP, UNSUITABLE, INSUFFICIENT_EVIDENCE)
- Level 2: open existing drawer when published; otherwise explicit **unavailable** (no promised content)
- Smoke: market matrix + open attrs + missing/unpublished states (`NODE_DOM_SMOKE`)

## Architecture
- PDI-2 read-only API + auth unchanged
- Single `#gdiDrawer` — no second engine

Paid/Airtable/publication: 0
