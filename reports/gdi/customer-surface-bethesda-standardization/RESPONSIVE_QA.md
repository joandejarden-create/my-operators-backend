# Responsive / Print QA

## Shared component
YOTEL and Bethesda use the same `brand-card brand-card--gdi-opp` grid (`.gdi-results-grid` / `--list`).

## Checks
- Desktop: tile grid + list mode toolbar
- Mobile: cards stack; contact path may wrap — no campaign section above fold
- Print/PDF: existing GDI report path unchanged; campaigns not injected into customer print

## Residual risk
Long public contact URLs may wrap in footer — same as Bethesda source URLs in detail.
