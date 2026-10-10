# UI QA — Hilton GDI

## Bag / gate (authoritative)
- Ready: **8** (same gate as yield audit)
- Renaissance Ready: **8**
- Facing filter applied via `filterCustomerFacingOpportunities` + `isGdiCustomerOpportunityReady`
- Campaign packs / second-gen: **0**
- Bethesda shared cards: **0**
- Venue/organizer shells promoted to Ready: **NO**

## Browser share
- Share URL with `?share=gdisht_150bd6bba6e32716d3a84999` returned “Access link unavailable” (signing/env) — not a Ready-count defect.
- ADP browser QA on Hilton completed PASS separately.

## Checklist
- [x] Ready count = 8 (gate)
- [x] No generators/campaign packs
- [x] No Bethesda shared cards
- [x] Shells not Ready
- [ ] Share UI cards (blocked by share-link env)
