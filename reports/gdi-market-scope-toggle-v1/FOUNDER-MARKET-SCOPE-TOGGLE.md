# GDI Market Scope Toggle V1

## Summary

Added one **Market Scope** segmented control to the GDI Opportunities toolbar (All / Hotel / Competitive), immediately to the right of Opportunity Type on desktop. Commercial population selection was consolidated out of the Opportunity Type dropdown so the two controls no longer duplicate each other.

CMI remains an orthogonal intelligence classification — **not** a fifteenth demand segment.

## Before → After

| Control | Before | After |
|---|---|---|
| Opportunity Type | Sales Opportunities / CMI / All Commercial Intelligence **plus** demand categories | Demand categories only (`All types` + segment categories) |
| Market Scope | *(missing)* | **All / Hotel / Competitive** with live counts |
| Default / Reset View | Sales Opportunities | **All Opportunities** (hotel preserved) |

## Bethesda filter counts (authorized sales-team inventory)

| Scope | Unique IDs | Notes |
|---|---:|---|
| All | **67** | Unique baseline IDs; dual-classified rows counted once |
| Hotel | **61** (dynamic) | Commercially actionable for the property — excludes confirmed competitor bookings without independent target-hotel demand |
| Competitive | **10** (dynamic) | Recovered CMI ledger: 6 verified / 3 needs verification / 1 historical (not hardcoded; Competitive total ≠ verified bookings) |

Unique-ID reconciliation: `All = 67`; Hotel excludes 6 confirmed competitor bookings without independent target-hotel demand; dual membership counted once in All.

## Authorization (unchanged)

Market Scope is a **display filter only**.

| Audience | Count | Scope stamp |
|---|---:|---|
| Sales-team inventory (local capability / admin) | 67 | `BETHESDA_SALES_TEAM_INVENTORY_67` |
| Published customer feed (`inventoryAudience=published`) | 19 | `BETHESDA_PUBLISHED_CUSTOMER_FEED_19` |

No publication widening. Detail + CSV remain hotel-scoped and server-authorized.

## Multi-hotel

- AC Hotel A Coruña: Market Scope renders `All (0) / Hotel (0) / Competitive (0)` with empty state.
- No Bethesda ledger hardcoding in the toggle UI — counts come from the loaded hotel dataset + `cmiVerified` / `inventoryPopulations`.

## CSV / detail parity

- Export prefers currently visible row `ids=` (subject to server auth).
- Fallback query maps Market Scope → inventory population `opportunityType` for the API filter.
- Detail drawer unchanged; competitor / sourcing / actionability dimensions preserved.

## Browser certification

Screenshots under `reports/gdi-market-scope-toggle-v1/screenshots/`:

1. Desktop All (67)
2. Desktop Competitive (4) — ACVNU, NADO 2028, NADO 2027, ECS
3. Mobile wrap — Market Scope on next row
4. Reset View → All Opportunities

Verified interactively: scope switching, demand-type dropdown independence, list/tiles, Reset View, published-19 API protection.

## Tests

- `npm run test:gdi-market-scope-toggle-v1`
- `npm run test:gdi-bethesda-67-sales-baseline-cmi-v1`
- `npm run test:gdi-bethesda-67-sales-team-inventory-display-v1`
- `npm run test:gdi-commercial-intelligence-consistency-audit-v1`
- `npm run test:gdi-hotel-toolbar-reset-view-only-v1`

## Merge readiness

Focused UI/filter PR. Does not alter CMI evidence ledger or demand-segment ontology. Ready to merge when CI is green.