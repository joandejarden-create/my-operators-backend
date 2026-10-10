# GDI Customer Surface — Bethesda Standardization

## Verdict
**BETHESDA_CARD_PARITY_CAMPAIGNS_HIDDEN**

## What changed (presentation only)
1. Removed Demand Campaigns / Generators panel + fetch from customer `app.js`
2. Neutral empty states (no campaign copy) on auth + share
3. Shared Bethesda card shows buyer entity / role / org contact path when no named person
4. Account-first `displayTitle` on list DTO (canonical `title` unchanged)
5. Future Watch badge when `customerFacingState` is Future Watch
6. Generator-only records blocked from customer list filter

## Counts
- YOTEL customer-ready / cards rendered: **13** (API + UI All (13))
- YOTEL validated Future Watch universe: **83** unique (thresholds unchanged)
- Customer-list Watch-only cards: **0** — matches Bethesda gate (list = ready|legacy)
- Bethesda browse: **35** cards, no campaigns section
- Internal YOTEL campaigns: preserved (`/demand-campaigns` API + store)

## Browser QA
YOTEL + Bethesda loaded live: no Demand Campaigns/Generators section; shared Bethesda tile; buyer entity/role/path on YOTEL cards without inventing named people.

## Guardrails
Thresholds unchanged. Canonical opportunity records not rewritten. ADP/share tokens untouched. Parent-child lineage preserved.
