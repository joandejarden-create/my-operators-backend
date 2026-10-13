# Root Cause — YOTEL empty generators

## Primary

1. **CANONICAL_GAP** — 8/10 named generators were never persisted as opportunities or demand campaigns.
2. **UI MODEL GAP** — GDI customer UI only rendered `filterCustomerFacingOpportunities`; no Demand Campaign layer.
3. **EMPTY STATE BUG** — Copy claimed “no opportunities match filters” even when research universe should exist.

## Secondary

4. AidEx existed as `gdi_opp_aidex_geneva_11` (KEEP_ACTIVE) and passes live readiness — but alone does not represent the 10-generator universe.
5. Generic WHA SERP signal ≠ WHA80 campaign.
6. ECOSOC 2026 was New York — not YOTEL market; 2027 Geneva is the correct current cycle.
7. Child decomposition never run for these generators (`CHILD_DECOMPOSITION_NOT_YET_RUN`).
8. Default GDI hotel in auth UI is Bethesda pilot id — YOTEL must be selected to see YOTEL data.

## Not the cause

- Thresholds were not “too high” for generators (generators were never registered).
- No stale snapshot of the 10 as campaigns (file did not exist).
- ADP / share tokens unrelated.
