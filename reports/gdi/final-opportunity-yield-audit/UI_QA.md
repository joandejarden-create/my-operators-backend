# UI QA — YOTEL / AI for Good

## Expected
- AI for Good visible as demand campaign
- Child-account section reflects canary children when API/surface allows
- Ready children only if `isGdiCustomerOpportunityReady` passes (expect 0 for canary)
- Watch children only if `isValidFutureWatch` passes
- Research leads not mislabeled as ready
- Empty-state copy accurate when no ready children

## Live HTTP (post server restart with surface fix)
- `GET .../demand-campaigns` → **10** campaigns including **AI for Good Global Summit 2027**
- `GET .../opportunities` (customer facing) → **1** row = AidEx (ready after CQ)
- AI for Good children in Airtable canonical bag: **11** (`customerVisible=false`)
- Customer facing filter on those 11: **0** (correct — not ready; not mislabeled Ready)
- `isValidFutureWatch` on those 11: **11**
- Salesperson filter on those 11: **11** (admin/salesperson can see research leads)

## Gap vs ideal UI
Customer opportunities endpoint applies `filterCustomerFacingOpportunities` only — valid Future Watch children are **not** listed there until a dedicated watch surface or salesperson mode is used. Campaigns already show the research universe.

## Browser
- Unauthenticated `/group-demand-intelligence.html` stays on auth Loading gate (expected).
- Verified via live API routes the UI calls (`demand-campaigns` + `opportunities`).

## Manual checklist (authenticated)
1. Open `/group-demand-intelligence` as owner/admin → select YOTEL Geneva Lake.
2. Demand Campaigns should list all 10 including AI for Good.
3. Ready list should show AidEx (after surface fix), not AI for Good children as Ready.
4. Child research leads must not appear as Ready.
5. If UI has a Future Watch / salesperson bag, expect AI for Good children there (11 watch-valid).
