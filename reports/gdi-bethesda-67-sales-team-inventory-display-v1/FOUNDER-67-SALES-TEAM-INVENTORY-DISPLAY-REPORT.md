# FOUNDER — Bethesda 67 Sales-Team Inventory Display Fix

**Date:** 2026-11-12  
**Branch:** `feature/gdi-bethesda-67-sales-team-inventory-display-v1`  
**Baseline:** PR #218 @ `750b249a12257fc8e893e2752a690581dd04aeb5`  
**Hotel:** Bethesda Marriott (`recLuxvwwxID7U2B8`)

---

## 1. Exact root cause of the 19-record local display

**File:** `api/group-demand-intelligence.js`  
**Function:** `getGdiOpportunities`  
**Stage:** After commercial-surface filtering retained all **67** baseline IDs, the default commercial path called:

`filterCustomerFacingOpportunities(opportunities, { hotelId })`

That publication eligibility filter collapses **67 → 19** before `annotateInventoryPopulations` runs.

### Proven stage counts (canonical path)

| Stage | Total rows | Baseline 67 present |
|-------|------------|---------------------|
| Store | 59 | 40 |
| Internal union | 88 | **67** |
| Event-family collapse | 81 | **67** |
| Commercial surface | 67 | **67** |
| **Customer-facing filter** | **19** | **19** (48 removed) |
| Bethesda projection | 19 | 19 |

**Removal point:** customer-publication filter — not union, not family collapse, not commercial surface, not UI rendering.

PR #218 recovered data + population classification, but the live Opportunities endpoint still applied the **published customer feed** filter to the local sales-team page.

---

## 2. Fix

Separated audiences:

| Audience | Scope | Count |
|----------|-------|-------|
| `SALES_TEAM_INVENTORY` | `BETHESDA_SALES_TEAM_INVENTORY_67` | **67** exact baseline IDs |
| `PUBLISHED_CUSTOMER_FEED` | `BETHESDA_PUBLISHED_CUSTOMER_FEED_19` | **19** published |

Authorization for sales-team inventory:

- Authenticated Dealality user (`req.dealalityUser`), **or**
- Established GDI pilot-read auth page session (`req.gdiPilotUnauthRead` from `gdiPilotReadAuth`)

Explicit `?inventoryAudience=published` forces the 19-record feed.  
Share/external surfaces remain published-only (`forcePublished` / share capability).

Sales-team path uses `selectBethesdaSalesOpportunityInventory(..., { salesTeamInventory: true })` and **does not** apply `filterCustomerFacingOpportunities` to the baseline.

CMI: expanded verified seeds to all **7** IDs from `03-cmi-verified.json`; annotate before presentation DTO trim so CMI survives sanitization.

---

## 3. Running server

| Item | Value |
|------|-------|
| Port | **8080** |
| Process | `node server.js` (restarted after fix) |
| Working directory | `C:\Dev\dealality\my-operators-backend` |
| URL | `http://localhost:8080/group-demand-intelligence.html?hotelId=recLuxvwwxID7U2B8` |

Published verification URL:

`http://localhost:8080/group-demand-intelligence.html?hotelId=recLuxvwwxID7U2B8&inventoryAudience=published`

---

## 4. Before / after API

### Before (stale server)

- `count: 19`
- `listScope: BETHESDA_PRIMARY_COMMERCIAL`
- No usable `inventoryPopulationCounts`

### After (restarted server)

**Sales-team default**

- HTTP 200
- `count: 67`
- `listScope: BETHESDA_SALES_TEAM_INVENTORY_67`
- `inventoryAudience: SALES_TEAM_INVENTORY`
- Sales Opportunities: **67**
- CMI verified: **7**
- All Commercial Intelligence unique: **67** (7 CMI ⊆ 67 sales)
- Maturity: 14 Sales Ready · 21 Account Development · 1 Qualification Required · 31 Research Continuation / Monitor

**Published audience** (`?inventoryAudience=published`)

- `count: 19`
- `listScope: BETHESDA_PUBLISHED_CUSTOMER_FEED_19`

---

## 5. Browser acceptance

| View | Result |
|------|--------|
| Sales Opportunities | **Showing 67 of 67** · dropdown `Sales Opportunities (67)` |
| Competitive Market Intelligence | **Showing 7 of 67** · 7 verified CMI cards |
| All Commercial Intelligence | **Showing 67 of 67** |
| Published (`inventoryAudience=published`) | **Showing 19 of 19** |
| Staged-48 example | Visible (e.g. High-Risk, High-Reward Research Symposium 2027) |

Screenshots: `reports/gdi-bethesda-67-sales-team-inventory-display-v1/screenshots/`

---

## 6. Export CSV

- Sales-team export larger than published export (full 67 inventory).
- Published export remains gated via `inventoryAudience=published`.
- Share routes unchanged (customer-facing 19).

---

## 7. Tests

```text
npm run test:gdi-bethesda-67-sales-team-inventory-display-v1   PASS 9/9
npm run test:gdi-bethesda-67-sales-baseline-cmi-v1             PASS 7/7
npm run test:gdi-bethesda-67-customer-feed-reconciliation-v1   PASS (19 facing)
```

---

## 8. Non-goals preserved

- No paid Exa research
- No auto-publication of the 48
- No 88/152 substitution
- Hotel Need Times & Priorities tab untouched by this change
- Customer share stays 19 until separate founder approval

---

## Verdict

**Fixed in the running local application.** Authorized Bethesda sales-team Opportunities now displays the original **67**, with separate **7** CMI selection, while published/customer audience remains **19**.
