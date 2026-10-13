# GDI PE Test/Fixture Cleanup — Founder Report

**Generated:** 2026-09-24  
**Base:** `appa2cE7FTRmIbB32`  
**Hotel:** Bethesda Marriott (`recLuxvwwxID7U2B8`)  
**Share token:** `gdisht_47c25d74c79216021fb36150` — **unchanged** (not regenerated)

---

## G. FINAL VERDICT

**TEST DATA CLEANED — CUSTOMER GDI UI HYGIENIC**

---

## A. OPPORTUNITY AUDIT

TOTAL PE GDI ROWS: **1**  
PRODUCTION_REAL: **1**  
LIVE_CANARY_REAL_BUT_NOT_PROMOTED: **0**  
TEST: **0**  
FIXTURE: **0**  
SAMPLE: **0**  
TEMP_VALIDATION: **0**  
AMBIGUOUS: **0**

| Record ID | Opportunity ID | Title | Classification | Action |
|-----------|----------------|-------|----------------|--------|
| `recUD78wICYV2Ec17` | `gdi_pe_781f12393f8117e7` | Woman's Club of Bethesda — Preferred Lodging Partnership | PRODUCTION_REAL | KEEP |

Note: “PE link test” and “Venue partnership: Bethesda GARDEN / WEDDING VENUE …” GDI opportunity rows were **already absent** from canonical Opportunities at audit time (108 total GDI rows scanned; only Woman’s Club matched PE). Customer-visible synthetic cards were driven by leftover **fixture venue graph** rows, now removed.

---

## B. CLEANUP

DELETED (GDI opportunities): **0**  
ARCHIVED: **0**  
KEPT: **1** (Woman’s Club)  
REVIEW: **0**

---

## C. FIXTURE GRAPH CLEANUP

FIXTURE VENUES DELETED: **16**  
FIXTURE FIT ROWS DELETED: **24**  
FIXTURE SIGNALS DELETED: **4**  
REAL VENUES PRESERVED: **11** (incl. Woman’s Club of Bethesda, Bethesda Country Club, Glenview, Brookside, etc.)

Deleted venue names (conclusive `*.example` / numbered synthetics only):

- Bethesda GARDEN 11 / 4  
- Bethesda WEDDING VENUE 6 / 13  
- Bethesda EVENT VENUE 14 / 7  
- Bethesda BANQUET HALL 5 / 12  
- Bethesda COUNTRY CLUB 1 / 8 / 15  
- Bethesda HISTORIC ESTATE 2 / 9  
- Bethesda RELIGIOUS VENUE 3 / 10  
- Regional Garden Estate Shared  

---

## D. UI

PE LINK TEST VISIBLE: **NO**  
SYNTHETIC VENUE CARDS VISIBLE: **NO**  
REAL GDI OPPORTUNITIES INTACT: **YES** (Woman’s Club + 30 other non-PE customer opps)  
SHARE URL: **PASS** (existing Bethesda token ACTIVE; not regenerated)

Post-filter customer opportunity count: **31**

---

## E. SAFETY

PRODUCTION OPPORTUNITIES DELETED: **0**  
PRODUCTION DECISIONS LOST: **0**  
PRODUCTION VALIDATIONS LOST: **0**  
PRODUCTION ACTIONS LOST: **0**  
PRODUCTION OUTCOMES LOST: **0**

No cascade delete of real venue / fit / signal rows tied to Woman’s Club.

---

## F. FUTURE GUARD

isTestData FIELD: **CREATED** (`fldnkWSxXQL0x8a3a` checkbox on Group Demand Opportunities)  
CUSTOMER API FILTER: **PASS** — `filterCustomerFacingOpportunities` on list, detail, CSV, weekly, share  
TEST WRITE PATH: **PASS** — PE promote auto-sets `isTestData=true` / `customerVisible=false` for `*.example` domains and numbered Bethesda synthetics; `markAsTestOpportunity()` helper for explicit validation writes  

Modules:

- `lib/group-demand-intelligence/customer-visibility.js`
- `lib/group-demand-intelligence/private-events/promote-to-gdi.js`
- `api/group-demand-intelligence.js` (customer + share surfaces)
- `scripts/gdi-pe-test-fixture-cleanup.mjs`
- `scripts/test-gdi-customer-visibility.mjs` → `npm run test:gdi-customer-visibility`

---

## Regressions run

| Suite | Result |
|-------|--------|
| PE V1 | PASS (17) |
| PE V1.1 | PASS (13) |
| PE V1.2 | PASS (10) |
| PE V1.4 | PASS (12) |
| New Opp V1 | PASS (17) |
| Live CQ V1 | PASS (15) |
| Customer CSV | PASS (13) |
| Weekly delta | PASS |
| Share durability | PASS |
| Customer visibility | PASS |

---

## Artifacts

- `reports/group-demand-intelligence/private-events-test-cleanup/AUDIT.json`
- `reports/group-demand-intelligence/private-events-test-cleanup/DRY_RUN.md`
- `reports/group-demand-intelligence/private-events-test-cleanup/FOUNDER_REPORT.md`
