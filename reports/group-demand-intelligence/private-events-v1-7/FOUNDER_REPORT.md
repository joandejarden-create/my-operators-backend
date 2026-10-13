# GDI Private Events V1.7 — Test Data Cleanup + Customer Share Deploy Closure

**Generated:** 2026-09-24T11:05:00.000Z  
**Base:** `appa2cE7FTRmIbB32` (canonical)  
**Hotel:** Bethesda Marriott (`recLuxvwwxID7U2B8`)  
**Share token:** `gdisht_47c25d74c79216021fb36150` — **unchanged** (not regenerated)  
**Branch:** `deploy/gdi-pe-v1-7-customer-closure`  
**HEAD SHA:** `2e69cd9`  
**Deploy:** `e0798a3d-6660-4d1b-bd1e-3d3a1ee15427` · **SUCCESS** · Online  
**Deploy method:** `railway up --detach` (no `--from-source`)

---

## K. FINAL VERDICT

**GDI CUSTOMER CLEANUP + PRIVATE EVENTS DEPLOY PASSES — READY FOR RAD**

---

## A. CLEANUP AUDIT

| Classification | Count |
|----------------|------:|
| TOTAL PE GDI ROWS | **1** |
| PRODUCTION_REAL | **1** |
| TEST | **0** |
| FIXTURE | **0** |
| SAMPLE | **0** |
| TEMP_VALIDATION | **0** |
| AMBIGUOUS | **0** |

| Airtable Record ID | Opportunity ID | Title | Venue | Hotel | Type | Demand Family | Classification | Action |
|--------------------|----------------|-------|-------|-------|------|---------------|----------------|--------|
| `recUD78wICYV2Ec17` | `gdi_pe_781f12393f8117e7` | Woman's Club of Bethesda — Preferred Lodging Partnership | Woman's Club of Bethesda | `recLuxvwwxID7U2B8` | VENUE_PARTNERSHIP | PRIVATE_EVENTS | PRODUCTION_REAL | **KEEP** |

Dry-run re-audit (V1.7): `CUSTOMER VISIBILITY GUARD ADDED — NO CLEANUP NEEDED`  
Fixture venues remaining for delete: **0** (prior cycle already removed 16 synthetics)

---

## B. CLEANUP ACTION

| Action | Count |
|--------|------:|
| DELETED | **0** (this cycle; already clean) |
| ARCHIVED | **0** |
| KEPT | **1** |
| REVIEW | **0** |

Prior cycle (already applied; preserved here for closure): fixture venues **16**, fit rows **24**, signals **4** deleted; production PE opportunity deletes **0**.

---

## C. FIXTURE GRAPH

| Metric | Count |
|--------|------:|
| FIXTURE VENUES DELETED (this cycle) | **0** |
| FIXTURE FIT ROWS DELETED (this cycle) | **0** |
| FIXTURE SIGNALS DELETED (this cycle) | **0** |
| REAL LIVE VENUES PRESERVED | **11** |

Woman's Club venue `pev_d7991905b5642dc4` / fit `hvf_2e7744ffe743dd` preserved.

---

## D. TEST VISIBILITY GUARD

| Check | Result |
|-------|--------|
| isTestData | **REUSED** (`fldnkWSxXQL0x8a3a` checkbox on Group Demand Opportunities) |
| Woman's Club `isTestData` | unchecked / null (= not test; Airtable checkbox false) |
| CUSTOMER API FILTER | **PASS** (`filterCustomerFacingOpportunities` + heuristics) |
| SHARE API FILTER | **PASS** (same filter on share list/detail/CSV) |
| FUTURE TEST WRITE PATH | **PASS** (`markAsTestOpportunity` + promote synthetic auto-flag) |

Modules: `lib/group-demand-intelligence/customer-visibility.js`, PE `promote-to-gdi.js`, `api/group-demand-intelligence.js`.

---

## E. WOMAN’S CLUB

| Check | Result |
|-------|--------|
| OPPORTUNITY PRESENT | **YES** |
| DUPLICATES | **0** |
| AUTH CARD | **PASS** (same list DTO / customer filter path; share UI verified live) |
| AUTH DETAIL | **PASS** (detail enrichment live) |
| SHARE CARD | **PASS** |
| SHARE DETAIL | **PASS** |
| SOURCES | **PASS** (2 official clickable URLs) |
| CONTACT PATH | **PASS** (Venue Events Team + venue/org contact path + inquiry URL) |
| HOTEL FIT | **PASS** (Product Strong · Partnership High · Lodging capture High · Core catchment) |

Live detail confirms:

- Opportunity Type: **Venue Partnership**
- Demand Family: **Private Events**
- Venue: Woman's Club of Bethesda · 5500 Sonoma Road, Bethesda, MD
- Distance: **1.4 mi / ~4 min**
- Lodging: **No on-site lodging**
- Activity: **Strong repeated private-event activity**
- Partner: **No public preferred hotel partner identified**
- Evidence: **85** · 6 verified · 1 estimated · 3 inferred · 2 official sources
- Layout: type-aware `VENUE_PARTNERSHIP`

Residual polish (non-blocking): Hotel Fit “signals found” / one source subtype still show internal codes such as `OFFICIAL_WEDDING_PAGE` (not the blocked customer enums).

---

## F. LIVE UI HYGIENE

| Check | Result |
|-------|--------|
| PE LINK TEST VISIBLE | **NO** |
| SYNTHETIC GARDEN CARD VISIBLE | **NO** |
| SYNTHETIC WEDDING VENUE CARD VISIBLE | **NO** |
| OTHER TEST PE ROWS VISIBLE | **0** |
| Customer opportunity count | **31** |

Required customer labels verified (no underscore enum leakage for):

- Venue Partnership / Private Events / Strong repeated private-event activity / No public preferred hotel partner identified

---

## G. DEPLOYMENT

| Field | Value |
|-------|-------|
| BRANCH | `deploy/gdi-pe-v1-7-customer-closure` |
| SHA | `2e69cd9` |
| DEPLOY ID | `e0798a3d-6660-4d1b-bd1e-3d3a1ee15427` |
| STATUS | **SUCCESS** · service **Online** |
| SOURCE VERIFIED | **YES** (`railway up` from current branch worktree; **not** `--from-source`) |
| RAILWAY PROJECT | serene-reverence (`48bd3650-29f9-4db5-a357-293646f6a8b0`) |
| RAILWAY SERVICE | my-operators-backend |

Prior failed attempts (`6b55cd0f`, `64db257d`) were boot/import mismatches; final deploy healthy. Non-blocking boot note: census-map-snapshot module missing (pre-existing / unrelated).

---

## H. SHARE

| Check | Result |
|-------|--------|
| TOKEN UNCHANGED | **YES** (`gdisht_47c25d74c79216021fb36150` ACTIVE) |
| RESOLVE | **PASS** (200) |
| LIST | **PASS** (200 · 31 opps) |
| DETAIL | **PASS** (200 · Woman's Club) |

---

## I. INTEGRITY

| Metric | Count |
|--------|------:|
| PRODUCTION OPPORTUNITIES DELETED | **0** |
| DECISIONS LOST | **0** |
| VALIDATIONS LOST | **0** |
| ACTIONS LOST | **0** |
| OUTCOMES LOST | **0** |
| SHARE TOKENS CHANGED | **0** |

---

## J. REGRESSION

| Suite | Result |
|-------|--------|
| PE V1 | **PASS** (17) |
| PE V1.1 | **PASS** (13) |
| PE V1.2 | **PASS** (10) |
| PE V1.4 | **PASS** (12) |
| PE V1.6 | **PASS** |
| PE V1.7 cleanup dry-run | **PASS** (no deletes needed) |
| New Opportunities V1 | **PASS** (17) |
| New Opportunities V1.2 | **PASS** (16) |
| Discovery V4 | **PASS** (16) |
| Commercial Quality V1 | **PASS** (15) |
| Weekly delta | **PASS** |
| Share durability | **PASS** |
| CSV | **PASS** (13 unit + live share export 31 rows, WC included, no synthetics) |
| UI / share smoke | **PASS** |
| test-data customer exclusion | **PASS** |

### Known pre-existing (not introduced this cycle)

Discovery hygiene **V3 Expo Parks** freeze flake — report separately; not a V1.7 regression.

### Legacy GDI

Sample non-PE cards present on share: PRIMARY_PURSUIT / OVERFLOW_HOUSING / FUTURE_CYCLE / WATCH — layout intact.

---

## Working tree note

After PE commits, remaining dirty files are **unrelated** WIP (market-alerts UI, artifacts, contact-intelligence experiments, etc.). They were **not** silently included in this closure verdict beyond what was required to boot Railway (`2e69cd9` also carried broader boot middleware deps). Do not treat the full dirty tree as part of the PE package.

---

## Artifacts

- `reports/group-demand-intelligence/private-events-test-cleanup/` (prior cleanup + guard)
- `reports/group-demand-intelligence/private-events-v1-6/`
- `reports/group-demand-intelligence/private-events-v1-7/LIVE_SHARE_SMOKE.json`
- `reports/group-demand-intelligence/private-events-v1-7/WC_DETAIL_LIVE.json`
- `reports/group-demand-intelligence/private-events-v1-7/FOUNDER_REPORT.md`
