# GDI Private Events V1.6 — Founder Report

**Generated:** 2026-09-24  
**Opportunity:** Woman's Club of Bethesda — Preferred Lodging Partnership  
**ID:** `gdi_pe_781f12393f8117e7`  
**Share token:** unchanged (`gdisht_47c25d74c79216021fb36150`)

---

## H. FINAL VERDICT

**PRIVATE EVENT DETAIL REPAIRED — CUSTOMER READY**

---

## A. DATA AVAILABLE

| Field | Value |
|-------|--------|
| VENUE | Woman's Club of Bethesda |
| ADDRESS | 5500 Sonoma Road, Bethesda, MD |
| DISTANCE | 1.4 mi (~4 min drive) |
| LODGING | No on-site lodging |
| EVENT ACTIVITY | Strong repeated private-event activity |
| PARTNER STATUS | No public preferred hotel partner identified |
| COMMERCIAL PATH | Venue / organization contact path |
| SOURCES | 2 clickable official URLs (deduped to venue domain) |
| CONTACT | Venue Events Team + official venue inquiry link |

Root cause: data existed on Venue + Fit + payload; customer detail never resolved/normalized it (`sources[].title` vs UI `sources[].name`; no PE enrichment; ORGANIZATION_PATH treated as “no contact”).

---

## B. DTO

PE FIELDS MAPPED: **30+** (venue identity/location, distance/drive, lodging, activity, partner, fit quadrants, sources, contact path, evidence breakdown, standing date label, detailLayout)

MISSING: **none** for Woman's Club required set

Server enrichment: `enrichPrivateEventOpportunityDetail` on auth + share detail GET.

---

## C. SOURCES

| | Count |
|--|------:|
| SOURCE LINKS BEFORE (raw payload) | 10 (titles present; UI showed blanks because it read `name`) |
| SOURCE LINKS AFTER (customer) | **2** clickable (official-domain preferred; BCC noise dropped) |
| OFFICIAL | **2** |

URLs:
- https://www.womansclubofbethesda.org/venue/
- https://www.womansclubofbethesda.org/

---

## D. CONTACT

| | |
|--|--|
| BEFORE | “No usable contact resolved yet” (`primaryContact` null despite `ORGANIZATION_PATH`) |
| AFTER | Contact path: Venue / organization contact path · Venue Events Team · official inquiry link |

---

## E. UI

| Check | Result |
|-------|--------|
| DUPLICATE THESIS | **NO** (summary short; thesis once in Why This Matters; action only) |
| TBD DATE ON VENUE PARTNERSHIP | **NO** → “Ongoing partnership opportunity” |
| EMPTY IRRELEVANT SECTIONS | **0** (PE layout hides room demand / meeting history / reactivation / competitive clutter) |
| VENUE DETAILS | **PASS** |
| HOTEL FIT | **PASS** (CORE / STRONG / HIGH / NO_LODGING / 1.4 mi) |
| SOURCES | **PASS** |
| CONTACT PATH | **PASS** |
| ACTION | **PASS** |

Type-aware layouts in `dealality-gdi-ui.js`; auth `app.js` routes PE types to the shared renderer.

---

## F. EVIDENCE CONFIDENCE

| | |
|--|--|
| NUMERIC SCORE | **85** |
| VERIFIED FIELDS | **6** |
| ESTIMATED | **1** |
| INFERRED | **3** |
| OFFICIAL SOURCES | **2** |
| CONSISTENT | **YES** (no more “85 with 0 verified / 0 official”) |

Explanation example: `High evidence confidence · 6 verified fields · 1 estimated · 3 inferred · 2 official sources`

---

## G. REGRESSION

| Suite | Result |
|-------|--------|
| LEGACY GDI (share durability / CSV) | **PASS** |
| PRIVATE EVENTS V1 / V1.4 / V1.6 | **PASS** |
| AUTH detail enrichment path | **PASS** (wired) |
| SHARE sanitize PE fields | **PASS** |
| CSV | **PASS** |
| Customer visibility | **PASS** |

---

## What changed

- `lib/group-demand-intelligence/private-events/customer-detail-enrichment.js` — hydrate venue/fit, normalize sources, evidence, contact, labels
- `api/group-demand-intelligence.js` — enrich on auth + share detail
- `lib/.../share/gdi-signed-share-capability-v1.js` — PE allowlist
- `public/js/.../dealality-gdi-ui.js` — VENUE_PARTNERSHIP / SPECIFIC_PRIVATE_EVENT detail layouts
- `public/js/.../app.js` + `share-app.js` — route PE detail + contact path

Artifacts: `reports/group-demand-intelligence/private-events-v1-6/`
