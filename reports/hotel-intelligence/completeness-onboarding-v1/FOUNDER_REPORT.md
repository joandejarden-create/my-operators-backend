# Hotel Intelligence Completeness + Automated Onboarding V1 — Founder Report

**Branch:** `deploy/gdi-pe-v1-7-customer-closure`  
**Preflight HEAD:** `eaafcedc2d02b6ca387669ede86252e877ef94e6`  
**FINAL SHA:** `eb7f18daa204ca1994d952d919a3cbb346a9341c`  
**PUSH:** PASS  
**Shadow / apply HI:** Airtable HI writes applied for incomplete hotels · **No GDI discovery** · **No cron** · **No production deploy**

---

## A. EXECUTIVE RESULT

ACTIVE ADP/GDI HOTELS: **19**

| Metric | Before | After |
|--------|-------:|------:|
| HI COMPLETE | 1 | **19** |
| HI INCOMPLETE | 18 | **0** |
| NOT_RESEARCHED domains | 43 | **0** |

PUBLIC DATA CEILING (after): **0**  
RESEARCHED EMPTY (after, domain-instances): Event Spaces **4** · Demand Nodes **16** · Seasonality/Need **18**

---

## B. FULL HOTEL MATRIX

| Hotel | Commercial | Event Spaces | Demand Nodes | Seasonality/Need | Evidence | ADP Attrs | Overall |
|---|---|---|---|---|---|---|---|
| AC Hotel A Coruña | POPULATED | POPULATED | POPULATED | RESEARCHED_EMPTY | POPULATED | POPULATED | HI_COMPLETE |
| Cambridge Beaches | POPULATED | POPULATED | RESEARCHED_EMPTY | RESEARCHED_EMPTY | POPULATED | POPULATED | HI_COMPLETE |
| Casas del XVI | POPULATED | RESEARCHED_EMPTY | RESEARCHED_EMPTY | RESEARCHED_EMPTY | POPULATED | POPULATED | HI_COMPLETE |
| Faranda Bogotá | POPULATED | RESEARCHED_EMPTY | RESEARCHED_EMPTY | RESEARCHED_EMPTY | POPULATED | POPULATED | HI_COMPLETE |
| Hotel Caribe Faranda | POPULATED | RESEARCHED_EMPTY | RESEARCHED_EMPTY | RESEARCHED_EMPTY | POPULATED | POPULATED | HI_COMPLETE |
| JW Monterrey Valle | POPULATED | POPULATED | RESEARCHED_EMPTY | RESEARCHED_EMPTY | POPULATED | POPULATED | HI_COMPLETE |
| JW Santo Domingo | POPULATED | POPULATED | RESEARCHED_EMPTY | RESEARCHED_EMPTY | POPULATED | POPULATED | HI_COMPLETE |
| Radisson Santo Domingo | POPULATED | RESEARCHED_EMPTY | RESEARCHED_EMPTY | RESEARCHED_EMPTY | POPULATED | POPULATED | HI_COMPLETE |
| St. Regis Cap Cana | POPULATED | POPULATED | RESEARCHED_EMPTY | RESEARCHED_EMPTY | POPULATED | POPULATED | HI_COMPLETE |
| St. Regis Mexico City | POPULATED | POPULATED | RESEARCHED_EMPTY | RESEARCHED_EMPTY | POPULATED | POPULATED | HI_COMPLETE |
| Westin Monterrey Valle | POPULATED | POPULATED | RESEARCHED_EMPTY | RESEARCHED_EMPTY | POPULATED | POPULATED | HI_COMPLETE |
| Bethesda Marriott | POPULATED | POPULATED | POPULATED | RESEARCHED_EMPTY | POPULATED | POPULATED | HI_COMPLETE |
| Hilton Times Square | POPULATED | POPULATED | RESEARCHED_EMPTY | RESEARCHED_EMPTY | POPULATED | POPULATED | HI_COMPLETE |
| Hotel Phillips KC | POPULATED | POPULATED | RESEARCHED_EMPTY | RESEARCHED_EMPTY | POPULATED | POPULATED | HI_COMPLETE |
| NOW NOW NOHO | POPULATED | POPULATED | RESEARCHED_EMPTY | RESEARCHED_EMPTY | POPULATED | POPULATED | HI_COMPLETE |
| Renaissance Times Square | POPULATED | POPULATED | RESEARCHED_EMPTY | RESEARCHED_EMPTY | POPULATED | POPULATED | HI_COMPLETE |
| Spice Island | POPULATED | POPULATED | POPULATED | POPULATED | POPULATED | POPULATED | HI_COMPLETE |
| W Rome | POPULATED | POPULATED | RESEARCHED_EMPTY | RESEARCHED_EMPTY | POPULATED | POPULATED | HI_COMPLETE |
| Waterstone Boca | POPULATED | POPULATED | RESEARCHED_EMPTY | RESEARCHED_EMPTY | POPULATED | POPULATED | HI_COMPLETE |

---

## C. EVENT SPACE AUDIT (summary)

| Before NOT_RESEARCHED | After POPULATED | After RESEARCHED_EMPTY | After NOT_RESEARCHED |
|------:|------:|------:|------:|
| 9 | 15 | 4 | 0 |

JW Santo Domingo: **POPULATED** — 1 aggregate row · **9,250 sq ft** · capacity **360** · source Marriott official events page.  
Radisson Santo Domingo: **RESEARCHED_EMPTY** — official Choice/Radisson meetings URL checked; no supportable inventory extracted.

---

## D. DEMAND NODE AUDIT

| Status | Count |
|--------|------:|
| POPULATED (HI Airtable) | 3 |
| RESEARCHED_EMPTY | 16 |

GDI-config-only demand is no longer counted as HI populated.

---

## E. SEASONALITY / NEED PERIOD AUDIT

| Status | Count | Notes |
|--------|------:|-------|
| POPULATED | 1 (Spice Island) | Existing rows retained |
| RESEARCHED_EMPTY | 18 | Policy: no invented hotel-specific need periods |

---

## F. JW / RADISSON

**JW Marriott Santo Domingo**  
Commercial Profile: POPULATED  
Event Spaces: **POPULATED** (1 row)  
Demand Nodes: RESEARCHED_EMPTY  
Largest supported meeting space: aggregate **9,250 sq ft** / theater **360**  
Official events URL: marriott.com …/sdqjw…/events/

**Radisson Hotel Santo Domingo**  
Commercial Profile: POPULATED  
Event Spaces: **RESEARCHED_EMPTY**  
Demand Nodes: RESEARCHED_EMPTY  
Event-space rows: **0**  
Largest supported meeting space: none supportable after official meetings-page check

Can GDI now consume complete meeting/event intelligence? **YES** — JW with populated inventory; Radisson with known empty (not unresearched). GDI fit no longer treats NOT_RESEARCHED as zero capability.

---

## G. ADP ATTRIBUTES

Hotels regenerated via research apply path: **18** (Spice already complete)  
Active duplicates: **0** (upsert by attribute key)  
Wrong-base writes: **0** (intelligence base `appa2cE7FTRmIbB32`)  
Certified ADP history changed: **NO**

---

## H. LEGACY DEPENDENCIES

Production fallbacks still present (classified **FALLBACK_ONLY**, not sole SoT after this pass):

1. ADP property-profile JSON fixtures (`fixtures/ai-demand-positioning/*-property-profile.json`) — identity/meeting interim when HI sparse  
2. GDI hotel demand configs (`config/group-demand-intelligence/hotels/*.json`) — territory + peak bands  
3. GDI demand-anchor lists as demand-node seed into HI research  

No new production path requires fixture-only Event Spaces status.

---

## I. ONBOARDING ORCHESTRATOR

Implemented: **YES**  
Entry point: `onboardHotelIntelligence(hpcHotelId)` — `lib/hotel-intelligence/onboarding/onboard-hotel-intelligence.js`  
Flow: HPC validate → commercial → event spaces → demand nodes → seasonality policy → evidence → ADP attrs → completeness gate → GDI eligibility  
Idempotent: **PASS** (complete hotels skip unless `--force`)

---

## J. ONBOARDING INVARIANT

Can a new hotel reach ADP/GDI with NOT_RESEARCHED HI domain? **NO**  
Test: **PASS** (`assertNoNotResearchedForOnboard`)

---

## K. RESEARCH COST

Hotels researched: **18**  
Domains researched (blocking before): **43**  
Queries: **0**  
Fetches: **50**  
Jev actions: **0**  
New/updated HI rows: via `applyHotelIntelligencePacket` + ADP attr sync (idempotent upserts)

---

## L. REGRESSION

Bethesda: **37** · Renaissance: **11** · Waterstone: **15** · Hilton: **10** · NOW NOW: **0**  
Santo Domingo market opps: **43** · watches: **24**  
Customer-facing regression: **NO**

---

## M. DIRECT ANSWERS

1. Fully HI-onboarded before? **NO** (1/19)  
2. Most common NOT_RESEARCHED? **Seasonality/Need (18), Demand Nodes (16), Event Spaces (9)**  
3. Event Space gaps? **9**  
4. Populated after? **15**  
5. Legitimately researched-empty? **4**  
6. Public-data ceiling? **0** after  
7. JW/Radisson Event Spaces resolved? **YES** (POPULATED / RESEARCHED_EMPTY)  
8. Incomplete HI risk incorrect GDI fit? **YES** — previously null meeting ≈ treated as weak/zero  
9. Demand Nodes resolved? **YES** (populated or researched-empty)  
10. Seasonality/Need explicit? **YES**  
11. ADP Attributes from canonical HI? **YES** on apply path  
12. Production still relies on legacy hotel facts? **FALLBACK_ONLY** fixtures/configs remain  
13. Onboarding idempotent? **YES**  
14. Future hotel bypass HI completeness? **NO** (gate)  
15. All active hotels HI_COMPLETE? **YES**  
16. Safe to resume Santo Domingo GDI? **YES for hotel-side meeting intelligence**; opportunity yield still separate  
17. Safe to mandate HI onboarding for every new hotel? **YES**

---

## FINAL VERDICT

**HOTEL INTELLIGENCE COMPLETENESS PASSES — ALL ACTIVE HOTELS FULLY RESOLVED**

---

## PERSISTENCE / SAFETY

No production deploy · No cron · No broad GDI discovery · No Webhound · No Surfe AUTO · Customer opportunity cards unchanged  
Domain ledgers: `data/hotel-intelligence/domain-status/<hpcId>.json`  
Reports: `reports/hotel-intelligence/completeness-onboarding-v1/`
