# GDI New Opportunities V1 — Founder Report

**Generated:** 2026-09-23  
**Mode:** Offline fixture dry-run (Bethesda) + archetype generalization (Renaissance / Cambridge)  
**Base SHA:** `bf532f9`  
**Working tree:** dirty (New Opportunities V1 **not committed**, **not deployed**)  
**Live write:** **NONE** — dry-run / offline only  
**Contact enrichment:** **NONE** (Surfe 0 / PDL 0)

---

## A. ARCHITECTURE

**DEMAND SIGNAL TYPES:**  
EVENT · TRAINING_PROGRAM · GOVERNMENT_CONTRACTOR_PROGRAM · PROJECT_TEAM · CORPORATE_RELOCATION · MEDICAL_HEALTHCARE_PROGRAM · UNIVERSITY_ACADEMIC_PROGRAM · SPORTS_ACADEMIC_COMPETITION · CONSULTING_ADVISORY_TEAM · CORPORATE_MEETING · BOARD_COMMITTEE_MEETING · NONPROFIT_PROGRAM · INSTITUTIONAL_GATHERING · PROFESSIONAL_PROJECT_CREW · INCENTIVE_RETREAT · OVERFLOW_HOUSING · OTHER

**CANONICAL GDI MODEL REUSED:** YES  

**SEPARATE DB CREATED:** NO  

Also: public trigger classifier (TRIGGER ≠ opportunity), org research reuse cache, recurrence classes without inventing future cycles, archetype lane routing (no hotel/city hardcodes).

---

## B. BETHESDA DISCOVERY

Baseline canonical universe: **38** (dedupe baseline; IDs unchanged).

| Demand Type | Candidates | Watch | Actionable | New |
|-------------|------------|-------|------------|-----|
| EVENT | 1 | 0 | 1 | 1 |
| TRAINING | 2 | 0 | 2 | 2 |
| GOVERNMENT / CONTRACTOR | 2 | 0 | 1 | 1 |
| PROJECT TEAM | 0 | 0 | 0 | 0 |
| RELOCATION | 1 | 0 | 1 | 1 |
| MEDICAL / HEALTHCARE | 1 | 0 | 0 | 0 |
| UNIVERSITY / ACADEMIC | 1 | 0 | 1 | 1 |
| SPORTS / ACADEMIC | 1 | 0 | 1 | 1 |
| CONSULTING / ADVISORY | 1 | 0 | 1 | 1 |
| CORPORATE MEETING | 0 | 0 | 0 | 0 |
| BOARD / COMMITTEE | 1 | 0 | 1 | 1 |
| NONPROFIT / INSTITUTIONAL | 1 | 0 | 1 | 1 |
| PROFESSIONAL PROJECT CREW | 0 | 0 | 0 | 0 |
| INCENTIVE / RETREAT | 0 | 0 | 0 | 0 |
| OVERFLOW / HOUSING | 0 | 0 | 0 | 0 |

Noise correctly rejected: award-only government contract; local one-hour medical seminar (`NO_HOTEL_ROOM_THESIS` / `NON_EVENT_CONTENT`).

---

## C. NEW OPPORTUNITIES

All proposed NEW rows are **NEW_TO_GDI** only (never NEW_TO_HOTEL).

| Opportunity ID | Demand Type | Organization | Opportunity | Timing | Location | Room-Demand Thesis | Peak Rooms | Potential Room Nights | Recurrence | Buyer Path | Suggested Action | Source | Why Hotel Could Win |
|----------------|-------------|--------------|-------------|--------|----------|--------------------|------------|----------------------|------------|------------|------------------|--------|---------------------|
| fx_train_25 | Training Program | Public Sector Training Consortium | Federal PM Certification Training Academy 2026 | 2026-04-14 | Bethesda, MD | Multi-day certification lodging; room block TBD | 25 EST | 75 EST | UNKNOWN | FUNCTIONAL_BUYER | QUALIFY_NOW | example.org/training-academy-2026 | Multi-day trainees need lodging; hotel venue TBD |
| fx_gov_team | Government Contractor | Health Systems Integrator | EHR Implementation Mobilization | 2026-05-01 | Bethesda, MD | Multi-week traveling implementation team housing | 18 EST | EST | UNKNOWN | ORGANIZATION_PATH | QUALIFY_NOW | example.org/ehr-mobilization | Temporary housing near campus |
| fx_reloc | Corporate Relocation | Mid-Atlantic Corporate Group | HQ Relocation Temporary Housing | 2026-06-01 | Bethesda, MD | Workforce relocation temporary hotel housing | EST | EST | UNKNOWN | ORGANIZATION_PATH | QUALIFY_NOW | example.org/hq-relocation-housing | Relocating employees near new HQ |
| fx_univ | University / Academic | Regional Research University | Visiting Faculty Residential Program | 2026-07-06 | Bethesda, MD | Multi-day visiting faculty lodging | EST | 330 EST | UNKNOWN | ORGANIZATION_PATH | QUALIFY_NOW | example.org/visiting-faculty-2026 | Hotel venue TBD for residential program |
| fx_sports | Sports / Academic | Youth Sports Federation | Youth Championship Housing Block | 2026-05-22 | Bethesda, MD | Tournament housing for traveling teams | EST | 360 EST | UNKNOWN | ORGANIZATION_PATH | QUALIFY_NOW | example.org/championship-housing | Official hotels program accepting proposals |
| fx_consult | Consulting / Advisory | Advisory Partners | ERP Transformation Project Team | 2026-04-01 | Bethesda, MD | Multi-week traveling consultants lodging | EST | EST | UNKNOWN | ORGANIZATION_PATH | QUALIFY_NOW | example.org/erp-engagement | Near client site lodging |
| fx_board | Board / Committee | National Nonprofit Board | Annual Board Retreat 2026 | 2026-09-18 | Bethesda, MD | Multi-day board retreat lodging | EST | 44 EST | POSSIBLE_RECURRING | ORGANIZATION_PATH | QUALIFY_NOW | example.org/board-retreat-2026 | Venue TBD |
| fx_npo | Nonprofit Program | Community Foundation Network | Annual Meeting & Leadership Institute | 2026-10-08 | Bethesda, MD | Multi-day nonprofit lodging | EST | 160 EST | POSSIBLE_RECURRING | ORGANIZATION_PATH | QUALIFY_NOW | example.org/foundation-annual-2026 | Room block TBD |
| fx_small_strong | Training Program | Leadership Institute East | Annual Leadership Certification | 2026-08-10 | Bethesda, MD | Recurring 25-peak training lodging | 25 EST | 75 EST | POSSIBLE_RECURRING | ORGANIZATION_PATH | QUALIFY_NOW | example.org/leadership-training-annual | Strong fit vs mega convention |
| fx_large_poor | Event / Association | Global Trade Federation | Mega International Convention | 2026-11-01 | Bethesda, MD | Official hotels / room block | EST | 3200 EST | UNKNOWN | ORGANIZATION_PATH | QUALIFY_NOW | example.org/mega-convention | Large but weaker commercial fit |

---

## D. MORE AT-BATS

| Metric | Count |
|--------|-------|
| EVENT-ONLY NEW | 1 |
| EXPANDED-DEMAND NEW | 9 |
| OVERLAP | 0 |
| INCREMENTAL CREDIBLE AT-BATS | **9** |

Not counted: award-only contract, local seminar, duplicates of the 38 canonical set.

---

## E. QUALITY

| Metric | Value |
|--------|-------|
| NEW ACTIONABLE | 10 |
| TRUE AFTER MANUAL AUDIT | 10 (offline curated) |
| FALSE ACTIONABLE | 0 |
| PRECISION | **100%** offline |
| SOURCE | **100%** |
| THESIS | **100%** |
| ACTION | **100%** |
| GEOGRAPHY | **100%** |

Live Bethesda SERP audit still required before default-on (watch item).

---

## F. LANE YIELD

| Lane | Queries | URLs | Candidates | Actionable | New | Cost |
|------|---------|------|------------|------------|-----|------|
| EVENT_ASSOCIATION | 3 | 0 | 1 | 1 | 1 | offline |
| TRAINING | 3 | 0 | 2 | 2 | 2 | offline |
| GOVERNMENT_CONTRACTOR | 3 | 0 | 2 | 1 | 1 | offline |
| PROJECT_TEAM | 3 | 0 | 0 | 0 | 0 | offline |
| RELOCATION | (fixture) | 0 | 1 | 1 | 1 | offline |
| MEDICAL | 3 | 0 | 1 | 0 | 0 | offline |
| UNIVERSITY | (fixture) | 0 | 1 | 1 | 1 | offline |
| SPORTS_ACADEMIC | (fixture) | 0 | 1 | 1 | 1 | offline |
| CONSULTING_PROJECT | (fixture) | 0 | 1 | 1 | 1 | offline |
| CORPORATE_MEETING | 3 | 0 | 0 | 0 | 0 | offline |
| BOARD_COMMITTEE | (fixture) | 0 | 1 | 1 | 1 | offline |
| NONPROFIT | (fixture) | 0 | 1 | 1 | 1 | offline |
| INCENTIVE_RETREAT | 3 | 0 | 0 | 0 | 0 | offline |
| OVERFLOW | 3 | 0 | 0 | 0 | 0 | offline |

Recall V4 merge: `maxLanes=14`, `maxDemandLaneQueries=24`. Yield classes recorded; lanes not permanently disabled from one hotel.

---

## G. NOISE

**LOW_YIELD LANES (this offline set):** CORPORATE_MEETING, PROJECT_TEAM (no fixture hits), INCENTIVE_RETREAT, OVERFLOW (urban Bethesda fixture set).

**FALSE SIGNAL CLASSES:** award-only procurement; local one-hour / no-overnight seminars.

**OUT-OF-MARKET PATTERNS:** remote overflow without evidence (existing V3/CQ rules).

**LOCAL-NO-ROOM PATTERNS:** half-day / webinar / commuter-only language → INVALID.

**DUPLICATE PATTERNS:** series/cycle/alias match before NEW (weekly-delta + canonical ID).

---

## H. SPEED

| Item | Value |
|------|-------|
| TOTAL RUN TIME | ~18s offline suites + canary |
| AVG LANE TIME | N/A (fixture; no live SERP) |
| SERIAL BOTTLENECKS | Live SERP/extract not run this cycle |
| PARALLELIZED | Partial (lane packs independent; live parallel fetch deferred) |
| ORG RESEARCH REUSED | Cache helper shipped; 0 live reuse hits (offline) |

---

## I. COST

| Item | Value |
|------|-------|
| SEARCH/API COST | $0 (offline fixtures) |
| SURFE | **0** |
| PDL | **0** |

---

## J. GENERALIZATION

| Check | Result |
|-------|--------|
| BETHESDA | **PASS** (offline dry-run) |
| RENAISSANCE OFFLINE | **PASS** — URBAN_BUSINESS_MEETINGS (Event → Corporate → Training → Gov → Project) |
| CAMBRIDGE OFFLINE | **PASS** — RESORT_DESTINATION (Incentive → Event → Sports → Corporate → Board) |
| HOTEL-SPECIFIC LOGIC | **NO** |
| CITY-SPECIFIC LOGIC | **NO** |

---

## K. TESTS

| Suite | Result |
|-------|--------|
| `test:gdi-new-opportunities-v1` | **17/17 PASS** |
| `test:gdi-discovery-hygiene-v3` | PASS |
| `test:gdi-discovery-recall-v4` | 16/16 PASS |
| `test:gdi-weekly-delta` | PASS |
| `test:gdi-live-commercial-quality-v1` | 15/15 PASS |
| `test:gdi-share-durability` | PASS |
| `test:gdi-customer-csv-export` | 13/13 PASS |

**NEW:** 17 · **PASS:** 17 · **FAIL:** 0  
Regression relevant: **~80+** · **FAIL:** 0

---

## L. DECISION

1. **Did expanded discovery materially increase credible at-bats?** Yes — +9 incremental credible NEW vs 1 event-only in the offline canary.
2. **Strongest incremental lanes?** Training, government/contractor (with lodging), relocation, consulting/project teams, university, sports housing, board/nonprofit multi-day.
3. **Which produced mostly noise?** Award-only procurement; local medical seminars — correctly INVALID.
4. **Did commercial precision remain intact?** Yes offline (100%). Live SERP precision still a watch item.
5. **Did smaller recurring opportunities surface appropriately?** Yes — 25-room recurring training preferred over mega convention (`preferCommercialFit`).
6. **Did trigger-based research create useful leads?** Classifier shipped (TRIGGER ≠ opportunity); live trigger→thesis loop not exercised this cycle.
7. **Did demand-generator discovery improve yield?** Generator stub + org reuse cache shipped; not live-run.
8. **Did recurrence improve future research without inventing cycles?** Yes — RECURRING_* labels only; no synthetic future opps.
9. **Did discovery run efficiently?** Offline yes; live parallel lane execution still watch.
10. **Were costs controlled?** Yes — Surfe 0 / PDL 0 / no raw-candidate enrichment.
11. **Did existing GDI behavior remain intact?** Yes — V3/V4/CQ/weekly/share/CSV green.
12. **Is this reusable across hotel types?** Yes — archetype lane mix differs without hardcodes.
13. **Become default weekly GDI behavior?** **Not yet as default-on live** until Bethesda live research audit passes. Architecture ready for **controlled weekly use** behind existing Recall V4 path.

---

## M. FINAL VERDICT

**PASSES WITH WATCH ITEMS — CONTROLLED WEEKLY USE**

Watch items before default-on / live promote:
1. Bethesda **live** demand-lane SERP/extract audit vs the 38 canonical set (offline proves qualification, not live yield).
2. Multi-week project `potentialRoomNights` (peak × calendar) can look large — keep ESTIMATED; do not treat as confirmed booking volume.
3. Do not auto-deploy / live-write until live precision is acceptable.

---

## PERSISTENCE / HARDCODE AUDIT

**CODE FILES CHANGED**
- `lib/group-demand-intelligence/demand-signal-types.js` (new)
- `lib/group-demand-intelligence/discovery-hygiene-v3.js`
- `lib/group-demand-intelligence/discovery-recall-v4.js`
- `lib/group-demand-intelligence/weekly-delta.js`
- `lib/group-demand-intelligence/opportunity-list-dto.js`
- `lib/group-demand-intelligence/contact-candidate/ontology.js`
- `lib/group-demand-intelligence/index.js`
- `public/js/group-demand-intelligence/dealality-gdi-ui.js`
- `scripts/test-gdi-new-opportunities-v1.mjs`
- `scripts/gdi-new-opportunities-v1-offline-canary.mjs`
- `package.json`

**DEMAND SIGNAL TYPES ADDED:** full V1 enum (above)  
**QUERY ROUTING CHANGED:** archetype lanes + PROJECT_TEAM + urban maxLanes 14 / query budget 24  
**TRIGGER LOGIC ADDED:** `classifyPublicTrigger` (not auto-opp)  
**RECURRING GRAPH CHANGED:** recurrence classes + demand-generator stub (no invented futures)  
**FIXTURES ADDED:** `data/group-demand-intelligence/evals/gdi-new-opportunities-v1-offline-fixtures.json`  
**TESTS ADDED:** 17 new-opp checks + npm `test:gdi-new-opportunities-v1`

| Hardcode | Result |
|----------|--------|
| HOTEL-SPECIFIC | **NO** |
| CITY-SPECIFIC | **NO** |
| EVENT-SPECIFIC | **NO** |
| ORGANIZATION-SPECIFIC | **NO** |
| PERSON-SPECIFIC | **NO** |

---

## STOP

- No live promotion of new opportunities  
- No contact-coverage work  
- No custom demand themes  
- No day-use demand  
- TRUE_ACTIONABLE bar not lowered  
- No automatic deploy
