# GDI Discovery Recall + Commercial Evidence V4 — Founder Report

Marker: `gdi_discovery_recall_v4_20260922`  
Baseline SHA: `1a3ebc79d0fe299c4b13d5df704247556e91c3a2`  
As-of: `2026-09-22`  
Offline suite: `test:gdi-discovery-recall-v4` **16/16 PASS** · Hygiene V3 regression **PASS**

Do not treat this as “find more events.” Objective was: find more plausible demand + preserve strict commercial qualification + capture enough evidence for a salesperson to act — without recreating Bethesda sales-noise classes.

---

## A. DISCOVERY FUNNEL

| Hotel | Queries | URLs | Fetched | Parsed | Event Pages | Event Entities | Candidates |
|---|---:|---:|---:|---:|---:|---:|---:|
| Radisson Hotel Santo Domingo | 36 | 319 | 70 | 70 | — | 17 raw | **12** |
| Casas del XVI | 36 | 305 | 63 | 63 | — | 25 raw | **14** |
| Faranda Collection Bogotá | 36 | 324 | 67 | 67 | — | 17 raw | **13** |

Wave 3 baseline: Radisson 2 / Casas **0** / Faranda **0**.

---

## B. ROOT CAUSE

### Radisson Hotel Santo Domingo

QUERY: bilingual + stratified V4 (Spanish SERP `hl=es,gl=do`); URBAN_BUSINESS_MEETINGS archetype  
FETCH: 70 ok / 2 fail  
RENDER: static fetch only (no blanket browser render)  
EXTRACTION: empty batches **1/8** (Wave 3: 3/6)  
CANDIDATE CREATION: find≠qualify (UNKNOWN venue/sourcing/rooms/WHO allowed)  
PRE-HYGIENE: dedupe 17→12 + commercial evidence enrichment  
PRIMARY BOTTLENECK (Wave 3): extract refusal + English SERP — **recovered**

### Casas del XVI

QUERY: Spanish SERP + LUXURY_DESTINATION_SMALL (retreat / board / incentive families — not congress-only)  
FETCH: 63 ok / 9 fail  
RENDER: static  
EXTRACTION: empty batches **0/8** (Wave 3: **6/6**)  
CANDIDATE CREATION: recovered  
PRE-HYGIENE: 25→14  
PRIMARY BOTTLENECK (Wave 3): **EVENT_EXTRACTION_FAILURE** — **cleared**

### Faranda Collection Bogotá

QUERY: Spanish SERP `hl=es,gl=co` + CORPORATE_SECONDARY  
FETCH: 67 ok / 5 fail  
RENDER: static  
EXTRACTION: empty batches **1/8** (Wave 3: **6/6**)  
CANDIDATE CREATION: recovered  
PRE-HYGIENE: 17→13  
PRIMARY BOTTLENECK (Wave 3): **EVENT_EXTRACTION_FAILURE** — **cleared**

---

## C. V4 YIELD

| Hotel | Real Event Candidates | TRUE | Watch | Insufficient | Invalid |
|---|---:|---:|---:|---:|---:|
| Radisson | 12 | **0** | 5 | 1 | 6 |
| Casas | 14 | **0** | 2 | 0 | 12 |
| Faranda | 13 | **0** | 3 | 1 | 9 |

Level 1 (discovery): non-zero candidate universes on all three hotels.  
Level 2 (Hygiene V3 unchanged): **zero engine TRUE_ACTIONABLE** — precision held by refusing weak commercial rows.

---

## D. COMMERCIAL PRECISION

| Hotel | Engine TRUE | Manual TRUE | False | Precision |
|---|---:|---:|---:|---:|
| Radisson | 0 | 0 | 0 | **100%** (vacuous — no TRUE emitted) |
| Casas | 0 | 0 | 0 | **100%** |
| Faranda | 0 | 0 | 0 | **100%** |

≥90% actionable precision: **YES** (no false TRUE; V3 gates untouched).

---

## E. COMMERCIAL COMPLETENESS

Computed on TRUE_ACTIONABLE only → **N/A** (TRUE=0).

Among pre-V3 candidates, evidence fields are present but often weak:

| Signal | Observation |
|---|---|
| Date | Often year-floor `2026-01-01` or PAST vs as-of 2026-09-22; one Casas row CONFIRMED (Red GEALC 2026-09-29) |
| Attendance | Mostly UNKNOWN (after V4 status fix for string `"UNKNOWN"`) |
| Peak rooms | Mostly UNKNOWN — correctly not equated from attendance |
| Venue | Frequently UNKNOWN (allowed at candidate stage) |
| Source | Present on nearly all candidates (URL evidence) |
| Action path | Not salesperson-ready at TRUE=0 |

Operational readiness (TRUE set): date / attendance / peak rooms / venue / named WHO / contactable WHO / source / action — **all N/A until TRUE>0**.

---

## F. FALSE-POSITIVE REGRESSION

| Class | Result |
|---|---|
| REMOTE OVERFLOW | **PASS** (offline catchment demotion) |
| VENUE ALREADY SELECTED | **PASS** (fullyPlaced overlay → V3) |
| LOCAL / ZERO ROOM | **PASS** (offline demotion) |
| UNCONFIRMED FUTURE CYCLE | **PASS** (invention guard) |
| DUPLICATE SERIES | **PASS** (series/cycle identity helpers) |
| SOURCELESS TRUE | **PASS** (readiness demotion) |

Hygiene V3 / V11 / V12 / Surfe / PDL / Bethesda weekly UI: **not modified**.

---

## G. SOURCE / LANGUAGE PERFORMANCE

| Dimension | Result |
|---|---|
| Spanish SERP | Default for DO/CO (`hl=es`, country `gl`) |
| English | Still available as locale alternative stage; not sole default |
| Query selection | Stratified by stage/family (not flat truncation) |
| Empty extract | Collapsed from 6/6 (Casas/Faranda Wave 3) to 0–1/8 |

Query-family yield: stratified routing restored incentive / association / locale families that Wave 3 truncated. No blind query-count inflation beyond bound `maxQueries=36`.

---

## H. WHO

TRUE opportunities: **0**  
NAMED WHO / FUNCTIONAL / NO WHO: **N/A** (WHO stage intentionally skipped when TRUE=0)  
V11/V12: **unchanged**

---

## I. PROVIDERS

SURFE: not invoked (no TRUE)  
PDL: not invoked (no TRUE)

---

## J. CUSTOMER-USEFULNESS

| # | Question | Answer |
|---|---|---|
| 1 | Would salesperson understand when event is? | **Partial** — dates often year-floor or PAST relative to as-of; not yet salesperson-clean |
| 2 | Guestrooms actually plausible? | **Not proven at TRUE** — V3 correctly withheld TRUE; demand thesis fields exist but many are generic |
| 3 | Peak-room data where public? | **Weak** — mostly UNKNOWN (correct non-fabrication) |
| 4 | Attendance where public? | **Weak** — mostly UNKNOWN |
| 5 | Geography commercially sensible? | **Mixed** — archetype routing helps; some Level-1 noise remains (e.g. geo-mismatched mega-events) — V3 blocks TRUE |
| 6 | Proposed action supported? | **N/A** at TRUE=0; V3 venue-lock / overflow guards remain |
| 7 | Usable source? | **Yes** on nearly all candidates |
| 8 | Best public contact surfaced? | **N/A** — WHO not run |
| 9 | Related cycles/sub-events as one relationship? | **Improved** — `eventSeriesId` / `eventCycleId` attached; UI grouping not shipped this cycle |

---

## K. PRIOR COHORT REGRESSION

| Cohort | Result |
|---|---|
| Bethesda FP classes (offline fixtures) | **PASS** |
| Wave 1 / Wave 2 Hygiene V3 suite | **PASS** |
| NOW / Cambridge / JW (V3 retention suite) | **PASS** |

Live Bethesda / Wave 1–2 packs not re-promoted.

---

## L. PERSISTENCE

### Code files (V4 scope)

- `lib/group-demand-intelligence/discovery-recall-v4.js`
- `lib/group-demand-intelligence/commercial-evidence-v4.js`
- `lib/group-demand-intelligence/native-blind-discovery.js` (locale SERP, stratified budget, extract telemetry, evidence enrich)
- `lib/group-demand-intelligence/discovery-contract.js` (dependency)
- `lib/group-demand-intelligence/candidate-dedupe.js` (dependency)
- `lib/group-demand-intelligence/discovery-completeness-gate.js` (dependency)
- `scripts/test-gdi-discovery-recall-v4.mjs`
- `scripts/gdi-discovery-recall-v4-rerun.mjs`
- `package.json` scripts: `test:gdi-discovery-recall-v4`, `gdi:discovery-recall-v4`
- Reports: funnel + this founder report

### Hardcode audit

| Check | Result |
|---|---|
| HOTEL-SPECIFIC PROD LOGIC | **NO** |
| CITY-SPECIFIC PROD LOGIC | **NO** |
| EVENT-SPECIFIC PROD LOGIC | **NO** |
| PERSON-SPECIFIC PROD LOGIC | **NO** |
| DOMAIN BLACKLIST | **NO** |

Market locale + demand-archetype configuration: **acceptable**.

TESTS: **16/16** V4 offline · Hygiene V3 suite **PASS**

COMMIT SHA: *(filled after commit)*

---

## M. DECISION

1. Where was Wave 3 discovery collapsing? **OpenAI extract returned empty candidate arrays** despite successful fetches (EVENT_EXTRACTION_FAILURE), amplified by US-English SERP and flat query truncation.
2. Did V4 restore meaningful discovery recall? **YES** — Radisson 12, Casas 14, Faranda 13.
3. Did Casas and Faranda move above zero? **YES**.
4. Did commercial precision remain ≥90%? **YES** (0 FALSE TRUE).
5. Avoid Bethesda sales-noise classes? **YES** (offline guards + V3 unchanged).
6. Is date evidence trustworthy? **NOT YET** — year-floor / PAST-as-of noise remains a watch item.
7. Room demand / attendance / peak rooms materially better? **Fields exist; confirmed values still scarce** (correct non-fabrication).
8. Geography/overflow commercially credible? **Guards PASS offline; live TRUE not emitted**.
9. Duplicate/series identities improved? **YES** (series/cycle IDs on candidates).
10. Contact research surfacing named staff? **N/A** (TRUE=0; V11/V12 untouched).
11. Is V4 reusable across markets? **YES** — locale + archetype config, no hotel/city hardcodes.
12. Is Wave 3 closure ready? **Discovery gap closed; salesperson-ready TRUE yield still zero — one small evidence-quality cycle remains.**

---

## N. FINAL VERDICT

# V4 IMPROVED WITH WATCH ITEMS — ONE SMALL CYCLE REMAINS

### What passed
- Native discovery yield recovered on all three Wave 3 hotels (Casas/Faranda out of zero).
- Find ≠ qualify separation held; Hygiene V3/V11/V12 untouched.
- False-actionable classes closed offline; no sales-noise TRUE emitted.
- Reusable locale + demand-archetype routing (Casas as LUXURY_DESTINATION_SMALL, not a Casas-specific rule).

### Watch items (next small cycle — not Wave 4)
1. **Date evidence quality** — stop year-floor `YYYY-01-01` invention; keep UNKNOWN when only year is known; FUTURE_CYCLE/WATCH for unconfirmed next cycles.
2. **Level-1 REAL_EVENT quality** — reduce social-only / geo-mismatched mega-event noise without re-collapsing extract yield.
3. **Evidence completeness when TRUE becomes possible** — confirmed date / attendance / peak rooms / lodging thesis / action path must be measurable on any future TRUE set.
4. **Do not live-promote** Wave 3 hotels until a TRUE set exists with completeness scorecard ≥ salesperson bar.

No Wave 4. No live promote.
