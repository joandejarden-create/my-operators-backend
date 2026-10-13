# GDI Wave 1 Closure — Person-Boundary V11 + Venue-Locked Overflow

Marker: `gdi_wave1_closure_v11_20260921`  
Policy: **existing Wave 1 hotels only** · no Wave 2 · no live promote · no Webhound · no new Surfe architecture · prior `wave1-*` freezes untouched

## A. WAVE 1 BASELINE (pre-V11)

| Hotel | WHO Precision | False | WHO Coverage |
|---|---:|---:|---:|
| St. Regis Mexico City | 40% | 60% | 12.5% |
| St. Regis Cap Cana | 100% | 0% | 60% |
| Hotel Phillips KC | 58.3% | 41.7% | 57.1% |

## B. PERSON-BOUNDARY FIX

INVALID WAVE1 PERSON FIXTURES: **8**

| Class | Count |
|---|---:|
| NAME_FRAGMENT | 1 |
| TITLE / ACADEMIC_TITLE | 2 |
| ORG / DEPT | 2 (1 org + 1 dept) |
| ADJACENT_TEXT_BLEED | 1 |
| TRUNCATED | 0 |
| AGGREGATOR | 2 |
| OTHER | 0 |

Fixture file: `data/contact-intelligence/evals/gdi-wave1-v11-person-boundary-fixtures.json` (evidence only — **no string blacklists** in production logic).

## C. TESTS

| Suite | Result |
|---|---|
| PERSON BOUNDARY | **20/20** |
| VENUE CLASSIFICATION | **6/6** |
| PRIOR V9 ENTITY GATE (compat) | **15/15** |
| PRIOR COHORT REGRESSION (offline) | **3/3 cohorts PASS** |

Commands: `npm run test:native-who-v11-person-boundary` · `npm run test:native-who-v11-venue-classification` · `npm run gdi:wave1-closure-v11`

## D. V11 WAVE1 REWHO (offline, frozen TRUE_ACTIONABLE only)

| Hotel | True Actionable | Named WHO | Valid | Invalid (still accepted) | Precision | False | Coverage |
|---|---:|---:|---:|---:|---:|---:|---:|
| St. Regis Mexico City | 8 | 1 | 2 | 0 | **100%** | **0%** | 12.5% |
| St. Regis Cap Cana | 5 | 3 | 4 | 0 | **100%** | **0%** | 60% |
| Hotel Phillips KC | 7 | 4 | 7 | 0 | **100%** | **0%** | 57.1% |

Refreeze: `data/contact-intelligence/evals/gdi-wave1-who-v11-refreeze.json` (no Surfe/PDL in this freeze).

## E. FALSE-POSITIVE CLOSURE

| Pattern | Status |
|---|---|
| NAME FRAGMENTS | **FIXED** |
| TITLE-AS-PERSON | **FIXED** |
| ORG-AS-PERSON | **FIXED** |
| AGGREGATOR MISASSOCIATION | **FIXED** (structural conference-listing host + corroboration law) |
| MC/MAC | **PASS** |
| INTERNATIONAL NAMES | **PASS** |

## F. VENUE CLASSIFICATION

| Metric | Count |
|---|---:|
| PRIMARY → OVERFLOW | **2** (CANTO, AAEA) |
| PRIMARY → WATCH/CLOSED | **0** |
| PRIMARY RETAINED WITH EVIDENCE | **0** venue-locked exceptions |
| OVERFLOW status reinforced (already OVERFLOW) | **1** (WAPOR → `PRIMARY_VENUE_SELECTED_OVERFLOW_POSSIBLE`) |

Material changes:

1. **CANTO 2026** — PRIMARY → OVERFLOW — host Hard Rock Punta Cana; overflow/housing thesis retained for Cap Cana.
2. **AAEA Annual Meeting** — PRIMARY → OVERFLOW — host Marriott Downtown; overflow/housing thesis for Phillips.
3. **WAPOR** — type already OVERFLOW; venue status upgraded to primary-selected + overflow possible (Barceló Reforma).

Artifact: `data/group-demand-intelligence/evals/gdi-wave1-venue-locked-v11-reclass.json`

## G. WHO RECALL

| Path | Count |
|---|---:|
| V9 DIRECT (named opps, not v10 flag) | varies by hotel; Cap Cana + Phillips retain prior valids |
| V10 RECOVERED retained | Kseniya/Francisco; Teresa/Chrisal; Mary/Eli |
| STILL NO WHO (opportunities) | 12 of 20 (Mexico City thin) |
| OVERALL WHO COVERAGE | **8/20 = 40%** (unchanged; precision-first, no forced recall) |

## H. REACHABILITY

| Metric | Value |
|---|---|
| NAMED WHO (people, accepted) | **13** |
| CONTACTABLE | unchanged vs Wave 1 staged Surfe (~40% path) — **no new provider calls** |
| HIGH CONTACTABILITY | unchanged |
| SURFE NEW CALLS | **0** (reuse staged; no newly recovered WHO requiring email/mobile) |
| PDL NEW CALLS | **0** |

PDL remains field-level fallback; Wave 1 + recent validation still show **near-zero incremental** after Surfe — demote further only after another staged cycle confirms.

## I. CROSS-COHORT REGRESSION

| Cohort | Result | Valid retained | Valid lost | New false |
|---|---|---:|---:|---:|
| BETHESDA | **PASS** | 8 | 0 | 0 |
| WS/REN | **PASS** | 14 | 0 | 0 |
| NOW/CAMBRIDGE/JW | **PASS** | 20 | 0 | 0 |

## J. PERSISTENCE / GENERALIZATION CHECK

CODE FILES CHANGED:
- `lib/group-demand-intelligence/contact-candidate/native-who-v3/person-boundary-v11.js` — entity typing before role; fragment/honorific/academic/org/dept gates; multi-entity block parse; Mc/Mac + LATAM shapes; positive person evidence
- `lib/group-demand-intelligence/contact-candidate/native-who-v3/aggregator-who-gate-v11.js` — aggregator may discover events, cannot independently establish WHO
- `lib/group-demand-intelligence/contact-candidate/native-who-v3/official-domain.js` — structural conference-listing aggregator host detection
- `lib/group-demand-intelligence/contact-candidate/native-who-v3/discover-who-v9.js` — wires V11 person + aggregator gates on confirmation path
- `lib/group-demand-intelligence/venue-locked-classification-v11.js` — venue-selected → OVERFLOW/WATCH/CLOSED with overflow evidence requirement + priority cap
- `docs/ai-build-system/BUILD_DECISIONS.md` — GDI pilot correction operating law
- `package.json` — test + closure scripts
- `scripts/test-native-who-v11-person-boundary.mjs` / `test-native-who-v11-venue-classification.mjs` / `gdi-wave1-closure-v11.mjs`

FIXTURES ADDED: **8** Wave-1 invalid WHO + synthetic suite classes (full block, honorific, title, academic, org, dept, location, Mc/Mac, accented, Hispanic, Asian-order, multi-person, aggregator ± corroboration)

Hotel/person/event-specific production logic: **NO** (fixtures are historical evidence only)

TESTS ADDED/UPDATED: **2** suites (20 + 6) + prior V9 suite still green (15)

TESTS PASS: **41/41** relevant offline (20+6+15)

HOTEL-SPECIFIC LOGIC: **NO**  
PERSON-SPECIFIC LOGIC: **NO**  
EVENT-SPECIFIC LOGIC: **NO**  
HARDCODED DOMAIN RULES: **NO** (structural `*conferences.com` / listing-aggregator patterns only)

REUSABLE RULES CREATED:
| FAILURE | REUSABLE RULE | TEST |
|---|---|---|
| Name fragment | Honorific/incomplete span + source longer-person + block parse | honorific fragment / full block recovery |
| Title-as-person | Academic/role phrase entity type before role scoring | academic title / title phrase |
| Org/dept-as-person | Institutional token dominance → ORG/DEPT | org/dept fixtures + Wave1 fixtures |
| Aggregator mis-association | Source-to-event relation; aggregator WHO needs official corroboration | aggregator ± corroboration |
| Venue-locked PRIMARY | Selected host + overflow evidence → OVERFLOW; no-overflow cannot HIGH PRIMARY | venue suite C–F |

BRANCH: `deploy/adp-final-trust-closure-20260910`  
PRE-COMMIT SHA: `ded77484a6b71d7d128edc1600880b3051369389`  
POST-COMMIT SHA: *(not committed — founder did not request commit)*  
WORKING TREE CLEAN: **NO** — V11 closure changes are local uncommitted (plus unrelated dirty tree)

ARTIFACTS (new only; prior wave1-* preserved):
- `data/contact-intelligence/evals/gdi-wave1-v11-person-boundary-fixtures.json`
- `data/contact-intelligence/evals/gdi-wave1-who-v11-refreeze.json`
- `data/group-demand-intelligence/evals/gdi-wave1-venue-locked-v11-reclass.json`
- this report

## K. DECISION

1. Is the systemic person-boundary problem now closed? **YES for the Wave-1 failure classes (offline + gates)**  
2. Did WHO precision return to ≥90% for all three Wave 1 hotels? **YES — 100% / 100% / 100%**  
3. Did prior cohorts remain stable? **YES**  
4. Did venue-locked opportunities classify correctly? **YES** (CANTO/AAEA → OVERFLOW; WAPOR status reinforced)  
5. Is WHO coverage commercially usable? **NOT YET** — overall **40%** (Mexico City 12.5%); precision fixed, recall still thin  
6. Is Surfe still the primary reachability provider? **YES**  
7. Should PDL remain field-level fallback? **YES, for now** — yield still near-zero; do not remove  
8. Are all fixes persisted as reusable code/tests? **YES in working tree** — **not yet committed**  
9. Is Wave 1 safe to live-promote after founder review? **NO** — precision gate restored, but coverage/contactability and commit persistence remain before promote  
10. Should Controlled Expansion Wave 2 begin? **NO — HOLD** until coverage/contactability improve on a staged re-WHO+reach pass and changes are committed

## L. FINAL VERDICT

**WAVE 1 CLOSURE PASSES WITH WATCH ITEMS — PROCEED TO WAVE 2 STAGED** is **not** selected.

**ONE REUSABLE WHO GAP REMAINS — HOLD**

Person-boundary false-positive class is closed under V11. Remaining blocker for expansion/promote is **WHO coverage / contactable path (~40%)**, not precision. Do **not** start Wave 2 hotels. Do **not** live-promote. Commit V11 modules/tests when ready, then run a staged reachability refresh only for any newly accepted WHO.

---

STOP: no Wave 2 · no live promote · no new hotels.
