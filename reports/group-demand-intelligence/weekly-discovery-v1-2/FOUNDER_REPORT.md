# GDI Weekly Discovery V1.2 — Founder Report

**Hotel:** Bethesda Marriott (`recLuxvwwxID7U2B8`)  
**Generated:** 2026-09-24  
**Weekly run:** `gdir_luxvwwxid7u2_2026_09_24_weekly_fb0844`  
**Orchestrator:** `weekly-discovery-orchestrator-v1-2.js`

---

## A. WEEKLY DISCOVERY

| Metric | Value |
|--------|-------|
| TARGETS DUE | 8 (force-due bounded proof; natural due was thin after prior cycles) |
| TARGETS RESEARCHED | 8 |
| QUERIES | 199 |
| FETCHES | 21 |
| FULL HARVEST LANES EXECUTED | New Opportunities · Demand Generators · Private Events / Venue |

---

## B. OPPORTUNITY RESULTS

| Metric | Count |
|--------|-------|
| CANDIDATES | 5 |
| WATCH | 6 |
| TRUE | 0 |
| PROMOTED | 0 |
| NEW | 0 (this cycle — prior Yield Recovery NEWs preserved) |
| UPDATED | contact material updates only |

TRUE=0 is correct: registry URL harvest does **not** lower the V3 TRUE_ACTIONABLE bar. Prior NRC/ACCP/ACVNU TRUEs remain customer-visible NEW.

---

## C. CONTACT COVERAGE BEFORE

| Tier | Count (all visible) |
|------|---------------------|
| ACTIONABLE / visible bag | 42 |
| NAMED_DIRECT | 3 |
| NAMED_PARTIAL | 3 |
| FUNCTIONAL | 3 |
| ORGANIZATION_PATH | 15 |
| GENERIC_ONLY | 3 |
| NO_CONTACT | 15 |

---

## D. CONTACT COVERAGE AFTER

| Tier | All visible | Actionable (excl. disqualified) |
|------|-------------|----------------------------------|
| NAMED_DIRECT | 4 | 4 |
| NAMED_PARTIAL | 7 | 7 |
| FUNCTIONAL | 7 | 7 |
| ORGANIZATION_PATH | 12 | 12 |
| GENERIC_ONLY | 3 | 3 |
| NO_CONTACT | 9 | **2** |

Actionable NO_CONTACT remaining: UMD alumni watch + Marriott HQ corridor watch (category monitors — no named owner expected).

---

## E. CONTACT IMPROVEMENT

| Metric | Count |
|--------|-------|
| NAMED PEOPLE ADDED | 5 (research cycle) |
| FUNCTIONAL PATHS ADDED | +4 net (incl. NEW opps org paths) |
| GENERIC → NAMED | partial via backfill |
| NO_CONTACT → USABLE | 15 → 9 (−6) |

NEW opportunities now have usable paths:

| Opportunity | Tier after |
|-------------|------------|
| NRC RIC | FUNCTIONAL_CONTACT (official hotel page) |
| ACCP Annual Meeting | FUNCTIONAL_CONTACT (travel/venue page) |
| ACVNU Renal Week | FUNCTIONAL_CONTACT (housing accommodations) |
| Woman's Club of Bethesda | FUNCTIONAL_CONTACT (venue org path) |
| Premier Cup | FUNCTIONAL_CONTACT (HBC housing — pre-existing A) |

---

## F. CONTACT QUALITY (post)

| Grade | Count |
|-------|-------|
| A | 5 |
| B | 2 |
| C | 8 |
| D | 12 |
| E | 15 |

---

## G. BLANKS REMAINING (NO_CONTACT highlights)

Many remaining blanks are `_disqualified` convention-scale watches with thin sources.

| Opportunity | Why blank | Research attempted | Next best path |
|-------------|-----------|--------------------|----------------|
| Several `*_disqualified` | SOURCE_TOO_THIN / no official contact page | Official URL missing | Do not prioritize; already unfit |
| Watch-only corp/alumni | ORGANIZER_UNKNOWN | Category watch pages | Keep WATCH; no named owner expected |
| Remaining actionable with thin pages | NO_PUBLIC_STAFF | Official page scrape | Second-pass site:staff / meetings queries next cycle |

---

## H. SURFE

| Check | Result |
|-------|--------|
| SURFE CALLS | 0 |
| BULK SURFE | NO |
| SURFE PII PERSISTED | 0 |
| ON-DEMAND CTA | PASS (`Get Contact Details` when named + missing HOW) |

---

## I. AIRTABLE

| Check | Result |
|-------|--------|
| PUBLIC CONTACT RECORDS UPDATED | ≥10 (backfill + NEW org paths) |
| PUBLIC CONTACT SOURCES STORED | official URLs on functional paths |
| SURFE EMAILS STORED | 0 |
| SURFE PHONES STORED | 0 |

---

## J. WEEKLY ARCHITECTURE

| Capability | Status |
|------------|--------|
| NEW OPPS HARVEST AUTO-INVOKED | YES |
| DEMAND GENERATOR HARVEST AUTO-INVOKED | YES |
| PE MONITORING AUTO-INVOKED | YES |
| PROMOTION SERVICE | PASS (`promoteQualifiedGdiOpportunity`) |
| CONTACT RESOLUTION POST-PROMOTION | PASS |

---

## K. REGRESSION

| Suite | Result |
|-------|--------|
| GDI / WEEKLY V1.2 | PASS |
| NEW OPPS | PASS |
| DG | PASS |
| PE | PASS |
| COVERAGE | PASS |
| WEEKLY DELTA | PASS |
| CQ | PASS |
| SHARE | PASS |
| CSV | PASS |
| CONTACT / SURFE PERSISTENCE | PASS |

---

## L. DECISION

1. Is the weekly orchestrator now invoking the full validated harvest lanes? **YES**
2. Are new TRUE opportunities promoted automatically? **YES when hygiene TRUE + page bar; this cycle produced 0 new TRUE (standards held)**
3. Did contact coverage materially improve? **YES** — NO_CONTACT 15→9; named 6→11; functional 3→7
4. How many actionable opportunities still have no usable contact path? **~9 NO_CONTACT in full bag (many disqualified/watches)**
5. Are named contacts actually relevant decision-makers? **YES where named — graded via existing role gates; org desks labeled functional**
6. Are functional contacts used appropriately when named people unavailable? **YES**
7. Is Surfe still strictly on-demand and non-persistent? **YES (0 calls, strip guard tested)**
8. Biggest contact-coverage bottleneck? **Official pages often lack published named meetings/housing owners; deeper WHO (Native Who V7 SERP) needed for stubborn blanks — not bulk Surfe**

---

## M. FINAL VERDICT

**GDI WEEKLY HARVEST + CONTACT COVERAGE PASSES — READY FOR CONTINUOUS USE**

Caveat: continuous use should run **due targets only** (not permanent force-due). Full New Opps market SERP rediscovery remains a separate bounded budget lane when registry due set includes EVENT_SERIES without URLs.

---

## PERSISTENCE / GENERALIZATION

**CODE FILES CHANGED:**
- `lib/group-demand-intelligence/weekly-discovery-orchestrator-v1-2.js` (new)
- `lib/group-demand-intelligence/weekly-lane-harvest-v1-2.js` (new)
- `lib/group-demand-intelligence/weekly-contact-resolution-v1-2.js` (new)
- `lib/group-demand-intelligence/contact-tiers-v1-2.js` (new)
- `lib/group-demand-intelligence/promote-qualified-opportunity.js` (preserve NEW on contact update; materialUpdateOnly gate)
- `public/js/group-demand-intelligence/dealality-gdi-ui.js` (Who Sales Should Contact + Get Contact Details CTA)
- `scripts/gdi-weekly-discovery-v1-2-run.mjs` (new)
- `scripts/test-gdi-weekly-discovery-v1-2.mjs` (new)
- `package.json`

**FIXTURES:** unit fixtures in test script  
**TESTS:** `npm run test:gdi-weekly-discovery-v1-2`  
**HOTEL-SPECIFIC LOGIC:** NO  
**HARD-CODED PEOPLE:** NO  
**HARD-CODED DOMAINS:** NO  
**GIT SHA:** `b4352b8` (working tree dirty)  
**WORKING TREE CLEAN:** NO
