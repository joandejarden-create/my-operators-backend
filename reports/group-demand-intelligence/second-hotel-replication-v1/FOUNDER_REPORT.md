# Second Hotel Replication V1 — Founder Report

**Hotel:** Renaissance New York Times Square Hotel  
**Hotel ID:** `recG66DQJKP2c0UNh`  
**ADP property:** `adp_renaissance_times_square`  
**Reference:** Bethesda Marriott `recLuxvwwxID7U2B8`  
**Branch:** `deploy/gdi-pe-v1-7-customer-closure`  
**Date:** 2026-09-25

## T. FINAL VERDICT

**SECOND HOTEL REPLICATION PASSES — MINOR AUTOMATION GAPS REMAIN**

---

## A. SECOND HOTEL

| | |
|--|--|
| HOTEL | Renaissance New York Times Square Hotel |
| HOTEL ID | `recG66DQJKP2c0UNh` |

---

## B. GDI LIVE RUN

| | |
|--|--|
| RUN ID | `gdir_g66dqjkp2c0u_2026_09_25_weekly_d85efe` |
| TARGETS DUE | 12 (force-due bounded first live) |
| TARGETS RESEARCHED | 12 |
| QUERIES | 100 |
| FETCHES | 27 |
| CANDIDATES | 9 |
| TRUE | 0 |
| NEW PROMOTED | 0 |
| UPDATED | 0 |
| WATCH | 9 |

Surfe auto calls: **0**. Seed Fits/Targets did **not** inflate as weekly NEW.

---

## C. GDI BY LANE

| Lane | Targets | Researched | Candidates | TRUE | Promoted |
|------|---------|------------|------------|------|----------|
| New Opportunities | (playbook-routed) | included | part of 9 | 0 | 0 |
| Demand Generators | (playbook-routed) | included | part of 9 | 0 | 0 |
| Private Events / Venue | 0 this cycle | 0 | 0 | 0 | 0 |
| Sports / Training / Gov | 0 this cycle | 0 | 0 | 0 | 0 |

Lanes executed: **New Opportunities · Demand Generators** (registry seed is portable national associations — expected vs Bethesda medical/federal mix).

---

## D. GDI QUALITY

| Metric | Renaissance |
|--------|-------------|
| OFFICIAL SOURCE % | ~high on generator domains (pcma.org, asaecenter.org, …) |
| CONTACT USABLE % | Named/partial present on bag; 2 NO_CONTACT remain |
| HOTEL FIT COMPLETE % | Territory labels portable (TERRITORY_*) |
| DATE QUALITY | No Jan-1 invention this cycle |
| PROVENANCE | Weekly run + target runs persisted |
| DUPLICATES | 0 new |
| CQ BAR | Held (TRUE=0 correct for registry harvest) |

---

## E. CONTACT

| Tier (after apply) | Count |
|--------------------|-------|
| NAMED_DIRECT | 8 |
| NAMED_PARTIAL | 2 |
| FUNCTIONAL | 0 |
| ORGANIZATION_PATH | 2 |
| GENERIC | 2 |
| NO_CONTACT | 2 |
| SURFE AUTO | **0** |
| SURFE PII | **0** |

Named contact added this cycle: **1**.

---

## F. JEV GDI SHADOW

| | |
|--|--|
| CALLS | 52 live shadow |
| VERDICT | JEV V1 PROMISING — KEEP SHADOW |
| HIGH-CONF WRONG | present on some types (incl. OPPORTUNITY_PREQUAL) |
| APPLY READY | **NO** |

---

## G. GDI VS BETHESDA

| | |
|--|--|
| QUALITY PARITY | **PASS** (CQ / contacts / no bleed after repair — not volume) |
| EXPECTED MARKET DIFFERENCES | Urban Times Square vs DMV medical/federal; smaller mature bag (16 vs 42) |
| QUALITY GAPS | Market-local generator depth beyond portable national templates |
| GENERALIZATION DEFECTS | **Found + fixed:** hardcoded “Bethesda Marriott / NIH / Metro” recommended-action template; “DMV competitiveness” label |

---

## H. ADP STEWARDSHIP

| | |
|--|--|
| PEER CANDIDATES | 5 (Bonvoy L1 Adequacy ADEQUATE) |
| PEERS APPROVED | Encoded in peer-set pack — commercial boundary retained |
| SCENARIOS | 65 (generic scenario universe) |
| MANUAL REVIEW REQUIRED | YES (legitimate stewardship) |
| BLOCKERS | **none** — gate **PASS** |

Declared comps + Bonvoy peers: Marquis, Westin TS, W TS, Sheraton TS, AC TS (+ declared Crowne/Hilton/Knickerbocker for STR context).

---

## I. ADP RUN

| | |
|--|--|
| RUN ID | Republish of certified `adp_period_adp_renaissance_times_square_20260902235427_7d74ca` |
| DEMAND TERRITORIES / PROMPTS | Existing certified period (65-scenario universe available) |
| PROVIDER RUNS | **0 new** this cycle (no full portfolio re-baseline) |
| SUCCESS / FAIL | N/A new pulls |
| RUNTIME | ~11s publish |
| COST | **$0** new LLM |

Filesystem PRIMARY_SOT snapshot saved under `data/ai-demand-positioning/published/adp_renaissance_times_square/`.

---

## J. ADP OUTPUT

Existing certified findings retained via republish. Demand Capture **24.6%** on published period. No fabricated assertions added.

---

## K. ADP JEV SHADOW

| | |
|--|--|
| RUN | **NO** (adapter not built — documented only) |
| CALLS | 0 |
| APPLY | N/A |

---

## L. ADP VS REFERENCE

| Dimension | Result |
|-----------|--------|
| QUALITY PARITY | **PASS** (certified cohort member) |
| EVIDENCE | **PASS** |
| ROOT CAUSE / ACTIONS | Existing certified content |
| UI | Uses published snapshot path |
| PERSISTENCE | **PASS** (filesystem SoT; history Airtable DISABLED) |

---

## M. AIRTABLE / STORAGE

| | |
|--|--|
| WRONG-BASE WRITES | **0** |
| CROSS-HOTEL LINKS | **0** |
| BETHESDA ACTION BLEED AFTER REPAIR | **0** |
| FILESYSTEM SNAPSHOT | **PASS** |
| LIVE OVERLAY | Available via fail-closed ADP base resolution |

---

## N. SECURITY

| | |
|--|--|
| AUTH / SHARE isolation | Not re-run full Playwright matrix this cycle |
| CSV SCOPE | Hotel-scoped pattern unchanged |
| SECRET / TEST LEAKS | **0** observed |
| SURFE | On-demand only |

---

## O. PERFORMANCE

| Flow | Renaissance | Notes |
|------|-------------|-------|
| Weekly V1.2 apply | ~82s | 12 targets / 27 fetches |
| ADP publish | ~11s | no provider calls |
| Bethesda weekly ref | ~similar V1.2 class | No >50% structural regression |

---

## P. MANUAL INTERVENTIONS

| | |
|--|--|
| EXPECTED | 6–8 |
| ACTUAL | **6** |
| LEGITIMATE STEWARDSHIP | 3 |
| AUTOMATION DEBT (closed this cycle) | 2 |
| OPERATIONAL | 1 |

| Step | Category | Reason |
|------|----------|--------|
| Force-due first weekly | OPERATIONAL | Natural due thin after seed |
| Peer pack commercial review boundary | LEGITIMATE_STEWARDSHIP | Retain human judgment |
| Scenario universe acceptance | LEGITIMATE_STEWARDSHIP | Generic 65 scenarios |
| ADP certified period selection / republish | LEGITIMATE_STEWARDSHIP | No new LLM burn |
| Fix Bethesda action template bleed | AUTOMATION_DEBT (fixed) | GENERALIZATION_DEFECT |
| Rescrub Renaissance opportunity copy | AUTOMATION_DEBT (fixed) | Apply generic repair |

**Automate before hotel #3:** market-local Fit expansion beyond portable nationals; bleed regression already gated; optional ADP single-hotel measurement CLI without portfolio apply.

---

## Q. GENERALIZATION

| | |
|--|--|
| RENAISSANCE PRODUCTION HARDCODES | **0** |
| GENERIC CODE IMPROVEMENTS | 4+ (action templates, DMV label, rad bethesdaWinThesis, V1.2 HOTEL_NAMES) |
| RENAISSANCE-SPECIFIC PATCHES | **0** |

---

## R. HOTEL #3 READINESS

| | |
|--|--|
| CAN RUN WITHOUT CODE CHANGE | **YES** for seed → weekly → CQ → contact → ADP republish path |
| REMAINING AUTOMATION GAPS | Market-local generator discovery; formal peer-approval workflow UI; ADP Jev shadow adapter; full share isolation retest |

---

## S. DECISION

1. Renaissance GDI E2E from generic seed? **YES**  
2. GDI quality comparable (not volume)? **YES** after bleed fix  
3. Contacts usable without bulk Surfe? **YES**  
4. Lane fail from hotel-specific assumptions? **NO** (after generic repair)  
5. Jev improve routing? **SHADOW only — keep shadow**  
6. ADP pack stewarded? **YES (gate PASS)**  
7. ADP E2E generic architecture? **YES (republish certified; no new LLM)**  
8. ADP quality vs reference? **YES (certified cohort)**  
9. Hotel-specific production hardcode added? **NO**  
10. Airtable/isolation clean? **YES** (wrong-base 0; bleed 0)  
11. Actual manual interventions? **6**  
12. Automate before #3? Market-local generators; peer-approval UX  
13. Platform repeatable? **YES with minor gaps**  
14. Hotel #3 without major architecture cycle? **YES**

---

## Persistence / generalization

**CODE FILES CHANGED:**  
`qualification-precision.js`, `opportunity-factory.js`, `rad-feedback-enrichment.js`, `gdi-weekly-discovery-v1-2-run.mjs`, plus replication scripts/tests/reports

**TESTS ADDED:** `test-gdi-hotel-action-bleed-guard.mjs`

**RENAISSANCE-SPECIFIC PRODUCTION LOGIC:** NO  
**BETHESDA-SPECIFIC DEFAULT LOGIC:** NO (quarantined)  
**JEV PRODUCTION BEHAVIOR CHANGED:** NO  

Evidence: `reports/group-demand-intelligence/second-hotel-replication-v1/`
