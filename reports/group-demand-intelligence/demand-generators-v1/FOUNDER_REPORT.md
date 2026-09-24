# GDI Demand Generator Intelligence V1 — Founder Report

**Generated:** 2026-09-24T12:10:00.000Z  
**Mode:** APPLY (schema + Bethesda canary graph writes)  
**Base:** `appa2cE7FTRmIbB32` (canonical=YES)  
**Hotel canary:** Bethesda Marriott (`recLuxvwwxID7U2B8`) — **data seed only; no Bethesda hardcodes in production logic**  
**Contact enrichment:** Surfe=0 · PDL=0  
**Mass promotion:** NO  

Artifacts:
- `reports/group-demand-intelligence/demand-generators-v1/SCHEMA_DECISION.json`
- `reports/group-demand-intelligence/demand-generators-v1/CANARY.json`
- `lib/group-demand-intelligence/demand-generators/`
- `npm run test:gdi-demand-generators-v1` → **18/18 PASS**

---

## L. FINAL VERDICT

**DEMAND GENERATOR V1 IMPROVED — ONE SMALL CYCLE REMAINS**

Rationale: Persistent ORGANIZATION → PROGRAM → HOTEL FIT → SIGNAL graph is live on the canonical base, reusable across hotels, and correctly refuses invented futures / generator-only opportunities. Bethesda canary stores **20** real generators with programs and fit. Live **TRUE_ACTIONABLE** incremental at-bats this cycle = **0** because lodging-thesis promotion was fail-closed (correct). Next small cycle: guided live harvest on ACTIVE generators to convert WATCH monitors into lodging-backed opportunities without lowering the bar.

---

## A. ARCHITECTURE

| Entity | Decision |
|--------|----------|
| GENERATOR ENTITY | **CREATED** (`Demand Generators` `tblykUVOjGVkawvdD`) |
| PROGRAM ENTITY | **CREATED** (`Demand Programs` `tblxm7wqfFNG37Z2m`) |
| HOTEL GENERATOR FIT | **CREATED** (`Hotel Demand Generator Fit` `tblqamXbIk9n6gXfV`) |
| SIGNAL ENTITY | **CREATED** (`Demand Generator Signals` `tblZ0cOM78uYMCTXx`) |
| GDI OPPORTUNITY TABLE | **REUSED + EXTENDED** (`tblRuReslJMwsfRQj` + `dgGeneratorId` / `dgProgramId` / `dgSignalId` / `hotelGeneratorFitId` + linked-record fields) |

Separate opportunity DB: **NO**  
Inspect-first: no existing GDI Organizations table to reuse — orgs were text-only on opportunities.

Core flow enforced in code:

```text
GENERATOR → TRIGGER → PROGRAM → MARKET → HOTEL THESIS → QUALIFY → GDI OPPORTUNITY
```

Generator records are `isCustomerFacing: false`. Customer UI shows commercial opportunities only; detail may show **Recurring demand source** context after promotion.

---

## B. BETHESDA GENERATORS

| Class | Count |
|-------|------:|
| TOTAL | **20** |
| HIGH | **10** |
| MEDIUM | **5** |
| WATCH | **5** |
| LOW | **0** |

---

## C. GENERATOR TYPES

| Type | Count |
|------|------:|
| ASSOCIATION | 4 |
| UNIVERSITY | 3 |
| SPORTS_ORGANIZATION | 3 |
| GOVERNMENT_AGENCY | 2 |
| HOSPITAL_HEALTH_SYSTEM | 2 |
| PROFESSIONAL_BODY | 2 |
| NONPROFIT | 1 |
| GOVERNMENT_CONTRACTOR | 1 |
| CONSULTING_FIRM | 1 |
| TRAINING_PROVIDER | 1 |

---

## D. PROGRAMS

| Metric | Count |
|--------|------:|
| GENERATORS WITH IDENTIFIED PROGRAM | **19** |
| PROGRAMS | **19** |
| RECURRING_CONFIRMED | **13** |
| RECURRING_HISTORICAL | **1** |
| POSSIBLE_RECURRING | **4** |
| ONE_TIME | **1** |

No invented future cycles from historical recurrence (guard + tests).

---

## E. FUTURE SIGNALS

| Metric | Count |
|--------|------:|
| SIGNALS FOUND | **13** |
| QUALIFIED | **0** |
| TRUE | **0** |
| WATCH | **13** |

All seed monitors correctly stayed **WATCH** (official calendar / program pages without confirmed future lodging thesis). This is intentional fail-closed behavior.

---

## F. INCREMENTAL AT-BATS

| Metric | Count |
|--------|------:|
| GENERATOR-DRIVEN NEW (TRUE promotions) | **0** |
| GENERIC SEARCH OVERLAP (existing GDI org-name matches) | **16** |
| INCREMENTAL GENERATOR AT-BATS | **0** |

**Interpretation:** The generator layer **maps onto the same organizations already producing Bethesda GDI opportunities** (NIH, NIST/NICE, AMWA, ACTS, AFCEA Bethesda, Bethesda Soccer Club, MSYSA, NADO, ACC, AHIMA, NDSS, Potomac Soccer, Georgetown, UMD, etc.). That validates generator selection quality. Incremental *new* TRUE opportunities require the next guided lodging-evidence harvest — not speculative promotion from org identity alone.

---

## G. EFFICIENCY

| Mode | Queries (planned) | URLs (est.) | Qualified | TRUE |
|------|------------------:|------------:|----------:|-----:|
| GENERIC SEARCH | 24 | 120 | 19* | 1* |
| GENERATOR-GUIDED | 59 | 177 | 0 | 0 |

\*Generic “qualified” here = existing GDI org overlaps with the generator set (proxy), not a fresh SERP run. Live SERP was **not** enabled this cycle (`--live-serp` off) to control cost.

Guided query count is higher because each known org gets domain-targeted follow-ups — that is the point of monitoring. Yield-per-query for **new TRUE** remains the open cycle (needs live lodging evidence).

---

## H. AIRTABLE

| Metric | Count |
|--------|------:|
| GENERATOR RECORDS | **20** |
| PROGRAM RECORDS | **19** |
| FIT RECORDS | **20** |
| SIGNAL RECORDS | **13** |
| ORPHANS | **0** (writes followed generator → program/fit/signal order with links) |
| BROKEN LINKS | **0** observed in apply log |

Linked-record fields: Generator ↔ Program, Generator ↔ Fit, Generator/Program/Fit ↔ Signal, Signal/Generator/Program ↔ GDI Opportunities (schema extended).

---

## I. REPLICATION

| Check | Result |
|-------|--------|
| BETHESDA | **PASS** (live canary apply) |
| RENAISSANCE | **PASS** (offline profile → different org-type mix) |
| CAMBRIDGE | **PASS** (offline profile → different org-type mix) |
| HOTEL-SPECIFIC LOGIC | **NO** (audit clean: HOTEL/CITY/ORG/PROGRAM/DOMAIN/PERSON) |

---

## J. COST / SPEED

| Metric | Value |
|--------|-------|
| QUERIES | 24 generic planned + 59 guided planned (SERP spend **$0** this cycle) |
| FETCHES | 0 live SERP |
| RUNTIME | ~33s (Airtable upserts) |
| COST | ~$0 research; Airtable API only |
| SURFE | **0** |
| PDL | **0** |

---

## K. DECISION

| # | Question | Answer |
|---|----------|--------|
| 1 | Can GDI identify real repeat demand generators? | **YES** — 20 diverse, real public orgs aligned to hotel profile |
| 2 | Can those generators be stored persistently? | **YES** — Airtable graph on canonical base |
| 3 | Can programs/series be tracked without inventing future cycles? | **YES** — recurrence stored; invented futures rejected |
| 4 | Does generator-guided research outperform generic broad search? | **NOT YET MEASURED LIVE** — architecture ready; SERP harvest deferred |
| 5 | Did generator intelligence create incremental credible at-bats? | **NOT YET** — 0 TRUE promotions; 16 org overlaps with existing GDI |
| 6 | Is the model reusable across hotels? | **YES** — profile-driven discovery + fit layer; replication audit PASS |
| 7 | Should generator monitoring become part of weekly GDI? | **YES, CONTROLLED** — after one guided lodging-evidence cycle on ACTIVE generators |

---

## Non-negotiables honored

- No speculative opportunities from organizations alone  
- No NEW_TO_HOTEL  
- TRUE_ACTIONABLE bar unchanged  
- No contact enrichment  
- No Bethesda hardcodes in production modules  
- Canonical GDI opportunity model reused  

---

## Recommended next cycle (small)

1. `--live-serp` guided harvest on HIGH/ACTIVE generators only (bounded budget).  
2. Extract lodging/housing/registration signals with dates.  
3. Promote only TRUE_ACTIONABLE drafts into GDI (still no mass auto-promote).  
4. Re-measure incremental at-bats + guided vs generic yield.  
5. Then enable weekly monitor cadence via `nextResearchAt`.

---

## Modules / commands

```bash
npm run test:gdi-demand-generators-v1
npm run gdi:demand-generators-v1-schema -- --apply
npm run gdi:demand-generators-v1-canary -- --apply
# next: npm run gdi:demand-generators-v1-canary -- --apply --live-serp
```
