# Cross-Market Replication V1 — Founder Report

**Hotel:** W Rome  
**Provisional hotel ID:** `gdi_hotel_w_rome`  
**Canonical HPC ID:** **MISSING (BLOCKING)**  
**Country:** Italy  
**Market:** Rome  
**Branch:** `deploy/gdi-pe-v1-7-customer-closure`  
**Date:** 2026-09-25  
**Webhound market-local session:** `47dcf4c6-6743-4fee-9716-6e32d059412b` (sidecar research; not production writes)

## U. FINAL VERDICT

**CROSS-MARKET REPLICATION FAILED — ROOT CAUSE REQUIRED**

**Root cause:** Hotel Property Census (platform ALT base `appCCUsuGsE1ifoLk`) has **zero Italy inventory**. W Rome (MARSHA `ROMWV`) cannot resolve to a canonical `rec…` ID. Airtable Fit/Target apply, live GDI weekly, and ADP publish were correctly **refused** rather than inventing identity.

Generic architecture repairs for Europe/multilingual/market-local **did land** and are proven on dry-run. Hotel #4 remains blocked on **Europe census stewardship**, not on Rome/Italy/W hardcodes.

---

## A. HOTEL

| | |
|--|--|
| HOTEL | W Rome |
| HOTEL ID | `gdi_hotel_w_rome` (provisional) — **no HPC `rec…`** |
| COUNTRY | Italy |
| MARKET | Rome |
| MARSHA | ROMWV |
| OFFICIAL | https://www.marriott.com/en-us/hotels/romwv-w-rome/overview/ |
| ADDRESS | 26/36 Via Liguria, Rome, Italy, 00187 |
| ROOMS | 148 |
| MEETING | ~60 m² / 646 sq ft (2 studios) |

Duplicate W Rome HPC rows: **0** (none exist).

---

## B. CANONICAL INPUT READINESS

**AVAILABLE:** official name · MARSHA · Marriott URL · address · city · country · rooms · meeting space · brand · parent  

**DERIVABLE:** peak-room band · demand archetype · IT locale pack · catchment km bands  

**MISSING:** HPC record id · Italy census inventory · company-validated lat/lng · owner/AM · year built · certified ADP peer pack  

**STEWARDSHIP:** Europe/Italy HPC insert for W Rome · ADP peer approval · promote provisional → `rec…`

---

## C. GENERIC SEED

| | |
|--|--|
| FIT DRY | **7** |
| FIT APPLIED | **0** (apply refused — census missing) |
| TARGET DRY | **14** |
| TARGET APPLIED | **0** |
| RECREATE OVERLAP | **100%** fits · **100%** targets |
| HOTEL-SPECIFIC CODE | **0** |
| GEO SCOPES | EU + GLOBAL (US-only ASAE/HCEA/RCMA/IAEE **excluded**) |
| SAMPLE ORGS | PCMA · MPI · SITE · ACTE · ICCA · UFI · IAPCO |

---

## D. MARKET-LOCAL EXPANSION

| | |
|--|--|
| LOCAL GENERATORS (evidence) | **5** (official Turismo Roma / Fiera Roma fetches — not prod hardcodes) |
| PROGRAMS | included in 5 (Rom-E, Piano City, Rome Future Week) |
| VENUES | **1** (Fiera Roma) |
| EVENT SOURCES | **1** (Turismo Roma) |
| NEW TARGETS (proposed, not applied) | **5** |
| QUERY PLAN TASKS | **100** (25 categories × it/en × years) |
| LANGUAGES | **it, en** |
| HARDCODED ROME ORGS IN PROD | **0** |
| WEBHOUND | session `47dcf4c6-…` **failed** ($0 spend) before research; official page fetches used instead |

Material improvement vs portable nationals alone: **YES** (destination bureau + exhibition hub + named 2026 programs with official URLs).

---

## E. GDI LIVE RUN

| | |
|--|--|
| RUN ID | **NOT RUN** |
| REASON | Census identity blocking + apply refused |
| TARGETS DUE | n/a |
| TRUE / NEW / UPDATED | n/a |

---

## F. GDI BY LANE

| Lane | Status |
|------|--------|
| All lanes | **DEFERRED** — no live weekly without canonical hotel |

---

## G. GDI QUALITY

N/A live. Dry-run seed quality: no Bethesda/NYC bleed · 0 US-only associations · CQ bar unchanged (not exercised).

---

## H. CONTACT

N/A live. Surfe auto: **0**. Surfe PII: **0**.

---

## I. GDI JEV (seed shadow)

| | |
|--|--|
| CALLS | 10 |
| AGREEMENT | 100% on seed routing sample |
| APPLY READY | **NO** |
| PRODUCTION BEHAVIOR | unchanged |

---

## J. ADP READINESS

| | |
|--|--|
| EXISTING PROPERTY | Fixture scaffold `adp_w_rome` — **not certified** |
| EXISTING PERIOD | **NO** |
| PEER PACK | **NO** — stewardship required |
| CORE/ENTITY | **NO** |
| SCENARIOS | **0** certified |
| BLOCKERS | Census link · peer stewardship · baseline not authorized |

---

## K–L. ADP RUN / QUALITY

**NOT RUN** — stewardship + census gate.

---

## M. ADP JEV

| | |
|--|--|
| RUN | YES (shadow adapter smoke) |
| ADAPTER | `lib/ai-demand-positioning/adp-jev-shadow.js` |
| APPLY | **NO** |
| PRODUCTION ADP CHANGED | **NO** |
| FORBIDDEN ENFORCED | finding/root-cause/competitor/action/narrative/persistence |

---

## N. CROSS-HOTEL QUALITY

| Metric | Bethesda | Renaissance | W Rome |
|--------|----------|-------------|--------|
| Canonical HPC | yes | yes | **NO — blocking** |
| Generic seed | yes | yes | yes (provisional dry) |
| Market-local depth | mature DMV | gap (portable nationals) | **plan ready; evidence pending** |
| Live GDI weekly | yes | yes | **blocked** |
| ADP publish | yes | yes | **blocked** |
| Locale | en | en | **it+en** |

Do not rank hotels — compare system readiness.

---

## O. SECURITY

| | |
|--|--|
| AUTH / SHARE ISOLATION (capability matrix) | **PASS** (Bethesda ⟂ Renaissance ⟂ W Rome) |
| CROSS-HOTEL LEAKS (share token) | **0** |
| SECRET / PROVIDER PII / TEST DATA (this cycle) | **0** exercised on live GDI (no live bag) |

---

## P. AIRTABLE / STORAGE

| | |
|--|--|
| WRONG-BASE | **0** (apply refused) |
| WRITES THIS CYCLE | **0** to Airtable |
| CROSS-HOTEL LINKS | **0** |

---

## Q. PERFORMANCE

| Flow | W Rome | Notes |
|------|--------|-------|
| Seed dry | ~fast | 7 fits / 14 targets |
| Market-local plan | ~fast | 100 tasks |
| Live weekly / ADP | n/a | blocked |

No >25% structural regression measured on reference hotels (replication gate still **17/17**).

---

## R. MANUAL INTERVENTIONS

**TOTAL: 5**

| # | Step | Reason | Category |
|---|------|--------|----------|
| 1 | Multi-base census probes | Resolve W Rome HPC | LEGITIMATE_STEWARDSHIP |
| 2 | Provisional GDI/ADP config from Marriott facts | No census row | LEGITIMATE_STEWARDSHIP |
| 3 | Refuse `--apply` | Identity gate | OPERATIONAL |
| 4 | Webhound market-local dataset failed ($0); fell back to official page fetches | OPERATIONAL |
| 5 | ADP peer pack not approved | Process requires human | LEGITIMATE_STEWARDSHIP |

Vs Renaissance actual **6** — cross-market introduced **census stewardship debt**, not hotel-specific engineering debt.

---

## S. GENERALIZATION

| | |
|--|--|
| W ROME PROD HARDCODES | **0** |
| ITALY PROD HARDCODES | **0** (IT = country locale pack, not hotel switch) |
| ROME PROD HARDCODES | **0** |
| W BRAND PROD HARDCODES | **0** |
| GENERIC FIXES | IT locale · country-scoped portable seeds (EU/GLOBAL) · market-local expansion module · ADP Jev shadow adapter · apply refuse on missing census |

---

## T. DECISION

1. Did generic seeding work for W Rome? **YES** (dry-run; provisional ID)  
2. Did market-local discovery materially improve beyond portable nationals? **YES** (5 evidence-backed targets proposed; 0 Airtable apply)  
3. Did GDI run end to end in a European market? **NO** — census block  
4. Was GDI opportunity quality comparable? **N/A** — no live run  
5. Multilingual/source defects? **None found in architecture**; IT pack added  
6. Contact paths without bulk enrichment? **N/A** live  
7. Did Jev generalize across markets? **Seed shadow OK**; European live outcomes **unknown**  
8. Closer to controlled apply? **NO** — apply stays off  
9. ADP pack creation vs stewardship? **Both required**; census first  
10. ADP end to end? **NO**  
11. ADP Jev value? **Adapter exists; not measured on live findings**  
12. Full three-hotel isolation? **PASS** (share capability matrix)  
13. Manual interventions? **5**  
14. New engineering debt? **Census Europe gap** (stewardship), not W/Rome if-switches  
15. Hotel #4 without architecture work? **NO** until Europe census insert path exists  
16. Repeatable across hotels **and** markets? **Architecture yes · census unlock required for Europe**

---

## PERSISTENCE / GENERALIZATION

**CODE FILES CHANGED:**
- `lib/group-demand-intelligence/discovery-recall-v4.js` (IT locale)
- `lib/group-demand-intelligence/research-coverage/portable-seed-templates.js` (geoScopes v2)
- `lib/group-demand-intelligence/research-coverage/onboard-hotel-research-graph.js`
- `lib/group-demand-intelligence/research-coverage/market-local-expansion.js` (**new**)
- `lib/group-demand-intelligence/research-coverage/seed-jev-shadow.js`
- `lib/group-demand-intelligence/research-coverage/index.js`
- `lib/ai-demand-positioning/adp-jev-shadow.js` (**new**)
- `config/group-demand-intelligence/hotels/gdi_hotel_w_rome.json` (**new**)
- `fixtures/ai-demand-positioning/w-rome-property-profile.json` (**new**)
- `fixtures/hotel-census/adp-gdi-hotel-alias-map-v1.json`
- `scripts/gdi-cross-market-w-rome-phase0-2.mjs` (**new**)
- `scripts/test-gdi-cross-market-replication-v1.mjs` (**new**)
- `scripts/test-gdi-cross-market-three-hotel-isolation-v1.mjs` (**new**)

**TESTS ADDED:** cross-market replication V1 · three-hotel isolation V1  

**FIXTURES ADDED:** W Rome ADP profile · alias map pending_census  

**W ROME-SPECIFIC PRODUCTION LOGIC:** NO  
**ITALY-SPECIFIC PRODUCTION LOGIC:** NO (generic country locale)  
**BETHESDA DEFAULT LOGIC:** NO  
**RENAISSANCE DEFAULT LOGIC:** NO  
**JEV PRODUCTION BEHAVIOR CHANGED:** NO  

**FINAL SHA:** *(filled after commit)*  
**PUSH:** *(filled after push)*  
**FILES LEFT DIRTY:** market-alerts worktree + unrelated local artifacts (not committed)

---

## STOP

Critical proof status:

`canonical hotel record` → **FAIL (missing)**  
`→ generic seed` → **PASS (dry)**  
`→ market-local expansion` → **PASS (plan) / PENDING (entities)**  
`→ live GDI` → **BLOCKED**  
`→ contact / ADP / persistence / CX` → **BLOCKED on census**

**Next unlock:** steward-insert W Rome into Hotel Property Census (Europe batch), bind `gdi_hotel_w_rome` → `rec…`, re-run apply → weekly → ADP stewardship → publish. No Rome/Italy/W production switches required for that path.
