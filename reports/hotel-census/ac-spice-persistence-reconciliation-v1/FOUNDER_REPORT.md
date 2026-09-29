# AC A Coruña + Spice Island — Persistence Reconciliation Founder Report

Generated: 2026-09-29  
Branch: `deploy/gdi-pe-v1-7-customer-closure`  
Preflight HEAD: `6d6a7714a556f3f86b7bb8b959f90063611b47c5`  
Canonical intelligence base: `appa2cE7FTRmIbB32`  
Forbidden legacy base: `appvtnDurnMSjINP6` (guard active)  
HPC base (ALT): `appCCUsuGsE1ifoLk`

Machine-readable IDs:  
`reports/hotel-census/ac-spice-persistence-reconciliation-v1/ALL_GDI_ADP_ATTR_IDS.json`  
`reports/hotel-intelligence/ac-spice-reconciliation-v1/SUMMARY-apply.json`  
`reports/hotel-census/ac-spice-persistence-reconciliation-v1/CLEAN_PROCESS_ISOLATION.json`

---

## A. EXECUTIVE STATUS

| Hotel | HPC | HI | ADP | GDI Persistence | GDI Ready | Final |
|---|---|---|---|---|---|---|
| AC Hotel A Coruña | PASS | PASS (5/7) | PASS certified | PASS (7 fits / 14 targets / 2 runs) | 0 ready (quality held) | **AC HPC + HI + ADP PASS — GDI QUALITY HELD, PERSISTENCE PASS** |
| Spice Island Beach Resort | PASS | PASS (6/7) | PASS certified | PASS (7 fits / 14 targets / 1 run) | 0 ready (quality held) | **SPICE HPC + HI + ADP PASS — GDI QUALITY HELD, PERSISTENCE PASS** |

---

## B. HPC

### AC
- HPC ID: `rec2PVBDavppGpenm`
- EXISTS IN AIRTABLE: **YES**
- Classify: **EXACT_CANONICAL_MATCH**
- DUPLICATES: **0**
- Verified: AC Hotel A Coruña · Enrique Mariñas 36 · A Coruña · 15009 · Spain · 116 rooms · AC Hotels by Marriott · LCGCO identity via Property Identity Key / census link · official Marriott URL

### SPICE
- HPC ID: `recKRJjcPnb4tVDDS`
- EXISTS IN AIRTABLE: **YES**
- CREATE/UPDATE: **BIND existing (no create)**
- Classify: **EXACT_CANONICAL_MATCH**
- DUPLICATES: **0** (search returned nearby Grand Anse inventory; Spice Island Beach Resort exact = 1)
- Verified: 64 suites · St George's · Grenada · SLH · official spiceislandbeachresort.com

---

## C. HOTEL INTELLIGENCE RECORDS

### AC (`rec2PVBDavppGpenm`)
| Table | Count | IDs |
|---|---|---|
| Commercial Profile | 1 | `recaPI3mGBJqMNR5Z` |
| Event Spaces | 6 | `recQMn2B8jEpN8W5b`, `recKCc4GBbh3rll8Z`, `recSE6LyfoNNbqN8s`, `rec1ZqDOHMUEyl605`, `recfVmd6vgOmKe8Wv`, `rec0GToL4nu1UERbq` |
| Demand Nodes | 7 | `recY41zDD13MKLGN6`, `reckJH7hIbyJrfY8P`, `rec4nf4NR9dhnneJR`, `recRDLAk0tEm9asea`, `recDm6vjTOQSgjtFx`, `recR3OAFIy8lZlpdi`, `recbJhboa2bmDqWKA` |
| Seasonality / Need Periods | 0 / 0 | empty by policy (no hotel-supplied need periods) |
| Evidence | 15 | see `ac-hi-apply-apply.json` |
| Completeness | **5/7** | identity + commercial + meeting + event spaces + demand nodes |

### SPICE (`recKRJjcPnb4tVDDS`)
| Table | Count | IDs |
|---|---|---|
| Commercial Profile | 1 | `recGmqE6vHJHYh20Q` |
| Event Spaces | 1 | `recYwSBH6NfUsiHM3` (outdoor wedding / beach venue) |
| Demand Nodes | 6 | `recC2TFRExQFSoq1D`, `recwtpIvaAyco6HhW`, `rechpPglxHTPOt4Qc`, `recABMMlGVQq5chrU`, `recX9XZRIfjCHnyqF`, `reckI3SR4keY9FILg` |
| Seasonality | 2 | `rec8xgwNu4VeTcfc6`, `recXlZooexCHNy2Yd` (public Caribbean high season + NOAA hurricane window) |
| Need Periods | 0 | empty by policy |
| Evidence | 11 | see `spice-hi-apply-apply.json` |
| Completeness | **6/7** | need periods empty by policy |

---

## D. ADP ATTRIBUTES

### AC
- ACTIVE: **39**
- USED: **37**
- UNUSED: **2**
- CREATED: **39**
- UPDATED: **0**
- DEACTIVATED: **0**
- Missing critical: none

### SPICE
- ACTIVE: **47**
- USED: **43**
- UNUSED: **4**
- CREATED: **47**
- UPDATED: **0**
- DEACTIVATED: **0**
- Missing critical (honest): Total Meeting Space Sq Ft, Meeting Room Count — indoor inventory not quantified on first-party pages

Full attribute Airtable IDs: `ALL_GDI_ADP_ATTR_IDS.json` (AC 39 / Spice 47).

---

## E. LEGACY DEPENDENCIES

| Hotel | Dependency | Classification | Blocking? |
|---|---|---|---|
| AC | `fixtures/ai-demand-positioning/ac-hotel-a-coruna-property-profile.json` | LEGACY_TEMPORARY (HI now Airtable-primary; fixture still used by ADP profile compiler) | NO |
| AC | `config/group-demand-intelligence/hotels/rec2PVBDavppGpenm.json` | CONFIG_ALLOWED | NO |
| Spice | `fixtures/ai-demand-positioning/spice-island-beach-resort-property-profile.json` | LEGACY_TEMPORARY | NO |
| Spice | `config/group-demand-intelligence/hotels/recKRJjcPnb4tVDDS.json` | CONFIG_ALLOWED | NO |
| Both | Published ADP report JSON on disk | CANONICAL_DATA (filesystem snapshot history) | NO |
| Both | Hotel-specific HI production code | **0** (shared `researchHotelIntelligence` / `applyHotelIntelligencePacket`) | NO |

---

## F. ADP

### AC
- PROPERTY ID: `adp_ac_hotel_a_coruna`
- HPC BINDING: **PASS** (`census-links` + alias map → `rec2PVBDavppGpenm`)
- PERIOD: `adp_period_adp_ac_hotel_a_coruna_20260929113035_c74256`
- CERTIFIED: **YES** (252/252 prior baseline; immutable; not overwritten)
- CURRENT PUBLISHED: **YES** (`publishStatus: Live`)

### SPICE
- PROPERTY ID: `adp_spice_island_beach_resort`
- HPC BINDING: **PASS** → `recKRJjcPnb4tVDDS`
- PERIOD: `adp_period_adp_spice_island_beach_resort_20260929123443_966d88`
- CERTIFIED: **YES** (252/252)
- CURRENT PUBLISHED: **YES**

Note: published report wrapper `censusRecordId` field is null in manifest payload envelope; binding is via census-link registry (canonical). Not rewritten into historical evidence.

---

## G. ADP RESULTS

### AC
- CONSIDERATION: **52.8%** (133/252)
- PRESENCE (scenario): **76.2%** (48/63)
- PROPERTY REALITY GAP: **57.1%**
- TOP FINDINGS: Top-3 prominence strength; consideration consistency constraint; AI Reality Gap 4 key attributes

### SPICE
- CONSIDERATION: **73.8%** (186/252)
- PRESENCE (scenario): **100%** (63/63)
- PROPERTY REALITY GAP: **62.5%**
- TOP FINDINGS: Top-3 prominence; consideration consistency; Reality Gap 15 attributes (meeting inventory thin)

---

## H. GDI PERSISTENCE — AC

| Entity | Count |
|---|---|
| Demand Generators | 0 hotel-scoped rows (generators are org-global; fits link them) |
| Hotel Demand Generator Fits | **7** |
| Research Targets | **14** |
| Research Runs | **2** (`recXlAbX3OTHQiJu5` first cycle; `recW70AA38pHw01DT` market cycle 2) |
| Target Runs | **0** (not created for open-universe discovery; infrastructure ready) |
| Signals | **0** |
| GDI Opportunities | **0** customer-ready |

Fit sample IDs: `rec1BJd9WTBy8mODw`, `recA5FI3pF0eaCQHq`, `recEpvKICLE0rdduF`, `recIXBWLKggAotD04`, `recqDzjNH0PQm7LKR` (+2 in ALL_GDI file)  
Target sample IDs: `rec5jCzY4GbHzijCK`, `recGLRvTs5OeYSokj`, `recIZJnELW5xZupgM`, … (14 total in ALL_GDI file)

**Prior claim 7 fits / 14 targets:** **CONFIRMED in Airtable** — not local-only.

---

## I. GDI PERSISTENCE — SPICE

| Entity | Count |
|---|---|
| Hotel Demand Generator Fits | **7** |
| Research Targets | **14** |
| Research Runs | **1** (`rec5Og31GyLUo1E8d`) |
| Target Runs | **0** |
| Signals | **0** |
| GDI Opportunities | **0** customer-ready |

Fit sample: `recHFTVAFwTptmBNL`, `recIF55COyR8nIQ8X`, `recaFATTfMi6atUiB`, …  
Target sample: `recAIFvU1tEyYgdDP`, `recHxsVvYkRwE5Dmb`, …  

---

## J. GDI DISCOVERY — AC NEW CYCLE (cycle 2)

- QUERIES: **22**
- FETCHES: **25**
- VALID ENTITIES: VALID_WATCH **1** + VALID_FUTURE **1** (true actionable **0**)
- CUSTOMER READY: **0**
- ACTIONABLE: **0**
- WATCH: **1**
- Runtime: ~7.5 min
- Festival/public-calendar noise not promoted

---

## K. GDI DISCOVERY — SPICE (prior first cycle, re-verified)

- QUERIES: **30**
- FETCHES: **33**
- VALID ENTITIES: VALID_WATCH **1**
- CUSTOMER READY: **0**
- ACTIONABLE: **0**
- WATCH: **1**

---

## L. SUMMARY QUALITY

| Hotel | Strong | Adequate | Thin | Invalid |
|---|---|---|---|---|
| AC | 0 promoted | 0 | 0 promoted | quality-held (no promote) |
| Spice | 0 | 0 | 0 | quality-held |

THIN promoted: **0**

---

## M. WHO

| Hotel | Named Direct | Named Partial | Functional | Org Path | Ceiling | Not Researched |
|---|---|---|---|---|---|---|
| AC | 0 | 0 | 0 | 0 | — | 0 promoted (N/A — nothing customer-ready) |
| Spice | 0 | 0 | 0 | 0 | — | 0 promoted |

NOT_RESEARCHED promoted: **0**  
Surfe AUTO: **0** · Surfe persisted PII: **0** · JEV PERSON APPLY: **NO**

---

## N. JEV

### AC (cycle 2 / contact skipped — no customer-ready)
- CALLS: 0 · SAFE APPLY: 0 · HELPFUL: 0 · WRONG: 0

### SPICE (first cycle — no customer-ready population)
- CALLS: 0 · SAFE APPLY: 0 · HELPFUL: 0 · WRONG: 0

---

## O. CUSTOMER READINESS

### AC
- READY: **0**
- HELD: watchlist / hygiene holds
- DQ: INVALID candidates rejected by gate

### SPICE
- READY: **0**
- HELD: quality gate
- DQ: INVALID majority

Zero opportunities = **EMPTY_BECAUSE_NO_READY_OPPORTUNITIES** (not persistence failure).

---

## P. AIRTABLE FOUNDER VISIBILITY

Base: **`appa2cE7FTRmIbB32`** (intelligence / ADP / GDI)  
HPC: **`appCCUsuGsE1ifoLk`** → Hotel Property Census

### AC Hotel A Coruña
| Table | Expected count | Example ID | Founder should see |
|---|---|---|---|
| Hotel Property Census (ALT) | 1 | `rec2PVBDavppGpenm` | AC Hotel A Coruña, 116 rooms, Spain |
| Hotel Commercial Profiles | 1 | `recaPI3mGBJqMNR5Z` | 116 rooms, 6 meeting rooms, 7254 sq ft |
| Hotel Event Spaces | 6 | `recQMn2B8jEpN8W5b` | Banquets + 5 other rooms |
| Hotel Demand Nodes | 7 | `rec4nf4NR9dhnneJR` | Expocoruña, Airport, Port, UDC, … |
| Hotel Intelligence Evidence | 15 | (see apply JSON) | Official events + node evidence |
| Hotel ADP Attributes | 39 | (ALL_GDI file) | Active attributes for ADP |
| Hotel Demand Generator Fit | 7 | `rec1BJd9WTBy8mODw` | Seed fits |
| GDI Research Targets | 14 | `rec5jCzY4GbHzijCK` | Seed targets |
| GDI Research Runs | 2 | `recXlAbX3OTHQiJu5` | First + cycle 2 runs |
| GDI Opportunities | 0 | — | Empty — quality held |

### Spice Island Beach Resort
| Table | Expected count | Example ID | Founder should see |
|---|---|---|---|
| Hotel Property Census (ALT) | 1 | `recKRJjcPnb4tVDDS` | Spice Island, 64 suites, Grenada |
| Hotel Commercial Profiles | 1 | `recGmqE6vHJHYh20Q` | 64 suites, outdoor/wedding flag |
| Hotel Event Spaces | 1 | `recYwSBH6NfUsiHM3` | Outdoor wedding venue |
| Hotel Demand Nodes | 6 | airport / Grand Anse / … | Grenada demand nodes |
| Seasonality & Need Periods | 2 | `rec8xgwNu4VeTcfc6` | Public seasonality only |
| Hotel ADP Attributes | 47 | (ALL_GDI file) | Active attributes |
| Hotel Demand Generator Fit | 7 | `recHFTVAFwTptmBNL` | Seed fits |
| GDI Research Targets | 14 | `recAIFvU1tEyYgdDP` | Seed targets |
| GDI Research Runs | 1 | `rec5Og31GyLUo1E8d` | First-cycle run |
| GDI Opportunities | 0 | — | Empty — quality held |

---

## Q. CLEAN PROCESS

### AC
- HPC: **PASS**
- HI: **PASS** (`fromHiAirtable: true`)
- ADP: **PASS** (Live published period loads)
- GDI: **PASS** (config + fits + targets + runs)
- LOCAL-ONLY DEPENDENCY: **NO** for HI facts / GDI seed state (config JSON is CONFIG_ALLOWED)

### SPICE
- HPC: **PASS**
- HI: **PASS**
- ADP: **PASS**
- GDI: **PASS**
- LOCAL-ONLY DEPENDENCY: **NO** for persisted HI/GDI Airtable state

---

## R. CROSS-HOTEL

| Hotel | Status |
|---|---|
| BETHESDA | PASS (no bleed into AC/Spice targets) |
| RENAISSANCE | PASS |
| HILTON | PASS |
| W ROME | PASS |
| AC | PASS |
| SPICE | PASS |
| LEAKS | **0** |

---

## S. DIRECT QUESTIONS

1. Does AC exist in canonical HPC? **YES** (`rec2PVBDavppGpenm`)
2. Does Spice Island? **YES** (`recKRJjcPnb4tVDDS`)
3. Both in new HI tables? **YES**
4. ADP Attributes derived from HI for both? **YES** (synced after HI apply)
5. AC certified ADP baseline intact? **YES** (period `…c74256`, not overwritten)
6. Spice ADP fully certified? **YES** (period `…966d88`, 252/252)
7. AC 7 fits / 14 targets actually persisted? **YES**
8. If not, why not? **N/A — they are persisted**
9. Spice GDI fits/targets/runs persisted? **YES** (7/14/1)
10. Why zero GDI Opportunities? **Quality gate — no customer-ready candidates**
11. Quality held or persistence failed? **EMPTY_BECAUSE_NO_READY_OPPORTUNITIES** (persistence PASS)
12. Summaries at global standard? **N/A promote path** — gate enforces `buildGdiOpportunitySummary` / readiness when promoting
13. WHO researched for every promoted opportunity? **N/A — 0 promoted**
14. Reconstruct after clean restart? **YES** (verified)
15. Customer-facing state on local-only files? **NO** for HI/GDI Airtable; ADP published snapshot is durable FS by design
16. Wrong-base writes? **0**
17. Duplicate HPC rows? **0**
18. Cross-hotel leakage? **0**
19. Genuinely complete end-to-end? **HPC+HI+ADP+GDI infrastructure YES; customer-ready GDI opportunities NO (quality held)**
20. Remaining incomplete? Target Runs not written for open-universe discovery; Spice indoor meeting sq ft unknown; AC seasonality empty by policy; ADP published envelope `censusRecordId` null (binding via registry); ADP property profile still has LEGACY_TEMPORARY fixture fallback for some compilers

---

## T. FINAL VERDICT — AC

**AC HPC + HI + ADP PASS — GDI QUALITY HELD, PERSISTENCE PASS**

---

## U. FINAL VERDICT — SPICE

**SPICE HPC + HI + ADP PASS — GDI QUALITY HELD, PERSISTENCE PASS**

---

## PERSISTENCE / GENERALIZATION

| Field | Value |
|---|---|
| AC HPC ID | `rec2PVBDavppGpenm` |
| SPICE HPC ID | `recKRJjcPnb4tVDDS` |
| AC ADP PROPERTY ID | `adp_ac_hotel_a_coruna` |
| SPICE ADP PROPERTY ID | `adp_spice_island_beach_resort` |
| AC ADP PERIOD | `adp_period_adp_ac_hotel_a_coruna_20260929113035_c74256` |
| SPICE ADP PERIOD | `adp_period_adp_spice_island_beach_resort_20260929123443_966d88` |
| AC GDI HOTEL ID | `rec2PVBDavppGpenm` |
| SPICE GDI HOTEL ID | `recKRJjcPnb4tVDDS` |
| HOTEL-SPECIFIC PROD LOGIC | **0** |
| SUMMARY HARDCODES | **0** |
| CONTACT HARDCODES | **0** |
| SURFE AUTO | **0** |
| SURFE PERSISTED PII | **0** |
| WEBHOUND REQUIRED | **0** |
| JEV PERSON APPLY | **NO** |
| CARD UI CHANGES | **0** |
| WRONG BASE WRITES | **0** |

### Cost / runtime (this reconciliation pass)
| Slice | Notes |
|---|---|
| AC HI apply | HTTP fetches + Airtable upserts (~30s class) |
| Spice HI apply | ~same |
| AC GDI cycle 2 | ~7.5 min · queries 22 · fetches 25 · $0 researchCostUsd reported |
| Spice GDI | prior cycle only (re-verified; no new discovery this pass) |
| WHO research | 0 (nothing customer-ready) |
| Total provider cost (this pass) | ~$0 billed researchCostUsd; ADP baselines not re-run |

---

STOP — Completion proven against Airtable persistence, not prior Founder claims alone.
