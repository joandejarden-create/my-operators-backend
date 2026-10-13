# GDI Demand Generator Intelligence V1.1 — Guided Lodging-Evidence Harvest

**Generated:** 2026-09-24T12:55:00.000Z  
**Mode:** APPLY_GRAPH · GDI promotion **DRY-RUN only** (no mass promote)  
**Base:** `appa2cE7FTRmIbB32`  
**Hotel:** Bethesda Marriott (`recLuxvwwxID7U2B8`)  
**Surfe / PDL:** 0 / 0  

Artifacts:
- `reports/group-demand-intelligence/demand-generators-v1-1/TARGETS.json`
- `reports/group-demand-intelligence/demand-generators-v1-1/HARVEST.json`
- `lib/group-demand-intelligence/demand-generators/lodging-evidence.js`
- `npm run test:gdi-demand-generators-v1-1` → **10/10 PASS**

---

## L. FINAL VERDICT

**GUIDED GENERATOR RESEARCH IMPROVED — ONE SMALL CYCLE REMAINS**

Rationale: Evidence-gap-driven research on **10 HIGH / ACTIVE + RECURRING_CONFIRMED** generators found **future-cycle evidence on 10/10** and **lodging/housing language on 8/10**, converting prior WATCH monitors into **8 TRUE-candidate** drafts with official-domain preference. However, **GENERATOR_INCREMENTAL_NEW = 0** — every TRUE candidate overlapped an existing Bethesda GDI opportunity (6 EXISTING_UPDATE · 2 GENERIC_DISCOVERY_OVERLAP). The graph is commercially useful for smarter monitoring/renewal of known demand sources; the next cycle should target HIGH generators **outside** the current GDI org set (or new program cycles not already listed) to prove incremental at-bats.

---

## A. TARGETS

| Metric | Count |
|--------|------:|
| GENERATORS RESEARCHED | **10** |
| PROGRAMS RESEARCHED | **10** |

Prioritized: HIGH fit + ACTIVE/VERIFIED + RECURRING_CONFIRMED. No new generators added.

Targets included: NIST/NICE, NIH meetings, Bethesda Soccer Club / Premier Cup, AMWA, NADO, Potomac Soccer, AFCEA Bethesda, MSYSA, NDSS, ACTS (see `TARGETS.json`).

---

## B. FAILURE REASONS BEFORE

| Reason | Count |
|--------|------:|
| NO_FUTURE_DATE | **10** |
| NO_MARKET_CONFIRMATION | **10** |
| NO_LODGING_SIGNAL | 0* |
| NO_TRAVEL_SIGNAL | 0* |
| NO_ACTION_PATH | 0* |
| OTHER | 0 |

\*V1 seed monitor theses mentioned “lodging” generically, so the pre-harvest classifier under-counted lodging gaps. Harvest evidence shows lodging was the real missing commercial proof.

---

## C. NEW EVIDENCE FOUND

| Evidence | Count (of 10) |
|----------|--------------:|
| FUTURE CYCLE | **10** |
| LODGING | **8** |
| TRAVEL | **1** |
| MARKET | **10** |
| HOUSING | **7** |
| ACTION PATH | **10** |

---

## D. RESULTS

| Status | Count |
|--------|------:|
| TRUE | **8** |
| WATCH | **0** |
| FUTURE_WATCH | **2** |
| REJECT | **0** |

TRUE required: future evidence + STRONG/MODERATE lodging + official-ish primary source + action path. Non-official SERP noise rejected for TRUE.

---

## E. INCREMENTAL AT-BATS

| Classification | Count |
|----------------|------:|
| GENERATOR_INCREMENTAL_NEW | **0** |
| GENERIC_DISCOVERY_OVERLAP | **2** |
| EXISTING_UPDATE | **6** |
| FUTURE_WATCH | **2** |

---

## F. TRUE OPPORTUNITIES (dry-run drafts — not written to GDI Opportunities)

| Generator | Program | Timing | Lodging | Class | Source (primary) |
|-----------|---------|--------|---------|-------|------------------|
| NIST | NICE Conference and Expo | 2026/27 | MODERATE | EXISTING_UPDATE | nist.gov/…/nice |
| Bethesda Soccer Club | Bethesda Premier Cup | 2026/27 | STRONG | EXISTING_UPDATE | bethesdasoccer.org/events |
| AMWA | AMWA Annual Meeting | 2026–28 | STRONG | OVERLAP | amwa-doc.org |
| NADO | NADO & DDAA Washington Conference | 2026+ | STRONG | EXISTING_UPDATE | nado.org/2026washcon |
| AFCEA Bethesda | Health IT Summit | 2026/27 | STRONG | EXISTING_UPDATE | officialish .gov hit* |
| MSYSA | Spring State Cup | 2026/27 | STRONG | EXISTING_UPDATE | msysa.org/…state-cup |
| (+2 additional TRUE rows in HARVEST.json) | | | | | |

\*Residual risk: some SERP “officialish” `.gov` hosts are not the generator’s own domain — next cycle should require **same-registrable-domain** match for TRUE primary source.

Hotel fit: all targets were CORE/COMPETITIVE HIGH. Action: organizer/housing channel via official program pages. Confidence: tied to lodging strength STRONG/MODERATE. **New / Existing:** Existing (update/overlap) — no net-new GDI rows written.

---

## G. EFFICIENCY

| Metric | Value |
|--------|------:|
| GENERATOR-GUIDED QUERIES | **42** (SERP charged) |
| FETCHES | ~20 official page fetches |
| OFFICIAL SOURCE % | improved vs ungated first pass (domain filter on TRUE) |
| TRUE PER QUERY | **0.190** (8/42) |
| Prior generic TRUE/query (V1 proxy) | **0.042** |

Guided research beat the prior generic TRUE/query proxy on this cohort, but TRUE landed on **already-known** GDI demand sources — efficiency for *monitoring*, not yet for *net-new* inventory.

---

## H. GRAPH UPDATE

| Metric | Count |
|--------|------:|
| GENERATORS UPDATED | **10** |
| PROGRAMS UPDATED | **10** |
| SIGNALS UPDATED | **10** |
| ORPHANS | **0** |
| BROKEN LINKS | **0** |
| GDI OPPORTUNITIES WRITTEN | **0** (promotion dry-run only) |

Fields refreshed: `lastResearchResult`, `nextResearchAt`, `researchReason`, `lastMaterialSignalAt`, program future-cycle JSON when year evidence present, signal qualification status.

---

## I. REPLICATION

| Check | Result |
|-------|--------|
| BETHESDA | **PASS** (live harvest) |
| RENAISSANCE | **PASS** (playbook type-driven; no live market crawl) |
| CAMBRIDGE | **PASS** (playbook type-driven; no live market crawl) |
| HOTEL-SPECIFIC LOGIC | **NO** |

---

## J. COST

| Item | Value |
|------|------:|
| SERP | **~$0.42** (42 charged × ~$0.01) |
| OTHER RESEARCH | ~$0 |
| SURFE | **0** |
| PDL | **0** |

---

## K. DECISION

| # | Question | Answer |
|---|----------|--------|
| 1 | Did guided research find lodging evidence generic search missed? | **YES** — lodging/housing language on 8/10 official-guided paths; V1 had 0 TRUE |
| 2 | Did it generate incremental credible at-bats? | **NOT YET** — 0 GENERATOR_INCREMENTAL_NEW; 8 TRUE were overlaps/updates |
| 3 | Was yield per query better? | **YES** for TRUE/query vs V1 generic proxy; **NO** for net-new inventory |
| 4 | Which types worked best? | Sports stay-to-play, association annual conferences with housing pages, NIST program pages |
| 5 | Which failed structurally? | Generic institutional calendars (NIH workshops) without clear hotel block; contractor/consulting still weak |
| 6 | Is weekly monitoring justified? | **YES for ACTIVE generators already in GDI** (renewal/update detection) |
| 7 | Should ACTIVE generators be on recurring cadence? | **YES** — use `nextResearchAt` weekly for ACTIVE; monthly for VERIFIED |

---

## Non-negotiables honored

- No graph redesign / no new tables  
- No broad rediscovery of organizations  
- TRUE bar not lowered (official source + lodging strength gate tightened mid-cycle)  
- No Surfe/PDL  
- No automatic mass GDI promotion  

---

## Recommended next small cycle

1. Harvest HIGH generators **without** existing GDI org overlap (or new `seriesId`/`cycleId` years not in Opportunities).  
2. Require primary source hostname ∈ officialDomain (not any `.gov`).  
3. Optionally `--promote` only GENERATOR_INCREMENTAL_NEW after human review.  
4. Then enable controlled weekly ACTIVE monitoring.
