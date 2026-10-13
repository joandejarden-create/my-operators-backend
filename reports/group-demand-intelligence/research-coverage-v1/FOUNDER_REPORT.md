# GDI Research Coverage & Target Registry V1 — Founder Report

**Generated:** 2026-09-24  
**Base:** `appa2cE7FTRmIbB32`  
**Version:** `gdi_research_coverage_v1`

---

## A. AIRTABLE ARCHITECTURE

| Entity | Action | Table ID |
|--------|--------|----------|
| **RESEARCH TARGETS** | **CREATED** | `tblVyuEf5vjooWDKX` |
| **RESEARCH RUNS** | **CREATED** | `tblzNMUIo2T6onaKH` |
| **TARGET RUNS** | **CREATED** | `tblSA0cFVplNfWMjp` |
| **GDI OPPORTUNITIES** | **REUSED** (+ provenance columns extended) | `tblRuReslJMwsfRQj` |

Linked to existing: Demand Generators, Demand Programs, Private Event Venues.  
Did **not** create a second opportunity table. Did **not** replace weekly NEW/UPDATED/REACTIVATED logic.

---

## B. TARGET REGISTRY (Bethesda backfill)

| Metric | Count |
|--------|------:|
| **TOTAL ACTIVE** | **50** |
| **HIGH** | **20** |
| **MEDIUM** | **21** |
| **LOW** | **9** |

**By type**

| Type | Count |
|------|------:|
| Demand Generator | 20 |
| Program | 19 |
| Private Event Venue | 11 |
| Event Series / Official Source / Other | 0 (not in verified seed set) |

Source: existing verified DG (20) + Programs (19) + PE venue fits (11). No fabricated history.

---

## C. FIRST COVERAGE RUN

| Field | Value |
|-------|-------|
| **RUN ID** | `gdir_luxvwwxid7u2_2026_09_24_weekly_c1c0b6` |
| **TARGETS DUE** | 50 |
| **TARGETS RESEARCHED** | 50 |
| **TARGETS MISSED** | 0 |
| **TARGETS FAILED** | 0 |
| **NO MATERIAL CHANGE** | 50 |

First cycle is a **baseline** coverage pass (existing signals are verified, not counted as “new this week”).

---

## D. SIGNALS

| | Count |
|--|------:|
| **NEW SIGNALS** | **0** |
| **UPDATED SIGNALS** | **0** |

---

## E. OPPORTUNITIES

| State | Count |
|-------|------:|
| **NEW** | **0** |
| **UPDATED** | **0** |
| **REACTIVATED** | **0** |
| **UNCHANGED** | n/a (no opp mutations this cycle) |

Coverage cycle did **not** create or rewrite opportunities (by design).

---

## F. PROVENANCE

| Check | Result |
|-------|--------|
| NEW OPPORTUNITIES WITH RUN ID | **0/0** |
| NEW OPPORTUNITIES WITH TARGET ID | **0/0** |
| NEW OPPORTUNITIES WITH TARGET RUN ID | **0/0** |
| FALSE NEW CLASSIFICATIONS | **0** |

Helpers + opportunity column extensions are in place; next material discovery must call `attachDiscoveryProvenance`.

---

## G. COVERAGE

**COVERAGE %:** 50 / 50 = **100%**

---

## H. AIRTABLE INTEGRITY

| Check | Count |
|-------|------:|
| ORPHAN TARGETS | 0 |
| ORPHAN TARGET RUNS | 0 |
| BROKEN RUN LINKS | 0 |
| BROKEN OPPORTUNITY PROVENANCE LINKS | 0 |

---

## I. CUSTOMER SUMMARY

```
Last Research: Sep 24, 2026

50 monitored demand targets reviewed

0 new demand signals identified

0 new opportunities added

0 existing opportunities materially updated
```

Grounded only in completed RESEARCHED target runs.

---

## J. LEARNING

| Metric | Count |
|--------|------:|
| TARGETS PRIORITY INCREASED | 0 |
| TARGETS PRIORITY DECREASED | 0 |
| TARGETS CADENCE RELAXED | 0 |
| NEW TARGETS ADDED | 50 (backfill) |

Cadence relaxation requires ≥3 consecutive no-change runs (threshold not yet met after baseline).

---

## K. REPLICATION

| Hotel | Result |
|-------|--------|
| **BETHESDA** | **PASS** (50 hotel-fit-scoped targets) |
| **RENAISSANCE** | **PASS** (0 targets — no DG/PE fits for hotel; no Bethesda bleed) |
| **CAMBRIDGE** | **PASS** (0 targets — same) |
| **HOTEL-SPECIFIC LOGIC** | **YES** (target IDs include hotel; require hotel Fit rows) |

---

## L. REGRESSION

| Suite | Result |
|-------|--------|
| GDI weekly delta | **PASS** |
| NEW opps / weekly | **PASS** |
| Demand Generators | **PASS** |
| CSV | **PASS** |
| Research coverage V1 fixtures | **PASS** (15/15) |
| Private Events / Share | Not re-run live; no PE/share code paths mutated |

---

## M. DECISION

1. **Can we prove exactly what was researched each week?** YES — Target Runs × Run ledger in Airtable.  
2. **Distinguish coverage from opportunity count?** YES — due/researched/missed/failed ≠ opp NEW count.  
3. **Trace new opps to run + target?** YES — helpers + Airtable provenance columns; 0/0 this baseline cycle.  
4. **Target list persistent across runs?** YES — GDI Research Targets registry.  
5. **Universe evolves without broad weekly rediscovery?** YES — weekly = registry; expansion = separate run type.  
6. **Deprioritize low-yield safely?** YES — consecutiveNoChange → cadence relax / priority down (counts first).  
7. **Customer summary grounded?** YES — only from RESEARCHED target runs.

---

## N. FINAL VERDICT

**GDI RESEARCH COVERAGE V1 PASSES — WEEKLY RESEARCH AUDITABLE**

---

### Commands

```bash
npm run ensure:gdi-research-coverage-schema -- --apply
npm run gdi:research-coverage-backfill -- --apply --hotel recLuxvwwxID7U2B8
npm run gdi:research-coverage-run -- --apply --force-due --hotel recLuxvwwxID7U2B8
npm run test:gdi-research-coverage-v1
```

### Notes / non-goals honored

- No contact enrichment  
- No opportunity lifecycle rewrite  
- No fake historical runs  
- No raw SERP/HTML in Airtable (URLs + short summaries only)  
- Weekly opportunity delta unchanged  
