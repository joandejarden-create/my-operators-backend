# Operator Alignment — After Deal Backfill Score Comparison

**Date:** 2026-05-25  
**Sample deal:** `recIeGRZP21udmTnt` (Aeropuerto Cancún Select-Service Hotel)  
**Backfill report:** `reports/deal-operator-alignment-backfill-2026-05-25T174024.json`  
**Before audit:** `reports/operator-alignment-scoring-before-backfill-recIeGRZP21udmTnt.json`  
**After audit:** `reports/operator-alignment-scoring-after-backfill-recIeGRZP21udmTnt.json`

---

## Executive summary

Deal-side Phase 5B fields were populated on **12 CALA sample deals** (including Cancún). **Numeric operator alignment scores did not change** for `recIeGRZP21udmTnt` because `scoreOperatorMatchForDeal` still reads legacy fields (`Preferred Deal Structure` on Market Performance, free-text must-haves, operator service token overlap) and **does not yet consume** the new SI fields (`Preferred Management Structure`, `Operating Model`, `Required Operator Services`, etc.).

This is expected for Phase 5B scope. **Phase 5E** should wire the new fields into structure/service factors and missing-data handling.

**Operator card narrative repetition** is also unchanged — it is driven by UI default bullets and factor templates, not deal intake fields alone.

---

## Score distribution — before vs after (recIeGRZP21udmTnt)

| Metric | Before backfill | After backfill | Delta |
|--------|-----------------|----------------|-------|
| Average score | **64.1** | **64.1** | 0 |
| Min / Max | 54 / 69.4 | 54 / 69.4 | 0 |
| Strong (80+) | 0 | 0 | 0 |
| Moderate (65–79) | 5 | 5 | 0 |
| Conditional (50–64) | 5 | 5 | 0 |

### Top 10 operators (unchanged order and scores)

| Company | Score | Band |
|---------|------:|------|
| Viento Sur Gestión Hotelera | 69.4 | Moderate |
| Mangle Azul Hospitalidad | 66.4 | Moderate |
| Panamerican Lodging Partners S.A. | 66.4 | Moderate |
| Barrio Hotelero CDMX | 66.4 | Moderate |
| Metro Lodging São Paulo | 66.4 | Moderate |
| Cenote Azul Operadores | 64.7 | Conditional |
| Antillano Norte Hospitality Group | 64.7 | Conditional |
| Río Plata Hotel Partners | 63.3 | Conditional |
| Oro Verde Lodge & Hotel Operators | 59.7 | Conditional |
| Hotel Equities (CALA) | 54.0 | Conditional |

---

## Factors — before vs after (per-operator pattern identical)

| Factor | Before | After | Notes |
|--------|--------|-------|-------|
| geographyMarkets | 100 (all) | 100 | Unchanged — Mexico in market strings |
| chainScale | 100 (most) | 100 | Unchanged |
| dealStructureAssignment | **20** (all) | **20** | Still uses **MP `Preferred Deal Structure` = Franchise Only** |
| serviceOfferings | **30** (most) | **30** | Still uses legacy **Must-Haves From Brand/Operator** token overlap |
| assetProjectStageFit | ~59.5 | ~59.5 | Unchanged |
| feeCommercial | 75 | 75 | Unchanged |
| systemsReporting | 70–90 | 70–90 | Unchanged |
| ownerRelations | 70 | 70 | Unchanged |
| brandPortfolioRelevance | 25–70 | 25–70 | Unchanged |

**Primary suppressors remain:**

1. **Deal structure factor** — engine compares MP `Franchise Only` to operator management structures → score 20. New SI fields (`Brand Agreement Structure` = Franchise, `Operating Model` = Third-party managed, `Preferred Management Structure` = franchise + full management) are **not read** by scoring yet.
2. **Service offerings** — engine uses `Must-Haves From Brand/Operator` strings, not `Must-Have Operator Services` multis → overlap still ~30.

---

## Deal fields populated (Cancún) — realistic demo context

| Field | Value | Resolves confusion? |
|-------|-------|---------------------|
| Brand Agreement Structure | Franchise | Yes — brand dimension |
| Operating Model | Third-party managed | Yes — operations dimension |
| Preferred Management Structure | Franchise with third-party operator; Full third-party management | Yes — explicit operator path |
| Operator Review Status | Ready for operator shortlist | Workflow/demo |
| Required / Must-Have Operator Services | Structured multis | Yes — future service overlap |
| MP Preferred Deal Structure | **Franchise Only** (unchanged) | Legacy field preserved |

**Franchise and Third-party managed are not treated as conflicting** — they apply to different dimensions.

---

## Narrative / card repetition

| Layer | Changed after deal backfill? |
|-------|------------------------------|
| `scoreOperatorMatchForDeal` output | No |
| `alignmentSignalsFromBreakdown` templates | No |
| OAS UI `buildCompanyWhatSupports` default bullets | No |

Deal backfill alone does not differentiate operator cards. **Phase 5F** (narrative) + **operator-side** Phase 5B/C backfill still required.

---

## Realistic enough for demo?

| Area | Ready? | Gap |
|------|--------|-----|
| Deal intake / OAS deal context panels | **Yes** — SI fields now populated on samples | UI must read new fields in API payloads |
| OAS numeric scores | **Partial** — scores reflect legacy MP/SI until 5E | Wire new fields in scoring |
| Operator cards | **No** — needs operator structured fields + narrative pass | Operator Setup backfill |
| Structure conflict explanation | **Yes in data** — not yet in score/rationale | 5E mapping |

---

## What still requires operator-side enrichment

- Active Countries / Active Markets multis on operators (still footprint/text for scoring geography substring)
- Management Structures Supported aligned to deal Preferred Management Structure
- Offered Services aligned to deal Required/Must-Have Operator Services
- Data Confidence / Source Type on Master (admin) optional for publish workflow

---

## Phase 5E flags (scoring engine)

1. Read `Preferred Management Structure` + `Operating Model` for **dealStructureAssignment** (not MP franchise-only alone).
2. Read `Required Operator Services` / `Must-Have Operator Services` for **serviceOfferings** (deprecate free-text-only overlap).
3. Do not score missing operator geography as 35 when deal has structured market requirement — use data gap / Needs Validation.
4. Optional: surface new deal fields in `fetchDealScoringContext` merged payload for OAS deal-context section.

---

## Commands used

```bash
node scripts/backfill-deal-operator-alignment-fields.mjs --sample-deals --apply
node scripts/audit-operator-alignment-scoring.mjs recIeGRZP21udmTnt
```
