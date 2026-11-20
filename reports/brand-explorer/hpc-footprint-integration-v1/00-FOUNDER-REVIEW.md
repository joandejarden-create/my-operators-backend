# Brand Explorer — HPC Airtable Footprint Integration (Founder Review)

**Date:** 2026-11-19  
**Workspace:** `C:\Dev\dealality\ws-brand-explorer`  
**Curio / Ascend:** **NOT APPROVED**  
**Airtable writes:** 0 · Paid research: none · Publication: none  

> **Follow-on (breakdowns + enrichment plan):** see [`00-FOUNDER-EVIDENCE-BREAKDOWNS.md`](./00-FOUNDER-EVIDENCE-BREAKDOWNS.md) and [`hpc-correction-plan-READONLY.md`](./hpc-correction-plan-READONLY.md).

---

## 1. Is Footprint Metrics connected to HPC Airtable?

**PARTIAL → YES (after this fix)**

| Before | After |
| --- | --- |
| **NO** for Comparison Studio / product metrics table | **YES** — live HPC queried on `GET /api/brand-library/brand` |
| UI used Brand Setup MVP zeros or transitional Hotel Census | UI prefers `brand.hpcFootprint` (designated authority) |

**Evidence (before):** Ascend Compare Studio tables showed `Total (Portfolio) | 0 | 0 | 0 | 0…` while live API already had Hotel Census 11/2277 — because `brand-explorer-compare.html` did **not** load `brand-explorer-census-metrics.js`, so atelier fell back to raw MVP footprint with `showVerifiedMetrics: true`.

**Evidence (after):** Curio UI `Total (HPC-Observed) | 20 | 653 | 0 | 0 | 20 | 653`. Ascend UI `Total (HPC-Observed) | 13 | (blank rooms) | 0 | 0 | 13 | (blank)`.

Screenshots:

- `screenshots/01-curio-hpc-footprint-metrics-RIGHT.png`
- `screenshots/02b-ascend-hpc-footprint-metrics-blank-rooms.png`

---

## 2. Exact source and mapping

| Layer | Value |
| --- | --- |
| Base | `AIRTABLE_BASE_ID_ALT` (platform) |
| Table | `Hotel Property Census` |
| Brand match | `{Current Brand}` exact matchers (pilot + Brand Name) |
| Qualify | `{Radar Status}` ∈ `Open`, `Pipeline` |
| Rooms | `{Rooms / Keys}` |
| Country | `{Country}` |
| Asset archetype | `{Property Type}` |
| Region | derived via `mapCountryToFootprintRegion` |
| API | `brand.hpcFootprint` on `/api/brand-library/brand` |
| Client trust | `BrandExplorerCensusMetrics.footprintDisplayModel` prefers HPC |
| Module | `lib/brand-explorer/be-hpc-footprint-summary-v1.js` |
| SoT contract | `BE_FOOTPRINT_METRIC_CONTRACT` / `BE_HPC_FOOTPRINT_SUMMARY_V1` |

Refresh: **live Airtable query per brand request** (not a frozen research JSON). Flag `BRAND_EXPLORER_HPC_METRICS=0` disables. Does not overwrite Presentation / Brand Basics content.

---

## 3. Why the screenshot showed zeros

1. **Primary (Compare Studio):** missing `brand-explorer-census-metrics.js` → raw Ascend Brand Setup footprint (all regional cells = 0) rendered as “verified” metrics.  
2. **Secondary:** HPC was never attached to the brand-library API response for the product metrics path (helper existed only for factory/phase tooling).  
3. **Not** an empty HPC query for Curio/Ascend — live HPC returns matches when configured.

---

## 4. Corrected Curio figures (live HPC, unverified)

| Metric | Value |
| --- | --- |
| Brand Basics | `receQkxgjlezsc1xg` |
| Raw HPC rows (matcher) | 29 |
| Qualifying Open/Pipeline | **20** |
| Excluded non-Open/Pipeline | 9 |
| Open hotels | **20** |
| Open keys (sum known) | **653** (15 properties missing Rooms/Keys) |
| Pipeline hotels / keys | **0 / 0** (no Radar Status=Pipeline in matched set) |
| certState | `LIVE_UNVERIFIED_INVENTORY` |
| Official Brand Setup (separate) | Open **235** / Rooms **34,000** · Pipeline **26** / **3,650** |

Regional UI: CALA 17 + AM 2 = **19** (1 qualifying property not region-mapped — coverage limitation).  
Country + Property Type distributions populated from HPC rows.  
Reconciliation JSON: `curio-ascend-hpc-reconciliation.json`

---

## 5. Corrected Ascend figures (live HPC, unverified)

| Metric | Value |
| --- | --- |
| Brand Basics | `reclkgOzvAcBheUSo` |
| Qualifying Open | **13** |
| Open keys | **incomplete** (13/13 missing Rooms/Keys) → UI shows **blank**, not verified 0 |
| Pipeline | **0** |
| Region | CALA 13 |
| Countries | Mexico 11 · Dominican Republic 2 |
| Property types | All-Inclusive 9 · Unknown 3 · Boutique Hotel 1 |
| Official Brand Setup | none (MVP totals are 0 — no official block shown) |
| certState | `LIVE_UNVERIFIED_INVENTORY` |

Earlier factory note “13 open / keys=0” matches live HPC hotel count; keys remain incomplete inventory, not certified zero.

---

## 6. Do regional / country aggregations reconcile?

| Brand | Hotel total vs region sum | Country sum |
| --- | --- | --- |
| Curio | 20 vs region 19 (**1 gap**) | countries sum to 20 |
| Ascend | 13 = CALA 13 | Mexico 11 + DR 2 = 13 |

**PARTIAL** for Curio regions — one property lacks a mappable country→region code. Not fabricating allocations for the gap.

---

## 7. Does data refresh work?

**YES (read path):** each brand-library request re-queries HPC when `BRAND_EXPLORER_HPC_METRICS` is on.  
**Persistence:** none written to Airtable this cycle. Official Brand Setup figures remain separate. Future HPC updates refresh BE metrics without publishing Presentation content.

---

## 8. Browser screenshots

See `screenshots/` — Curio HPC-Observed 20/653 + Official 235/34k; Ascend 13 with blank rooms + incomplete-keys banner.

---

## 9. Remaining limitations / blockers

1. Inventory remains **`LIVE_UNVERIFIED_INVENTORY`** — not independently certified worldwide coverage.  
2. Ascend (and many Curio rows) **missing Rooms / Keys** in HPC.  
3. Curio region mapping gap (20 vs 19).  
4. Pipeline=0 reflects **Radar Status**, not Affiliation “Future / Pipeline” text.  
5. Country/archetype sub-rows may still show `0` keys where per-row rooms are missing (portfolio total blanks when all missing).  
6. Curio/Ascend remain **NOT APPROVED**; no production writes.  
7. Combined Brand Explorer HTML already had census-metrics; Compare Studio / Gate1 parity pages were the broken mounts — now fixed.

---

## Tests

```bash
npm run test:be-hpc-footprint-summary-v1
npm run test:be-hpc-footprint-ui-integration-v1
npm run be-hpc-footprint-reconcile:dry-run
```

Fail closed: Airtable unavailable → `displayMetrics` nulls (never silent zeros). Incomplete keys → null open rooms on HPC-Observed total.

---

## Verdict

Footprint Metrics now reflect **qualifying live HPC Open/Pipeline inventory**, clearly separated from **official Brand Setup** totals, with coverage banners. Misleading all-zero Ascend Compare Studio tables are fixed.

`BLOCKED — FOUNDER_GATES_AND_ACTIVATION_REQUIRED` (approvals unchanged)
