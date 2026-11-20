# Founder Evidence — HPC Footprint Breakdowns & Enrichment Plan

**Date:** 2026-11-19  
**Workspace:** `C:\Dev\dealality\ws-brand-explorer` (`ws/brand-explorer`)  
**URL:** `http://127.0.0.1:8082/brand-explorer-compare.html?beInternalPreview=1`  
**Airtable writes:** **0**  
**Paid research:** **0**  
**Publication:** none  
**Curio / Ascend:** **NOT APPROVED**

---

## 1. Current HPC data-source confirmation

| Item | Value |
|------|--------|
| Source | Platform Airtable → **Hotel Property Census** |
| Matching | `Current Brand` matchers from `BE_PHASE2_PILOT_BRANDS` |
| Qualifying statuses | Radar Status **Open** / **Pipeline** only |
| Integration | `lib/brand-explorer/be-hpc-footprint-summary-v1.js` → `brand.hpcFootprint` on `GET /api/brand-library/brand` |
| UI contract | `brand-explorer-census-metrics.js` → `BrandExplorerAtelierFromApi` (shared LEFT/RIGHT renderer) |
| Live configured | `true` (reconcile + API verified this session) |

One canonical aggregation feeds Region, Country, and Asset Archetype. No parallel datasets.

---

## 2–3. Curio & Ascend — three functioning breakdowns (both Compare columns)

Browser: Comparison Studio Footprint tab, Highlight OFF. DOM scrape confirmed LEFT === RIGHT numeric tables.

### Curio (`receQkxgjlezsc1xg`)

| View | Result |
|------|--------|
| Region | CALA 18 / AM 2 → **Total 20** · Known rooms 298 + 355 = **653** |
| Country | MX 5, CO 3, BR 3, US 2, AR 2, DO 2, LC 1, CR 1, PR 1 → **Total 20** |
| Asset Archetype | Hotel 7, Boutique 5, Resort 3, All-Inclusive 2, **Unknown 3** → **Total 20** |

Customer copy (both columns):

> Dealality has identified 20 affiliated properties in its hotel database. Room counts are available for 5 of these properties. This represents observed coverage rather than the brand's complete worldwide portfolio.

Screenshots:

- `screenshots/01-curio-compare-region.png`
- `screenshots/02-curio-compare-country.png`
- `screenshots/03-curio-compare-archetype.png`

### Ascend (`reclkgOzvAcBheUSo`)

| View | Result |
|------|--------|
| Region | CALA **13** → Total 13 |
| Country | Mexico 11, Dominican Republic 2 → **Total 13** |
| Asset Archetype | All-Inclusive 9, Boutique 1, **Unknown 3** → **Total 13** |

Customer copy (both columns):

> Dealality has identified 13 affiliated properties in its hotel database. Room counts are not yet available for these properties. This represents observed coverage rather than the brand's complete worldwide portfolio.

Known room cells render as **blank** (`&nbsp;`), not verified zero (DOM-verified). Pipeline hotel count `0` is a true zero.

Screenshots:

- `screenshots/04-ascend-compare-region.png`
- `screenshots/05-ascend-compare-country.png`
- `screenshots/06-ascend-compare-archetype.png`

---

## 4. Current-versus-proposed

Both columns use the canonical product renderer. RIGHT retains `brand.hpcFootprint` SoT (not candidate JSON prose). Region / Country / Asset Archetype tables match LEFT for both pilots.

---

## 5–6. Reconciliation + known / missing rooms

| Brand | Open hotels | Known open rooms | Props with keys | Props missing keys | Pipeline | Region/Country/Type sums |
|-------|------------:|-----------------:|----------------:|-------------------:|---------:|--------------------------|
| Curio | 20 | **653** (not complete inventory) | 5 | **15** | 0 | **20 / 20 / 20** |
| Ascend | 13 | *(blank — all missing)* | 0 | **13** | 0 | **13 / 13 / 13** |

Avg Keys uses `openWithKeys` only (e.g. Curio Hotel 355÷2 = **178**, not 355÷7). Segments with no known rooms show blank Known Rooms + blank Avg Keys.

Saint Lucia maps to **CALA** (region mapper fix). Curio Unclassified region count after fix: **0**.

Official Brand Setup (Curio 235 / 34,000 as of **2026-02-09**) remains in a separate Official Reported section — not mixed into HPC observed totals.

---

## 7. Customer-facing language

Removed from customer Footprint path: `LIVE_UNVERIFIED_INVENTORY`, `certState`, Radar Status jargon, steward/fixture/candidate wording. Internal cert metadata retained on API/diagnostics only. Browser `hasJargon: false` on both columns.

---

## 8. Quality gates

| Gate | Result |
|------|--------|
| `npm run test:be-hpc-footprint-breakdowns-v1` | PASS |
| `npm run test:be-hpc-footprint-summary-v1` | PASS |
| `npm run test:be-hpc-footprint-ui-integration-v1` | PASS |
| Browser Curio + Ascend × 3 toggles × 2 columns | PASS |

Lessons registry updated: `BE-QL-HPC-FOOTPRINT-001` in `configs/brand-explorer-founder-quality-lessons-registry-v1.json`.

---

## 9. HPC enrichment — read-only correction plan (no writes)

| Metric | Count |
|--------|------:|
| Qualifying HPC records (both brands) | 33 |
| Incomplete records | 29 |
| Field-level deficiency rows | 34 |
| Verified write candidates | **0** (evidence not yet attached) |

Artifacts:

- `hpc-deficiency-register-curio-ascend.json` — record IDs + field deficiencies  
- `hpc-correction-plan-READONLY.md` — founder review plan  
- Command: `npm run be-hpc-deficiency-register-v1`

**Curio targets:** 15 missing Rooms/Keys · 3 Unknown/blank Property Type · 0 unclassified region (after mapper)  
**Ascend targets:** 13 missing Rooms/Keys · 3 Unknown/blank Property Type · geography OK (CALA)

**STOP — no production HPC Airtable writes until founder approves exact correction rows.**

---

## Confirmation

- No production Airtable data changed.  
- No paid research.  
- No publication.  
- Curio and Ascend remain **NOT APPROVED**.
