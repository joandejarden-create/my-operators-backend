# Wyndham + Accor Census Enrichment — Run Report

**Date:** 2026-07-05

## Pipelines built

| Component | Wyndham | Accor |
|-----------|---------|-------|
| Sitemap extract | `lib/wyndham-brand-directory-extract.js` | `lib/accor-brand-directory-extract.js` |
| Amenity fetch | `lib/wyndham-hotel-content-fetch.js` | `lib/accor-hotel-content-fetch.js` |
| Census match | `lib/hotel-census/plan-wyndham-census-sitemap-match.js` | `lib/hotel-census/plan-accor-census-sitemap-match.js` |
| Orchestrator | `scripts/run-wyndham-census-enrichment.mjs` | `scripts/run-accor-census-enrichment.mjs` |

## Accor — completed steps

1. **Full sitemap metadata scan:** 5,813 hotel pages → **347 CALA** properties in directory  
   - `reports/accor-property-directory-extract.json`
2. **Census match:** 567 census rows → **67** Website + Property ID matches  
3. **Applied to Airtable:** **67** fill-blank `Website` + `Property ID`  
4. **Amenities applied (2026-07-05):** **67/67** matched rows via `refetch-accor-census-amenities.mjs --delay-ms=1500`  
5. **Match quality:** 59 low / 8 medium confidence — steward spot-check advised

## Wyndham — completed steps (2026-07-05)

1. **Sitemap + metadata extract:** 46,462 overview URLs scanned → **621 CALA** properties  
   - `reports/wyndham-property-directory-extract.json`
2. **Census match:** 332 rows → **76** Website + Property ID matches (71 medium / 5 low)
3. **Applied:** **76** Website + Property ID
4. **Amenities:** **21/76** via `/services-amenities` subpage parser (55 pages had no parseable list)

## Match expansion pass (2026-07-05 afternoon)

Greedy 1:1 planner: `scripts/run-brand-census-match-expansion.mjs`  
Reports: `reports/*-census-match-expansion-plan.json`, `reports/*-steward-review.csv`

| Brand | Directory | Assigned | Auto-applied (medium+) | Steward (low) |
|-------|-----------|----------|----------------------|---------------|
| Wyndham | 621 | 93 | **+9** Website/Property ID | 84 |
| Accor | 347 | 333 | **+47** Website/Property ID | 286 |

**Accor amenities after expansion:** `refetch-accor-census-amenities.mjs --apply --delay-ms=1500` → **47/47** new matches filled.

### Post-expansion census fill

| Parent | Filled | Total | Fill% | Open amenity blanks |
|--------|--------|-------|-------|---------------------|
| Accor | **114** | 567 | 20% | 357 |
| Wyndham | 21 | 332 | 6% | 219 |
| **All census** | **953** | 15,644 | 6% | — |

### Blockers observed this pass

- **Wyndham amenities:** `/services-amenities` now returns **HTTP 200 with empty body** (soft block after sustained crawl). Re-try after cooldown; no invented data.
- **Accor cache apply:** URL-keyed cache missed because expansion writes `index.en.shtml` URLs while extract cache keys differ; live refetch at 1500ms worked.
- **286 Accor + 84 Wyndham low-confidence** matches in steward CSV — review before manual apply.

**Wyndham amenity parser fix:** overview pages are JS-shell; amenities fetched from `…/services-amenities` URL.

## Questions for steward

1. Accept **59/67 Accor low-confidence** Website matches or require medium+ only?
2. Schedule Accor amenity refetch with **1.5s+ delay** from your machine?
3. Wyndham: retry sitemap tomorrow or provide manual property URL list for CALA census?
