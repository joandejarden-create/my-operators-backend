# Active Brand CALA — Wave 1 enrichment summary

**Generated:** 2026-07-24  
**Scope:** Vignette Collection · BW Premier Collection · BW Signature Collection · Dazzler by Wyndham · Trademark Collection by Wyndham  
**Rule:** Official brand sources only · fill-blank · Affiliation = exact Brand Setup `Brand Name` · dry-run → apply

## Coverage delta (Wave 1 brands)

| Brand | CALA before → after | % Website | % Property ID | % Amenities | % Hotel Description |
|-------|--------------------:|----------:|--------------:|------------:|--------------------:|
| Vignette Collection | 3 → **5** | 0 → **40** | 0 → **40** | 0 → 0 | 0 → 0 |
| BW Premier Collection | 6 → 6 | ~0 → **66.7** | ~0 → **66.7** | 0 → 0 | 0 → 0 |
| BW Signature Collection | 6 → 6 | ~0 → **100** | ~0 → **100** | 0 → 0 | 0 → 0 |
| Dazzler by Wyndham | 13 → 13 | ~62 → **61.5** | ~62 → **61.5** | 0 → 0 | 0 → **61.5** |
| Trademark Collection by Wyndham | 22 → 22 | ~41 → **40.9** | ~41 → **40.9** | 0 → 0 | 0 → **40.9** |

Baseline: `reports/active-brand-cala-enrichment-coverage-baseline.csv`  
After: `reports/active-brand-cala-enrichment-coverage-wave1-after.csv`

## Applied counts

| Brand | Website filled | Property ID filled | Amenities | Hotel Description | Creates |
|-------|---------------:|-------------------:|----------:|------------------:|--------:|
| Vignette Collection | 2 (on create) | 2 (on create) | 0 (IHG page blocked/empty) | 0 (skipped this wave) | **2** |
| BW Premier Collection | 3 | 4 | 0 (hotelDetails captcha) | 0 | 0 |
| BW Signature Collection | 6 | 6 | 0 (hotelDetails captcha) | 0 | 0 |
| Dazzler by Wyndham | 0 (already URL-linked where catalog-matched) | 0 (fill-blank; slugs already present) | 0 (soft-blocked) | **9** | 0 |
| Trademark Collection by Wyndham | 0 | 0 | 0 (soft-blocked) | **9** | 0 |

**Totals applied this wave:** Website **11** · Property ID **12** · Hotel Description **18** · Creates **2**

### Vignette Collection (IHG)

- Directory refresh already included `vignettecollection` (LIMMP Lima, SDQCD Santo Domingo).
- Census had only **3 Mexico Pipeline** rows — not on public open directory → stewarded.
- **Created** open directory hotels:
  - `Vignette Collection SOUMA Hotel` (Peru) — Website + Property ID `LIMMP`
  - `Vignette Collection Casas del XVI` (Dominican Republic) — Website + Property ID `SDQCD`
- Amenities: IHG hoteldetail blocked/empty → no Amenities write.
- Artifacts: `reports/vignette-cala-census-create-plan.json`, `reports/vignette-cala-census-create-apply-log.json`, `reports/vignette-cala-pipeline-steward.csv`

### BW Premier + BW Signature (new BWH pipeline)

- New modules:
  - `lib/bwh-brand-directory-extract.js`
  - `lib/hotel-census/plan-bwh-census-directory-match.js`
  - `scripts/run-bwh-census-enrichment.mjs`
  - Seed: `fixtures/bwh-cala-directory-seed.json` (official `propertyCode` URLs; live bestwestern.com is captcha-blocked from this environment)
- Applied **10** fill-blank Website/Property ID updates (Premier 4 · Signature 6).
- Terra Nova already had a non-BW Website → Property ID only.
- Steward leftovers (no safe open propertyCode page):
  - Best Western Premier St. Mary's Court (Antigua, Pipeline)
  - La Estancia Hotel BW Premier Collection (DR, Pipeline — press only)
- Amenities/description: hotelDetails proxy returns captcha interstitial → soft-blocked.
- Artifacts: `reports/bwh-cala-directory-extract.json`, `reports/bwh-census-enrichment-plan.json`, `reports/bwh-census-enrichment-apply-log.json`, `reports/bwh-census-steward-review.csv`

### Dazzler + Trademark (Wyndham)

- Matcher tightened with `brandSlug` gates (`dazzler` / `trademark`) + name normalization (`lib/hotel-census/plan-wyndham-census-sitemap-match.js`).
- Website/ID rematch for blank rows: **0** safe catalog matches (blank open hotels not on public sitemap; e.g. Rosario, Colonia, Viva Dominicus Beach, Port de Plaisance).
- Amenities: `/services-amenities` returns empty body; overview is SPA shell without amenity labels → **soft-blocked** (18 fetch_empty).
- Hotel Description: fixed JSON-LD parser (script tags with `id=…`) and applied **18** official overview descriptions.
- Steward: rows without Website remain unmatched; Bel Air Unique appears under `wyndham` brandSlug (not Trademark) on official site — do not force Trademark URL.
- Artifacts: `reports/wyndham-wave1-census-enrichment-plan.json`, `reports/wyndham-census-amenities-fetch-plan.json`, `reports/wyndham-wave1-jsonld-backfill-plan.json`, `reports/wyndham-wave1-jsonld-backfill-apply-log.json`, `reports/wyndham-wave1-steward-review.csv`

## Steward leftovers (do not invent)

| Item | Action |
|------|--------|
| Vignette Mexico Pipeline ×3 | Wait for IHG open listing |
| BW Premier St. Mary's Court | Pipeline; no propertyCode page |
| La Estancia BW Premier | Pipeline; press URL only |
| ~18 Wyndham Wave1 blank Website rows | Not on public dazzler/trademark CALA sitemap |
| Wyndham Amenities (all Wave1 URL-linked) | Soft-blocked until amenities HTML/API exposes labels |
| BW Amenities | Soft-blocked (captcha on hotelDetails) |
| Unmatched Trademark directory (Hotel MX CDMX, etc.) | Catalog create candidates deferred — review before create |

## Change impact

**High** — Airtable Hotel Census writes (creates + Website / Property ID / Hotel Description).

**Rollback:** revert create records `recjsU593pZbIEQjd`, `recrmwxQMhxkCYT05`; clear applied Website/Property ID/Description fields from apply logs if needed.

## Data contract snapshot

| Module | Tables | Field map | Required | Optional | Selects | Linked |
|--------|--------|-----------|----------|----------|---------|--------|
| Vignette creates | Hotel Census | `MAP_VIGNETTE_CREATE` | name, Affiliation, Parent, country, Website, Property ID, status Open | city, Market, Region | Affiliation exact `Vignette Collection` | none |
| BWH match | Hotel Census | `MAP_BWH_CENSUS_BACKFILL` | Website and/or Property ID when blank | Amenities if hotelDetails works | Affiliation exact Premier/Signature | none |
| Wyndham JSON-LD | Hotel Census | website / Property ID / Hotel Description | Website present | Description from JSON-LD | Affiliation Dazzler / Trademark | none |

## Manual QA checklist

- [ ] Vignette: 5 CALA rows; 2 Open with ihg.com Website + 4–5 char Property ID; 3 Pipeline stewarded blank
- [ ] BW Signature: all 6 have bestwestern.com `propertyCode` Website + numeric Property ID
- [ ] BW Premier: 4/6 Open enriched; 2 Pipeline blank
- [ ] Dazzler/Trademark: URL-linked rows have Hotel Description; Amenities still blank
- [ ] No Affiliation renamed away from Brand Setup names

## Regression risks

- What could break: mistaken Premier↔Signature cross-match (gated by brandFamily); Wyndham description overwriting (fill-blank only)
- Retest: Operator Explorer / Scout property cards for these affiliations if they surface Website
- Fields touched: `Website`, `Property ID`, `Hotel Description`, `name`, `Affiliation`, `Parent Company`, `status`, `country`, `city`, `Market`, `Region`, `Sub-Continent`, `project_phase`
