# Active Brand CALA — Wave 6 enrichment summary

**Generated:** 2026-07-24  
**Prior:** Waves 1–5  
**Scope:** IHG amenities · Wyndham amenities attempt · BWH amenities attempt · SLH amenity residual · Design/Tribute amenity residual  
**Rule:** Official sources only · fill-blank · Affiliation = Brand Setup `Brand Name`

## Headline results

| Brand | Metric | Before → After (wave5 → wave6) |
|-------|--------|--------------------------------|
| **Hotel Indigo** | % Amenities | **53.3 → 60.0** (+Grand Cayman) |
| **Kimpton Hotels** | % Amenities | **36.4 → 54.5** (+Virgilio, Mas Olas) |
| Vignette / Dazzler / Trademark / BW | Amenities | **no safe apply** (blocked or false-positive parse) |
| SLH | Amenities residual | still **1 blank** (catalog has no fillable keyFeatures for that row) |

Coverage: `reports/active-brand-cala-enrichment-coverage-wave6-after.csv`

## Applied counts

| Action | Count |
|--------|------:|
| IHG Amenities (Kimpton Virgilio, Kimpton Mas Olas, Hotel Indigo Grand Cayman) | **3** |
| Wyndham Dazzler/Trademark amenities | **0** (rejected — breadcrumb false positives) |
| BWH hotelDetails amenities/descriptions | **0** (HTTP 403 captcha) |
| SLH amenities residual | **0** |
| Design Hotels / Tribute blank amenities | **0** (no extractable content) |

## Details

### IHG amenities
- Script: `scripts/backfill-ihg-wave6-amenities.mjs`
- Source: official IHG hoteldetail HTML (`amenity-title` / JSON-LD) via `lib/ihg-hotel-amenities-extract.js`
- Applied 3 URL-linked blanks that returned real amenity lists
- Remaining blanks with Website returned soft-blocked shells (~310KB, no amenity markers) — steward/retry later
- Artifacts: `reports/ihg-wave6-amenities-plan.json`, `…-apply-log.json`

### Wyndham (Dazzler + Trademark) — **not applied**
- Script dry-run found 17 rows with “amenities,” but labels were **nav breadcrumbs** (`${hotelName}; Home; City Hotels`)
- Documented reject: `reports/wyndham-wave6-amenities-rejected.json`
- `/services-amenities` remains soft-blocked for real amenity chips

### BWH Premier / Signature
- `hotelDetails` proxy returns **403** for all 10 Property ID candidates — captcha-blocked
- Artifacts: `reports/bwh-wave6-amenities-plan.json`

### Hilton Curio/Tapestry
- Amenities sync fill-blank: **0** ready (blanks lack ctyhocn / already populated where matched)

### SLH
- Amenities backfill: Ready 0; one residual blank Amenities row remains after catalog pass

## Steward leftovers

- Wyndham amenities: need steward HTML or improved parser once real amenity DOM is available
- BWH amenities/descriptions: steward browser save of property pages / hotelDetails when unblocked
- IHG amenity soft-blocks (Las Mercedes, La Paz Indigo, SOUMA, Casas del XVI, …)
- Choice Ascend/Comfort amenity blanks without Wayback
- Autograph description blanks without Bazaarvoice product

## Change impact

**High** — 3 Hotel Census Amenities updates (IHG).

**Rollback:** clear Amenities on the 3 IHG wave6 apply-log record IDs.

## Manual QA

- [ ] Kimpton Virgilio / Mas Olas Amenities look like real property amenities (not nav text)
- [ ] Hotel Indigo Grand Cayman Amenities populated
- [ ] Confirm no Wyndham Amenities write from Wave 6 false-positive plan
- [ ] No overwrite of previously filled Amenities
