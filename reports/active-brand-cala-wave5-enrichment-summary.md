# Active Brand CALA — Wave 5 enrichment summary

**Generated:** 2026-07-24  
**Prior:** Waves 1–4  
**Scope:** Accor descriptions · IHG descriptions · Ascend/Choice Wayback amenities · Marriott Autograph attempt  
**Rule:** Official sources only · fill-blank · Affiliation = Brand Setup `Brand Name`

## Headline results

| Brand | Metric | Before → After (wave4 → wave5) |
|-------|--------|--------------------------------|
| **MGallery Collection** | % Hotel Description | **0 → 62.5** (5 Accor JSON-LD/meta) |
| **Handwritten Collection** | % Hotel Description | **0 → 100** (1 Accor) |
| **Kimpton Hotels** | % Hotel Description | **0 → 63.6** (IHG pages) |
| **Hotel Indigo** | % Hotel Description | **0 → 66.7** (IHG pages) |
| **Vignette Collection** | % Hotel Description | **0 → 40** (2 open IHG pages) |
| **Ascend Hotel Collection** | % Amenities / Description | **30.8→46.2 / 15.4→30.8** (Wayback HTML) |
| **Radisson by Choice** | Amenities + Description | +CL010 Curico from Wayback |
| Autograph / Tribute blanks | Hotel Description | **0 apply** (marriott.com blocked; Bazaarvoice has no product for remaining MARSHAs) |

Coverage: `reports/active-brand-cala-enrichment-coverage-wave5-after.csv`

## Applied counts

| Action | Count |
|--------|------:|
| Accor Hotel Description (MGallery 5 + Handwritten 1) | **6** |
| IHG Hotel Description (Kimpton / Indigo / Vignette) | **19** |
| Ascend Amenities + Description (EC001, DO013 Wayback) | **2** |
| Radisson Curico Amenities + Description (CL010 Wayback) | **1** |
| Marriott Autograph/Tribute/Design BV descriptions | **0** |

## Details

### Accor (MGallery + Handwritten)
- Script: `scripts/backfill-accor-wave5-descriptions.mjs`
- Source: official Accor property page JSON-LD / meta description
- Remaining MGallery blanks: 3 without Accor Website (Pipeline steward)
- Artifacts: `reports/accor-wave5-descriptions-plan.json`, `…-apply-log.json`

### IHG (Kimpton / Hotel Indigo / Vignette)
- Script: `scripts/backfill-ihg-wave5-descriptions.mjs`
- Source: ihg.com property pages (JSON-LD / meta); HTML entities decoded
- Applied all URL-linked blank descriptions (includes duplicate census rows where both blank)
- Remaining blanks: Pipeline / no Website
- Artifacts: `reports/ihg-wave5-descriptions-plan.json`, `…-apply-log.json`

### Choice amenities (Wayback)
- Ascend harvest: **2/9** CDX hits with amenity markers (EC001, DO013); 7 no archive
- Broader missing harvest (20): **1** usable save (CL010 Curico); most CDX empty or markers=false
- Apply path: `scripts/backfill-choice-wave4-from-html.mjs`
- Live choicehotels.com still Akamai-blocked → steward browser save remains primary for remaining amenity blanks

### Marriott Autograph / Tribute
- Overview + puppeteer: access denied for all blank candidates
- Bazaarvoice: remaining blank MARSHAs (SJUAO, CURAK, TQOXT, MEXUL, BGITY) have **no** Description product payload
- Steward: save marriott.com overview HTML or wait for BV product publish
- Artifacts: `reports/marriott-wave5-descriptions-plan.json`, `reports/marriott-wave5-bazaarvoice-descriptions-plan.json`

## Steward leftovers

- Autograph/Tribute blank descriptions without BV product
- Ascend amenity blanks without Wayback (El Cid MX, PR029, TT006/008, MX228)
- Choice Comfort/Quality amenity blanks without usable archive HTML
- Trademark / Dazzler / BW amenities still 0% (catalog soft-blocked / seed-only)

## Change impact

**High** — ~28 Hotel Census updates (6 Accor + 19 IHG + 3 Choice).

**Rollback:** clear Hotel Description / Amenities on apply-log record IDs for Accor wave5, IHG wave5, Choice wave4 Ascend/CL010 applies.

## Manual QA

- [ ] MGallery Santa Teresa / Manto Lima descriptions match Accor property pages
- [ ] Handwritten Marival Distinct description is Accor official copy
- [ ] Kimpton Seafire / Virgilio / Indigo Miraflores descriptions look property-specific
- [ ] Ascend EC001 / DO013 Amenities populated from Wayback (spot-check amenity count)
- [ ] No overwrite of previously filled description fields
