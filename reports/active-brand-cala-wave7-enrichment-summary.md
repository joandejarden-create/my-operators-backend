# Active Brand CALA — Wave 7 enrichment summary

**Generated:** 2026-07-24  
**Prior:** Waves 1–6  
**Scope:** Steward worklist for residual blanks · last automated amenity sweeps (Hilton / Accor / Choice HTML)  
**Rule:** Official sources only · fill-blank · Affiliation = Brand Setup `Brand Name`

## Verdict

Automated fill paths for Active Brand CALA are largely exhausted. Wave 7 delivers the **steward queue** for remaining gaps and confirms **0 new automated applies**.

Coverage snapshot (unchanged vs Wave 6): `reports/active-brand-cala-enrichment-coverage-wave7-after.csv`

## Steward worklist (primary deliverable)

| Path | Rows | Meaning |
|------|-----:|---------|
| **browser_save_property_page** | **105** | Have official Website; need Amenities and/or Hotel Description (and 2 Property ID) |
| **directory_research** | **63** | Missing Website/ID — find open brand listing; do not invent Pipeline IDs |
| **extract_id_from_url** | **2** | Website present; extract Property ID from URL |
| **Total residual** | **170** | ≥1 blank among Website / Property ID / Amenities / Hotel Description |

**Files:**
- `reports/active-brand-cala-steward-worklist-wave7.csv`
- `reports/active-brand-cala-steward-worklist-wave7.json`
- `reports/active-brand-cala-steward-worklist-wave7.md`
- Script: `scripts/export-active-brand-cala-steward-worklist.mjs`

### Browser-save volume by brand (top)

| Brand | Rows |
|-------|-----:|
| Comfort Inn & Suites | 19 |
| Quality Inn | 14 |
| Radisson by Choice | 13 |
| Ascend Hotel Collection | 9 |
| Trademark Collection by Wyndham | 9 |
| Dazzler by Wyndham | 8 |
| Autograph Collection | 6 |
| BW Signature Collection | 6 |
| BW Premier Collection | 4 |

Browser-save blank fields: **Amenities 94** · **Hotel Description 79** · **Property ID 2**

### Directory-research volume by brand (top)

Trademark (13), Curio (9), Autograph (6), Dazzler (5), Hotel Indigo (5), Kimpton (4), MGallery / Radisson / Tapestry / Vignette (3 each)

## Automated sweeps (Wave 7)

| Path | Ready / Applied |
|------|-----------------|
| Hilton GraphQL amenities (Curio/Tapestry with ctyhocn + blank Amenities) | **0 / 0** |
| Accor amenities (MGallery/Handwritten URL-linked blanks) | **0 / 0** |
| Choice steward-HTML residual | **0 / 0** |

## How to close browser-save rows

1. **Choice family** (Comfort / Quality / Radisson / Ascend): open Website → save as `reports/choice-amenity-html/{propertyId}.html` →  
   `node scripts/backfill-choice-wave4-from-html.mjs --apply`
2. **Wyndham / BWH / Marriott / IHG soft-blocks:** save official property HTML; use brand backfill scripts only when parse yields real amenities (not breadcrumbs / shells)
3. **Directory-research:** match official open catalog only; leave Pipeline without invented IDs

## Change impact

**Low** for census data (no writes). **Medium** for ops — steward queue is the next execution surface.

**Rollback:** N/A (no Airtable writes this wave).

## Manual QA

- [ ] Open Wave 7 CSV; spot-check 5 browser-save Choice URLs still resolve
- [ ] Confirm Trademark/Curio directory_research rows look Pipeline (no public open listing)
- [ ] After first steward HTML batch, re-run Choice HTML apply dry-run then `--apply`
