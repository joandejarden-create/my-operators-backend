# Handwritten Collection — CALA census check

**Generated:** 2026-07-23  
**Source:** Accor Catalog API `brand=SOU` + official page [C280](https://all.accor.com/hotel/C280/index.en.shtml)

## Verdict

| Scope | Result |
|-------|--------|
| Open Handwritten in CALA (Accor catalog) | **1** — C280 Marival Distinct, Nuevo Vallarta, Mexico |
| Other open CALA Handwritten | **None** found across all Dealality CALA countries + Brazil bboxes |
| Pipeline (not open / not in catalog) | **1** — Nui Handwritten Collection, João Pessoa, Brazil (2028–2029 announced) |

Brand code confirmed from C280 catalog payload: **`SOU`**.  
Brand Alias Mapping: **Handwritten Collection** → Parent **AccorHotels** (`rec42DV8KJlqGDmUZ`).

## Census update applied

| Record | Before | After |
|--------|--------|-------|
| `rec6sn8qZJU1Kl0VF` Marival Distinct Riviera Nayarit | Affiliation Independent; no Website/Property ID/Parent | Affiliation **Handwritten Collection**; Parent **AccorHotels**; Website Accor C280; Property ID **C280**; **12** Amenities from Accor page |

Match: same Marival Distinct property in Nuevo Vallarta / Riviera Nayarit (score 94). No duplicate create.

## Not Handwritten

- **Hotel Nuit** (Mar Del Plata, Argentina) — name collision only; remains Independent; not Accor SOU.
- **Mama Shelter Mexico City (C4I1)** — different Accor brand (`MSH`), pipeline opening.

## Files

- `lib/handwritten-census-enrichment.js`
- `scripts/apply-handwritten-cala-census-affiliation.mjs`
- `scripts/discover-handwritten-cala-deep.mjs`
- `reports/handwritten-cala-affiliation-apply-plan.json`
- `reports/handwritten-cala-affiliation-applies.csv`
- `reports/handwritten-cala-amenities-apply-log.json`
- `reports/handwritten-cala-sou-deep-scan.json`

## Change impact

**High** (Affiliation / Parent / Website / Property ID / Amenities writes).  
Rollback: clear or restore fields for `rec6sn8qZJU1Kl0VF` via apply CSV.

## Steward next

- When Nui João Pessoa opens and appears on Accor catalog `brand=SOU`, re-run `apply-handwritten-cala-census-affiliation.mjs` (create or match).
- Optional: set Operation Type Independent → Branded on Marival Distinct (not changed this pass).
