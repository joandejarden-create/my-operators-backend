# Choice Explorer presentation gap audit

Generated: 2026-05-27

Compares live Airtable rows to expected rows from `buildCompletePresentationRows()` (includes default `footprint.openings` when profile has none).

## CALA footprint openings (2026-05-27)

Illustrative `footprint.openings` placeholders (Comfort Mérida comp, generic CALA gateway cards, US-only extended-stay samples) were replaced with **real choicehotels.com CALA property URLs** from `reports/independent-census-choice-property-match-cala-2026-05-20.csv`, Radisson fixtures, and curated press-backed cards.

```bash
npm run apply-choice-cala-footprint-openings-batch -- --dry-run
npm run apply-choice-cala-footprint-openings-batch
```

Source module: `scripts/lib/choice-cala-openings-from-census.mjs`. Brands with **no CALA consumer listings** (Cambria, MainStay, WoodSpring, Suburban, Everhome, Park Plaza, Radisson Collection, Radisson Inn, Rodeway, Clarion Pointe) use **labeled tier-family comps** with explicit case-summary disclaimers—not fake same-flag hotels.

## Ascend Hotel Collection

- Profile: `Ascend Hotel Collection` (ascend-hotel-collection)
- Existing rows: 199 · Expected unique slot keys: 161
- Missing slot keys: **0** · Short counts: **0**
- Status: **complete**

## Cambria Hotels

- Profile: `Cambria Hotels` (cambria-hotels)
- Existing rows: 199 · Expected unique slot keys: 161
- Missing slot keys: **0** · Short counts: **0**
- Status: **complete**

## Clarion

- Profile: `Clarion` (clarion)
- Existing rows: 198 · Expected unique slot keys: 161
- Missing slot keys: **0** · Short counts: **0**
- Status: **complete**

## Clarion Pointe

- Profile: `Clarion Pointe` (clarion-pointe)
- Existing rows: 198 · Expected unique slot keys: 161
- Missing slot keys: **0** · Short counts: **0**
- Status: **complete**

## Comfort Inn & Suites

- Profile: `Comfort Inn & Suites` (comfort-inn-suites)
- Existing rows: 198 · Expected unique slot keys: 161
- Missing slot keys: **0** · Short counts: **0**
- Status: **complete**

## Country Inn & Suites by Radisson (Choice)

- Profile: `Country Inn & Suites by Radisson (Choice)` (country-inn-suites-by-radisson-choice)
- Existing rows: 198 · Expected unique slot keys: 161
- Missing slot keys: **0** · Short counts: **0**
- Status: **complete**

## Econo Lodge

- Profile: `Econo Lodge` (econo-lodge)
- Existing rows: 197 · Expected unique slot keys: 161
- Missing slot keys: **0** · Short counts: **0**
- Status: **complete**

## Everhome Suites

- Profile: `Everhome Suites` (everhome-suites)
- Existing rows: 198 · Expected unique slot keys: 161
- Missing slot keys: **0** · Short counts: **0**
- Status: **complete**

## MainStay Suites

- Profile: `MainStay Suites` (mainstay-suites)
- Existing rows: 198 · Expected unique slot keys: 161
- Missing slot keys: **0** · Short counts: **0**
- Status: **complete**

## Park Inn by Choice

- Profile: `Park Inn by Radisson (Choice)` (park-inn-by-radisson-choice)
- Existing rows: 199 · Expected unique slot keys: 161
- Missing slot keys: **0** · Short counts: **0**
- Status: **complete**

## Park Plaza by Choice

- Profile: `Park Plaza (Choice)` (park-plaza-choice)
- Existing rows: 199 · Expected unique slot keys: 161
- Missing slot keys: **0** · Short counts: **0**
- Status: **complete**

## Quality Inn

- Profile: `Quality Inn` (quality-inn)
- Existing rows: 198 · Expected unique slot keys: 161
- Missing slot keys: **0** · Short counts: **0**
- Status: **complete**

## Radisson Blu by Choice

- Profile: `Radisson Blu (Choice)` (radisson-blu-choice)
- Existing rows: 217 · Expected unique slot keys: 161
- Missing slot keys: **0** · Short counts: **0**
- Status: **complete**

## Radisson by Choice

- Profile: `Radisson (Choice)` (radisson-choice)
- Existing rows: 212 · Expected unique slot keys: 161
- Missing slot keys: **0** · Short counts: **0**
- Status: **complete**

## Radisson Collection by Choice

- Profile: `Radisson Collection  (Choice)` (radisson-collection-choice)
- Existing rows: 198 · Expected unique slot keys: 161
- Missing slot keys: **0** · Short counts: **0**
- Status: **complete**

## Radisson Individuals by Choice

- Profile: `Radisson Individual (Choice)` (radisson-individual-choice)
- Existing rows: 203 · Expected unique slot keys: 161
- Missing slot keys: **0** · Short counts: **0**
- Status: **complete**

## Radisson Inn & Suites

- Profile: `Radisson Inn & Suites` (radisson-inn-suites)
- Existing rows: 198 · Expected unique slot keys: 161
- Missing slot keys: **0** · Short counts: **0**
- Status: **complete**

## Radisson RED by Choice

- Profile: `Radisson RED  (Choice)` (radisson-red-choice)
- Existing rows: 202 · Expected unique slot keys: 161
- Missing slot keys: **0** · Short counts: **0**
- Status: **complete**

## Rodeway Inn

- Profile: `Rodeway Inn` (rodeway-inn)
- Existing rows: 197 · Expected unique slot keys: 161
- Missing slot keys: **0** · Short counts: **0**
- Status: **complete**

## Sleep Inn

- Profile: `Sleep Inn` (sleep-inn)
- Existing rows: 198 · Expected unique slot keys: 161
- Missing slot keys: **0** · Short counts: **0**
- Status: **complete**

## Suburban Studios

- Profile: `Suburban Studios` (suburban-studios)
- Existing rows: 198 · Expected unique slot keys: 161
- Missing slot keys: **0** · Short counts: **0**
- Status: **complete**

## WoodSpring Suites

- Profile: `WoodSpring Suites` (woodspring-suites)
- Existing rows: 201 · Expected unique slot keys: 161
- Missing slot keys: **0** · Short counts: **0**
- Status: **complete**

## Summary

Total brands: 22 · Brands with gaps: 0
Total missing/short slot groups: 0