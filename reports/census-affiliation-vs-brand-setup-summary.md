# Census Affiliation ↔ Brand Setup Active/Live

**Generated:** 2026-07-24  
**Authority:** `Brand Setup - Brand Basics` · `Brand Status` ∈ {Active, Live} · field `Brand Name`  
**Census base:** `AIRTABLE_BASE_ID_ALT`

## Result

| Metric | Count |
|--------|------:|
| Active/Live Brand Setup brands | 27 |
| Affiliation rewrites applied | **83** |
| Apply failures | 0 |
| Remaining safe rewrites | **0** |

All Active Brand Setup brands that appear in census now use the **exact Brand Setup Brand Name** as Affiliation (or have zero census rows — OK for non-CALA brands).

## Rewrites applied

| From (census) | To (Brand Setup) | N |
|---|---|--:|
| Radisson (Choice) | Radisson by Choice | 38 |
| Radisson Individual (Choice) | Radisson Individuals by Choice | 14 |
| Kimpton | Kimpton Hotels | 12 |
| Radisson Blu (Choice) | Radisson Blu by Choice | 7 |
| Best Western Premier | BW Premier Collection | 5 |
| Radisson RED  (Choice) | Radisson RED by Choice | 4 |
| Ascend Collection | Ascend Hotel Collection | 1 |
| Country Inn & Suites by Radisson (Choice) | Country Inn & Suites by Choice | 1 |
| Best Western Premier Collection | BW Premier Collection | 1 |

## Steward leftover (not Active Brand Setup)

| Affiliation | CALA rows | Note |
|---|--:|---|
| Park Inn by Radisson (Choice) | 6 | No Active Brand Setup brand named Park Inn — left unchanged |

## Scripts

```bash
node scripts/audit-census-affiliation-vs-brand-setup.mjs
node scripts/apply-census-affiliation-to-brand-setup.mjs          # dry-run
node scripts/apply-census-affiliation-to-brand-setup.mjs --apply
```

Alias seed (Canonical → Brand Setup names):  
`fixtures/brand-setup-affiliation-canonical-align-reviewed.json`

## Change impact

**High** (Affiliation writes). Rollback via `reports/census-affiliation-vs-brand-setup-applies.csv`.
