# Deal Terms Active/Live — Manual QA checklist

Post blank-only populate (2026-07-23). Completeness rose **68% → 84%**. Remaining empties are almost entirely:

1. Three termination columns **missing from Deal Terms Meta** (all 24 brands) — see `reports/deal-terms-select-option-proposals.md`
2. Renewal Length/Duration blank on **no-contractual-renewal** brands (Quantity = 0) — intentional

## Spot-check (Brand Setup → Deal Terms tab)

| Cohort | Brand | What to verify |
|--------|-------|----------------|
| Choice FDD | Ascend Hotel Collection | Initial term years; renewal qty; PIP $/room; conditions mention FDD confirm |
| Choice FDD | Comfort Inn & Suites | Same pattern as Ascend |
| IHG FDD | Kimpton Hotels | ~20y term; no auto renewal language in conditions; Mutual Agreement structure |
| Hilton FDD | Curio Collection by Hilton | ~23y initial; conversion PIP; conditions note re-license |
| Soft / directional | Autograph Collection | Soft-brand conditions; PIP estimate |
| Soft empty→full | BW Premier Collection | Full row populated (was 0%) |
| Membership | Preferred Hotels & Resorts | Shorter term; membership framing in conditions; PIP blank OK |

## Regression

- Brand Explorer fee/terms tab still loads for Ascend, Curio, Kimpton (no blank crash)
- Match Score deal-terms inputs still resolve when present
- Do **not** expect termination selects on Deal Terms until Meta columns exist (or confirm Fee Structure holds them)

## Commands

```bash
npm run audit-active-live-deal-terms-gaps
npm run apply-active-live-deal-terms -- --dry-run
npm run apply-active-live-deal-terms
```

## Rollback

Use `reports/deal-terms-active-live-dry-run.json` / apply report to see which fields were written; clear those cells in Airtable or re-run with a restore script if needed. Blank-only did not overwrite non-empty cells.
