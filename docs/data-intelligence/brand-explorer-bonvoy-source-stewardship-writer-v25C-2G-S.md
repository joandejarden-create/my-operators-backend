# Brand Explorer Bonvoy Source Stewardship Writer v25C-2G-S

- Generated: 2026-07-09T18:22:51.519Z
- Mode: **dry-run**
- Brand: **Tribute Portfolio** (`recCvV0PuZOi8c3hC`)
- v25C-2G-S exists: **yes**

## Summary

| Metric | Value |
|--------|-------|
| Target sources inspected | 3 |
| Sources would update | 0 |
| Sources matched (idempotent) | 0 |
| Sources excluded | 0 |
| Non-target sources touched | no |
| Partner facts untouched | yes |
| Presentation rows untouched | yes |
| Airtable modified | no |
| Company Validated untouched | yes |

## Existing Bonvoy hub (report only)

- `recu6AFRZBBBNiCQn` · Explorer: **Yes** · Extraction: **Yes** · touched: **no**

## Source records inspected

### rec8eRACSCyGnHRXH

- URL: https://www.marriott.com/loyalty/member-benefits.mi
- Purpose: Elite tier summaries / member benefits
- Official Marriott Bonvoy: **yes**
- Explorer approved: **Yes** · Extraction approved: **Yes**
- Assessment: **hold** · would update: **no**

### recc9NVMd7gvDKGBF

- URL: https://www.marriott.com/loyalty/earn.mi
- Purpose: Earn mechanics
- Official Marriott Bonvoy: **yes**
- Explorer approved: **Yes** · Extraction approved: **Yes**
- Assessment: **hold** · would update: **no**

### recaAmqeCbXN3n89z

- URL: https://www.marriott.com/loyalty/redeem.mi
- Purpose: Redeem mechanics
- Official Marriott Bonvoy: **yes**
- Explorer approved: **Yes** · Extraction approved: **Yes**
- Assessment: **hold** · would update: **no**

## Exact apply command

```bash
npm run brand-explorer-bonvoy-source-stewardship-writer -- --brand tribute-portfolio --apply --approve-brand-explorer-v25C-2G-source-stewardship --confirm-official-marriott-bonvoy-sources
```

## v25C-2G re-run

```bash
npm run brand-explorer-bonvoy-loyalty-rich-fact-approval-writer -- --brand tribute-portfolio --dry-run
```

## v25C-2G apply (after stewardship)

```bash
npm run brand-explorer-bonvoy-loyalty-rich-fact-approval-writer -- --brand tribute-portfolio --apply --approve-brand-explorer-v25C-2G-rich-bonvoy-facts --founder-reviewed-rich-bonvoy-facts --confirm-bonvoy-sources-explorer-safe
```

## Does not do

- Create or approve Partner Facts
- Create new Source Library records
- Patch recu6AFRZBBBNiCQn or any non-target source
- Update Brand Explorer Presentation rows
- Change Brand Basics or Company Validated
- Imply Marriott validated anything
