# Brand Explorer Openings + Momentum Row Creation Writer v25C-3C

- Generated: 2026-07-09T17:36:26.297Z
- Mode: **dry-run**
- Writer exists: **yes**
- Brand: **Tribute Portfolio** (`recCvV0PuZOi8c3hC`)
- Source package: `brand-explorer-openings-momentum-row-review-package.json`

## Summary

| Metric | Value |
|--------|-------|
| Rows would create | 0 |
| Rows update required | 0 |
| Rows matched (idempotent) | 11 |
| Duplicate rows found | 0 |
| Openings planned | 0 |
| Momentum planned | 0 |
| Casa Nizuc excluded from momentum | yes |
| Future-dated momentum excluded | yes |
| PR/newsroom claims avoided | yes |
| All rows have source URLs | yes |
| All openings have images | yes |
| Source Library gaps reported (non-blocking) | yes |
| Geographic footprint untouched | yes |
| Loyalty rows untouched | yes |
| Registry assets untouched | yes |
| Airtable modified | no |
| Company Validated untouched | yes |

## Governance labels (report metadata only)

- AI-drafted from official-source metadata
- Founder-review package
- Not company-validated
- Not Marriott-validated

## Source Library gaps (reported, non-blocking)

- https://www.marriott.com/en-us/hotels/cunan-casa-nizuc-a-tribute-portfolio-resort/photos/
- https://www.marriott.com/en-us/hotels/bgity-crystal-cove-barbados-a-tribute-portfolio-all-inclusive-resort/photos/
- https://www.marriott.com/en-us/hotels/sjutx-hotel-rumbao-a-tribute-portfolio-hotel/photos/
- https://www.marriott.com/en-us/hotels/limtx-humano-lima-a-tribute-portfolio-hotel/photos/
- https://www.marriott.com/en-us/hotels/mdetx-loma-medellin-a-tribute-portfolio-hotel/photos/

## Exact apply command

```bash
npm run brand-explorer-openings-momentum-row-creation-writer -- --brand tribute-portfolio --apply --approve-brand-explorer-v25C-3C-openings-momentum-rows --founder-reviewed-openings-momentum-row-copy --approve-brand-explorer-v25C-3C-row-create
```

## Does not do

- Create Partner Facts or Source Library records
- Modify geographic footprint region rows or loyalty rows
- Update existing matched rows or their images
- Modify Brand Asset Registry assets
- Change Brand Basics, Sort Order on existing rows, or Company Validated
- Write governance labels into presentation Body copy
- Use future-dated listings in Recent Momentum
- Place Casa Nizuc in Recent Momentum
- Claim newsroom/PR support without a PR/newsroom source
- Imply Marriott validated anything