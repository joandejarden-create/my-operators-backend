# Brand Explorer Required-Section Contract Generalization Writer v27B

- Generated: 2026-07-10T01:47:10.319Z
- Mode: **dry-run**
- Brands: tribute-portfolio, curio-collection, kimpton, radisson-blu, radisson, ascend
- Airtable modified: **no**
- Company Validated untouched: **yes**

## Summary
- v27B exists: **yes**
- Contract identity bug fixed: **yes**
- Tribute preserved (contract 100 / 8/8): **yes**
- Recommended next brand: **Radisson Blu by Choice**

## Per-brand contract scores
| Brand | Pre | Post | Moved to ready | Still blocked |
| --- | ---: | ---: | --- | --- |
| Tribute Portfolio | 100 | 100 | — | — |
| Curio Collection by Hilton | 63 | 100 | Portfolio Context; Standard Detail / Where Available; Demand Scenario View | — |
| Kimpton Hotels | 63 | 100 | Portfolio Context; Standard Detail / Where Available; Demand Scenario View | — |
| Radisson Blu by Choice | 63 | 88 | Portfolio Context; Demand Scenario View | Standard Detail / Where Available |
| Radisson by Choice | 63 | 88 | Portfolio Context; Demand Scenario View | Standard Detail / Where Available |
| Ascend Hotel Collection | 63 | 100 | Portfolio Context; Standard Detail / Where Available; Demand Scenario View | — |

### Tribute Portfolio (`recCvV0PuZOi8c3hC`)
- Parent: Marriott International, Inc.
- Pre: 100 (8/8)
- Post: 100 (8/8)
- All required sections ready under v27B contract.

### Curio Collection by Hilton (`receQkxgjlezsc1xg`)
- Parent: Hilton Worldwide
- Pre: 63 (5/8)
- Post: 100 (8/8)
- False negatives resolved: Portfolio Context, Standard Detail / Where Available, Demand Scenario View
- All required sections ready under v27B contract.

### Kimpton Hotels (`recCKuXCmGvxHPfb3`)
- Parent: IHG Hotels & Resorts
- Pre: 63 (5/8)
- Post: 100 (8/8)
- False negatives resolved: Portfolio Context, Standard Detail / Where Available, Demand Scenario View
- All required sections ready under v27B contract.

### Radisson Blu by Choice (`recWPEvxBQxVVzSq3`)
- Parent: Choice Hotels International
- Pre: 63 (5/8)
- Post: 88 (7/8)
- False negatives resolved: Portfolio Context, Demand Scenario View
- Real blockers:
  - **Standard Detail / Where Available**: governance_review_state_incomplete
- Next writer: `brand-explorer-standard-detail-governance-writer`

### Radisson by Choice (`recywbx1YQSTCPqW1`)
- Parent: Choice Hotels International
- Pre: 63 (5/8)
- Post: 88 (7/8)
- False negatives resolved: Portfolio Context, Demand Scenario View
- Real blockers:
  - **Standard Detail / Where Available**: incomplete_requirement_columns:0<5; governance_review_state_incomplete
- Next writer: `brand-explorer-standard-detail-governance-writer`

### Ascend Hotel Collection (`reclkgOzvAcBheUSo`)
- Parent: Choice Hotels International
- Pre: 63 (5/8)
- Post: 100 (8/8)
- False negatives resolved: Portfolio Context, Standard Detail / Where Available, Demand Scenario View
- All required sections ready under v27B contract.

## Exact next command
```bash
npm run brand-explorer-complete-build -- --all-active --dry-run --target-quality active-profile
```