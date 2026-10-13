# Regression QA

| Hotel | Facing | Ready | Watch unique | Generator-only facing | Pass |
|---|---:|---:|---:|---:|---|
| Bethesda | 35 | 35 | 23 | 0 | YES |
| YOTEL | 13 | 13 | 83 | 0 | YES |
| AC | 0 | 0 | 3 | 0 | YES |
| Spice | 0 | 0 | 2 | 0 | YES |
| Cambridge | 0 | 0 | 0 | 0 | YES |
| NOW NOW | 0 | 0 | 2 | 0 | YES |

## Notes
- Bethesda card renderer unchanged in structure (shared module enhanced for buyer org-path + displayTitle).
- Campaign panel removal is customer-auth only; API `/demand-campaigns` remains for internal use.
