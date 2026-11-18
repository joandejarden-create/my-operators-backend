# YOTEL dependency and feedback audit

- GDO rows with decisionId field: **0**
- Linked Decision records found via hotel lookup: **0**
- Decision lookup: {"ok":true,"matched":0}
- Shared DG-linked GDO rows: 0
- Publication STAGED/CURRENT must remain unchanged: **yes**

### Policy
- Delete GDO rows only.
- Do not delete Demand Generators / Programs / Signals / Venues.
- Preserve Decision / Decision Event history independently.
- Customer publication packages are out of scope.
