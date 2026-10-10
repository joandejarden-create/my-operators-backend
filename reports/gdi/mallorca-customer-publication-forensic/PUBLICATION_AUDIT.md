# Customer Publication Record Audit

GDI does **not** use a separate publication/materialization table.
Customer publication = canonical row with facing eligibility + Ready or Valid Watch.

## Mallorca Sheraton seeds

| Field | Tournament | Golf Planet |
|-------|------------|-------------|
| customerVisible | false | false |
| customerFacingState | WATCH | WATCH |
| surfaceEligibility | DOWNGRADE_TO_DEMAND_GENERATOR | same |
| publication ID | n/a | n/a |

## Control (YOTEL visible)

| Field | Value |
|-------|-------|
| id | `gdi_opp_aidex_geneva_11` |
| title | AidEx / Clarion Events — AidEx Geneva |
| hotelId | `recrPQcZg7SFARRb2` |
| customerVisible | `true` |
| customerFacingState | `ACTIVE` |
| priority | `WATCHLIST` |
| Watch gate | `true` |
| Ready gate | `true` |
| In facing filter | YES |

**First material difference:** control is `customerVisible:true` and facing; Sheraton seeds fail Watch/surface and stay `customerVisible:false`.
