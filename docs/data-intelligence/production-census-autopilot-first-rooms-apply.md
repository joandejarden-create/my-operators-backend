# First Autopilot Rooms / Keys Production Apply

**Status:** `production_census_autopilot_first_rooms_apply_clean`  
**Generated:** 2026-08-05T20:33:49.211Z  
**Apply executed:** true  
**Bound to:** `2026-08-05_20-24-38-CALA-active-brands` approval bundle / dry-run

## Contract

- Exactly **5** High Rooms / Keys proposals from the controlled dry-run
- No re-plan, no descriptions, no coordinates, no amenities, no property type, no name cleanup
- No Brand Explorer / Brand Setup writes
- No owner/operator/developer/date writes
- Rooms Notes only if present in frozen patch (not present → not written)

## Results

| Metric | Value |
| --- | ---: |
| Records updated | 5 |
| Records failed | 0 |
| Rooms filled before → after | 0 → 5 |
| Census record count | 666 |
| Geocode 34 still blocked | true |

## Fields written

- Rooms / Keys
- Rooms Confidence
- Rooms Source URL
- Rooms Source Type
- Rooms Reviewed Date

## Next

First rooms apply clean. Continue Autopilot controlled on next queue; do not apply geocode until provider decision.
