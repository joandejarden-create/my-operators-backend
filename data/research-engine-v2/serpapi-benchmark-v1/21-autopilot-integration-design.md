# Autopilot Integration Design — SerpApi (NOT ACTIVATED)

Decision: **INTEGRATE FOR LIMITED GOLDEN CENSUS FIELDS**

## Status
- Design only. **Do not activate** in Census Autopilot until rights clarified and steward approval.
- Production writes remain **blocked**.

## Proposed `SerpApiProvider` (field-level routing only)
| Resolver | May call SerpApi? | Notes |
|----------|-------------------|-------|
| Property identity | Yes (Exact/High only) | Never fuzzy-only |
| Address | Yes | After official/first-party miss |
| Coordinates | Yes if Exact/High | Prefer official / geocode of official address |
| Telephone / Website | Yes | Firewall allowed |
| Amenities | Yes with explicit-No support | Absent ≠ No |
| Hotel class | Raw store only | No STR/Segment auto-map |
| Rooms / Keys | **Never** | SERPAPI_ROOMS_CAPABILITY=NOT_SUPPORTED |
| Operator / Owner | **Never** | |
| Market / Submarket | **Never replace** | |
| Images | **Never production** | Reference QA only |

## Hierarchy (recommended)
1. Official brand/property source
2. SerpApi Google Hotels (Exact/High)
3. Other approved independent sources
