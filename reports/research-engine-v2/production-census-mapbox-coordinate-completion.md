# Production Census — Mapbox Coordinate Completion

**Status:** `production_census_mapbox_coordinate_completion_applied_clean`  
**Generated:** 2026-08-16T03:19:44.935Z  
**Queue:** `coordinate_completion`  
**Write target:** Deal Capture Platform → Hotel Property Census (`tbl9aY5ijiuIzzWam`)

## Provider

- Ready: **yes**
- Missing flags: none
- Temporary geocoding: **blocked**
- Nominatim: **blocked**
- Max requests / run: 9750

## Counts

| Metric | Count |
|--------|------:|
| Records missing coordinates | 7264 |
| Eligible for Mapbox | 3797 |
| Eligible via Medium match_high address | 0 |
| Geocoded (API or cache) | 3785 |
| Mapbox API requests | 3785 |
| Cache hits | 6 |
| Coordinate write proposals | 2106 |
| Medium-address Mapbox proposals | 0 |
| High-address Mapbox proposals | 2106 |
| Mapbox rejects | 1712 |
| Skipped — no address | 3440 |
| Skipped — Address Confidence not High | 0 |
| City-centroid rejected | 0 |
| 0,0 rejected | 0 |
| Country mismatch rejected | 54 |
| Low confidence | 1556 |
| Provider errors | 0 |
| Steward review | 0 |
| Provider decision needed | 0 |
| Skipped identical | 0 |

## Cost

- Requests: 3785
- Estimated USD: 18.925 (mapbox_permanent)
- Pricing configured: no (request count primary)

## Exact fields written (High proposals)

- Latitude
- Longitude
- Coordinate Source Type
- Coordinate Confidence
- Geocode Provider
- Geocode Method
- Geocode Reviewed Date
- Radar Geography Status
- Radar Display Status
- Radar Display Reason
- Public Census Eligibility
- Public Display Confidence
- Public Display Review Status
- Last Reviewed Date
- Enrichment Status
- City
- State / Region
- Postal Code

## Expected values

- Coordinate Source Type = `official_address_geocode`
- Coordinate Confidence = `High`
- Geocode Provider = `Mapbox`
- Geocode Method = `permanent_geocoding_official_address`

## Constraints

- Hotel Property Census only
- No Brand Setup / Brand Explorer / owner / operator / date / Recent Momentum / validation writes
