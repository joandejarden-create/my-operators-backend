# Production Census — Mapbox Coordinate Completion

**Status:** `production_census_mapbox_coordinate_completion_partial_provider_or_steward_remaining`  
**Generated:** 2026-08-07T23:54:14.401Z  
**Queue:** `coordinate_completion`  
**Write target:** Deal Capture Platform → Hotel Property Census (`tbl9aY5ijiuIzzWam`)

## Provider

- Ready: **yes**
- Missing flags: none
- Temporary geocoding: **blocked**
- Nominatim: **blocked**
- Max requests / run: 100

## Counts

| Metric | Count |
|--------|------:|
| Records missing coordinates | 786 |
| Eligible for Mapbox | 143 |
| Eligible via Medium match_high address | 143 |
| Geocoded (API or cache) | 100 |
| Mapbox API requests | 100 |
| Cache hits | 1 |
| Coordinate write proposals | 57 |
| Medium-address Mapbox proposals | 57 |
| High-address Mapbox proposals | 0 |
| Mapbox rejects | 114 |
| Skipped — no address | 566 |
| Skipped — Address Confidence not High | 7 |
| City-centroid rejected | 0 |
| 0,0 rejected | 0 |
| Country mismatch rejected | 0 |
| Low confidence | 44 |
| Provider errors | 0 |
| Steward review | 0 |
| Provider decision needed | 42 |
| Skipped identical | 0 |

## Cost

- Requests: 100
- Estimated USD: 0.5 (mapbox_permanent)
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

## Expected values

- Coordinate Source Type = `official_address_geocode`
- Coordinate Confidence = `High`
- Geocode Provider = `Mapbox`
- Geocode Method = `permanent_geocoding_official_address`

## Constraints

- Hotel Property Census only
- No Brand Setup / Brand Explorer / owner / operator / date / Recent Momentum / validation writes
