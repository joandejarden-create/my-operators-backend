# Mapbox Permanent Geocoding — Census Coordinate Completion

# Production Census — Mapbox Coordinate Completion

**Status:** `production_census_mapbox_coordinate_completion_ready_for_production_cycle`  
**Generated:** 2026-08-06T12:19:58.023Z  
**Queue:** `coordinate_completion`  
**Write target:** Deal Capture Platform → Hotel Property Census (`tbl9aY5ijiuIzzWam`)

## Provider

- Ready: **yes**
- Missing flags: none
- Temporary geocoding: **blocked**
- Nominatim: **blocked**
- Max requests / run: 250

## Counts

| Metric | Count |
|--------|------:|
| Records missing coordinates | 0 |
| Eligible for Mapbox | 0 |
| Geocoded (API or cache) | 0 |
| Mapbox API requests | 0 |
| Cache hits | 0 |
| Coordinate write proposals | 0 |
| Skipped — no address | 0 |
| Skipped — Address Confidence not High | 0 |
| City-centroid rejected | 0 |
| 0,0 rejected | 0 |
| Country mismatch rejected | 0 |
| Low confidence | 0 |
| Provider errors | 0 |
| Steward review | 0 |
| Provider decision needed | 0 |
| Skipped identical | 0 |

## Cost

- Requests: 0
- Estimated USD: 0 (none)
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


## Commands

```bash
npm run census:autopilot -- --region CALA --scope active-brand-setup --mode controlled \
  --strategy fastest-safe \
  --queue coordinate_completion \
  --run-until-complete \
  --batch-size 250
```

Required env: `MAPBOX_ACCESS_TOKEN`, `MAPBOX_PERMANENT_GEOCODING=1`, `CENSUS_COORDINATE_COMPLETION_ENABLED=1`, `GEOCODING_PROVIDER=mapbox`, `MAX_GEOCODE_REQUESTS_PER_RUN=250`.
