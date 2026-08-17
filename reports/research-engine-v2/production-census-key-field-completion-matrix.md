# Hotel Property Census — Key Field Completion Matrix

**Status:** `production_census_key_field_completion_ready_provider_blocked`
**Generated:** 2026-08-07T17:06:09.723Z
**Records:** 1224
**Target:** Deal Capture Platform → Hotel Property Census (`tbl9aY5ijiuIzzWam`)

## Provider readiness

- Geocode apply approved: **false**
- Block reason: —
- Recommended: Set MAPBOX_ACCESS_TOKEN + MAPBOX_PERMANENT_GEOCODING=1 + CENSUS_COORDINATE_COMPLETION_ENABLED=1 + GEOCODING_PROVIDER=mapbox.

## Summary

| Metric | Count |
| --- | ---: |
| Autofill opportunities | 1979 |
| Provider-blocked coordinate records | 94 |
| Source-adapter gap records | 1224 |
| Steward-review gaps | 382 |
| Canonical blank | 3 |
| Canonical dirty | 0 |
| Canonical safe autofill | 0 |
| Canonical safe cleanup | 0 |
| Canonical steward | 176 |

## Completion by field

| Group | Field | Complete | Missing | % | Autofill | Provider blocked | Source adapter | Steward |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| core_identity | Property Name | 1224 | 0 | 100 | 0 | 0 | 0 | 0 |
| core_identity | Canonical Property Name | 1045 | 179 | 85.4 | 0 | 0 | 3 | 176 |
| core_identity | Brand | 1224 | 0 | 100 | 0 | 0 | 0 | 0 |
| core_identity | City | 1224 | 0 | 100 | 0 | 0 | 0 | 0 |
| core_identity | State / Region | 1018 | 206 | 83.2 | 0 | 0 | 0 | 206 |
| core_identity | Country | 1224 | 0 | 100 | 0 | 0 | 0 | 0 |
| core_identity | Parent Company | 1210 | 14 | 98.9 | 0 | 0 | 0 | 0 |
| source | Source URL | 1224 | 0 | 100 | 0 | 0 | 0 | 0 |
| source | Source Family | 1210 | 14 | 98.9 | 0 | 0 | 14 | 0 |
| governance | Data Confidence Tier | 1106 | 118 | 90.4 | 0 | 0 | 0 | 0 |
| governance | Identity Confidence | 1224 | 0 | 100 | 0 | 0 | 0 | 0 |
| source | Data Confidence Tier | 0 | 1224 | 0 | 0 | 0 | 1224 | 0 |
| source | Production Use Status | 1224 | 0 | 100 | 0 | 0 | 0 | 0 |
| address | Address | 400 | 824 | 32.7 | 0 | 0 | 824 | 0 |
| address | Address Confidence | 325 | 899 | 26.6 | 75 | 0 | 0 | 0 |
| address | Address Source URL | 325 | 899 | 26.6 | 9 | 0 | 0 | 0 |
| contact | Phone Number | 350 | 874 | 28.6 | 0 | 0 | 0 | 0 |
| coordinates | Latitude | 379 | 845 | 31 | 0 | 94 | 751 | 0 |
| coordinates | Longitude | 379 | 845 | 31 | 0 | 94 | 751 | 0 |
| coordinates | Coordinate Source Type | 0 | 1224 | 0 | 379 | 845 | 0 | 0 |
| coordinates | Coordinate Confidence | 0 | 1224 | 0 | 379 | 845 | 0 | 0 |
| coordinates | Geocode Provider | 0 | 1224 | 0 | 379 | 845 | 0 | 0 |
| coordinates | Geocode Method | 0 | 1224 | 0 | 379 | 845 | 0 | 0 |
| coordinates | Geocode Reviewed Date | 0 | 1224 | 0 | 379 | 845 | 0 | 0 |
| public_readiness | Radar Display Status | 0 | 1224 | 0 | 0 | 0 | 1224 | 0 |
| public_readiness | Radar Display Reason | 0 | 1224 | 0 | 0 | 0 | 1224 | 0 |
| public_readiness | Radar Geography Status | 0 | 1224 | 0 | 0 | 0 | 1224 | 0 |
| public_readiness | Public Census Eligibility | 0 | 1224 | 0 | 0 | 0 | 1224 | 0 |
| public_readiness | Public Display Confidence | 0 | 1224 | 0 | 0 | 0 | 1224 | 0 |
| public_readiness | Public Display Review Status | 0 | 1224 | 0 | 0 | 0 | 1224 | 0 |
| classification | Property Type | 0 | 1224 | 0 | 0 | 0 | 1224 | 0 |
| classification | Asset Context | 0 | 1224 | 0 | 0 | 0 | 1224 | 0 |
| classification | Market / Submarket | 0 | 1224 | 0 | 0 | 0 | 1224 | 0 |
| rooms | Rooms / Keys | 0 | 1224 | 0 | 0 | 0 | 1224 | 0 |
| rooms | Rooms Confidence | 0 | 1224 | 0 | 0 | 0 | 1224 | 0 |
| rooms | Rooms Source URL | 0 | 1224 | 0 | 0 | 0 | 1224 | 0 |
| rooms | Rooms Source Type | 0 | 1224 | 0 | 0 | 0 | 1224 | 0 |
| rooms | Rooms Reviewed Date | 0 | 1224 | 0 | 0 | 0 | 1224 | 0 |
| content | Hotel Description - Source Text | 0 | 1224 | 0 | 0 | 0 | 1224 | 0 |
| content | Hotel Description - AI Summary | 0 | 1224 | 0 | 0 | 0 | 1224 | 0 |
| content | Amenities - Source Text | 0 | 1224 | 0 | 0 | 0 | 1224 | 0 |
| content | Amenities - Structured Tags | 0 | 1224 | 0 | 0 | 0 | 1224 | 0 |

## Recommended next production-cycle action

Run production-cycle with key_field_completion after source_discovery to apply High autofills, then enrichment queues.

## Guards

- Brand Setup / Brand Explorer: read-only
- VIC / old Census: not written
- Owner / operator / dates / Recent Momentum / Company Validated / Brand Verified: blocked
- Coordinates: official source or approved provider only
