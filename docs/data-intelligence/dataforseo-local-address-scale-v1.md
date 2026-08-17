# DataForSEO Local Address Scale v1

**Status:** `production_census_dataforseo_local_address_scale_v1_partial_source_remaining`
**Objective:** `dataforseo-local-address-scale-v1`
**Generated:** 2026-08-08T00:23:50.362Z
**Mode:** field-completion-only · match_high Address (Medium) + Mapbox eligibility prep

## Summary

- Records scanned: **1224**
- Missing-address eligible (Clean Core or equivalent identity): **884**
- Records queried (Maps): **282**
- match_high reviewed: **55**
- Address writes: **0**
- Address conflicts: **10**
- Records updated: **55**
- Address complete before / after: **603** → **603**
- Address Confidence Medium (census after): **181**
- Address Confidence High (census after): **347**
- Mapbox eligible after address writes: **53**
- mapbox_pending_address_confidence: **0**
- Phone candidates held: **0**
- Coordinate candidates held: **55**
- New hotel candidates still queued: **35**
- Estimated cost: **$0.2000**

## Rejected / held reasons

- `attempt_city_state_country`: 46
- `coordinate_local_policy_not_approved`: 55
- `address_already_same`: 43
- `existing_brand_official_url_preserved`: 49
- `attempt_city_country_quoted`: 18
- `match_class_match_low`: 34
- `attempt_country_only_fallback`: 36
- `match_medium_not_approved`: 6
- `match_class_reject`: 4
- `address_conflict`: 10
- `rejected_ota_affiliate_or_directory_host`: 6
- `address_not_street_level`: 2
- `no_local_match`: 1

## Fields written

- Phone
- Last Reviewed Date
- Enrichment Status
- Notes for Steward

## Schema notes

- Address Source Type field exists: **false** (not written)
- Coordinate Eligibility Status field exists: **false** (report-only classification)

## Safety

- Census table: Hotel Property Census (`tbl9aY5ijiuIzzWam`)
- Inserts: **0**
- Phone / Lat / Long / Rooms / Website writes: **0**
- Brand Setup / Brand Explorer: **0**
- DataForSEO as SoT: **false**
- DataForSEO-only Address Confidence: **Medium** (never Official High)

## Next scale opportunity

784 Clean Core missing-address records remain beyond this workset/cost cap — re-run with higher DATAFORSEO_MAX_RECORDS or another pass.

## Next approval decision

Direct DataForSEO coordinate writes remain held. Medium phone writes require ENABLE_CENSUS_INTERNAL_MEDIUM_COMPLETION + ENABLE_DATAFORSEO_LOCAL_PHONE_WRITES. Mapbox Permanent after Medium match_high address is approved. Phone Confidence schema field still missing (provenance in Notes for Steward).
