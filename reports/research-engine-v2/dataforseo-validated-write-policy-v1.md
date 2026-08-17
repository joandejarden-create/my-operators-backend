# DataForSEO Validated Write Policy v1

**Status:** `production_census_dataforseo_validated_write_policy_v1_partial_source_remaining`
**Objective:** `dataforseo-validated-write-policy-v1`
**Generated:** 2026-08-07T22:06:30.573Z
**Mode:** field-completion-only · DataForSEO ≠ source of truth

## Summary

- Candidates reviewed: **621**
- Candidates validated (URL or rooms): **0**
- Official URL writes: **0**
- hotel_official accepted: **7**
- hotel_official rejected: **6**
- Rooms writes: **0**
- Records updated: **0**
- Address/phone/maps held: **55**
- Live blank Official Property URL (workset): **0**
- Live blank Rooms (workset): **200**

## Rooms split

- By source type:
- By confidence:

## Rejected reasons

- `travel_weekly_or_trusted_secondary_not_approved`: 48
- `rejected_host_fetch_skipped`: 81
- `rejected_ota_or_affiliate_host`: 9
- `no_rooms_on_page`: 34
- `affiliate_wording`: 3
- `maps_candidate_only`: 7
- `fetch_error:AbortError`: 3
- `http_403`: 140
- `multi_hotel_generic_page`: 3
- `http_405`: 1

## Fields written


## Safety

- Census table: Hotel Property Census (`tbl9aY5ijiuIzzWam`)
- Brand Setup / Brand Explorer writes: **0**
- Address / Phone / Coordinate writes: **0**
- Google Maps writes: **0**
- Travel Weekly direct writes: **0**
- SERP-snippet-only writes: **0**
- DataForSEO as SoT: **false**

## Next policy decision

Official Property URL already populated for this v2 set. Rooms blocked mainly by brand-site bot 403s — next: unblocked official fetch path, tourism-registry adapters (e.g. Colombia RNT), or steward factsheet pack. Maps/address/phone/Travel Weekly still not approved.

## Scale estimate

This v2 set: blank Official Property URL=0, blank Rooms=200. Brand HTML mostly HTTP 403 from Autopilot runtime. Scale rooms only after unblocked official fetch or registry adapters; do not re-spend DataForSEO SERP for the same 200.
