# HBX Content API CALA Wave 1 Dry-Run v1

**Status:** `production_census_hbx_content_api_cala_wave1_dry_run_v1_partial_license_policy_needed`
**Also:** `production_census_hbx_content_api_cala_wave1_dry_run_v1_partial_source_remaining` (Mexico capped 1000/5399; Colombia capped 1000/1783)
**Objective:** `hbx-content-api-cala-wave1-dry-run-v1`
**Generated:** 2026-08-09T14:16:24.675Z
**Mode:** candidate-only / dry-run

## No-write confirmation

- Airtable writes: **0**
- Hotel Property Census updates: **0**
- Inserts: **0**
- Brand Explorer / Brand Setup: **0**
- `ENABLE_HBX_CENSUS_WRITES`: **0**
- `ENABLE_HBX_INSERTS`: **0**
- Future target: Hotel Property Census (`tbl9aY5ijiuIzzWam`) — **not written**

## Scope

- Countries: Mexico, Dominican Republic, Colombia, Costa Rica, Panama
- Batch size: 100
- Max hotels/country: 1000
- Census records indexed (Wave 1 filter): 1480

## Totals

| Metric | Count |
|--------|-------|
| HBX hotels pulled | 3385 |
| existing_match_high | 607 |
| existing_match_medium | 40 |
| new_candidate_high | 2589 |
| new_candidate_medium | 0 |
| duplicate holds/reviews | 124 |
| rejected | 25 |
| address candidates | 3385 |
| website candidates | 2169 |
| PHONEHOTEL candidates | 3364 |
| coordinate candidates held | 3377 |
| image/description/facility held | 3385 |
| room count unsupported | 3385 |
| estimated write yield if approved | 3185 |

## By country

| Country | Pulled | Exist High | Exist Med | New High | New Med | Dup holds | Rejected | Addr | Web | Phone | Coords held | Est. yield |
|---------|--------|------------|-----------|----------|---------|-----------|----------|------|-----|-------|-------------|------------|
| Mexico | 1000 | 224 | 20 | 692 | 0 | 57 | 7 | 1000 | 862 | 1000 | 998 | 901 |
| Dominican Republic | 425 | 160 | 5 | 238 | 0 | 18 | 4 | 425 | 217 | 420 | 424 | 338 |
| Colombia | 1000 | 122 | 9 | 835 | 0 | 23 | 11 | 1000 | 682 | 998 | 995 | 979 |
| Costa Rica | 734 | 65 | 4 | 647 | 0 | 16 | 2 | 734 | 292 | 728 | 734 | 737 |
| Panama | 226 | 36 | 2 | 177 | 0 | 10 | 1 | 226 | 116 | 218 | 226 | 230 |

## Policy guards

- PHONEBOOKING / PHONEMANAGEMENT: **rejected as hotel phone**
- Rooms / Keys from `rooms[]`: **unsupported** (room-type catalog only)
- Coordinates / images / descriptions / facilities: **license_policy_needed**

## License policy decisions needed

- Confirm whether HBX coordinates may be permanently stored in Hotel Property Census (else Mapbox-after-validated-address only).
- Confirm whether HBX images may be stored or linked (likely internal-only / do-not-publish without license).
- Confirm whether HBX descriptions may be stored (likely internal-only).
- Confirm whether facilities tags may be stored as internal enrichment.
- Confirm schema field for Hotelbeds/HBX external hotel code before apply.

## Artifacts

- `reports/research-engine-v2/hbx-cala-wave1-candidate-pack.json`
- `reports/research-engine-v2/hbx-cala-wave1-candidate-pack.md`
- `reports/research-engine-v2/hbx-cala-wave1-write-plan.json`
- `reports/research-engine-v2/hbx-cala-wave1-write-plan.md`
- Checkpoint: `data/research-engine-v2/hbx-content-api-cala-wave1/hbx-wave1-checkpoint.json`

## Next step

Policy review (coords/images/descriptions/facilities + schema external ID). Then optional apply under ENABLE_HBX_CENSUS_WRITES with Medium-internal only for address/website/PHONEHOTEL.
