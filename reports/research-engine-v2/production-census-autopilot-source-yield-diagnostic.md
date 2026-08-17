# Production Census Autopilot — Source Yield Diagnostic

**Status:** `production_census_autopilot_source_yield_improved_ready_for_controlled_review`  
**Prior run diagnosed:** `2026-08-05_21-13-16-CALA-active-brands` (1 High)  
**Improved controlled run:** `2026-08-05_21-49-37-CALA-active-brands` (**108 High**, Airtable writes: false)

## 1. Queue yield (prior run → improved)

| Queue | Eligible (prior) | High (prior) | High (improved) | Top blocker |
| --- | ---: | ---: | ---: | --- |
| description_extraction | 341 | 0 | 0 | C official_page_blocked + A already_enriched (IHG) + O fetch deferred |
| amenities_extraction | 1007* | 0 | 0 | Hilton/Choice/Marriott already filled or page blocked |
| radar_public_readiness | 425 | 0 | 0 | Fields already populated for active brands |
| address_confirmation | 0* | 0 | **107** | Fixed: address-only path + coords+blank Address queue |
| property_name_cleanup | 2 | 0 | 0 | official_page_blocked (2) |
| property_type_asset_context | 425 | 1 | 1 | Mostly populated |
| rooms_keys | 420 | 0 | 0 | no_room_count / empty IHG numberOfRooms / bot blocks |
| coordinate_resolution | — | soft-deferred | soft-deferred | I provider decision |

\*Prior address eligible=0 because dry-run skipped records that already had coordinates.

## 2. Family gaps (active-brand matched = 425)

| Family | N | Miss desc | Miss amen | Miss ptype/asset | Miss addr | Miss rooms | URL | Fetch |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |
| Marriott | 197 | 197 | 197 | 100 | 197 | 197 | 100% | **403 blocked** |
| Hilton | 95 | 95 | 0 | 0 | 95 | 95 | 100% | **403 Page Reference Code** |
| IHG | 84 | 0 | 0 | 0 | 84 | 79 | 100% | **OK** |
| Choice | 49 | 49 | 13 | 10 | 49 | 49 | 100% | **403 blocked** |

### Fastest improvement by family
- **Hilton / Choice:** High VIC street addresses → Address-only writes (done)  
- **IHG:** Address via official JSON-LD fetch; rooms only when prose/numberOfRooms present  
- **Marriott:** Descriptions/amenities blocked until public fetch path exists (Webhound learning candidate)

## 3. No-proposal reason taxonomy (why prior High=1)

| Code | Reason | Dominant cause |
| --- | --- | --- |
| A | Already populated | IHG descriptions/amenities/ptype filled |
| C | Official page fetch blocked | Hilton/Marriott/Choice 403 |
| O | Fetch budget deferred | Tiny smoke fetchLimit + Hilton burned budget |
| I | Provider decision missing | Geocode soft-deferred |
| D | Page fetched, data not present | IHG empty `numberOfRooms` |
| F | High-only confidence | VIC rooms Medium never auto-High |
| K | Brand/Census match | Held / not in active universe |

**Did not loosen confidence rules.** Yield came from unused High VIC addresses + fixing the address queue skip.

## 4. Apply recommendation (improved run)

- High proposals: **108** (≥ 10 threshold)  
- **recommend_apply: true** — approval-bundle-bound after founder review  
- Bundle composition: 107 Address + 1 Asset Context  
- No Airtable writes in controlled mode  

## 5. Recommended next command

```bash
# Founder review bundle first, then (when approved):
npm run census:autopilot -- --region CALA --scope active-brand-setup --mode apply \
  --strategy fastest-safe --run-until-complete --batch-size 250 \
  --approval-bundle reports/research-engine-v2/autopilot/2026-08-05_21-49-37-CALA-active-brands/approval-bundle.json \
  --confirm-approval-bundle-bound --enable-production-writes \
  (+ all confirm flags)
```

Do **not** apply until founder reviews Address Source URLs (many Hilton VIC claims cite brand location directories — High freeze claims with evidence_url, street-level only).
