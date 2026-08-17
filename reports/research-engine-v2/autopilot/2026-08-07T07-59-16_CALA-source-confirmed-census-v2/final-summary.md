# Source-Confirmed Census v2 — Brand Steward Mission

**Status:** `production_census_source_confirmed_census_v2_partial_steward_remaining`
**Objective:** `source-confirmed-census-v2`
**Write target:** Hotel Property Census (`tbl9aY5ijiuIzzWam`)
**Airtable writes:** no

## Before / After

| Metric | Before | After |
| --- | ---: | ---: |
| Unknown brands (not in official census registry) | 16 | 16 |
| Human Review Required | 16 | 16 |
| Clean Core pass (approx) | 784 | 784 |
| High remaps applied | — | 0 |

## Classification counters

| Class | Count |
| --- | ---: |
| Scanned | 1091 |
| brand_valid_source_confirmed | 1075 |
| high_safe_remap | 0 |
| soft_brand_listing_confirmed | 0 |
| brand_code_unresolved | 8 |
| brand_setup_promotion_candidate | 4 |
| brand_unknown_not_in_registry | 4 |
| source_conflict_steward | 0 |

## High-safe remaps (examples)

_None_

## Brand Setup promotion candidates (read-only; not written)

- **Marriott Bonvoy — Brand Unconfirmed** ×4 (family hint: Marriott)
- **IHG Partner / Spnd** ×2 (family hint: IHG)
- **Choice Hotels** ×2 (family hint: Choice)

## Unresolved steward (reason codes)

- Total steward cases: 16
- Excluded from Clean Core: 307

## Safety

- Hotel Property Census only
- Brand Setup / Brand Explorer untouched
- No address / coords / phone / rooms
- No owner/operator/date writes
- No hotel-name-only brand guesses
- Opaque codes stewarded as `brand_code_unresolved` when undecodable
