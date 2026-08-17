# Source-Confirmed Census v2 — Brand Steward Mission

**Status:** `production_census_source_confirmed_census_v2_partial_steward_remaining`
**Objective:** `source-confirmed-census-v2`
**Write target:** Hotel Property Census (`tbl9aY5ijiuIzzWam`)
**Airtable writes:** yes

## Before / After

| Metric | Before | After |
| --- | ---: | ---: |
| Unknown brands (not in official census registry) | 204 | 202 |
| Human Review Required | 24 | 24 |
| Clean Core pass (approx) | 771 | 773 |
| High remaps applied | — | 170 |

## Classification counters

| Class | Count |
| --- | ---: |
| Scanned | 1091 |
| brand_valid_source_confirmed | 887 |
| high_safe_remap | 170 |
| soft_brand_listing_confirmed | 0 |
| brand_code_unresolved | 20 |
| brand_setup_promotion_candidate | 10 |
| brand_unknown_not_in_registry | 4 |
| source_conflict_steward | 0 |

## High-safe remaps (examples)

- `Tryp` → `Tryp by Wyndham` (census_high_alias) — TRYP by Wyndham Panama Centro
- `Tryp` → `Tryp by Wyndham` (census_high_alias) — TRYP by Wyndham Mexico City World Trade Center Area

## Brand Setup promotion candidates (read-only; not written)

- **Marriott Bonvoy — Brand Unconfirmed** ×4 (family hint: Marriott)
- **IHG Partner / Spnd** ×2 (family hint: IHG)
- **Choice Hotels** ×2 (family hint: Choice)
- **JOIA Iberostar** ×1 (family hint: IHG)
- **TRIBE** ×1 (family hint: Accor)
- **Mama Shelter** ×1 (family hint: Accor)
- **joia-iberostar** ×1 (family hint: IHG)
- **Apartments by Marriott Bonvoy** ×1 (family hint: Marriott)
- **Garner** ×1 (family hint: IHG)

## Unresolved steward (reason codes)

- Total steward cases: 34
- Excluded from Clean Core: 318

## Safety

- Hotel Property Census only
- Brand Setup / Brand Explorer untouched
- No address / coords / phone / rooms
- No owner/operator/date writes
- No hotel-name-only brand guesses
- Opaque codes stewarded as `brand_code_unresolved` when undecodable
