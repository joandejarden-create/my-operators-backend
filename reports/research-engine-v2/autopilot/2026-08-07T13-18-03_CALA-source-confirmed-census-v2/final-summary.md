# Source-Confirmed Census v2 — Brand Steward Mission

**Status:** `production_census_source_confirmed_census_v2_partial_steward_remaining`
**Objective:** `source-confirmed-census-v2`
**Write target:** Hotel Property Census (`tbl9aY5ijiuIzzWam`)
**Airtable writes:** yes

## Before / After

| Metric | Before | After |
| --- | ---: | ---: |
| Unknown brands (not in official census registry) | 111 | 111 |
| Human Review Required | 109 | 106 |
| Clean Core pass (approx) | 359 | 942 |
| High remaps applied | — | 611 |

## Classification counters

| Class | Count |
| --- | ---: |
| Scanned | 1205 |
| brand_valid_source_confirmed | 1082 |
| high_safe_remap | 0 |
| soft_brand_listing_confirmed | 3 |
| brand_code_unresolved | 19 |
| brand_setup_promotion_candidate | 74 |
| brand_unknown_not_in_registry | 18 |
| source_conflict_steward | 9 |

## High-safe remaps (examples)

- `Small Luxury Hotels of the World (HR)` → `Small Luxury Hotels of the World (HR cleared)` (soft_brand_official_listing_confirmed) — Bespoke Tulum, an SLH Hotel
- `Small Luxury Hotels of the World (HR)` → `Small Luxury Hotels of the World (HR cleared)` (soft_brand_official_listing_confirmed) — Senda Monteverde Hotel, an SLH Hotel
- `Small Luxury Hotels of the World (HR)` → `Small Luxury Hotels of the World (HR cleared)` (soft_brand_official_listing_confirmed) — Diez Diez Collection, an SLH Hotel

## Brand Setup promotion candidates (read-only; not written)

- **Independent** ×14 (family hint: —)
- **Bahía Príncipe** ×9 (family hint: Bahía Príncipe)
- **Barceló** ×8 (family hint: Barceló)
- **Hodelpa** ×8 (family hint: Hodelpa)
- **Wyndham** ×5 (family hint: Wyndham)
- **Marriott Bonvoy — Brand Unconfirmed** ×4 (family hint: Marriott)
- **Dreams (Hyatt Inclusive Collection)** ×4 (family hint: Hyatt)
- **Be Live** ×3 (family hint: Be Live)
- **Catalonia** ×3 (family hint: Catalonia)
- **Majestic Resorts** ×3 (family hint: Majestic Resorts)
- **Occidental** ×3 (family hint: Barceló)
- **Excellence Resorts** ×3 (family hint: Excellence Resorts)
- **Meliá** ×3 (family hint: Meliá)
- **IHG Partner / Spnd** ×2 (family hint: IHG)
- **Choice Hotels** ×2 (family hint: Choice)
- **Hard Rock Hotels** ×2 (family hint: Hard Rock Hotels)
- **Breathless (Hyatt Inclusive Collection)** ×2 (family hint: Hyatt)
- **Karisma Hotels** ×2 (family hint: Karisma Hotels)
- **Amhsa Marina Hotels** ×2 (family hint: Amhsa Marina Hotels)
- **Lopesan** ×2 (family hint: Lopesan)
- **Sirenis Hotels & Resorts** ×1 (family hint: Sirenis Hotels & Resorts)
- **Hyatt** ×1 (family hint: Hyatt)
- **Marriott** ×1 (family hint: Marriott)
- **Hyatt Zilara** ×1 (family hint: Hyatt)
- **Secrets (Hyatt Inclusive Collection)** ×1 (family hint: Hyatt)
- **Starfish Resorts** ×1 (family hint: Karisma Hotels)
- **Blau Hotels** ×1 (family hint: Blau Hotels)
- **Breezes (SuperClubs)** ×1 (family hint: SuperClubs)

## Unresolved steward (reason codes)

- Total steward cases: 120
- Excluded from Clean Core: 263

## Safety

- Hotel Property Census only
- Brand Setup / Brand Explorer untouched
- No address / coords / phone / rooms
- No owner/operator/date writes
- No hotel-name-only brand guesses
- Opaque codes stewarded as `brand_code_unresolved` when undecodable
