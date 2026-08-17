# Source-Confirmed Census v2 — Brand Steward Mission

**Status:** `production_census_source_confirmed_census_v2_partial_steward_remaining`
**Objective:** `source-confirmed-census-v2`
**Write target:** Hotel Property Census (`tbl9aY5ijiuIzzWam`)
**Airtable writes:** yes

## Before / After

| Metric | Before | After |
| --- | ---: | ---: |
| Unknown brands (not in official census registry) | 295 | 260 |
| Human Review Required | 61 | 24 |
| Clean Core pass (approx) | 690 | 766 |
| High remaps applied | — | 287 |

## Classification counters

| Class | Count |
| --- | ---: |
| Scanned | 1091 |
| brand_valid_source_confirmed | 733 |
| high_safe_remap | 210 |
| soft_brand_listing_confirmed | 57 |
| brand_code_unresolved | 20 |
| brand_setup_promotion_candidate | 67 |
| brand_unknown_not_in_registry | 4 |
| source_conflict_steward | 0 |

## High-safe remaps (examples)

- `Design Hotels (HR)` → `Design Hotels (HR cleared)` (soft_brand_official_listing_confirmed) — Elena de Cobre, a Member of Design Hotels™
- `Small Luxury Hotels of the World (HR)` → `Small Luxury Hotels of the World (HR cleared)` (soft_brand_official_listing_confirmed) — Mezzanine, an SLH Hotel
- `Small Luxury Hotels of the World (HR)` → `Small Luxury Hotels of the World (HR cleared)` (soft_brand_official_listing_confirmed) — American Trade Hotel, an SLH Hotel
- `Es Xl` → `Esplendor by Wyndham` (census_high_alias) — Wyndham Alltra Playa del Carmen Adults Only All Inclusive
- `Es Xl` → `Esplendor by Wyndham` (census_high_alias) — Microtel Inn & Suites by Wyndham Guadalajara Sur
- `Small Luxury Hotels of the World (HR)` → `Small Luxury Hotels of the World (HR cleared)` (soft_brand_official_listing_confirmed) — Rio Perdido, an SLH Hotel
- `Design Hotels (HR)` → `Design Hotels (HR cleared)` (soft_brand_official_listing_confirmed) — La Purificadora, Puebla, a Member of Design Hotels™
- `Design Hotels (HR)` → `Design Hotels (HR cleared)` (soft_brand_official_listing_confirmed) — Terrestre, a Member of Design Hotels™
- `Design Hotels (HR)` → `Design Hotels (HR cleared)` (soft_brand_official_listing_confirmed) — Baja Club Hotel, La Paz, Baja California Sur, a Member of Design Hotels™
- `intercontinental` → `InterContinental` (official_casing) — InterContinental Medellin - Movich
- `holidayinn` → `Holiday Inn` (alias_full_service_url) — Holiday Inn Cartagena Morros
- `Es Xl` → `Esplendor by Wyndham` (census_high_alias) — Los Cabos Golf Resort, Trademark Collection by Wyndham
- `Design Hotels (HR)` → `Design Hotels (HR cleared)` (soft_brand_official_listing_confirmed) — Rosas & Xocolate, Mérida, a Member of Design Hotels™
- `iberostar-waves` → `Iberostar Waves` (census_high_alias) — Iberostar Waves Dominicana
- `intercontinental` → `InterContinental` (official_casing) — InterContinental Costa Rica at Multiplaza Mall
- `holidayinn` → `Holiday Inn` (alias_full_service_url) — Holiday Inn Santo Domingo
- `SOL` → `Sofitel` (accor_url) — Sofitel Legend Santa Clara Cartagena
- `Design Hotels (HR)` → `Design Hotels (HR cleared)` (soft_brand_official_listing_confirmed) — Wake BioHotel, a Member of Design Hotels™
- `holidayinn` → `Holiday Inn` (alias_full_service_url) — Holiday Inn Bogota Airport
- `Design Hotels (HR)` → `Design Hotels (HR cleared)` (soft_brand_official_listing_confirmed) — CONDESA df, Mexico City, a Member of Design Hotels™
- `Es Xl` → `Esplendor by Wyndham` (census_high_alias) — La Quinta by Wyndham Medellin
- `Small Luxury Hotels of the World (HR)` → `Small Luxury Hotels of the World (HR cleared)` (soft_brand_official_listing_confirmed) — La Valise San Miguel de Allende, an SLH Hotel
- `crowneplaza` → `Crowne Plaza` (census_high_alias) — Crowne Plaza Barranquilla
- `intercontinental` → `InterContinental` (official_casing) — InterContinental Cali
- `Small Luxury Hotels of the World (HR)` → `Small Luxury Hotels of the World (HR cleared)` (soft_brand_official_listing_confirmed) — Hacienda Pena Pobre, an SLH Hotel
- `Es Xl` → `Esplendor by Wyndham` (census_high_alias) — Hotel MX mas roma CDMX, Trademark Collection by Wyndham
- `Es Xl` → `Esplendor by Wyndham` (census_high_alias) — Wyndham Garden Cartagena
- `Es Xl` → `Esplendor by Wyndham` (census_high_alias) — Bristol Panama, a Registry Collection Hotel
- `Small Luxury Hotels of the World (HR)` → `Small Luxury Hotels of the World (HR cleared)` (soft_brand_official_listing_confirmed) — The Retreat Costa Rica - Wellness Resort and Spa, an SLH Hotel
- `Es Xl` → `Esplendor by Wyndham` (census_high_alias) — Wyndham Garden Monterrey Aeropuerto

## Brand Setup promotion candidates (read-only; not written)

- **DoubleTree by Hilton** ×12 (family hint: Hilton)
- **Curio Collection by Hilton** ×10 (family hint: Hilton)
- **Quality Inn** ×7 (family hint: Choice)
- **Tru by Hilton** ×6 (family hint: Hilton)
- **Homewood Suites by Hilton** ×5 (family hint: Hilton)
- **Kimpton Hotels** ×5 (family hint: IHG)
- **Marriott Bonvoy — Brand Unconfirmed** ×4 (family hint: Marriott)
- **IHG Partner / Spnd** ×2 (family hint: IHG)
- **Choice Hotels** ×2 (family hint: Choice)
- **Motto by Hilton** ×2 (family hint: Hilton)
- **Tryp** ×2 (family hint: Wyndham)
- **Conrad Hotels & Resorts** ×2 (family hint: Hilton)
- **Fairmont Hotels & Resorts** ×2 (family hint: Accor)
- **Embassy Suites by Hilton** ×2 (family hint: Hilton)
- **JOIA Iberostar** ×1 (family hint: IHG)
- **TRIBE** ×1 (family hint: Accor)
- **Mama Shelter** ×1 (family hint: Accor)
- **joia-iberostar** ×1 (family hint: IHG)
- **Apartments by Marriott Bonvoy** ×1 (family hint: Marriott)
- **Comfort Suites** ×1 (family hint: Choice)
- **Garner** ×1 (family hint: IHG)
- **Canopy by Hilton** ×1 (family hint: Hilton)

## Unresolved steward (reason codes)

- Total steward cases: 91
- Excluded from Clean Core: 325

## Safety

- Hotel Property Census only
- Brand Setup / Brand Explorer untouched
- No address / coords / phone / rooms
- No owner/operator/date writes
- No hotel-name-only brand guesses
- Opaque codes stewarded as `brand_code_unresolved` when undecodable
