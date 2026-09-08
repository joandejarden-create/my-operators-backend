# ADP CALA Six-Hotel Cohort — Identity Freeze V1

**Status:** FROZEN for first-official ADP onboarding  
**Cohort:** St. Regis Mexico City · St. Regis Cap Cana · JW Santo Domingo · Radisson Hotel Santo Domingo · Hotel Caribe by Faranda Grand · Faranda Collection Bogotá  
**Base:** reuse Phillips → Monterrey Valle first-official pipeline  
**Dropdown:** `customerDropdownVisible: false` until each property is certified  

## Properties

| Field | St. Regis Mexico City | St. Regis Cap Cana Resort | JW Marriott Santo Domingo |
|-------|----------------------|---------------------------|---------------------------|
| ADP Property ID | `adp_st_regis_mexico_city` | `adp_st_regis_cap_cana` | `adp_jw_marriott_santo_domingo` |
| Registry entityId | `st_regis_mexico_city` | `st_regis_cap_cana` | `jw_marriott_santo_domingo` |
| Canonical name | The St. Regis Mexico City | The St. Regis Cap Cana Resort | JW Marriott Hotel Santo Domingo |
| Census Record ID | `recRXmrakhSAuctwz` | `recN76iEE6yAaPh8H` | `recESHsNsWUFYZrxR` |
| Census identity_key | `ind_marriott_mx_mexxr` | `ind_marriott_do_pujxr` | `ind_marriott_do_sdqjw` |
| MARSHA / code | `MEXXR` | `PUJXR` | `SDQJW` |
| Official page | https://www.marriott.com/en-us/hotels/mexxr-the-st-regis-mexico-city/overview/ | https://www.marriott.com/en-us/hotels/pujxr-the-st-regis-cap-cana-resort/overview/ | https://www.marriott.com/en-us/hotels/sdqjw-jw-marriott-hotel-santo-domingo/overview/ |
| Address | Paseo de la Reforma 439, Mexico City | Cap Cana, Punta Cana, Dominican Republic | Av. Winston Churchill 93, Blue Mall / Piantini, Santo Domingo |
| Market / submarket | Mexico City / Reforma | Punta Cana / Cap Cana | Santo Domingo / Piantini |
| Rooms | 189 (HIGH — Marriott / PR / CJ) | 200 (HIGH — Marriott rooms page) | 150 (HIGH — AHSD / Forbes) |
| Chain scale | Luxury | Luxury | Luxury |
| Brand / parent | St. Regis / Marriott International | St. Regis / Marriott International | JW Marriott / Marriott International |
| Management / owner | Unresolved — do not invent | Unresolved — do not invent | Unresolved — do not invent |

| Field | Radisson Hotel Santo Domingo | Hotel Caribe by Faranda Grand | Faranda Collection Bogotá |
|-------|------------------------------|-------------------------------|---------------------------|
| ADP Property ID | `adp_radisson_santo_domingo` | `adp_hotel_caribe_faranda_grand` | `adp_faranda_collection_bogota` |
| Registry entityId | `radisson_santo_domingo` | `hotel_caribe_faranda_grand` | `faranda_collection_bogota` |
| Canonical name | Radisson Hotel Santo Domingo | Hotel Caribe by Faranda Grand, a member of Radisson Individuals | Faranda Collection Bogotá |
| Census Record ID | `recUOyzOXn2Zdp98I` (preferred Choice identity; OSM duplicate `recSjnOKVqhrJP5Ol` noted) | `recCEpdskZeUBvQwG` | `rec9Tp0WBb2uk6w3u` |
| Census identity_key | `ind_choice_do_do003` | `ind_choice_co_cb014` | `ind_choice_co_cb012` |
| Official page | https://www.radissonhotels.com/en-us/hotels/radisson-santo-domingo (verify live Choice URL) | https://www.hotelcaribe.com / Choice Faranda Grand Cartagena | Choice / Faranda Collection Bogotá page |
| Address | Presidente González 10 Esq. Tiradentes, Naco, Santo Domingo | Bocagrande, Cartagena, Colombia | Calle 112 No. 13A-45, Bogotá, Colombia |
| Market / submarket | Santo Domingo / Naco–Tiradentes | Cartagena / Bocagrande | Bogotá / Norte |
| Rooms | 160 (MEDIUM — Oyster; alternate listings cite 175) | TBD MEDIUM — large Faranda Grand inventory; do not invent without official count | TBD MEDIUM — pending official count |
| Chain scale | Upper Upscale | Upper Upscale | Upscale |
| Brand / affiliation | Radisson (Choice Hotels) | Faranda Grand / Radisson Individuals by Choice | Faranda Collection / Radisson Individuals by Choice |
| Management | Unresolved — do not invent | Faranda (brand/management context; economic owner unresolved) | Faranda (brand/management context; economic owner unresolved) |

## Market packs (standard scenarios)

| Market key | Subjects sharing pack |
|------------|------------------------|
| `mexico_city_reforma` | St. Regis Mexico City |
| `cap_cana` | St. Regis Cap Cana |
| `santo_domingo` | JW Santo Domingo + Radisson Hotel Santo Domingo |
| `cartagena_bocagrande` | Hotel Caribe by Faranda Grand |
| `bogota_norte` | Faranda Collection Bogotá |

## Explicit exclusions

- St. Regis Cap Cana **Residences** (PUJRX) — not this ADP subject  
- Hotel Caribe is **Cartagena, Colombia** — not Dominican Republic  
- No paid provider measurement until preflight PASS + founder cost approval (~$8–12/hotel × 6 ≈ **$50–72**)  
- No Leak Audit substitute for full ADP  
- Do not invent economic owners  

## Next gates

1. Profiles + scenarios + entity/CORE + census links  
2. Cohort onboarding preflight (zero cost)  
3. Founder cost approval  
4. Dry-run → apply → certify/publish → `railway run` share issue → deploy registry + published  
