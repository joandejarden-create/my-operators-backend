# ADP Attribute Audit — AC Hotel A Coruña

Generated: 2026-10-03T14:58:46.426Z
HPC Hotel ID: `rec2PVBDavppGpenm`
ADP Property ID: `adp_ac_hotel_a_coruna`
Mode: DRY-RUN

## Summary

| Metric | Value |
|---|---|
| Active attributes proposed | 39 |
| Used in ADP | 37 |
| Not used in ADP | 2 |
| Categories | Identity, Positioning, Location, Commercial, Meeting / Group, Amenity, Demand Node, Need Period |
| Missing critical | none |
| Profile completeness | 5/7 |
| Airtable creates | 0 |
| Airtable updates | 39 |
| Airtable deactivates | 0 |

## Attributes used by ADP

| Attribute | Value | Category | ADP Use Type | Source Type | Confidence |
|---|---|---|---|---|---|
| Hotel Name | AC Hotel A Coruña | Identity | Prompt Context; Query Generation; Exclusion Logic | HPC | HIGH |
| Brand | AC Hotels by Marriott | Positioning | Prompt Context; Query Generation; Recommendation Context | HPC | HIGH |
| Property Identity Key | ind_marriott_es_lcgco | Identity | Exclusion Logic; Reporting Only | HPC | HIGH |
| Address | Enrique Mariñas 36 | Location | Query Generation; Prompt Context; Exclusion Logic | HPC | HIGH |
| City | A Coruña | Location | Query Generation; Prompt Context; Exclusion Logic | HPC | HIGH |
| State | Galicia | Location | Query Generation; Prompt Context; Exclusion Logic | HPC | HIGH |
| Postal Code | 15009 | Location | Query Generation; Prompt Context; Exclusion Logic | HPC | HIGH |
| Market | A Coruña | Location | Query Generation; Prompt Context; Exclusion Logic | HPC | HIGH |
| Submarket | Matogrande / business corridor | Location | Query Generation; Prompt Context; Exclusion Logic | HPC | HIGH |
| Rooms | 116 | Commercial | Prompt Context; Recommendation Context | Hotel Commercial Profile | HIGH |
| Meeting Room Count | 6 | Meeting / Group | Prompt Context; Query Generation | Hotel Commercial Profile | HIGH |
| Total Meeting Space Sq Ft | 7254 | Meeting / Group | Prompt Context; Query Generation; Recommendation Context | Hotel Commercial Profile | HIGH |
| Largest Meeting Space Sq Ft | 3251 | Meeting / Group | Prompt Context; Query Generation | Hotel Event Space | HIGH |
| Largest Event Capacity | 290 | Meeting / Group | Prompt Context; Recommendation Context | Hotel Event Space | HIGH |
| Largest Meeting Space Name | Banquets | Meeting / Group | Prompt Context; Query Generation | Hotel Event Space | HIGH |
| Official Events URL | https://www.marriott.com/en-us/hotels/lcgco-ac-hotel-a-coruna/events/ | Meeting / Group | Reporting Only; Prompt Context | Hotel Commercial Profile | HIGH |
| Official Property URL | https://www.marriott.com/es/hotels/lcgco-ac-hotel-a-coruna/overview/ | Identity | Reporting Only; Prompt Context | HPC | HIGH |
| marriott_bonvoy | true | Amenity | Query Generation; Scoring; Prompt Context | Hotel Commercial Profile | MEDIUM |
| full_service | true | Amenity | Query Generation; Scoring; Prompt Context | Hotel Commercial Profile | MEDIUM |
| meeting_space | true | Amenity | Query Generation; Scoring; Prompt Context | Hotel Commercial Profile | MEDIUM |
| ballroom | true | Amenity | Query Generation; Scoring; Prompt Context | Hotel Commercial Profile | MEDIUM |
| business_center | true | Amenity | Query Generation; Scoring; Prompt Context | Hotel Commercial Profile | MEDIUM |
| urban_lifestyle | true | Amenity | Query Generation; Scoring; Prompt Context | Hotel Commercial Profile | MEDIUM |
| design_forward | true | Amenity | Query Generation; Scoring; Prompt Context | Hotel Commercial Profile | MEDIUM |
| Demand Node: Expocoruña | Expocoruña | Demand Node | Query Generation; Demand Family Selection; Prompt Context | Public Research | HIGH |
| Demand Node: Universidade da Coruña | Universidade da Coruña | Demand Node | Query Generation; Demand Family Selection; Prompt Context | Public Research | HIGH |
| Demand Node: CHUAC — Complexo Hospitalario Universitario A Coruña | CHUAC — Complexo Hospitalario Universitario A Coruña | Demand Node | Query Generation; Demand Family Selection; Prompt Context | Public Research | MEDIUM |
| Demand Node: Coliseum da Coruña | Coliseum da Coruña | Demand Node | Query Generation; Demand Family Selection; Prompt Context | Public Research | MEDIUM |
| Demand Node: Aeropuerto de A Coruña (LCG) | Aeropuerto de A Coruña (LCG) | Demand Node | Query Generation; Demand Family Selection; Prompt Context | Public Research | HIGH |
| Demand Node: Inditex / Arteixo industrial-corporate corridor | Inditex / Arteixo industrial-corporate corridor | Demand Node | Query Generation; Demand Family Selection; Prompt Context | Public Research | MEDIUM |
| Demand Node: Puerto de A Coruña | Puerto de A Coruña | Demand Node | Query Generation; Demand Family Selection; Prompt Context | Public Research | HIGH |
| Event Space: Congress | {"sqFt":1776,"type":"Meeting Room","capacities":{"theaterOrEvent":70}} | Meeting / Group | Prompt Context; Recommendation Context | Hotel Event Space | HIGH |
| Event Space: Fórum B | {"sqFt":463,"type":"Meeting Room","capacities":{"theaterOrEvent":30}} | Meeting / Group | Prompt Context; Recommendation Context | Hotel Event Space | HIGH |
| Event Space: Consejo | {"sqFt":226,"type":"Boardroom","capacities":{"theaterOrEvent":null}} | Meeting / Group | Prompt Context; Recommendation Context | Hotel Event Space | HIGH |
| Event Space: Banquets | {"sqFt":3251,"type":"Ballroom","capacities":{"theaterOrEvent":290}} | Meeting / Group | Prompt Context; Recommendation Context | Hotel Event Space | HIGH |
| Event Space: Fórum A | {"sqFt":409,"type":"Meeting Room","capacities":{"theaterOrEvent":28}} | Meeting / Group | Prompt Context; Recommendation Context | Hotel Event Space | HIGH |
| Event Space: Gran Fórum | {"sqFt":1130,"type":"Meeting Room","capacities":{"theaterOrEvent":90}} | Meeting / Group | Prompt Context; Recommendation Context | Hotel Event Space | HIGH |

## Attributes present but not used by ADP

| Attribute | Value | Notes |
|---|---|---|
| Country | Spain | Available in Hotel Intelligence; not currently wired into ADP consumption paths. |
| Need Period | NOT_PROVIDED | Present in Hotel Intelligence; ADP does not yet consume seasonality/need periods in live prompts. Hotel-supplied need periods not provided. Public/market seasonality is separate. Not an ADP production blocker; ADP does not consume need periods today. |

## How ADP uses key attributes

- **Hotel Name** = `AC Hotel A Coruña` → Prompt Context / Query Generation / Exclusion Logic
- **Brand** = `AC Hotels by Marriott` → Prompt Context / Query Generation / Recommendation Context
- **Property Identity Key** = `ind_marriott_es_lcgco` → Exclusion Logic / Reporting Only
- **Address** = `Enrique Mariñas 36` → Query Generation / Prompt Context / Exclusion Logic
- **City** = `A Coruña` → Query Generation / Prompt Context / Exclusion Logic
- **State** = `Galicia` → Query Generation / Prompt Context / Exclusion Logic
- **Postal Code** = `15009` → Query Generation / Prompt Context / Exclusion Logic
- **Market** = `A Coruña` → Query Generation / Prompt Context / Exclusion Logic
- **Submarket** = `Matogrande / business corridor` → Query Generation / Prompt Context / Exclusion Logic
- **Rooms** = `116` → Prompt Context / Recommendation Context
- **Meeting Room Count** = `6` → Prompt Context / Query Generation
- **Total Meeting Space Sq Ft** = `7254` → Prompt Context / Query Generation / Recommendation Context
- **Largest Meeting Space Sq Ft** = `3251` → Prompt Context / Query Generation
- **Largest Event Capacity** = `290` → Prompt Context / Recommendation Context
- **Largest Meeting Space Name** = `Banquets` → Prompt Context / Query Generation
- **Official Events URL** = `https://www.marriott.com/en-us/hotels/lcgco-ac-hotel-a-corun` → Reporting Only / Prompt Context
- **Official Property URL** = `https://www.marriott.com/es/hotels/lcgco-ac-hotel-a-coruna/o` → Reporting Only / Prompt Context
- **marriott_bonvoy** = `true` → Query Generation / Scoring / Prompt Context
- **full_service** = `true` → Query Generation / Scoring / Prompt Context
- **meeting_space** = `true` → Query Generation / Scoring / Prompt Context
- **ballroom** = `true` → Query Generation / Scoring / Prompt Context
- **business_center** = `true` → Query Generation / Scoring / Prompt Context
- **urban_lifestyle** = `true` → Query Generation / Scoring / Prompt Context
- **design_forward** = `true` → Query Generation / Scoring / Prompt Context
- **Demand Node: Expocoruña** = `Expocoruña` → Query Generation / Demand Family Selection / Prompt Context
