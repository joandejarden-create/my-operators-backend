# Castillo Hotel Son Vida — Hotel Intelligence / Attribute Preflight

## Trace
HPC `rec82D9zpB8fede1I` → Hotel Intelligence (GDI config) → ADP Attributes → ADP subject `adp_castillo_hotel_son_vida`

## Canonical facts available
| Fact | Value | Source |
|---|---|---|
| HPC hotelId | rec82D9zpB8fede1I | ALT Hotel Property Census |
| Identity key | ind_marriott_es_pmilc | Marriott PMILC |
| Name | Castillo Hotel Son Vida, a Luxury Collection Hotel, Mallorca | Official |
| Brand | Luxury Collection | Official |
| Address | C/Raixa 2, Urbanización Son Vida, 07013 Palma | Official |
| Lat/Lng | 39.5928 / 2.5936 | Steward approximate |
| Market / Country | Mallorca / Spain | Census |
| Rooms | 164 | Marriott rooms page |
| Meeting rooms | 21 | Marriott weddings/events |
| Event space | ~3,000 sqm | Marriott |
| Largest reception | ~400 | Marriott |
| Property type | luxury_resort_castle / adults-only | Official |
| Positioning | Luxury leisure, spa, golf access, celebrations | Official |

## Attribute inputs available
luxury, heritage_castle, adults_only, spa, golf_access, meeting_space, wedding_celebrations, fine_dining, wellness, leisure_resort, marriott_bonvoy, luxury_collection

## Missing facts
- Exact largest ballroom sqm name binding (Baleares capacity known; sqm not independently verified beyond total)
- Former names: none supported

## Mapping failures
None — census linked; ADP profile bound; GDI config `config/group-demand-intelligence/hotels/rec82D9zpB8fede1I.json` present.

## Eligible attributes
Capability engine decides scenario/attribute eligibility — scores not manually forced. Adults-only and luxury/meetings facts are present enough to avoid Westin-style empty attribute inputs.
