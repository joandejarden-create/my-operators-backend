# Sheraton Mallorca Arabella Golf — Hotel Intelligence / Attribute Preflight

## Trace
HPC `recjhQdAUiSyxfCqE` → Hotel Intelligence (GDI config) → ADP Attributes → ADP subject `adp_sheraton_mallorca_arabella_golf`

## Canonical facts available
| Fact | Value | Source |
|---|---|---|
| HPC hotelId | recjhQdAUiSyxfCqE | ALT Hotel Property Census |
| Identity key | ind_marriott_es_pmisi | Marriott PMISI |
| Name | Sheraton Mallorca Arabella Golf Hotel | Official |
| Brand | Sheraton | Official |
| Address | Carrer de la Vinagrella, Urbanización Son Vida, 07013 Palma | Official |
| Lat/Lng | 39.5912 / 2.5958 | Steward approximate |
| Market / Country | Mallorca / Spain | Census |
| Rooms | 93 | Arabella.com + Marriott |
| Meeting rooms | 2 (Dragonera max 45 theater; S'Estaca smaller) | Marriott events |
| Conference area | ~110 sqm | Arabella.com |
| Spa | ~843 sqm | Arabella.com |
| Property type | golf_resort / family-capable | Official |
| Positioning | Golf / leisure / small meetings / family | Official |

## Attribute inputs available
golf_resort, family_friendly, spa, meeting_space_small, leisure_resort, sports_golf_travel, upper_upscale, sheraton, marriott_bonvoy, outdoor_pool

## Missing facts
- Exact banquet reception capacity for combined spaces (not published clearly)
- Former names: none supported

## Mapping failures
None — census linked; ADP profile bound; GDI config `config/group-demand-intelligence/hotels/recjhQdAUiSyxfCqE.json` present.

## Eligible attributes
Capability engine decides eligibility independently from Castillo. Small meetings capacity is an exclusion input for large congress scenarios — not forced.
