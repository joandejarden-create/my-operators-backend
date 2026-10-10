# Mallorca Son Vida Cross-Hotel Identity Collision Audit

## Verdict
PASS — no identity crossover detected

## Castillo
- HPC: `rec82D9zpB8fede1I`
- Name: Castillo Hotel Son Vida, a Luxury Collection Hotel, Mallorca
- Brand: Luxury Collection
- Rooms: 164

## Sheraton
- HPC: `recjhQdAUiSyxfCqE`
- Name: Sheraton Mallorca Arabella Golf Hotel
- Brand: Sheraton
- Rooms: 93

## Structural collisions
- none

## Probe results
| Mention | Via Castillo registry | Via Sheraton registry | Pass |
|---|---|---|---|
| Castillo Hotel Son Vida | castillo_hotel_son_vida | castillo_hotel_son_vida | YES |
| Castillo Hotel Son Vida, a Luxury Collection Hotel | castillo_hotel_son_vida | castillo_hotel_son_vida | YES |
| Castillo Hotel Son Vida Mallorca | castillo_hotel_son_vida | castillo_hotel_son_vida | YES |
| Castillo Son Vida | castillo_hotel_son_vida | castillo_hotel_son_vida | YES |
| Sheraton Mallorca Arabella Golf Hotel | sheraton_mallorca_arabella_golf | sheraton_mallorca_arabella_golf | YES |
| Sheraton Mallorca Arabella | sheraton_mallorca_arabella_golf | sheraton_mallorca_arabella_golf | YES |
| Sheraton Arabella Golf Hotel | sheraton_mallorca_arabella_golf | sheraton_mallorca_arabella_golf | YES |
| Sheraton Mallorca | sheraton_mallorca_arabella_golf | sheraton_mallorca_arabella_golf | YES |
| Arabella Golf Hotel | null | null | YES |
| Son Vida Golf | null | null | YES |
| Luxury Collection | null | null | YES |
| Sheraton | null | null | YES |
| Marriott | null | null | YES |

## IDENTITY_COLLISIONS_FOUND
0
