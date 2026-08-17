# Rooms Source Audit (V1.3)

## IHG
- hoteldetail often has `"numberOfRooms": ""` (empty) — not inventable
- JSON-LD / prose extraction via production-census-rooms-keys-extractor
- Standalone hotel site ladder when linked from hoteldetail
- No Cvent / legacy

## Hilton
- Public GraphQL does **not** expose room inventory fields (validated)
- facilityOverview.shortDesc may state room counts (prose)
- Property HTML often HTTP 403
- Directory locations do not include rooms

## Choice
- Property pages frequently 403
- Sitewide `numberOfRooms=25` rejected as false positive
- First-party validation primary for unresolved

## Ladder
A official standalone → B owner/operator (opportunistic) → C fact sheet → … → G first-party
