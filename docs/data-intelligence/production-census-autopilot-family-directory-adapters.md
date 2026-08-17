# Production Census Autopilot — Family Directory Adapters

Status: **production_census_autopilot_family_directory_adapters_wired**

- Version: census-autopilot-family-directory-adapters-v1
- Airtable writes: false
- Hilton Mexico directory rows: 102
- Choice Mexico regional rows: 50
- Warm errors: 0

## Sample resolutions

```json
[
  {
    "family": "Hilton",
    "identity_key": "ind_hilton_mx_qrohwhw",
    "address": {
      "ok": true,
      "method": "hilton_locations_directory",
      "address": "Gasa de Inc. a Carr. QRO-SLP  681",
      "source_url": "https://www.hilton.com/en/locations/mexico/homewood-suites/"
    },
    "amenities": {
      "ok": true,
      "method": "hilton_directory_amenityIds",
      "tag_count": 13,
      "tags_preview": [
        "Adjoining Rooms",
        "Airport Shuttle",
        "Cribs Available",
        "EV Charging",
        "Extended Stay",
        "Fitness Center"
      ]
    },
    "description": {
      "ok": false,
      "reason": "hilton_directory_has_amenities_not_narrative_description"
    },
    "coordinates": {
      "ok": true,
      "method": "hilton_locations_directory_coordinate",
      "lat": 20.69,
      "lng": -100.437334
    }
  },
  {
    "family": "Choice",
    "identity_key": "ind_choice_mx_mx165",
    "address": {
      "ok": true,
      "method": "choice_regional_hotel_card",
      "address": "Av Camaron Sabalo 811",
      "source_url": "https://www.choicehotels.com/sinaloa/mazatlan/ascend-hotels/mx165"
    },
    "amenities": {
      "ok": true,
      "method": "choice_regional_amenity_groups",
      "tag_count": 25,
      "tags_preview": [
        "All-Inclusive Resort",
        "Family Friendly",
        "Lounge/Bar",
        "Outdoor Pool",
        "Business Center",
        "Elevator(s)"
      ]
    },
    "description": {
      "ok": false,
      "reason": "choice_regional_cards_lack_hotel_narrative_description"
    },
    "coordinates": {
      "ok": true,
      "method": "choice_regional_geoLocation",
      "lat": 23.253705,
      "lng": -106.456958
    }
  },
  {
    "family": "Marriott",
    "identity_key": "ind_marriott_mx_mexcy",
    "address": {
      "ok": false,
      "reason": "no_directory_address"
    },
    "amenities": {
      "ok": false,
      "reason": "family_has_no_directory_amenities_adapter"
    },
    "description": {
      "ok": false,
      "reason": "no_directory_description_adapter"
    },
    "coordinates": {
      "ok": false,
      "reason": "akamai_or_bot_blocked"
    }
  }
]
```

## Wiring

- Address: Hilton locations + Choice regional cards before property URL
- Amenities: Hilton amenityIds + Choice amenity groups
- Descriptions: Choice regional narrative unsupported (amenities only)
- Coordinates: Marriott HQV + Hilton/Choice directory geo
- Deep page signals when official HTML fetch succeeds
- Webhound candidates only for repeated unresolved patterns (not run)
