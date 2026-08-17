# Production Census Description Extraction — IHG Apply

**Status:** `production_census_ihg_descriptions_applied_ready_for_next_family`  
**Generated:** 2026-08-05T18:29:04.255Z  
**Apply executed:** true

## 1. Executive summary

Applied grounded IHG hotel descriptions from official property pages. Geocode proposals remain blocked. Brand Explorer untouched.

| Metric | Value |
| --- | ---: |
| Approved from dry-run | 84 |
| Rebuilt for apply | 84 |
| Updates written | 84 |
| Excluded | 0 |
| Validation pass | true |

## 2. Records updated

84

## 3. Brands updated

```json
{
  "Hotel Indigo": 5,
  "Holiday Inn Express": 62,
  "Kimpton": 4,
  "voco": 8,
  "avid hotels": 5
}
```

## 4. Fields updated

- Hotel Description - Source Text
- Hotel Description - AI Summary
- Enrichment Status
- Enrichment Priority
- Last Reviewed Date

## 5. Source methods used

```json
{
  "json_ld_hotel_description": 18,
  "official_page_amenities_factual_assembly": 5,
  "html_paragraph": 61
}
```

## 6. Examples

```json
[
  {
    "record_id": "rec0pT…5MKA",
    "identity_key": "ind_ihg_mx_tijgc",
    "property_name": "Hotel Indigo Tijuana Downtown",
    "brand": "Hotel Indigo",
    "method": "json_ld_hotel_description",
    "confidence": "High",
    "source_url": "https://www.ihg.com/hotelindigo/hotels/us/en/tijuana/tijgc/hoteldetail",
    "source_text_preview": "Discover comfort and luxury at our Downtown Tijuana hotel near Tijuana International Airport with premium rooms, an on-site restaurant, and complimentary Wi-Fi.",
    "ai_summary_preview": "Discover comfort and luxury at our Downtown Tijuana hotel near Tijuana International Airport with premium rooms, an on-site restaurant, and complimentary Wi-Fi."
  },
  {
    "record_id": "rec3Nc…LaeR",
    "identity_key": "ind_ihg_mx_mexan",
    "property_name": "Holiday Inn Express Mexico City - Toreo",
    "brand": "Holiday Inn Express",
    "method": "official_page_amenities_factual_assembly",
    "confidence": "Medium",
    "source_url": "https://www.ihg.com/holidayinnexpress/hotels/us/en/naucalpan/mexan/hoteldetail",
    "source_text_preview": "Holiday Inn Express Mexico City - Toreo (official property page). Facilities listed: Fitness Center; Housekeeping; On-site Parking; Smoke-free Hotel; Wi-fi; Free breakfast; Breakfa",
    "ai_summary_preview": "Holiday Inn Express Mexico City - Toreo (official property page). Facilities listed: Fitness Center; Housekeeping; On-site Parking; Smoke-free Hotel; Wi-fi; Free breakfast; Breakfa"
  },
  {
    "record_id": "rec4HI…ZCbT",
    "identity_key": "ind_ihg_mx_mexca",
    "property_name": "Holiday Inn Express Mexico City Satelite",
    "brand": "Holiday Inn Express",
    "method": "html_paragraph",
    "confidence": "Medium",
    "source_url": "https://www.ihg.com/holidayinnexpress/hotels/us/en/mexico-city/mexca/hoteldetail",
    "source_text_preview": "Holiday Inn Express México City Satelite offers complimentary buffet breakfast, complimentary high-speed internet, high standards of cleanliness and the necessary amenities to cont",
    "ai_summary_preview": "Holiday Inn Express México City Satelite offers complimentary buffet breakfast, complimentary high-speed internet, high standards of cleanliness and the necessary amenities to cont"
  },
  {
    "record_id": "rec4Tk…ggnD",
    "identity_key": "ind_ihg_mx_cuugt",
    "property_name": "Holiday Inn Express & Suites Chihuahua Juventud",
    "brand": "Holiday Inn Express",
    "method": "html_paragraph",
    "confidence": "Medium",
    "source_url": "https://www.ihg.com/holidayinnexpress/hotels/us/en/chihuahua/cuugt/hoteldetail",
    "source_text_preview": "Located near a variety of shops and restaurants, the Holiday Inn Express and Suites ® Chihuahua Juventud hotel has everything you need to enjoy your days in this city. For those wh",
    "ai_summary_preview": "Located near a variety of shops and restaurants, the Holiday Inn Express and Suites ® Chihuahua Juventud hotel has everything you need to enjoy your days in this city. For those wh"
  },
  {
    "record_id": "rec5ip…jwck",
    "identity_key": "ind_ihg_mx_sjdtd",
    "property_name": "Kimpton Mas Olas Resort and Spa",
    "brand": "Kimpton",
    "method": "json_ld_hotel_description",
    "confidence": "High",
    "source_url": "https://www.ihg.com/kimptonhotels/hotels/us/en/mas-olas-resort-spa-todos-santos/sjdtd/hoteldetail",
    "source_text_preview": "Stay at Kimpton Mas Olas hotel in Todos Santos, featuring an outdoor pool, room service for an additional fee, and meeting rooms available for booking.",
    "ai_summary_preview": "Stay at Kimpton Mas Olas hotel in Todos Santos, featuring an outdoor pool, room service for an additional fee, and meeting rooms available for booking."
  }
]
```

## 7. Records excluded

```json
[]
```

## 8. Geocode proposals still blocked

```json
{
  "count": 34,
  "applied": false,
  "blocked": true,
  "note": "Provider/storage decision still required — no geocode writes in this batch"
}
```

## 9. Forbidden fields untouched

OK=true

## 10. Brand Explorer untouched

Confirmed: no Brand Explorer writes in this lane (BE gates run post-apply).

## 11. Validation

```json
{
  "record_count": 666,
  "duplicate_identity_keys": 0,
  "updates_attempted": 84,
  "updates_written": 84,
  "ihg_descriptions_filled_total": 84,
  "approved_batch_filled": 84,
  "coords_filled": 132,
  "zero_zero": 0,
  "held_public_eligible": 0,
  "owner_filled": 0,
  "operator_filled": 0,
  "rooms_filled": 0,
  "opening_filled": 0,
  "renovation_filled": 0,
  "affiliation_start_filled": 0,
  "geocode_provider_still_blank_or_unchanged": true,
  "airtable_errors": 0,
  "pass": true
}
```

## 12. Learning ledger

Batch: `ihg_description_extraction_apply`

## 13. Recommended next lane

Next family description lane (Choice/Hilton/Marriott) after safe fetch strategy; or Mapbox Permanent for 34 geocodes.

## Brand Explorer gate results

all_pass=true (Active 62 / semantic / PVQL / momentum / mandatory)
