# Production Census Description Extraction — Dry Run

**Status:** `production_census_description_extraction_dry_run_ready_for_founder_review`  
**Generated:** 2026-08-05T17:49:52.645Z  
**Extractor:** production-census-description-extractor-v1

## Router / provider

- Selected lane: **description_extraction**
- Geocode apply approved: **false**
- Mapbox Permanent ready: **false**
- Google terms confirmed: **false**

Geocode apply blocked — proceed with description extraction dry-run.

## Summary

| Metric | Value |
| --- | ---: |
| Records scanned | 666 |
| Eligible | 425 |
| Blocked | 241 |
| Pages fetched | 90 (ok 84 / blocked 6 / failed 0) |
| Fetch deferred | 335 |
| Updates if applied | 84 |
| Descriptions | 84 |
| Amenities | 0 |
| Property type | 0 |
| Asset context | 0 |
| Market/Submarket | 0 |
| Strategic flags | 0 |
| Geocode 34 | blocked=true |

## Sample before/after

```json
[
  {
    "record_id": "rec0pT…5MKA",
    "identity_key": "ind_ihg_mx_tijgc",
    "property_name": "Hotel Indigo Tijuana Downtown",
    "family": "IHG",
    "lanes": [
      "description_source_text",
      "description_ai_summary"
    ],
    "before": {
      "Hotel Description - Source Text": null,
      "Hotel Description - AI Summary": null,
      "Amenities - Source Text": "Pool; Spa / Wellness; Fitness; Restaurant / F&B; Bar / Lounge; Meeting / Events; Parking; Wi-Fi; Pet Friendly; Business Center",
      "Property Type": "Resort"
    },
    "after": {
      "Hotel Description - Source Text": "Discover comfort and luxury at our Downtown Tijuana hotel near Tijuana International Airport with premium rooms, an on-site restaurant, and complimentary Wi-Fi.",
      "Hotel Description - AI Summary": "Discover comfort and luxury at our Downtown Tijuana hotel near Tijuana International Airport with premium rooms, an on-site restaurant, and complimentary Wi-Fi.",
      "Enrichment Status": "Partial",
      "Enrichment Priority": "Medium",
      "Last Reviewed Date": "2026-08-05"
    },
    "sources": [
      {
        "lane": "description",
        "source_url": "https://www.ihg.com/hotelindigo/hotels/us/en/tijuana/tijgc/hoteldetail",
        "method": "json_ld_hotel_description",
        "confidence": "High"
      }
    ]
  },
  {
    "record_id": "rec3Nc…LaeR",
    "identity_key": "ind_ihg_mx_mexan",
    "property_name": "Holiday Inn Express Mexico City - Toreo",
    "family": "IHG",
    "lanes": [
      "description_source_text",
      "description_ai_summary"
    ],
    "before": {
      "Hotel Description - Source Text": null,
      "Hotel Description - AI Summary": null,
      "Amenities - Source Text": "Pool; Spa / Wellness; Fitness; Restaurant / F&B; Bar / Lounge; Meeting / Events; Parking; Wi-Fi; Business Center",
      "Property Type": "Resort"
    },
    "after": {
      "Hotel Description - Source Text": "Holiday Inn Express Mexico City - Toreo (official property page). Facilities listed: Fitness Center; Housekeeping; On-site Parking; Smoke-free Hotel; Wi-fi; Free breakfast; Breakfast included. Address: Av. Primero de Mayo 226, Naucalpan, MEX.",
      "Hotel Description - AI Summary": "Holiday Inn Express Mexico City - Toreo (official property page). Facilities listed: Fitness Center; Housekeeping; On-site Parking; Smoke-free Hotel; Wi-fi; Free breakfast; Breakfast included. Address: Av. Primero de Mayo 226, Naucalpan, MEX.",
      "Enrichment Status": "Partial",
      "Enrichment Priority": "Medium",
      "Last Reviewed Date": "2026-08-05"
    },
    "sources": [
      {
        "lane": "description",
        "source_url": "https://www.ihg.com/holidayinnexpress/hotels/us/en/naucalpan/mexan/hoteldetail",
        "method": "official_page_amenities_factual_assembly",
        "confidence": "Medium"
      }
    ]
  },
  {
    "record_id": "rec4HI…ZCbT",
    "identity_key": "ind_ihg_mx_mexca",
    "property_name": "Holiday Inn Express Mexico City Satelite",
    "family": "IHG",
    "lanes": [
      "description_source_text",
      "description_ai_summary"
    ],
    "before": {
      "Hotel Description - Source Text": null,
      "Hotel Description - AI Summary": null,
      "Amenities - Source Text": "Pool; Spa / Wellness; Fitness; Restaurant / F&B; Bar / Lounge; Meeting / Events; Parking; Wi-Fi; Business Center",
      "Property Type": "Resort"
    },
    "after": {
      "Hotel Description - Source Text": "Holiday Inn Express México City Satelite offers complimentary buffet breakfast, complimentary high-speed internet, high standards of cleanliness and the necessary amenities to continue with your leisure or business activities. You can also request our transportation service that takes you 10 km around so you don't have to worry about mobility to those places you have to attend",
      "Hotel Description - AI Summary": "Holiday Inn Express México City Satelite offers complimentary buffet breakfast, complimentary high-speed internet, high standards of cleanliness and the necessary amenities to continue with your leisure or business activities. You can also request our transportation service that takes you 10 km around so you don't have to worry about mobility to those places you have to attend",
      "Enrichment Status": "Partial",
      "Enrichment Priority": "Medium",
      "Last Reviewed Date": "2026-08-05"
    },
    "sources": [
      {
        "lane": "description",
        "source_url": "https://www.ihg.com/holidayinnexpress/hotels/us/en/mexico-city/mexca/hoteldetail",
        "method": "html_paragraph",
        "confidence": "Medium"
      }
    ]
  },
  {
    "record_id": "rec4Tk…ggnD",
    "identity_key": "ind_ihg_mx_cuugt",
    "property_name": "Holiday Inn Express & Suites Chihuahua Juventud",
    "family": "IHG",
    "lanes": [
      "description_source_text",
      "description_ai_summary"
    ],
    "before": {
      "Hotel Description - Source Text": null,
      "Hotel Description - AI Summary": null,
      "Amenities - Source Text": "Pool; Spa / Wellness; Fitness; Restaurant / F&B; Bar / Lounge; Meeting / Events; Parking; Wi-Fi; Pet Friendly",
      "Property Type": "Resort"
    },
    "after": {
      "Hotel Description - Source Text": "Located near a variety of shops and restaurants, the Holiday Inn Express and Suites ® Chihuahua Juventud hotel has everything you need to enjoy your days in this city. For those who travel for business,Ford plant, Zodiac plant, TRW, Jabil, American Industries Park the industrial zone and the Expo Chihuahua Convention Center are located less than seven miles away from this hotel.",
      "Hotel Description - AI Summary": "Located near a variety of shops and restaurants, the Holiday Inn Express and Suites ® Chihuahua Juventud hotel has everything you need to enjoy your days in this city. For those who travel for business,Ford plant, Zodiac plant, TRW, Jabil, American Industries Park the industrial zone and the Expo Chihuahua Convention Center are located less than seven miles away from this hotel.",
      "Enrichment Status": "Partial",
      "Enrichment Priority": "Medium",
      "Last Reviewed Date": "2026-08-05"
    },
    "sources": [
      {
        "lane": "description",
        "source_url": "https://www.ihg.com/holidayinnexpress/hotels/us/en/chihuahua/cuugt/hoteldetail",
        "method": "html_paragraph",
        "confidence": "Medium"
      }
    ]
  },
  {
    "record_id": "rec5ip…jwck",
    "identity_key": "ind_ihg_mx_sjdtd",
    "property_name": "Kimpton Mas Olas Resort and Spa",
    "family": "IHG",
    "lanes": [
      "description_source_text",
      "description_ai_summary"
    ],
    "before": {
      "Hotel Description - Source Text": null,
      "Hotel Description - AI Summary": null,
      "Amenities - Source Text": "Pool; Spa / Wellness; Fitness; Restaurant / F&B; Bar / Lounge; Meeting / Events; Parking; Wi-Fi; Pet Friendly",
      "Property Type": "Resort"
    },
    "after": {
      "Hotel Description - Source Text": "Stay at Kimpton Mas Olas hotel in Todos Santos, featuring an outdoor pool, room service for an additional fee, and meeting rooms available for booking.",
      "Hotel Description - AI Summary": "Stay at Kimpton Mas Olas hotel in Todos Santos, featuring an outdoor pool, room service for an additional fee, and meeting rooms available for booking.",
      "Enrichment Status": "Partial",
      "Enrichment Priority": "Medium",
      "Last Reviewed Date": "2026-08-05"
    },
    "sources": [
      {
        "lane": "description",
        "source_url": "https://www.ihg.com/kimptonhotels/hotels/us/en/mas-olas-resort-spa-todos-santos/sjdtd/hoteldetail",
        "method": "json_ld_hotel_description",
        "confidence": "High"
      }
    ]
  },
  {
    "record_id": "rec6TQ…NIlx",
    "identity_key": "ind_ihg_mx_tappf",
    "property_name": "Holiday Inn Express Tapachula",
    "family": "IHG",
    "lanes": [
      "description_source_text",
      "description_ai_summary"
    ],
    "before": {
      "Hotel Description - Source Text": null,
      "Hotel Description - AI Summary": null,
      "Amenities - Source Text": "Pool; Spa / Wellness; Fitness; Restaurant / F&B; Bar / Lounge; Meeting / Events; Parking; Wi-Fi; Pet Friendly; Business Center",
      "Property Type": "Resort"
    },
    "after": {
      "Hotel Description - Source Text": "Start your days with our free-of-charge hot breakfast buffet, which serves a variety of delicious items, including our homemade sweet bread and Arabica bean coffee. Work from the Business Center and stay updated with complimentary Internet access throughout our facilities. After a day of traveling or touring the city, come back and relax in the beautiful outdoor patio with pool and sun loungers or go for an invigorating workout at our Fitness Center.",
      "Hotel Description - AI Summary": "Start your days with our free-of-charge hot breakfast buffet, which serves a variety of delicious items, including our homemade sweet bread and Arabica bean coffee. Work from the Business Center and stay updated with complimentary Internet access throughout our facilities.",
      "Enrichment Status": "Partial",
      "Enrichment Priority": "Medium",
      "Last Reviewed Date": "2026-08-05"
    },
    "sources": [
      {
        "lane": "description",
        "source_url": "https://www.ihg.com/holidayinnexpress/hotels/us/en/tapachula/tappf/hoteldetail",
        "method": "html_paragraph",
        "confidence": "Medium"
      }
    ]
  },
  {
    "record_id": "rec7jj…1Z86",
    "identity_key": "ind_ihg_mx_mlmra",
    "property_name": "Holiday Inn Express Morelia",
    "family": "IHG",
    "lanes": [
      "description_source_text",
      "description_ai_summary"
    ],
    "before": {
      "Hotel Description - Source Text": null,
      "Hotel Description - AI Summary": null,
      "Amenities - Source Text": "Pool; Spa / Wellness; Fitness; Restaurant / F&B; Bar / Lounge; Meeting / Events; Parking; Wi-Fi",
      "Property Type": "Resort"
    },
    "after": {
      "Hotel Description - Source Text": "Discover the Historic Downtown of Morelia, which was declared a World Heritage site by the UNESCO and admire the colonial architecture of its buildings and the cobblestone aqueducts with the iconic Las Tarascas fountain. Also, within walking distance from the hotel you will find the Las Americas shopping mall and Plaza Morelia, where you can find a convenience store, a movie theater and different restaurants.",
      "Hotel Description - AI Summary": "Discover the Historic Downtown of Morelia, which was declared a World Heritage site by the UNESCO and admire the colonial architecture of its buildings and the cobblestone aqueducts with the iconic Las Tarascas fountain.",
      "Enrichment Status": "Partial",
      "Enrichment Priority": "Medium",
      "Last Reviewed Date": "2026-08-05"
    },
    "sources": [
      {
        "lane": "description",
        "source_url": "https://www.ihg.com/holidayinnexpress/hotels/us/en/morelia/mlmra/hoteldetail",
        "method": "html_paragraph",
        "confidence": "Medium"
      }
    ]
  },
  {
    "record_id": "rec7v1…eXP6",
    "identity_key": "ind_ihg_mx_pvrfl",
    "property_name": "Holiday Inn Express Puerto Vallarta",
    "family": "IHG",
    "lanes": [
      "description_source_text",
      "description_ai_summary"
    ],
    "before": {
      "Hotel Description - Source Text": null,
      "Hotel Description - AI Summary": null,
      "Amenities - Source Text": "Pool; Spa / Wellness; Fitness; Restaurant / F&B; Bar / Lounge; Meeting / Events; Parking; Wi-Fi; Business Center",
      "Property Type": "Resort"
    },
    "after": {
      "Hotel Description - Source Text": "In the vicinity of the Marina, the comfortable Holiday Inn Express® Puerto Vallarta hotel offers guests the best experience for business and leisure travel. We are a short distance from the Puerto Vallarta International Airport , close to the International Convention Center and 19 minutes from downtown. We are the closest hotel to the city's industrial zone with a location just steps away from Hospiten Hospital, a renowned medical tourism center.",
      "Hotel Description - AI Summary": "In the vicinity of the Marina, the comfortable Holiday Inn Express® Puerto Vallarta hotel offers guests the best experience for business and leisure travel. We are a short distance from the Puerto Vallarta International Airport , close to the International Convention Center and 19 minutes from downtown.",
      "Enrichment Status": "Partial",
      "Enrichment Priority": "Medium",
      "Last Reviewed Date": "2026-08-05"
    },
    "sources": [
      {
        "lane": "description",
        "source_url": "https://www.ihg.com/holidayinnexpress/hotels/us/en/puerto-vallarta/pvrfl/hoteldetail",
        "method": "html_paragraph",
        "confidence": "Medium"
      }
    ]
  }
]
```

## Forbidden fields

OK=true

## Next

Founder review description/amenity proposals; apply only after confirm flags. Geocode remains blocked until provider/storage decision.

## Brand Explorer safety

all_pass=true
