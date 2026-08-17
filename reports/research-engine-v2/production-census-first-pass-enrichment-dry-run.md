# Production Census First Pass — Dry Run

**Status:** `production_census_first_pass_dry_run_ready_for_founder_review`  
**Generated:** 2026-08-05T13:24:55.562Z  
**Base:** `appCCU…foLk`

## 1. Executive summary

- Census scanned: **666**
- Active-brand mapped: **425**
- Eligible for first pass: **425**
- Blocked: **241**
- Coordinate updates proposed: **132**
- Radar updates proposed: **666**
- Amenity updates proposed: **215**
- Description updates proposed: **0**
- Exact Airtable update count: **666**
- Dry-run pass: **true**

## 2. Active-brand Census scope

```json
{
  "active_universe": 62,
  "brand_mapping_counts": {
    "exact_match": 390,
    "not_in_active_universe": 237,
    "alias_match": 35,
    "blocked_brand_unconfirmed": 4
  },
  "vic_sources": [
    {
      "family": "Hilton",
      "path": "data/research-engine-v2/verified-independent-census-wave1b-hilton/02-hilton-full-records.json",
      "ok": true,
      "count": 102
    },
    {
      "family": "Choice",
      "path": "data/research-engine-v2/verified-independent-census-wave1c-choice/02-choice-full-records.json",
      "ok": true,
      "count": 68
    },
    {
      "family": "Marriott",
      "path": "data/research-engine-v2/verified-independent-census-wave1d-marriott/02-marriott-full-records.json",
      "ok": true,
      "count": 301
    },
    {
      "family": "IHG",
      "path": "data/research-engine-v2/verified-independent-census-v1/08-expanded-benchmark-full-records.json",
      "ok": true,
      "count": 195
    }
  ]
}
```

## 3. Coordinate coverage audit

```json
{
  "before": {
    "with_coordinates": 0,
    "missing": 666
  },
  "proposed_updates": 132,
  "shared_campus_pins": [
    {
      "coord": "19.36529,-99.26257",
      "properties": [
        "Hilton Garden Inn Mexico City Santa Fe",
        "DoubleTree by Hilton Hotel México City Santa Fe"
      ]
    },
    {
      "coord": "25.46152,-100.98124",
      "properties": [
        "Tru by Hilton Saltillo",
        "Homewood Suites by Hilton Saltillo"
      ]
    }
  ],
  "geography_coverage": {
    "latitude": 0,
    "longitude": 0,
    "address": 0,
    "city": 666,
    "state_region": 0,
    "country": 666
  }
}
```

## 4. Proposed coordinate updates

See JSON `field_update_counts.Latitude/Longitude` and sample before/after.

## 5. Radar readiness classification counts

```json
{
  "Public List Eligible": 293,
  "Public Map Eligible": 132,
  "Internal Only": 237,
  "Hold": 4
}
```

## 6–9. Safe enrichment proposals

| Lane | Count |
| --- | ---: |
| Descriptions | 0 |
| Amenities | 215 |
| Property type / asset context | 295 |
| Strategic flags | 215 |
| Market / Submarket | 217 |

## 10. Blocked field research queue

Count: **298** (owner / operator / rooms / dates researched but not written)

## 11. Source support summary

All proposed coordinate/amenity writes require evidence URL + Medium/High confidence from VIC official directory/property claims.

## 12. Webhound usage

See durable doc after sidecar completes (Marriott coordinate extraction patterns). Webhound output is never written directly to Airtable.

## 13. Forbidden fields untouched

```json
{
  "fields": [
    "Owner Name",
    "Developer Name",
    "Developer",
    "Operator / Management Company",
    "Rooms / Keys",
    "Opening Date",
    "Renovation / Conversion Date",
    "Renovation Date",
    "Affiliation Start Date",
    "Company Validated",
    "Brand Verified",
    "Recent Momentum"
  ],
  "proposed_writes": [],
  "ok": true
}
```

## 14. Brand Explorer safety

Run post-apply gates. This dry-run does not touch Brand Explorer.

## 15. Next recommended lane

Founder review → apply with confirm flags → Marriott/IHG coordinate sourcing lane + description page scrape

## Sample before/after

```json
[
  {
    "record_id": "rec02w…jPjx",
    "identity_key": "ind_marriott_mx_bjxds",
    "property_name": "Elena de Cobre, a Member of Design Hotels™",
    "before": {
      "Radar Display Status": null,
      "Radar Display Reason": null,
      "Radar Geography Status": null,
      "Public Census Eligibility": null,
      "Public Display Confidence": null,
      "Public Display Review Status": null,
      "Enrichment Status": "Not Started",
      "Enrichment Priority": "Low",
      "Last Reviewed Date": null
    },
    "after": {
      "Radar Display Status": "Public List Eligible",
      "Radar Display Reason": "List-eligible; city/country/source present; coordinates missing or not property-level",
      "Radar Geography Status": "City-Level Only",
      "Public Census Eligibility": "Eligible With Limits",
      "Public Display Confidence": "Medium",
      "Public Display Review Status": "Auto-Classified",
      "Enrichment Status": "In Progress",
      "Enrichment Priority": "Medium",
      "Last Reviewed Date": "2026-08-05"
    },
    "sources": []
  },
  {
    "record_id": "rec0Dl…HAYh",
    "identity_key": "ind_hilton_mx_cywcedt",
    "property_name": "DoubleTree by Hilton Celaya",
    "before": {
      "Latitude": null,
      "Longitude": null,
      "Amenities - Source Text": null,
      "Amenities - Structured Tags": null,
      "F&B Flag": null,
      "Meeting Space Flag": null,
      "Resort / Leisure Flag": null,
      "Property Type": null,
      "Radar Display Status": null,
      "Radar Display Reason": null,
      "Radar Geography Status": null,
      "Public Census Eligibility": null,
      "Public Display Confidence": null,
      "Public Display Review Status": null,
      "Enrichment Status": "Not Started",
      "Enrichment Priority": "Medium",
      "Last Reviewed Date": null
    },
    "after": {
      "Latitude": 20.518729,
      "Longitude": -100.79289,
      "Amenities - Source Text": "Adjoining Rooms; Business Center; Cribs Available; Digital Key; EV Charging; Fitness Center; Free Parking; Free WiFi; Meeting Rooms; Non-Smoking Rooms; On-Site Restaurant; Outdoor Pool; Pets Not Allowed; Room Service",
      "Amenities - Structured Tags": "Adjoining Rooms\nBusiness Center\nCribs Available\nDigital Key\nEV Charging\nFitness Center\nFree Parking\nFree WiFi\nMeeting Rooms\nNon-Smoking Rooms\nOn-Site Restaurant\nOutdoor Pool\nPets Not Allowed\nRoom Service",
      "F&B Flag": true,
      "Meeting Space Flag": true,
      "Resort / Leisure Flag": true,
      "Property Type": "Resort",
      "Radar Display Status": "Public Map Eligible",
      "Radar Display Reason": "Property-level coordinates from Hilton directory localization.coordinate; affiliation clear; source URL present",
      "Radar Geography Status": "Coordinates Available",
      "Public Census Eligibility": "Eligible",
      "Public Display Confidence": "High",
      "Public Display Review Status": "Auto-Classified",
      "Enrichment Status": "Partial",
      "Enrichment Priority": "Medium",
      "Last Reviewed Date": "2026-08-05"
    },
    "sources": [
      {
        "fields": [
          "Latitude",
          "Longitude"
        ],
        "source_url": "https://www.hilton.com/en/locations/mexico/doubletree-by-hilton/",
        "confidence": "High",
        "source": "Hilton directory localization.coordinate"
      },
      {
        "fields": [
          "Amenities - Source Text",
          "Amenities - Structured Tags"
        ],
        "source_url": "https://www.hilton.com/en/locations/mexico/doubletree-by-hilton/",
        "confidence": "High",
        "source": "Hilton directory amenityIds (structured)"
      },
      {
        "fields": [
          "F&B Flag",
          "Meeting Space Flag",
          "Resort / Leisure Flag"
        ],
        "source_url": "https://www.hilton.com/en/locations/mexico/doubletree-by-hilton/",
        "confidence": "High",
        "source": "amenity_text_and_brand_evidence"
      },
      {
        "fields": [
          "Property Type"
        ],
        "source_url": "https://www.hilton.com/en/locations/mexico/doubletree-by-hilton/",
        "confidence": "Medium",
        "source": "resort_signal"
      }
    ]
  },
  {
    "record_id": "rec0H6…St9L",
    "identity_key": "ind_marriott_mx_cenxo",
    "property_name": "City Express by Marriott Ciudad Obregón",
    "before": {
      "Radar Display Status": null,
      "Radar Display Reason": null,
      "Radar Geography Status": null,
      "Public Census Eligibility": null,
      "Public Display Confidence": null,
      "Public Display Review Status": null,
      "Enrichment Status": "Not Started",
      "Enrichment Priority": "Low",
      "Last Reviewed Date": null
    },
    "after": {
      "Radar Display Status": "Public List Eligible",
      "Radar Display Reason": "List-eligible; city/country/source present; coordinates missing or not property-level",
      "Radar Geography Status": "City-Level Only",
      "Public Census Eligibility": "Eligible With Limits",
      "Public Display Confidence": "Medium",
      "Public Display Review Status": "Auto-Classified",
      "Enrichment Status": "In Progress",
      "Enrichment Priority": "Medium",
      "Last Reviewed Date": "2026-08-05"
    },
    "sources": []
  },
  {
    "record_id": "rec0Iq…vstj",
    "identity_key": "ind_hilton_mx_cunmelx",
    "property_name": "Mezzanine, an SLH Hotel",
    "before": {
      "Latitude": null,
      "Longitude": null,
      "Amenities - Source Text": null,
      "Amenities - Structured Tags": null,
      "F&B Flag": null,
      "Property Type": null,
      "Market / Submarket": null,
      "Asset Context": null,
      "Radar Display Status": null,
      "Radar Display Reason": null,
      "Radar Geography Status": null,
      "Public Census Eligibility": null,
      "Public Display Confidence": null,
      "Public Display Review Status": null,
      "Enrichment Status": "Not Started",
      "Enrichment Priority": "Medium",
      "Last Reviewed Date": null
    },
    "after": {
      "Latitude": 20.197377,
      "Longitude": -87.43729,
      "Amenities - Source Text": "Concierge; Cribs Available; Free WiFi; Luxury; Non-Smoking Rooms; On-Site Restaurant; Pets Not Allowed; Room Service",
      "Amenities - Structured Tags": "Concierge\nCribs Available\nFree WiFi\nLuxury\nNon-Smoking Rooms\nOn-Site Restaurant\nPets Not Allowed\nRoom Service",
      "F&B Flag": true,
      "Property Type": "Boutique Hotel",
      "Market / Submarket": "Cancún / Riviera Maya · Tulum",
      "Asset Context": "Beach / Waterfront",
      "Radar Display Status": "Public Map Eligible",
      "Radar Display Reason": "Property-level coordinates from Hilton directory localization.coordinate; affiliation clear; source URL present",
      "Radar Geography Status": "Coordinates Available",
      "Public Census Eligibility": "Eligible",
      "Public Display Confidence": "High",
      "Public Display Review Status": "Auto-Classified",
      "Enrichment Status": "Partial",
      "Enrichment Priority": "Medium",
      "Last Reviewed Date": "2026-08-05"
    },
    "sources": [
      {
        "fields": [
          "Latitude",
          "Longitude"
        ],
        "source_url": "https://www.hilton.com/en/locations/mexico/small-luxury-hotels-slh/",
        "confidence": "High",
        "source": "Hilton directory localization.coordinate"
      },
      {
        "fields": [
          "Amenities - Source Text",
          "Amenities - Structured Tags"
        ],
        "source_url": "https://www.hilton.com/en/locations/mexico/small-luxury-hotels-slh/",
        "confidence": "High",
        "source": "Hilton directory amenityIds (structured)"
      },
      {
        "fields": [
          "F&B Flag"
        ],
        "source_url": "https://www.hilton.com/en/locations/mexico/small-luxury-hotels-slh/",
        "confidence": "High",
        "source": "amenity_text_and_brand_evidence"
      },
      {
        "fields": [
          "Property Type"
        ],
        "source_url": "https://www.hilton.com/en/locations/mexico/small-luxury-hotels-slh/",
        "confidence": "Medium",
        "source": "boutique_or_collection_brand"
      },
      {
        "fields": [
          "Market / Submarket"
        ],
        "source_url": "https://www.hilton.com/en/locations/mexico/small-luxury-hotels-slh/",
        "confidence": "High",
        "source": "Dealality Market + corridor inference from Census city (not STR)"
      },
      {
        "fields": [
          "Asset Context"
        ],
        "source_url": "https://www.hilton.com/en/locations/mexico/small-luxury-hotels-slh/",
        "confidence": "Medium",
        "source": "coastal_market_or_name"
      }
    ]
  },
  {
    "record_id": "rec0Rc…rrqD",
    "identity_key": "ind_marriott_mx_vsacy",
    "property_name": "Courtyard by Marriott Villahermosa Tabasco",
    "before": {
      "Radar Display Status": null,
      "Radar Display Reason": null,
      "Radar Geography Status": null,
      "Public Census Eligibility": null,
      "Public Display Confidence": null,
      "Public Display Review Status": null,
      "Enrichment Status": "Not Started",
      "Enrichment Priority": "Medium",
      "Last Reviewed Date": null
    },
    "after": {
      "Radar Display Status": "Public List Eligible",
      "Radar Display Reason": "List-eligible; city/country/source present; coordinates missing or not property-level",
      "Radar Geography Status": "City-Level Only",
      "Public Census Eligibility": "Eligible With Limits",
      "Public Display Confidence": "Medium",
      "Public Display Review Status": "Auto-Classified",
      "Enrichment Status": "In Progress",
      "Enrichment Priority": "Medium",
      "Last Reviewed Date": "2026-08-05"
    },
    "sources": []
  },
  {
    "record_id": "rec0dx…O1nZ",
    "identity_key": "ind_marriott_mx_nogxn",
    "property_name": "City Express by Marriott Nogales",
    "before": {
      "Radar Display Status": null,
      "Radar Display Reason": null,
      "Radar Geography Status": null,
      "Public Census Eligibility": null,
      "Public Display Confidence": null,
      "Public Display Review Status": null,
      "Enrichment Status": "Not Started",
      "Enrichment Priority": "Low",
      "Last Reviewed Date": null
    },
    "after": {
      "Radar Display Status": "Public List Eligible",
      "Radar Display Reason": "List-eligible; city/country/source present; coordinates missing or not property-level",
      "Radar Geography Status": "City-Level Only",
      "Public Census Eligibility": "Eligible With Limits",
      "Public Display Confidence": "Medium",
      "Public Display Review Status": "Auto-Classified",
      "Enrichment Status": "In Progress",
      "Enrichment Priority": "Medium",
      "Last Reviewed Date": "2026-08-05"
    },
    "sources": []
  },
  {
    "record_id": "rec0kS…q3cG",
    "identity_key": "ind_hilton_mx_qrohwhw",
    "property_name": "Homewood Suites by Hilton Queretaro",
    "before": {
      "Latitude": null,
      "Longitude": null,
      "Amenities - Source Text": null,
      "Amenities - Structured Tags": null,
      "Resort / Leisure Flag": null,
      "Extended Stay Flag": null,
      "Property Type": null,
      "Market / Submarket": null,
      "Asset Context": null,
      "Radar Display Status": null,
      "Radar Display Reason": null,
      "Radar Geography Status": null,
      "Public Census Eligibility": null,
      "Public Display Confidence": null,
      "Public Display Review Status": null,
      "Enrichment Status": "Not Started",
      "Enrichment Priority": "Medium",
      "Last Reviewed Date": null
    },
    "after": {
      "Latitude": 20.69,
      "Longitude": -100.437334,
      "Amenities - Source Text": "Adjoining Rooms; Airport Shuttle; Cribs Available; EV Charging; Extended Stay; Fitness Center; Free Breakfast; Free Parking; Free WiFi; In Room Kitchen; Non-Smoking Rooms; Outdoor Pool; Pet-Friendly",
      "Amenities - Structured Tags": "Adjoining Rooms\nAirport Shuttle\nCribs Available\nEV Charging\nExtended Stay\nFitness Center\nFree Breakfast\nFree Parking\nFree WiFi\nIn Room Kitchen\nNon-Smoking Rooms\nOutdoor Pool\nPet-Friendly",
      "Resort / Leisure Flag": true,
      "Extended Stay Flag": true,
      "Property Type": "Extended Stay",
      "Market / Submarket": "Other · Querétaro",
      "Asset Context": "Airport",
      "Radar Display Status": "Public Map Eligible",
      "Radar Display Reason": "Property-level coordinates from Hilton directory localization.coordinate; affiliation clear; source URL present",
      "Radar Geography Status": "Coordinates Available",
      "Public Census Eligibility": "Eligible",
      "Public Display Confidence": "High",
      "Public Display Review Status": "Auto-Classified",
      "Enrichment Status": "Partial",
      "Enrichment Priority": "Medium",
      "Last Reviewed Date": "2026-08-05"
    },
    "sources": [
      {
        "fields": [
          "Latitude",
          "Longitude"
        ],
        "source_url": "https://www.hilton.com/en/locations/mexico/homewood-suites/",
        "confidence": "High",
        "source": "Hilton directory localization.coordinate"
      },
      {
        "fields": [
          "Amenities - Source Text",
          "Amenities - Structured Tags"
        ],
        "source_url": "https://www.hilton.com/en/locations/mexico/homewood-suites/",
        "confidence": "High",
        "source": "Hilton directory amenityIds (structured)"
      },
      {
        "fields": [
          "Resort / Leisure Flag",
          "Extended Stay Flag"
        ],
        "source_url": "https://www.hilton.com/en/locations/mexico/homewood-suites/",
        "confidence": "High",
        "source": "amenity_text_and_brand_evidence"
      },
      {
        "fields": [
          "Property Type"
        ],
        "source_url": "https://www.hilton.com/en/locations/mexico/homewood-suites/",
        "confidence": "High",
        "source": "brand_or_amenity_extended_stay"
      },
      {
        "fields": [
          "Market / Submarket"
        ],
        "source_url": "https://www.hilton.com/en/locations/mexico/homewood-suites/",
        "confidence": "Medium",
        "source": "Dealality Market + corridor inference from Census city (not STR)"
      },
      {
        "fields": [
          "Asset Context"
        ],
        "source_url": "https://www.hilton.com/en/locations/mexico/homewood-suites/",
        "confidence": "High",
        "source": "airport_in_name_or_city"
      }
    ]
  },
  {
    "record_id": "rec0pT…5MKA",
    "identity_key": "ind_ihg_mx_tijgc",
    "property_name": "Hotel Indigo Tijuana Downtown",
    "before": {
      "Amenities - Source Text": null,
      "Amenities - Structured Tags": null,
      "F&B Flag": null,
      "Meeting Space Flag": null,
      "Resort / Leisure Flag": null,
      "Property Type": null,
      "Radar Display Status": null,
      "Radar Display Reason": null,
      "Radar Geography Status": null,
      "Public Census Eligibility": null,
      "Public Display Confidence": null,
      "Public Display Review Status": null,
      "Enrichment Status": "Not Started",
      "Enrichment Priority": "Medium",
      "Last Reviewed Date": null
    },
    "after": {
      "Amenities - Source Text": "Pool; Spa / Wellness; Fitness; Restaurant / F&B; Bar / Lounge; Meeting / Events; Parking; Wi-Fi; Pet Friendly; Business Center",
      "Amenities - Structured Tags": "Pool\nSpa / Wellness\nFitness\nRestaurant / F&B\nBar / Lounge\nMeeting / Events\nParking\nWi-Fi\nPet Friendly\nBusiness Center",
      "F&B Flag": true,
      "Meeting Space Flag": true,
      "Resort / Leisure Flag": true,
      "Property Type": "Resort",
      "Radar Display Status": "Public List Eligible",
      "Radar Display Reason": "List-eligible; city/country/source present; coordinates missing or not property-level",
      "Radar Geography Status": "City-Level Only",
      "Public Census Eligibility": "Eligible With Limits",
      "Public Display Confidence": "Medium",
      "Public Display Review Status": "Auto-Classified",
      "Enrichment Status": "Partial",
      "Enrichment Priority": "Medium",
      "Last Reviewed Date": "2026-08-05"
    },
    "sources": [
      {
        "fields": [
          "Amenities - Source Text",
          "Amenities - Structured Tags"
        ],
        "source_url": "https://www.ihg.com/hotelindigo/hotels/us/en/tijuana/tijgc/hoteldetail",
        "confidence": "Medium",
        "source": "IHG hoteldetail explicit amenity mentions (Yes only)"
      },
      {
        "fields": [
          "F&B Flag",
          "Meeting Space Flag",
          "Resort / Leisure Flag"
        ],
        "source_url": "https://www.ihg.com/hotelindigo/hotels/us/en/tijuana/tijgc/hoteldetail",
        "confidence": "Medium",
        "source": "amenity_text_and_brand_evidence"
      },
      {
        "fields": [
          "Property Type"
        ],
        "source_url": "https://www.ihg.com/hotelindigo/hotels/us/en/tijuana/tijgc/hoteldetail",
        "confidence": "Medium",
        "source": "resort_signal"
      }
    ]
  }
]
```
