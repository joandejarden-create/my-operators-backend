# Production Census Coordinate Resolver — Dry Run

**Status:** `production_census_coordinate_resolver_needs_code_improvement`  
**Generated:** 2026-08-05T14:15:44.434Z  
**Apply executed:** false

## 1. Executive summary

- Scanned: **666**
- Already valid coordinates: **132**
- Active missing: **293**
- Proposed updates (dry-run only): **0**
- Steward review: **25**
- Blocked (held / brand-unconfirmed / not active): see blocked_sample
- Fetch-deferred candidates: **268**
- Pages fetched: **25**
- Exact Airtable update count if applied: **0**
- Webhound production writes: **0**

## 2–5. Webhound sidecar + learnings

See `reports/research-engine-v2/webhound-coordinate-learning-sidecar-closed.md`. Production writes from Webhound = **0**.

## 6. Coordinate resolver method

Order: Census Source/Official URL → fetch official page → JSON-LD / family payloads / map embeds / Marriott HQV GraphQL → optional official-address geocode → validate → High/Medium propose, Low → steward.

```json
[
  {
    "family": "Marriott",
    "page_types": [
      "mexico hotel sitemap (MARSHA seed)",
      "GraphQL phoenixShopHQVPropertyInfoCall (preferred)",
      "overview HTML (usually negative for coords)"
    ],
    "patterns": [
      "data.property.basicInformation.latitude/longitude via HQV",
      "sitemap /hotels/([A-Z0-9]{5})- MARSHA",
      "__NEXT_DATA__.props.pageProps.operationSignatures[] → MARRIOTT_GRAPHQL_OPERATION_SIGNATURE",
      "optional JSON-LD geo (rarely present on Mexico overview)"
    ],
    "reproducible_without_webhound": true,
    "becomes_crawler_rule": true,
    "steward_if": "Akamai blocks HQV or signature missing",
    "learning_source": "webhound_sidecar_bbaa85f9",
    "env_required": [
      "MARRIOTT_GRAPHQL_OPERATION_SIGNATURE (optional but usually required)"
    ]
  },
  {
    "family": "Hilton",
    "page_types": [
      "hilton.com/en/hotels/{ctyhocn}-.../",
      "locations directory GraphQL"
    ],
    "patterns": [
      "localization.coordinate.latitude/longitude",
      "JSON-LD geo"
    ],
    "reproducible_without_webhound": true,
    "becomes_crawler_rule": true
  },
  {
    "family": "Choice",
    "page_types": [
      "choicehotels.com regional hotel cards",
      "property pages"
    ],
    "patterns": [
      "\"geoLocation\":{\"latitude\":n,\"longitude\":n}"
    ],
    "reproducible_without_webhound": true,
    "becomes_crawler_rule": true
  },
  {
    "family": "IHG",
    "page_types": [
      "ihg.com/.../hoteldetail"
    ],
    "patterns": [
      "hoteldetail latitude/longitude JSON",
      "JSON-LD geo",
      "map embed"
    ],
    "reproducible_without_webhound": true,
    "becomes_crawler_rule": true
  }
]
```

## 7. First-pass coordinate validation

(No first-pass coordinates modified in this task.)

```json
{
  "coordinates_present": 132,
  "safe_count": 132,
  "needs_review_count": 0,
  "downgrade_later_count": 4,
  "public_map_eligible_count": 132,
  "public_map_missing_coords": 0,
  "zero_zero": 0,
  "held_with_coords": 0,
  "pass": true,
  "code_gaps": [
    "Marriott VIC freeze lat/lng are null — production path is GraphQL HQV (phoenixShopHQVPropertyInfoCall) after MARSHA from sitemap/URL; overview HTML usually has no coords.",
    "Marriott HQV currently needs MARRIOTT_GRAPHQL_OPERATION_SIGNATURE (harvest from search __NEXT_DATA__) and may hit Akamai without browser/XHR.",
    "IHG freeze has almost no lat/lng — hoteldetail HTML/JSON lane needed; current sample fetches returned official_page_blocked.",
    "Shared campus pins remain Medium confidence; optional later steward downgrade of Public Display Confidence only (no coord rewrite in this task).",
    "Official-address geocode path requires GOOGLE_MAPS_API_KEY and street-level address (often blank on Census)."
  ],
  "needs_review": [],
  "downgrade_later": [
    {
      "record_id": "recR3o…faLf",
      "identity_key": "ind_hilton_mx_safgigi",
      "property_name": "Hilton Garden Inn Mexico City Santa Fe",
      "lat": 19.3652911176,
      "lng": -99.2625657482,
      "radar": "Public Map Eligible",
      "held": false,
      "source_support": true,
      "source": "Hilton directory localization.coordinate",
      "source_url": "https://www.hilton.com/en/locations/mexico/hilton-garden-inn/",
      "shared_campus_pin": true,
      "reason": "shared_campus_pin_medium_confidence"
    },
    {
      "record_id": "recZdz…wnqb",
      "identity_key": "ind_hilton_mx_mexstdt",
      "property_name": "DoubleTree by Hilton Hotel México City Santa Fe",
      "lat": 19.3652911176,
      "lng": -99.2625657482,
      "radar": "Public Map Eligible",
      "held": false,
      "source_support": true,
      "source": "Hilton directory localization.coordinate",
      "source_url": "https://www.hilton.com/en/locations/mexico/doubletree-by-hilton/",
      "shared_campus_pin": true,
      "reason": "shared_campus_pin_medium_confidence"
    },
    {
      "record_id": "recbxG…uGvk",
      "identity_key": "ind_hilton_mx_slwruru",
      "property_name": "Tru by Hilton Saltillo",
      "lat": 25.46152,
      "lng": -100.98124,
      "radar": "Public Map Eligible",
      "held": false,
      "source_support": true,
      "source": "Hilton directory localization.coordinate",
      "source_url": "https://www.hilton.com/en/locations/mexico/tru-by-hilton/",
      "shared_campus_pin": true,
      "reason": "shared_campus_pin_medium_confidence"
    },
    {
      "record_id": "reccxy…KpBd",
      "identity_key": "ind_hilton_mx_slwsmhw",
      "property_name": "Homewood Suites by Hilton Saltillo",
      "lat": 25.46152,
      "lng": -100.98124,
      "radar": "Public Map Eligible",
      "held": false,
      "source_support": true,
      "source": "Hilton directory localization.coordinate",
      "source_url": "https://www.hilton.com/en/locations/mexico/homewood-suites/",
      "shared_campus_pin": true,
      "reason": "shared_campus_pin_medium_confidence"
    }
  ]
}
```

## 8–11. Next-lane dry-run

### Proposed (sample)

```json
[]
```

### Steward review (sample)

```json
[
  {
    "record_id": "rec02w…jPjx",
    "identity_key": "ind_marriott_mx_bjxds",
    "property_name": "Elena de Cobre, a Member of Design Hotels™",
    "family": "Marriott",
    "reason": "official_page_blocked",
    "source_url": "https://www.marriott.com/en-us/hotels/bjxds-elena-de-cobre-a-member-of-design-hotels/overview"
  },
  {
    "record_id": "rec0H6…St9L",
    "identity_key": "ind_marriott_mx_cenxo",
    "property_name": "City Express by Marriott Ciudad Obregón",
    "family": "Marriott",
    "reason": "official_page_blocked",
    "source_url": "https://www.marriott.com/en-us/hotels/cenxo-city-express-ciudad-obregon/overview"
  },
  {
    "record_id": "rec0Rc…rrqD",
    "identity_key": "ind_marriott_mx_vsacy",
    "property_name": "Courtyard by Marriott Villahermosa Tabasco",
    "family": "Marriott",
    "reason": "official_page_blocked",
    "source_url": "https://www.marriott.com/en-us/hotels/vsacy-courtyard-villahermosa-tabasco/overview"
  },
  {
    "record_id": "rec0dx…O1nZ",
    "identity_key": "ind_marriott_mx_nogxn",
    "property_name": "City Express by Marriott Nogales",
    "family": "Marriott",
    "reason": "official_page_blocked",
    "source_url": "https://www.marriott.com/en-us/hotels/nogxn-city-express-nogales/overview"
  },
  {
    "record_id": "rec0pT…5MKA",
    "identity_key": "ind_ihg_mx_tijgc",
    "property_name": "Hotel Indigo Tijuana Downtown",
    "family": "IHG",
    "reason": "official_page_blocked",
    "source_url": "https://www.ihg.com/hotelindigo/hotels/us/en/tijuana/tijgc/hoteldetail"
  },
  {
    "record_id": "rec1Sn…zOfR",
    "identity_key": "ind_marriott_mx_gdlar",
    "property_name": "AC Hotel Guadalajara Expo, Mexico",
    "family": "Marriott",
    "reason": "official_page_blocked",
    "source_url": "https://www.marriott.com/en-us/hotels/gdlar-ac-hotel-guadalajara-expo-mexico/overview"
  },
  {
    "record_id": "rec1f0…A50p",
    "identity_key": "ind_marriott_mx_pbcds",
    "property_name": "La Purificadora, Puebla, a Member of Design Hotels™",
    "family": "Marriott",
    "reason": "official_page_blocked",
    "source_url": "https://www.marriott.com/en-us/hotels/pbcds-la-purificadora-puebla-a-member-of-design-hotels/overview"
  },
  {
    "record_id": "rec1o8…eEFe",
    "identity_key": "ind_marriott_mx_mxlcy",
    "property_name": "Courtyard by Marriott Mexicali",
    "family": "Marriott",
    "reason": "official_page_blocked",
    "source_url": "https://www.marriott.com/en-us/hotels/mxlcy-courtyard-mexicali/overview"
  },
  {
    "record_id": "rec22I…bWAS",
    "identity_key": "ind_marriott_mx_bjxxl",
    "property_name": "City Express by Marriott Leon",
    "family": "Marriott",
    "reason": "official_page_blocked",
    "source_url": "https://www.marriott.com/en-us/hotels/bjxxl-city-express-leon/overview"
  },
  {
    "record_id": "rec2UD…Dwq9",
    "identity_key": "ind_marriott_mx_bjxxn",
    "property_name": "City Express by Marriott Irapuato Norte",
    "family": "Marriott",
    "reason": "official_page_blocked",
    "source_url": "https://www.marriott.com/en-us/hotels/bjxxn-city-express-irapuato-norte/overview"
  },
  {
    "record_id": "rec2XQ…APEn",
    "identity_key": "ind_marriott_mx_slpac",
    "property_name": "AC Hotel By Marriott San Luis Potosi",
    "family": "Marriott",
    "reason": "official_page_blocked",
    "source_url": "https://www.marriott.com/en-us/hotels/slpac-ac-hotel-san-luis-potosi/overview"
  },
  {
    "record_id": "rec2ua…2cuA",
    "identity_key": "ind_marriott_mx_mtyxt",
    "property_name": "City Express by Marriott Monterrey Lindavista",
    "family": "Marriott",
    "reason": "official_page_blocked",
    "source_url": "https://www.marriott.com/en-us/hotels/mtyxt-city-express-monterrey-lindavista/overview"
  },
  {
    "record_id": "rec30e…QKhJ",
    "identity_key": "ind_marriott_mx_pxmtr",
    "property_name": "Terrestre, a Member of Design Hotels™",
    "family": "Marriott",
    "reason": "official_page_blocked",
    "source_url": "https://www.marriott.com/en-us/hotels/pxmtr-terrestre-a-member-of-design-hotels/overview"
  },
  {
    "record_id": "rec3Nc…LaeR",
    "identity_key": "ind_ihg_mx_mexan",
    "property_name": "Holiday Inn Express Mexico City - Toreo",
    "family": "IHG",
    "reason": "official_page_blocked",
    "source_url": "https://www.ihg.com/holidayinnexpress/hotels/us/en/naucalpan/mexan/hoteldetail"
  },
  {
    "record_id": "rec3Qg…Vw6c",
    "identity_key": "ind_marriott_mx_lapbc",
    "property_name": "Baja Club Hotel, La Paz, Baja California Sur, a Member of Design Hotels™",
    "family": "Marriott",
    "reason": "official_page_blocked",
    "source_url": "https://www.marriott.com/en-us/hotels/lapbc-baja-club-hotel-la-paz-baja-california-sur-a-member-of-design-hotels/overview"
  },
  {
    "record_id": "rec3Tj…r2lz",
    "identity_key": "ind_marriott_mx_slpxi",
    "property_name": "City Express by Marriott San Luis Potosí Zona Industrial",
    "family": "Marriott",
    "reason": "official_page_blocked",
    "source_url": "https://www.marriott.com/en-us/hotels/slpxi-city-express-san-luis-potosi-zona-industrial/overview"
  },
  {
    "record_id": "rec3Wv…Hk7A",
    "identity_key": "ind_marriott_mx_pbcxc",
    "property_name": "City Express by Marriott Puebla Centro",
    "family": "Marriott",
    "reason": "official_page_blocked",
    "source_url": "https://www.marriott.com/en-us/hotels/pbcxc-city-express-puebla-centro/overview"
  },
  {
    "record_id": "rec3r0…30uk",
    "identity_key": "ind_marriott_mx_tlcxt",
    "property_name": "City Express by Marriott Toluca",
    "family": "Marriott",
    "reason": "official_page_blocked",
    "source_url": "https://www.marriott.com/en-us/hotels/tlcxt-city-express-toluca/overview"
  },
  {
    "record_id": "rec430…ylLe",
    "identity_key": "ind_marriott_mx_mtycy",
    "property_name": "Courtyard by Marriott Monterrey Airport",
    "family": "Marriott",
    "reason": "official_page_blocked",
    "source_url": "https://www.marriott.com/en-us/hotels/mtycy-courtyard-monterrey-airport/overview"
  },
  {
    "record_id": "rec432…9CAv",
    "identity_key": "ind_marriott_mx_cunan",
    "property_name": "Casa Nizuc, a Tribute Portfolio Resort",
    "family": "Marriott",
    "reason": "official_page_blocked",
    "source_url": "https://www.marriott.com/en-us/hotels/cunan-casa-nizuc-a-tribute-portfolio-resort/overview"
  },
  {
    "record_id": "rec4HI…ZCbT",
    "identity_key": "ind_ihg_mx_mexca",
    "property_name": "Holiday Inn Express Mexico City Satelite",
    "family": "IHG",
    "reason": "official_page_blocked",
    "source_url": "https://www.ihg.com/holidayinnexpress/hotels/us/en/mexico-city/mexca/hoteldetail"
  },
  {
    "record_id": "rec4IM…IHAu",
    "identity_key": "ind_marriott_mx_qroqa",
    "property_name": "AC Hotel Queretaro Antea",
    "family": "Marriott",
    "reason": "official_page_blocked",
    "source_url": "https://www.marriott.com/en-us/hotels/qroqa-ac-hotel-queretaro-antea/overview"
  },
  {
    "record_id": "rec4Tk…ggnD",
    "identity_key": "ind_ihg_mx_cuugt",
    "property_name": "Holiday Inn Express & Suites Chihuahua Juventud",
    "family": "IHG",
    "reason": "official_page_blocked",
    "source_url": "https://www.ihg.com/holidayinnexpress/hotels/us/en/chihuahua/cuugt/hoteldetail"
  },
  {
    "record_id": "rec4UM…Ovpp",
    "identity_key": "ind_marriott_mx_pazxt",
    "property_name": "City Express by Marriott Tuxpan",
    "family": "Marriott",
    "reason": "official_page_blocked",
    "source_url": "https://www.marriott.com/en-us/hotels/pazxt-city-express-tuxpan/overview"
  },
  {
    "record_id": "rec51O…tqZy",
    "identity_key": "ind_marriott_mx_hmocy",
    "property_name": "Courtyard by Marriott Hermosillo",
    "family": "Marriott",
    "reason": "official_page_blocked",
    "source_url": "https://www.marriott.com/en-us/hotels/hmocy-courtyard-hermosillo/overview"
  }
]
```

### Blocked (sample)

```json
[]
```

## 12. Fields not touched

- Owner Name
- Developer Name
- Operator / Management Company
- Rooms / Keys
- Opening Date
- Renovation / Conversion Date
- Affiliation Start Date
- Company Validated
- Brand Verified
- Recent Momentum
- Brand Explorer fields
- (no Airtable writes in this dry-run)

## 13. Brand Explorer safety

No Brand Explorer files, fixtures, or Airtable Brand Explorer fields were written. Protected Active 62 / PVQL / semantic / momentum gates were not re-run because this lane is Census-only and made zero BE changes.

## 14. Recommended next step

Marriott/IHG official pages blocked (Akamai). Next code improvement: harvest GraphQL operation signature from a rendered Marriott search page (__NEXT_DATA__.props.pageProps.operationSignatures for phoenixShopHQVPropertyInfoCall), set MARRIOTT_GRAPHQL_OPERATION_SIGNATURE, retry HQV dry-run. Do not restart Webhound for full-census coordinates.
