# Production Census Population Lane 2 — Dry Run

**Status:** `production_census_population_lane_2_dry_run_ready_for_founder_review`  
**Generated:** 2026-08-05T17:01:44.964Z  
**Apply executed:** false

## Executive summary

| Metric | Value |
| --- | ---: |
| Scanned | 666 |
| Eligible | 425 |
| Blocked | 241 |
| Provenance backfills | 132 |
| Provenance unclear (blank) | 0 |
| Descriptions | 0 |
| Amenities | 0 |
| Property type | 45 |
| Asset context | 0 |
| Market/Submarket | 0 |
| Strategic flags | 0 |
| Exact updates if applied | 177 |
| Geocode proposals | 34 (blocked: true) |

## Geocode lane (34)

```json
{
  "count": 34,
  "ready_but_blocked": true,
  "provider_decision": {
    "provider_info": {
      "provider": "google",
      "reason": "explicit_google",
      "credentials_ok": true,
      "key_present": true,
      "storage_terms_reviewed": false
    },
    "approved_for_coordinate_apply": false,
    "block_reason": "provider_or_storage_terms_not_confirmed",
    "recommended": "Prefer Mapbox Permanent (MAPBOX_ACCESS_TOKEN + MAPBOX_PERMANENT_GEOCODING=1). Google only if GOOGLE_GEOCODE_STORAGE_TERMS_REVIEWED=1."
  },
  "note": "34 High/Medium proposals remain blocked until Mapbox Permanent or Google storage terms confirmed",
  "sample": [
    {
      "record_id": "rec0pT…5MKA",
      "identity_key": "ind_ihg_mx_tijgc",
      "property_name": "Hotel Indigo Tijuana Downtown",
      "brand": "Hotel Indigo",
      "family": "IHG",
      "latitude": 32.5326077,
      "longitude": -117.0370202,
      "official_address": "Calle Salvador Diaz Miron 4ta-8177",
      "address_source_url": "https://www.ihg.com/hotelindigo/hotels/us/en/tijuana/tijgc/hoteldetail",
      "coordinate_source_type": "official_address_geocode",
      "confidence": "High",
      "geocode_provider": "google",
      "geocode_method": "google_geocode_official_address",
      "storage_terms_ok_for_apply": false,
      "fields_if_applied": [
        "Latitude",
        "Longitude",
        "Address",
        "Radar Geography Status",
        "Radar Display Status",
        "Radar Display Reason",
        "Public Census Eligibility",
        "Public Display Confidence",
        "Public Display Review Status",
        "Last Reviewed Date"
      ]
    },
    {
      "record_id": "rec3Nc…LaeR",
      "identity_key": "ind_ihg_mx_mexan",
      "property_name": "Holiday Inn Express Mexico City - Toreo",
      "brand": "Holiday Inn Express",
      "family": "IHG",
      "latitude": 19.4657687,
      "longitude": -99.2300351,
      "official_address": "Av. Primero de Mayo 226",
      "address_source_url": "https://www.ihg.com/holidayinnexpress/hotels/us/en/naucalpan/mexan/hoteldetail",
      "coordinate_source_type": "official_address_geocode",
      "confidence": "High",
      "geocode_provider": "google",
      "geocode_method": "google_geocode_official_address",
      "storage_terms_ok_for_apply": false,
      "fields_if_applied": [
        "Latitude",
        "Longitude",
        "Address",
        "Radar Geography Status",
        "Radar Display Status",
        "Radar Display Reason",
        "Public Census Eligibility",
        "Public Display Confidence",
        "Public Display Review Status",
        "Last Reviewed Date"
      ]
    },
    {
      "record_id": "rec4Tk…ggnD",
      "identity_key": "ind_ihg_mx_cuugt",
      "property_name": "Holiday Inn Express & Suites Chihuahua Juventud",
      "brand": "Holiday Inn Express",
      "family": "IHG",
      "latitude": 28.6272774,
      "longitude": -106.119417,
      "official_address": "PASEO VISTAS DEL SOL 6403",
      "address_source_url": "https://www.ihg.com/holidayinnexpress/hotels/us/en/chihuahua/cuugt/hoteldetail",
      "coordinate_source_type": "official_address_geocode",
      "confidence": "High",
      "geocode_provider": "google",
      "geocode_method": "google_geocode_official_address",
      "storage_terms_ok_for_apply": false,
      "fields_if_applied": [
        "Latitude",
        "Longitude",
        "Address",
        "Radar Geography Status",
        "Radar Display Status",
        "Radar Display Reason",
        "Public Census Eligibility",
        "Public Display Confidence",
        "Public Display Review Status",
        "Last Reviewed Date"
      ]
    },
    {
      "record_id": "rec6TQ…NIlx",
      "identity_key": "ind_ihg_mx_tappf",
      "property_name": "Holiday Inn Express Tapachula",
      "brand": "Holiday Inn Express",
      "family": "IHG",
      "latitude": 14.87476,
      "longitude": -92.2842895,
      "official_address": "Carretera A Puerto Madero KM 3.5",
      "address_source_url": "https://www.ihg.com/holidayinnexpress/hotels/us/en/tapachula/tappf/hoteldetail",
      "coordinate_source_type": "official_address_geocode",
      "confidence": "High",
      "geocode_provider": "google",
      "geocode_method": "google_geocode_official_address",
      "storage_terms_ok_for_apply": false,
      "fields_if_applied": [
        "Latitude",
        "Longitude",
        "Address",
        "Radar Geography Status",
        "Radar Display Status",
        "Radar Display Reason",
        "Public Census Eligibility",
        "Public Display Confidence",
        "Public Display Review Status",
        "Last Reviewed Date"
      ]
    },
    {
      "record_id": "rec7jj…1Z86",
      "identity_key": "ind_ihg_mx_mlmra",
      "property_name": "Holiday Inn Express Morelia",
      "brand": "Holiday Inn Express",
      "family": "IHG",
      "latitude": 19.687166,
      "longitude": -101.1570039,
      "official_address": "Periferico Paseo de la Republica No. 5000",
      "address_source_url": "https://www.ihg.com/holidayinnexpress/hotels/us/en/morelia/mlmra/hoteldetail",
      "coordinate_source_type": "official_address_geocode",
      "confidence": "High",
      "geocode_provider": "google",
      "geocode_method": "google_geocode_official_address",
      "storage_terms_ok_for_apply": false,
      "fields_if_applied": [
        "Latitude",
        "Longitude",
        "Address",
        "Radar Geography Status",
        "Radar Display Status",
        "Radar Display Reason",
        "Public Census Eligibility",
        "Public Display Confidence",
        "Public Display Review Status",
        "Last Reviewed Date"
      ]
    }
  ],
  "exact_airtable_update_count_if_geocode_applied": 0
}
```

## Sample before/after

```json
[
  {
    "record_id": "rec02w…jPjx",
    "identity_key": "ind_marriott_mx_bjxds",
    "property_name": "Elena de Cobre, a Member of Design Hotels™",
    "lanes": [
      "property_type"
    ],
    "before": {
      "Property Type": null,
      "Enrichment Status": "In Progress",
      "Enrichment Priority": "Medium",
      "Last Reviewed Date": "2026-08-05"
    },
    "after": {
      "Property Type": "Boutique Hotel",
      "Enrichment Status": "Partial",
      "Enrichment Priority": "Medium",
      "Last Reviewed Date": "2026-08-05"
    }
  },
  {
    "record_id": "rec0Dl…HAYh",
    "identity_key": "ind_hilton_mx_cywcedt",
    "property_name": "DoubleTree by Hilton Celaya",
    "lanes": [
      "provenance_backfill"
    ],
    "before": {
      "Coordinate Source Type": null,
      "Coordinate Confidence": null,
      "Geocode Provider": null,
      "Geocode Method": null,
      "Geocode Reviewed Date": null,
      "Last Reviewed Date": "2026-08-05",
      "Enrichment Status": "Partial",
      "Enrichment Priority": "Medium"
    },
    "after": {
      "Coordinate Source Type": "structured_data_extraction",
      "Coordinate Confidence": "High",
      "Geocode Provider": "Existing Source",
      "Geocode Method": "structured_data_extraction",
      "Geocode Reviewed Date": "2026-08-05",
      "Last Reviewed Date": "2026-08-05",
      "Enrichment Status": "Partial",
      "Enrichment Priority": "Medium"
    }
  },
  {
    "record_id": "rec0Iq…vstj",
    "identity_key": "ind_hilton_mx_cunmelx",
    "property_name": "Mezzanine, an SLH Hotel",
    "lanes": [
      "provenance_backfill"
    ],
    "before": {
      "Coordinate Source Type": null,
      "Coordinate Confidence": null,
      "Geocode Provider": null,
      "Geocode Method": null,
      "Geocode Reviewed Date": null,
      "Last Reviewed Date": "2026-08-05",
      "Enrichment Status": "Partial",
      "Enrichment Priority": "Medium"
    },
    "after": {
      "Coordinate Source Type": "structured_data_extraction",
      "Coordinate Confidence": "High",
      "Geocode Provider": "Existing Source",
      "Geocode Method": "structured_data_extraction",
      "Geocode Reviewed Date": "2026-08-05",
      "Last Reviewed Date": "2026-08-05",
      "Enrichment Status": "Partial",
      "Enrichment Priority": "Medium"
    }
  },
  {
    "record_id": "rec0kS…q3cG",
    "identity_key": "ind_hilton_mx_qrohwhw",
    "property_name": "Homewood Suites by Hilton Queretaro",
    "lanes": [
      "provenance_backfill"
    ],
    "before": {
      "Coordinate Source Type": null,
      "Coordinate Confidence": null,
      "Geocode Provider": null,
      "Geocode Method": null,
      "Geocode Reviewed Date": null,
      "Last Reviewed Date": "2026-08-05",
      "Enrichment Status": "Partial",
      "Enrichment Priority": "Medium"
    },
    "after": {
      "Coordinate Source Type": "structured_data_extraction",
      "Coordinate Confidence": "High",
      "Geocode Provider": "Existing Source",
      "Geocode Method": "structured_data_extraction",
      "Geocode Reviewed Date": "2026-08-05",
      "Last Reviewed Date": "2026-08-05",
      "Enrichment Status": "Partial",
      "Enrichment Priority": "Medium"
    }
  },
  {
    "record_id": "rec0zU…Z6cE",
    "identity_key": "ind_hilton_mx_mexsahh",
    "property_name": "Hilton Mexico City Santa Fe",
    "lanes": [
      "provenance_backfill"
    ],
    "before": {
      "Coordinate Source Type": null,
      "Coordinate Confidence": null,
      "Geocode Provider": null,
      "Geocode Method": null,
      "Geocode Reviewed Date": null,
      "Last Reviewed Date": "2026-08-05",
      "Enrichment Status": "Partial",
      "Enrichment Priority": "Medium"
    },
    "after": {
      "Coordinate Source Type": "structured_data_extraction",
      "Coordinate Confidence": "High",
      "Geocode Provider": "Existing Source",
      "Geocode Method": "structured_data_extraction",
      "Geocode Reviewed Date": "2026-08-05",
      "Last Reviewed Date": "2026-08-05",
      "Enrichment Status": "Partial",
      "Enrichment Priority": "Medium"
    }
  },
  {
    "record_id": "rec1f0…A50p",
    "identity_key": "ind_marriott_mx_pbcds",
    "property_name": "La Purificadora, Puebla, a Member of Design Hotels™",
    "lanes": [
      "property_type"
    ],
    "before": {
      "Property Type": null,
      "Enrichment Status": "Partial",
      "Enrichment Priority": "Medium",
      "Last Reviewed Date": "2026-08-05"
    },
    "after": {
      "Property Type": "Boutique Hotel",
      "Enrichment Status": "Partial",
      "Enrichment Priority": "Medium",
      "Last Reviewed Date": "2026-08-05"
    }
  },
  {
    "record_id": "rec1mZ…gvI3",
    "identity_key": "ind_hilton_mx_upnmtgi",
    "property_name": "Hilton Garden Inn Uruapan",
    "lanes": [
      "provenance_backfill"
    ],
    "before": {
      "Coordinate Source Type": null,
      "Coordinate Confidence": null,
      "Geocode Provider": null,
      "Geocode Method": null,
      "Geocode Reviewed Date": null,
      "Last Reviewed Date": "2026-08-05",
      "Enrichment Status": "Partial",
      "Enrichment Priority": "Medium"
    },
    "after": {
      "Coordinate Source Type": "structured_data_extraction",
      "Coordinate Confidence": "High",
      "Geocode Provider": "Existing Source",
      "Geocode Method": "structured_data_extraction",
      "Geocode Reviewed Date": "2026-08-05",
      "Last Reviewed Date": "2026-08-05",
      "Enrichment Status": "Partial",
      "Enrichment Priority": "Medium"
    }
  },
  {
    "record_id": "rec2iv…etaa",
    "identity_key": "ind_hilton_mx_qrojshh",
    "property_name": "Hilton Queretaro",
    "lanes": [
      "provenance_backfill"
    ],
    "before": {
      "Coordinate Source Type": null,
      "Coordinate Confidence": null,
      "Geocode Provider": null,
      "Geocode Method": null,
      "Geocode Reviewed Date": null,
      "Last Reviewed Date": "2026-08-05",
      "Enrichment Status": "Partial",
      "Enrichment Priority": "Medium"
    },
    "after": {
      "Coordinate Source Type": "structured_data_extraction",
      "Coordinate Confidence": "High",
      "Geocode Provider": "Existing Source",
      "Geocode Method": "structured_data_extraction",
      "Geocode Reviewed Date": "2026-08-05",
      "Last Reviewed Date": "2026-08-05",
      "Enrichment Status": "Partial",
      "Enrichment Priority": "Medium"
    }
  }
]
```

## Provenance sample

```json
[
  {
    "record_id": "rec0Dl…HAYh",
    "identity_key": "ind_hilton_mx_cywcedt",
    "property_name": "DoubleTree by Hilton Celaya",
    "patch": {
      "Coordinate Source Type": "structured_data_extraction",
      "Coordinate Confidence": "High",
      "Geocode Provider": "Existing Source",
      "Geocode Method": "structured_data_extraction",
      "Geocode Reviewed Date": "2026-08-05",
      "Last Reviewed Date": "2026-08-05",
      "Enrichment Status": "Partial",
      "Enrichment Priority": "Medium"
    },
    "sources": [
      {
        "lane": "provenance_backfill",
        "source_url": "https://www.hilton.com/en/locations/mexico/doubletree-by-hilton/",
        "source": "Hilton directory localization.coordinate",
        "fields": [
          "Coordinate Source Type",
          "Coordinate Confidence",
          "Geocode Provider",
          "Geocode Method",
          "Geocode Reviewed Date",
          "Last Reviewed Date"
        ]
      }
    ]
  },
  {
    "record_id": "rec0Iq…vstj",
    "identity_key": "ind_hilton_mx_cunmelx",
    "property_name": "Mezzanine, an SLH Hotel",
    "patch": {
      "Coordinate Source Type": "structured_data_extraction",
      "Coordinate Confidence": "High",
      "Geocode Provider": "Existing Source",
      "Geocode Method": "structured_data_extraction",
      "Geocode Reviewed Date": "2026-08-05",
      "Last Reviewed Date": "2026-08-05",
      "Enrichment Status": "Partial",
      "Enrichment Priority": "Medium"
    },
    "sources": [
      {
        "lane": "provenance_backfill",
        "source_url": "https://www.hilton.com/en/locations/mexico/small-luxury-hotels-slh/",
        "source": "Hilton directory localization.coordinate",
        "fields": [
          "Coordinate Source Type",
          "Coordinate Confidence",
          "Geocode Provider",
          "Geocode Method",
          "Geocode Reviewed Date",
          "Last Reviewed Date"
        ]
      }
    ]
  },
  {
    "record_id": "rec0kS…q3cG",
    "identity_key": "ind_hilton_mx_qrohwhw",
    "property_name": "Homewood Suites by Hilton Queretaro",
    "patch": {
      "Coordinate Source Type": "structured_data_extraction",
      "Coordinate Confidence": "High",
      "Geocode Provider": "Existing Source",
      "Geocode Method": "structured_data_extraction",
      "Geocode Reviewed Date": "2026-08-05",
      "Last Reviewed Date": "2026-08-05",
      "Enrichment Status": "Partial",
      "Enrichment Priority": "Medium"
    },
    "sources": [
      {
        "lane": "provenance_backfill",
        "source_url": "https://www.hilton.com/en/locations/mexico/homewood-suites/",
        "source": "Hilton directory localization.coordinate",
        "fields": [
          "Coordinate Source Type",
          "Coordinate Confidence",
          "Geocode Provider",
          "Geocode Method",
          "Geocode Reviewed Date",
          "Last Reviewed Date"
        ]
      }
    ]
  },
  {
    "record_id": "rec0zU…Z6cE",
    "identity_key": "ind_hilton_mx_mexsahh",
    "property_name": "Hilton Mexico City Santa Fe",
    "patch": {
      "Coordinate Source Type": "structured_data_extraction",
      "Coordinate Confidence": "High",
      "Geocode Provider": "Existing Source",
      "Geocode Method": "structured_data_extraction",
      "Geocode Reviewed Date": "2026-08-05",
      "Last Reviewed Date": "2026-08-05",
      "Enrichment Status": "Partial",
      "Enrichment Priority": "Medium"
    },
    "sources": [
      {
        "lane": "provenance_backfill",
        "source_url": "https://www.hilton.com/en/locations/mexico/hilton-hotels/",
        "source": "Hilton directory localization.coordinate",
        "fields": [
          "Coordinate Source Type",
          "Coordinate Confidence",
          "Geocode Provider",
          "Geocode Method",
          "Geocode Reviewed Date",
          "Last Reviewed Date"
        ]
      }
    ]
  },
  {
    "record_id": "rec1mZ…gvI3",
    "identity_key": "ind_hilton_mx_upnmtgi",
    "property_name": "Hilton Garden Inn Uruapan",
    "patch": {
      "Coordinate Source Type": "structured_data_extraction",
      "Coordinate Confidence": "High",
      "Geocode Provider": "Existing Source",
      "Geocode Method": "structured_data_extraction",
      "Geocode Reviewed Date": "2026-08-05",
      "Last Reviewed Date": "2026-08-05",
      "Enrichment Status": "Partial",
      "Enrichment Priority": "Medium"
    },
    "sources": [
      {
        "lane": "provenance_backfill",
        "source_url": "https://www.hilton.com/en/locations/mexico/hilton-garden-inn/",
        "source": "Hilton directory localization.coordinate",
        "fields": [
          "Coordinate Source Type",
          "Coordinate Confidence",
          "Geocode Provider",
          "Geocode Method",
          "Geocode Reviewed Date",
          "Last Reviewed Date"
        ]
      }
    ]
  },
  {
    "record_id": "rec2iv…etaa",
    "identity_key": "ind_hilton_mx_qrojshh",
    "property_name": "Hilton Queretaro",
    "patch": {
      "Coordinate Source Type": "structured_data_extraction",
      "Coordinate Confidence": "High",
      "Geocode Provider": "Existing Source",
      "Geocode Method": "structured_data_extraction",
      "Geocode Reviewed Date": "2026-08-05",
      "Last Reviewed Date": "2026-08-05",
      "Enrichment Status": "Partial",
      "Enrichment Priority": "Medium"
    },
    "sources": [
      {
        "lane": "provenance_backfill",
        "source_url": "https://www.hilton.com/en/locations/mexico/hilton-hotels/",
        "source": "Hilton directory localization.coordinate",
        "fields": [
          "Coordinate Source Type",
          "Coordinate Confidence",
          "Geocode Provider",
          "Geocode Method",
          "Geocode Reviewed Date",
          "Last Reviewed Date"
        ]
      }
    ]
  },
  {
    "record_id": "rec3Gp…yEoT",
    "identity_key": "ind_choice_mx_mx165",
    "property_name": "El Cid El Moro Beach Hotel",
    "patch": {
      "Coordinate Source Type": "structured_data_extraction",
      "Coordinate Confidence": "High",
      "Geocode Provider": "Existing Source",
      "Geocode Method": "structured_data_extraction",
      "Geocode Reviewed Date": "2026-08-05",
      "Last Reviewed Date": "2026-08-05",
      "Enrichment Status": "Partial",
      "Enrichment Priority": "Medium"
    },
    "sources": [
      {
        "lane": "provenance_backfill",
        "source_url": "https://www.choicehotels.com/en-uk/mexico/regional-hotels?placeId=ChIJU1NoiDs6BIQREZgJa760ZO0",
        "source": "Choice regional geoLocation",
        "fields": [
          "Coordinate Source Type",
          "Coordinate Confidence",
          "Geocode Provider",
          "Geocode Method",
          "Geocode Reviewed Date",
          "Last Reviewed Date"
        ]
      }
    ]
  },
  {
    "record_id": "rec3MH…z2gY",
    "identity_key": "ind_hilton_mx_sjdtlup",
    "property_name": "Tropicana Los Cabos, Tapestry Collection by Hilton",
    "patch": {
      "Coordinate Source Type": "structured_data_extraction",
      "Coordinate Confidence": "High",
      "Geocode Provider": "Existing Source",
      "Geocode Method": "structured_data_extraction",
      "Geocode Reviewed Date": "2026-08-05",
      "Last Reviewed Date": "2026-08-05",
      "Enrichment Status": "Partial",
      "Enrichment Priority": "Medium"
    },
    "sources": [
      {
        "lane": "provenance_backfill",
        "source_url": "https://www.hilton.com/en/locations/mexico/tapestry-collection/",
        "source": "Hilton directory localization.coordinate",
        "fields": [
          "Coordinate Source Type",
          "Coordinate Confidence",
          "Geocode Provider",
          "Geocode Method",
          "Geocode Reviewed Date",
          "Last Reviewed Date"
        ]
      }
    ]
  },
  {
    "record_id": "rec3Vt…xDk8",
    "identity_key": "ind_choice_mx_mx184",
    "property_name": "Radisson Hotel Tapatio Guadalajara",
    "patch": {
      "Coordinate Source Type": "structured_data_extraction",
      "Coordinate Confidence": "High",
      "Geocode Provider": "Existing Source",
      "Geocode Method": "structured_data_extraction",
      "Geocode Reviewed Date": "2026-08-05",
      "Last Reviewed Date": "2026-08-05",
      "Enrichment Status": "Partial",
      "Enrichment Priority": "Medium"
    },
    "sources": [
      {
        "lane": "provenance_backfill",
        "source_url": "https://www.choicehotels.com/en-uk/mexico/regional-hotels?placeId=ChIJU1NoiDs6BIQREZgJa760ZO0",
        "source": "Choice regional geoLocation",
        "fields": [
          "Coordinate Source Type",
          "Coordinate Confidence",
          "Geocode Provider",
          "Geocode Method",
          "Geocode Reviewed Date",
          "Last Reviewed Date"
        ]
      }
    ]
  },
  {
    "record_id": "rec44w…Ler8",
    "identity_key": "ind_hilton_mx_gdlapgi",
    "property_name": "Hilton Garden Inn Guadalajara Airport",
    "patch": {
      "Coordinate Source Type": "structured_data_extraction",
      "Coordinate Confidence": "High",
      "Geocode Provider": "Existing Source",
      "Geocode Method": "structured_data_extraction",
      "Geocode Reviewed Date": "2026-08-05",
      "Last Reviewed Date": "2026-08-05",
      "Enrichment Status": "Partial",
      "Enrichment Priority": "Medium"
    },
    "sources": [
      {
        "lane": "provenance_backfill",
        "source_url": "https://www.hilton.com/en/locations/mexico/hilton-garden-inn/",
        "source": "Hilton directory localization.coordinate",
        "fields": [
          "Coordinate Source Type",
          "Coordinate Confidence",
          "Geocode Provider",
          "Geocode Method",
          "Geocode Reviewed Date",
          "Last Reviewed Date"
        ]
      }
    ]
  }
]
```

## Forbidden fields

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
    "Recent Momentum",
    "Latitude",
    "Longitude"
  ],
  "proposed_writes": [],
  "ok": true
}
```

## Next

Founder review dry-run → apply lane-2 with confirm flags (geocode remain blocked until provider decision).
