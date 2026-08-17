# Production Census Address-First Geocode Resolver — Dry Run

**Status:** `production_census_address_geocode_needs_provider_or_terms_decision`  
**Generated:** 2026-08-05T14:29:23.100Z  
**Apply executed:** false  
**Geocoding provider:** `google` (explicit_google)

## 1. Executive summary

| Metric | Value |
| --- | ---: |
| Scanned | 666 |
| Already valid coordinates | 132 |
| Active missing coordinates | 293 |
| Official address found | 78 |
| Official coordinates found | 0 |
| Sent to geocoder | 40 |
| Proposed updates | 34 |
| Blocked | 26 |
| Exact Airtable updates if applied | 34 |
| Est. geocode API cost (USD) | 0.2 |
| Webhound production writes | 0 |

## 2. Address-first resolution method

1. Existing Census Source URL / Official Property URL  
2. Official brand/property directory or hotel page (fetch when address missing)  
3. Structured page data (JSON-LD / schema.org / map payload / address block)  
4. Confirm official address (street-level; property/city/state/country checks)  
5. If official coordinates found → propose as `official_coordinates`  
6. Else geocode **property name + official street address only** via `GEOCODING_PROVIDER`  
7. High/Medium only; Low → blocked  

## 3. Geocoding provider strategy

```json
{
  "provider": "google",
  "resolve_reason": "explicit_google",
  "credentials_ok": true,
  "permanent_storage_enabled": null,
  "storage_terms_reviewed": false,
  "terms_warnings": [
    "Google Geocoding API: do not permanently store lat/long in Airtable unless Dealality's Maps Platform terms allow storage/display for this use case. Set GOOGLE_GEOCODE_STORAGE_TERMS_REVIEWED=1 only after legal/founder review.",
    "GOOGLE_GEOCODE_STORAGE_TERMS_REVIEWED is not set — dry-run may propose coords for review, but apply must not proceed until terms are confirmed.",
    "Do not use the public Nominatim/OSM endpoint for bulk production geocoding."
  ],
  "terms_block_apply": true
}
```

## 4. Terms / storage warning

- Google Geocoding API: do not permanently store lat/long in Airtable unless Dealality's Maps Platform terms allow storage/display for this use case. Set GOOGLE_GEOCODE_STORAGE_TERMS_REVIEWED=1 only after legal/founder review.
- GOOGLE_GEOCODE_STORAGE_TERMS_REVIEWED is not set — dry-run may propose coords for review, but apply must not proceed until terms are confirmed.
- Do not use the public Nominatim/OSM endpoint for bulk production geocoding.

## 5. Official address coverage

- Census street-level among active missing: **0**
- VIC street-level among active missing: **78**
- Official address found during resolution: **78**

## 6. Official coordinate coverage

- Official coordinates proposed: **0**
- Official-address geocode proposed: **34**

## 7. Proposed coordinate updates (sample)

```json
[
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
  },
  {
    "record_id": "rec7v1…eXP6",
    "identity_key": "ind_ihg_mx_pvrfl",
    "property_name": "Holiday Inn Express Puerto Vallarta",
    "brand": "Holiday Inn Express",
    "family": "IHG",
    "latitude": 20.672388,
    "longitude": -105.2490177,
    "official_address": "Blvd. Francisco Medina Ascencio 3974",
    "address_source_url": "https://www.ihg.com/holidayinnexpress/hotels/us/en/puerto-vallarta/pvrfl/hoteldetail",
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
    "record_id": "rec80T…pUXJ",
    "identity_key": "ind_ihg_mx_pbcpl",
    "property_name": "voco Royalty Puebla Downtown",
    "brand": "voco",
    "family": "IHG",
    "latitude": 19.044289,
    "longitude": -98.1981065,
    "official_address": "Portal Hidalgo 8",
    "address_source_url": "https://www.ihg.com/voco/hotels/us/en/puebla/pbcpl/hoteldetail",
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
    "record_id": "rec8CH…YdHv",
    "identity_key": "ind_ihg_mx_mexre",
    "property_name": "Holiday Inn Express Mexico Reforma",
    "brand": "Holiday Inn Express",
    "family": "IHG",
    "latitude": 19.4291703,
    "longitude": -99.1616329,
    "official_address": "Paseo de la Reforma #208",
    "address_source_url": "https://www.ihg.com/holidayinnexpress/hotels/us/en/mexico/mexre/hoteldetail",
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
    "record_id": "recBe2…QUIs",
    "identity_key": "ind_ihg_mx_sjdsl",
    "property_name": "Holiday Inn Express Cabo San Lucas",
    "brand": "Holiday Inn Express",
    "family": "IHG",
    "latitude": 22.902208,
    "longitude": -109.88377,
    "official_address": "Corredor Csl - SJD KM. 4.5",
    "address_source_url": "https://www.ihg.com/holidayinnexpress/hotels/us/en/cabo-san-lucas/sjdsl/hoteldetail",
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
    "record_id": "recBnB…Fc7Z",
    "identity_key": "ind_ihg_mx_cuuci",
    "property_name": "Holiday Inn Express Chihuahua",
    "brand": "Holiday Inn Express",
    "family": "IHG",
    "latitude": 28.7144374,
    "longitude": -106.1351341,
    "official_address": "Av. Cristobal Colon : 11390",
    "address_source_url": "https://www.ihg.com/holidayinnexpress/hotels/us/en/chihuahua/cuuci/hoteldetail",
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
    "record_id": "recCly…dCcx",
    "identity_key": "ind_ihg_mx_tijav",
    "property_name": "Welcome to avid hotels in Tijuana, where the essentials are done right. Every time.",
    "brand": "avid hotels",
    "family": "IHG",
    "latitude": 32.517244,
    "longitude": -116.9683629,
    "official_address": "Cda. Ing. Juan Ojeda Robles 14802",
    "address_source_url": "https://www.ihg.com/avidhotels/hotels/us/en/tijuana/tijav/hoteldetail",
    "coordinate_source_type": "official_address_geocode",
    "confidence": "Medium",
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
    "record_id": "recCrO…A2qs",
    "identity_key": "ind_ihg_mx_zclav",
    "property_name": "Welcome to avid hotels in Fresnillo, where the essentials are done right. Every time.",
    "brand": "avid hotels",
    "family": "IHG",
    "latitude": 23.1797065,
    "longitude": -102.8564086,
    "official_address": "Paseo del Mineral #105",
    "address_source_url": "https://www.ihg.com/avidhotels/hotels/us/en/fresnillo/zclav/hoteldetail",
    "coordinate_source_type": "official_address_geocode",
    "confidence": "Medium",
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
    "record_id": "recCt0…pmke",
    "identity_key": "ind_ihg_mx_culpy",
    "property_name": "Holiday Inn Express Culiacan",
    "brand": "Holiday Inn Express",
    "family": "IHG",
    "latitude": 24.7950762,
    "longitude": -107.4327883,
    "official_address": "Av. Jesus Manuel Sarabia: 2460 Nte",
    "address_source_url": "https://www.ihg.com/holidayinnexpress/hotels/us/en/culiacan/culpy/hoteldetail",
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
    "record_id": "recEGM…sLZ0",
    "identity_key": "ind_ihg_mx_lapgo",
    "property_name": "Hotel Indigo La Paz Puerta Cortes",
    "brand": "Hotel Indigo",
    "family": "IHG",
    "latitude": 24.2180734,
    "longitude": -110.3010719,
    "official_address": "Carretera Pichilingue Km 7.5",
    "address_source_url": "https://www.ihg.com/hotelindigo/hotels/us/en/la-paz/lapgo/hoteldetail",
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
    "record_id": "recEMO…bslI",
    "identity_key": "ind_ihg_mx_bjxrr",
    "property_name": "Holiday Inn Express & Suites Silao Aeropuerto - Terminal",
    "brand": "Holiday Inn Express",
    "family": "IHG",
    "latitude": 20.9793932,
    "longitude": -101.4758622,
    "official_address": "Carretera 45 Silao - León Km. 156",
    "address_source_url": "https://www.ihg.com/holidayinnexpress/hotels/us/en/leon/bjxrr/hoteldetail",
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
    "record_id": "recG2A…cpeO",
    "identity_key": "ind_ihg_mx_hermx",
    "property_name": "voco Hermosillo",
    "brand": "voco",
    "family": "IHG",
    "latitude": 29.0971322,
    "longitude": -110.9394199,
    "official_address": "Blvd. Kino #205",
    "address_source_url": "https://www.ihg.com/voco/hotels/us/en/hermosillo/hermx/hoteldetail",
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
    "record_id": "recLrJ…rmTf",
    "identity_key": "ind_ihg_mx_mexsf",
    "property_name": "Holiday Inn Express Mexico Santa Fe",
    "brand": "Holiday Inn Express",
    "family": "IHG",
    "latitude": 19.3642118,
    "longitude": -99.26512699999999,
    "official_address": "Guillermo Gonzalez Camarena 1400",
    "address_source_url": "https://www.ihg.com/holidayinnexpress/hotels/us/en/mexico/mexsf/hoteldetail",
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
    "record_id": "recLzp…LPjw",
    "identity_key": "ind_ihg_mx_slwvc",
    "property_name": "voco Saltillo Suites",
    "brand": "voco",
    "family": "IHG",
    "latitude": 25.4957563,
    "longitude": -100.9647776,
    "official_address": "Blvd. Venustiano Carranza #8800",
    "address_source_url": "https://www.ihg.com/voco/hotels/us/en/saltillo/slwvc/hoteldetail",
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
    "record_id": "recOan…Jwsv",
    "identity_key": "ind_ihg_mx_mtyzo",
    "property_name": "Holiday Inn Express & Suites Monterrey Aeropuerto",
    "brand": "Holiday Inn Express",
    "family": "IHG",
    "latitude": 25.7862416,
    "longitude": -100.1365873,
    "official_address": "BLVD. AEROPUERTO : 400",
    "address_source_url": "https://www.ihg.com/holidayinnexpress/hotels/us/en/monterrey/mtyzo/hoteldetail",
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
    "record_id": "recPBo…rAUe",
    "identity_key": "ind_ihg_mx_gdlop",
    "property_name": "Holiday Inn Express Guadalajara Aeropuerto",
    "brand": "Holiday Inn Express",
    "family": "IHG",
    "latitude": 20.5765816,
    "longitude": -103.3157266,
    "official_address": "Carretera Chapala #7012",
    "address_source_url": "https://www.ihg.com/holidayinnexpress/hotels/us/en/guadalajara/gdlop/hoteldetail",
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
    "record_id": "recPOq…snlN",
    "identity_key": "ind_ihg_mx_vsasa",
    "property_name": "Holiday Inn Express Villahermosa Tabasco 2000",
    "brand": "Holiday Inn Express",
    "family": "IHG",
    "latitude": 18.0023101,
    "longitude": -92.9569645,
    "official_address": "Circuito Interior Carlos Pellicer Cámara #3916",
    "address_source_url": "https://www.ihg.com/holidayinnexpress/hotels/us/en/villahermosa/vsasa/hoteldetail",
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
    "record_id": "recQ0c…696v",
    "identity_key": "ind_ihg_mx_gdlqp",
    "property_name": "Holiday Inn Express Guadalajara Vallarta Poniente",
    "brand": "Holiday Inn Express",
    "family": "IHG",
    "latitude": 20.6983829,
    "longitude": -103.4568552,
    "official_address": "Carretera Guadalajara-Nogales No. 440",
    "address_source_url": "https://www.ihg.com/holidayinnexpress/hotels/us/en/guadalajara/gdlqp/hoteldetail",
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
    "record_id": "recShp…L1dQ",
    "identity_key": "ind_ihg_mx_pcmaa",
    "property_name": "Hotel Indigo Playa del Carmen",
    "brand": "Hotel Indigo",
    "family": "IHG",
    "latitude": 20.6417023,
    "longitude": -87.0872461,
    "official_address": "1A. Avenida Norte #300",
    "address_source_url": "https://www.ihg.com/hotelindigo/hotels/us/en/playa-del-carmen/pcmaa/hoteldetail",
    "coordinate_source_type": "official_address_geocode",
    "confidence": "Medium",
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
    "record_id": "recU0c…LLHJ",
    "identity_key": "ind_ihg_mx_mexmk",
    "property_name": "Holiday Inn Express Pachuca",
    "brand": "Holiday Inn Express",
    "family": "IHG",
    "latitude": 20.1105544,
    "longitude": -98.7748512,
    "official_address": "Blvd. Luis Donaldo Colosio 220",
    "address_source_url": "https://www.ihg.com/holidayinnexpress/hotels/us/en/pachuca/mexmk/hoteldetail",
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
    "record_id": "recW07…vcxF",
    "identity_key": "ind_ihg_mx_gdlor",
    "property_name": "Holiday Inn Express Guadalajara Expo",
    "brand": "Holiday Inn Express",
    "family": "IHG",
    "latitude": 20.6500538,
    "longitude": -103.3992137,
    "official_address": "Mariano Otero : 2397",
    "address_source_url": "https://www.ihg.com/holidayinnexpress/hotels/us/en/guadalajara/gdlor/hoteldetail",
    "coordinate_source_type": "official_address_geocode",
    "confidence": "Medium",
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
    "record_id": "recWA4…sCOg",
    "identity_key": "ind_ihg_mx_slpmv",
    "property_name": "Holiday Inn Express San Luis Potosi",
    "brand": "Holiday Inn Express",
    "family": "IHG",
    "latitude": 22.1385873,
    "longitude": -100.9392088,
    "official_address": "Av. Benito Juarez : 1270",
    "address_source_url": "https://www.ihg.com/holidayinnexpress/hotels/us/en/san-luis-potosi/slpmv/hoteldetail",
    "coordinate_source_type": "official_address_geocode",
    "confidence": "Medium",
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
    "record_id": "recWTn…jvwL",
    "identity_key": "ind_ihg_mx_middt",
    "property_name": "Holiday Inn Express Merida Centro",
    "brand": "Holiday Inn Express",
    "family": "IHG",
    "latitude": 20.9701172,
    "longitude": -89.6225661,
    "official_address": "Calle 60 No. 491",
    "address_source_url": "https://www.ihg.com/holidayinnexpress/hotels/us/en/merida/middt/hoteldetail",
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
    "record_id": "recWao…ojht",
    "identity_key": "ind_ihg_mx_gdlea",
    "property_name": "voco Guadalajara Expo",
    "brand": "voco",
    "family": "IHG",
    "latitude": 20.6556207,
    "longitude": -103.391453,
    "official_address": "Av. Mariano Otero #1326",
    "address_source_url": "https://www.ihg.com/voco/hotels/us/en/guadalajara/gdlea/hoteldetail",
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
    "record_id": "recZQK…LTMb",
    "identity_key": "ind_ihg_mx_vsaga",
    "property_name": "Holiday Inn Express Paraiso Dos Bocas",
    "brand": "Holiday Inn Express",
    "family": "IHG",
    "latitude": 18.408535,
    "longitude": -93.20368099999999,
    "official_address": "Carretera Federal Paraiso a Puerto Ceiba No. 100",
    "address_source_url": "https://www.ihg.com/holidayinnexpress/hotels/us/en/paraiso/vsaga/hoteldetail",
    "coordinate_source_type": "official_address_geocode",
    "confidence": "Medium",
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
    "record_id": "recZkW…AFx9",
    "identity_key": "ind_ihg_mx_gdlit",
    "property_name": "Holiday Inn Express Guadalajara Iteso",
    "brand": "Holiday Inn Express",
    "family": "IHG",
    "latitude": 20.6127716,
    "longitude": -103.4155631,
    "official_address": "Av Camino Al Iiteso :8650",
    "address_source_url": "https://www.ihg.com/holidayinnexpress/hotels/us/en/guadalajara/gdlit/hoteldetail",
    "coordinate_source_type": "official_address_geocode",
    "confidence": "Medium",
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
    "record_id": "recarZ…tv6Q",
    "identity_key": "ind_ihg_mx_mtyrr",
    "property_name": "Holiday Inn Express & Suites Monterrey Valle",
    "brand": "Holiday Inn Express",
    "family": "IHG",
    "latitude": 25.6535619,
    "longitude": -100.3481387,
    "official_address": "Vasconcelos 345 Oriente",
    "address_source_url": "https://www.ihg.com/holidayinnexpress/hotels/us/en/monterrey/mtyrr/hoteldetail",
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
    "record_id": "recbOY…GXp6",
    "identity_key": "ind_ihg_mx_hmota",
    "property_name": "Holiday Inn Express & Suites Hermosillo",
    "brand": "Holiday Inn Express",
    "family": "IHG",
    "latitude": 29.0825915,
    "longitude": -111.0139091,
    "official_address": "Blvd. Luis Donaldo Colosio 829",
    "address_source_url": "https://www.ihg.com/holidayinnexpress/hotels/us/en/hermosillo/hmota/hoteldetail",
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
    "record_id": "recdMS…Qdsj",
    "identity_key": "ind_ihg_mx_bjxas",
    "property_name": "Holiday Inn Express Silao-Aeropuerto Bajio",
    "brand": "Holiday Inn Express",
    "family": "IHG",
    "latitude": 20.954033,
    "longitude": -101.4173186,
    "official_address": "Libramiento Norte: 3360",
    "address_source_url": "https://www.ihg.com/holidayinnexpress/hotels/us/en/silao/bjxas/hoteldetail",
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
    "record_id": "recdRc…EHkz",
    "identity_key": "ind_ihg_mx_mexba",
    "property_name": "Holiday Inn Express Mexico Basilica",
    "brand": "Holiday Inn Express",
    "family": "IHG",
    "latitude": 19.456101,
    "longitude": -99.12908689999999,
    "official_address": "Calzada Guadalupe No. 54",
    "address_source_url": "https://www.ihg.com/holidayinnexpress/hotels/us/en/mexico-city/mexba/hoteldetail",
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
]
```

## 8. Blocked records (sample)

```json
[
  {
    "record_id": "rec4HI…ZCbT",
    "identity_key": "ind_ihg_mx_mexca",
    "property_name": "Holiday Inn Express Mexico City Satelite",
    "family": "IHG",
    "blocked_reason": "geocode_failed_place_mismatch",
    "coordinate_source_type": "blocked_low_confidence",
    "official_address": "Circuito Arquitectos #3"
  },
  {
    "record_id": "rec9pQ…ikrj",
    "identity_key": "ind_ihg_mx_bjxce",
    "property_name": "Holiday Inn Express & Suites Celaya",
    "family": "IHG",
    "blocked_reason": "geocode_failed_geocode_approximate_only",
    "coordinate_source_type": "blocked_low_confidence",
    "official_address": "Eje Nor-Oriente Luis Donaldo Colosio 285"
  },
  {
    "record_id": "recIw8…IPly",
    "identity_key": "ind_ihg_mx_mexpl",
    "property_name": "Kimpton Virgilio Polanco",
    "family": "IHG",
    "blocked_reason": "geocode_failed_place_mismatch",
    "coordinate_source_type": "blocked_low_confidence",
    "official_address": "Anatole France 79"
  },
  {
    "record_id": "recKiv…fvcS",
    "identity_key": "ind_ihg_mx_juazo",
    "property_name": "Holiday Inn Express & Suites Cd. Juarez - Las Misiones",
    "family": "IHG",
    "blocked_reason": "geocode_failed_place_mismatch",
    "coordinate_source_type": "blocked_low_confidence",
    "official_address": "AV PASEO DE LA VICTORIA : 4202"
  },
  {
    "record_id": "recMA7…GLW7",
    "identity_key": "ind_ihg_mx_cjsjz",
    "property_name": "voco Ciudad Juárez",
    "family": "IHG",
    "blocked_reason": "geocode_failed_place_mismatch",
    "coordinate_source_type": "blocked_low_confidence",
    "official_address": "Blvd. Teófilo Borunda 6941"
  },
  {
    "record_id": "recSLt…5qtR",
    "identity_key": "ind_ihg_mx_cenar",
    "property_name": "Holiday Inn Express & Suites Ciudad Obregon",
    "family": "IHG",
    "blocked_reason": "geocode_failed_place_mismatch",
    "coordinate_source_type": "blocked_low_confidence",
    "official_address": "Av. Miguel Aleman 737 Nte."
  },
  {
    "record_id": "rec02w…jPjx",
    "identity_key": "ind_marriott_mx_bjxds",
    "property_name": "Elena de Cobre, a Member of Design Hotels™",
    "family": "Marriott",
    "blocked_reason": "official_page_blocked",
    "coordinate_source_type": "blocked_no_official_address",
    "official_address": null
  },
  {
    "record_id": "rec0H6…St9L",
    "identity_key": "ind_marriott_mx_cenxo",
    "property_name": "City Express by Marriott Ciudad Obregón",
    "family": "Marriott",
    "blocked_reason": "official_page_blocked",
    "coordinate_source_type": "blocked_no_official_address",
    "official_address": null
  },
  {
    "record_id": "rec0Rc…rrqD",
    "identity_key": "ind_marriott_mx_vsacy",
    "property_name": "Courtyard by Marriott Villahermosa Tabasco",
    "family": "Marriott",
    "blocked_reason": "official_page_blocked",
    "coordinate_source_type": "blocked_no_official_address",
    "official_address": null
  },
  {
    "record_id": "rec0dx…O1nZ",
    "identity_key": "ind_marriott_mx_nogxn",
    "property_name": "City Express by Marriott Nogales",
    "family": "Marriott",
    "blocked_reason": "official_page_blocked",
    "coordinate_source_type": "blocked_no_official_address",
    "official_address": null
  },
  {
    "record_id": "rec0qm…jWLZ",
    "identity_key": "ind_choice_mx_mx086",
    "property_name": "Choice property MX086",
    "family": "Choice",
    "blocked_reason": "fetch_failed_err",
    "coordinate_source_type": "blocked_no_official_address",
    "official_address": null
  },
  {
    "record_id": "rec1Sn…zOfR",
    "identity_key": "ind_marriott_mx_gdlar",
    "property_name": "AC Hotel Guadalajara Expo, Mexico",
    "family": "Marriott",
    "blocked_reason": "official_page_blocked",
    "coordinate_source_type": "blocked_no_official_address",
    "official_address": null
  },
  {
    "record_id": "rec1f0…A50p",
    "identity_key": "ind_marriott_mx_pbcds",
    "property_name": "La Purificadora, Puebla, a Member of Design Hotels™",
    "family": "Marriott",
    "blocked_reason": "official_page_blocked",
    "coordinate_source_type": "blocked_no_official_address",
    "official_address": null
  },
  {
    "record_id": "rec1o8…eEFe",
    "identity_key": "ind_marriott_mx_mxlcy",
    "property_name": "Courtyard by Marriott Mexicali",
    "family": "Marriott",
    "blocked_reason": "official_page_blocked",
    "coordinate_source_type": "blocked_no_official_address",
    "official_address": null
  },
  {
    "record_id": "rec22I…bWAS",
    "identity_key": "ind_marriott_mx_bjxxl",
    "property_name": "City Express by Marriott Leon",
    "family": "Marriott",
    "blocked_reason": "official_page_blocked",
    "coordinate_source_type": "blocked_no_official_address",
    "official_address": null
  },
  {
    "record_id": "rec29i…48Hi",
    "identity_key": "ind_choice_mx_mx153",
    "property_name": "Choice property MX153",
    "family": "Choice",
    "blocked_reason": "fetch_failed_err",
    "coordinate_source_type": "blocked_no_official_address",
    "official_address": null
  },
  {
    "record_id": "rec2UD…Dwq9",
    "identity_key": "ind_marriott_mx_bjxxn",
    "property_name": "City Express by Marriott Irapuato Norte",
    "family": "Marriott",
    "blocked_reason": "official_page_blocked",
    "coordinate_source_type": "blocked_no_official_address",
    "official_address": null
  },
  {
    "record_id": "rec2XQ…APEn",
    "identity_key": "ind_marriott_mx_slpac",
    "property_name": "AC Hotel By Marriott San Luis Potosi",
    "family": "Marriott",
    "blocked_reason": "official_page_blocked",
    "coordinate_source_type": "blocked_no_official_address",
    "official_address": null
  },
  {
    "record_id": "rec2mY…Sb3h",
    "identity_key": "ind_choice_mx_mx073",
    "property_name": "Choice property MX073",
    "family": "Choice",
    "blocked_reason": "fetch_failed_err",
    "coordinate_source_type": "blocked_no_official_address",
    "official_address": null
  },
  {
    "record_id": "rec2ua…2cuA",
    "identity_key": "ind_marriott_mx_mtyxt",
    "property_name": "City Express by Marriott Monterrey Lindavista",
    "family": "Marriott",
    "blocked_reason": "official_page_blocked",
    "coordinate_source_type": "blocked_no_official_address",
    "official_address": null
  },
  {
    "record_id": "rec30e…QKhJ",
    "identity_key": "ind_marriott_mx_pxmtr",
    "property_name": "Terrestre, a Member of Design Hotels™",
    "family": "Marriott",
    "blocked_reason": "official_page_blocked",
    "coordinate_source_type": "blocked_no_official_address",
    "official_address": null
  },
  {
    "record_id": "rec35J…HECD",
    "identity_key": "ind_choice_mx_mx155",
    "property_name": "Choice property MX155",
    "family": "Choice",
    "blocked_reason": "fetch_failed_err",
    "coordinate_source_type": "blocked_no_official_address",
    "official_address": null
  },
  {
    "record_id": "rec3Qg…Vw6c",
    "identity_key": "ind_marriott_mx_lapbc",
    "property_name": "Baja Club Hotel, La Paz, Baja California Sur, a Member of Design Hotels™",
    "family": "Marriott",
    "blocked_reason": "official_page_blocked",
    "coordinate_source_type": "blocked_no_official_address",
    "official_address": null
  },
  {
    "record_id": "rec3Tj…r2lz",
    "identity_key": "ind_marriott_mx_slpxi",
    "property_name": "City Express by Marriott San Luis Potosí Zona Industrial",
    "family": "Marriott",
    "blocked_reason": "official_page_blocked",
    "coordinate_source_type": "blocked_no_official_address",
    "official_address": null
  },
  {
    "record_id": "rec3Wv…Hk7A",
    "identity_key": "ind_marriott_mx_pbcxc",
    "property_name": "City Express by Marriott Puebla Centro",
    "family": "Marriott",
    "blocked_reason": "official_page_blocked",
    "coordinate_source_type": "blocked_no_official_address",
    "official_address": null
  },
  {
    "record_id": "rec3r0…30uk",
    "identity_key": "ind_marriott_mx_tlcxt",
    "property_name": "City Express by Marriott Toluca",
    "family": "Marriott",
    "blocked_reason": "official_page_blocked",
    "coordinate_source_type": "blocked_no_official_address",
    "official_address": null
  }
]
```

## 9. Confidence distribution

```json
{
  "High": 27,
  "Medium": 7,
  "Low": 0
}
```

## 10. Estimated API cost

```json
{
  "requests": 40,
  "estimated_usd": 0.2,
  "basis": "google",
  "rate_per_1k_usd": 5,
  "note": "Order-of-magnitude estimate only; confirm current provider pricing."
}
```

## 11. Fields proposed for future write

- Address
- Latitude
- Longitude
- Radar Geography Status
- Radar Display Status
- Radar Display Reason
- Public Census Eligibility
- Public Display Confidence
- Public Display Review Status
- Last Reviewed Date

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
- Address Confidence (not in schema yet)
- Address Source URL (not in schema yet)
- Coordinate Source Type (not in schema yet)
- Coordinate Confidence (not in schema yet)
- Geocode Provider (not in schema yet)
- Geocode Method (not in schema yet)
- Geocode Reviewed Date (not in schema yet)

## 13. Brand Explorer safety

```json
{
  "touched": false,
  "writes": 0,
  "gates_run": true,
  "all_pass": true,
  "results": [
    {
      "command": "npm run brand-explorer-active-universe-source-of-truth -- --dry-run",
      "exit_code": 0,
      "pass": true
    },
    {
      "command": "npm run brand-explorer-global-active-semantic-audit -- --dry-run --fresh",
      "exit_code": 0,
      "pass": true
    },
    {
      "command": "node scripts/brand-explorer-quiet-sequential-pvql.mjs",
      "exit_code": 0,
      "pass": true,
      "note": "overallPass true · publicFullProfileCount 62"
    },
    {
      "command": "npm run test:brand-explorer-recent-momentum-evidence-quality",
      "exit_code": 0,
      "pass": true
    },
    {
      "command": "npm run test:brand-explorer-mandatory-release-gates",
      "exit_code": 0,
      "pass": true
    }
  ]
}
```

## 14. Schema v1.1.3 recommendation

```json
{
  "supporting_fields_present": {
    "Address Confidence": false,
    "Address Source URL": false,
    "Coordinate Source Type": false,
    "Coordinate Confidence": false,
    "Geocode Provider": false,
    "Geocode Method": false,
    "Geocode Reviewed Date": false
  },
  "v113_recommended": true,
  "recommended_fields": [
    {
      "name": "Address Confidence",
      "type": "singleSelect",
      "options": [
        "High",
        "Medium",
        "Low",
        "Hold"
      ]
    },
    {
      "name": "Address Source URL",
      "type": "url"
    },
    {
      "name": "Coordinate Source Type",
      "type": "singleSelect",
      "options": [
        "official_coordinates",
        "official_address_geocode",
        "blocked_low_confidence",
        "blocked_no_official_address"
      ]
    },
    {
      "name": "Coordinate Confidence",
      "type": "singleSelect",
      "options": [
        "High",
        "Medium",
        "Low",
        "Hold"
      ]
    },
    {
      "name": "Geocode Provider",
      "type": "singleSelect",
      "options": [
        "mapbox",
        "google",
        "none",
        "n/a"
      ]
    },
    {
      "name": "Geocode Method",
      "type": "singleLineText"
    },
    {
      "name": "Geocode Reviewed Date",
      "type": "date"
    }
  ],
  "note": "Capture provider/method/confidence in dry-run report until v1.1.3 schema is approved. Do not create fields in this task."
}
```

## 15. Recommended next step

Founder decision: (1) enable Mapbox permanent geocoding (MAPBOX_ACCESS_TOKEN + MAPBOX_PERMANENT_GEOCODING=1), or (2) confirm Google storage terms (GOOGLE_GEOCODE_STORAGE_TERMS_REVIEWED=1) if Google remains the provider. Then re-run dry-run. Do not apply until terms are confirmed. Optional: approve schema v1.1.3 provenance fields.

## Proposal validation

```json
{
  "zero_zero": 0,
  "invalid": 0,
  "rejected_pin": 0,
  "held": 0,
  "brand_unconfirmed": 0,
  "low_confidence": 0,
  "missing_support": 0,
  "pass": true,
  "failures": []
}
```

## Future apply (do not run until founder approval)

```bash
npm run research-engine-v2:production-census-address-geocode-resolver -- --apply \
  --confirm-address-first-coordinate-resolution \
  --confirm-official-address-only \
  --confirm-approved-geocoding-provider \
  --confirm-storage_terms_reviewed \
  --confirm-no-city-centroids \
  --confirm-no-zero-zero-coordinates \
  --confirm-no-held-records \
  --confirm-no-brand-explorer-writes \
  --confirm-no-owner-operator-writes \
  --confirm-no-room-date-writes
```
