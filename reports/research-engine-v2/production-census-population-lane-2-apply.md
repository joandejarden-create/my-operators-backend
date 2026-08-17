# Production Census Population Lane 2 — Apply

**Status:** `production_census_population_lane_2_applied_ready_for_next_population_lane`  
**Generated:** 2026-08-05T17:02:00.668Z  
**Apply executed:** true

## Summary

- Updates attempted: **177**
- Updates written: **177**
- Validation pass: **true**
- Geocode applied: **false**

## Post-apply validation

```json
{
  "record_count": 666,
  "duplicate_identity_keys": 0,
  "coords_filled": 132,
  "zero_zero": 0,
  "held_public_eligible": 0,
  "provenance_populated": 132,
  "description_filled": 0,
  "amenities_filled": 215,
  "owner_filled": 0,
  "operator_filled": 0,
  "rooms_filled": 0,
  "opening_filled": 0,
  "renovation_filled": 0,
  "affiliation_start_filled": 0,
  "pass": true
}
```

## Geocode lane

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
  "exact_airtable_update_count_if_geocode_applied": 0,
  "applied": false,
  "blocked": true
}
```

## Brand Explorer safety

```json
{
  "touched": false,
  "writes": 0,
  "gates": [
    {
      "label": "active_universe_sot",
      "ok": true,
      "exit_code": 0
    },
    {
      "label": "global_active_semantic",
      "ok": true,
      "exit_code": 0
    },
    {
      "label": "pvql_quiet",
      "ok": true,
      "exit_code": 0
    },
    {
      "label": "momentum_evidence",
      "ok": true,
      "exit_code": 0
    },
    {
      "label": "mandatory_release_gates",
      "ok": true,
      "exit_code": 0
    }
  ],
  "all_pass": true
}
```

## Learning ledger

```json
{
  "entry_id": "census-population-lane-2-provenance-enrichment",
  "ledger_entries": 24,
  "audit_status": "dealality_batch_learning_system_ready",
  "process_actually_learned": true
}
```

## Next

Next: provider/storage decision for 34 geocode proposals; continue description source extraction lane.
