# Key Field Completion (Hotel Property Census)

**Status:** `production_census_key_field_completion_ready_provider_blocked`

Autopilot queue `key_field_completion` classifies foundational Census fields and proposes **High-confidence** autofills only. Wired into production-cycle after `source_discovery` / inserts.

## Priority order

1. Core identity (Property Name, **Canonical Property Name**, Brand, City, State / Region, Country)  
2. Source URL / Source Family / confidence / Production Use Status  
3. Address  
4. State / Region  
5. Latitude / Longitude (official or approved provider)  
6. Radar / public readiness  
7. Property Type / Asset Context / Market  
8. Rooms / Keys  
9. Descriptions / Amenities  

## Coordinate rule

- Allowed: official coordinates; approved provider geocode from confirmed official address  
- Mapbox: `MAPBOX_ACCESS_TOKEN` + `MAPBOX_PERMANENT_GEOCODING=1`  
- Google: `GOOGLE_GEOCODE_STORAGE_TERMS_REVIEWED=1`  
- If not approved: do not write lat/long; continue other fields; route to `provider_decision_needed`

## Reports

- `reports/research-engine-v2/production-census-key-field-completion-matrix.md`
- `reports/research-engine-v2/production-census-key-field-completion-matrix.json`

## Latest snapshot

- Records: **1224**
- Autofill opportunities: **1979**
- Provider-blocked coordinate records: **94**
- Next: Run production-cycle with key_field_completion after source_discovery to apply High autofills, then enrichment queues.
