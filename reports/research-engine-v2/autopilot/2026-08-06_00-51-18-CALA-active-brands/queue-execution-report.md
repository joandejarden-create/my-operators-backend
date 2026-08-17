# Queue Execution Report

- **Run:** 2026-08-06_00-51-18-CALA-active-brands
- **Mode:** controlled
- **Strategy:** fastest-safe
- **Targeted queue:** (none — full fastest-safe plan)
- **Airtable writes:** false

## Results

| Queue | Status | High proposals | Eligible scanned | Notes |
| --- | --- | ---: | --- | --- |
| description_extraction | executed_exhausted | 0 | 416 | no_high_confidence_proposals |
| amenities_extraction | executed_exhausted | 0 | 1157 | no_high_confidence_proposals |
| radar_public_readiness | executed_exhausted | 0 | 500 | no_high_confidence_proposals |
| address_confirmation | executed_exhausted | 0 | 0 | geocode_calls_disabled_in_address_queue_geocodeLimit=0; no_high_confidence_proposals |
| property_name_cleanup | executed_exhausted | 0 | 2 | no_high_confidence_proposals |
| property_type_asset_context | executed | 24 | 500 | — |
| rooms_keys | executed_exhausted | 0 | 495 | no_high_confidence_proposals |
| source_discovery | executed | 0 | 353 | insert_candidates=16; existing_exact=337; status=production_census_cala_discovery_mode_ready_needs_source_adapter |
| coordinate_resolution | soft_deferred | 0 | 0 | geocode_provider_or_storage_terms_missing |

## Summary

- Executed: description_extraction, amenities_extraction, radar_public_readiness, address_confirmation, property_name_cleanup, property_type_asset_context, rooms_keys, source_discovery
- Skipped: (none)
- Soft-deferred: coordinate_resolution
- Total High proposals: 24
- Runtime ms: 333725
