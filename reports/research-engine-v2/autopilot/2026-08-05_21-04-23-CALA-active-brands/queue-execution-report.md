# Queue Execution Report

- **Run:** 2026-08-05_21-04-23-CALA-active-brands
- **Mode:** controlled
- **Strategy:** fastest-safe
- **Targeted queue:** (none — full fastest-safe plan)
- **Airtable writes:** false

## Results

| Queue | Status | High proposals | Eligible scanned | Notes |
| --- | --- | ---: | --- | --- |
| description_extraction | skipped_exhausted | 0 | 341 | no_high_confidence_proposals |
| amenities_extraction | skipped_exhausted | 0 | 341 | no_high_confidence_proposals |
| radar_public_readiness | skipped_exhausted | 0 | 0 | no_high_confidence_proposals |
| address_confirmation | skipped_exhausted | 0 | 0 | geocode_calls_disabled_in_address_queue_geocodeLimit=0; no_high_confidence_proposals |
| property_name_cleanup | skipped_exhausted | 0 | 2 | no_high_confidence_proposals |
| property_type_asset_context | skipped_exhausted | 0 | 0 | no_high_confidence_proposals |
| rooms_keys | skipped_exhausted | 0 | 420 | no_high_confidence_proposals |
| coordinate_resolution | soft_deferred | 0 | 0 | geocode_provider_or_storage_terms_missing |

## Summary

- Executed: (none)
- Skipped: description_extraction, amenities_extraction, radar_public_readiness, address_confirmation, property_name_cleanup, property_type_asset_context, rooms_keys
- Soft-deferred: coordinate_resolution
- Total High proposals: 0
- Runtime ms: 336439
