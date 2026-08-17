# Queue Execution Report

- **Run:** 2026-08-05_21-49-37-CALA-active-brands
- **Mode:** controlled
- **Strategy:** fastest-safe
- **Targeted queue:** (none — full fastest-safe plan)
- **Airtable writes:** false

## Results

| Queue | Status | High proposals | Eligible scanned | Notes |
| --- | --- | ---: | --- | --- |
| description_extraction | executed_exhausted | 0 | 341 | no_high_confidence_proposals |
| amenities_extraction | executed_exhausted | 0 | 1007 | no_high_confidence_proposals |
| radar_public_readiness | executed_exhausted | 0 | 425 | no_high_confidence_proposals |
| address_confirmation | executed | 107 | 107 | geocode_calls_disabled_in_address_queue_geocodeLimit=0 |
| property_name_cleanup | executed_exhausted | 0 | 2 | no_high_confidence_proposals |
| property_type_asset_context | executed | 1 | 425 | — |
| rooms_keys | executed_exhausted | 0 | 420 | no_high_confidence_proposals |
| coordinate_resolution | soft_deferred | 0 | 0 | geocode_provider_or_storage_terms_missing |

## Summary

- Executed: description_extraction, amenities_extraction, radar_public_readiness, address_confirmation, property_name_cleanup, property_type_asset_context, rooms_keys
- Skipped: (none)
- Soft-deferred: coordinate_resolution
- Total High proposals: 108
- Runtime ms: 269205
