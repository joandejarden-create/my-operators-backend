# Production Cycle — Final Summary

- Status: **production_census_autopilot_production_cycle_partial_steward_remaining**
- Records before → after: 907 → 907
- Inserts applied: 0
- Updates applied: 17
- Fields written: Address Confidence, Address Source URL, Canonical Property Name
- Queues executed: source_discovery, key_field_completion, address_confirmation, property_name_cleanup, description_extraction, amenities_extraction, property_type_asset_context, rooms_keys, radar_public_readiness, coordinate_completion
- Queues exhausted: address_confirmation, property_name_cleanup, description_extraction, amenities_extraction, property_type_asset_context, rooms_keys, radar_public_readiness, coordinate_completion
- Steward cases: 184
- Provider-decision cases: 0
- Runtime: 938s
- Safety stops: none
- Brand Explorer / Brand Setup writes: false
- Next: Clean Choice Radisson Individuals name/city steward queue, then re-run production-cycle.

## Passes

- Pass 1: updates=17, inserts=0, steward_inserts=14
- Pass 2: updates=0, inserts=0, steward_inserts=14
