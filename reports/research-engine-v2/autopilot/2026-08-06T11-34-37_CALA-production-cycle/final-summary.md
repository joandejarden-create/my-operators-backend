# Production Cycle — Final Summary

- Status: **production_census_autopilot_production_cycle_partial_steward_remaining**
- Records before → after: 757 → 907
- Inserts applied: 150
- Updates applied: 113
- Fields written: Address, Asset Context, Market / Submarket, Property Type
- Queues executed: source_discovery, address_confirmation, property_name_cleanup, description_extraction, amenities_extraction, property_type_asset_context, rooms_keys, radar_public_readiness
- Queues exhausted: coordinate_resolution, address_confirmation, property_name_cleanup, description_extraction, amenities_extraction, property_type_asset_context, rooms_keys, radar_public_readiness
- Steward cases: 42
- Provider-decision cases: 0
- Runtime: 1187s
- Safety stops: none
- Brand Explorer / Brand Setup writes: false
- Next: Clean Choice Radisson Individuals name/city steward queue, then re-run production-cycle.

## Passes

- Pass 1: updates=0, inserts=150, steward_inserts=14
- Pass 2: updates=113, inserts=0, steward_inserts=14
- Pass 3: updates=0, inserts=0, steward_inserts=14
