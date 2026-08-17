# Complete CALA Census Mission Plan (v1)

**Objective:** `complete-census-v1`  
**Region:** CALA  
**Scope:** active-brand-setup  
**Write target:** Deal Capture Platform → Hotel Property Census (`tbl9aY5ijiuIzzWam`)  
**Founder approval:** mission CLI command (no per-phase ChatGPT gate)  
**Inserts:** blocked  
**Max passes (per phase cap):** 6  
**Pass semantics:** each phase uses its own max_passes (capped by CLI --max-passes); all phases run unless a hard safety stop.

## Soft targets

- address_complete_min: 400
- lat_long_complete_min: 350
- phone_complete_min: 100
- rooms_complete_min: 100
- complete_census_v1_min: 100

## Phases

1. **Reconfirm Clean Core + Geography** (`phase_1_reconfirm_clean_core_geography`) — queues: core_identity_quality, canonical_property_name_completion, city_state_normalization, market_geography_completion
2. **State / Region Completion (Clean Core)** (`phase_2_state_region`) — queues: city_state_normalization, core_identity_quality · Clean Core only
3. **Address Completion (Clean Core)** (`phase_3_address`) — queues: address_confirmation · Clean Core only
4. **Coordinate Completion (Clean Core + High Address)** (`phase_4_coordinates`) — queues: coordinate_completion · Clean Core only
5. **Phone Completion (Clean Core / official only)** (`phase_5_phone`) — queues: phone_number_enrichment · Clean Core only
6. **Rooms Completion (Clean Core / official only)** (`phase_6_rooms`) — queues: rooms_keys · Clean Core only
7. **Final Readiness Classification** (`phase_7_final_readiness`) — queues: clean_core_classification · read-only

## Hard stops only

- wrong_census_table
- protected_field
- brand_setup_write
- brand_explorer_write
- legacy_census_or_vic_write
- owner_operator_date_field
- systemic_duplicate_insert_risk
- repeated_airtable_write_failure
- schema_conflict

## Soft continues

- unknown_city
- blank_canonical
- blocked_property_pages
- no_high_proposals_in_queue
- mapbox_not_eligible
- missing_phone_source
- missing_room_source
- steward_cases
- source_insufficient
- missing_address_source
