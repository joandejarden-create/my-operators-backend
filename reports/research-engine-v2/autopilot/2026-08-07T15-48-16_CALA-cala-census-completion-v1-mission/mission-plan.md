# Clean CALA Census Mission Plan (v1)

**Objective:** `cala-census-completion-v1`  
**Region:** CALA  
**Scope:** official-parent-inventory  
**Write target:** Deal Capture Platform → Hotel Property Census (`tbl9aY5ijiuIzzWam`)  
**Founder approval:** mission CLI command (no per-phase ChatGPT gate)  
**Inserts:** blocked  
**Max passes (per phase cap):** 10  
**Pass semantics:** each phase uses its own max_passes (capped by CLI --max-passes); all phases run unless a hard safety stop.

## Soft targets

- address_complete_min: 400
- lat_long_complete_min: 350
- phone_complete_min: 100
- rooms_complete_min: 100
- complete_census_v1_min: 100
- clean_core_min: 875

## Phases

1. **Reconfirm Brand / Core Identity** (`phase_2_reconfirm_brand_core`) — queues: brand_normalization, parent_company_normalization, core_identity_quality, core_identity_source_lookup, canonical_property_name_completion, city_state_normalization
2. **Clean Core Classification** (`phase_3_clean_core_classification`) — queues: clean_core_classification · read-only
3. **Market Geography** (`phase_4_market_geography`) — queues: market_geography_completion
4. **Address Completion (Clean Core)** (`phase_5_address`) — queues: address_confirmation · Clean Core only
5. **Coordinate Completion (Clean Core + High Address)** (`phase_6_coordinates`) — queues: coordinate_completion · Clean Core only
6. **Phone Completion (Clean Core / official only)** (`phase_7_phone`) — queues: phone_number_enrichment · Clean Core only
7. **Rooms Completion (Clean Core / official only)** (`phase_8_rooms`) — queues: rooms_keys · Clean Core only
8. **Final Readiness Classification** (`phase_9_final_readiness`) — queues: clean_core_classification · read-only

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
