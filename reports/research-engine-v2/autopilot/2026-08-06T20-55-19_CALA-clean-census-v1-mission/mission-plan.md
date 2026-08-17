# Clean CALA Census Mission Plan (v1)

**Objective:** `clean-census-v1`  
**Region:** CALA  
**Scope:** active-brand-setup  
**Write target:** Deal Capture Platform → Hotel Property Census (`tbl9aY5ijiuIzzWam`)  
**Founder approval:** mission CLI command (no per-phase ChatGPT gate)  
**Inserts:** blocked  
**Max passes budget:** 6

## Soft targets

- Clean Core ≥ 875
- Unknown City ≤ 10
- Canonical blank ≤ 10
- Dirty-identity coordinate block ≤ 40

## Phases

1. **Core Identity** (`phase_1_core_identity`) — queues: core_identity_quality, core_identity_source_lookup, canonical_property_name_completion, city_state_normalization, key_field_completion
2. **Clean Core Classification** (`phase_2_classification`) — queues: clean_core_classification · read-only
3. **Address Completion (Clean Core only)** (`phase_3_address`) — queues: address_confirmation · Clean Core only
4. **Coordinate Completion (Clean Core + High Address)** (`phase_4_coordinates`) — queues: coordinate_completion · Clean Core only
5. **Phone + Rooms (Clean Core only)** (`phase_5_contact_size`) — queues: phone_number_enrichment, rooms_keys · Clean Core only
6. **Rich Enrichment** (`phase_6_rich_enrichment`) — queues: property_type_asset_context, description_extraction, amenities_extraction, radar_public_readiness
7. **Final Classification** (`phase_7_final_classification`) — queues: clean_core_classification · read-only

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
