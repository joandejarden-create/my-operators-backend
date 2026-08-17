# Rooms Resolver V2 Design

Version: census-autopilot-v2.1-rooms-resolver

## Ladder
1. official_brand_structured_data_api
2. official_hotel_property_page
3. official_hotel_fact_sheet_pdf
4. official_owner_website
5. official_operator_management_page
6. official_opening_development_announcement
7. tourism_authority_government_registry
8. official_convention_tourism_profile
9. first_party_brand_operator_validation
10. deep_research_escalation

## Never from
- room_types
- bedrooms
- meeting_rooms
- occupancy
- availability
- booking_inventory
- review_counts
- serpapi_google_hotels
- cvent
- legacy_census

## Provenance contract
rooms_value, rooms_source, rooms_source_type, retrieved_at, confidence, property_identity_match, evidence_quote_or_structured_field, rights_status

## Scope this wave
IHG / Hilton / Choice Mexico via existing V1.3 family resolvers. Architecture generalizes by adding family adapters at ladder step 1–2.
