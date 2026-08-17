# Queue Priority Plan — fastest-safe

- **Formula:** (expected_safe_writes * business_value_weight * extractor_readiness * source_access_success) / (runtime_risk * ambiguity_risk * dependency_penalty)
- **Why:** Targeted queue run (--queue brand_normalization); fastest-safe order overridden.

## Order

1. **A0brand. Brand normalization / Brand Source-of-Truth** (`brand_normalization`) — score 0.3207
2. **A0. Core identity quality gate** (`core_identity_quality`) — score 0.2975
3. **E. Description extraction** (`description_extraction`) — score 0.2945
   - IHG description extractor proven in production
4. **C. Coordinate completion (Mapbox Permanent)** (`coordinate_completion`) — score 0.2677
5. **A0b. City / State normalization** (`city_state_normalization`) — score 0.2261
6. **F. Amenities extraction** (`amenities_extraction`) — score 0.2008
7. **A1. Key field completion** (`key_field_completion`) — score 0.1978
8. **D. Radar / public readiness** (`radar_public_readiness`) — score 0.1978
9. **B. Address confirmation** (`address_confirmation`) — score 0.1785
10. **A2. Property Name cleanup** (`property_name_cleanup`) — score 0.1704
   - official-source name cleanup; marketing/tagline detection
11. **G. Property type / asset context** (`property_type_asset_context`) — score 0.1666
12. **A0f. Market geography completion** (`market_geography_completion`) — score 0.166
13. **H. Rooms / Keys** (`rooms_keys`) — score 0.1547
   - rooms extractor ready; mixed-use ambiguity elevates risk
14. **C1. Phone number enrichment** (`phone_number_enrichment`) — score 0.1264
15. **A0e. Canonical Property Name completion** (`canonical_property_name_completion`) — score 0.0892
16. **C2. Coordinate resolution (legacy soft-defer)** (`coordinate_resolution`) — score 0.0595
17. **A0c. Core identity source lookup** (`core_identity_source_lookup`) — score 0.0547
18. **A0d. Clean Core classification** (`clean_core_classification`) — score 0.0541
19. **A. Source discovery / record matching** (`source_discovery`) — score 0.0476
20. **I. Steward / Webhound hard cases** (`steward_webhound_hard_cases`) — score 0.0014
   - learning-only queue; never production writes
