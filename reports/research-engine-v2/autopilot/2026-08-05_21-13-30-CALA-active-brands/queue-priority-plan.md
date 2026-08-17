# Queue Priority Plan — fastest-safe

- **Formula:** (expected_safe_writes * business_value_weight * extractor_readiness * source_access_success) / (runtime_risk * ambiguity_risk * dependency_penalty)
- **Why:** Targeted queue run (--queue description_extraction); fastest-safe order overridden.

## Order

1. **E. Description extraction** (`description_extraction`) — score 0.2945
   - IHG description extractor proven in production
2. **F. Amenities extraction** (`amenities_extraction`) — score 0.2008
3. **D. Radar / public readiness** (`radar_public_readiness`) — score 0.1978
4. **B. Address confirmation** (`address_confirmation`) — score 0.1785
5. **A2. Property Name cleanup** (`property_name_cleanup`) — score 0.1704
   - official-source name cleanup; marketing/tagline detection
6. **G. Property type / asset context** (`property_type_asset_context`) — score 0.1666
7. **H. Rooms / Keys** (`rooms_keys`) — score 0.1547
   - rooms extractor ready; mixed-use ambiguity elevates risk
8. **A. Source discovery / record matching** (`source_discovery`) — score 0.0196
9. **I. Steward / Webhound hard cases** (`steward_webhound_hard_cases`) — score 0.0014
   - learning-only queue; never production writes
10. **C. Coordinate resolution** (`coordinate_resolution`) — score 0.0037 — soft-skip (provider)
   - geocode_provider_decision_missing → heavy penalty; soft-skip in apply
