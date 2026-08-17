# Queue Priority Plan — fastest-safe

- **Formula:** (expected_safe_writes * business_value_weight * extractor_readiness * source_access_success) / (runtime_risk * ambiguity_risk * dependency_penalty)
- **Why:** Prioritize geography/radar and proven extractors with high expected High-confidence writes; soft-defer geocode without provider; rooms early but after lower-ambiguity description when scores dictate; hard cases last.

## Order

1. **E. Description extraction** (`description_extraction`) — score 0.2945
   - IHG description extractor proven in production
2. **F. Amenities extraction** (`amenities_extraction`) — score 0.2008
3. **D. Radar / public readiness** (`radar_public_readiness`) — score 0.1978
4. **B. Address confirmation** (`address_confirmation`) — score 0.1785
5. **G. Property type / asset context** (`property_type_asset_context`) — score 0.1666
6. **H. Rooms / Keys** (`rooms_keys`) — score 0.1547
   - rooms extractor ready; mixed-use ambiguity elevates risk
7. **A. Source discovery / record matching** (`source_discovery`) — score 0.0196
8. **I. Steward / Webhound hard cases** (`steward_webhound_hard_cases`) — score 0.0014
   - learning-only queue; never production writes
9. **C. Coordinate resolution** (`coordinate_resolution`) — score 0.0037 — soft-skip (provider)
   - geocode_provider_decision_missing → heavy penalty; soft-skip in apply
