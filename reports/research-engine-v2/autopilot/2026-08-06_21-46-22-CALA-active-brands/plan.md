# Census Autopilot Plan (v2)

- **Parent company:** (required)
- **Region / country:** CALA
- **Mode:** controlled
- **Batch size (chunk):** 250
- **Max records (sample cap):** (none — full scope)
- **Run until complete:** true
- **Confidence threshold:** High
- **Geocode apply ready:** true
- **v1.1.4 schema ready:** true

## Queue order

1. **A. Source discovery / record matching** (`source_discovery`) — status: `scheduled`
2. **A0. Core identity quality gate** (`core_identity_quality`) — status: `scheduled`
3. **A0b. City / State normalization** (`city_state_normalization`) — status: `scheduled`
4. **A0c. Core identity source lookup** (`core_identity_source_lookup`) — status: `scheduled`
5. **A0d. Clean Core classification** (`clean_core_classification`) — status: `scheduled`
6. **A0e. Canonical Property Name completion** (`canonical_property_name_completion`) — status: `scheduled`
7. **A0f. Market geography completion** (`market_geography_completion`) — status: `scheduled`
8. **A1. Key field completion** (`key_field_completion`) — status: `scheduled`
9. **A2. Property Name cleanup** (`property_name_cleanup`) — status: `scheduled`
10. **B. Address confirmation** (`address_confirmation`) — status: `scheduled`
11. **C. Coordinate completion (Mapbox Permanent)** (`coordinate_completion`) — status: `scheduled`
12. **C1. Phone number enrichment** (`phone_number_enrichment`) — status: `scheduled`
13. **C2. Coordinate resolution (legacy soft-defer)** (`coordinate_resolution`) — status: `scheduled`
14. **D. Radar / public readiness** (`radar_public_readiness`) — status: `scheduled`
15. **E. Description extraction** (`description_extraction`) — status: `scheduled`
16. **F. Amenities extraction** (`amenities_extraction`) — status: `scheduled`
17. **G. Property type / asset context** (`property_type_asset_context`) — status: `scheduled`
18. **H. Rooms / Keys** (`rooms_keys`) — status: `scheduled`
19. **I. Steward / Webhound hard cases** (`steward_webhound_hard_cases`) — status: `learning_only`

## Recommended next

```bash
npm run census:autopilot -- --region CALA --parent-company IHG --mode dry-run --run-until-complete --batch-size 250
```
