# Census Autopilot Plan (v2)

- **Parent company:** (required)
- **Region / country:** CALA
- **Mode:** controlled
- **Batch size (chunk):** 250
- **Max records (sample cap):** (none — full scope)
- **Run until complete:** true
- **Confidence threshold:** High
- **Geocode apply ready:** false
- **v1.1.4 schema ready:** true

## Queue order

1. **A. Source discovery / record matching** (`source_discovery`) — status: `scheduled`
2. **A2. Property Name cleanup** (`property_name_cleanup`) — status: `scheduled`
3. **B. Address confirmation** (`address_confirmation`) — status: `scheduled`
4. **C. Coordinate resolution** (`coordinate_resolution`) — status: `blocked_provider`
   - blockers: geocode_provider_or_storage_terms_missing
5. **D. Radar / public readiness** (`radar_public_readiness`) — status: `scheduled`
6. **E. Description extraction** (`description_extraction`) — status: `scheduled`
7. **F. Amenities extraction** (`amenities_extraction`) — status: `scheduled`
8. **G. Property type / asset context** (`property_type_asset_context`) — status: `scheduled`
9. **H. Rooms / Keys** (`rooms_keys`) — status: `scheduled`
10. **I. Steward / Webhound hard cases** (`steward_webhound_hard_cases`) — status: `learning_only`

## Recommended next

```bash
npm run census:autopilot -- --region CALA --parent-company IHG --mode dry-run --run-until-complete --batch-size 250
```
