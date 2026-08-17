# Census Autopilot Plan

- **Parent company:** IHG
- **Region / country:** CALA
- **Mode:** plan
- **Limit:** 250
- **Confidence threshold:** High
- **Geocode apply ready:** false
- **v1.1.4 schema ready:** false

## Queue order

1. **A. Source discovery / record matching** (`source_discovery`) — status: `scheduled`
   - fields: Source URL, Official Website, Official Property URL
2. **B. Address confirmation** (`address_confirmation`) — status: `scheduled`
   - fields: Address, Street Address, City, State / Province, Postal Code, Country
3. **C. Coordinate resolution** (`coordinate_resolution`) — status: `blocked_provider`
   - blockers: geocode_provider_or_storage_terms_missing
   - fields: Latitude, Longitude, Coordinate Provenance, Coordinate Confidence, Coordinate Source URL
4. **D. Radar / public readiness** (`radar_public_readiness`) — status: `scheduled`
   - fields: Radar Display Status, Radar Geography Status, Public Census Eligibility, Public Display Confidence, Public Display Review Status
5. **E. Description extraction** (`description_extraction`) — status: `scheduled`
   - fields: Hotel Description - Source Text, Hotel Description - AI Summary
6. **F. Amenities extraction** (`amenities_extraction`) — status: `scheduled`
   - fields: Amenities - Source Text, Amenities - Structured Tags
7. **G. Property type / asset context** (`property_type_asset_context`) — status: `scheduled`
   - fields: Property Type, Asset Context, Market, Submarket
8. **H. Rooms / Keys** (`rooms_keys`) — status: `runnable_needs_schema`
   - blockers: v1.1.4_rooms_provenance_fields_missing
   - fields: Rooms / Keys, Rooms Confidence, Rooms Source URL
9. **I. Steward / Webhound hard cases** (`steward_webhound_hard_cases`) — status: `learning_only`

## Blocked production lanes

- Owner Name
- Developer
- Operator / Management Company
- Opening Date
- Renovation Date
- Affiliation Start Date
- Recent Momentum
- Brand Explorer fields
- Company Validated
- Brand Verified

## Recommended next

```bash
npm run census:autopilot -- --region CALA --parent-company IHG --mode dry-run --limit 250
```
