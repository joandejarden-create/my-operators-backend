# Map / Contact / Size Readiness

# Production Census — Map / Contact / Size Readiness

**Status:** `production_census_map_contact_size_readiness_ready_needs_production_cycle`  
**Generated:** 2026-08-07T17:28:24.118Z  
**Write target:** Deal Capture Platform → Hotel Property Census (`tbl9aY5ijiuIzzWam`)  
**Phone field:** `Phone` (exists=true)  
**Airtable writes:** no (audit)

## Counts

| Metric | Count |
|--------|------:|
| Total records | 1224 |
| Clean Core (Level 1) | 1023 |
| Map / Contact / Size Ready (Level 2) | 0 |
| Rich Enrichment Ready (Level 3) | 0 |
| Below Clean Core | 201 |
| Address complete | 400 |
| Lat/Long complete | 379 |
| Lat/Long eligible (Mapbox) | 89 |
| Phone complete | 350 |
| Phone source available | 874 |
| Rooms complete | 0 |
| Rooms source available | 1224 |
| Blocked dirty identity | 199 |
| Blocked missing address | 712 |
| Blocked source insufficient | 0 |

## Mapbox Permanent

- Ready: yes
- Estimated geocode requests: 89
- Estimated cost (USD): 0.445 (mapbox_permanent)

## Production-cycle order

1. source_discovery
2. core_identity_quality
3. core_identity_source_lookup
4. clean_core_classification
5. key_field_completion
6. address_confirmation
7. coordinate_completion
8. phone_number_enrichment
9. rooms_keys
10. property_type_asset_context
11. description_extraction
12. amenities_extraction
13. radar_public_readiness

## Guards

- No Clean Core block for missing lat/long/phone/rooms
- No Mapbox on dirty identity
- No phone from third-party sources
- No weak room inference


## Rules

- Clean Core does **not** require Latitude, Longitude, Phone, or Rooms / Keys.
- Coordinates only after Clean Core + High Address + Address Source URL + Mapbox Permanent (or official coords).
- Phone only from official property / directory / JSON-LD — never OTA / Google / Mapbox.
- Rooms / Keys only High official property-level counts — never inferred.
- Brand Setup / Brand Explorer / VIC / owner-operator / dates blocked.

## Commands

```bash
npm run census:autopilot -- --region CALA --scope active-brand-setup --mode controlled \
  --strategy fastest-safe --queue clean_core_classification --run-until-complete --batch-size 250
```
