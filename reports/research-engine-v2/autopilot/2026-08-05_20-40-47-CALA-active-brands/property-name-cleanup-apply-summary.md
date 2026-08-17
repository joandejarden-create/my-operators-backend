# Property Name Cleanup Apply (Approval-Bundle-Bound)

**Status:** `production_census_property_name_cleanup_apply_clean`  
**Generated:** 2026-08-05T20:45:33.167Z  
**Apply executed:** true

## Summary

- Records updated: **5**
- Records failed: **0**
- Rooms filled: **5 → 5**
- Other Property Name changes: **0**
- Brand Explorer / Brand Setup writes: **false**

## Write results

```json
[
  {
    "record_id": "recClyVxmPwDndCcx",
    "identity_key": "ind_ihg_mx_tijav",
    "ok": true,
    "fields_written": [
      "Property Name"
    ],
    "proposed_property_name": "avid hotels Tijuana - Otay"
  },
  {
    "record_id": "recCrOmuncVJsA2qs",
    "identity_key": "ind_ihg_mx_zclav",
    "ok": true,
    "fields_written": [
      "Property Name"
    ],
    "proposed_property_name": "avid hotels Fresnillo"
  },
  {
    "record_id": "recmHhHstI1XY1tI0",
    "identity_key": "ind_ihg_mx_gdlet",
    "ok": true,
    "fields_written": [
      "Property Name"
    ],
    "proposed_property_name": "avid hotels Guadalajara Aeropuerto Norte"
  },
  {
    "record_id": "reco7guOJ29TMdlAQ",
    "identity_key": "ind_ihg_mx_qroav",
    "ok": true,
    "fields_written": [
      "Property Name"
    ],
    "proposed_property_name": "avid hotels Queretaro Centro Sur"
  },
  {
    "record_id": "rectmFdQjVlrRcylq",
    "identity_key": "ind_ihg_mx_gdlav",
    "ok": true,
    "fields_written": [
      "Property Name"
    ],
    "proposed_property_name": "avid hotels Guadalajara Av Vallarta Pte"
  }
]
```

## Post-verify

```json
[
  {
    "record_id": "recClyVxmPwDndCcx",
    "identity_key": "ind_ihg_mx_tijav",
    "property_name": "avid hotels Tijuana - Otay",
    "name_match_expected": true,
    "rooms_unchanged": true,
    "coords_unchanged": true,
    "description_unchanged": true,
    "amenities_unchanged": true,
    "property_type_unchanged": true,
    "owner_still_blank": true,
    "operator_still_blank": true
  },
  {
    "record_id": "recCrOmuncVJsA2qs",
    "identity_key": "ind_ihg_mx_zclav",
    "property_name": "avid hotels Fresnillo",
    "name_match_expected": true,
    "rooms_unchanged": true,
    "coords_unchanged": true,
    "description_unchanged": true,
    "amenities_unchanged": true,
    "property_type_unchanged": true,
    "owner_still_blank": true,
    "operator_still_blank": true
  },
  {
    "record_id": "recmHhHstI1XY1tI0",
    "identity_key": "ind_ihg_mx_gdlet",
    "property_name": "avid hotels Guadalajara Aeropuerto Norte",
    "name_match_expected": true,
    "rooms_unchanged": true,
    "coords_unchanged": true,
    "description_unchanged": true,
    "amenities_unchanged": true,
    "property_type_unchanged": true,
    "owner_still_blank": true,
    "operator_still_blank": true
  },
  {
    "record_id": "reco7guOJ29TMdlAQ",
    "identity_key": "ind_ihg_mx_qroav",
    "property_name": "avid hotels Queretaro Centro Sur",
    "name_match_expected": true,
    "rooms_unchanged": true,
    "coords_unchanged": true,
    "description_unchanged": true,
    "amenities_unchanged": true,
    "property_type_unchanged": true,
    "owner_still_blank": true,
    "operator_still_blank": true
  },
  {
    "record_id": "rectmFdQjVlrRcylq",
    "identity_key": "ind_ihg_mx_gdlav",
    "property_name": "avid hotels Guadalajara Av Vallarta Pte",
    "name_match_expected": true,
    "rooms_unchanged": true,
    "coords_unchanged": true,
    "description_unchanged": true,
    "amenities_unchanged": true,
    "property_type_unchanged": true,
    "owner_still_blank": true,
    "operator_still_blank": true
  }
]
```

## Next

Property Name cleanup apply clean. Continue Autopilot controlled queues; geocode still blocked.
