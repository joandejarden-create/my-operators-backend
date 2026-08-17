# First Autopilot Rooms / Keys Apply (Approval-Bundle-Bound)

**Status:** `production_census_autopilot_first_rooms_apply_clean`  
**Generated:** 2026-08-05T20:33:49.211Z  
**Mode:** apply  
**Apply executed:** true

## Summary

- Frozen proposals: **5**
- Records updated: **5**
- Records failed: **0**
- Rooms filled: **0 → 5** (Δ 5)
- Geocode 34 still blocked: **true** (34 blank)
- Brand Explorer writes: **false**
- Brand Setup writes: **false**

## Write results

```json
[
  {
    "record_id": "recClyVxmPwDndCcx",
    "identity_key": "ind_ihg_mx_tijav",
    "ok": true,
    "fields_written": [
      "Rooms / Keys",
      "Rooms Confidence",
      "Rooms Source URL",
      "Rooms Source Type",
      "Rooms Reviewed Date"
    ],
    "rooms": 126
  },
  {
    "record_id": "recCrOmuncVJsA2qs",
    "identity_key": "ind_ihg_mx_zclav",
    "ok": true,
    "fields_written": [
      "Rooms / Keys",
      "Rooms Confidence",
      "Rooms Source URL",
      "Rooms Source Type",
      "Rooms Reviewed Date"
    ],
    "rooms": 100
  },
  {
    "record_id": "recmHhHstI1XY1tI0",
    "identity_key": "ind_ihg_mx_gdlet",
    "ok": true,
    "fields_written": [
      "Rooms / Keys",
      "Rooms Confidence",
      "Rooms Source URL",
      "Rooms Source Type",
      "Rooms Reviewed Date"
    ],
    "rooms": 149
  },
  {
    "record_id": "reco7guOJ29TMdlAQ",
    "identity_key": "ind_ihg_mx_qroav",
    "ok": true,
    "fields_written": [
      "Rooms / Keys",
      "Rooms Confidence",
      "Rooms Source URL",
      "Rooms Source Type",
      "Rooms Reviewed Date"
    ],
    "rooms": 118
  },
  {
    "record_id": "rectmFdQjVlrRcylq",
    "identity_key": "ind_ihg_mx_gdlav",
    "ok": true,
    "fields_written": [
      "Rooms / Keys",
      "Rooms Confidence",
      "Rooms Source URL",
      "Rooms Source Type",
      "Rooms Reviewed Date"
    ],
    "rooms": 124
  }
]
```

## Post-verify

```json
[
  {
    "record_id": "recClyVxmPwDndCcx",
    "identity_key": "ind_ihg_mx_tijav",
    "rooms_keys": 126,
    "rooms_confidence": "High",
    "rooms_source_url": "https://www.ihg.com/avidhotels/hotels/us/en/tijuana/tijav/hoteldetail",
    "rooms_source_type": "official_property_page",
    "rooms_reviewed_date": "2026-08-05",
    "rooms_notes": null,
    "enrichment_status": "Partial",
    "rooms_match_expected": true,
    "confidence_high": true,
    "source_ok": true,
    "coords_unchanged": true,
    "owner_still_blank": true,
    "operator_still_blank": true,
    "description_unchanged": true,
    "amenities_unchanged": true,
    "property_type_unchanged": true
  },
  {
    "record_id": "recCrOmuncVJsA2qs",
    "identity_key": "ind_ihg_mx_zclav",
    "rooms_keys": 100,
    "rooms_confidence": "High",
    "rooms_source_url": "https://www.ihg.com/avidhotels/hotels/us/en/fresnillo/zclav/hoteldetail",
    "rooms_source_type": "official_property_page",
    "rooms_reviewed_date": "2026-08-05",
    "rooms_notes": null,
    "enrichment_status": "Partial",
    "rooms_match_expected": true,
    "confidence_high": true,
    "source_ok": true,
    "coords_unchanged": true,
    "owner_still_blank": true,
    "operator_still_blank": true,
    "description_unchanged": true,
    "amenities_unchanged": true,
    "property_type_unchanged": true
  },
  {
    "record_id": "recmHhHstI1XY1tI0",
    "identity_key": "ind_ihg_mx_gdlet",
    "rooms_keys": 149,
    "rooms_confidence": "High",
    "rooms_source_url": "https://www.ihg.com/avidhotels/hotels/us/en/tlaquepaque/gdlet/hoteldetail",
    "rooms_source_type": "official_property_page",
    "rooms_reviewed_date": "2026-08-05",
    "rooms_notes": null,
    "enrichment_status": "Partial",
    "rooms_match_expected": true,
    "confidence_high": true,
    "source_ok": true,
    "coords_unchanged": true,
    "owner_still_blank": true,
    "operator_still_blank": true,
    "description_unchanged": true,
    "amenities_unchanged": true,
    "property_type_unchanged": true
  },
  {
    "record_id": "reco7guOJ29TMdlAQ",
    "identity_key": "ind_ihg_mx_qroav",
    "rooms_keys": 118,
    "rooms_confidence": "High",
    "rooms_source_url": "https://www.ihg.com/avidhotels/hotels/us/en/queretaro/qroav/hoteldetail",
    "rooms_source_type": "official_property_page",
    "rooms_reviewed_date": "2026-08-05",
    "rooms_notes": null,
    "enrichment_status": "Partial",
    "rooms_match_expected": true,
    "confidence_high": true,
    "source_ok": true,
    "coords_unchanged": true,
    "owner_still_blank": true,
    "operator_still_blank": true,
    "description_unchanged": true,
    "amenities_unchanged": true,
    "property_type_unchanged": true
  },
  {
    "record_id": "rectmFdQjVlrRcylq",
    "identity_key": "ind_ihg_mx_gdlav",
    "rooms_keys": 124,
    "rooms_confidence": "High",
    "rooms_source_url": "https://www.ihg.com/avidhotels/hotels/us/en/zapopan/gdlav/hoteldetail",
    "rooms_source_type": "official_property_page",
    "rooms_reviewed_date": "2026-08-05",
    "rooms_notes": null,
    "enrichment_status": "Partial",
    "rooms_match_expected": true,
    "confidence_high": true,
    "source_ok": true,
    "coords_unchanged": true,
    "owner_still_blank": true,
    "operator_still_blank": true,
    "description_unchanged": true,
    "amenities_unchanged": true,
    "property_type_unchanged": true
  }
]
```

## Next

First rooms apply clean. Continue Autopilot controlled on next queue; do not apply geocode until provider decision.
