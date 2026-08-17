# Production Census Property Name Cleanup Queue

**Status:** `production_census_property_name_cleanup_controlled_ready_for_apply`  
**Generated:** 2026-08-05T20:40:47.986Z  
**Queue:** `property_name_cleanup`  
**Extractor:** production-census-property-name-cleanup-extractor-v1  
**Airtable writes:** false

## Summary

| Metric | Value |
| --- | ---: |
| Records scanned | 666 |
| Eligible (malformed + official URL) | 7 |
| Processed | 7 |
| High proposals | 5 |
| Medium review | 0 |
| Hold | 0 |
| Low/blocked | 2 |
| Exact writes if applied | 5 |
| Avid rows in proposals | 5 |
| Avid High writes | 5 |

## Sample High

```json
[
  {
    "record_id": "recClyVxmPwDndCcx",
    "identity_key": "ind_ihg_mx_tijav",
    "family": "IHG",
    "brand": "avid hotels",
    "city": "Tijuana",
    "action": "propose_high_write",
    "current_property_name": "Welcome to avid hotels in Tijuana, where the essentials are done right. Every time.",
    "proposed_property_name": "avid hotels Tijuana - Otay",
    "confidence": "High",
    "method": "json_ld_hotel_name",
    "source_url": "https://www.ihg.com/avidhotels/hotels/us/en/tijuana/tijav/hoteldetail",
    "reason": "official_clean_name_replaces_marketing_phrase",
    "name_problems": [
      "starts_with_marketing_intro",
      "contains_marketing_phrase",
      "multi_sentence_marketing"
    ],
    "write_allowed_now": true,
    "patch_fields": [
      "Property Name",
      "Last Reviewed Date",
      "Enrichment Status",
      "Enrichment Priority"
    ],
    "patch": {
      "Property Name": "avid hotels Tijuana - Otay",
      "Last Reviewed Date": "2026-08-05",
      "Enrichment Status": "Partial",
      "Enrichment Priority": "Medium"
    },
    "queue": "property_name_cleanup",
    "current_fields": {}
  },
  {
    "record_id": "recCrOmuncVJsA2qs",
    "identity_key": "ind_ihg_mx_zclav",
    "family": "IHG",
    "brand": "avid hotels",
    "city": "Fresnillo",
    "action": "propose_high_write",
    "current_property_name": "Welcome to avid hotels in Fresnillo, where the essentials are done right. Every time.",
    "proposed_property_name": "avid hotels Fresnillo",
    "confidence": "High",
    "method": "json_ld_hotel_name",
    "source_url": "https://www.ihg.com/avidhotels/hotels/us/en/fresnillo/zclav/hoteldetail",
    "reason": "official_clean_name_replaces_marketing_phrase",
    "name_problems": [
      "starts_with_marketing_intro",
      "contains_marketing_phrase",
      "multi_sentence_marketing"
    ],
    "write_allowed_now": true,
    "patch_fields": [
      "Property Name",
      "Last Reviewed Date",
      "Enrichment Status",
      "Enrichment Priority"
    ],
    "patch": {
      "Property Name": "avid hotels Fresnillo",
      "Last Reviewed Date": "2026-08-05",
      "Enrichment Status": "Partial",
      "Enrichment Priority": "Medium"
    },
    "queue": "property_name_cleanup",
    "current_fields": {}
  },
  {
    "record_id": "recmHhHstI1XY1tI0",
    "identity_key": "ind_ihg_mx_gdlet",
    "family": "IHG",
    "brand": "avid hotels",
    "city": "Tlaquepaque",
    "action": "propose_high_write",
    "current_property_name": "Welcome to avid hotels in Tlaquepaque, where the essentials are done right. Every time.",
    "proposed_property_name": "avid hotels Guadalajara Aeropuerto Norte",
    "confidence": "High",
    "method": "json_ld_hotel_name",
    "source_url": "https://www.ihg.com/avidhotels/hotels/us/en/tlaquepaque/gdlet/hoteldetail",
    "reason": "official_clean_name_replaces_marketing_phrase",
    "name_problems": [
      "starts_with_marketing_intro",
      "contains_marketing_phrase",
      "multi_sentence_marketing"
    ],
    "write_allowed_now": true,
    "patch_fields": [
      "Property Name",
      "Last Reviewed Date",
      "Enrichment Status",
      "Enrichment Priority"
    ],
    "patch": {
      "Property Name": "avid hotels Guadalajara Aeropuerto Norte",
      "Last Reviewed Date": "2026-08-05",
      "Enrichment Status": "Partial",
      "Enrichment Priority": "Medium"
    },
    "queue": "property_name_cleanup",
    "current_fields": {}
  },
  {
    "record_id": "reco7guOJ29TMdlAQ",
    "identity_key": "ind_ihg_mx_qroav",
    "family": "IHG",
    "brand": "avid hotels",
    "city": "Queretaro",
    "action": "propose_high_write",
    "current_property_name": "Welcome to avid hotels in Queretaro, where the essentials are done right. Every time.",
    "proposed_property_name": "avid hotels Queretaro Centro Sur",
    "confidence": "High",
    "method": "json_ld_hotel_name",
    "source_url": "https://www.ihg.com/avidhotels/hotels/us/en/queretaro/qroav/hoteldetail",
    "reason": "official_clean_name_replaces_marketing_phrase",
    "name_problems": [
      "starts_with_marketing_intro",
      "contains_marketing_phrase",
      "multi_sentence_marketing"
    ],
    "write_allowed_now": true,
    "patch_fields": [
      "Property Name",
      "Last Reviewed Date",
      "Enrichment Status",
      "Enrichment Priority"
    ],
    "patch": {
      "Property Name": "avid hotels Queretaro Centro Sur",
      "Last Reviewed Date": "2026-08-05",
      "Enrichment Status": "Partial",
      "Enrichment Priority": "Medium"
    },
    "queue": "property_name_cleanup",
    "current_fields": {}
  },
  {
    "record_id": "rectmFdQjVlrRcylq",
    "identity_key": "ind_ihg_mx_gdlav",
    "family": "IHG",
    "brand": "avid hotels",
    "city": "Zapopan",
    "action": "propose_high_write",
    "current_property_name": "Welcome to avid hotels in Zapopan, where the essentials are done right. Every time.",
    "proposed_property_name": "avid hotels Guadalajara Av Vallarta Pte",
    "confidence": "High",
    "method": "json_ld_hotel_name",
    "source_url": "https://www.ihg.com/avidhotels/hotels/us/en/zapopan/gdlav/hoteldetail",
    "reason": "official_clean_name_replaces_marketing_phrase",
    "name_problems": [
      "starts_with_marketing_intro",
      "contains_marketing_phrase",
      "multi_sentence_marketing"
    ],
    "write_allowed_now": true,
    "patch_fields": [
      "Property Name",
      "Last Reviewed Date",
      "Enrichment Status",
      "Enrichment Priority"
    ],
    "patch": {
      "Property Name": "avid hotels Guadalajara Av Vallarta Pte",
      "Last Reviewed Date": "2026-08-05",
      "Enrichment Status": "Partial",
      "Enrichment Priority": "Medium"
    },
    "queue": "property_name_cleanup",
    "current_fields": {}
  }
]
```

## Allowed fields

- Property Name
- Last Reviewed Date
- Enrichment Status
- Enrichment Priority

## Next

Founder review High proposals; apply approval-bundle-bound with Property Name + Last Reviewed only.
