# Production Census Schema v1.1.1 Cleanup Plan
**Status:** `production_census_schema_v111_requires_founder_decisions`
**Mode:** read-only (no schema/record/BE writes)
**Generated:** 2026-08-05T10:39:28.266Z
## 1. Executive summary
- Cleanup plan ready: founder decisions required on Last Reviewed Date rename, amenity hide-vs-delete, and Resort/Extended Stay naming before enrichment.
- Amenities: 6 over-modeled amenity flags exist and are blank across all 666 records — recommend hide from default views (or delete with approval).
- Naming: Align Last Verified Date → Last Reviewed Date; keep Owner Name and Operator / Management Company as Census-specific.
- Enrichment: Do not start enrichment until founder approves v1.1.1 structure decisions.
## 2. Current Census schema status
```json
{
  "base": "Deal Capture Platform",
  "base_id_masked": "appCCU…foLk",
  "table": "Hotel Property Census",
  "table_id": "tbl9aY5ijiuIzzWam",
  "record_count": 666,
  "field_count": 95,
  "production_use_status": "Census Only / Not Owner-Facing",
  "enrichment_not_started": 666,
  "human_review_true": 4,
  "freeze_hash": "c1cb244a95d7311b4ab2cf31d4988685879ef492f4f6420710633267d0effda3",
  "amenity_fields_all_blank": true
}
```
## 3. Naming alignment findings
| Current Field | Similar Existing Field | Existing Table | Classification | Recommended Final Name | Reason | Risk |
| --- | --- | --- | --- | --- | --- | --- |
| Property Name | Property Name / Brand Name / Verified Hotel Name | Hotel Ownership / Brand Basics / VIC stub | keep_as_is | Property Name | Correct property-master label; Brand Name is brand-level. | none |
| State / Region | Region Offered / Region / Verified State / State | Brand Basics / Hotel Census / VIC stub | census_specific_name_ok | State / Region | Geo admin for property; not Brand Region Offered. Keep. | none |
| Source URL | Brand Website / Branded Residences Source URL | Brand Setup - Brand Basics | keep_as_is | Source URL | Source URL is the standard; do not add Source Link. | none |
| Source Confidence | Confidence Level | Brand Setup - Brand Basics | possible_duplicate | Source Confidence | Keep; document vs Relationship Confidence and Data Confidence Tier. | low |
| Production Use Status | Brand Status / External Display Status | Brand Basics | do_not_change | Production Use Status | Must stay distinct from Brand Status. | high_if_renamed |
| Current Brand | Brand Name | Brand Setup - Brand Basics | census_specific_name_ok | Current Brand | Property affiliation snapshot; not Brand Basics Brand Name. | low |
| Brand Explorer Slug if mapped | (code-derived slug) | Brand Explorer runtime | rename_recommended | Brand Explorer Slug | Drop 'if mapped' suffix for clarity; optional v1.1.1 rename. | low |
| Steward Review Status | Validation Status | Brand Basics | do_not_change | Steward Review Status | Census steward lane specific. | medium_if_renamed |
| Market / Submarket | Market / Submarket / Dealality Market | Hotel Census | possible_duplicate | Market / Submarket (short-term) → later Dealality Market + Submarket | Combined field OK for now; long-term split to Dealality geography. | low |
| Rooms / Keys | rooms | Hotel Census | founder_decision_needed | Rooms / Keys (preferred) OR Rooms | Rooms / Keys is clearer than legacy rooms. No Key Count field in Brand/Company Setup. Prefer keep Rooms / Keys. | low |
| Owner Name | Owner Company / Owner/Operator Name / Company Name | Hotel Census / Hotel Ownership / Company Profile | census_specific_name_ok | Owner Name | Keep Census-specific. Company Name stays on Company Profile; link later via enrichment, do not rename Owner Name to Company Name. | medium_if_renamed_to_company_name |
| Operator / Management Company | Management Company / company_name | Hotel Census / Operator Setup - Master | founder_decision_needed | Operator / Management Company (keep) OR Management Company | Slash form is explicit for property-level ops. Aligning to Management Company matches legacy Hotel Census; company_name is Operator Setup entity naming — different layer. | medium |
| Last Verified Date | Last Reviewed Date | Brand Setup - Brand Basics / Operator Setup - Master | rename_recommended | Last Reviewed Date | Align with Brand/Operator Setup common name before enrichment starts. | low |
| Enrichment Status | Validation Status | Brand Basics | do_not_change | Enrichment Status | Enrichment lane progress ≠ validation. | medium_if_renamed |
Other Census fields: **keep_as_is** (see JSON for full list).
## 4. Amenities simplification findings
Preferred model: **Amenities - Source Text** + **Amenities - Structured Tags** + strategic flags only.
| Amenity Field | Classification | Recommendation | Filled /666 | Delete candidate if blank? |
| --- | --- | --- | ---: | --- |
| Amenities - Source Text | keep_as_primary_amenity_field | keep | 0 | false |
| Amenities - Structured Tags | keep_as_primary_amenity_field | keep | 0 | false |
| F&B Flag | keep_as_strategic_flag | keep | 0 | false |
| Meeting Space Flag | keep_as_strategic_flag | keep | 0 | false |
| Fitness Flag | move_to_structured_tags_later | hide_from_default_view_later_or_delete_candidate_if_blank | 0 | true |
| Pool Flag | move_to_structured_tags_later | hide_from_default_view_later_or_delete_candidate_if_blank | 0 | true |
| Resort Amenities Flag | rename_recommended | rename_later | 0 | false |
| Extended Stay Amenity Flag | rename_recommended | rename_later | 0 | false |
| Parking Flag | move_to_structured_tags_later | hide_from_default_view_later_or_delete_candidate_if_blank | 0 | true |
| Airport Shuttle Flag | move_to_structured_tags_later | hide_from_default_view_later_or_delete_candidate_if_blank | 0 | true |
| Spa Flag | move_to_structured_tags_later | hide_from_default_view_later_or_delete_candidate_if_blank | 0 | true |
| Beach / Waterfront Flag | move_to_structured_tags_later | hide_from_default_view_later_or_delete_candidate_if_blank | 0 | true |
| Branded Residences Flag | keep_as_strategic_flag | keep | 0 | false |
| Mixed-Use Flag | keep_as_strategic_flag | keep | 0 | false |
## 5. Over-modeled amenity fields
```json
[
  {
    "amenity_field": "Fitness Flag",
    "classification": "move_to_structured_tags_later",
    "recommendation": "hide_from_default_view_later_or_delete_candidate_if_blank",
    "filled_count": 0,
    "blank_across_all_666": true,
    "script_references": [
      "lib/research-engine-v2/production-census-schema-v11.js",
      "docs/data-intelligence/production-census-v11-post-apply-review.md"
    ],
    "would_deleting_break_brand_explorer": false,
    "would_deleting_require_script_updates": true,
    "reason": "Over-modeled vs preferred Structured Tags model. Blank today. Prefer hide from default views; founder decides delete vs keep deferred.",
    "founder_decision_needed": true,
    "delete_candidate_if_blank": true,
    "delete_safe_if_blank": true
  },
  {
    "amenity_field": "Pool Flag",
    "classification": "move_to_structured_tags_later",
    "recommendation": "hide_from_default_view_later_or_delete_candidate_if_blank",
    "filled_count": 0,
    "blank_across_all_666": true,
    "script_references": [
      "lib/research-engine-v2/production-census-schema-v11.js",
      "docs/data-intelligence/production-census-v11-post-apply-review.md"
    ],
    "would_deleting_break_brand_explorer": false,
    "would_deleting_require_script_updates": true,
    "reason": "Over-modeled vs preferred Structured Tags model. Blank today. Prefer hide from default views; founder decides delete vs keep deferred.",
    "founder_decision_needed": true,
    "delete_candidate_if_blank": true,
    "delete_safe_if_blank": true
  },
  {
    "amenity_field": "Parking Flag",
    "classification": "move_to_structured_tags_later",
    "recommendation": "hide_from_default_view_later_or_delete_candidate_if_blank",
    "filled_count": 0,
    "blank_across_all_666": true,
    "script_references": [
      "lib/research-engine-v2/production-census-schema-v11.js"
    ],
    "would_deleting_break_brand_explorer": false,
    "would_deleting_require_script_updates": true,
    "reason": "Over-modeled vs preferred Structured Tags model. Blank today. Prefer hide from default views; founder decides delete vs keep deferred.",
    "founder_decision_needed": true,
    "delete_candidate_if_blank": true,
    "delete_safe_if_blank": true
  },
  {
    "amenity_field": "Airport Shuttle Flag",
    "classification": "move_to_structured_tags_later",
    "recommendation": "hide_from_default_view_later_or_delete_candidate_if_blank",
    "filled_count": 0,
    "blank_across_all_666": true,
    "script_references": [
      "lib/research-engine-v2/production-census-schema-v11.js"
    ],
    "would_deleting_break_brand_explorer": false,
    "would_deleting_require_script_updates": true,
    "reason": "Over-modeled vs preferred Structured Tags model. Blank today. Prefer hide from default views; founder decides delete vs keep deferred.",
    "founder_decision_needed": true,
    "delete_candidate_if_blank": true,
    "delete_safe_if_blank": true
  },
  {
    "amenity_field": "Spa Flag",
    "classification": "move_to_structured_tags_later",
    "recommendation": "hide_from_default_view_later_or_delete_candidate_if_blank",
    "filled_count": 0,
    "blank_across_all_666": true,
    "script_references": [
      "lib/research-engine-v2/production-census-schema-v11.js"
    ],
    "would_deleting_break_brand_explorer": false,
    "would_deleting_require_script_updates": true,
    "reason": "Over-modeled vs preferred Structured Tags model. Blank today. Prefer hide from default views; founder decides delete vs keep deferred.",
    "founder_decision_needed": true,
    "delete_candidate_if_blank": true,
    "delete_safe_if_blank": true
  },
  {
    "amenity_field": "Beach / Waterfront Flag",
    "classification": "move_to_structured_tags_later",
    "recommendation": "hide_from_default_view_later_or_delete_candidate_if_blank",
    "filled_count": 0,
    "blank_across_all_666": true,
    "script_references": [
      "lib/research-engine-v2/production-census-schema-v11.js"
    ],
    "would_deleting_break_brand_explorer": false,
    "would_deleting_require_script_updates": true,
    "reason": "Over-modeled vs preferred Structured Tags model. Blank today. Prefer hide from default views; founder decides delete vs keep deferred.",
    "founder_decision_needed": true,
    "delete_candidate_if_blank": true,
    "delete_safe_if_blank": true
  }
]
```
## 6. Founder decisions needed
### A_last_verified_date
- **Question:** Rename Last Verified Date → Last Reviewed Date to match Brand/Operator Setup?
- **Options:** rename_to_Last_Reviewed_Date | keep_Last_Verified_Date | document_alias_only
- **Recommendation:** rename_to_Last_Reviewed_Date
- **Timing:** before_enrichment
### B_rooms_keys
- **Question:** Keep Rooms / Keys or rename to Rooms (legacy Hotel Census)?
- **Options:** keep_Rooms_/_Keys | rename_to_Rooms
- **Recommendation:** keep_Rooms_/_Keys
- **Timing:** before_enrichment
### C_operator_management
- **Question:** Keep Operator / Management Company or rename to Management Company?
- **Options:** keep_Operator_/_Management_Company | rename_to_Management_Company
- **Recommendation:** keep_Operator_/_Management_Company
- **Timing:** before_enrichment
### D_owner_name
- **Question:** Keep Owner Name (Census-specific) vs confusing with Company Name?
- **Options:** keep_Owner_Name | document_relationship_to_Company_Profile
- **Recommendation:** keep_Owner_Name
- **Timing:** before_enrichment
### E_source_url
- **Question:** Confirm Source URL (not Source Link) as standard?
- **Options:** keep_Source_URL
- **Recommendation:** keep_Source_URL
- **Timing:** immediate_docs_only
### F_state_region
- **Question:** Keep State / Region as property geo (vs Brand Region Offered)?
- **Options:** keep_State_/_Region
- **Recommendation:** keep_State_/_Region
- **Timing:** immediate_docs_only
### G_amenity_overmodel
- **Question:** For blank over-modeled amenity flags (Fitness/Pool/Parking/Shuttle/Spa/Beach): hide from views, delete, or keep deferred?
- **Options:** hide_from_default_views | delete_blank_fields | keep_deferred
- **Recommendation:** hide_from_default_views
- **Timing:** before_enrichment
- **Note:** All 6 are blank across 666 records. Deleting is low data risk but needs schema write + script map updates. Hiding is safest.
### H_resort_extended_naming
- **Question:** Rename Resort Amenities Flag → Resort / Leisure Flag and Extended Stay Amenity Flag → Extended Stay Flag?
- **Options:** rename_both | keep_current | rename_later_after_enrichment
- **Recommendation:** rename_both
- **Timing:** before_enrichment
## 7. Safe actions (bucket A)
```json
[
  {
    "action": "document_field_roles",
    "field": "(docs)",
    "reason": "Document Source Confidence vs Relationship Confidence vs Data Confidence Tier; Production Use Status vs Brand Status",
    "risk": "none",
    "requires_schema_change": false,
    "requires_record_migration": false,
    "recommended_timing": "now",
    "bucket": "A"
  },
  {
    "action": "confirm_source_url_standard",
    "field": "Source URL",
    "reason": "Do not introduce Source Link",
    "risk": "none",
    "requires_schema_change": false,
    "requires_record_migration": false,
    "recommended_timing": "now",
    "bucket": "A"
  }
]
```
## 8. Deferred actions (bucket C)
```json
[
  {
    "action": "split_market_submarket_later",
    "field": "Market / Submarket",
    "reason": "Long-term Dealality Market + corridor Submarket",
    "risk": "medium",
    "requires_schema_change": true,
    "requires_record_migration": true,
    "recommended_timing": "after_enrichment",
    "bucket": "C"
  }
]
```
## 9. Do-not-change fields (bucket D)
```json
[
  {
    "action": "do_not_change",
    "field": "Production Use Status; Enrichment Status; Steward Review Status; Property Name; Source URL",
    "reason": "Correct Census-specific or already aligned",
    "risk": "high_if_changed",
    "requires_schema_change": false,
    "requires_record_migration": false,
    "recommended_timing": "never_or_docs_only",
    "bucket": "D"
  }
]
```
## 10. Suggested Airtable views (do not create yet)
### Census - Core Identity
- Property Name
- Current Brand
- City
- State / Region
- Country
- Affiliation Status
- Production Use Status
- Data Confidence Tier
- Enrichment Status
- Human Review Required
### Census - Enrichment
- Property Name
- Hotel Description - Source Text
- Hotel Description - AI Summary
- Amenities - Structured Tags
- Property Type
- Asset Context
- Market / Submarket
- Enrichment Status
- Enrichment Priority
### Census - Owner Operator
- Property Name
- Owner Name
- Owner Confidence
- Operator / Management Company
- Operator Confidence
- Ownership Review Status
- Operator Review Status
### Census - Steward Review
- Property Name
- Human Review Required
- Notes for Steward
- Brand-Unassigned Reason
- Enrichment Priority
## 11. Brand Explorer safety result
```json
{
  "gate_results": [
    {
      "cmd": "full_chain",
      "ok": true,
      "exit_code": 0
    }
  ],
  "active_universe": 62,
  "semantic": {
    "activeCount": 62,
    "severityTotals": {
      "critical": 0,
      "high": 0,
      "medium": 0,
      "low": 0
    },
    "freezeDecision": "ready_to_freeze_62_semantic_qa_clean"
  },
  "pvql": {
    "overallPass": true,
    "publicFullProfileCount": 62,
    "scopedCount": 62,
    "universeActiveCount": 62,
    "brandsFilter": null,
    "hardFails": [],
    "notPublicFull": []
  },
  "overall_pass": true,
  "momentum_and_mandatory": "PASS (chain exit 0)"
}
```
## 12. Recommended next step
Founder decides A–H (especially Last Reviewed Date rename + amenity hide/delete + Resort/Extended Stay renames). Then run a separate apply command for approved v1.1.1 changes only. Enrichment stays blocked until then.
### Action table
| Action | Field | Reason | Risk | Schema change? | Record migration? | Timing |
| --- | --- | --- | --- | --- | --- | --- |
| document_field_roles | (docs) | Document Source Confidence vs Relationship Confidence vs Data Confidence Tier; Production Use Status vs Brand Status | none | false | false | now |
| confirm_source_url_standard | Source URL | Do not introduce Source Link | none | false | false | now |
| rename_field | Last Verified Date | Align to Brand/Operator Last Reviewed Date | low | true | false | after_founder_approval |
| rename_field | Brand Explorer Slug if mapped | Simplify to Brand Explorer Slug | low | true | false | after_founder_approval |
| rename_field | Resort Amenities Flag | Align to Resort / Leisure Flag | low | true | false | after_founder_approval |
| rename_field | Extended Stay Amenity Flag | Align to Extended Stay Flag | low | true | false | after_founder_approval |
| hide_or_delete_blank_overmodeled_flags | Fitness Flag, Pool Flag, Parking Flag, Airport Shuttle Flag, Spa Flag, Beach / Waterfront Flag | Blank across 666; prefer Structured Tags; hide safest, delete only with founder OK | low_if_hide / medium_if_delete | true | false | after_founder_approval |
| keep_owner_operator_names | Owner Name; Operator / Management Company | Census-specific property relationships; do not conflate with Company Profile Company Name | high_if_wrongly_renamed | false | false | confirm_then_keep |
| split_market_submarket_later | Market / Submarket | Long-term Dealality Market + corridor Submarket | medium | true | true | after_enrichment |
| do_not_change | Production Use Status; Enrichment Status; Steward Review Status; Property Name; Source URL | Correct Census-specific or already aligned | high_if_changed | false | false | never_or_docs_only |

## Scope

Read-only plan. No Airtable field renames/deletes, no record writes, no Brand Explorer patches, no enrichment.
