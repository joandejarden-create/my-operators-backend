# Production Census v1.1 Post-Apply Review

**Status:** `production_census_v11_post_apply_review_minor_cleanup_recommended`
**Mode:** read-only (no schema/record/BE writes)
**Generated:** 2026-08-05T09:59:43.080Z

## 1. Executive summary

- Verdict: `production_census_v11_post_apply_review_minor_cleanup_recommended`
- Mostly aligned; Last Verified Date vs Last Reviewed Date is the main naming tension
- Amenities: 6 amenity flags recommended for later Structured Tags consolidation
- Enrichment: Start with descriptions + amenities + property type / asset context. Do not start with owner/operator until sourcing process is defined.
- Final: Proceed to descriptions + amenities + property type enrichment; schedule non-blocking v1.1.1 amenity/naming cleanup (hide tag-level amenity flags; decide Last Verified vs Last Reviewed).

## 2. Census count validation

| Table | Expected | Actual | Pass |
| --- | ---: | ---: | --- |
| Hotel Property Census | 666 | 666 | true |
| Brand Affiliations | 666 | 666 | true |
| Source Evidence | 666 | 666 | true |
| Steward Review | 4 | 4 | true |

Duplicates: 0 · 0,0 coords: 0 · Human Review true: 4

## 3. Existing Brand Setup / Company Setup naming conventions

```json
{
  "brand_setup": {
    "brand_name": "Brand Name",
    "brand_status": "Brand Status",
    "parent_company": "Parent Company",
    "validation_status": "Validation Status",
    "source_type": "Source Type",
    "confidence_level": "Confidence Level",
    "company_validated": "Company Validated",
    "last_reviewed_date": "Last Reviewed Date",
    "external_display_status": "External Display Status"
  },
  "company_setup": {
    "company_name": "Company Name",
    "company_type": "Company Type",
    "owner_profile_status": "Owner Profile Status",
    "operator_profile_status": "Operator Profile Status",
    "developer_profile_status": "Developer Profile Status",
    "operator_master_company_name": "company_name",
    "data_confidence_level": "Data Confidence Level"
  },
  "legacy_hotel_census": {
    "name": "name",
    "rooms": "rooms",
    "management_company": "Management Company",
    "owner_company": "Owner Company",
    "market": "Market",
    "submarket": "Submarket",
    "amenities": "Amenities"
  }
}
```

## 4. Census field naming alignment

| Census Field | Similar Existing Field | Existing Table | Classification | Recommendation | Reason |
| --- | --- | --- | --- | --- | --- |
| Property Name | Property Name / Brand Name / Verified Hotel Name | Hotel Ownership / Brand Basics / VIC stub | census_specific_name_ok | keep | Brand Basics uses Brand Name; Hotel Ownership uses Property Name; Verified Independent uses Verified Hotel Name. Property Name is correct for property master. |
| Canonical Property Name | Property Name | Hotel Ownership | possible_duplicate_concept | add_alias/documentation_only | Related concept exists on another table; document difference rather than rename now. |
| Property Identity Key | — | — | census_specific_name_ok | keep | No close Brand/Company Setup match; Census-specific field OK. |
| Family / Source Family | — | — | census_specific_name_ok | keep | No close Brand/Company Setup match; Census-specific field OK. |
| Country | Country | Hotel Ownership | aligned_with_existing_name | keep | Closely matches existing naming; keep Census field as-is. |
| State / Region | Region Offered / Region / Verified State | Brand Basics / Hotel Census / VIC stub | census_specific_name_ok | keep | Distinct from Brand Basics Region Offered (commercial regions). State/Region is geo admin for the property. |
| City | city | Hotel Census | aligned_with_existing_name | keep | Closely matches existing naming; keep Census field as-is. |
| Address | Company Address | Company Profile | possible_duplicate_concept | add_alias/documentation_only | Related concept exists on another table; document difference rather than rename now. |
| Latitude | Latitude | Hotel Census | aligned_with_existing_name | keep | Closely matches existing naming; keep Census field as-is. |
| Longitude | Longitude | Hotel Census | aligned_with_existing_name | keep | Closely matches existing naming; keep Census field as-is. |
| Phone | Management Phone | Hotel Census | possible_duplicate_concept | add_alias/documentation_only | Related concept exists on another table; document difference rather than rename now. |
| Official Property URL | — | — | census_specific_name_ok | keep | No close Brand/Company Setup match; Census-specific field OK. |
| Source URL | Brand Website / Branded Residences Source URL | Brand Setup - Brand Basics | aligned_with_existing_name | keep | Matches Source URL usage pattern; Brand Basics uses Brand Website / branded-residences Source URL variants. |
| Source Type | Source Type | Brand Setup - Brand Basics | aligned_with_existing_name | keep | Closely matches existing naming; keep Census field as-is. |
| Source Confidence | Confidence Level | Brand Setup - Brand Basics | possible_duplicate_concept | add_alias/documentation_only | Brand Setup uses Confidence Level; Census also has Relationship Confidence + Data Confidence Tier. Document roles: Source=evidence quality, Relationship=link certainty, Tier=overall. |
| Discovery Date | — | — | census_specific_name_ok | keep | No close Brand/Company Setup match; Census-specific field OK. |
| VIC Freeze Hash | — | — | census_specific_name_ok | keep | No close Brand/Company Setup match; Census-specific field OK. |
| Data Eligible | Explorer Hero Data Source | Brand Setup - Brand Basics | possible_duplicate_concept | add_alias/documentation_only | Related concept exists on another table; document difference rather than rename now. |
| Identity Confidence | — | — | census_specific_name_ok | keep | No close Brand/Company Setup match; Census-specific field OK. |
| Production Use Status | Brand Status / External Display Status / status | Brand Basics / Hotel Census | census_specific_name_ok | keep | Must not collide with Brand Status or External Display Status. Census-only rendering gate. |
| Current Brand | Brand Setup - Brand Footprint | Brand Setup - Brand Basics | possible_duplicate_concept | add_alias/documentation_only | Related concept exists on another table; document difference rather than rename now. |
| Brand Family | Brand | Brand Setup - Brand Explorer Presentation | possible_duplicate_concept | add_alias/documentation_only | Related concept exists on another table; document difference rather than rename now. |
| Brand Explorer Slug if mapped | (code slug / Brand Name) | Brand Setup - Brand Basics | census_specific_name_ok | add_alias/documentation_only | No literal Brand Slug field on Brand Basics (slug is derived in code). Name is verbose but clear; optional later rename to Brand Explorer Slug. |
| Affiliation Status | Affiliation | Hotel Census | possible_duplicate_concept | add_alias/documentation_only | Related concept exists on another table; document difference rather than rename now. |
| Affiliation As-Of Date | Affiliation | Hotel Census | possible_duplicate_concept | add_alias/documentation_only | Related concept exists on another table; document difference rather than rename now. |
| Affiliation Start Date | — | — | census_specific_name_ok | keep | No close Brand/Company Setup match; Census-specific field OK. |
| Prior Brand | Brand Setup - Brand Footprint | Brand Setup - Brand Basics | possible_duplicate_concept | add_alias/documentation_only | Related concept exists on another table; document difference rather than rename now. |
| Future Opening Flag | — | — | census_specific_name_ok | keep | No close Brand/Company Setup match; Census-specific field OK. |
| Brand Confidence | Brand | Brand Setup - Brand Explorer Presentation | possible_duplicate_concept | add_alias/documentation_only | Related concept exists on another table; document difference rather than rename now. |
| Steward Review Status | Validation Status / Brand Status | Brand Setup - Brand Basics | census_specific_name_ok | keep | Distinct from Validation Status / Brand Status / Ownership Review Status. Steward lane is Census-ops specific. |
| Hotel Property Brand Affiliations | Brand Setup - Brand Footprint | Brand Setup - Brand Basics | possible_duplicate_concept | add_alias/documentation_only | Related concept exists on another table; document difference rather than rename now. |
| Hotel Property Source Evidence | — | — | census_specific_name_ok | keep | No close Brand/Company Setup match; Census-specific field OK. |
| Hotel Property Steward Review | — | — | census_specific_name_ok | keep | No close Brand/Company Setup match; Census-specific field OK. |
| Hotel Description - Source Text | Hotel Description | Hotel Census | possible_duplicate_concept | add_alias/documentation_only | Related concept exists on another table; document difference rather than rename now. |
| Hotel Description - AI Summary | Hotel Description | Hotel Census | possible_duplicate_concept | add_alias/documentation_only | Related concept exists on another table; document difference rather than rename now. |
| Short Property Summary | — | — | census_specific_name_ok | keep | No close Brand/Company Setup match; Census-specific field OK. |
| Property Positioning | Property Name | Hotel Ownership | possible_duplicate_concept | add_alias/documentation_only | Related concept exists on another table; document difference rather than rename now. |
| Hotel Class / Segment | — | — | census_specific_name_ok | keep | No close Brand/Company Setup match; Census-specific field OK. |
| Property Type | Property Type | Hotel Census | aligned_with_existing_name | keep | Closely matches existing naming; keep Census field as-is. |
| Asset Context | Partner Intelligence - Brand Asset Registry | Brand Setup - Brand Basics | possible_duplicate_concept | add_alias/documentation_only | Related concept exists on another table; document difference rather than rename now. |
| Market / Submarket | Market / Submarket / Dealality Market | Hotel Census | possible_duplicate_concept | add_alias/documentation_only | Legacy Hotel Census splits Market + Submarket + Dealality Market. Combined field is OK short-term; long-term prefer Dealality Market + corridor Submarket as separate fields. |
| Amenities - Source Text | — | — | census_specific_name_ok | keep | No close Brand/Company Setup match; Census-specific field OK. |
| Amenities - Structured Tags | — | — | census_specific_name_ok | keep | No close Brand/Company Setup match; Census-specific field OK. |
| F&B Flag | — | — | census_specific_name_ok | keep | No close Brand/Company Setup match; Census-specific field OK. |
| Meeting Space Flag | Largest Meeting Space | Hotel Census | possible_duplicate_concept | add_alias/documentation_only | Related concept exists on another table; document difference rather than rename now. |
| Fitness Flag | — | — | census_specific_name_ok | keep | No close Brand/Company Setup match; Census-specific field OK. |
| Pool Flag | — | — | census_specific_name_ok | keep | No close Brand/Company Setup match; Census-specific field OK. |
| Resort Amenities Flag | — | — | census_specific_name_ok | keep | No close Brand/Company Setup match; Census-specific field OK. |
| Extended Stay Amenity Flag | — | — | census_specific_name_ok | keep | No close Brand/Company Setup match; Census-specific field OK. |
| Parking Flag | — | — | census_specific_name_ok | keep | No close Brand/Company Setup match; Census-specific field OK. |
| Airport Shuttle Flag | — | — | census_specific_name_ok | keep | No close Brand/Company Setup match; Census-specific field OK. |
| Spa Flag | Spa (Y/N) | Hotel Census | possible_duplicate_concept | add_alias/documentation_only | Related concept exists on another table; document difference rather than rename now. |
| Beach / Waterfront Flag | — | — | census_specific_name_ok | keep | No close Brand/Company Setup match; Census-specific field OK. |
| Branded Residences Flag | Branded Residences Status | Brand Setup - Brand Basics | possible_duplicate_concept | add_alias/documentation_only | Related concept exists on another table; document difference rather than rename now. |
| Mixed-Use Flag | — | — | census_specific_name_ok | keep | No close Brand/Company Setup match; Census-specific field OK. |
| Rooms / Keys | rooms | Hotel Census | census_specific_name_ok | keep | Legacy Hotel Census uses rooms; Rooms / Keys is clearer for hospitality. Do not invent values until sourced. |
| Rooms Source URL | Branded Residences Source URL | Brand Setup - Brand Basics | possible_duplicate_concept | add_alias/documentation_only | Related concept exists on another table; document difference rather than rename now. |
| Rooms Confidence | rooms | Hotel Census | possible_duplicate_concept | add_alias/documentation_only | Related concept exists on another table; document difference rather than rename now. |
| Building / Asset Notes | — | — | census_specific_name_ok | keep | No close Brand/Company Setup match; Census-specific field OK. |
| Opening Date | — | — | census_specific_name_ok | keep | No close Brand/Company Setup match; Census-specific field OK. |
| Opening Date Source URL | Branded Residences Source URL | Brand Setup - Brand Basics | possible_duplicate_concept | add_alias/documentation_only | Related concept exists on another table; document difference rather than rename now. |
| Renovation / Conversion Status | — | — | census_specific_name_ok | keep | No close Brand/Company Setup match; Census-specific field OK. |
| Renovation / Conversion Date | — | — | census_specific_name_ok | keep | No close Brand/Company Setup match; Census-specific field OK. |
| Renovation / Conversion Source URL | Branded Residences Source URL | Brand Setup - Brand Basics | possible_duplicate_concept | add_alias/documentation_only | Related concept exists on another table; document difference rather than rename now. |
| Owner Name | Owner Company / Owner/Operator Name / Company Name | Hotel Census / Hotel Ownership / Company Profile | aligned_with_existing_name | keep | Aligns with Owner Company (legacy Hotel Census) and Owner/Operator Name (Hotel Ownership); distinct from Company Name on Company Profile. |
| Owner Type | Case Summary Owner Objective | Brand Setup - Brand Explorer Presentation | possible_duplicate_concept | add_alias/documentation_only | Related concept exists on another table; document difference rather than rename now. |
| Owner Source URL | Branded Residences Source URL | Brand Setup - Brand Basics | possible_duplicate_concept | add_alias/documentation_only | Related concept exists on another table; document difference rather than rename now. |
| Owner Confidence | Case Summary Owner Objective | Brand Setup - Brand Explorer Presentation | possible_duplicate_concept | add_alias/documentation_only | Related concept exists on another table; document difference rather than rename now. |
| Developer Name | Developer Profile Status | Company Profile | possible_duplicate_concept | add_alias/documentation_only | Related concept exists on another table; document difference rather than rename now. |
| Developer Source URL | Branded Residences Source URL | Brand Setup - Brand Basics | possible_duplicate_concept | add_alias/documentation_only | Related concept exists on another table; document difference rather than rename now. |
| Developer Confidence | Developer Profile Status | Company Profile | possible_duplicate_concept | add_alias/documentation_only | Related concept exists on another table; document difference rather than rename now. |
| Ownership Review Status | Owner Profile Status / Branded Residences Review Status | Company Profile / Brand Basics | aligned_with_existing_name | keep | Parallel to Brand Review Status patterns and Company Profile Owner Profile Status. |
| Operator / Management Company | Management Company / company_name | Hotel Census / Operator Setup - Master | census_specific_name_ok | keep | Bridges Management Company (legacy Hotel Census) and Operator company_name (Operator Setup). Slash form is explicit for property-level ops. |
| Operator Type | Operator | Operator Setup - Profile & Positioning | possible_duplicate_concept | add_alias/documentation_only | Related concept exists on another table; document difference rather than rename now. |
| Management Model | User Management | Brand Setup - Brand Basics | possible_duplicate_concept | add_alias/documentation_only | Related concept exists on another table; document difference rather than rename now. |
| Operator Source URL | Operator Setup - Operator Basics | Brand Setup - Brand Basics | possible_duplicate_concept | add_alias/documentation_only | Related concept exists on another table; document difference rather than rename now. |
| Operator Confidence | Operator | Operator Setup - Profile & Positioning | possible_duplicate_concept | add_alias/documentation_only | Related concept exists on another table; document difference rather than rename now. |
| Operator Review Status | Operator Profile Status | Company Profile | aligned_with_existing_name | keep | Parallel to Operator Profile Status on Company Profile. |
| Possible Operator Target | Operator Setup - Operator Basics | Brand Setup - Brand Basics | possible_duplicate_concept | add_alias/documentation_only | Related concept exists on another table; document difference rather than rename now. |
| Independent Hotel Flag | — | — | census_specific_name_ok | keep | No close Brand/Company Setup match; Census-specific field OK. |
| Independent Classification | brand_soft_independent_narrative | Operator Setup - Profile & Positioning | possible_duplicate_concept | add_alias/documentation_only | Related concept exists on another table; document difference rather than rename now. |
| Brand-Unassigned Reason | Brand Setup - Brand Footprint | Brand Setup - Brand Basics | possible_duplicate_concept | add_alias/documentation_only | Related concept exists on another table; document difference rather than rename now. |
| Possible Soft-Brand Candidate | Brand Setup - Brand Footprint | Brand Setup - Brand Basics | possible_duplicate_concept | add_alias/documentation_only | Related concept exists on another table; document difference rather than rename now. |
| Possible Brand Conversion Candidate | Brand Setup - Brand Footprint | Brand Setup - Brand Basics | possible_duplicate_concept | add_alias/documentation_only | Related concept exists on another table; document difference rather than rename now. |
| Possible Owner Outreach Target | — | — | census_specific_name_ok | keep | No close Brand/Company Setup match; Census-specific field OK. |
| Possible Financing Target | — | — | census_specific_name_ok | keep | No close Brand/Company Setup match; Census-specific field OK. |
| Possible Dealality Opportunity | — | — | census_specific_name_ok | keep | No close Brand/Company Setup match; Census-specific field OK. |
| Data Confidence Tier | Data Confidence | Hotel Census | possible_duplicate_concept | add_alias/documentation_only | Related concept exists on another table; document difference rather than rename now. |
| Relationship Confidence | brand_narrative_relationship | Operator Setup - Profile & Positioning | census_specific_name_ok | keep | Census-specific for ownership/operator relationship certainty; not a Brand Setup duplicate. |
| Last Verified Date | Last Reviewed Date | Brand Setup - Brand Basics / Operator Setup - Master | naming_conflict | rename_later | Brand/Operator Setup standard is Last Reviewed Date. Prefer aligning to Last Reviewed Date in v1.1.1 (documentation + optional rename later). Does not block enrichment. |
| Next Review Needed | — | — | census_specific_name_ok | keep | No close Brand/Company Setup match; Census-specific field OK. |
| Enrichment Status | — | — | census_specific_name_ok | keep | Distinct from Validation Status / submission_status. Tracks enrichment lane progress. |
| Enrichment Priority | — | — | census_specific_name_ok | keep | No close Brand/Company Setup match; Census-specific field OK. |
| Human Review Required | — | — | census_specific_name_ok | keep | No close Brand/Company Setup match; Census-specific field OK. |
| Notes for Steward | — | — | census_specific_name_ok | keep | No close Brand/Company Setup match; Census-specific field OK. |

## 5. Amenities simplification review

| Amenity Field | Current Type | Classification | Recommendation | Reason |
| --- | --- | --- | --- | --- |
| Amenities - Source Text | multilineText | keep_as_source_text | keep | Primary amenity model — source text + structured tags. |
| Amenities - Structured Tags | multilineText | keep_as_source_text | keep | Primary amenity model — source text + structured tags. |
| F&B Flag | checkbox | keep_as_strategic_flag | keep | High-signal strategic flag useful across BE / OE / Owner lanes. |
| Meeting Space Flag | checkbox | keep_as_strategic_flag | keep | High-signal strategic flag useful across BE / OE / Owner lanes. |
| Fitness Flag | checkbox | possible_overmodeling | move_to_structured_tags_later | Prefer Amenities - Structured Tags for fitness/pool/parking/shuttle/spa/beach; hide from default views until consolidated. |
| Pool Flag | checkbox | possible_overmodeling | move_to_structured_tags_later | Prefer Amenities - Structured Tags for fitness/pool/parking/shuttle/spa/beach; hide from default views until consolidated. |
| Resort Amenities Flag | checkbox | founder_decision_needed | rename_later | Prefer strategic name Resort / Leisure Flag in v1.1.1; keep current flag until then. |
| Extended Stay Amenity Flag | checkbox | founder_decision_needed | rename_later | Prefer Extended Stay Flag naming alignment; Amenity suffix is redundant with Structured Tags model. |
| Parking Flag | checkbox | possible_overmodeling | move_to_structured_tags_later | Prefer Amenities - Structured Tags for fitness/pool/parking/shuttle/spa/beach; hide from default views until consolidated. |
| Airport Shuttle Flag | checkbox | possible_overmodeling | move_to_structured_tags_later | Prefer Amenities - Structured Tags for fitness/pool/parking/shuttle/spa/beach; hide from default views until consolidated. |
| Spa Flag | checkbox | possible_overmodeling | move_to_structured_tags_later | Prefer Amenities - Structured Tags for fitness/pool/parking/shuttle/spa/beach; hide from default views until consolidated. |
| Beach / Waterfront Flag | checkbox | possible_overmodeling | move_to_structured_tags_later | Prefer Amenities - Structured Tags for fitness/pool/parking/shuttle/spa/beach; hide from default views until consolidated. |
| Branded Residences Flag | checkbox | keep_as_strategic_flag | keep | High-signal strategic flag useful across BE / OE / Owner lanes. |
| Mixed-Use Flag | checkbox | keep_as_strategic_flag | keep | High-signal strategic flag useful across BE / OE / Owner lanes. |

## 6. Master Census foundation review

```json
{
  "census_666": true,
  "affiliations_666": true,
  "evidence_666": true,
  "steward_4": true,
  "no_duplicates": true,
  "production_use_all": true,
  "enrichment_not_started_all": true,
  "human_review_only_held": true,
  "enrichment_fields_blank": true,
  "no_zero_zero": true
}
```

## 7. Record sample review by family

### IHG

- Totals: {"rows":195,"branded":186,"soft":9,"unconfirmed":0,"held":0}
- Issues: 0

### Hilton

- Totals: {"rows":102,"branded":72,"soft":26,"unconfirmed":0,"held":0}
- Issues: 0

### Choice

- Totals: {"rows":68,"branded":56,"soft":12,"unconfirmed":0,"held":0}
- Issues: 0

### Marriott

- Totals: {"rows":301,"branded":255,"soft":42,"unconfirmed":4,"held":4}
- Issues: 0

## 8. Held record review

| Property | Family | Classification | Reason | Next action |
| --- | --- | --- | --- | --- |
| SJ Grand Hotel Monterrey | Marriott | Brand-Unconfirmed | marriott_steward_manual_review_required | needs brand steward review |
| Gran Hotel de Puebla by HNF | Marriott | Brand-Unconfirmed | marriott_steward_exclude_from_brand_completion | exclude from Brand Explorer |
| Hotel Guadalajara Country Club by HNF | Marriott | Brand-Unconfirmed | marriott_steward_exclude_from_brand_completion | exclude from Brand Explorer |
| CASA MAYOR Saltillo, Hotel Hacienda | Marriott | Brand-Unconfirmed | marriott_steward_manual_review_required | needs brand steward review |

## 9. Enrichment readiness scoring

| Lane | Composite | BE | OE | Owner | Risk |
| --- | ---: | ---: | ---: | ---: | ---: |
| C_property_type_asset_context | 4.33 | 4 | 5 | 4 | 1 |
| B_amenities | 4.17 | 5 | 4 | 4 | 2 |
| A_hotel_descriptions | 4 | 5 | 3 | 4 | 2 |
| G_rooms_keys | 3.17 | 3 | 4 | 4 | 4 |
| D_independent_hotel_classification | 3 | 2 | 3 | 4 | 3 |
| F_operator_management | 3 | 2 | 5 | 4 | 5 |
| E_owner_developer | 2.83 | 2 | 3 | 5 | 5 |
| H_opening_renovation_dates | 2.5 | 3 | 3 | 3 | 4 |

## 10. Recommended first enrichment lane

Start with descriptions + amenities + property type / asset context. Do not start with owner/operator until sourcing process is defined.

## 11. Fields safe to enrich next

- Hotel Description - Source Text
- Hotel Description - AI Summary
- Short Property Summary
- Property Positioning
- Amenities - Source Text
- Amenities - Structured Tags
- F&B Flag
- Meeting Space Flag
- Resort Amenities Flag
- Extended Stay Amenity Flag
- Mixed-Use Flag
- Branded Residences Flag
- Property Type
- Asset Context
- Hotel Class / Segment
- Market / Submarket

## 12. Fields that must remain blank until sourced

- Owner Name
- Owner Type
- Owner Source URL
- Developer Name
- Operator / Management Company
- Management Model
- Rooms / Keys
- Opening Date
- Renovation / Conversion Date
- Latitude/Longitude when unknown (never 0,0)

## 13. Possible v1.1.1 cleanup plan (do not apply)

```json
{
  "A_should_fix_before_enrichment": [],
  "B_can_live_with_for_now": [
    {
      "current_field": "Market / Submarket",
      "issue": "Legacy Hotel Census splits Market + Submarket + Dealality Market. Combined field is OK short-term; long-term prefer Dealality Market + corridor Submarket as separate fields.",
      "proposed_action": "add_alias/documentation_only",
      "risk": "low",
      "requires_data_migration": false,
      "founder_decision": false,
      "bucket": "B"
    },
    {
      "current_field": "Fitness Flag",
      "issue": "Prefer Amenities - Structured Tags for fitness/pool/parking/shuttle/spa/beach; hide from default views until consolidated.",
      "proposed_action": "move_to_structured_tags_later",
      "risk": "low",
      "requires_data_migration": false,
      "founder_decision": false,
      "bucket": "B",
      "note": "Can live with for now; hide from default views before enrichment UI work"
    },
    {
      "current_field": "Pool Flag",
      "issue": "Prefer Amenities - Structured Tags for fitness/pool/parking/shuttle/spa/beach; hide from default views until consolidated.",
      "proposed_action": "move_to_structured_tags_later",
      "risk": "low",
      "requires_data_migration": false,
      "founder_decision": false,
      "bucket": "B",
      "note": "Can live with for now; hide from default views before enrichment UI work"
    },
    {
      "current_field": "Parking Flag",
      "issue": "Prefer Amenities - Structured Tags for fitness/pool/parking/shuttle/spa/beach; hide from default views until consolidated.",
      "proposed_action": "move_to_structured_tags_later",
      "risk": "low",
      "requires_data_migration": false,
      "founder_decision": false,
      "bucket": "B",
      "note": "Can live with for now; hide from default views before enrichment UI work"
    },
    {
      "current_field": "Airport Shuttle Flag",
      "issue": "Prefer Amenities - Structured Tags for fitness/pool/parking/shuttle/spa/beach; hide from default views until consolidated.",
      "proposed_action": "move_to_structured_tags_later",
      "risk": "low",
      "requires_data_migration": false,
      "founder_decision": false,
      "bucket": "B",
      "note": "Can live with for now; hide from default views before enrichment UI work"
    },
    {
      "current_field": "Spa Flag",
      "issue": "Prefer Amenities - Structured Tags for fitness/pool/parking/shuttle/spa/beach; hide from default views until consolidated.",
      "proposed_action": "move_to_structured_tags_later",
      "risk": "low",
      "requires_data_migration": false,
      "founder_decision": false,
      "bucket": "B",
      "note": "Can live with for now; hide from default views before enrichment UI work"
    },
    {
      "current_field": "Beach / Waterfront Flag",
      "issue": "Prefer Amenities - Structured Tags for fitness/pool/parking/shuttle/spa/beach; hide from default views until consolidated.",
      "proposed_action": "move_to_structured_tags_later",
      "risk": "low",
      "requires_data_migration": false,
      "founder_decision": false,
      "bucket": "B",
      "note": "Can live with for now; hide from default views before enrichment UI work"
    }
  ],
  "C_founder_decision_needed": [
    {
      "current_field": "Last Verified Date",
      "issue": "Brand/Operator Setup standard is Last Reviewed Date. Prefer aligning to Last Reviewed Date in v1.1.1 (documentation + optional rename later). Does not block enrichment.",
      "proposed_action": "rename_later",
      "risk": "medium",
      "requires_data_migration": true,
      "founder_decision": true,
      "bucket": "C"
    },
    {
      "current_field": "Resort Amenities Flag",
      "issue": "Prefer strategic name Resort / Leisure Flag in v1.1.1; keep current flag until then.",
      "proposed_action": "rename_later",
      "risk": "low",
      "requires_data_migration": true,
      "founder_decision": true,
      "bucket": "C"
    },
    {
      "current_field": "Extended Stay Amenity Flag",
      "issue": "Prefer Extended Stay Flag naming alignment; Amenity suffix is redundant with Structured Tags model.",
      "proposed_action": "rename_later",
      "risk": "low",
      "requires_data_migration": true,
      "founder_decision": true,
      "bucket": "C"
    }
  ],
  "D_do_not_change": [
    {
      "current_field": "Property Name",
      "proposed_action": "keep"
    },
    {
      "current_field": "Property Identity Key",
      "proposed_action": "keep"
    },
    {
      "current_field": "Family / Source Family",
      "proposed_action": "keep"
    },
    {
      "current_field": "Country",
      "proposed_action": "keep"
    },
    {
      "current_field": "State / Region",
      "proposed_action": "keep"
    },
    {
      "current_field": "City",
      "proposed_action": "keep"
    },
    {
      "current_field": "Latitude",
      "proposed_action": "keep"
    },
    {
      "current_field": "Longitude",
      "proposed_action": "keep"
    },
    {
      "current_field": "Official Property URL",
      "proposed_action": "keep"
    },
    {
      "current_field": "Source URL",
      "proposed_action": "keep"
    },
    {
      "current_field": "Source Type",
      "proposed_action": "keep"
    },
    {
      "current_field": "Discovery Date",
      "proposed_action": "keep"
    },
    {
      "current_field": "VIC Freeze Hash",
      "proposed_action": "keep"
    },
    {
      "current_field": "Identity Confidence",
      "proposed_action": "keep"
    },
    {
      "current_field": "Production Use Status",
      "proposed_action": "keep"
    },
    {
      "current_field": "Affiliation Start Date",
      "proposed_action": "keep"
    },
    {
      "current_field": "Future Opening Flag",
      "proposed_action": "keep"
    },
    {
      "current_field": "Steward Review Status",
      "proposed_action": "keep"
    },
    {
      "current_field": "Hotel Property Source Evidence",
      "proposed_action": "keep"
    },
    {
      "current_field": "Hotel Property Steward Review",
      "proposed_action": "keep"
    }
  ]
}
```

## 14. Brand Explorer safety result

```json
{
  "gate_results": [
    {
      "cmd": "universe+semantic+quiet_pvql+momentum+mandatory",
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

## 15. Final recommendation

Proceed to descriptions + amenities + property type enrichment; schedule non-blocking v1.1.1 amenity/naming cleanup (hide tag-level amenity flags; decide Last Verified vs Last Reviewed).
