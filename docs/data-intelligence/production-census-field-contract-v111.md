# Production Census Field Contract v1.1.1

**Status:** `production_census_field_contract_v111_frozen_radar_fields_missing_v112_needed`
**Contract ID:** `production_census_field_contract_v1.1.1`
**Generated:** 2026-08-05T12:22:46.161Z
**Read-only freeze:** true (no Airtable / Brand Explorer writes)

## 1. Executive summary

- Census: 666 records / 95 fields
- Validation pass: true
- Radar fields present: 0/6
- First enrichment may proceed: true
- v1.1.2 needed for Radar: true

## 2. Frozen Census field groups

### A. Core Identity

- **Property Name** — `contract_required` · tags: public_safe_if_approved
- **Canonical Property Name** — `contract_optional` · tags: public_safe_if_approved
- **Property Identity Key** — `contract_required` · tags: internal_only, source_only
- **Phone** — `contract_optional` · tags: do_not_use_publicly_yet
- **Official Property URL** — `contract_optional` · tags: public_safe_if_approved
- **Future Opening Flag** — `contract_optional` · tags: do_not_use_publicly_yet

### B. Geography

- **Country** — `contract_required` · tags: public_safe_if_approved
- **State / Region** — `contract_required` · tags: public_safe_if_approved
- **City** — `contract_required` · tags: public_safe_if_approved
- **Address** — `contract_optional` · tags: do_not_use_publicly_yet
- **Latitude** — `contract_optional` · tags: public_safe_if_approved, do_not_use_publicly_yet — Present for many rows; never invent 0,0
- **Longitude** — `contract_optional` · tags: public_safe_if_approved, do_not_use_publicly_yet — Present for many rows; never invent 0,0
- **Market / Submarket** — `enrichment_target` · tags: public_safe_if_approved — First enrichment lane — Dealality geography, not STR

### C. Brand / Affiliation

- **Current Brand** — `contract_required` · tags: public_safe_if_approved
- **Brand Family** — `contract_optional` · tags: public_safe_if_approved
- **Brand Explorer Slug if mapped** — `internal_only` · tags: do_not_use_publicly_yet
- **Affiliation Status** — `contract_required` · tags: public_safe_if_approved
- **Affiliation As-Of Date** — `contract_optional` · tags: source_only
- **Affiliation Start Date** — `contract_optional` · tags: source_only, do_not_use_publicly_yet — Blocked in first enrichment lane
- **Prior Brand** — `contract_optional` · tags: do_not_use_publicly_yet
- **Brand Confidence** — `governance_field` · tags: internal_only

### D. Source Evidence

- **Family / Source Family** — `contract_required` · tags: source_only, internal_only
- **Source URL** — `contract_required` · tags: source_only
- **Source Type** — `contract_optional` · tags: source_only, internal_only
- **Source Confidence** — `governance_field` · tags: source_only, internal_only
- **Discovery Date** — `contract_optional` · tags: source_only, internal_only
- **VIC Freeze Hash** — `contract_required` · tags: source_only, internal_only
- **Hotel Property Source Evidence** — `internal_only` — Link inverse

### F. Description / Amenities

- **Hotel Description - Source Text** — `enrichment_target` · tags: source_only, public_safe_if_approved — First enrichment lane
- **Hotel Description - AI Summary** — `enrichment_target` · tags: public_safe_if_approved — First enrichment lane — governed AI only
- **Short Property Summary** — `enrichment_target` · tags: do_not_use_publicly_yet — Not in first lane
- **Property Positioning** — `enrichment_target` · tags: do_not_use_publicly_yet — Not in first lane
- **Amenities - Source Text** — `enrichment_target` · tags: source_only, public_safe_if_approved — First enrichment lane
- **Amenities - Structured Tags** — `enrichment_target` · tags: public_safe_if_approved — First enrichment lane
- **F&B Flag** — `enrichment_target` · tags: public_safe_if_approved — First enrichment lane
- **Meeting Space Flag** — `enrichment_target` · tags: public_safe_if_approved — First enrichment lane
- **Resort / Leisure Flag** — `enrichment_target` · tags: public_safe_if_approved — First enrichment lane
- **Extended Stay Flag** — `enrichment_target` · tags: public_safe_if_approved — First enrichment lane
- **Branded Residences Flag** — `enrichment_target` · tags: public_safe_if_approved — First enrichment lane
- **Mixed-Use Flag** — `enrichment_target` · tags: public_safe_if_approved — First enrichment lane
- **Fitness Flag** — `contract_optional` · tags: do_not_use_publicly_yet, internal_only — Over-modeled — keep hidden; do not populate in first lane
- **Pool Flag** — `contract_optional` · tags: do_not_use_publicly_yet, internal_only — Over-modeled — keep hidden; do not populate in first lane
- **Parking Flag** — `contract_optional` · tags: do_not_use_publicly_yet, internal_only — Over-modeled — keep hidden; do not populate in first lane
- **Airport Shuttle Flag** — `contract_optional` · tags: do_not_use_publicly_yet, internal_only — Over-modeled — keep hidden; do not populate in first lane
- **Spa Flag** — `contract_optional` · tags: do_not_use_publicly_yet, internal_only — Over-modeled — keep hidden; do not populate in first lane
- **Beach / Waterfront Flag** — `contract_optional` · tags: do_not_use_publicly_yet, internal_only — Over-modeled — keep hidden; do not populate in first lane

### G. Asset Context

- **Hotel Class / Segment** — `enrichment_target` · tags: do_not_use_publicly_yet — Not in first lane
- **Property Type** — `enrichment_target` · tags: public_safe_if_approved — First enrichment lane
- **Asset Context** — `enrichment_target` · tags: public_safe_if_approved — First enrichment lane
- **Rooms / Keys** — `enrichment_target` · tags: source_only, do_not_use_publicly_yet — Blocked in first enrichment lane
- **Rooms Source URL** — `source_only` · tags: do_not_use_publicly_yet
- **Rooms Confidence** — `governance_field` · tags: internal_only
- **Building / Asset Notes** — `enrichment_target` · tags: do_not_use_publicly_yet
- **Opening Date** — `enrichment_target` · tags: source_only, do_not_use_publicly_yet — Blocked in first enrichment lane
- **Opening Date Source URL** — `source_only` · tags: do_not_use_publicly_yet
- **Renovation / Conversion Status** — `enrichment_target` · tags: do_not_use_publicly_yet
- **Renovation / Conversion Date** — `enrichment_target` · tags: source_only, do_not_use_publicly_yet — Blocked in first enrichment lane
- **Renovation / Conversion Source URL** — `source_only` · tags: do_not_use_publicly_yet

### H. Owner / Developer

- **Owner Name** — `enrichment_target` · tags: source_only, do_not_use_publicly_yet — Blocked in first enrichment lane
- **Owner Type** — `enrichment_target` · tags: do_not_use_publicly_yet
- **Owner Source URL** — `source_only` · tags: do_not_use_publicly_yet
- **Owner Confidence** — `governance_field` · tags: internal_only
- **Developer Name** — `enrichment_target` · tags: source_only, do_not_use_publicly_yet — Blocked in first enrichment lane
- **Developer Source URL** — `source_only` · tags: do_not_use_publicly_yet
- **Developer Confidence** — `governance_field` · tags: internal_only
- **Ownership Review Status** — `governance_field` · tags: internal_only

### I. Operator / Management

- **Operator / Management Company** — `enrichment_target` · tags: source_only, do_not_use_publicly_yet — Blocked in first enrichment lane
- **Operator Type** — `enrichment_target` · tags: do_not_use_publicly_yet
- **Management Model** — `enrichment_target` · tags: do_not_use_publicly_yet
- **Operator Source URL** — `source_only` · tags: do_not_use_publicly_yet
- **Operator Confidence** — `governance_field` · tags: internal_only
- **Operator Review Status** — `governance_field` · tags: internal_only
- **Possible Operator Target** — `internal_only` · tags: do_not_use_publicly_yet

### J. Independent / Brand-Unconfirmed Handling

- **Independent Hotel Flag** — `contract_optional` · tags: do_not_use_publicly_yet
- **Independent Classification** — `contract_optional` · tags: do_not_use_publicly_yet
- **Brand-Unassigned Reason** — `governance_field` · tags: internal_only
- **Possible Soft-Brand Candidate** — `internal_only` · tags: do_not_use_publicly_yet
- **Possible Brand Conversion Candidate** — `internal_only` · tags: do_not_use_publicly_yet
- **Possible Owner Outreach Target** — `internal_only` · tags: do_not_use_publicly_yet
- **Possible Financing Target** — `internal_only` · tags: do_not_use_publicly_yet
- **Possible Dealality Opportunity** — `internal_only` · tags: do_not_use_publicly_yet

### K. Enrichment Governance

- **Data Eligible** — `governance_field` · tags: internal_only
- **Identity Confidence** — `governance_field` · tags: internal_only
- **Production Use Status** — `governance_field` · tags: contract_required, internal_only — Must remain Census Only / Not Owner-Facing until product approval
- **Data Confidence Tier** — `governance_field` · tags: internal_only
- **Relationship Confidence** — `governance_field` · tags: internal_only
- **Last Reviewed Date** — `governance_field` · tags: internal_only
- **Next Review Needed** — `governance_field` · tags: internal_only
- **Enrichment Status** — `governance_field` · tags: contract_required
- **Enrichment Priority** — `governance_field` · tags: internal_only

### L. Steward Review

- **Steward Review Status** — `governance_field` · tags: internal_only
- **Human Review Required** — `governance_field` · tags: contract_required
- **Notes for Steward** — `governance_field` · tags: internal_only
- **Hotel Property Brand Affiliations** — `internal_only` — Link inverse
- **Hotel Property Steward Review** — `internal_only` — Link inverse

## 3. Required fields

- Property Name
- Property Identity Key
- Country
- State / Region
- City
- Current Brand
- Affiliation Status
- Family / Source Family
- Source URL
- VIC Freeze Hash
- Production Use Status
- Enrichment Status
- Human Review Required

## 4. Optional fields

- Canonical Property Name
- Phone
- Official Property URL
- Future Opening Flag
- Address
- Latitude
- Longitude
- Brand Family
- Affiliation As-Of Date
- Affiliation Start Date
- Prior Brand
- Source Type
- Discovery Date
- Fitness Flag
- Pool Flag
- Parking Flag
- Airport Shuttle Flag
- Spa Flag
- Beach / Waterfront Flag
- Independent Hotel Flag
- Independent Classification

## 5. Enrichment target fields

- Market / Submarket
- Hotel Description - Source Text
- Hotel Description - AI Summary
- Short Property Summary
- Property Positioning
- Amenities - Source Text
- Amenities - Structured Tags
- F&B Flag
- Meeting Space Flag
- Resort / Leisure Flag
- Extended Stay Flag
- Branded Residences Flag
- Mixed-Use Flag
- Hotel Class / Segment
- Property Type
- Asset Context
- Rooms / Keys
- Building / Asset Notes
- Opening Date
- Renovation / Conversion Status
- Renovation / Conversion Date
- Owner Name
- Owner Type
- Developer Name
- Operator / Management Company
- Operator Type
- Management Model

## 6. Public / Radar display fields

```json
{
  "required_for_radar_integration": [
    "Radar Display Status",
    "Radar Display Reason",
    "Radar Geography Status",
    "Public Census Eligibility",
    "Public Display Confidence",
    "Public Display Review Status"
  ],
  "present": [],
  "missing": [
    "Radar Display Status",
    "Radar Display Reason",
    "Radar Geography Status",
    "Public Census Eligibility",
    "Public Display Confidence",
    "Public Display Review Status"
  ],
  "all_present": false,
  "v112_needed": true,
  "blocks_first_enrichment": false,
  "note": "Radar/public display fields are absent. Do not invent them. Schedule schema v1.1.2 before Radar integration. Description/amenity enrichment may proceed under this freeze."
}
```

## 7. Fields safe for first enrichment

- Hotel Description - Source Text
- Hotel Description - AI Summary
- Amenities - Source Text
- Amenities - Structured Tags
- Property Type
- Asset Context
- Market / Submarket
- Resort / Leisure Flag
- Extended Stay Flag
- F&B Flag
- Meeting Space Flag
- Mixed-Use Flag
- Branded Residences Flag

## 8. Fields still blocked

- Owner Name
- Developer Name
- Operator / Management Company
- Rooms / Keys
- Opening Date
- Renovation / Conversion Date
- Affiliation Start Date
- Brand Explorer public fields
- Recent Momentum
- Company Validated
- Brand Verified

Over-modeled amenity flags (not in first lane):
- Fitness Flag
- Pool Flag
- Parking Flag
- Airport Shuttle Flag
- Spa Flag
- Beach / Waterfront Flag

## 9. Steward review rules

```json
{
  "human_review_required_true_count_expected": 4,
  "human_review_required_true_count_actual": 4,
  "steward_view": "Census - Steward Review",
  "steward_filter": "Human Review Required is checked",
  "rules": [
    "Do not clear Human Review Required on held records without steward decision.",
    "Use Notes for Steward and Brand-Unassigned Reason for hold rationale.",
    "Enrichment may run on Data Eligible rows with Enrichment Status = Not Started, but held records stay in Steward Review queue.",
    "Production Use Status must remain Census Only / Not Owner-Facing until explicit product approval.",
    "Never write Brand Explorer Company Validated / Brand Verified / Recent Momentum from Census enrichment."
  ]
}
```

## 10. Brand Explorer safety result

```json
{
  "gates": [
    {
      "label": "active_universe_sot",
      "ok": true,
      "exit_code": 0,
      "command": "npm run brand-explorer-active-universe-source-of-truth -- --dry-run"
    },
    {
      "label": "global_active_semantic",
      "ok": true,
      "exit_code": 0,
      "command": "npm run brand-explorer-global-active-semantic-audit -- --dry-run --fresh"
    },
    {
      "label": "pvql_quiet",
      "ok": true,
      "exit_code": 0,
      "command": "node scripts/brand-explorer-quiet-sequential-pvql.mjs"
    },
    {
      "label": "momentum_evidence",
      "ok": true,
      "exit_code": 0,
      "command": "npm run test:brand-explorer-recent-momentum-evidence-quality"
    },
    {
      "label": "mandatory_release_gates",
      "ok": true,
      "exit_code": 0,
      "command": "npm run test:brand-explorer-mandatory-release-gates"
    }
  ],
  "all_pass": true,
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
  "summary": {
    "active_universe": 62,
    "semantic_c_h_m": "0/0/0",
    "pvql": "PASS",
    "momentum": "PASS",
    "mandatory_gates": "PASS"
  },
  "expected": {
    "active_universe": 62,
    "semantic_c_h_m": "0/0/0",
    "pvql": "PASS",
    "momentum": "PASS",
    "mandatory_gates": "PASS"
  }
}
```

## 11. Final recommendation

Proceed with first enrichment lane (descriptions + amenities + property type/asset context). Schedule schema v1.1.2 for Radar/public display fields before any Radar integration or public Census surfacing.

## Appendix — Census validation

```json
{
  "base": "Deal Capture Platform",
  "table": "Hotel Property Census",
  "table_id": "tbl9aY5ijiuIzzWam",
  "record_count": 666,
  "field_count": 95,
  "duplicates": 0,
  "production_use_ok": 666,
  "enrichment_not_started": 666,
  "human_review_true": 4,
  "description_filled": 0,
  "amenities_filled": 0,
  "owner_filled": 0,
  "operator_filled": 0,
  "rooms_filled": 0,
  "opening_filled": 0,
  "renovation_filled": 0,
  "affiliation_start_filled": 0,
  "zero_zero": 0,
  "renames_present": {
    "Last Reviewed Date": true,
    "Resort / Leisure Flag": true,
    "Extended Stay Flag": true
  },
  "old_names_absent": {
    "Last Verified Date": true,
    "Resort Amenities Flag": true,
    "Extended Stay Amenity Flag": true
  },
  "views": [
    {
      "name": "Grid view",
      "visible_field_count": 89
    },
    {
      "name": "Census - Core Identity",
      "visible_field_count": 16
    },
    {
      "name": "Census - Enrichment",
      "visible_field_count": 25
    },
    {
      "name": "Census - Owner Operator",
      "visible_field_count": 17
    },
    {
      "name": "Census - Steward Review",
      "visible_field_count": 14
    }
  ],
  "pass": true
}
```
