# HBX Census Schema + Identity Linkage v1

**Status:** `production_census_hbx_census_schema_and_identity_linkage_v1_complete`  
**Objective:** `hbx-census-schema-and-identity-linkage-v1`  
**Generated:** 2026-08-09T15:40:33.662Z  
**Dry run:** false  
**Airtable writes:** **470**

## Schema
- Fields present before: **0** of 16 target fields
- Fields created this run: **16**
- Fields still missing: **0**
- Hotel Property Census field count after: **132**

### Created
- `HBX Hotel Code` (singleLineText)
- `HBX Chain Code` (singleLineText)
- `HBX Category Code` (singleLineText)
- `HBX Category Name` (singleLineText)
- `HBX Accommodation Type` (singleLineText)
- `HBX License / Registration Number` (singleLineText)
- `HBX Last Update` (date)
- `HBX Linkage Confidence` (singleSelect: High, Medium, Low, Review Needed)
- `HBX Source Status` (singleSelect: Active, Matched, Candidate, Held, Conflict, Needs Review)
- `HBX Content Review Status` (singleSelect: Internal Only, Needs Review, License Review Needed, Approved Internal, Hold)
- `Phone Confidence` (singleSelect: High, Medium, Low, Needs Review)
- `Phone Source Type` (singleSelect: hbx_content_api, dataforseo, official_property_page, official_brand_directory, steward_review, other)
- `Phone Source URL` (url)
- `Phone Review Status` (singleSelect: Internal Only, Needs Review, Approved, Hold)
- `Phone Reviewed Date` (date)
- `Phone Notes` (multilineText)

## Linkage (existing_match_high only)
- Records reviewed: **470** unique Census IDs
- Records updated: **470**
- HBX Hotel Codes written: **470**
- HBX Chain Codes written: **392** (78 candidates had blank chain_code)
- HBX Category Codes written: **470**
- Category name / accommodation / license / lastUpdate: not in Wave1 pack → skipped (no invent)
- Phone provenance writes: **469**
- Notes for Steward: left in place (Phase 1 `hbx_linkage` notes not stripped)
- Conflicts: **83** (census Phone differs from HBX phonehotel → Phone Confidence/Review = Needs Review; Phone value not rewritten)

## Confirmations
- No inserts: **true**
- No Rooms / Keys: **true**
- No coordinates / images / descriptions / facilities: **true**
- No Address / Phone / Official Property URL rewrites: **true**
- No owner / operator / dates / Recent Momentum / Company Validated / Brand Verified: **true**
- Hotel Property Census only (`tbl9aY5ijiuIzzWam`): **true**
- No Brand Explorer / Brand Setup writes: **true**

## Validation
- `npm run test:census-autopilot` — pass
- `npm run test:census-brand-governance-scope` — pass
- `npm run dealality:batch-learning-audit` — pass
