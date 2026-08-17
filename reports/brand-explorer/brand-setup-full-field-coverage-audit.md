# Brand Setup — Full Field Coverage Audit (Read-Only)

**Status:** `brand_setup_full_field_coverage_audit_complete_additional_validation_needed`
**Generated:** 2026-08-05T16:31:23.805Z
**Airtable writes:** false · **Patches applied:** false

## 1. Executive summary

**Founder question:** Is the current process reviewing all Brand Setup fields?

**Answer: No.** Active-62 Brand Explorer gates cover Presentation owner-facing text (Title / Body / Case Summary*) plus selected Brand Basics governance fields (Brand Status, validation/footnote inputs). They do **not** cover the full Brand Setup schema (Footprint, Fee Structure, Standards, Deal Terms, Project Fit, Operational Support, Legal Terms, Loyalty, Sustainability, etc.).

- Active universe: **62** · Basics matched: **62** · Presentation rows matched: **8085**
- Brand Setup tables reviewed: **12** · Total fields inventoried: **601**
- Batch 1A patches (plan): **36** · Batch 1B: **32** · Batch 1A safe within narrow scope: **true**
- Note: local apply report shows Batch 1A already applied previously; **this audit did not apply anything**.

## 2. All Brand Setup tables reviewed

| Table | Fields | Brand Setup? |
| --- | ---: | --- |
| Brand Setup - Brand Basics | 81 | true |
| Brand Setup - Sustainability & ESG | 11 | true |
| Brand Setup - Brand Footprint | 138 | true |
| Brand Setup - Project Fit | 57 | true |
| Brand Setup - Portfolio & Performance | 31 | true |
| Brand Setup - Brand Standards | 28 | true |
| Brand Setup - Deal Terms | 28 | true |
| Brand Setup - Fee Structure | 66 | true |
| Brand Setup - Operational Support | 57 | true |
| Brand Setup - Legal Terms | 20 | true |
| Brand Setup - Loyalty & Commercial | 19 | true |
| Brand Setup - Brand Explorer Presentation | 28 | true |
| Partner Intelligence - Brand Asset Registry | 37 | false |

## 3. Complete field inventory

Full machine-readable inventory: `reports/brand-explorer/brand-setup-full-field-coverage-audit.json` → `fieldInventory`.

| Table | Field | Type | Class | Populated (active62) | Empty | Public | Protected | In validation | In Batch1 |
| --- | --- | --- | --- | ---: | ---: | --- | --- | --- | --- |
| Brand Setup - Brand Basics | Brand Name | singleLineText | public_rendered_supporting | 62 | 0 | true | false | false | false |
| Brand Setup - Brand Basics | Brand_Basics_ID | autoNumber | internal_only | 62 | 0 | false | false | false | false |
| Brand Setup - Brand Basics | Record_ID | formula | internal_only | 62 | 0 | false | false | false | false |
| Brand Setup - Brand Basics | User_ID | multipleRecordLinks | internal_only | 39 | 23 | false | false | false | false |
| Brand Setup - Brand Basics | Brand Status | singleSelect | protected_governance | 62 | 0 | false | true | true | false |
| Brand Setup - Brand Basics | Parent Company | singleLineText | public_rendered_supporting | 62 | 0 | true | false | false | false |
| Brand Setup - Brand Basics | Original Parent Company | singleLineText | unknown_needs_review | 61 | 1 | false | false | false | false |
| Brand Setup - Brand Basics | Hotel Chain Scale | singleSelect | public_rendered_supporting | 62 | 0 | true | false | false | false |
| Brand Setup - Brand Basics | Region Offered | multipleSelects | public_rendered_supporting | 62 | 0 | true | false | true | false |
| Brand Setup - Brand Basics | Brand Architecture | singleSelect | public_rendered_supporting | 62 | 0 | true | false | false | false |
| Brand Setup - Brand Basics | Brand Model | singleSelect | public_rendered_supporting | 62 | 0 | true | false | false | false |
| Brand Setup - Brand Basics | Hotel Service Model | singleSelect | public_rendered_supporting | 62 | 0 | true | false | false | false |
| Brand Setup - Brand Basics | Year Brand Launched | singleLineText | public_rendered_supporting | 62 | 0 | true | false | false | false |
| Brand Setup - Brand Basics | Brand Website | url | unknown_needs_review | 62 | 0 | false | false | false | false |
| Brand Setup - Brand Basics | Brand Development Stage | singleSelect | public_rendered_supporting | 62 | 0 | true | false | false | false |
| Brand Setup - Brand Basics | Brand Positioning | singleLineText | public_rendered_supporting | 62 | 0 | true | false | false | false |
| Brand Setup - Brand Basics | Brand Tagline | singleLineText | public_rendered_supporting | 56 | 6 | true | false | false | false |
| Brand Setup - Brand Basics | Brand Customer Promise | multilineText | public_rendered_supporting | 56 | 6 | true | false | false | false |
| Brand Setup - Brand Basics | Brand Value Proposition | multilineText | public_rendered_supporting | 60 | 2 | true | false | false | false |
| Brand Setup - Brand Basics | Brand Pillars | multilineText | public_rendered_supporting | 55 | 7 | true | false | false | false |
| Brand Setup - Brand Basics | Target Guest Segments | multipleSelects | public_rendered_supporting | 62 | 0 | true | false | false | false |
| Brand Setup - Brand Basics | Guest Psychographics Description | multilineText | public_rendered_supporting | 61 | 1 | true | false | false | false |
| Brand Setup - Brand Basics | Key Brand Differentiators | singleLineText | public_rendered_supporting | 56 | 6 | true | false | false | false |
| Brand Setup - Brand Basics | Sustainability Positioning | multilineText | public_rendered_supporting | 55 | 7 | true | false | false | false |
| Brand Setup - Brand Basics | Brand History | multilineText | public_rendered_supporting | 56 | 6 | true | false | false | false |
| Brand Setup - Brand Basics | Logo | multipleAttachments | public_rendered_supporting | 62 | 0 | true | false | false | false |
| Brand Setup - Brand Basics | Completion Rate | formula | internal_only | 62 | 0 | false | false | false | false |
| Brand Setup - Brand Basics | Brand Setup - Brand Footprint | multipleRecordLinks | internal_only | 62 | 0 | false | false | false | false |
| Brand Setup - Brand Basics | Brand Setup - Project Fit | multipleRecordLinks | internal_only | 62 | 0 | false | false | false | false |
| Brand Setup - Brand Basics | Brand Setup - Fee Structure | multipleRecordLinks | internal_only | 62 | 0 | false | false | false | false |
| Brand Setup - Brand Basics | Brand Setup - Operational Support | multipleRecordLinks | internal_only | 62 | 0 | false | false | false | false |
| Brand Setup - Brand Basics | Brand Setup - Legal Terms | multipleRecordLinks | internal_only | 62 | 0 | false | false | false | false |
| Brand Setup - Brand Basics | Brand Setup - Deal Terms | multipleRecordLinks | internal_only | 62 | 0 | false | false | false | false |
| Brand Setup - Brand Basics | Brand Setup - Brand Standards | multipleRecordLinks | internal_only | 62 | 0 | false | false | false | false |
| Brand Setup - Brand Basics | Brand Setup - Sustainability & ESG | multipleRecordLinks | internal_only | 62 | 0 | false | false | false | false |
| Brand Setup - Brand Basics | Company Profile | multipleRecordLinks | internal_only | 61 | 1 | false | false | false | false |
| Brand Setup - Brand Basics | Company Profile 2 | multipleRecordLinks | internal_only | 61 | 1 | false | false | false | false |
| Brand Setup - Brand Basics | Brand Setup - Portfolio & Performance | multipleRecordLinks | internal_only | 62 | 0 | false | false | false | false |
| Brand Setup - Brand Basics | Brand Setup - Loyalty & Commercial | multipleRecordLinks | internal_only | 62 | 0 | false | false | false | false |
| Brand Setup - Brand Basics | Operator Setup - Operator Basics | singleLineText | unknown_needs_review | 5 | 57 | false | false | false | false |
| Brand Setup - Brand Basics | 3rd Party Operator - Representative Properties | singleLineText | unknown_needs_review | 1 | 61 | false | false | false | false |
| Brand Setup - Brand Basics | 3rd Party Operator - Representative Properties 2 | singleLineText | unknown_needs_review | 0 | 62 | false | false | false | false |
| Brand Setup - Brand Basics | 3rd Party Operator - Case Examples | singleLineText | unknown_needs_review | 7 | 55 | false | false | false | false |
| Brand Setup - Brand Basics | Operator Setup - Profile & Positioning | multipleRecordLinks | internal_only | 52 | 10 | false | false | false | false |
| Brand Setup - Brand Basics | Brand Setup - Brand Explorer Presentation | multipleRecordLinks | internal_only | 62 | 0 | false | false | false | false |
| Brand Setup - Brand Basics | Explorer Hero Verification | singleLineText | protected_governance | 62 | 0 | false | true | false | false |
| Brand Setup - Brand Basics | Explorer Hero Data Source | singleLineText | protected_governance | 62 | 0 | false | true | false | false |
| Brand Setup - Brand Basics | Brand Explorer Favorites | multipleRecordLinks | internal_only | 1 | 61 | false | false | false | false |
| Brand Setup - Brand Basics | Dealality Check | multipleSelects | unknown_needs_review | 13 | 49 | false | false | false | false |
| Brand Setup - Brand Basics | User Management | multipleRecordLinks | internal_only | 5 | 57 | false | false | false | false |
| Brand Setup - Brand Basics | Users | multipleRecordLinks | internal_only | 5 | 57 | false | false | false | false |
| Brand Setup - Brand Basics | Partner Intelligence - Source Library | multipleRecordLinks | source_evidence | 36 | 88 | false | false | false | false |
| Brand Setup - Brand Basics | Partner Intelligence - Source Library | multipleRecordLinks | source_evidence | 36 | 88 | false | false | false | false |
| Brand Setup - Brand Basics | Partner Intelligence - Extracted Facts | multipleRecordLinks | internal_only | 24 | 100 | false | false | false | false |
| Brand Setup - Brand Basics | Partner Intelligence - Extracted Facts | multipleRecordLinks | internal_only | 24 | 100 | false | false | false | false |
| Brand Setup - Brand Basics | Partner Intelligence - Published Explorer Fields | multipleRecordLinks | internal_only | 0 | 62 | false | false | false | false |
| Brand Setup - Brand Basics | Partner Intelligence - Helena Outreach Intake | multipleRecordLinks | internal_only | 0 | 124 | false | false | false | false |
| Brand Setup - Brand Basics | Partner Intelligence - Helena Outreach Intake | multipleRecordLinks | internal_only | 0 | 124 | false | false | false | false |
| Brand Setup - Brand Basics | Validation Status | singleSelect | unknown_needs_review | 24 | 38 | false | false | false | false |
| Brand Setup - Brand Basics | Usage Permission | singleSelect | unknown_needs_review | 24 | 38 | false | false | false | false |
| Brand Setup - Brand Basics | Source Type | singleSelect | source_evidence | 23 | 39 | false | false | false | false |
| Brand Setup - Brand Basics | Source Region | singleSelect | source_evidence | 22 | 40 | false | false | false | false |
| Brand Setup - Brand Basics | Confidence Level | singleSelect | source_evidence | 24 | 38 | false | false | false | false |
| Brand Setup - Brand Basics | Evidence Notes | multilineText | source_evidence | 24 | 38 | false | false | false | false |
| Brand Setup - Brand Basics | Missing Data Flags | multilineText | unknown_needs_review | 12 | 50 | false | false | false | false |
| Brand Setup - Brand Basics | Company Validated | checkbox | validation_status | 0 | 62 | false | true | true | false |
| Brand Setup - Brand Basics | Reviewed By | singleLineText | unknown_needs_review | 0 | 62 | false | false | false | false |
| Brand Setup - Brand Basics | External Display Status | singleSelect | release_control | 24 | 38 | false | true | true | false |
| Brand Setup - Brand Basics | Internal Notes | multilineText | unknown_needs_review | 24 | 38 | false | false | false | false |
| Brand Setup - Brand Basics | Last Reviewed Date | date | protected_governance | 24 | 38 | false | true | true | false |
| Brand Setup - Brand Basics | Refresh Due Date | date | unknown_needs_review | 0 | 62 | false | false | false | false |
| Brand Setup - Brand Basics | Company Validation Date | date | validation_status | 0 | 62 | false | true | false | false |
| Brand Setup - Brand Basics | Partner Intelligence - Brand Asset Registry | multipleRecordLinks | internal_only | 8 | 54 | false | false | false | false |
| Brand Setup - Brand Basics | Branded Residences Status | singleSelect | public_rendered_supporting | 62 | 0 | true | false | false | false |
| Brand Setup - Brand Basics | Branded Residences Notes | multilineText | public_rendered_supporting | 2 | 60 | true | false | false | false |
| Brand Setup - Brand Basics | Branded Residences Source URL | url | source_evidence | 1 | 61 | false | false | false | false |
| Brand Setup - Brand Basics | Branded Residences Review Status | singleSelect | protected_governance | 2 | 60 | false | true | false | false |
| Brand Setup - Brand Basics | Active Profile Approved Date | date | release_control | 62 | 0 | false | true | false | false |
| Brand Setup - Brand Basics | Founder Visual Review Pass | checkbox | release_control | 62 | 0 | false | true | false | false |
| Brand Setup - Brand Basics | Active Profile Approved | checkbox | unknown_needs_review | 62 | 0 | false | false | false | false |
| Brand Setup - Brand Basics | Ready for Active Profile | checkbox | unknown_needs_review | 62 | 0 | false | false | false | false |
| Brand Setup - Sustainability & ESG | Brand_Footprint_ID | autoNumber | internal_only | — | — | false | false | false | false |
| Brand Setup - Sustainability & ESG | Record_ID | formula | internal_only | — | — | false | false | false | false |
| Brand Setup - Sustainability & ESG | Brand | multipleRecordLinks | internal_only | — | — | false | false | false | false |
| Brand Setup - Sustainability & ESG | BrandIDLookup | multipleLookupValues | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Sustainability & ESG | User_Record_ID | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Sustainability & ESG | Brand Name | singleLineText | internal_only | — | — | false | false | false | false |
| Brand Setup - Sustainability & ESG | Sustainability Programs | singleSelect | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Sustainability & ESG | ESG Reporting | singleSelect | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Sustainability & ESG | Carbon Footprint Tracking | singleSelect | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Sustainability & ESG | Energy Efficiency Initiatives | multilineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Sustainability & ESG | Waste Reduction Programs | multilineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | Brand_Footprint_ID | autoNumber | internal_only | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | Record_ID | formula | internal_only | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | Brand | multipleRecordLinks | internal_only | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | BrandIDLookup | multipleLookupValues | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | User_Record_ID | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | Brand Name | singleLineText | internal_only | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | AM Existing Hotel | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | AM Existing Rooms | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | AM Pipeline Hotel | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | AM Pipeline Rooms | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | AM Total Distribution Hotel | formula | internal_only | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | AM Total Distribution Rooms | formula | internal_only | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | CALA Existing Hotel | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | CALA Existing Rooms | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | CALA Pipeline Hotel | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | CALA Pipeline Rooms | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | CALA Total Distribution Hotel | formula | internal_only | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | CALA Total Distribution Rooms | formula | internal_only | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | EU Existing Hotel | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | EU Existing Rooms | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | EU Pipeline Hotel | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | EU Pipeline Rooms | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | EU Total Distribution Hotel | formula | internal_only | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | EU Total Distribution Rooms | formula | internal_only | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | MEA Existing Hotel | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | MEA Existing Rooms | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | MEA Pipeline Hotel | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | MEA Pipeline Rooms | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | MEA Total Distribution Hotel | formula | internal_only | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | MEA Total Distribution Rooms | formula | internal_only | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | APAC Existing Hotel | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | APAC Existing Rooms | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | APAC Pipeline Hotel | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | APAC Pipeline Rooms | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | APAC Total Distribution Hotel | formula | internal_only | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | APAC Total Distribution Rooms | formula | internal_only | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | Total Existing Hotels | formula | internal_only | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | Total Existing Rooms | formula | internal_only | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | Total Pipeline Hotels | formula | internal_only | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | Total Pipeline Rooms | formula | internal_only | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | Total Distribution Hotels | formula | internal_only | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | Total Distribution Rooms | formula | internal_only | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | AM Managed Hotel | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | AM Managed Rooms | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | AM Franchised Hotel | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | AM Franchised Rooms | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | AM Total Distribution Hotels - MF | formula | internal_only | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | AM Total Distribution Rooms - MF | formula | internal_only | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | CALA Managed Hotel | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | CALA Managed Rooms | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | CALA Franchised Hotel | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | CALA Franchised Rooms | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | CALA Total Distribution Hotels - MF | formula | internal_only | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | CALA Total Distribution Rooms - MF | formula | internal_only | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | EU Managed Hotel | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | EU Managed Rooms | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | EU Franchised Hotel | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | EU Franchised Rooms | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | EU Total Distribution Hotels - MF | formula | internal_only | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | EU Total Distribution Rooms - MF | formula | internal_only | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | MEA Managed Hotel | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | MEA Managed Rooms | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | MEA Franchised Hotel | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | MEA Franchised Rooms | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | MEA Total Distribution Hotels - MF | formula | internal_only | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | MEA Total Distribution Rooms - MF | formula | internal_only | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | APAC Managed Hotel | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | APAC Managed Rooms | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | APAC Franchised Hotel | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | APAC Franchised Rooms | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | APAC Total Distribution Hotels - MF | formula | internal_only | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | APAC Total Distribution Rooms - MF | formula | internal_only | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | Total Managed Hotels | formula | internal_only | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | Total Managed Rooms | formula | internal_only | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | Total Franchised Hotels | formula | internal_only | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | Total Franchised Rooms | formula | internal_only | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | Total Distribution Hotels - MF | formula | internal_only | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | Total Distribution Rooms - MF | formula | internal_only | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | AM New Build Hotel | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | AM New Build Rooms | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | AM Conversion Hotel | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | AM Conversion Rooms | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | AM Total Distribution Hotels - NBC | formula | internal_only | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | AM Total Distribution Rooms - NBC | formula | internal_only | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | CALA New Build Hotel | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | CALA New Build Rooms | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | CALA Conversion Hotel | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | CALA Conversion Rooms | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | CALA Total Distribution Hotels - NBC | formula | internal_only | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | CALA Total Distribution Rooms - NBC | formula | internal_only | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | EU New Build Hotel | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | EU New Build Rooms | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | EU Conversion Hotel | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | EU Conversion Rooms | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | EU Total Distribution Hotels - NBC | formula | internal_only | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | EU Total Distribution Rooms - NBC | formula | internal_only | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | MEA New Build Hotel | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | MEA New Build Rooms | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | MEA Conversion Hotel | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | MEA Conversion Rooms | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | MEA Total Distribution Hotels - NBC | formula | internal_only | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | MEA Total Distribution Rooms - NBC | formula | internal_only | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | APAC New Build Hotel | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | APAC New Build Rooms | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | APAC Conversion Hotel | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | APAC Conversion Rooms | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | APAC Total Distribution Hotels - NBC | formula | internal_only | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | APAC Total Distribution Rooms - NBC | formula | internal_only | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | Total New Build Hotel | formula | internal_only | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | Total New Build Rooms | formula | internal_only | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | Total Conversion Hotel | formula | internal_only | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | Total Conversion Rooms | formula | internal_only | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | Total Distribution Hotels - NBC | formula | internal_only | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | Total Distribution Rooms - NBC | formula | internal_only | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | Urban - Existing Systemwide Location | percent | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | Suburban - Existing Systemwide Location | percent | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | Resort - Existing Systemwide Location | percent | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | Airport - Existing Systemwide Location | percent | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | Small Metro - Existing Systemwide Location | percent | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | Interstate - Existing Systemwide Location | percent | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | Number of Exits in Past 24 Months | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | Figures as of | dateTime | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | Number of Markets Operated In | number | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | Specific Markets/Cities | multilineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | New Build Experience (New build %) | percent | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | Conversion Experience (Conversion %) | percent | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | Turnaround Experience (%) | percent | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | Renovation/Rebrand Experience (%) | percent | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | Typical Managed % | formula | internal_only | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | Typical Franchised % | formula | internal_only | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | Typical Loyalty Program Name | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | Typical % of Rooms from Loyalty (est.) | percent | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | Typical Direct Booking % (est.) | percent | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | Typical OTA Reliance % (est.) | percent | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | Footprint Data Status | singleSelect | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | Footprint Data Source | singleLineText | source_evidence | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | Footprint Figures As Of | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Footprint | Footprint Notes | multilineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Project Fit | Project_Fit_ID | autoNumber | internal_only | — | — | false | false | false | false |
| Brand Setup - Project Fit | Record_ID | formula | internal_only | — | — | false | false | false | false |
| Brand Setup - Project Fit | Brand | multipleRecordLinks | internal_only | — | — | false | false | false | false |
| Brand Setup - Project Fit | BrandIDLookup | multipleLookupValues | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Project Fit | User_Record_ID | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Project Fit | Brand Name | singleLineText | internal_only | — | — | false | false | false | false |
| Brand Setup - Project Fit | Acceptable Project Type | multipleSelects | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Project Fit | Acceptable Building Types | multipleSelects | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Project Fit | Acceptable Agreements Type | multipleSelects | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Project Fit | Acceptable Project Stages | multipleSelects | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Project Fit | Priority Markets | multipleSelects | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Project Fit | Other - Priority Markets Text | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Project Fit | Markets to Avoid | multipleSelects | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Project Fit | Other - Markets to Avoid Text | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Project Fit | Min - Room Count | number | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Project Fit | Max - Room Count | number | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Project Fit | Min - Ideal Project Size | number | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Project Fit | Max - Ideal Project Size | number | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Project Fit | Req Operator Exp | number | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Project Fit | Min Lead Time | number | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Project Fit | Preferred Owner/Investor Type | singleSelect | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Project Fit | Co-Branding Allowed | singleSelect | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Project Fit | Branded Residences Allowed | singleSelect | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Project Fit | Soft/Collection Brand | singleSelect | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Project Fit | Owner Training | singleSelect | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Project Fit | Mixed-Use Development Allowed | singleSelect | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Project Fit | Minimum Percent - Mixed-Use Development Allowed | percent | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Project Fit | Discussion to Selection - Target Milestones | number | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Project Fit | Selection to Construction - Target Milestones | number | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Project Fit | PreOpen to SoftOpen - Target Milestones | number | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Project Fit | SoftOpen to GrandOpen - Target Milestones | number | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Project Fit | Flexibility On Dates | singleSelect | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Project Fit | Owner / Sponsor Hotel Experience | singleSelect | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Project Fit | Acceptable Owner Involvement Levels | multipleSelects | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Project Fit | Owner Non-Negotiables | multipleSelects | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Project Fit | Other (Text) - Owner Non-Negotiables | multilineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Project Fit | Owner Non-Negotiables & Decision Rights | multilineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Project Fit | Acceptable Capital Status at Engagement | multipleSelects | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Project Fit | Red Flag Items That Typically Make You Decline or Proceed With Caution - Risk & Compliance | multilineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Project Fit | ESG / Sustainability Expectations You Prefer Projects to Meet - Risk & Compliance | multilineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Project Fit | Brand Status Scenarios You Will Consider | multipleSelects | protected_governance | — | — | false | true | false | false |
| Brand Setup - Project Fit | Typical PIP / Repositioning Profile You Will Consider (If Existing Hotel) | multilineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Project Fit | Acceptable Fee Expectations vs Market | multipleSelects | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Project Fit | CapEx and FF&E Support | multilineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Project Fit | Acceptable Exit Horizon | multipleSelects | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Project Fit | Anything else about your commercial 'sweet spot' we should know? | multilineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Project Fit | Typical PIP Range ($/room or %) | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Project Fit | Who Pays for PIP | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Project Fit | Supported Owner Strategy Types | multipleSelects | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Project Fit | Brand Role Suitability | multipleSelects | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Project Fit | Typical Decision Cycle Compatibility | multipleSelects | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Project Fit | Supported Micro-Location Types | multipleSelects | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Project Fit | Target Demand Mix Strengths | multipleSelects | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Project Fit | Regional / City Appetite Notes | multilineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Project Fit | Operational Complexity Capabilities | multipleSelects | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Project Fit | Owner Control Flexibility Areas | multipleSelects | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Project Fit | Contract Flexibility Areas | multipleSelects | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Portfolio & Performance | Portfolio Metrics As of Date | dateTime | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Portfolio & Performance | Brand | multipleRecordLinks | internal_only | — | — | false | false | false | false |
| Brand Setup - Portfolio & Performance | BrandIDLookup | multipleLookupValues | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Portfolio & Performance | Brand Name | singleLineText | internal_only | — | — | false | false | false | false |
| Brand Setup - Portfolio & Performance | Total Brand Portfolio Value | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Portfolio & Performance | Annual Revenue (Brand Wide) | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Portfolio & Performance | Brand Portfolio Growth Rate | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Portfolio & Performance | Minimum Property Size (Rooms) | number | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Portfolio & Performance | Maximum Property Size (Rooms) | number | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Portfolio & Performance | Typical Franchise Agreement Term | singleSelect | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Portfolio & Performance | Franchise Fee Structure | singleSelect | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Portfolio & Performance | Typical RevPAR Improvement (Brand Benchmark) | percent | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Portfolio & Performance | Typical Occupancy Improvement (Brand Benchmark) | percent | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Portfolio & Performance | Typical NOI Improvement (Brand Benchmark) | percent | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Portfolio & Performance | Franchisee Retention Rate | percent | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Portfolio & Performance | Typical Agreement Renewal Rate | percent | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Portfolio & Performance | Turnaround Properties in Brand | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Portfolio & Performance | Typical Time to Stabilization | number | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Portfolio & Performance | Reporting Frequency Required or Provided | singleSelect | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Portfolio & Performance | Report Types Required | multipleSelects | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Portfolio & Performance | Budget Process (Brand Requirement or Support) | singleSelect | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Portfolio & Performance | Capital Expenditure Planning (Brand Requirement) | singleSelect | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Portfolio & Performance | Performance Review Cadence | singleSelect | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Portfolio & Performance | PMS (Property Management System) | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Portfolio & Performance | Revenue Management System | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Portfolio & Performance | Accounting / Reporting System | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Portfolio & Performance | Guest Communication / Mobile Check-in | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Portfolio & Performance | Mobile Check-in / Digital Key Required | singleSelect | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Portfolio & Performance | Franchisee Reporting Portal / Data Access | singleSelect | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Portfolio & Performance | Data / Analytics Platform Required or Approved | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Portfolio & Performance | Required Integrations (OTAs, Payments, etc.) | multilineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Standards | Brand_Standards_ID | autoNumber | internal_only | — | — | false | false | false | false |
| Brand Setup - Brand Standards | Record_ID | formula | internal_only | — | — | false | false | false | false |
| Brand Setup - Brand Standards | Brand | multipleRecordLinks | internal_only | — | — | false | false | false | false |
| Brand Setup - Brand Standards | BrandIDLookup | multipleLookupValues | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Standards | User_Record_ID | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Standards | Brand Name | singleLineText | internal_only | — | — | false | false | false | false |
| Brand Setup - Brand Standards | F&B Outlets Required | singleSelect | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Standards | Typical Number of F&B Outlets | number | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Standards | Typical F&B Program Type | multipleSelects | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Standards | Typical Outlet Names / Concepts | multilineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Standards | Typical Total F&B Outlet Size | number | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Standards | F&B Outlet Size Unit | singleSelect | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Standards | Meeting Space Required | singleSelect | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Standards | Typical Number of Meeting Rooms | number | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Standards | Typical Meeting Space Size | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Standards | Condo Residences Allowed | singleSelect | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Standards | Hotel Rental Program | singleSelect | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Standards | Parking Required | singleSelect | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Standards | Typical Total Parking Spaces | number | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Standards | Parking Program | multipleSelects | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Standards | Sustainability Features | multipleSelects | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Standards | Other Sustainability Text | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Standards | Additional Amenities | multipleSelects | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Standards | Other Amenities Text - Amenities | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Standards | Compliance & Safety | multipleSelects | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Standards | Other Text - Compliance | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Standards | Typical QA / Brand Standards Expectations | multilineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Standards | Additional Brand Standards Notes | multilineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Deal Terms | Deal_Terms_ID | autoNumber | internal_only | — | — | false | false | false | false |
| Brand Setup - Deal Terms | Record_ID | formula | internal_only | — | — | false | false | false | false |
| Brand Setup - Deal Terms | Brand | multipleRecordLinks | internal_only | — | — | false | false | false | false |
| Brand Setup - Deal Terms | BrandIDLookup | multipleLookupValues | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Deal Terms | User_Record_ID | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Deal Terms | Brand Name | singleLineText | internal_only | — | — | false | false | false | false |
| Brand Setup - Deal Terms | Length - Typical Minimum Initial Term | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Deal Terms | Quantity - Typical Minimum Initial Term | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Deal Terms | Duration - Typical Minimum Initial Term | singleSelect | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Deal Terms | Length - Typical Renewal Option | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Deal Terms | Quantity - Typical Renewal Option | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Deal Terms | Duration - Typical Renewal Option | singleSelect | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Deal Terms | Length - Typical Renewal Notice Period | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Deal Terms | Quantity - Typical Renewal Notice Period | singleSelect | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Deal Terms | Renewal Structure | singleSelect | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Deal Terms | Renewal Notice Responsibility | singleSelect | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Deal Terms | Typical Renewal Conditions | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Deal Terms | Performance Test Requirement | singleSelect | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Deal Terms | Typical Cure Period for Performance Test Failure | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Deal Terms | Duration - Typical Cure Period for Performance Test Failure | singleSelect | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Deal Terms | Typical QA | singleSelect | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Deal Terms | Mandatory PIP at Renewal | singleSelect | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Deal Terms | Mandatory PIP for Conversions | singleSelect | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Deal Terms | Typical Mandatory PIP for Conversions ($/room) | currency | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Deal Terms | Conversion - Typical max time allowed for completion | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Deal Terms | Conversion - Typical max time allowed for completion -Duration | singleSelect | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Deal Terms | Renewal - Typical max time allowed for completion | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Deal Terms | Renewal - Typical max time allowed for completion -Duration | singleSelect | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Fee Structure | Fee_Structure_ID | autoNumber | internal_only | — | — | false | false | false | false |
| Brand Setup - Fee Structure | Record_ID | formula | internal_only | — | — | false | false | false | false |
| Brand Setup - Fee Structure | Brand | multipleRecordLinks | internal_only | — | — | false | false | false | false |
| Brand Setup - Fee Structure | BrandIDLookup | multipleLookupValues | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Fee Structure | User_Record_ID | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Fee Structure | Brand Name | singleLineText | internal_only | — | — | false | false | false | false |
| Brand Setup - Fee Structure | Min - Typical Application Fee | number | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Fee Structure | Max - Typical Application Fee | number | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Fee Structure | Basis - Typical Application Fee | singleSelect | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Fee Structure | Application Fee Per Unit Over Threshold | number | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Fee Structure | Application Fee Threshold (Units) | number | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Fee Structure | Additional Notes - Typical Application Fee | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Fee Structure | Min - Typical Royalty Fee Range | percent | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Fee Structure | Max - Typical Royalty Fee Range | percent | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Fee Structure | Basis - Typical Royalty Fee Range | singleSelect | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Fee Structure | Additional Notes - Typical Royalty Fee Range | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Fee Structure | Min - Typical Marketing Fee Range | percent | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Fee Structure | Max - Typical Marketing Fee Range | percent | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Fee Structure | Basis - Typical Marketing Fee Range | singleSelect | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Fee Structure | Additional Notes - Typical Marketing Fee Range | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Fee Structure | Min - Typical Tech | number | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Fee Structure | Max - Typical Tech | number | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Fee Structure | Basis - Typical Tech | singleSelect | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Fee Structure | Additional Notes - Typical Tech | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Fee Structure | Min - Typical Loyalty Program Fee | number | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Fee Structure | Max - Typical Loyalty Program Fee | number | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Fee Structure | Basis - Typical Loyalty Program Fee | singleSelect | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Fee Structure | Additional Notes - Typical Loyalty Program Fee | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Fee Structure | Typical Incentives Offered | multilineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Fee Structure | Typical Owner Early-Termination Rights (without cause) | singleSelect | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Fee Structure | Early-Termination Notes | multilineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Fee Structure | Typical Termination Fee Structure (if any) | singleSelect | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Fee Structure | Typical Termination Fee Structure (if any) Text | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Fee Structure | Who Can Exercise Termination Right After Failed Test? | singleSelect | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Fee Structure | Key Money / Co-Investment | singleSelect | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Fee Structure | Typical Expectations for Owner-Funded Reserves | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Fee Structure | Do Agreements Typically Cap Operator Reimbursable Expenses? | singleSelect | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Fee Structure | Do You Usually Require Audit Rights for Owner Books / Operator Systems? | singleSelect | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Fee Structure | Min - Typical Training Fee | currency | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Fee Structure | Max - Typical Training Fee | currency | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Fee Structure | Basis - Typical Training Fee | singleSelect | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Fee Structure | Additional Notes - Typical Training Fee | multilineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Fee Structure | Min - Typical Reservation / Distribution Fee | number | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Fee Structure | Max - Typical Reservation / Distribution Fee | number | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Fee Structure | Basis - Typical Reservation / Distribution Fee | singleSelect | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Fee Structure | DELETE>>>> | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Fee Structure | Min - Other | number | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Fee Structure | Max - Typical Technical Fee Range | number | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Fee Structure | Basis - Typical Technical Fee Range | singleSelect | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Fee Structure | Min - Other Required Program Fees | number | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Fee Structure | Max - Other Required Program Fees | number | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Fee Structure | Basis - Other Required Program Fees | singleSelect | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Fee Structure | Description of Fee | multilineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Fee Structure | Min - Typical Management Fee | percent | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Fee Structure | Max - Typical Management Fee | percent | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Fee Structure | Basis - Typical Management Fee | singleSelect | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Fee Structure | Min - Typical Incentive Fee | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Fee Structure | Max - Typical Incentive Fee | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Fee Structure | Basis - Typical Incentive Fee | singleSelect | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Fee Structure | Notes - Typical Incentive Fee | multilineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Fee Structure | Min - Typical Incentive Fee Excess | percent | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Fee Structure | Max - Typical Incentive Fee Excess | percent | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Fee Structure | Basis - Typical Incentive Fee Excess | singleSelect | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Fee Structure | Notes - Typical Incentive Fee Excess | multilineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Fee Structure | Fee Positioning Band | singleSelect | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Fee Structure | Typical CapEx / PIP Intensity Band | singleSelect | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Operational Support | Operational_Support_ID | autoNumber | internal_only | — | — | false | false | false | false |
| Brand Setup - Operational Support | Record_ID | formula | internal_only | — | — | false | false | false | false |
| Brand Setup - Operational Support | Brand | multipleRecordLinks | internal_only | — | — | false | false | false | false |
| Brand Setup - Operational Support | BrandIDLookup | multipleLookupValues | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Operational Support | User_Record_ID | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Operational Support | Brand Name | singleLineText | internal_only | — | — | false | false | false | false |
| Brand Setup - Operational Support | Willing to Negotiate Incentives | singleSelect | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Operational Support | Incentive Types | multipleSelects | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Operational Support |  Other Text | multilineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Operational Support | Key Money Payment Timing | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Operational Support | Key Money Clawback Terms | multilineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Operational Support | Key Money Payment Structure | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Operational Support | Revenue Management Services | multipleSelects | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Operational Support | Other Text - Revenue Management Services | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Operational Support | Sales & Marketing Support | multipleSelects | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Operational Support | Other Text - Sales & Marketing Support | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Operational Support | Accounting & Financial Reporting | multipleSelects | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Operational Support | Other Text - Accounting & Financial Reporting | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Operational Support | Procurement Services | multipleSelects | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Operational Support | Other Text - Procurement Services | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Operational Support | HR & Training Services | multipleSelects | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Operational Support | Other Text - HR & Training Services | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Operational Support | Technology Services | multipleSelects | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Operational Support | Other Text - Technology Services | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Operational Support | Design & Renovation Support | multipleSelects | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Operational Support | Other Text - Design & Renovation Support | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Operational Support | Development Services | multipleSelects | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Operational Support | Other Text - Development Services | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Operational Support | Service Offering Summary | multilineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Operational Support | Owner Communication Style | singleSelect | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Operational Support | Owner Involvement Level | singleSelect | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Operational Support | Typical Response Time for Owner Inquiries | singleSelect | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Operational Support | Decision-Making Process | multilineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Operational Support | Dispute Resolution Approach | multilineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Operational Support | Average Time to Resolve Owner Concerns | singleSelect | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Operational Support | Owner Advisory Board | singleSelect | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Operational Support | Owner Education/Training Provided | singleSelect | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Operational Support | Owner References Available | number | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Operational Support | Case Studies Available | number | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Operational Support | Testimonial Links | multilineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Operational Support | Industry Recognition | multilineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Operational Support | Owner Satisfaction Score (NPS) | number | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Operational Support | Lender References Available | singleSelect | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Operational Support | Major Lenders Worked With | multilineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Operational Support | Specializations | multilineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Operational Support | Key Owner Success Stories | multilineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Operational Support | Owner Portal Features | multilineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Operational Support | Additional Notes | multilineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Operational Support | Ongoing Support Included | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Operational Support | CRS / Central Res. Participation | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Operational Support | GDS Participation | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Operational Support | Owner Portal Available | singleSelect | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Operational Support | Owner Portal Tier | singleSelect | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Operational Support | Owner Portal Notes | multilineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Operational Support | Incentive Availability Level | singleSelect | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Operational Support | Pre-Opening Support Depth | singleSelect | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Operational Support | Governance / Reporting Intensity Supported | singleSelect | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Legal Terms | Legal_Terms_ID | autoNumber | internal_only | — | — | false | false | false | false |
| Brand Setup - Legal Terms | Record_ID | formula | internal_only | — | — | false | false | false | false |
| Brand Setup - Legal Terms | Brand | multipleRecordLinks | internal_only | — | — | false | false | false | false |
| Brand Setup - Legal Terms | BrandIDLookup | multipleLookupValues | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Legal Terms | User_Record_ID | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Legal Terms | Brand Name | singleLineText | internal_only | — | — | false | false | false | false |
| Brand Setup - Legal Terms | Radius - Typical Area of Protection | multilineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Legal Terms | Restrictions - Typical Area of Protection | multilineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Legal Terms | LOI Timeline to Agreement | multilineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Legal Terms | For Cause - Termination Rights | multilineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Legal Terms | Without Cause - Termination Rights | multilineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Legal Terms | On Sale - Termination Rights | multilineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Legal Terms | Liquidated Damages | multilineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Legal Terms | Exclusivity | multilineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Legal Terms | Binding - LOI Binding vs NonBinding Terms | multilineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Legal Terms | Non-Binding - LOI Binding vs NonBinding Terms | multilineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Legal Terms | Confidentiality | multilineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Legal Terms | Conditions Precedent | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Legal Terms | Buyout / Transfer Provisions | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Legal Terms | Assignment Restrictions | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Loyalty & Commercial | Brand Name | singleLineText | internal_only | — | — | false | false | false | false |
| Brand Setup - Loyalty & Commercial | Brand | multipleRecordLinks | internal_only | — | — | false | false | false | false |
| Brand Setup - Loyalty & Commercial | BrandIDLookup | multipleLookupValues | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Loyalty & Commercial | Typical Loyalty Program Name | singleLineText | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Loyalty & Commercial | Typical % of Rooms from Loyalty (est.) | percent | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Loyalty & Commercial | Typical Direct Booking % (est.) | percent | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Loyalty & Commercial | Typical OTA Reliance % (est.) | percent | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Loyalty & Commercial | Total Global Members (Approx. Millions) | number | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Loyalty & Commercial | Loyalty Program Cost per Stay (Approximate) | currency | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Loyalty & Commercial | OTA Commission (Typical % of Reservation) | percent | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Loyalty & Commercial | CRS Usage (% of bookings flowing through) | percent | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Loyalty & Commercial | Distribution Cost (Per Reservation) | currency | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Loyalty & Commercial | Website/App Conv. Rates (%) | percent | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Loyalty & Commercial | Avg. Cost of Cust. Acquisition | currency | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Loyalty & Commercial | Regional Members - NA (Millions) | number | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Loyalty & Commercial | Regional Members - CALA (Millions) | number | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Loyalty & Commercial | Regional Members - EU (Millions) | number | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Loyalty & Commercial | Regional Members - MEA (Millions) | number | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Loyalty & Commercial | Regional Members - APAC (Millions) | number | unknown_needs_review | — | — | false | false | false | false |
| Brand Setup - Brand Explorer Presentation | Slot Key | singleLineText | public_rendered_owner_facing | 8085 | 0 | true | false | true | false |
| Brand Setup - Brand Explorer Presentation | Title | singleLineText | public_rendered_owner_facing | 5695 | 2390 | true | false | true | true |
| Brand Setup - Brand Explorer Presentation | Body | multilineText | public_rendered_owner_facing | 8008 | 77 | true | false | true | true |
| Brand Setup - Brand Explorer Presentation | Sort Order | number | release_control | 8085 | 0 | false | false | true | false |
| Brand Setup - Brand Explorer Presentation | Active | checkbox | release_control | 7943 | 142 | false | false | true | false |
| Brand Setup - Brand Explorer Presentation | Brand | multipleRecordLinks | internal_only | 8085 | 0 | false | false | false | false |
| Brand Setup - Brand Explorer Presentation | Brand Name | singleLineText | internal_only | 8085 | 0 | false | false | false | false |
| Brand Setup - Brand Explorer Presentation | Image | multipleAttachments | public_rendered_owner_facing | 846 | 7239 | true | false | true | false |
| Brand Setup - Brand Explorer Presentation | Created | createdTime | internal_only | 8085 | 0 | false | false | false | false |
| Brand Setup - Brand Explorer Presentation | Case Summary Overview | multilineText | public_rendered_owner_facing | 424 | 7661 | true | false | true | true |
| Brand Setup - Brand Explorer Presentation | Case Summary Owner Objective | multilineText | public_rendered_owner_facing | 372 | 7713 | true | false | true | false |
| Brand Setup - Brand Explorer Presentation | Case Summary Brand Relevance | multilineText | public_rendered_owner_facing | 365 | 7720 | true | false | true | false |
| Brand Setup - Brand Explorer Presentation | Case Summary Interpretation | multilineText | public_rendered_owner_facing | 420 | 7665 | true | false | true | true |
| Brand Setup - Brand Explorer Presentation | Case Summary Tags | multilineText | public_rendered_owner_facing | 463 | 7622 | true | false | true | false |
| Brand Setup - Brand Explorer Presentation | Validation Status | singleSelect | unknown_needs_review | 0 | 8085 | false | false | false | false |
| Brand Setup - Brand Explorer Presentation | Usage Permission | singleSelect | unknown_needs_review | 0 | 8085 | false | false | false | false |
| Brand Setup - Brand Explorer Presentation | Source Type | singleSelect | source_evidence | 0 | 8085 | false | false | false | false |
| Brand Setup - Brand Explorer Presentation | Source Region | singleSelect | source_evidence | 0 | 8085 | false | false | false | false |
| Brand Setup - Brand Explorer Presentation | Confidence Level | singleSelect | source_evidence | 0 | 8085 | false | false | false | false |
| Brand Setup - Brand Explorer Presentation | Evidence Notes | multilineText | source_evidence | 0 | 8085 | false | false | false | false |
| Brand Setup - Brand Explorer Presentation | Missing Data Flags | multilineText | unknown_needs_review | 0 | 8085 | false | false | false | false |
| Brand Setup - Brand Explorer Presentation | Company Validated | checkbox | validation_status | 0 | 8085 | false | true | true | false |
| Brand Setup - Brand Explorer Presentation | Reviewed By | singleLineText | unknown_needs_review | 0 | 8085 | false | false | false | false |
| Brand Setup - Brand Explorer Presentation | External Display Status | singleSelect | release_control | 210 | 7875 | false | true | true | false |
| Brand Setup - Brand Explorer Presentation | Internal Notes | multilineText | unknown_needs_review | 0 | 8085 | false | false | false | false |
| Brand Setup - Brand Explorer Presentation | Last Reviewed Date | date | protected_governance | 0 | 8085 | false | true | true | false |
| Brand Setup - Brand Explorer Presentation | Refresh Due Date | date | unknown_needs_review | 0 | 8085 | false | false | false | false |
| Brand Setup - Brand Explorer Presentation | Company Validation Date | date | validation_status | 0 | 8085 | false | true | false | false |

## 4. Field classification matrix

| Classification | Count |
| --- | ---: |
| `unknown_needs_review` | 415 |
| `internal_only` | 117 |
| `public_rendered_supporting` | 22 |
| `source_evidence` | 18 |
| `public_rendered_owner_facing` | 9 |
| `protected_governance` | 8 |
| `validation_status` | 6 |
| `release_control` | 6 |

## 5. Current validation coverage

### Surfaces (Presentation / Basics)

- **semantic:** `Title`, `Body`, `Case Summary Overview`, `Case Summary Brand Relevance`, `Case Summary Owner Objective`, `Case Summary Interpretation`, `Case Summary Tags`, `Slot Key`
- **pvql:** `Title`, `Body`, `Case Summary Overview`, `Case Summary Brand Relevance`, `Case Summary Owner Objective`, `Case Summary Interpretation`, `Case Summary Tags`, `External Display Status`, `Active`, `Slot Key`, `Image URL`
- **quality:** `Title`, `Body`, `Slot Key`, `Image`, `Image URL`, `Case Summary Overview`, `Case Summary Brand Relevance`, `Case Summary Interpretation`, `Case Summary Tags`
- **footnote:** `Company Validated`, `Brand Explorer Last Reviewed`, `Last Reviewed Date`, `Profile Last Reviewed`, `Last Reviewed`, `Brand Status`, `Region Offered`
- **momentum:** `Title`, `Body`, `Slot Key`
- **mandatory:** `Brand Status`, `External Display Status`, `Active`, `Company Validated`, `Title`, `Body`, `Slot Key`
- **webflow_product:** `Title`, `Body`, `Case Summary Overview`, `Case Summary Brand Relevance`, `Case Summary Owner Objective`, `Case Summary Interpretation`, `Case Summary Tags`, `Slot Key`, `Image`, `Sort Order`, `Active`, `External Display Status`
- **census_crosscheck:** `Title`, `Body`, `Case Summary Overview`, `Case Summary Brand Relevance`, `Case Summary Interpretation`, `Case Summary Tags`

### Presentation fields with no Active-62 validation surface

- `Brand`
- `Brand Name`
- `Created`
- `Validation Status`
- `Usage Permission`
- `Source Type`
- `Source Region`
- `Confidence Level`
- `Evidence Notes`
- `Missing Data Flags`
- `Reviewed By`
- `Internal Notes`
- `Refresh Due Date`
- `Company Validation Date`

### Child Brand Setup tables outside Active-62 gates

- **Brand Setup - Sustainability & ESG** (11 fields) — Do not claim Batch 1 / Active-62 gates cover these tables; separate Brand Setup form QA if needed
- **Brand Setup - Brand Footprint** (138 fields) — Do not claim Batch 1 / Active-62 gates cover these tables; separate Brand Setup form QA if needed
- **Brand Setup - Project Fit** (57 fields) — Do not claim Batch 1 / Active-62 gates cover these tables; separate Brand Setup form QA if needed
- **Brand Setup - Portfolio & Performance** (31 fields) — Do not claim Batch 1 / Active-62 gates cover these tables; separate Brand Setup form QA if needed
- **Brand Setup - Brand Standards** (28 fields) — Do not claim Batch 1 / Active-62 gates cover these tables; separate Brand Setup form QA if needed
- **Brand Setup - Deal Terms** (28 fields) — Do not claim Batch 1 / Active-62 gates cover these tables; separate Brand Setup form QA if needed
- **Brand Setup - Fee Structure** (66 fields) — Do not claim Batch 1 / Active-62 gates cover these tables; separate Brand Setup form QA if needed
- **Brand Setup - Operational Support** (57 fields) — Do not claim Batch 1 / Active-62 gates cover these tables; separate Brand Setup form QA if needed
- **Brand Setup - Legal Terms** (20 fields) — Do not claim Batch 1 / Active-62 gates cover these tables; separate Brand Setup form QA if needed
- **Brand Setup - Loyalty & Commercial** (19 fields) — Do not claim Batch 1 / Active-62 gates cover these tables; separate Brand Setup form QA if needed

## 6. Fields included in Batch 1

### Batch 1A

- `Case Summary Overview` × 10
- `Body` × 19
- `Case Summary Interpretation` × 7

### Batch 1B

- `Body` × 30
- `Case Summary Overview` × 1
- `Title` × 1

Allowed by Batch 1 design but **not present** as patches in this plan: Case Summary Brand Relevance, Case Summary Tags (and Case Summary Owner Objective).

## 7. Fields excluded from Batch 1

- Recent Momentum (slot-specific Body/Title unless separately approved)
- Company Validated / Company Validation Date
- Brand Verified
- Brand Status
- Founder Visual Review Pass
- release fields
- External Display Status / Active (visibility controls)
- Image / media fields
- all non-Presentation Brand Setup tables
- Case Summary Owner Objective (no Batch 1 hits; allowed only if Low safe_text)
- Case Summary Brand Relevance / Tags (allowed but no Batch 1 patches in this plan)

## 8. Public/rendered fields not yet validated

- No Presentation `public_facing` fields lack a mapped validation surface in this audit matrix.

- **Gap type that matters for the founder question:** entire child Brand Setup tables are unvalidated by Active-62 BE gates (see §5 / §11).

## 9. Internal / protected fields

| Table | Field | Classification |
| --- | --- | --- |
| Brand Setup - Brand Basics | Brand Status | protected_governance |
| Brand Setup - Brand Basics | Explorer Hero Verification | protected_governance |
| Brand Setup - Brand Basics | Explorer Hero Data Source | protected_governance |
| Brand Setup - Brand Basics | Company Validated | validation_status |
| Brand Setup - Brand Basics | External Display Status | release_control |
| Brand Setup - Brand Basics | Last Reviewed Date | protected_governance |
| Brand Setup - Brand Basics | Company Validation Date | validation_status |
| Brand Setup - Brand Basics | Branded Residences Review Status | protected_governance |
| Brand Setup - Brand Basics | Active Profile Approved Date | release_control |
| Brand Setup - Brand Basics | Founder Visual Review Pass | release_control |
| Brand Setup - Project Fit | Brand Status Scenarios You Will Consider | protected_governance |
| Brand Setup - Brand Explorer Presentation | Sort Order | release_control |
| Brand Setup - Brand Explorer Presentation | Active | release_control |
| Brand Setup - Brand Explorer Presentation | Company Validated | validation_status |
| Brand Setup - Brand Explorer Presentation | External Display Status | release_control |
| Brand Setup - Brand Explorer Presentation | Last Reviewed Date | protected_governance |
| Brand Setup - Brand Explorer Presentation | Company Validation Date | validation_status |
| Partner Intelligence - Brand Asset Registry | Last Reviewed Date | protected_governance |
| Partner Intelligence - Brand Asset Registry | Company Validated | validation_status |
| Partner Intelligence - Brand Asset Registry | Company Validation Date | validation_status |

## 10. Census-connected fields

### Brand Setup side

- `Brand Setup - Brand Basics`.`Brand Name`
- `Brand Setup - Brand Basics`.`Parent Company`
- `Brand Setup - Brand Basics`.`Original Parent Company`
- `Brand Setup - Brand Basics`.`Hotel Chain Scale`
- `Brand Setup - Brand Basics`.`Region Offered`
- `Brand Setup - Brand Basics`.`Hotel Service Model`
- `Brand Setup - Brand Basics`.`Source Region`
- `Brand Setup - Brand Basics`.`Branded Residences Status`
- `Brand Setup - Brand Basics`.`Branded Residences Notes`
- `Brand Setup - Brand Basics`.`Branded Residences Source URL`
- `Brand Setup - Brand Basics`.`Branded Residences Review Status`
- `Brand Setup - Brand Explorer Presentation`.`Title`
- `Brand Setup - Brand Explorer Presentation`.`Body`
- `Brand Setup - Brand Explorer Presentation`.`Case Summary Overview`
- `Brand Setup - Brand Explorer Presentation`.`Case Summary Brand Relevance`
- `Brand Setup - Brand Explorer Presentation`.`Case Summary Interpretation`
- `Brand Setup - Brand Explorer Presentation`.`Case Summary Tags`

### Census fields for crosscheck (read-only)

- `Property Name`
- `Brand`
- `Affiliation Status`
- `City`
- `State / Region`
- `Country`
- `Source URL`
- `Human Review Required`
- `Public Census Eligibility`
- `Public Display Confidence`
- `Property Type`
- `Asset Context`
- `Amenities - Structured Tags`
- `Resort / Leisure Flag`
- `Extended Stay Flag`
- `Mixed-Use Flag`
- `Branded Residences Flag`
- `Latitude`
- `Longitude`

Prior crosscheck artifact status: `brand_explorer_62_background_validation_patch_plan_ready`

**No Census writes in this audit.**

## 11. Coverage gaps

| Field | Table | Current Coverage | Risk | Recommendation |
| --- | --- | --- | --- | --- |
| Brand Name | Brand Setup - Brand Basics | none | Medium | Confirm whether Brand Explorer still falls back to this Basics field; add gate or mark internal |
| Parent Company | Brand Setup - Brand Basics | none | Medium | Confirm whether Brand Explorer still falls back to this Basics field; add gate or mark internal |
| Hotel Chain Scale | Brand Setup - Brand Basics | none | Medium | Confirm whether Brand Explorer still falls back to this Basics field; add gate or mark internal |
| Brand Architecture | Brand Setup - Brand Basics | none | Medium | Confirm whether Brand Explorer still falls back to this Basics field; add gate or mark internal |
| Brand Model | Brand Setup - Brand Basics | none | Medium | Confirm whether Brand Explorer still falls back to this Basics field; add gate or mark internal |
| Hotel Service Model | Brand Setup - Brand Basics | none | Medium | Confirm whether Brand Explorer still falls back to this Basics field; add gate or mark internal |
| Year Brand Launched | Brand Setup - Brand Basics | none | Medium | Confirm whether Brand Explorer still falls back to this Basics field; add gate or mark internal |
| Brand Development Stage | Brand Setup - Brand Basics | none | Medium | Confirm whether Brand Explorer still falls back to this Basics field; add gate or mark internal |
| Brand Positioning | Brand Setup - Brand Basics | none | Medium | Confirm whether Brand Explorer still falls back to this Basics field; add gate or mark internal |
| Brand Tagline | Brand Setup - Brand Basics | none | Medium | Confirm whether Brand Explorer still falls back to this Basics field; add gate or mark internal |
| Brand Customer Promise | Brand Setup - Brand Basics | none | Medium | Confirm whether Brand Explorer still falls back to this Basics field; add gate or mark internal |
| Brand Value Proposition | Brand Setup - Brand Basics | none | Medium | Confirm whether Brand Explorer still falls back to this Basics field; add gate or mark internal |
| Brand Pillars | Brand Setup - Brand Basics | none | Medium | Confirm whether Brand Explorer still falls back to this Basics field; add gate or mark internal |
| Target Guest Segments | Brand Setup - Brand Basics | none | Medium | Confirm whether Brand Explorer still falls back to this Basics field; add gate or mark internal |
| Guest Psychographics Description | Brand Setup - Brand Basics | none | Medium | Confirm whether Brand Explorer still falls back to this Basics field; add gate or mark internal |
| Key Brand Differentiators | Brand Setup - Brand Basics | none | Medium | Confirm whether Brand Explorer still falls back to this Basics field; add gate or mark internal |
| Sustainability Positioning | Brand Setup - Brand Basics | none | Medium | Confirm whether Brand Explorer still falls back to this Basics field; add gate or mark internal |
| Brand History | Brand Setup - Brand Basics | none | Medium | Confirm whether Brand Explorer still falls back to this Basics field; add gate or mark internal |
| Logo | Brand Setup - Brand Basics | none | Medium | Confirm whether Brand Explorer still falls back to this Basics field; add gate or mark internal |
| Branded Residences Status | Brand Setup - Brand Basics | none | Medium | Confirm whether Brand Explorer still falls back to this Basics field; add gate or mark internal |
| Branded Residences Notes | Brand Setup - Brand Basics | none | Medium | Confirm whether Brand Explorer still falls back to this Basics field; add gate or mark internal |
| User_Record_ID | Brand Setup - Sustainability & ESG | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Energy Efficiency Initiatives | Brand Setup - Sustainability & ESG | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Waste Reduction Programs | Brand Setup - Sustainability & ESG | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| User_Record_ID | Brand Setup - Brand Footprint | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| AM Existing Hotel | Brand Setup - Brand Footprint | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| AM Existing Rooms | Brand Setup - Brand Footprint | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| AM Pipeline Hotel | Brand Setup - Brand Footprint | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| AM Pipeline Rooms | Brand Setup - Brand Footprint | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| CALA Existing Hotel | Brand Setup - Brand Footprint | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| CALA Existing Rooms | Brand Setup - Brand Footprint | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| CALA Pipeline Hotel | Brand Setup - Brand Footprint | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| CALA Pipeline Rooms | Brand Setup - Brand Footprint | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| EU Existing Hotel | Brand Setup - Brand Footprint | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| EU Existing Rooms | Brand Setup - Brand Footprint | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| EU Pipeline Hotel | Brand Setup - Brand Footprint | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| EU Pipeline Rooms | Brand Setup - Brand Footprint | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| MEA Existing Hotel | Brand Setup - Brand Footprint | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| MEA Existing Rooms | Brand Setup - Brand Footprint | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| MEA Pipeline Hotel | Brand Setup - Brand Footprint | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| MEA Pipeline Rooms | Brand Setup - Brand Footprint | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| APAC Existing Hotel | Brand Setup - Brand Footprint | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| APAC Existing Rooms | Brand Setup - Brand Footprint | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| APAC Pipeline Hotel | Brand Setup - Brand Footprint | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| APAC Pipeline Rooms | Brand Setup - Brand Footprint | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| AM Managed Hotel | Brand Setup - Brand Footprint | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| AM Managed Rooms | Brand Setup - Brand Footprint | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| AM Franchised Hotel | Brand Setup - Brand Footprint | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| AM Franchised Rooms | Brand Setup - Brand Footprint | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| CALA Managed Hotel | Brand Setup - Brand Footprint | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| CALA Managed Rooms | Brand Setup - Brand Footprint | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| CALA Franchised Hotel | Brand Setup - Brand Footprint | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| CALA Franchised Rooms | Brand Setup - Brand Footprint | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| EU Managed Hotel | Brand Setup - Brand Footprint | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| EU Managed Rooms | Brand Setup - Brand Footprint | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| EU Franchised Hotel | Brand Setup - Brand Footprint | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| EU Franchised Rooms | Brand Setup - Brand Footprint | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| MEA Managed Hotel | Brand Setup - Brand Footprint | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| MEA Managed Rooms | Brand Setup - Brand Footprint | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| MEA Franchised Hotel | Brand Setup - Brand Footprint | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| MEA Franchised Rooms | Brand Setup - Brand Footprint | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| APAC Managed Hotel | Brand Setup - Brand Footprint | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| APAC Managed Rooms | Brand Setup - Brand Footprint | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| APAC Franchised Hotel | Brand Setup - Brand Footprint | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| APAC Franchised Rooms | Brand Setup - Brand Footprint | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| AM New Build Hotel | Brand Setup - Brand Footprint | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| AM New Build Rooms | Brand Setup - Brand Footprint | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| AM Conversion Hotel | Brand Setup - Brand Footprint | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| AM Conversion Rooms | Brand Setup - Brand Footprint | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| CALA New Build Hotel | Brand Setup - Brand Footprint | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| CALA New Build Rooms | Brand Setup - Brand Footprint | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| CALA Conversion Hotel | Brand Setup - Brand Footprint | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| CALA Conversion Rooms | Brand Setup - Brand Footprint | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| EU New Build Hotel | Brand Setup - Brand Footprint | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| EU New Build Rooms | Brand Setup - Brand Footprint | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| EU Conversion Hotel | Brand Setup - Brand Footprint | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| EU Conversion Rooms | Brand Setup - Brand Footprint | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| MEA New Build Hotel | Brand Setup - Brand Footprint | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| MEA New Build Rooms | Brand Setup - Brand Footprint | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| MEA Conversion Hotel | Brand Setup - Brand Footprint | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| MEA Conversion Rooms | Brand Setup - Brand Footprint | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| APAC New Build Hotel | Brand Setup - Brand Footprint | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| APAC New Build Rooms | Brand Setup - Brand Footprint | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| APAC Conversion Hotel | Brand Setup - Brand Footprint | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| APAC Conversion Rooms | Brand Setup - Brand Footprint | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Number of Exits in Past 24 Months | Brand Setup - Brand Footprint | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Specific Markets/Cities | Brand Setup - Brand Footprint | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Typical Loyalty Program Name | Brand Setup - Brand Footprint | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Footprint Figures As Of | Brand Setup - Brand Footprint | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Footprint Notes | Brand Setup - Brand Footprint | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| User_Record_ID | Brand Setup - Project Fit | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Other - Priority Markets Text | Brand Setup - Project Fit | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Other - Markets to Avoid Text | Brand Setup - Project Fit | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Other (Text) - Owner Non-Negotiables | Brand Setup - Project Fit | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Owner Non-Negotiables & Decision Rights | Brand Setup - Project Fit | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Red Flag Items That Typically Make You Decline or Proceed With Caution - Risk & Compliance | Brand Setup - Project Fit | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| ESG / Sustainability Expectations You Prefer Projects to Meet - Risk & Compliance | Brand Setup - Project Fit | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Typical PIP / Repositioning Profile You Will Consider (If Existing Hotel) | Brand Setup - Project Fit | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| CapEx and FF&E Support | Brand Setup - Project Fit | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Anything else about your commercial 'sweet spot' we should know? | Brand Setup - Project Fit | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Typical PIP Range ($/room or %) | Brand Setup - Project Fit | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Who Pays for PIP | Brand Setup - Project Fit | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Regional / City Appetite Notes | Brand Setup - Project Fit | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Total Brand Portfolio Value | Brand Setup - Portfolio & Performance | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Annual Revenue (Brand Wide) | Brand Setup - Portfolio & Performance | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Brand Portfolio Growth Rate | Brand Setup - Portfolio & Performance | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Turnaround Properties in Brand | Brand Setup - Portfolio & Performance | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| PMS (Property Management System) | Brand Setup - Portfolio & Performance | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Revenue Management System | Brand Setup - Portfolio & Performance | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Accounting / Reporting System | Brand Setup - Portfolio & Performance | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Guest Communication / Mobile Check-in | Brand Setup - Portfolio & Performance | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Data / Analytics Platform Required or Approved | Brand Setup - Portfolio & Performance | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Required Integrations (OTAs, Payments, etc.) | Brand Setup - Portfolio & Performance | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| User_Record_ID | Brand Setup - Brand Standards | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Typical Outlet Names / Concepts | Brand Setup - Brand Standards | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Typical Meeting Space Size | Brand Setup - Brand Standards | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Other Sustainability Text | Brand Setup - Brand Standards | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Other Amenities Text - Amenities | Brand Setup - Brand Standards | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Other Text - Compliance | Brand Setup - Brand Standards | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Typical QA / Brand Standards Expectations | Brand Setup - Brand Standards | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Additional Brand Standards Notes | Brand Setup - Brand Standards | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| User_Record_ID | Brand Setup - Deal Terms | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Length - Typical Minimum Initial Term | Brand Setup - Deal Terms | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Quantity - Typical Minimum Initial Term | Brand Setup - Deal Terms | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Length - Typical Renewal Option | Brand Setup - Deal Terms | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Quantity - Typical Renewal Option | Brand Setup - Deal Terms | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Length - Typical Renewal Notice Period | Brand Setup - Deal Terms | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Typical Renewal Conditions | Brand Setup - Deal Terms | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Typical Cure Period for Performance Test Failure | Brand Setup - Deal Terms | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Conversion - Typical max time allowed for completion | Brand Setup - Deal Terms | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Renewal - Typical max time allowed for completion | Brand Setup - Deal Terms | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| User_Record_ID | Brand Setup - Fee Structure | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Additional Notes - Typical Application Fee | Brand Setup - Fee Structure | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Additional Notes - Typical Royalty Fee Range | Brand Setup - Fee Structure | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Additional Notes - Typical Marketing Fee Range | Brand Setup - Fee Structure | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Additional Notes - Typical Tech | Brand Setup - Fee Structure | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Additional Notes - Typical Loyalty Program Fee | Brand Setup - Fee Structure | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Typical Incentives Offered | Brand Setup - Fee Structure | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Early-Termination Notes | Brand Setup - Fee Structure | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Typical Termination Fee Structure (if any) Text | Brand Setup - Fee Structure | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Typical Expectations for Owner-Funded Reserves | Brand Setup - Fee Structure | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Additional Notes - Typical Training Fee | Brand Setup - Fee Structure | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| DELETE>>>> | Brand Setup - Fee Structure | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Description of Fee | Brand Setup - Fee Structure | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Min - Typical Incentive Fee | Brand Setup - Fee Structure | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Max - Typical Incentive Fee | Brand Setup - Fee Structure | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Notes - Typical Incentive Fee | Brand Setup - Fee Structure | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Notes - Typical Incentive Fee Excess | Brand Setup - Fee Structure | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| User_Record_ID | Brand Setup - Operational Support | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
|  Other Text | Brand Setup - Operational Support | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Key Money Payment Timing | Brand Setup - Operational Support | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Key Money Clawback Terms | Brand Setup - Operational Support | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Key Money Payment Structure | Brand Setup - Operational Support | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Other Text - Revenue Management Services | Brand Setup - Operational Support | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Other Text - Sales & Marketing Support | Brand Setup - Operational Support | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Other Text - Accounting & Financial Reporting | Brand Setup - Operational Support | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Other Text - Procurement Services | Brand Setup - Operational Support | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Other Text - HR & Training Services | Brand Setup - Operational Support | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Other Text - Technology Services | Brand Setup - Operational Support | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Other Text - Design & Renovation Support | Brand Setup - Operational Support | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Other Text - Development Services | Brand Setup - Operational Support | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Service Offering Summary | Brand Setup - Operational Support | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Decision-Making Process | Brand Setup - Operational Support | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Dispute Resolution Approach | Brand Setup - Operational Support | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Testimonial Links | Brand Setup - Operational Support | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Industry Recognition | Brand Setup - Operational Support | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Major Lenders Worked With | Brand Setup - Operational Support | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Specializations | Brand Setup - Operational Support | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Key Owner Success Stories | Brand Setup - Operational Support | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Owner Portal Features | Brand Setup - Operational Support | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Additional Notes | Brand Setup - Operational Support | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Ongoing Support Included | Brand Setup - Operational Support | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| CRS / Central Res. Participation | Brand Setup - Operational Support | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| GDS Participation | Brand Setup - Operational Support | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Owner Portal Notes | Brand Setup - Operational Support | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| User_Record_ID | Brand Setup - Legal Terms | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Radius - Typical Area of Protection | Brand Setup - Legal Terms | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Restrictions - Typical Area of Protection | Brand Setup - Legal Terms | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| LOI Timeline to Agreement | Brand Setup - Legal Terms | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| For Cause - Termination Rights | Brand Setup - Legal Terms | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Without Cause - Termination Rights | Brand Setup - Legal Terms | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| On Sale - Termination Rights | Brand Setup - Legal Terms | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Liquidated Damages | Brand Setup - Legal Terms | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Exclusivity | Brand Setup - Legal Terms | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Binding - LOI Binding vs NonBinding Terms | Brand Setup - Legal Terms | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Non-Binding - LOI Binding vs NonBinding Terms | Brand Setup - Legal Terms | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Confidentiality | Brand Setup - Legal Terms | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Conditions Precedent | Brand Setup - Legal Terms | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Buyout / Transfer Provisions | Brand Setup - Legal Terms | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Assignment Restrictions | Brand Setup - Legal Terms | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| Typical Loyalty Program Name | Brand Setup - Loyalty & Commercial | none | Low | Classify usage; exclude from Batch 1; add validation only if product-rendered |
| (all fields) | Brand Setup - Sustainability & ESG | not_in_brand_explorer_active_62_gates | Medium | Do not claim Batch 1 / Active-62 gates cover these tables; separate Brand Setup form QA if needed |
| (all fields) | Brand Setup - Brand Footprint | not_in_brand_explorer_active_62_gates | Medium | Do not claim Batch 1 / Active-62 gates cover these tables; separate Brand Setup form QA if needed |
| (all fields) | Brand Setup - Project Fit | not_in_brand_explorer_active_62_gates | Medium | Do not claim Batch 1 / Active-62 gates cover these tables; separate Brand Setup form QA if needed |
| (all fields) | Brand Setup - Portfolio & Performance | not_in_brand_explorer_active_62_gates | Medium | Do not claim Batch 1 / Active-62 gates cover these tables; separate Brand Setup form QA if needed |
| (all fields) | Brand Setup - Brand Standards | not_in_brand_explorer_active_62_gates | Medium | Do not claim Batch 1 / Active-62 gates cover these tables; separate Brand Setup form QA if needed |
| (all fields) | Brand Setup - Deal Terms | not_in_brand_explorer_active_62_gates | Medium | Do not claim Batch 1 / Active-62 gates cover these tables; separate Brand Setup form QA if needed |
| (all fields) | Brand Setup - Fee Structure | not_in_brand_explorer_active_62_gates | Medium | Do not claim Batch 1 / Active-62 gates cover these tables; separate Brand Setup form QA if needed |
| (all fields) | Brand Setup - Operational Support | not_in_brand_explorer_active_62_gates | Medium | Do not claim Batch 1 / Active-62 gates cover these tables; separate Brand Setup form QA if needed |
| (all fields) | Brand Setup - Legal Terms | not_in_brand_explorer_active_62_gates | Medium | Do not claim Batch 1 / Active-62 gates cover these tables; separate Brand Setup form QA if needed |
| (all fields) | Brand Setup - Loyalty & Commercial | not_in_brand_explorer_active_62_gates | Medium | Do not claim Batch 1 / Active-62 gates cover these tables; separate Brand Setup form QA if needed |

## 12. Recommendation before applying Batch 1A

- **Batch 1A within its declared scope (Presentation Low-risk safe_text_cleanup):** **safe to apply**
- **Full Brand Setup field coverage claim:** **not met** — additional validation needed for child Brand Setup tables and unclassified Basics fields before saying “all Brand Setup fields are reviewed.”

- Confirm Batch 1A scope remains Presentation Title/Body/Case Summary* only
- Do not interpret Active-62 gate PASS as full Brand Setup field coverage
- Keep child Brand Setup tables out of Batch 1
- Keep protected/release/Recent Momentum fields out of Batch 1
- Schedule separate Brand Setup form/schema coverage if founder needs full-table QA

Local apply report indicates Batch 1A was already applied in a prior session; this audit is read-only and did not apply patches.

**Final status:** `brand_setup_full_field_coverage_audit_complete_additional_validation_needed`

