# Operator Explorer DNA — JSON subsection fields

**Registry:** `lib/operator-dna-explorer-json-fields.js`  
**Sync bindings + build sheet:** `node scripts/sync-operator-dna-explorer-json-fields.mjs`  
**Create Airtable columns:** `node scripts/ensure-operator-dna-explorer-json-schema.mjs --apply`

All 22 fields use **multilineText** in Airtable (column name = form key). No select options.

## Tables

| Airtable table | Fields |
|----------------|--------|
| Operator Setup - Platform & Markets | `op_*` (5), `mkt_regional_expertise_json`, `mkt_market_fit_signals_json` |
| Operator Setup - Profile & Positioning | `brand_*_json` (4) |
| Operator Setup - Commercial Fit & Terms | `ov_*_json` (6), `bf_*_json` (5) |

## Field list

| formKey | DNA tab | JSON shape |
|---------|---------|------------|
| `op_commercial_engine_json` | Operating Platform | `{ intro, items[{ title, description }] }` |
| `op_owner_reporting_json` | Operating Platform | same |
| `op_preopening_transition_json` | Operating Platform | same |
| `op_conversion_repositioning_json` | Operating Platform | same |
| `op_fb_lifestyle_resort_json` | Operating Platform | same |
| `brand_portfolio_mix_json` | Brand | `[{ brandFlagType, portfolioMix, assetContext, relationshipStatus }]` |
| `brand_relationship_depth_json` | Brand | `[{ brandSegment, relationshipType, depth, ownerContext }]` |
| `brand_execution_capabilities_json` | Brand | `[{ title, description }]` |
| `brand_governance_compliance_json` | Brand | `[{ title, description }]` |
| `ov_strategic_owner_value_json` | Owner Engagement | `[{ title, description }]` |
| `ov_engagement_cadence_json` | Owner Engagement | `[{ title, description }]` |
| `ov_controls_governance_json` | Owner Engagement | `[{ title, description }]` |
| `ov_reports_received_json` | Owner Engagement | `[{ title, description }]` |
| `ov_owner_tools_json` | Owner Engagement | `[{ title, description }]` |
| `ov_lifecycle_support_json` | Owner Engagement | `[{ title, description }]` |
| `mkt_regional_expertise_json` | Markets | `[{ title, description }]` |
| `mkt_market_fit_signals_json` | Markets | `[{ title, description }]` |
| `bf_fit_criteria_json` | Project Fit | `[{ title, description }]` |
| `bf_best_fit_project_types_json` | Project Fit | `[{ title, description }]` |
| `bf_preferred_deal_profile_json` | Project Fit | `[{ title, description }]` |
| `bf_evaluation_path_json` | Project Fit | `[{ title, description }]` |
| `bf_red_flags_json` | Project Fit | `[{ title, description }]` |

## Data flow

```text
Setup form (name=formKey) → POST intake → applyNewTwoFieldsToCompact → new-base writer (build sheet)
  → Airtable → buildPrefillObjectFromNewBaseRows → mergeExplorerPrefill → DNA section modules
```

When JSON is empty, section modules still show **DEFAULTS** until operators save valid JSON.

## Seed realistic data (all 22 fields)

```bash
node scripts/seed-operator-dna-explorer-json-data.mjs --apply
node scripts/seed-operator-dna-explorer-json-data.mjs --apply --force   # overwrite existing JSON
node scripts/verify-operator-dna-explorer-json-links.mjs                # prefill read-path check
```

Payload shapes are defined in `lib/operator-dna-explorer-json-seed-data.js` (aligned with DNA section parsers).

| DNA tab | Fields seeded on |
|---------|------------------|
| Operating Platform | Platform & Markets (`op_*`) |
| Brand & Relationships | Profile (`brand_*_json`) |
| Markets & Footprint | Platform (`mkt_*_json`) |
| Owner Engagement & Reporting | Commercial (`ov_*_json`) |
| Project Fit & Deal Profile | Commercial (`bf_*_json`) |
