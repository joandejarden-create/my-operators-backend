# Brand & Relationships — Explorer Airtable fields

**Primary table:** `Operator Setup - Profile & Positioning`  
**Read path:** `buildPrefillObjectFromNewBaseRows` → `prefill` / `mergeExplorerPrefill` → `ex`  
**UI module:** `public/js/operator-brand-relationships-sections.js`  
**Field map:** `api/lib/operator-brand-explorer-field-map.js`

## Coverage audit (tab vs Airtable)

| UI surface | Prefill key | Airtable column | Status |
|------------|-------------|-----------------|--------|
| Snapshot — Brand relationships | `numberOfBrands` | `numberOfBrands` | Existing (build sheet) |
| Snapshot — Branded / Independent % | `brandedVsIndependentMix` | `brandedVsIndependentMix` | **Created by explorer schema script** |
| Snapshot — Conversion / reflag | `brand_conversion_project_count` | `brand_conversion_project_count` | **Created by explorer schema script** |
| Snapshot — Approved families | `numberOfBrands` (fallback: mix JSON) | `numberOfBrands` / `brand_portfolio_mix_json` | Linked |
| Snapshot — Primary segments | (derived) | `brand_relationship_depth_json` row count | Linked via JSON |
| Portfolio Mix table | `brand_portfolio_mix_json` | `brand_portfolio_mix_json` | **Created by explorer schema script** |
| Relationship Depth table | `brand_relationship_depth_json` | `brand_relationship_depth_json` | **Created by explorer schema script** |
| Execution cards | `brand_execution_capabilities_json` | `brand_execution_capabilities_json` | **Created by explorer schema script** |
| Governance cards | `brand_governance_compliance_json` | `brand_governance_compliance_json` | **Created by explorer schema script** |
| Soft brand narrative | `brand_soft_independent_narrative` | `brand_soft_independent_narrative` | **Created by explorer schema script** |

### Existing Profile fields (not rendered on this tab)

Still on Profile for Setup / alignment: `brand_signal_*`, `brand_narrative_*`, `brands` (link).

### Platform field (legacy footprint; not this tab)

`Brands Portfolio Detail` on **Platform & Markets** → `brandsPortfolioDetail` — used elsewhere, not Brand & Relationships subsections.

## JSON shapes

| Prefill key | JSON shape |
|-------------|------------|
| `brand_portfolio_mix_json` | `[{ brandFlagType, portfolioMix, assetContext, relationshipStatus }]` |
| `brand_relationship_depth_json` | `[{ brandSegment, relationshipType, depth, ownerContext }]` |
| `brand_execution_capabilities_json` | `[{ title, description }]` |
| `brand_governance_compliance_json` | `[{ title, description }]` |

## Scripts

```bash
node scripts/ensure-operator-brand-explorer-schema.mjs --apply
node scripts/seed-operator-brand-explorer-data.mjs --apply
node scripts/seed-operator-brand-explorer-data.mjs --apply --force
```

Seed writes JSON subsections, narrative, conversion count, branded/independent mix string, and `numberOfBrands` when those cells are empty.
