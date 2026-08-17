# Operator case studies — Airtable fields

## Tables

| Base path | Table name |
|-----------|------------|
| New-base (Operator Setup) | `Operator Setup - Case Studies` |
| Legacy intake | `3rd Party Operator - Case Studies` |

## Required columns (existing)

| API key | New-base column | Legacy column |
|---------|-----------------|---------------|
| `property_name` | `property_name` | Property Name |
| `hotel_type` | `hotel_type` | Hotel Type |
| `region` | `region` | Region |
| `branded_independent` | `branded_independent` | Branded / Independent |
| `situation` | `situation` | Situation |
| `services` | `services` | Services |
| `outcome` | `outcome` | Outcome |
| `owner_relevance` | `owner_relevance` | Owner Relevance |
| `image_url` | `image_url` | Image URL |

## New columns (add in Airtable UI)

| API key | New-base column | Legacy column | Type | UI modal section |
|---------|-----------------|---------------|------|------------------|
| `challenge` | `challenge` | Challenge | Long text | **Challenge** |
| `data_status` | `data_status` | Data Status | Long text (or single select) | **Data Status** |

Until these columns exist, the app **derives** Challenge from Situation and Data Status from field completeness.

## Seed example data (Antillano Norte)

```bash
node scripts/seed-operator-case-studies.mjs recTUjuDxL96yWcQA --dry-run
node scripts/seed-operator-case-studies.mjs recTUjuDxL96yWcQA
```

Fixture: `fixtures/operator-case-studies-antillano-norte.json`

## Code mapping

Central map: `api/lib/operator-case-study-airtable-map.js` (`map_operatorCaseStudyFields`).
