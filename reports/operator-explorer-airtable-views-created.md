# Airtable OE Views

## API capability

Airtable Metadata API **cannot create** filtered views (attempt status 422).

## What Wave 01 did

Synced Master fields for view filters:

- OE Explorer Publishable
- OE Strong Profile
- OE Fit Data Ready
- OE Enrichment Class

## Recipes to create in Airtable UI

| View | Filter | Expected |
| ---- | ------ | -------- |
| OE — All | `(none)` | 46 |
| OE — Production | `{Record Purpose} = "Production"` | 24 |
| OE — Research | `{Record Purpose} = "Research"` | 13 |
| OE — Test Fixtures | `{Record Purpose} = "Test Fixture"` | 9 |
| OE — Explorer Publishable | `{OE Explorer Publishable}` | dynamic |
| OE — Needs Enrichment | `OR({OE Enrichment Class}="Production Needs Enrichment",{OE Enrichment Class}="Research Needs Enrichment",{OE Enrichment Class}="Research Content Complete Gated")` | dynamic |
| OE — Strong Profiles | `{OE Strong Profile}` | dynamic |
| OE — Fit Data Ready | `{OE Fit Data Ready}` | dynamic |

## Validated purpose counts (API)

```json
{
  "all": 46,
  "production": 36,
  "research": 1,
  "testFixtures": 9,
  "publishableCanonical": 36,
  "strongCanonical": 13,
  "fitReadyCanonical": 4,
  "needsEnrichmentCanonical": "dynamic"
}
```

> Post–Research Graduation: Production 36 · Research 1 (Radisson RHG EMEA/APAC scoped) · Test Fixtures 9. Refresh manual OE views if already created.
