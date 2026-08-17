# Recommended Airtable Views — Operator Setup - Master

Minimum set (do not create redundant views):

| View | Filter |
| ---- | ------ |
| **OE — All Operator Masters** | (none) |
| **OE — Production** | `{Record Purpose} = "Production"` |
| **OE — Research** | `{Record Purpose} = "Research"` |
| **OE — Test Fixtures** | `{Record Purpose} = "Test Fixture"` |
| **OE — Explorer Publishable** | Production + manual or synced publishable flag *(until formula/sync exists, use internal dashboard)* |
| **OE — Needs Enrichment** | Real operators not publishable *(approx: Production OR Research, exclude fixtures)* |
| **OE — Fit Data Ready** | Internal diagnostic only — optional |

**Do not rename** existing Grid view without founder approval. Add OE-prefixed views alongside.
