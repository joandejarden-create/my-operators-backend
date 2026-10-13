# Persistence QA

| Layer | Path / mechanism | Match |
|-------|------------------|-------|
| Filesystem | `data/group-demand-intelligence/hotels/{hotelId}/pursuits.json` | YES |
| Opportunity mirror | `pursuitId` / `pursuitStatus` via `upsertSingleOpportunity` | YES |
| Airtable | Opportunity payload JSON (no new required columns) | YES (payload fidelity) |
| API | `api/gdi-pursuit.js` | YES |
| UI | list overlay + drawer panel | YES |
| Audit | `pursuits.json` → `audit[]` | YES |

## Source of truth

Pursuit store is authoritative for workflow. Opportunity fields are a convenience mirror only.
