# Traveling Entity Persistence

## Bug found: YES (prior run)

`buildOpportunity` / customer enrich dropped second-gen stamps until factory pass-through was added.
Surface `teamProof()` ignored `travelingEntityProven` even when persisted — exhibitor-style rows failed `exhibitor_style_missing_team_proof`.

## Fix

1. Enrich factory pass-through (prior P0) keeps stamps in Airtable payload.
2. `teamProof()` now accepts `travelingEntityProven` + type/evidence and `groupMotionType`+`groupMotionEvidence`.
3. Final-mile material update restamps groupMotion* from traveling-entity fields.

## Counts

- Traveling entity persisted end-to-end BEFORE (cohort): 11
- Traveling entity persisted end-to-end AFTER: 11
