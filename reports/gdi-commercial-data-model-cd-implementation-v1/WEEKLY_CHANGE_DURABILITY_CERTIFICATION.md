# Weekly Change durability certification (CD-1)

**Contract:** `gdi-weekly-change-durability-v1`  
**Engine:** `weekly-delta.js` (`MATERIAL_FIELDS`)  
**History table:** none (approved)

## Durable storage layers

1. Filesystem hotel `opportunities.json` annotations (`weeklyDeltaState`, `firstSeenRunId`, material stamps)
2. `opportunityPayloadJson` commercial keys
3. Existing GDO typed columns: `firstDiscoveredAt`, `firstDiscoveredRunId`, `lastMaterialChangeAt`, `lastMaterialChangeRunId`

## Determinism rules

- NEW only when `firstSeenRunId` equals the current completed weekly run (not description rewrite).
- UPDATED only when a `MATERIAL_FIELDS` value changes.
- Description / summary / notes rewrite → UNCHANGED (certified in unit test).
- Survives process restart when FS + payload + typed columns are populated together.

## Deployment / reconciliation

Weekly Change remains correct across deployments when approved storage reconciliation keeps opportunity ids stable and provenance stamps are not wiped by research sync (research-owned columns now include provenance; sales-owned keys stay protected).
