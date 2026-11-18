# Weekly Change verification

- Research inventory upserts do **not** invent Weekly Change events
- `firstDiscoveredAt` / `firstDiscoveredRunId` preserved on UNCHANGED and UPDATE
- Material updates may refresh `lastMaterialChangeAt` / `lastResearchedAt` from source only when present
