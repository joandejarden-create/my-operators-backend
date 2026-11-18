# Weekly Change and history certification

- Persistence must preserve `firstDiscoveredAt` / `firstDiscoveredRunId` / `lastMaterialChangeAt`
- No-change reruns must not invent new opportunity IDs
- Weekly Change remains deterministic from durable history fields
- Research inventory layer must carry the same discovery/history metadata as FS SoT
- GDO updates must use commercial payload preservation (PR #366) — no silent compacting of thesis/evidence/contacts
