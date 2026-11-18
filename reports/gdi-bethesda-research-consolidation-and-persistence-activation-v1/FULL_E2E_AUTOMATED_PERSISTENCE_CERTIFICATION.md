# Full E2E automated persistence certification

Bridge: `gdi-full-e2e-research-persistence-bridge-v1`

| Capability | Status |
|------------|--------|
| Discover → resolve IDs → dedupe → persist research | Bridge wired in canonical Full E2E entry |
| Material update vs UNCHANGED | `diffMaterialResearchFields` / store upsert |
| Preserve firstDiscovered* | Store preserves on update |
| Partial failure isolation | Per-row try/catch in bridge |
| Read-back option | `readBackResearchPersistence` |
| GDO / publication separate | Rejected on research bridge |
| Default production | **DISABLED** (`GDI_RESEARCH_INVENTORY_PRODUCTION_WRITES`) |
| Apply with token but env off | dry-run + blockedApply (certified) |

Production unlocked now: **false**
