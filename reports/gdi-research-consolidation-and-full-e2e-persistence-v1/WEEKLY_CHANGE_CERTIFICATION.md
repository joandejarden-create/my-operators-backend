# Weekly Change certification

Uses existing `gdi-weekly-change-durability-v1` / multi-hotel reconciliation contracts.

| Requirement | Status |
|-------------|--------|
| firstDiscovered* preserved on research upsert | Enforced in `upsertResearchInventoryRecord` |
| Material change vs unchanged | `diffMaterialResearchFields` |
| No reset of discovery dates during consolidation | No Airtable migration applied |
| Shared DG/Venue reuse | Unchanged; no global rewrite |
| Hotel-private pursuits | Quarantined; not merged |

No live weekly-change mutation executed in this task.

