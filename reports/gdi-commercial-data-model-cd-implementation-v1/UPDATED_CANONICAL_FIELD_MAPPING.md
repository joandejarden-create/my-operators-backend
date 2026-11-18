# Updated canonical GDO field mapping

**Authority:** `lib/group-demand-intelligence/opportunity-field-map.js` → `MAP_GDI_OPPORTUNITY`  
**Writer:** `opportunityToAirtableFields` in `airtable-opportunity-store.js`

## Added to core map (existing live columns — not new schema)

| Map key | Airtable field | Role |
|---------|----------------|------|
| `dgGeneratorId` | `dgGeneratorId` | Shared org identity text id |
| `dgProgramId` | `dgProgramId` | Shared program / series text id |
| `dgSignalId` | `dgSignalId` | Shared dated occurrence text id |
| `hotelGeneratorFitId` | `hotelGeneratorFitId` | Hotel-private fit text id |
| `dgGeneratorLink` | `Demand Generator` | Optional linked-record mirror |
| `dgProgramLink` | `Demand Program` | Optional linked-record mirror |
| `dgSignalLink` | `Demand Generator Signal` | Optional linked-record mirror |
| `dgSignalsLink` | `Demand Generator Signals` | Optional multi-link mirror |

Provenance columns already mapped are now **written** (not payload-only):

- `firstDiscoveredAt` / `firstDiscoveredRunId`
- `discoveryTargetId` / `discoveryTargetRunId` / `discoveryMethod` / `discoveryPlaybook` / `discoverySource`
- `lastMaterialChangeAt` / `lastMaterialChangeRunId`

Linked-record fields are written only when resolved Airtable record id arrays are supplied (`dgGeneratorAirtableRecordIds`, etc.). Text ids remain the durable join keys.
