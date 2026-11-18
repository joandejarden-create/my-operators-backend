# Weekly Change data contract

**Engine:** `lib/group-demand-intelligence/weekly-delta.js`  
**States:** NEW · UPDATED · REACTIVATED · UNCHANGED · CLOSED_DOWNGRADED · NOT_IN_CURRENT  
**Material fields:** eventStart/End, venue/sourcing status, opportunityType, priority, roomDemandStatus, hostHotel/venue, primary contact name/email/phone.

## Reliable identification

| Change type | How |
|-------------|-----|
| NEW | `firstSeenRunId` == current run (not rewrite-only) |
| Materially UPDATED | Diff on `MATERIAL_FIELDS` → stamp `lastMaterialChangeAt` / `lastMaterialChangeRunId` |
| Newly VERIFIED evidence | Prefer researchVersion / evidenceConfidence change + material stamp if commercial |
| Changed dates / sourcing / buyer / rooms | Covered by MATERIAL_FIELDS |
| Competitor placement | Via CMI payload fields if included in material view (extend carefully) |
| No material change | UNCHANGED — description rewrite alone must not force NEW |

## Authority

- Detailed immutable change records → FS weekly refresh / execution outputs  
- Airtable → existing provenance columns + optional `weeklyDeltaState` in payload  
- **Do not** create a Weekly Change history table (unless founder overrides CD-1)
