# Proposed minimal schema changes

**Default recommendation: zero new columns this wave.**

## If founder approves CD-3-A only

| Owning table | Field | Type | Why alternatives insufficient |
|--------------|-------|------|-------------------------------|
| Group Demand Opportunities | `dgGeneratorId` | singleLineText | `organizationName` alone cannot stable-join DG |
| Group Demand Opportunities | `dgProgramId` | singleLineText | Recurrence SoR is Programs |
| Group Demand Opportunities | `dgSignalId` | singleLineText | Dated occurrence link |
| Group Demand Opportunities | `hotelGeneratorFitId` | singleLineText | Hotel-private fit join |

Optional later (not required to activate persistence): Airtable linked-record mirrors of the above.

## Explicitly not proposing

- Weekly Change history table  
- Contacts table  
- Competitor intelligence table  
- Research revenue $ columns  
- Need-date / Decisions schema changes for Full E2E
