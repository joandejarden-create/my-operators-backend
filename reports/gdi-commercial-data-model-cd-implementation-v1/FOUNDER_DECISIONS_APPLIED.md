# Founder decisions applied (PR #365 review)

| Decision | Approved option | Implementation |
|----------|-----------------|----------------|
| CD-1 | A (+ durability verification) | `gdi-weekly-change-durability-v1.js`; typed provenance columns populated; no history table |
| CD-2 | A | `REQUIRED_COMMERCIAL_PAYLOAD_KEYS` + reattach after compaction |
| CD-3 | Amended A | Wire existing `dgGeneratorId` / `dgProgramId` / `dgSignalId` / `hotelGeneratorFitId` + link fields into `MAP_GDI_OPPORTUNITY`; no recreate |
| CD-4 | A | `mayUpsertDemandProgram` requires RECURRING_CONFIRMED/HISTORICAL + evidence; blocks onboarding mirrors |
| CD-5 | A | CMI keys retained in commercial payload durability |
| CD-6 | A | `estimatedRoomNights*` claim/methodology keys; no $ revenue |
| CD-7 | A | `eligibilityGaps` / `salesReadinessBlockers` + rule version |
| CD-8 | A | Adapter apply remains `AIRTABLE_PRODUCTION_WRITES_NOT_AUTHORIZED` |
