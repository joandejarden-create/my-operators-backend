# Cross-hotel shared identity (CD-3 / CD-4)

**Module:** `gdi-shared-demand-identity-v1.js`

## Shared (global)

- `dgGeneratorId` / Demand Generator
- `dgProgramId` / Demand Program (only when recurrence evidence passes CD-4 gate)
- `dgSignalId` / Demand Generator Signal
- `eventSeriesId` / `eventCycleId` in payload

## Hotel-private

- Distinct `opportunityId` + `hotelId` GDO rows
- `hotelGeneratorFitId`
- Sales feedback / notes / Decisions (`stripHotelPrivateKeys`)

## CD-4 program writes

Allowed only for `RECURRING_CONFIRMED` or `RECURRING_HISTORICAL` with evidence.  
Onboarding mirrors / generic seeds are refused.
