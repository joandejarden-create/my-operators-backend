# Existing field coverage (GDO + related)

Source of truth for typed GDO columns: `lib/group-demand-intelligence/opportunity-field-map.js` (`MAP_GDI_OPPORTUNITY`).

## Group Demand Opportunities (structured)

Identity: `opportunityId`, `hotelId`, `organizationId`, `organizationName`, `organizationEntityId`, `opportunityName`  
Commercial: `opportunityType`, `priority`, `actionStatus`, `qualification`, `hotelFit`, `evidenceConfidence`  
Timing: `eventStartDate`, `eventEndDate`, `whyNow`, `recommendedAction`  
Sourcing: `venueStatus`, `sourcingStatus`, `roomDemandStatus`, `peakRooms`, `attendance`, `attendanceStatus`  
Contact: `primaryContactId/Name/Role/Email/Phone`, `contactQuality`  
Evidence: `sourceCount`, `sourceSummary`, `lastResearchedAt`, `researchVersion`  
Linkage: `decisionId`, `peVenueId`, `peSignalId`, `hotelVenueFitId`, `demandFamily`, `demandSignalType`  
Provenance: `firstDiscoveredAt`, `firstDiscoveredRunId`, `discovery*`, `lastMaterialChangeAt`, `lastMaterialChangeRunId`, `runId`  
Fidelity: `opportunityPayloadJson`, `isTestData`

## Demand Generators / Programs / Signals

See `demand-generators/airtable-field-map.js` — org identity, recurrence, cycle JSONs, fit, signal triggers.

## Decisions / Decision Events

See `docs/decision-outcomes-airtable-fields.md` — VALIDATION / ACTION / OUTCOME with optional roomNights / financialValue.

## Payload / FS (not typed)

`weeklyDeltaState`, `salesActionKey`, `bookingWindowStatus`, CMI competitor fields, `revenueStreamClass`, `eligibilityGaps`, estimated room nights, multi-contact arrays.
