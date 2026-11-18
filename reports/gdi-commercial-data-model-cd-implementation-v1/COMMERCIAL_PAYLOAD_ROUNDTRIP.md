# Commercial payload round-trip (CD-2 / CD-5 / CD-6 / CD-7)

**Module:** `gdi-commercial-payload-durability-v1.js`  
**Hook:** `compactOpportunityPayloadForAirtable` → `reattachDurablePayloadKeys`

## Preserved keys (non-exhaustive)

- Presentation: `salesActionKey`, `salesActionPillLabel`, `commercialStageKey`, `revenueStreamClass`, `bookingWindowStatus`
- CMI: `competitorHotel`, `cmiCompetitorHotel`, `competitorVenueStatus`, `cmiEvidenceStatus`
- Weekly Change: `weeklyDeltaState`, `firstSeenRunId`, material stamps
- Qualification: `eligibilityGaps`, `salesReadinessBlockers`, `salesReadinessRuleVersion`
- Room nights: `estimatedRoomNights` + claim kind / methodology / confidence (no $ revenue)

Internal research blobs (`exaRawHits`, etc.) may still drop under size pressure. Commercial keys are re-attached after every compaction floor.
