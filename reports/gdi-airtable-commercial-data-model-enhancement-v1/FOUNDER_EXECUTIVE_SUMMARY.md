# Commercial data-model enhancement audit V1 — founder summary

**STOP FOR FOUNDER REVIEW** before any new Airtable fields, tables, or production writes.  
**Related:** PR #362 purpose audit · PR #363 persistence adapter (fail-closed, merged to `main`)

## Answers

1. **Already exist (structured):** opportunityId/hotelId, venueStatus/sourcingStatus, event dates, whyNow, peakRooms/roomDemandStatus, primary contact typed fields, evidenceConfidence/lastResearchedAt, Decisions/Events feedback path, Demand Programs recurrence fields, DG org identity.  
2. **Not populated correctly / drift:** Weekly Change annotations; salesActionKey / date pills / CMI often payload-only and compacted; GDO↔DG links unused; discovery provenance map fields rarely written.  
3. **Trapped in JSON/FS:** bookingWindowStatus, competitorHotel/CMI, salesAction*, revenueStreamClass, eligibilityGaps, weeklyDeltaState, estimatedRoomNights, execution ledger evidence.  
4. **Genuinely missing:** almost nothing required — no research $ revenue SoR; no Contacts table needed now; no Weekly Change history table.  
5. **Highest commercial value:** populate payload keys without compaction; wire DG/Program ids onto GDO; durable Weekly Change provenance; CMI competitor fields.  
6. **Essential before persistence activation:** eligibility gates (done in #363); Match Key (done); deny-list (done); decide CD-1/CD-2/CD-3/CD-8.  
7. **Can wait:** Contacts table; typed competitor columns; modeled $ revenue; seasonality migration.  
8. **Weekly Change:** reuse `weekly-delta.js` + existing firstDiscovered/lastMaterialChange map fields — populate, don’t invent a history table.  
9. **Competitor booking:** reuse CMI payload contract (`competitorHotel`, `competitorVenueStatus`, evidence status).  
10. **Buyers/contacts:** keep embedded primary contact + contactClass in payload; DG for planner orgs.  
11. **Multi-hotel reuse:** Demand Generators / Programs / Signals as shared; hotel-private GDO + feedback.  
12. **Isolation:** hotelId on GDO; feedback never to global DG; publication packages for customer.  
13. **New fields necessary:** **prefer zero** this wave; optional text ids `dgGeneratorId`/`dgProgramId`/`dgSignalId` if CD-3-A approved.  
14. **Scale cleanliness:** yes — reuse + populate beats schema sprawl.  
15. **Implement first:** CD-8 dry-run scope → CD-1 Weekly Change population → CD-2 payload non-compaction → CD-3 link ids.

## Recommended posture

**GROUP A/B first. GROUP C only for optional DG link text ids. No GROUP D tables. Defer GROUP E.**
