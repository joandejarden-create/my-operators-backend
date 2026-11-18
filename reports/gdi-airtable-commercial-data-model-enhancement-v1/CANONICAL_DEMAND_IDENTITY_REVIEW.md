# Canonical demand identity review

| Layer | Identity | Table / store | Multi-hotel? |
|-------|----------|---------------|--------------|
| Organization | Demand Generator ID | Demand Generators + FS global-intelligence | Shared |
| Recurring series | Program ID / Series ID | Demand Programs | Shared |
| Dated occurrence | Signal ID / Cycle ID | Demand Generator Signals | Shared |
| Hotel opportunity | opportunityId + hotelId | GDO + FS opportunities.json | Hotel-private row |
| Venue | Venue ID | Private Event Venues | Shared |
| Hotel×org / hotel×venue | Fit IDs | Hotel Demand Generator Fit / Hotel Venue Fit | Hotel-private |
| Feedback | decisionId / eventId | Decisions / Events + FS feedback | Hotel-private |

**Conclusion:** Existing IDs can represent org / series / occurrence / hotel opportunity **if** GDO stores `dgGeneratorId` / `dgProgramId` / `dgSignalId` (text). No new “demand occurrence” table required. Never merge hotel GDOs into one global opportunity. Never put sales notes on shared DG.
