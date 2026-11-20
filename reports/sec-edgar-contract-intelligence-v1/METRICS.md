# SEC EDGAR Contract Intelligence — Pilot Metrics

**Contract:** SEC_EDGAR_CONTRACT_INTELLIGENCE_V1 `1.0.0`  
**Mode:** DRY_RUN  
**Generated:** 2026-11-20T06:36:45.111Z  
**Airtable gate:** PERSISTENCE_DESIGNED

| Metric | Count |
|---|---|
| companiesResearched | 4 |
| secFilingsReviewed | 42 |
| documentsAndExhibitsProcessed | 42 |
| hotelsDiscoveredIndependentOfHpc | 290 |
| hotelsMatchedToHpc | 0 |
| hotelsNotInHpc | 290 |
| contractVerifiedPendingHpc | 0 |
| matchAmbiguous | 0 |
| hotelInventoryRows | 631 |
| verifiedSigningDates | 0 |
| verifiedManagementExpirations | 0 |
| verifiedFranchiseExpirations | 0 |
| verifiedRenewals | 0 |
| verifiedTerminations | 0 |
| amendmentsAndExtensions | 6 |
| verifiedPropertyLevelContractFindings | 0 |
| ownershipAndOperatorRelationships | 0 |
| successfullyPersistedHpcUpdates | 0 |
| successfulReadbacks | 0 |
| conflictsAndQuarantinedFindings | 358 |
| potentialConversionOpportunities | 0 |
| estimatedOrUnsupportedDatesRejected | 346 |
| actualResearchSpendUsd | 0 |
| yieldNextCohortGuidance | Next research focus: ashford-hospitality-trust — PRIORITIZE_EXHIBITS_AND_SCHEDULES |
| discoveryInventoryDestination | data/sec-edgar/discovery-inventory |
| reconciliationNewlyMatchable | 0 |

## Notes

- Stages: discover → verify contracts → HPC match (independent).
- Missing HPC does **not** invalidate discovery or verified contracts.
- Inventory: `data/sec-edgar/discovery-inventory`
- SEC HTTP retrieval is free; actual paid spend = $0.
- Customer-facing publication remains blocked.
