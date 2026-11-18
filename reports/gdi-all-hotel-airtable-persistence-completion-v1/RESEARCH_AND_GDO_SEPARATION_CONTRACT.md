# Research and GDO separation contract

## Rules

1. **Research inventory** persists first (or independently) of commercial promotion.
2. **Qualified GDO** requires Confirmed/Reliable demand thesis **and** credible commercial entry point (existing Full E2E eligibility).
3. **Customer publication** remains independently founder-gated; persistence ≠ publication.
4. Research-only opportunities must survive future runs and become GDO-eligible later without losing earlier source research.
5. Idempotent identity: same `opportunityId` never forks into a new opportunity on rerun.
6. YOTEL legacy 108 GDO IDs must never be recreated (`108` IDs loaded from delete manifest).
7. Hotel-private pursuits/feedback stay hotel-scoped; never copy into global Demand Generators.

## Classification API

`classifyOpportunityLifecycleLayer(opp, hotel)` → `RESEARCH_INVENTORY | QUALIFIED_GDO | CUSTOMER_PUBLICATION`

## Status vocabulary (do not collapse)

| Status family | Examples |
|---------------|----------|
| Research lifecycle | exploratory, watchlist, continuation, research-only |
| Commercial qualification | RESEARCH_ONLY, GLOBAL_ORG_ONLY, CUSTOMER_ELIGIBLE_CANDIDATE |
| Customer publication | STAGED, CURRENT, unpublished |
