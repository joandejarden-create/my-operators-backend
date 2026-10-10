# Changelog — YOTEL generator visibility repair

## Added

- `isGdiDemandGeneratorVisible()` — separate from `isGdiCustomerOpportunityReady()`
- Hotel demand-campaign store: `data/.../recrPQcZg7SFARRb2/demand-campaigns.json`
- Seed of 10 verified YOTEL campaigns (no new opportunities)
- API `GET /api/group-demand-intelligence/hotels/:hotelId/demand-campaigns`
- Summary includes `demandCampaigns.visibleCount`
- UI Demand campaigns panel + campaign-aware empty state

## Did not change

- Customer-ready thresholds
- Future Watch standards
- ADP
- Share tokens
- No speculative child opportunities created
