# GDI List View — Optional Column Readiness Assessment

Date: 2026-11-27  
Server: http://localhost:8080 (ws-gdi cumulative)  
Cache: gdi-list-columns-hydration-v1  
Contract: DEALALITY_ADDITIVE_NON_REGRESSION_V1 — List presentation/mapping only

## Cross-hotel decisions

| Column | Canonical data source | Mapping corrections | Decision |
|---|---|---|---|
| Sales Opportunity Type | opportunityType* / salesOpportunityType* | Redundant with Demand Type | **CONSOLIDATED** — removed from Columns; Sales Opportunities filters kept |
| Market Scope | inventoryPopulations / hotelMarketScopeEligible / competitiveMarketScopeEligible / marketScope | Was empty because renderer ignored filter taxonomy | **RETAIN_HYDRATED** when Competitive or scope varies; else **HIDDEN_FILTER_SUFFICES** |
| Contact Readiness | contactActionabilityTier / Label | None | **RETAIN_HYDRATED** |
| Opportunity Status | hotelSourcingStatusLabel → commercialStagePillLabel | Was reading missing status fields | **RETAIN_HYDRATED** |
| Need Period Alignment | needDateIndicator.displayLabel / tileNeedDatePreview / needDateMatchV1.timingOverlapLabel | Was ignoring structured indicator objects | **RETAIN_HYDRATED** when present |
| Estimated Room Nights | estimatedRoomNights / peakRooms / roomNights | No values in audited STAGED packages | **HIDDEN_NO_DATA** |
| Weekly Change | weeklyDeltaStateOf / isNewThisWeek | No weekly delta on audited STAGED packages | **HIDDEN_NO_DATA** |
| Last Verified | lastVerifiedAt / evidenceAsOf | No per-opportunity verification timestamps | **HIDDEN_NO_DATA** |

## Wyndham San Diego Bayside (`recnPB3Q02NO8Ma1o`) — 26 opportunities

| Column | Canonical source | Populated | Coverage % | Decision |
|---|---|---:|---:|---|
| Sales Opportunity Type | duplicates Demand Type | — | — | CONSOLIDATED |
| Contact Readiness | contactActionabilityTier / contactActionabilityLabel | 26/26 | 100 | RETAIN_HYDRATED |
| Estimated Room Nights | estimatedRoomNights / peakRooms / roomNights / estimatedPeakRooms | 0/26 | 0 | HIDDEN_NO_DATA |
| Weekly Change | weeklyDeltaState / isNewThisWeek (weeklyDeltaStateOf) | 0/26 | 0 | HIDDEN_NO_DATA |
| Market Scope | inventoryPopulations / hotelMarketScopeEligible / competitiveMarketScopeEligible / marketScope (Market Scope filter taxonomy) | 26/26 | 100 | HIDDEN_FILTER_SUFFICES |
| Need Period Alignment | needDateIndicator.displayLabel / tileNeedDatePreview.displayLabel / needDateMatchV1.timingOverlapLabel | 0/26 | 0 | HIDDEN_NO_DATA |
| Opportunity Status | hotelSourcingStatusLabel / commercialStagePillLabel / statusLabel | 11/26 | 42.3 | RETAIN_HYDRATED |
| Last Verified | lastVerifiedAt / lastVerified / evidenceAsOf | 0/26 | 0 | HIDDEN_NO_DATA |

**Columns menu offers:** Contact Readiness, Opportunity Status

## Bethesda Marriott (`recLuxvwwxID7U2B8`) — 68 opportunities

| Column | Canonical source | Populated | Coverage % | Decision |
|---|---|---:|---:|---|
| Sales Opportunity Type | duplicates Demand Type | — | — | CONSOLIDATED |
| Contact Readiness | contactActionabilityTier / contactActionabilityLabel | 68/68 | 100 | RETAIN_HYDRATED |
| Estimated Room Nights | estimatedRoomNights / peakRooms / roomNights / estimatedPeakRooms | 0/68 | 0 | HIDDEN_NO_DATA |
| Weekly Change | weeklyDeltaState / isNewThisWeek (weeklyDeltaStateOf) | 0/68 | 0 | HIDDEN_NO_DATA |
| Market Scope | inventoryPopulations / hotelMarketScopeEligible / competitiveMarketScopeEligible / marketScope (Market Scope filter taxonomy) | 68/68 | 100 | RETAIN_HYDRATED |
| Need Period Alignment | needDateIndicator.displayLabel / tileNeedDatePreview.displayLabel / needDateMatchV1.timingOverlapLabel | 66/68 | 97.1 | RETAIN_HYDRATED |
| Opportunity Status | hotelSourcingStatusLabel / commercialStagePillLabel / statusLabel | 68/68 | 100 | RETAIN_HYDRATED |
| Last Verified | lastVerifiedAt / lastVerified / evidenceAsOf | 0/68 | 0 | HIDDEN_NO_DATA |

**Columns menu offers:** Contact Readiness, Market Scope, Need Period Alignment, Opportunity Status

## Validation

- `npm run test:gdi-list-optional-columns-hydration-v1`
- `npm run test:gdi-list-view-contact-actions-v1`
- `npm run test:gdi-sortable-commercial-list-view-v1`
- Browser List View Columns menu on Wyndham + Bethesda (:8080)

No paid research. No enrichment. No fabricated values.
