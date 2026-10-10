# GDI Customer Publication Data Flow

## Path (current)

1. **Canonical opportunity** — FS `data/group-demand-intelligence/hotels/{hotelId}/opportunities.json` + Airtable upsert via `upsertOpportunity` (`airtable-opportunity-store.js`)
2. **Packet / enrichment** — `enrichGdiOpportunityForCustomer` (`enrich-gdi-opportunity-for-customer.js`) inside `promoteQualifiedGdiOpportunity`
3. **Readiness** — `isGdiCustomerOpportunityReady` (`customer-readiness-gate-v1.js`)
4. **Valid Watch gate** — `isValidFutureWatch` (`future-watch/is-valid-future-watch-v1.js`)
5. **Customer visibility eligibility** — `isCustomerFacingOpportunity` / `filterCustomerFacingOpportunities` (`customer-visibility.js`)
6. **Publication record** — no separate publication table; materialization = `customerVisible` + facing filter. Promote sets `customerVisible:true` for FUTURE_WATCH **only when** `isValidFutureWatch.ok` (repaired 2026-10-08)
7. **API** — `GET /api/group-demand-intelligence/hotels/:hotelId/opportunities` (`api/group-demand-intelligence.js`)
8. **Hotel/property scope** — HPC hotelId (`rec…`); ADP subjects are separate namespaces
9. **Customer GDI page** — `/group-demand-intelligence.html` + `dealality-gdi-ui.js` / `app.js`
10. **Priority / Watch filters** — card pills + Priority/Action Window filters (All/Ready/Watching workflow chips removed)

## Modules by stage

| Stage | Module |
|-------|--------|
| Promote | `promote-qualified-opportunity.js` |
| Persist | `opportunity-persistence.js`, `airtable-opportunity-store.js` |
| Watch gate | `future-watch/is-valid-future-watch-v1.js` |
| Ready gate | `customer-readiness-gate-v1.js` |
| Surface | `customer-surface-revalidation-v1.js` |
| Facing filter | `customer-visibility.js` |
| Count invariant | `customer-publication-invariants-v1.js` |
| API | `api/group-demand-intelligence.js` |
| UI | `public/js/group-demand-intelligence/dealality-gdi-ui.js` |
