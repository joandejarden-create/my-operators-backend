# Changelog — GDI P0.5 WHO + Packet→Ready

## Fixed
- `buildOpportunity()` now preserves buyer/WHO/parent-link fields (root cause of buyer=0 / who_not_attempted)
- `classifyWhoHowPath` accepts `publicContactPath` as ORG_PATH
- `isCustomerSurfaceActiveEligible` uses classify (not stored visibility) for readiness gate
- Re-applied WHO + buyer on 13 YOTEL complete packets; 12 newly customer-ready
- FS mirror rewritten from Airtable canonical (95 opportunities)

## Not changed
- Readiness thresholds / six-pillar packet criteria
- Broad discovery universe
- ADP / share tokens
- Surfe / named-person invention
