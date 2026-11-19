# GDI Discovery Intake Disposition V1 — Offline Validation

**Status:** ACCEPTANCE PASS  
**Paid USD:** 0  
**Phase 2B excluded:** gdi_lmc_00562bf81b1e1957, gdi_opp_2027_ddaa_annual_conference_8

> Offline classifier experiment only. Do not claim improved live opportunity discovery from these results alone.

## Discovery provenance

- Yield probes passed: 5/5
- Zero historical/fixture labeled `LIVE_NEW`: true
- Zero inventory reopen claiming genuine new discovery: true

## Commercial disposition (frozen cohort)

- Scored rows: 13
- Accuracy: 100.0% (target ≥80%)
- Competitor-placement precision: 100.0%
- Competitor-placement recall: 100.0%
- False positives: none
- False negatives: none

## Duplicates / shared intelligence

- Shared demand keys indexed: 125
- Cross-hotel reusable keys: 0
- Within-hotel program-key collisions (informational): 2
- New canonical opportunities introduced by harness: 0

## Drawer readiness

- Audited: 13
- Fully ready: 6
- Top gaps: certified:missing_core_commercial_section:potentialHotelBusiness (5); credible_named_buyer_or_entry_point (4); whyActNow (2); certified:missing_core_commercial_section:whyActNow (2)

## Qualification / publication gates

- Speculative promoted to SALES_READY: 0
- Competitor-placed promoted to SALES_READY after intake gate: 0

## Remaining blockers to genuine live discovery

- Paid Exa / live providers require separate founder authorization
- Most segment executors remain fixture/historical/blocked without authorizePaidDiscovery
- Offline classifier does not create new LIVE_NEW organizations or opportunities
- Demand-mechanism mission packs (slice 2+) not yet implemented

## Recommended next slice

- **gdi-discovery-mission-packs-scaling-projects-v1** — Catchment-local mission packs for corporate_accounts_scaling + projects_infrastructure with $0 query diffs; optional bounded Exa only after founder budget auth
