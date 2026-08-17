# Operator Explorer Tab Factory Audit

Version: `operator-tab-factory-v1`
Generated: 2026-07-24T15:53:26.782Z
Source: **fixtures** · dryRun: **true**

## Summary

- Operators audited: **1**
- Total failFindings: **76**
- Total emptyRenderFails: **76**
- patchPlanComplete: **true**
- auditPass: **false**

## Operators

- **Playa Hotels & Resorts** (`playa-hotels-resorts`): auditPass=false fails=76 decision=field_complete_after_patch

---

# Tab Factory — Playa Hotels & Resorts

Slug: `playa-hotels-resorts` · Record: `rec3TUHT9Z4AnFp5P`
Source: **fixtures** · Protected baseline: **false**
auditComplete: **true** · patchPlanComplete: **true** · auditPass: **false**
failFindings: **76** · emptyRenderFails: **76**
decision: **field_complete_after_patch**

## Gates

- tab_factory_audit: **false**
- rendered_field_completeness: **false**
- no_empty_rendered_components: **false**
- source_provenance_by_tab: **true**
- section_pattern_parity: **false**
- golden_content_quality: **false**
- operator_specific_source_validation: **true**

## Tabs

- **Profile & Positioning**: pass=9/21 · fail=12 · auditPass=false
- **Operating Platform**: pass=0/11 · fail=11 · auditPass=false
- **Brand & Relationships**: pass=0/7 · fail=7 · auditPass=false
- **Markets & Footprint**: pass=0/9 · fail=9 · auditPass=false
- **Owner Engagement & Reporting**: pass=0/12 · fail=12 · auditPass=false
- **Infrastructure & Data**: pass=0/6 · fail=6 · auditPass=false
- **Leadership**: pass=0/7 · fail=7 · auditPass=false
- **Project Fit & Deal Profile**: pass=0/10 · fail=10 · auditPass=false
- **Proof & Track Record**: pass=3/5 · fail=2 · auditPass=false
- **Operator Materials**: pass=1/1 · fail=0 · auditPass=true

## Fail findings (hard)

- `Profile & Positioning` / `op.snapshot.companyName` — blocked_empty_render: Visible label/value pair with blank value is a hard fail.
  - Patch: Populate Company Name (op.snapshot.companyName) from operator-specific sources
- `Profile & Positioning` / `op.snapshot.yearEstablished` — blocked_empty_render: Visible label/value pair with blank value is a hard fail.
  - Patch: Populate Year Established (op.snapshot.yearEstablished) from operator-specific sources
- `Profile & Positioning` / `op.snapshot.managementPhilosophy` — blocked_empty_render: Visible label/value pair with blank value is a hard fail.
  - Patch: Populate Management Philosophy (op.snapshot.managementPhilosophy) from operator-specific sources
- `Profile & Positioning` / `op.snapshot.parentCompany` — blocked_empty_render: Visible label/value pair with blank value is a hard fail.
  - Patch: Populate Parent Company (op.snapshot.parentCompany) from operator-specific sources
- `Profile & Positioning` / `op.snapshot.primaryServiceModel` — blocked_empty_render: Visible label/value pair with blank value is a hard fail.
  - Patch: Populate Primary Service Model (op.snapshot.primaryServiceModel) from operator-specific sources
- `Profile & Positioning` / `op.snapshot.totalProperties` — blocked_empty_render: Visible label/value pair with blank value is a hard fail.
  - Patch: Populate Total Properties (op.snapshot.totalProperties) from operator-specific sources
- `Profile & Positioning` / `op.snapshot.totalRooms` — blocked_empty_render: Visible label/value pair with blank value is a hard fail.
  - Patch: Populate Total Rooms (op.snapshot.totalRooms) from operator-specific sources
- `Profile & Positioning` / `op.snapshot.website` — blocked_empty_render: Visible label/value pair with blank value is a hard fail.
  - Patch: Populate Website (op.snapshot.website) from operator-specific sources
- `Profile & Positioning` / `op.snapshot.certifications` — blocked_empty_render: Visible label/value pair with blank value is a hard fail.
  - Patch: Populate Certifications (op.snapshot.certifications) from operator-specific sources
- `Profile & Positioning` / `op.snapshot.industryRecognition` — blocked_empty_render: Visible label/value pair with blank value is a hard fail.
  - Patch: Populate Industry Recognition (op.snapshot.industryRecognition) from operator-specific sources
- `Profile & Positioning` / `op.snapshot.notableAchievements` — blocked_empty_render: Visible label/value pair with blank value is a hard fail.
  - Patch: Populate Notable Achievements (op.snapshot.notableAchievements) from operator-specific sources
- `Profile & Positioning` / `op.snapshot.companySize` — blocked_empty_render: Visible label/value pair with blank value is a hard fail.
  - Patch: Populate Company Size / Employees (op.snapshot.companySize) from operator-specific sources
- `Operating Platform` / `op.platform.revenueManagement` — blocked_empty_render: Visible label/value pair with blank value is a hard fail.
  - Patch: Populate Revenue Management Capability (op.platform.revenueManagement) from operator-specific sources
- `Operating Platform` / `op.platform.preOpeningSupport` — blocked_empty_render: Visible label/value pair with blank value is a hard fail.
  - Patch: Populate Pre-Opening Support Capability (op.platform.preOpeningSupport) from operator-specific sources
- `Operating Platform` / `op.platform.conversionExperience` — blocked_empty_render: Visible label/value pair with blank value is a hard fail.
  - Patch: Populate Conversion / Repositioning Experience (op.platform.conversionExperience) from operator-specific sources
- `Operating Platform` / `op.platform.ownerReporting` — blocked_empty_render: Visible label/value pair with blank value is a hard fail.
  - Patch: Populate Owner Reporting Level (op.platform.ownerReporting) from operator-specific sources
- `Operating Platform` / `op.platform.fbResortCapability` — blocked_empty_render: Visible label/value pair with blank value is a hard fail.
  - Patch: Populate F&B / Lifestyle / Resort Capability (op.platform.fbResortCapability) from operator-specific sources
- `Operating Platform` / `op.platform.offeredServices` — blocked_empty_render: Visible label/value pair with blank value is a hard fail.
  - Patch: Populate Offered Services (op.platform.offeredServices) from operator-specific sources
- `Operating Platform` / `op.json.op_commercial_engine_json` — blocked_empty_render: Visible label/value pair with blank value is a hard fail.
  - Patch: Populate Commercial Engine capability tiles (op.json.op_commercial_engine_json) from operator-specific sources
- `Operating Platform` / `op.json.op_owner_reporting_json` — blocked_empty_render: Visible label/value pair with blank value is a hard fail.
  - Patch: Populate Owner Reporting & Communication tiles (op.json.op_owner_reporting_json) from operator-specific sources
- `Operating Platform` / `op.json.op_preopening_transition_json` — blocked_empty_render: Visible label/value pair with blank value is a hard fail.
  - Patch: Populate Pre-Opening & Transition Support tiles (op.json.op_preopening_transition_json) from operator-specific sources
- `Operating Platform` / `op.json.op_conversion_repositioning_json` — blocked_empty_render: Visible label/value pair with blank value is a hard fail.
  - Patch: Populate Conversion & Repositioning tiles (op.json.op_conversion_repositioning_json) from operator-specific sources
- `Operating Platform` / `op.json.op_fb_lifestyle_resort_json` — blocked_empty_render: Visible label/value pair with blank value is a hard fail.
  - Patch: Populate F&B, Lifestyle & Resort capability tiles (op.json.op_fb_lifestyle_resort_json) from operator-specific sources
- `Brand & Relationships` / `op.brand.familiesOperated` — blocked_empty_render: Visible label/value pair with blank value is a hard fail.
  - Patch: Populate Brand Families Operated (op.brand.familiesOperated) from operator-specific sources
- `Brand & Relationships` / `op.brand.brandedVsIndependentMix` — blocked_empty_render: Visible label/value pair with blank value is a hard fail.
  - Patch: Populate Branded vs Independent Mix (op.brand.brandedVsIndependentMix) from operator-specific sources
- `Brand & Relationships` / `op.brand.softIndependentNarrative` — blocked_empty_render: Visible label/value pair with blank value is a hard fail.
  - Patch: Populate Soft Brand / Independent Narrative (op.brand.softIndependentNarrative) from operator-specific sources
- `Brand & Relationships` / `op.json.brand_portfolio_mix_json` — blocked_empty_render: Visible label/value pair with blank value is a hard fail.
  - Patch: Populate Portfolio mix by brand / flag type (op.json.brand_portfolio_mix_json) from operator-specific sources
- `Brand & Relationships` / `op.json.brand_relationship_depth_json` — blocked_empty_render: Visible label/value pair with blank value is a hard fail.
  - Patch: Populate Brands & relationship depth (op.json.brand_relationship_depth_json) from operator-specific sources
- `Brand & Relationships` / `op.json.brand_execution_capabilities_json` — blocked_empty_render: Visible label/value pair with blank value is a hard fail.
  - Patch: Populate Brand execution capabilities (op.json.brand_execution_capabilities_json) from operator-specific sources
- `Brand & Relationships` / `op.json.brand_governance_compliance_json` — blocked_empty_render: Visible label/value pair with blank value is a hard fail.
  - Patch: Populate Brand governance & compliance support (op.json.brand_governance_compliance_json) from operator-specific sources
- `Markets & Footprint` / `op.markets.activeCountries` — blocked_empty_render: Visible label/value pair with blank value is a hard fail.
  - Patch: Populate Active Countries (op.markets.activeCountries) from operator-specific sources
- `Markets & Footprint` / `op.markets.activeMarkets` — blocked_empty_render: Visible label/value pair with blank value is a hard fail.
  - Patch: Populate Active Markets (op.markets.activeMarkets) from operator-specific sources
- `Markets & Footprint` / `op.markets.geographicPriorities` — blocked_empty_render: Visible label/value pair with blank value is a hard fail.
  - Patch: Populate Priority / Target Markets (op.markets.geographicPriorities) from operator-specific sources
- `Markets & Footprint` / `op.markets.targetGrowthMarkets` — blocked_empty_render: Visible label/value pair with blank value is a hard fail.
  - Patch: Populate Target Growth Markets (op.markets.targetGrowthMarkets) from operator-specific sources
- `Markets & Footprint` / `op.markets.teamExperienceMarkets` — blocked_empty_render: Visible label/value pair with blank value is a hard fail.
  - Patch: Populate Team Experience Markets (op.markets.teamExperienceMarkets) from operator-specific sources
- `Markets & Footprint` / `op.markets.regionsSupported` — blocked_empty_render: Visible label/value pair with blank value is a hard fail.
  - Patch: Populate Regions Supported (op.markets.regionsSupported) from operator-specific sources
- `Markets & Footprint` / `op.markets.regionalPortfolio` — blocked_empty_render: Visible label/value pair with blank value is a hard fail.
  - Patch: Populate Regional Portfolio (structured) (op.markets.regionalPortfolio) from operator-specific sources
- `Markets & Footprint` / `op.json.mkt_regional_expertise_json` — blocked_empty_render: Visible label/value pair with blank value is a hard fail.
  - Patch: Populate Local / regional expertise (op.json.mkt_regional_expertise_json) from operator-specific sources
- `Markets & Footprint` / `op.json.mkt_market_fit_signals_json` — blocked_empty_render: Visible label/value pair with blank value is a hard fail.
  - Patch: Populate Market fit signals (op.json.mkt_market_fit_signals_json) from operator-specific sources
- `Owner Engagement & Reporting` / `op.engagement.ownerReportingLevel` — blocked_empty_render: Visible label/value pair with blank value is a hard fail.
  - Patch: Populate Owner Reporting Level (op.engagement.ownerReportingLevel) from operator-specific sources
- … +36 more

## Fixture files

- `fixtures/operator-best-fit-playa-hotels-resorts.json`
- `fixtures/operator-brand-explorer-playa-hotels-resorts.json`
- `fixtures/operator-engagement-explorer-playa-hotels-resorts.json`
- `fixtures/operator-infrastructure-explorer-playa-hotels-resorts.json`
- `fixtures/operator-leadership-explorer-playa-hotels-resorts.json`
- `fixtures/operator-markets-explorer-playa-hotels-resorts.json`
- `fixtures/operator-operating-explorer-playa-hotels-resorts.json`
- `fixtures/operator-profile-explorer-playa-hotels-resorts.json`
- `fixtures/operator-recognition-explorer-playa-hotels-resorts.json`
