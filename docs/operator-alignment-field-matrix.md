# Operator Alignment Snapshot — Field Matrix

**Date:** 2026-05-25  
**Repo:** `deal-capture-proxy` (Dealality)  
**Related:** [operator-alignment-snapshot-audit.md](./operator-alignment-snapshot-audit.md), [operator-alignment-snapshot-implementation-checklist.md](./operator-alignment-snapshot-implementation-checklist.md)  
**Scope:** Documentation and planning only — no code or Airtable changes

**Product name:** Operator Alignment Snapshot (OAS)  
**Scoring reference:** `scoreOperatorMatchForDeal` in `api/my-deals.js` (lines ~2682–3038)  
**Document pattern reference:** `api/brand-alignment-snapshot.js`, `public/js/brand-alignment-snapshot.js`

---

## 1. Executive Summary

| Surface | Enough for MVP? | Summary |
|---------|-----------------|---------|
| **Profile-level OAS** | **Partial — yes with mapping layer** | Deal has `Preferred Third-Party Operator Profile`, operating models, strategy status, and geography. Taxonomy does not match product profile categories; use `fixtures/operator-profile-archetypes.json` (proposed) to map existing selects + deal signals to five display categories. No numeric score required for MVP. |
| **Specific-operator alignment** | **Partial — yes for published operators** | Engine + `buildPrefillObjectFromNewBaseRows` cover geography, scale, project/stage, structure, services, reporting, brands, negative-fit. Fee/F&B/owner-involvement factors are weak. ~12 Operator Setup masters in base; completeness varies. |
| **Operator Explorer population** | **Partial** | List API (`buildNewBaseListRow`) is live. Detail relies on flattened `cap_*` / `bf_*` / `mkt_*` narratives + case studies; mock fallback remains. Alignment badges and completeness % are **not** populated today. |
| **Operator Setup completion** | **Partial** | New-base build sheet has 84 structured fields; P0 alignment fields exist but geography is footprint-derived, not country multi-select. Legacy `3rd Party Operator - *` columns still in write/read paths. |

**Bottom line:** MVP can ship **profile-level OAS + deal context + qualitative alignment signals** without schema changes. **Specific-operator numeric alignment** can reuse `scoreOperatorMatchForDeal` with documented fallbacks. **Explorer and Setup** need Phase 6 normalization for reliable cross-deal comparison.

---

## 2. Alignment Factor Matrix

Proposed OAS weights sum to **100** (explainable document model). Existing engine weights (parentheses) sum to **90** scored factors + **2** penalty; rebalancing is a **Modify** in Phase 4+.

| Alignment Factor | Weight / Importance | Deal Field(s) | Operator/Profile Field(s) | Existing Source | Existing Field Name(s) | Field Quality | MVP Handling | Future Field Recommendation | Notes / Risks |
|------------------|---------------------|---------------|---------------------------|-----------------|------------------------|---------------|--------------|----------------------------|---------------|
| **Geographic / market alignment** | **15** (engine: 18) | `Country`, `Primary Market Region`, `City` (Location); `Project Type` (context) | Prefill: `specificMarkets`, `regionsSupported`, `regions`; Platform: `mkt_signal_gateway`, `mkt_narrative_depth`; Footprint: `geo_cala_total_hotels`, `geo_*_total_hotels`; Legacy: `Regions Supported`, `Specific Markets` | Deals + Location; Operator Setup Platform & Markets; legacy Basics/Footprint | See left | **Partial** | Substring match country ∈ markets list; if operator markets empty → factor score **35**; if deal country empty → **60** or skip factor | `active_countries` (multi), `active_markets` (multi), `mexico_presence`, `cala_presence` (bool) | Engine uses naive `includes`; CALA inferred from footprint totals only |
| **Project type alignment** | **12** (engine: 14 asset/stage) | `Project Type`, `Building Type`, `Stage of Development` | `bf_selected_asset_types`, `bf_q_assets`; prefill `bestFitAssetTypes`, `propertyTypes`; case study `hotel_type`, `situation` | Deals; Location; Commercial; Case Studies; legacy Ideal Projects | `Project Type`, `Building Type`, `bf_selected_asset_types`, `Operating Situations` (stage) | **Partial** | `overlapScore` on project + building vs asset types; stage via `operatingSituations` / `bf_selected_situation_types` | `experience_tags` multi (new-build, conversion, urban, resort) | `assetProjectStageFit` blends 70% project / 30% stage |
| **Service model alignment** | **8** (not isolated in engine) | `Hotel Service Model`, `Preferred Future Operating Model`, `Plan to Self-Manage or Hire Third Party?` | `primaryServiceModel` (Profile); `bf_selected_deal_structures`; prefill `serviceModels` | Location; SI; Profile; Commercial | `Hotel Service Model`, `Primary Service Model`, `bf_selected_deal_structures` | **Partial** | Map deal service model to operator primary service model token match; else qualitative signal only in profile layer | `service_models_supported` multi | Often conflated with deal structure in engine |
| **Chain scale alignment** | **8** (engine: 8) | `Hotel Chain Scale` (Location) | `chainScalesSupported`, `chainScale` (Profile/Platform); legacy `Chain Scales You Support` | Location; Profile; Platform | `Hotel Chain Scale`, `chainScalesSupported` | **Strong** | Exact match → 100; partial substring → 65; else 25; empty operator → 45 | Normalize chain scale option sets deal ↔ operator | Same option vocabulary as brand match |
| **Conversion / reflag alignment** | **8** (engine: partial via project) | `Project Type`, `Open to Soft Brand First Then Reflag?`, `Operator Capability Priorities` (conversion item) | `brand_signal_reflag`, `brand_signal_audit`, `brand_signal_franchise_align`; case study `situation` | Deals; SI; Profile | `brand_signal_*`, conversion-type `Project Type` | **Partial** | Only score when `resolveProjectTypeKind` = conversion/reflag; else section omitted | `conversion_reflag_experience` level (select) | Brand signals are operator-side only |
| **Repositioning alignment** | **6** (engine: partial via project/stage) | `Project Type`, `CapEx / PIP Execution Importance`, `Stage of Development` | `cap_kpi_transition`, `bf_signal_transition`, `bf_selected_situation_types` | Deals; SI; Platform; Commercial | `cap_kpi_transition`, repositioning `Project Type` kinds | **Weak** | Qualitative alignment signal when project kind = renovation/repositioning; no dedicated factor in engine | `repositioning_experience` multi | No deal “repositioning complexity” field |
| **Brand compliance support** | **6** (engine: 6 brand portfolio) | `Preferred Brands`, `Must-Haves From Brand/Operator`, brand status on deal | `brands` (link), `brand_narrative_compliance`, `brand_signal_soft_retention` | SI; Profile | `Preferred Brands`, `brands`, `brand_narrative_compliance` | **Partial** | Reuse `brandPortfolioRelevance` overlap; compliance narrative as review consideration not score | `brand_families_operated`, `soft_brand_experience` | Overlaps brand match — keep operator-framed copy |
| **Commercial platform alignment** | **10** (engine: 10 fee + 12 structure) | `Preferred Deal Structure` (MP); `Royalty Fee Expectations`, `Marketing Fee Expectations`, `Loyalty Fee Expectations` | `bf_selected_deal_structures`, `bf_signal_capital`, `bf_signal_dealsize`; narratives `ov_card_commercial`, `cap_profile_commercial`, `feeStructureSummary` | MP; Commercial; legacy Deal Terms | MP fee columns; `bf_*`; fee narratives | **Weak** | Structure: exact/partial match (existing). Fees: placeholder **55/75** if both sides present — show as **data gap** in OAS | `typical_management_fee_structure`, `incentive_fee_flexibility` (structured) | Do not imply fee adequacy in copy |
| **Revenue management support** | **5** (engine: 8 services partial) | `Services Required From Operator`, `Operator Capability Priorities`, `Revenue / Yield Management Importance` | Granular: `revenueManagementServices` + checkboxes; `cap_deep_revenue_systems`, `cap_kpi_reporting` | SI; Governance granular; Platform | `Revenue Management` in services/priorities | **Partial** | `overlapScore` must-haves vs `revenueManagementServices`; if deal priority high and operator empty → review consideration | `revenue_management_capability` level | Granular columns many — aggregate for owner UI |
| **Sales and distribution support** | **5** (engine: 8 services partial) | `Services Required From Operator`, `Marketing & Distribution Importance` | `salesMarketingSupport` granular; `cap_deep_revenue_systems` | SI; Governance | Same pattern as RM | **Partial** | Token overlap on must-haves / services | `sales_distribution_platform` level | — |
| **Owner reporting / governance** | **8** (engine: 6 systems) | `Owner Reporting Frequency`, `Owner Reporting Package`, `Preferred Reporting Frequency` | `infra_kpi_reporting`, `infra_asset_management_reporting`, `ownerReportingCadence`, `reportTypesProvided`, `technologySystems` | SI; Governance; legacy Performance | Reporting fields above | **Partial** | Engine scores operator systems presence (**40–90**), not true cadence match; OAS should compare frequency tokens | `owner_reporting_level` select | Deal has two reporting frequency fields — alias risk |
| **Pre-opening / reopening support** | **8** (engine: stage partial) | `Opening / Transition Phase`, `Stage of Development`, `Operator Capability Priorities` (pre-opening) | `cap_kpi_transition`, `operatingSituations`, `bf_selected_situation_types` | Deals; SI; Platform; Commercial | `Opening / Transition Phase`, `cap_kpi_transition` | **Partial** | Overlap on stage/situation; opening phase as review consideration | `new_build_opening_experience`, `pre_opening_support_level` | OCS P0 `Opening / Transition Phase` shared |
| **F&B complexity alignment** | **4** (not in engine) | `F&B Outlets?`, meeting/F&B amenity fields (Deals) | F&B granular services; `cap_card_service_diff`; case study services | Deals § amenities; Governance services | `F&B Outlets?`, culinary ops in priorities | **Weak** | If `F&B Outlets?` ≠ None and operator lacks F&B in services → conditional signal; no numeric weight in MVP profile layer | `fb_capability_level` | Deal F&B is coarse |
| **Owner involvement preference** | **7** (engine: 6 owner relations) | `Level of Involvement in Day-to-Day Ops`, `Owner Control Priorities`, `Preferred Future Operating Model` | `ov_cluster_interaction`, `ownerEngagementNarrative`, `operatingCollaborationMode`, `ownerCommunicationStyle` | SI; Commercial | Involvement + owner value narratives | **Weak** | Engine uses hardcoded deal line “responsive communication”; keyword match on operator text | `owner_collaboration_model` select on both sides | Not a symmetric field pair |
| **Management economics compatibility** | **10** (engine: 12 structure + 10 fee) | `Preferred Deal Structure`, fee expectations, `Contract Flexibility Priorities` | `bf_selected_deal_structures`, `bf_signal_dealsize`, `ov_card_flexibility`, legacy Deal Terms | MP; Commercial; legacy Deal Terms | Structure + fee fields | **Partial** | Structure scored; economics narrative in review considerations only | `termination_flexibility`, `management_agreement_flexibility_importance` (deal) | “Compatibility” not “fit recommendation” |
| **Centralized services sensitivity** | **4** (engine: 8 services) | `Services Required From Operator`, `Operator Capability Priorities` | `OPERATOR_SERVICE_GRANULAR` checkboxes + aggregates (`primaryServices`, `additionalServices`) | SI; Governance; `lib/operator-setup-service-granular-fields.js` | Service multis + granular | **Partial** | overlapScore must-haves vs collected service arrays | `centralized_services_offered` multi | Renaming granular columns breaks writer |
| **Data completeness / confidence** | **8** (not in engine) | OCS P0: `Current Operating Model`, `Preferred Future Operating Model`, `Operator Strategy Status`, `Country`, `Project Type`, etc. | Master `submission_status`; Profile `readyForInvestorPublication`; % fill of `bf_*`, `regionsSupported`, `brands`, services | `lib/operator-capability-inputs.js`; Operator Setup tables | P0 deal + operator keys list | **Partial** | Compute fill % on required keys; label **Insufficient Data** if &lt;40% factors scorable; show **Data gap** list | `operator_profile_completeness`, `data_confidence_level`, `last_updated_at` | Down-rank confidence, not “low quality operator” |

---

## 3. Deal Intake Field Matrix

| Deal Field Needed | Current Field Found | Current Table / File Reference | Used in Existing Code? | Required for MVP? | Required for Future? | Recommended Action | Risk if Changed |
|-------------------|---------------------|--------------------------------|------------------------|-------------------|----------------------|--------------------|-----------------|
| Project type | `Project Type` | Deals — `api/schemas/deal-setup-fields.js`, `lib/project-type.js` | Yes — scoring, OCS, BAS, readiness | **Yes** | Yes | Keep; use `resolveProjectTypeKind` in OAS | Breaks match, OCS, sample deals |
| Development stage | `Stage of Development` | Deals / Location | Yes — `scoreOperatorMatchForDeal` | **Yes** | Yes | Keep | Scoring null factors |
| Country | `Country` | Location & Property | Yes — scoring, OCS P0 | **Yes** | Yes | Keep | Geography factor breaks |
| Primary market region | `Primary Market Region` | Location | Yes — OCS P0 | **Yes** | Yes | Keep | OCS scope + narrative |
| City / market | `City` (form: `City & State`) | Location | Yes — BAS, brief | Partial | Yes | Keep | Display only |
| Chain scale | `Hotel Chain Scale` | Location | Yes — scoring, brand match | **Yes** | Yes | Keep | Chain factor |
| Building type | `Building Type` | Location | Yes — scoring | **Yes** | Yes | Keep | Asset factor |
| Hotel service model | `Hotel Service Model` | Location | Yes — intake, readiness | Partial | Yes | Keep | Service model factor |
| Room count | `Total Number of Rooms` / keys fields | Location / Deals | Yes — brand pre-filters | Partial | Yes | Keep | Future min-keys compare |
| Current operating model | `Current Operating Model` | Deals | Yes — OCS, intake | **Yes** | Yes | Keep | OCS + OAS context |
| Preferred future operating model | `Preferred Future Operating Model` | Strategic Intent | Yes — OCS, intake | **Yes** | Yes | Keep | Profile taxonomy mapping |
| Operator strategy status | `Operator Strategy Status` | Strategic Intent | Yes — OCS, visibility | **Yes** | Yes | Keep | Gating OAS sections |
| Operator capability priorities | `Operator Capability Priorities` | Strategic Intent (multi) | Yes — OCS build | **Yes** | Yes | Keep | Capability + service signals |
| Preferred operator profile type | `Preferred Third-Party Operator Profile` | Strategic Intent (multi) | Yes — intake, readiness; **not** in `scoreOperatorMatchForDeal` | **Yes** | Yes | **Map** to archetypes (§6); do not rename without migration | Form options, SI_MULTI_SELECT |
| Named preferred operators | `Preferred Third-Party Operators (names)` / `(Names)` | Strategic Intent | Yes — intake; alias in `SI_FORM_TO_AIRTABLE` | Partial | Yes | Keep alias pair | PATCH/GET mismatch |
| Services required | `Services Required From Operator` | Strategic Intent (multi) | Yes — scoring overlap | **Yes** | Yes | Keep | Service factor |
| Must-haves | `Must-Haves From Brand/Operator` | Strategic Intent | Yes — scoring | **Yes** | Yes | Keep | Service factor |
| Deal breakers | `Top 3 Deal Breakers` | Strategic Intent | Yes — penalty factor | Partial | Yes | Keep | Negative-fit |
| Preferred brands | `Preferred Brands` | Strategic Intent | Yes — scoring | Partial | Yes | Keep | Portfolio factor |
| Preferred deal structure | `Preferred Deal Structure` | Market Performance | Yes — scoring | **Yes** | Yes | Keep | Structure factor |
| Fee expectations | `Royalty Fee Expectations`, `Marketing Fee Expectations`, `Loyalty Fee Expectations` | MP — env aliases in `deal-setup-fields.js` | Yes — fee factor (weak) | Partial | Yes | Keep columns; improve scoring later | Env column rename |
| Owner reporting | `Owner Reporting Frequency`, `Owner Reporting Package` | Strategic Intent | Yes — OCS, scoring | Partial | Yes | Keep | Reporting factor |
| Preferred reporting frequency | `Preferred Reporting Frequency` | Strategic Intent | Yes — scoring (fallback) | Partial | Yes | Consolidate narrative in OAS | Duplicate semantics |
| Owner involvement | `Level of Involvement in Day-to-Day Ops` | Strategic Intent | Yes — readiness | Partial | Yes | Use in owner-involvement factor | Weak pairing |
| Bid audience | `Who Should Receive Bids for This Project?` | Strategic Intent | Yes — OCS scope `isOperatorInScopeFromFields` | **Yes** | Yes | Keep | OCS/OAS gating |
| Self-manage vs third party | `Plan to Self-Manage or Hire Third Party?` | Strategic Intent | Yes — OCS legacy | Partial | Yes | Keep | Scope |
| F&B complexity | `F&B Outlets?` | Deals (amenities) | Partial — not in operator score | Partial | Yes | Use qualitative signal | Low risk |
| Repositioning / CapEx priority | `CapEx / PIP Execution Importance` | Strategic Intent | Readiness only | No | Yes | Add to repositioning factor | Low |
| RM / marketing importance | `Revenue / Yield Management Importance`, `Marketing & Distribution Importance` | Strategic Intent | Readiness / context | Partial | Yes | Use as deal signal weight | Low |
| Institutional reporting | *(none dedicated)* | — | No | No | **Yes** | **Add** `Institutional Reporting Requirement` (SI) | New field — forms + readiness |
| Owner in-house ops team | *(none dedicated)* | — | No | No | **Yes** | **Add** boolean SI field | New |
| Retain operating control | *(partial)* `Owner Control Priorities` | Strategic Intent multi | Readiness | Partial | Yes | Normalize options or add dedicated field | Multi-select ambiguity |
| Repositioning complexity | *(none)* | — | No | No | Yes | **Add** select | New |
| Management agreement flexibility | `Contract Flexibility Priorities` | Strategic Intent | Readiness | No | Yes | Map to review considerations | — |
| Operator review interest | *(partial)* `Operator Strategy Status` | SI | OCS | **Yes** (gating) | Yes | Keep; map to workflow actions | — |

---

## 4. Operator Setup Field Matrix

| Operator Field Needed | Current Field Found | Current Table / File Reference | Used in Existing Code? | Required for MVP? | Required for Future? | Audience | Recommended Action | Risk if Changed |
|----------------------|---------------------|--------------------------------|------------------------|-------------------|----------------------|----------|--------------------|-----------------|
| Company name | `company_name` | Operator Setup - Master | Yes — list, scoring name | **Yes** | Yes | Owner | Keep | List/detail labels |
| Submission / publish | `submission_status` | Master | Yes — `activeOnly=1` filter | **Yes** | Yes | Internal + filter | Keep | Explorer empty if changed |
| Investor publish flag | `readyForInvestorPublication` | Profile & Positioning | Yes — Explorer gating | Partial | Yes | Operator + admin | Keep | Owner visibility |
| HQ | `headquarters`, `website` | Profile | Yes — list row | Partial | Yes | Owner | Keep | — |
| Active countries | *(derived)* footprint `geo_*_total_hotels` | Platform / legacy Basics | Yes — `regionCodesFromFootprintTotals` | Partial | **Yes** | Owner | **Add** `active_countries` multi; keep footprint | Explorer tiles |
| Active markets / cities | `specificMarkets`, `mkt_signal_gateway`, `regions` | Platform; legacy `Specific Markets` | Yes — prefill `specificMarkets` | Partial | **Yes** | Owner | Normalize text → multi | Scoring substring |
| CALA / Mexico presence | `geo_cala_total_hotels`, CALA footprint keys | Platform / Basics prefill | Yes — footprint | Partial | Yes | Owner | **Add** explicit booleans | Inferred only today |
| Chain scales supported | `chainScalesSupported`, `chainScale` | Profile / Platform | Yes — scoring | **Yes** | Yes | Owner | Keep | Chain factor |
| Service models | `primaryServiceModel`, `bf_selected_deal_structures` | Profile / Commercial | Yes — scoring tokens | **Yes** | Yes | Owner | Keep | Structure factor |
| Brand families operated | `brands` (link → Brand Basics) | Profile | Yes — scoring `brandsManaged` | **Yes** | Yes | Owner | Keep link | Brand id resolution |
| Soft brand / conversion signals | `brand_signal_reflag`, `brand_signal_audit`, `brand_signal_franchise_align`, `brand_signal_soft_retention` | Profile | Yes — Explorer behavior | Partial | Yes | Owner | Keep | — |
| Asset / project fit | `bf_selected_asset_types`, `bf_selected_situation_types`, `bf_q_*` | Commercial | Yes — scoring + Explorer | **Yes** | Yes | Owner | Keep | Ideal table legacy mirror |
| Deal structures | `bf_selected_deal_structures` | Commercial | Yes — scoring | **Yes** | Yes | Owner | Keep | — |
| Less ideal / red flags | `bf_not_ideal_for`, legacy `Less Ideal Situations` | Commercial / Ideal | Yes — penalty | Partial | Yes | Owner | Keep | Penalty factor |
| Services granular | `revenueManagementServices`, etc. (`operator-setup-service-granular-fields.js`) | Governance | Yes — prefill + scoring | **Yes** | Yes | Owner (summary) | Expose aggregates only on Explorer | Many columns |
| Reporting / systems | `infra_kpi_reporting`, `infra_asset_management_reporting`, `systems_inventory_lines` | Governance | Yes — scoring | Partial | Yes | Owner | Keep | — |
| Commercial narratives | `ov_card_*`, `cap_profile_commercial` | Commercial / Platform | Yes — fee factor text | Partial | Yes | Owner | Keep | Free-text scoring weak |
| Leadership | child: `name`, `title`, `role`, `summary`, `bio`, `headshot` | Leadership Team Members | Yes — detail | Partial | Yes | Owner | `displayLeadershipOnExplorer` | — |
| Case studies | child: `property_name`, `hotel_type`, `region`, `situation`, `services`, `outcome` | Case Studies | Yes — Explorer inference | Partial | Yes | Owner | Keep | Asset/situation inference |
| Diligence Q&A | child: `category`, `question`, `answer` | Diligence QA | Yes — detail | Partial | Yes | Owner | Keep | — |
| Min keys | *(none in build sheet)* | — | No | No | Yes | Owner | **Add** number | — |
| Institutional / family owner exp | *(narrative only)* | Commercial `ov_*` | Partial | No | Yes | Owner | **Add** multis | — |
| Fee / termination flexibility | *(narrative)* | Commercial / legacy Deal Terms | Partial | No | Yes | Internal + owner summary | Structure select fields | — |
| Explorer JSON blob | `Explorer Profile JSON` | legacy Basics | Yes — `applyExplorerProfileJsonPrefill` | Partial | No | Internal | Deprecate after normalization | Large JSON dependency |
| Profile completeness | *(none)* | — | No | Partial | **Yes** | Internal | **Add** formula field | — |
| Data confidence | *(none)* | — | No | No | Yes | Internal | **Add** | — |
| Operator profile categories (operator-side) | *(none)* | — | No | No | **Yes** | Owner | **Add** `operator_profile_categories` multi aligned to §6 | Taxonomy |

---

## 5. Operator Explorer Field Matrix

| Explorer Section | Data Needed | Existing Field(s) | Current Quality | Owner-Facing Copy Use | Missing Data | Recommended Update |
|------------------|-------------|-------------------|-----------------|----------------------|--------------|---------------------|
| **List card — identity** | Name, logo, scale, regions | `company_name`, `companyLogo`, `chainScale`, `regionsSupported` (derived), `website` | **Strong** | Company name, region chips | Short summary empty (`explorerShortSummary`) | Populate summary from `companyDescription` |
| **List card — scale** | Properties, rooms, brands | `totalProperties`, `totalRooms`, `numberOfBrands`, `brandsManaged` | **Partial** | Counts | — | Validate numeric fields on publish |
| **Detail — overview** | Description, service model | `companyDescription`, `primaryServiceModel` | **Partial** | Overview paragraphs | Long narrative only | Structured bullets |
| **Detail — capabilities (`cap_*`)** | Operating, commercial, transition KPIs | `cap_kpi_*`, `cap_profile_*`, `cap_card_*`, `cap_deep_*`, `cap_signal_*` | **Partial** (longText + select) | Capability cards in `operator-setup-explorer-behavior.js` | Scoring not surfaced | Add **alignment signal** badge when `?dealId=` |
| **Detail — markets (`mkt_*`)** | Depth, gateway, mix | `mkt_narrative_depth`, `mkt_signal_*`, footprint totals | **Weak** for country-level | Market depth narrative | Active countries list | Normalized geography strip |
| **Detail — owner value (`ov_*`)** | Engagement, flexibility | `ov_card_*`, `ov_cluster_*`, `ownerEngagementNarrative` | **Partial** | Owner collaboration copy | — | Tie to owner-involvement factor |
| **Detail — best fit (`bf_*`)** | Assets, situations, structures | `bf_selected_*`, `bf_signal_*`, `bf_not_ideal_for` | **Strong** (structured multis) | “May be relevant if…” fit strip | — | Source for specific-operator OAS |
| **Detail — infrastructure (`infra_*`)** | Reporting, RM systems | `infra_kpi_*`, `infra_asset_management_reporting` | **Partial** | Systems & reporting section | — | Reporting level select |
| **Detail — leadership** | Exec team | Leadership child rows | **Strong** when `displayLeadershipOnExplorer` | Names, titles, bios | Flag off on some records | — |
| **Detail — case studies** | Proof points | Case Studies child | **Strong** | Situation / outcome stories | — | Tag experience types |
| **Detail — diligence** | Q&A | Diligence QA child | **Strong** | Owner diligence accordion | — | — |
| **Detail — brands** | Brands operated | `brands` link | **Strong** | Brand chips | — | — |
| **Alignment badge** | Score + label | — | **Missing** | “Moderate Alignment Signals” etc. | Entire section | Phase 4 — API with dealId |
| **Completeness indicator** | % P0 filled | — | **Missing** | “Profile data partial” | Entire section | `profile_completeness_pct` |
| **API fallback** | Live record | `third-party-operator-detail.js` | **Risk** — `MOCK_OPERATORS` in `api/operator-explorer.js` | — | Mock data shown on miss | Remove mock for `submission_status=Active` |

---

## 6. Profile Taxonomy Matrix

**Problem:** Deal field `Preferred Third-Party Operator Profile` options (`lib/deal-setup-form-options.json`) are: `No Preference`, `Independent / Boutique`, `Regional`, `National`, `International`. Product categories below require a **mapping layer** (`fixtures/operator-profile-archetypes.json` — proposed, not created in this pass).

### 6.1 Category definitions

#### Regional CALA Full-Service Operator

| Attribute | Value |
|-----------|--------|
| **Display label** | Regional CALA Full-Service Operator |
| **Internal key** | `regional_cala_full_service` |
| **Maps from existing deal options** | `Regional` **when** `Primary Market Region` or `Country` indicates CALA/Mexico/Caribbean OR footprint `geo_cala_total_hotels` &gt; 0 on matched operators |
| **Required deal signals** | `Country` or `Primary Market Region`; `Hotel Chain Scale` (upper upscale+); `Operator Strategy Status` ≠ “Not seeking operator input”; optional `Preferred Third-Party Operator Profile` includes `Regional` |
| **Required operator signals** (future) | CALA presence; full-service `primaryServiceModel`; resort/urban asset tags | 
| **Alignment signals (MVP)** | “Potential alignment if the deal is in CALA and the owner is reviewing regional full-service management profiles.” |
| **Review considerations** | Market depth outside gateway cities; resort vs urban mix; bilingual reporting needs |
| **Questions to clarify** | Confirm countries vs. recorded market; third-party vs. owner-operated target model |
| **Data gaps** | No dedicated CALA operator category on deal; operator countries not normalized |
| **MVP fallback** | Show card when deal region ∈ CALA set **OR** owner selected `Regional` + CALA country; qualitative only — **no numeric score** |

#### International Third-Party Manager with Market Presence

| Attribute | Value |
|-----------|--------|
| **Display label** | International Third-Party Manager with Market Presence |
| **Internal key** | `international_third_party_market_presence` |
| **Maps from existing deal options** | `International`; optionally `National` if `Country` is outside operator home region |
| **Required deal signals** | `Preferred Future Operating Model` includes third-party or brand+third-party; `Country` present |
| **Required operator signals** | `regionsSupported` ≠ single region; `totalProperties` &gt; threshold; `brands` non-empty |
| **Alignment signals** | “May be relevant if the owner is open to internationally affiliated third-party management with stated market presence.” |
| **Review considerations** | Local execution vs. centralized platform; brand portfolio overlap |
| **Questions to clarify** | Which markets must have active operations vs. pipeline-only |
| **Data gaps** | “International” on deal does not prove CALA experience |
| **MVP fallback** | Card when `International` selected **or** (third-party model + country not US-only heuristic) |

#### Lifestyle / Boutique Operator

| Attribute | Value |
|-----------|--------|
| **Display label** | Lifestyle / Boutique Operator |
| **Internal key** | `lifestyle_boutique` |
| **Maps from existing deal options** | `Independent / Boutique` |
| **Required deal signals** | `Hotel Chain Scale` (upscale/lifestyle); `Hotel Type` or `Project Type` lifestyle/urban; `Operator Capability Priorities` may include lifestyle programming |
| **Required operator signals** | `brand_signal_soft_retention`; asset types include lifestyle/urban; case studies lifestyle |
| **Alignment signals** | “Potential alignment for lifestyle-positioned assets where boutique operating depth may matter.” |
| **Review considerations** | F&B complexity; experience programming; smaller room count |
| **Questions to clarify** | Independent collection vs. soft brand path |
| **Data gaps** | No deal “lifestyle” boolean |
| **MVP fallback** | Card on `Independent / Boutique` **or** chain scale + project type keyword match |

#### Owner-Operated with Upgraded Commercial Support

| Attribute | Value |
|-----------|--------|
| **Display label** | Owner-Operated with Upgraded Commercial Support |
| **Internal key** | `owner_operated_commercial_support` |
| **Maps from existing deal options** | `No Preference` + `Preferred Future Operating Model` = `Owner-operated` or `Franchise/license only…`; `Services Required From Operator` without “Full Management” |
| **Required deal signals** | `Plan to Self-Manage or Hire Third Party?` self-manage; services = RM, Sales, Accounting, etc. |
| **Required operator signals** | Partial services granular checked; not full management positioning |
| **Alignment signals** | “May be relevant if the owner expects to retain operating control with selective commercial support.” |
| **Review considerations** | Scope creep into full management; reporting cadence |
| **Questions to clarify** | Which services must remain owner-controlled |
| **Data gaps** | No “owner retains control” boolean |
| **MVP fallback** | Card when future model is owner-operated **and** services exclude full management |

#### Brand-Managed Structure

| Attribute | Value |
|-----------|--------|
| **Display label** | Brand-Managed Structure |
| **Internal key** | `brand_managed_structure` |
| **Maps from existing deal options** | `No Preference` + `Preferred Future Operating Model` = `Brand-managed` or `Brand + third-party management` |
| **Required deal signals** | `Is the hotel currently branded?`; `Preferred Brands`; bid audience includes brand |
| **Required operator signals** | `brand_signal_franchise_align`; strong `brands` list |
| **Alignment signals** | “May be relevant if the deal path assumes brand-led operating standards with defined operator-of-record roles.” |
| **Review considerations** | Brand compliance; PIP/standards implementation |
| **Questions to clarify** | Division of responsibilities brand vs. operator-of-record |
| **Data gaps** | Brand-managed path conflated with franchise-only |
| **MVP fallback** | Card when `Preferred Future Operating Model` matches brand-managed options |

### 6.2 Mapping table (deal select → archetype)

| Existing `Preferred Third-Party Operator Profile` option | Primary archetype | Secondary archetype (when signals match) |
|----------------------------------------------------------|-------------------|------------------------------------------|
| No Preference | *(none — derive from operating model)* | All that pass signal rules |
| Independent / Boutique | Lifestyle / Boutique | Owner-operated + commercial (if services partial) |
| Regional | Regional CALA Full-Service *(if CALA)* | International *(if non-CALA country)* |
| National | International Third-Party *(domestic depth)* | Regional CALA *(if CALA)* |
| International | International Third-Party | Regional CALA *(if CALA + regional)* |

---

## 7. Existing Scoring Logic Mapping

Source: `OPERATOR_MATCH_WEIGHTS` and `scoreOperatorMatchForDeal` in `api/my-deals.js`.

| Existing factor name | Existing weight | Existing deal inputs | Existing operator prefill keys | Proposed OAS factor(s) | Keep / Modify / Deprecate | Notes |
|---------------------|-----------------|----------------------|-------------------------------|------------------------|---------------------------|-------|
| `geographyMarkets` | 18 | `Country` (Location) | `specificMarkets`, `regionsSupported`, `bestFitGeographies`, … | Geographic / market alignment (15) | **Modify** | Rebalance weight; improve country matching |
| `chainScale` | 8 | `Hotel Chain Scale` | `chainScale`, `chainScalesYouSupport` | Chain scale alignment (8) | **Keep** | — |
| `assetProjectStageFit` | 14 | `Project Type`, `Building Type`, `Stage of Development` | `bestFitAssetTypes`, `operatingSituations`, token scan | Project type (12) + Repositioning (6) + Pre-opening (8) | **Modify** | Split narrative factors |
| `dealStructureAssignment` | 12 | `Preferred Deal Structure` | `bestFitDealStructures`, `serviceModels` | Management economics / structure (10) | **Keep** | Label as structure compatibility |
| `feeCommercial` | 10 | MP fee expectations + structure | `feeStructureSummary`, `cap_profile_commercial`, token scan | Commercial platform (10) | **Modify** | Surface as data gap until structured fees |
| `serviceOfferings` | 8 | `Must-Haves From Brand/Operator` | `primaryServices`, granular arrays, `serviceDifferentiators` | Centralized services (4) + RM (5) + Sales (5) | **Modify** | Split by service category |
| `systemsReporting` | 6 | `Owner Reporting Frequency`, `Preferred Reporting Frequency` | `technologySystems`, `ownerReportingCadence`, `reportTypesProvided` | Owner reporting / governance (8) | **Modify** | Compare cadence tokens explicitly |
| `ownerRelations` | 6 | *(hardcoded deal text)* | `ownerCommunicationStyle`, `operatingCollaborationMode`, … | Owner involvement (7) | **Modify** | Use real deal involvement fields |
| `brandPortfolioRelevance` | 6 | `Preferred Brands` | `brands`, `brandsManaged` | Brand compliance support (6) | **Keep** | Frame as portfolio overlap signal |
| `negativeFitPenalty` | 2 | `Top 3 Deal Breakers` | `lessIdealSituations` | Review considerations (not positive points) | **Modify** | Show as penalty strip, not “alignment” |

**Not in engine — add in OAS document model:** Conversion/reflag (8), F&B (4), Data completeness (8), Profile-category qualitative layer (no weight).

**HTTP surface:** `GET /api/my-deals/:recordId/operator-match-score-breakdown?operatorId=` — **Keep**; OAS Phase 4 should call or batch this.

---

## 8. MVP Data Sufficiency

| OAS Section | Can Populate Now? | Required Fields | Missing Fields | MVP Fallback | Future Improvement |
|-------------|-------------------|-----------------|----------------|--------------|-------------------|
| **Deal Context** | **Yes** | `Property Name`, `Project Type`, `Country`, operating models, `Operator Strategy Status` | — | Pull from merged deal + readiness-style formatters (BAS) | Add institutional reporting |
| **Operator Review Signal** | **Partial** | `Operator Strategy Status`, OCS P0, `Who Should Receive Bids…` | Structured “review interest” | Derive signal from strategy status + in-scope check | Dedicated interest field |
| **Operator Profiles for Review** | **Partial** | `Preferred Third-Party Operator Profile`, `Preferred Future Operating Model`, region | Taxonomy mismatch | §6 mapping layer + 1–3 archetype cards | Operator-side category tags |
| **Alignment Score** | **No** (profile MVP) / **Partial** (specific-operator) | Scoring factors §2 | Profile-level weights undefined | Profile MVP: **omit numeric score** or show “—” with Insufficient Data | Batch operator scores |
| **Alignment Signals** | **Partial** | Deal + operator fields per factor | Many weak pairs | Bullet signals from factor notes; tier from score when available | Structured signal templates |
| **Review Considerations** | **Partial** | Project type, conversion, fees, reporting | Repositioning complexity | Static templates + `lessIdealSituations` / breakers overlap | `operator-alignment-rationale.js` |
| **Questions to Clarify** | **Yes** | Missing P0, `Country`, reporting, structure | — | Reuse BAS `formatClarificationLabel` patterns | Readiness cross-link |
| **Data Gaps** | **Yes** | Any null factor in breakdown | Operator fill rate | List factor `dealValue`/`operatorValue` “—” rows | Completeness % |
| **Suggested Workflow Action** | **Partial** | `Operator Strategy Status` | — | Map: Exploring → clarify capabilities; Ready for structured review → compare profiles; Not seeking → hide operator table | Playbook per status |
| **Standalone Print Page** | **Yes** (after Phase 2 build) | Same as modal payload | — | Clone `brand-alignment-snapshot.html` shell | PDF copy test like OCS |

---

## 9. Data Quality and Dependency Risks

| Risk | Severity | Detail | Mitigation |
|------|----------|--------|------------|
| **Field renames on Deals / SI / Location** | High | `api/schemas/deal-setup-fields.js`, `public/deal-setup.html`, `my-deals.js` PATCH, OCS scripts | Grep + `verify-deal-setup-routing`; no renames in Phase 1–4 |
| **Legacy 3rd Party Operator fields** | High | `lib/third-party-operator-airtable-fields-used.js` — 100+ columns; write-plan still routes | Treat legacy as read fallback until base confirmed empty |
| **`Preferred Third-Party Operators` name casing** | Medium | `(names)` vs `(Names)` alias | Always write via `SI_FORM_TO_AIRTABLE` |
| **`deal-setup-fields.js` OCS P0** | Medium | Shared with OCS intake `operator-capability-intake.js` | OAS read-only reuse; don’t fork field names |
| **`server.js` vs `server.upload-ready.js`** | Medium | OCS routes missing on upload-ready | Register OAS + OCS on both |
| **Duplicate `lib/` vs `api/lib/`** | Medium | `operator-setup-new-base-read.js` duplicated | Edit both or import single source |
| **Explorer mock fallback** | Medium | `MOCK_OPERATORS` in `api/operator-explorer.js` | Never show mock in OAS; use `third-party-operator-detail` bundle only |
| **Naming drift** | High (product) | Operator Match / Fit / Capability / Alignment | Enforce **Operator Alignment Snapshot** in UI; keep internal function names |
| **Fee factor false precision** | Medium | 55/75 placeholder scores | Label as **data gap** in OAS, not alignment signal |
| **Owner relations hardcoded deal value** | Medium | Scoring assumes generic owner priority | Replace with `Level of Involvement…` in Phase 4 |
| **Brand overlap confusion** | Low | Same brands as brand match | Copy must be operator-framed (“operators managing preferred brands”) |
| **`<40%` factor coverage** | Medium | Weighted score misleading | Force **Insufficient Data** label per §F in audit |

---

## 10. Recommended Next Implementation Plan

Aligned with [operator-alignment-snapshot-implementation-checklist.md](./operator-alignment-snapshot-implementation-checklist.md); phase order follows product request.

### Phase 1 — Profile-level OAS (mapping layer, no schema changes)

- [ ] Add `fixtures/operator-profile-archetypes.json` per §6 (internal keys, deal signals, copy templates)
- [ ] Implement `POST /api/ai/operator-alignment-snapshot` — profile cards + deal context only
- [ ] Reuse `fetchDealScoringContext` / merged linked records from `api/my-deals.js`
- [ ] Gate with `isOperatorInScopeFromFields` (`lib/operator-capability-inputs.js`)
- [ ] No numeric alignment score in response (or explicit `insufficientData: true`)

### Phase 2 — Standalone print page

- [ ] `public/operator-alignment-snapshot.html` + `public/js/operator-alignment-snapshot.js` + CSS (clone BAS)
- [ ] `?dealId=&embed=1&print=1` query contract
- [ ] Methodology note + banned-phrase test (`scripts/test-operator-alignment-snapshot-v1.mjs`)

### Phase 3 — My Deals action / modal

- [ ] `data-action="operator-alignment"` in `public/my-deals.html`
- [ ] Modal host mirroring brand-alignment pattern
- [ ] Link to standalone full page

### Phase 4 — Specific-operator alignment

- [ ] Batch or multi-call `scoreOperatorMatchForDeal` for Active + `readyForInvestorPublication` operators
- [ ] Wire `GET …/operator-match-score-breakdown` for Alignment Detail drawer
- [ ] Apply §2 weights; map tiers: Strong / Moderate / Conditional / Limited / Insufficient Data
- [ ] Register routes on `server.js` **and** `server.upload-ready.js`

### Phase 5 — Operator shortlist / cache (if product approves)

- [ ] Design **Deal Operator Cache** or **Operator Target List** (mirror `Target List` / `Deal Brand Cache`)
- [ ] Persist `alignmentScore`, `breakdownJson`, `alignmentLabel` per deal+operator

### Phase 6 — Airtable schema normalization

- [ ] Add §3–§4 “Future” fields (countries, experience tags, completeness, operator profile categories)
- [ ] Regenerate `operator-setup-new-base-build-sheet-rows.json`
- [ ] Reduce reliance on `Explorer Profile JSON` and legacy 3rd Party tables
- [ ] Explorer: alignment badges + completeness (§5)

---

## Appendix A — Quick reference: prefill keys used in scoring

From `scoreOperatorMatchForDeal` (`firstPresent` / `collectPresentList`):

| Prefill key | Typical Airtable origin |
|-------------|-------------------------|
| `specificMarkets`, `regionsSupported`, `regions` | Platform / Basics |
| `chainScale`, `chainScalesYouSupport` | Profile / Platform |
| `bestFitAssetTypes`, `propertyTypes`, `projectTypes` | Commercial `bf_*` / legacy Ideal |
| `operatingSituations`, `projectStages` | Commercial / legacy Ideal |
| `bestFitDealStructures`, `serviceModels` | Commercial |
| `primaryServices`, `revenueManagementServices`, … | Governance granular |
| `technologySystems`, `ownerReportingCadence` | Governance / Performance |
| `ownerCommunicationStyle`, `operatingCollaborationMode` | Owner Relations / Commercial |
| `brands`, `brandsManaged` | Profile `brands` link |
| `lessIdealSituations` | Commercial / Ideal |
| `feeStructureSummary`, `cap_profile_commercial` | Commercial / Platform narratives |

---

## Appendix B — Files to touch in implementation (read-only index)

| Concern | Files |
|---------|--------|
| Scoring | `api/my-deals.js` |
| Operator load | `api/lib/operator-setup-new-base-read.js`, `lib/operator-setup-new-base-read.js` |
| Deal fields | `api/schemas/deal-setup-fields.js`, `lib/operator-capability-inputs.js` |
| BAS template | `api/brand-alignment-snapshot.js`, `public/js/brand-alignment-snapshot.js` |
| Explorer | `api/third-party-operators-list.js`, `api/third-party-operator-detail.js`, `public/js/operator-explorer.js` |
| Legacy deps | `lib/third-party-operator-airtable-fields-used.js`, `lib/operator-setup-write-plan.js` |
| Form options | `lib/deal-setup-form-options.json` |

---

*End of field matrix.*
