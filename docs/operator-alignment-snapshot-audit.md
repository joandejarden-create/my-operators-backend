# Operator Alignment Snapshot — Read-Only Audit

**Date:** 2026-05-25  
**Repo:** `deal-capture-proxy` (Dealality platform)  
**Scope:** Audit only — no code, Airtable, migration, or copy changes  
**Product name (required):** **Operator Alignment Snapshot**

This audit inventories existing repo assets and Airtable field usage to plan an owner-facing feature modeled on **Brand Alignment Snapshot**, with two layers:

1. **Operator Profile Alignment** — deal vs. operator *profile categories* before specific-operator data is complete  
2. **Specific Operator Alignment** — deal vs. named operators from **Operator Setup** / **Operator Explorer**

---

## A. Executive Summary

### What exists today

| Capability | Status | Notes |
|------------|--------|-------|
| **Operator Alignment Snapshot** | **Missing** | No routes, HTML, API, or Airtable persistence under this name |
| **Brand Alignment Snapshot (BAS)** | **Production** | Full pattern: `POST /api/ai/brand-alignment-snapshot`, 2-page renderer, My Deals modal, print/PDF |
| **Operator Capability Snapshot (OCS)** | **Production** (`server.js`) | Deal-only capability themes; explicitly **no operator matching** |
| **Operator match scoring** | **API only** | `scoreOperatorMatchForDeal` + `GET /api/my-deals/:id/operator-match-score-breakdown` — **no owner UI** |
| **Operator Explorer** | **Partial** | Live list from Operator Setup; detail still has **mock fallback** |
| **Operator Setup (new base)** | **Active** | 12 Master records; Profile / Platform / Commercial / Governance + children |
| **Legacy 3rd Party Operator tables** | **Code-active** | Write/read paths remain; not in 2026 base record-count export |
| **Brand match / Target List** | **Production** | Brand shortlist only — **no operator shortlist table** |

### MVP data readiness

| Layer | Enough for MVP? | Rationale |
|-------|-----------------|-----------|
| **Profile alignment** | **Partial** | Deal has `Preferred Third-Party Operator Profile` but options are generic (Regional/National/International), not the product’s example profile taxonomy. No dedicated “profile category” reference table in code. |
| **Specific operator alignment** | **Partial** | Scoring engine + Operator Setup bundles exist; only ~12 operators in base; geography/market fields are narrative-heavy (`mkt_*`, `cap_*`) vs. normalized country/market multis. |
| **Explainable score** | **Strong foundation** | `scoreOperatorMatchForDeal` already returns weighted factors with deal/operator values and notes. |

### What is missing

- Named product surface (HTML, API, copy, My Deals action)
- Profile-category taxonomy aligned to product examples (or mapping from existing selects)
- Normalized operator geography (Active Countries, Mexico/CALA presence as structured fields)
- Operator shortlist / cache table (parallel to **Deal Brand Cache** / **Target List**)
- Owner-facing UI for operator scores
- Batch scoring across active operators (today: one operator per API call)
- Data completeness / confidence indicators on Explorer cards

### Biggest risks

1. **Naming drift:** Operator Match, Operator Fit, Operator Capability Snapshot, Operator Alignment Snapshot — different products; do not merge OCS with alignment scoring in copy or API.
2. **Field renames:** `lib/third-party-operator-airtable-fields-used.js`, `deal-setup-fields.js`, forms, and write-plan route hundreds of legacy + new-base fields.
3. **`server.upload-ready.js` drift:** OCS routes missing vs. `server.js` — deployment variant can break My Deals OCS.
4. **Duplicate lib trees:** `lib/` and `api/lib/` mirrors for operator setup — changes must stay in sync.
5. **Mock Explorer detail:** `api/operator-explorer.js` still ships `MOCK_OPERATORS` for detail fallback.
6. **Advisory language:** Legacy `brand-fit-analyzer` and some dashboard strings — must not leak into OAS.

### Recommended next step

**Phase 1:** Product sign-off on profile-category taxonomy + field mapping matrix (deal ↔ operator ↔ score factors). Then implement **Profile-level Operator Alignment Snapshot** on **My Deals** using BAS shell + extended `scoreOperatorMatchForDeal` (or profile-only variant), before Explorer-wide rollout.

---

## B. Existing Product Assets

### B.1 Core APIs and scoring

| Path | Purpose | Status | Reuse for OAS |
|------|---------|--------|---------------|
| `api/brand-alignment-snapshot.js` | Deal-level brand alignment document | **Active** | **Primary template** (API shape, tiers, disclaimers) |
| `api/operator-capability-snapshot.js` | Deal-only OCS HTTP | **Active** (`server.js`) | Gating, disclaimers, deal load — **not** scoring |
| `api/my-deals.js` | `scoreOperatorMatchForDeal`, `getOperatorMatchScoreBreakdown`, deal merge, brand cache | **Active** | **Core scoring engine** |
| `api/match-score-server.js` | Brand Match Score New (12 factors) | **Active** | Pattern only (brand side) |
| `api/operator-explorer.js` | List + detail (mock fallback) | **Active** | Operator discovery post-MVP |
| `api/third-party-operators-list.js` | Operator list (new base) | **Active** | Candidate operator pool |
| `api/third-party-operator-detail.js` | Single operator bundle | **Active** | Specific-operator cards |
| `api/third-party-operator-intake.js` | Operator setup POST | **Active** | Data ingestion |
| `api/target-list.js` | Brand shortlist per deal | **Active** | Pattern for future operator shortlist |
| `api/deal-readiness-review.js` | Deal Readiness Snapshot | **Active** | Document + modal pattern |
| `api/brand-fit-analyzer.js` | Legacy brand fit | **Legacy** | **Do not reuse** |

**Operator match API (exists, unused in UI):**

```
GET /api/my-deals/:recordId/operator-match-score-breakdown?operatorId=rec...
```

Returns: `operatorScore`, `operatorBreakdownDetails` (9 weighted factors).

### B.2 Frontend pages and JS

| Path | Purpose | Status | OAS relevance |
|------|---------|--------|---------------|
| `public/my-deals.html` | Owner workspace; BAS + OCS modals | **Active** | **MVP host** — add OAS action beside BAS/OCS |
| `public/js/brand-alignment-snapshot.js` | BAS 2-page book + print | **Active** | Clone renderer pattern |
| `public/js/operator-capability-snapshot.js` | OCS 2-page book + print | **Active** | Shell + disclaimer tone |
| `public/brand-alignment-snapshot.html` | Standalone BAS | **Active** | Standalone OAS page pattern |
| `public/operator-capability-snapshot.html` | Standalone OCS | **Active** | — |
| `public/operator-explorer.html` | Operator directory | **Active** | Phase 4 — profile cards + alignment badges |
| `public/js/operator-explorer.js` | List fetch | **Active** | — |
| `public/operator-explorer-detail.html` | Detail shell | **Active** | Phase 4 |
| `public/operator-explorer-gold-mock.html` | Gold UI prototype | **Prototype** | Visual reference only |
| `public/third-party-operator-setup-new-two.html` | Primary operator setup | **Active** | Data source for specific-operator layer |
| `public/my-third-party-operators-new.html` | Operator portfolio | **Active** | Operator-facing |
| `public/deal-setup.html`, `public/new-deal-setup.html` | Deal intake | **Active** | Deal field source |
| `public/deal-summary.html` | Deal summary | **Active** | Secondary placement |
| `public/deal-room-owner.html` | Owner deal room | **Active** | Later placement |
| `public/brand-development-dashboard.js` | Brand opportunity workspace | **Active** | Brand-only match breakdown today |

### B.3 CSS and shared shell

| Path | Purpose |
|------|---------|
| `public/css/brand-alignment-snapshot.css` | BAS document styles |
| `public/css/operator-capability-snapshot.css` | OCS styles |
| `public/css/snapshot-page-shell.css` | Shared snapshot chrome |
| `public/css/dealality-explorer-platform.css` | Explorer platform |
| `public/css/operator-explorer.css` | Explorer list/detail |

### B.4 Lib modules (operator)

| Path | Purpose | Status |
|------|---------|--------|
| `lib/operator-capability-inputs.js` | OCS P0 field names | **Active** — **flag:** used by forms, OCS API, `deal-setup-fields.js` |
| `lib/operator-capability-snapshot-build.js` | OCS builder | **Active** |
| `lib/operator-capability-rules.js` | Capability areas | **Active** |
| `lib/operator-capability-copy.js` | No-ranking disclaimers | **Active** — copy template for OAS |
| `lib/operator-setup-new-base-read.js` | Load operator bundle | **Active** — scoring + Explorer |
| `lib/operator-setup-write-plan.js` | Legacy + new write routing | **Active** — **flag:** field routing |
| `lib/third-party-operator-airtable-fields-used.js` | Legacy field catalog | **Active** — **flag:** dependency map |
| `lib/operator-setup-service-granular-fields.js` | Service checkbox columns | **Active** — **flag:** many Airtable columns |
| `lib/build-third-party-operator-prefill.js` | Prefill merge | **Active** |
| `lib/deal-brand-cache-snapshot.js` | Brand cache read model | **Active** | Brand only |

### B.5 Scripts and docs (reference)

| Path | Purpose |
|------|---------|
| `docs/brand-alignment-snapshot-v1-audit.md` | BAS pre-build audit |
| `docs/operator-capability-inputs-v1.md` | OCS field inventory |
| `docs/deal-readiness-scoring-audit.md` | Readiness fields |
| `scripts/inventory-operator-setup-he-cala.mjs` | Operator Setup fill audit |
| `scripts/generate-operator-setup-build-sheet-rows.mjs` | Build sheet generator |
| `api/lib/operator-setup-new-base-build-sheet-rows.json` | 84 committed setup fields |
| `scripts/test-operator-capability-snapshot-v1.mjs` | OCS tests |
| `reports/airtable-base-record-counts-2026-05-20.csv` | Live table inventory |

### B.6 Server routes (operator-related)

Registered in `server.js` (and mostly mirrored in `server.upload-ready.js`):

| Method | Path | Notes |
|--------|------|-------|
| GET | `/api/operator-explorer/operators`, `/operator` | Explorer |
| GET | `/api/third-party-operators`, `/list`, `/new` | Lists |
| GET/PATCH | `/api/intake/third-party-operators/:id`, `/status` | CRUD |
| POST | `/api/intake/third-party-operator`, `/api/third-party-operators/submit` | Intake |
| GET | `/api/deals/:dealId/operator-capability-snapshot` | OCS — **missing in upload-ready** |
| POST | `/api/ai/operator-capability-snapshot` | OCS — **missing in upload-ready** |
| GET | `/api/my-deals/:id/operator-match-score-breakdown` | **Scoring — no UI** |
| POST | `/api/ai/brand-alignment-snapshot` | BAS template |

HTML routes: `/operator-explorer`, `/my-deals`, `/brand-alignment-snapshot.html`, `/operator-capability-snapshot.html`, `/third-party-operator-intake` → setup form.

### B.7 PDF / export

- **Browser print only** (`window.print()` + snapshot book host) — BAS, OCS, DRS pattern  
- **No** server-side PDF for operator features  
- `scripts/test-ocs-pdf-export-copy.mjs` — Puppeteer guard for OCS copy

---

## C. Existing Airtable Assets

**Base:** `AIRTABLE_BASE_ID` (`appvtnDurnMSjINP6` per 2026 report). No table named `Operators`; production operators live under **Operator Setup - Master** (+ linked tables).

### C.1 Operator Setup (new base) — record counts (2026-05-20)

| Table | Records | Role |
|-------|---------|------|
| Operator Setup - Master | 12 | Identity, `submission_status`, `company_name` |
| Operator Setup - Profile & Positioning | 12 | Narrative, brand signals, `brands` link, `readyForInvestorPublication` |
| Operator Setup - Platform & Markets | 12 | `cap_*`, `mkt_*`, footprint totals, `regions` |
| Operator Setup - Commercial Fit & Terms | 13 | `bf_*` best-fit multis, owner-value narratives |
| Operator Setup - Governance, Delivery & Diligence | 13 | `infra_*`, `risk_*`, `lead_*`, granular services |
| Operator Setup - Leadership Team Members | 59 | Child |
| Operator Setup - Case Studies | 33 | Child |
| Operator Setup - Diligence QA | 100 | Child |

**Link field:** `Operator` → Master on all child / 1:1 tables.

**Explorer visibility fields (code):**

- `submission_status` = `Active` (list filter `activeOnly=1`)
- `readyForInvestorPublication` (Profile)
- `displayLeadershipOnExplorer` (Governance)
- Legacy: `Explorer Profile JSON` on **3rd Party Operator - Basics**

### C.2 Legacy 3rd Party Operator tables

Canonical inventory: `lib/third-party-operator-airtable-fields-used.js` (~170+ Basics fields, split tables: Footprint, Performance, Service Offerings, Ideal Projects & Deal Fit, Owner Relations, Deal Terms, Case Studies, Diligence QA).

**Dependency flag:** Intake write-plan, prefill, and Explorer still reference these names. **Not listed** in 2026 record-count CSV — may be empty or deprecated on base while code remains.

### C.3 Deals and linked intake (operator-relevant)

| Table | Key fields (exact Airtable names) | Used in code | Audience |
|-------|-----------------------------------|--------------|----------|
| **Deals** | `Project Type`, `Stage of Development`, `Current Operating Model`, `Opening / Transition Phase`, `Property Name`, `Deal Status` | Scoring, OCS, intake, My Deals | Owner |
| **Location & Property** | `Country`, `Primary Market Region`, `Hotel Chain Scale`, `Hotel Type`, `Building Type`, `City & State`, room counts | Scoring, OCS P0 | Owner |
| **Strategic Intent** | `Preferred Future Operating Model`, `Operator Strategy Status`, `Operator Capability Priorities`, `Preferred Third-Party Operators (names)`, `Preferred Third-Party Operator Profile`, `Services Required From Operator`, `Must-Haves From Brand/Operator`, `Top 3 Deal Breakers`, `Preferred Brands`, reporting fields | Scoring, OCS, readiness | Owner |
| **Market - Performance** | `Preferred Deal Structure`, royalty/marketing/loyalty fee expectation fields | Scoring | Owner |
| **Contact & Uploads**, **Lease Structure** | Various | Intake | Owner |

**Field alias flags** (`deal-setup-fields.js`): `Preferred Third-Party Operators (names)` ↔ `(Names)`; `Who should receive bids…` variants.

### C.4 Brand / pipeline tables (not operator, but parallel)

| Table | Records | Operator relevance |
|-------|---------|-------------------|
| Target List | 35 | Brand shortlist pattern only |
| Brand Deal Requests | 23 | Brand outreach |
| Deal Brand Cache | 115 | Pre-computed **brand** scores |
| Deal Activity Log | 239 | Outreach audit (`Stakeholder` can be Operator) |
| Franchise Applications | 3 | JSON `Form Data` blob |

### C.5 No dedicated operator scoring storage

- No **Deal Operator Cache** or **Operator Target List** in codebase  
- Match scores for operators are **computed at request time** only

---

## D. Reusable Logic

### D.1 Brand Alignment Snapshot (primary template)

| Component | Path | Inputs | Outputs | Generic enough? | Changes for OAS |
|-----------|------|--------|---------|-----------------|-----------------|
| API orchestration | `api/brand-alignment-snapshot.js` | Deal + SI + cache + targets | JSON document payload | **Yes** | Swap brand scoring for operator scoring; tier labels per §G |
| Tier mapping | `tierFromScore()` in BAS | 0–100 score | Higher/Moderate/Conditional/Lower Alignment Signal | **Yes** | Rename to operator alignment labels (§G) |
| Rationale copy | `lib/brand-alignment-rationale.js` | Brand + deal context | Review considerations, questions | **Partial** | New `operator-alignment-rationale.js` — neutral tone |
| Renderer | `public/js/brand-alignment-snapshot.js` | API JSON | 2-page DOM + print | **Yes** | New `operator-alignment-snapshot.js` |
| My Deals launch | `my-deals.html` `data-action="brand-alignment"` | dealId | Modal | **Yes** | Add `operator-alignment` action |

### D.2 Operator match scoring (specific-operator layer)

| Component | Path | Function | Inputs | Outputs |
|-----------|------|----------|--------|---------|
| Engine | `api/my-deals.js` | `scoreOperatorMatchForDeal` | `dealFields`, `locationData`, `mpData`, `siData`, `operatorPrefill`, `brandNameById` | `score` 0–100, `breakdownDetails` |
| Loader | `api/lib/operator-setup-new-base-read.js` | `loadNewBaseOperatorBundle`, `buildPrefillObjectFromNewBaseRows` | Master record id | Merged prefill object |
| HTTP | `api/my-deals.js` | `getOperatorMatchScoreBreakdown` | dealId + operatorId query | JSON breakdown |

**Current weights (sum 90 scored + 2 penalty = 92 — note weights in code sum to 90 for positive factors + 2 penalty):**

| Factor key | Weight | Deal fields | Operator prefill keys |
|------------|--------|-------------|------------------------|
| geographyMarkets | 18 | `Country` (Location) | `specificMarkets`, `regionsSupported`, `bestFitGeographies`, … |
| chainScale | 8 | `Hotel Chain Scale` | `chainScale`, `chainScalesYouSupport` |
| assetProjectStageFit | 14 | `Project Type`, `Building Type`, `Stage of Development` | `bestFitAssetTypes`, `operatingSituations`, … |
| dealStructureAssignment | 12 | `Preferred Deal Structure` (MP) | `bestFitDealStructures`, `serviceModels`, … |
| feeCommercial | 10 | Fee expectations (MP) | `feeStructureSummary`, `cap_profile_commercial`, … |
| serviceOfferings | 8 | `Must-Haves From Brand/Operator` | `primaryServices`, granular service arrays, … |
| systemsReporting | 6 | `Owner Reporting Frequency`, `Preferred Reporting Frequency` | `technologySystems`, `ownerReportingCadence`, … |
| ownerRelations | 6 | (implicit priority) | `ownerCommunicationStyle`, … |
| brandPortfolioRelevance | 6 | `Preferred Brands` | `brands`, `brandsManaged` |
| negativeFitPenalty | 2 | `Top 3 Deal Breakers` | `lessIdealSituations` |

**Missing-data behavior today:** Per-factor `score: null` excluded from weighted sum; partial defaults (e.g. 35, 45, 55) when one side empty.

**Generic enough?** **Yes** for specific-operator layer with extensions (§F).

### D.3 Operator Capability Snapshot (deal-only — do not conflate)

| Component | Path | Role for OAS |
|-----------|------|--------------|
| `lib/operator-capability-snapshot-build.js` | Deal capability **themes** — separate tab/section in My Deals, not replacement for alignment |
| `lib/operator-capability-inputs.js` | Shared P0 fields — **reuse fields**, not narrative engine |

### D.4 Readiness Review

| Component | Path | Reuse |
|-----------|------|-------|
| `api/deal-readiness-review.js` | `buildReadinessFromFields` | Clarifications, data-gap lists, executive summary pattern |
| `api/deal-readiness-context.js` | Field presence by tab | “Questions to Clarify” source |

### D.5 Brand Match (reference only)

`computeMatchScoreForDealBrand` — 12 factors, pre-filters, **Deal Brand Cache** — use as **scoring discipline** reference, not operator engine.

---

## E. Data Gap Analysis

### E.1 Deal / owner intake — already useful

| Field | Present? | Quality | OAS use |
|-------|----------|---------|---------|
| `Project Type` | Yes | Good | Project type alignment |
| `Stage of Development` | Yes | Good | Stage / transition alignment |
| `Current Operating Model` | Yes | Good (OCS P0) | Operating model alignment |
| `Preferred Future Operating Model` | Yes | Good (OCS P0) | Desired model alignment |
| `Operator Strategy Status` | Yes | Good | Workflow gating (exploring vs ready for review) |
| `Operator Capability Priorities` | Yes | Good | Service / capability alignment |
| `Country`, `Primary Market Region` | Yes | Good | Geography |
| `Hotel Chain Scale`, `Hotel Type` | Yes | Good | Scale / asset alignment |
| `Preferred Deal Structure` | Yes | Good | Structure alignment |
| `Services Required From Operator` | Yes | Good | Service alignment |
| `Must-Haves From Brand/Operator` | Yes | Good | Service / capability |
| `Preferred Third-Party Operator Profile` | Yes | **Weak taxonomy** | Profile layer — options ≠ product examples |
| `Preferred Third-Party Operators (names)` | Yes | Good for named shortlist | Specific-operator layer |
| `Top 3 Deal Breakers` | Yes | Good | Negative-fit penalty |
| `Preferred Brands` | Yes | Good | Portfolio relevance factor |
| Owner reporting fields | Yes | Moderate | Reporting alignment |
| Fee expectation fields (MP) | Yes | Moderate | Commercial alignment (coarse scoring today) |

### E.2 Deal fields — missing or weak for OAS

| Proposed field | Status | Gap |
|----------------|--------|-----|
| Current Operating Model | **Exists** (OCS) | Normalize with legacy self-manage fields |
| Desired Operating Model | **Exists** as `Preferred Future Operating Model` | — |
| Owner Has In-House Hotel Operations Team | **Not found** as canonical field | Gap |
| Owner Wants to Retain Operating Control | **Partial** — `Level of Involvement in Day-to-Day Ops` | Not same semantic; needs mapping |
| Repositioning Complexity | **Not found** | Gap — infer from `Project Type` only |
| Brand Standards Implementation Needed | **Partial** — brand-side fields | Weak for operator layer |
| Pre-Opening / Re-Opening Support Needed | **Partial** — `Operator Capability Priorities` | Not dedicated boolean |
| F&B Complexity | **Partial** — F&B amenity fields on deal | Weak |
| Commercial Support Needed | **Partial** — priorities + services | Weak |
| Revenue Management Support Needed | **Partial** | Weak |
| Sales Support Needed | **Partial** | Weak |
| Reporting Sophistication Needed | **Partial** | Weak |
| Preferred Operator Type | **Partial** — `Preferred Third-Party Operator Profile` | Taxonomy mismatch |
| Operator Review Interest | **Partial** — `Operator Strategy Status` | Close but not 1:1 |
| Third-Party Management Openness | **Partial** — bid audience + operating model | Derived |
| Owner Decision-Making Priorities | **Not structured** | Gap |
| Institutional Reporting Requirement | **Not found** | Gap |
| Management Agreement Flexibility Importance | **Not found** | Gap |
| Centralized Services Sensitivity | **Not found** | Gap |
| Brand Compliance Support Needed | **Not found** | Gap |

### E.3 Operator setup — already useful

| Signal | Where stored | Structured? |
|--------|--------------|-------------|
| Company name, HQ, website | Profile / Master | Moderate |
| Regions / markets | Platform `regions`, `specificMarkets`, footprint totals | Moderate |
| Chain scale | Profile `chainScalesSupported` | Moderate |
| Best-fit assets / situations / structures | Commercial `bf_*` | **Good** |
| Services (granular) | Governance checkboxes + aggregates | **Good** but many columns |
| Brands operated | Profile `brands` link | **Good** |
| Case studies | Child table | **Good** |
| Owner reporting / infra | Governance `infra_*`, narratives | Weakly structured |
| Less ideal situations | Commercial / legacy Ideal table | Moderate |
| Explorer publish flags | `readyForInvestorPublication`, `submission_status` | **Good** |

### E.4 Operator setup — missing or weak

| Proposed field | Status |
|----------------|--------|
| Active Countries (multi) | **Weak** — inferred from footprint totals / narrative |
| Active Markets / Cities | **Partial** — `specificMarkets` text |
| CALA Presence / Mexico Presence | **Partial** — `geo_cala_total_hotels` style keys in footprint |
| Service Models Supported | **Partial** — `primaryServiceModel`, `bf_selected_deal_structures` |
| Soft Brand / conversion / new-build / repositioning experience | **Partial** — `brand_signal_*`, narratives, not normalized multis |
| Urban / resort / all-inclusive / lifestyle experience | **Partial** — `bf_selected_asset_types`, case studies |
| F&B / RM / sales capability **levels** | **Partial** — `cap_kpi_*` singleSelects + longText |
| Institutional vs family-owned owner experience | **Weak** |
| Minimum key count | **Not found** in new-base build sheet |
| Typical management fee / incentive / termination flexibility | **Weak** — narratives only |
| Operator Explorer Visibility | **Partial** — publish flags exist |
| Operator Setup Completion Score | **Not found** |
| Operator Profile Completeness / Data Confidence | **Not found** |
| Last Updated / Source Type / Internal Notes | **Partial** — no standard completeness model |

### E.5 Normalization needs

| Area | Issue |
|------|-------|
| Profile categories (deal) | Form options: `No Preference`, `Independent / Boutique`, `Regional`, `National`, `International` — vs. product examples (CALA full-service, lifestyle, owner-operated + commercial support, brand-managed) |
| Geography | Country string match vs. operator market list — brittle (`includes` substring) |
| Fee commercial | Placeholder 55/75 when both sides present — not true fee alignment |
| Owner relations | Hardcoded deal copy — not from deal fields |
| Legacy vs new-base field names | Prefill merges aliases — risk if columns renamed |

---

## F. Recommended Operator Alignment Score Model (100 points)

**Principles:** Explainable, weighted, factor-level notes, no “recommended operator” language. Reuse `scoreOperatorMatchForDeal` structure where possible; add profile-category and data-confidence categories for v1.

### F.1 Score labels (alignment, not recommendation)

| Score range | Label |
|-------------|-------|
| ≥ 75 | **Strong Alignment Signals** |
| 55–74 | **Moderate Alignment Signals** |
| 35–54 | **Conditional Alignment Signals** |
| 1–34 | **Limited Alignment Signals** |
| No score / &lt; 35% factors scored | **Insufficient Data** |

(Map BAS “Higher/Moderate/Conditional/Lower” to above for consistency with product brief.)

### F.2 Categories, weights, and fields

| # | Category | Weight | Required deal fields | Required operator / profile fields | Logic summary | Missing data |
|---|----------|--------|----------------------|-----------------------------------|---------------|--------------|
| 1 | Geographic / market alignment | **15** | `Country`, `Primary Market Region`, city if present | `regionsSupported`, `specificMarkets`, CALA/Mexico footprint totals | Exact country in market list → 100; region overlap → 65; else 35 | Null factor if both empty; cap total if &lt; 40% weight scored |
| 2 | Project type alignment | **12** | `Project Type`, `Building Type` | `bf_selected_asset_types`, `bf_selected_situation_types`, conversion signals | Token overlap + conversion/reflag signals | Partial 45 if operator empty |
| 3 | Service model / chain scale alignment | **10** | `Hotel Chain Scale`, `Hotel Service Model` | `chainScalesSupported`, `primaryServiceModel` | Exact scale → 100; partial → 65 | Reuse existing chainScale factor |
| 4 | Brand conversion / compliance capability | **10** | `Project Type`, brand status fields, `Operator Capability Priorities` | `brand_signal_reflag`, `brand_signal_audit`, `brand_narrative_compliance` | Conversion deal + positive signals → boost; mismatch → lower | Skip if not conversion-type deal |
| 5 | Commercial platform alignment | **10** | MP fee expectations, `Preferred Deal Structure` | `bf_selected_deal_structures`, commercial narratives, fee summaries | Extend fee factor beyond 55/75 placeholder | Null if no commercial data |
| 6 | Owner reporting / governance alignment | **8** | `Owner Reporting Frequency`, `Owner Reporting Package` | `infra_kpi_reporting`, `ownerReportingCadence`, `infra_asset_management_reporting` | Cadence token match + infra signals | Default 45 if operator reporting unknown |
| 7 | Pre-opening / repositioning support | **8** | `Opening / Transition Phase`, `Stage of Development`, priorities | `cap_kpi_transition`, `operatingSituations`, `bf_signal_transition` | Stage/situation overlap | N/A factor for stabilized-only deals |
| 8 | Economic / deal structure compatibility | **12** | `Preferred Deal Structure` | `bf_selected_deal_structures`, `bf_signal_dealsize`, `bf_signal_capital` | Reuse dealStructureAssignment | — |
| 9 | Owner involvement preference | **7** | `Level of Involvement in Day-to-Day Ops`, operating models | `ov_cluster_interaction`, `ownerEngagementNarrative`, `operatingCollaborationMode` | Keyword + involvement band match | Weak until deal field strengthened |
| 10 | Data completeness / confidence | **8** | (deal P0 completeness for OCS fields) | Operator Setup fill rate on critical columns | % of required operator keys present | Always show as **Data gap** section; down-weight confidence not numeric score |

**Total: 100.** Existing 9-factor engine covers ~82 points of substantive alignment; add **profile-category match (Phase 2 profile layer)** as optional **modifier** (+0–10) comparing deal `Preferred Third-Party Operator Profile` to operator tags — **do not implement until taxonomy agreed**.

### F.3 Example factor output (JSON shape)

```json
{
  "alignmentScore": 68.4,
  "alignmentLabel": "Moderate Alignment Signals",
  "confidenceLabel": "Partial data — 3 factors insufficient",
  "factors": [
    {
      "key": "geographyMarkets",
      "label": "Geography & Markets",
      "weight": 15,
      "score": 100,
      "dealValue": "Country: Mexico",
      "operatorValue": "Supported markets: Cancún, Riviera Maya, CDMX",
      "note": "Deal country appears in operator stated markets.",
      "alignmentSignal": "Potential alignment on primary country"
    }
  ],
  "reviewConsiderations": [],
  "questionsToClify": [],
  "dataGaps": []
}
```

### F.4 Profile-level layer (Phase 2)

Until taxonomy exists, map **deal** `Preferred Third-Party Operator Profile` selects to **profile archetypes** (config JSON, not Airtable):

| Product example archetype | Interim mapping from current options |
|---------------------------|--------------------------------------|
| Regional CALA full-service operator | `Regional` + CALA region on deal |
| International third-party manager with market presence | `International` |
| Lifestyle / boutique operator | `Independent / Boutique` |
| Owner-operated with upgraded commercial support | `No Preference` + `Preferred Future Operating Model` = Owner-operated |
| Brand-managed structure | `Preferred Future Operating Model` = Brand-managed |

**No numeric score against archetypes in MVP** — show **Operator Profiles for Review** cards with qualitative **alignment signals** only.

---

## G. Recommended UI Structure

### G.1 Section labels (use exactly)

- Operator Alignment Snapshot  
- Deal Context  
- Operator Review Signal  
- Operator Profiles for Review *(profile layer)*  
- Alignment Score  
- Alignment Signals  
- Review Considerations  
- Questions to Clarify  
- Data Gaps  
- Suggested Workflow Action  
- Alignment Detail  

### G.2 Copy rules

**Use:** alignment signal, potential alignment, review consideration, clarify before outreach, may be relevant if, suggested workflow action, data gap, owner decision point, operator profiles for review, alignment detail.

**Avoid:** Dealality recommends, the owner should, best strategy, recommended path, we advise, strongest path, operator recommendation, operating model recommendation, best operator, preferred operator.

### G.3 MVP layout (My Deals modal + standalone page)

1. **Cover** — Deal name, market, project type, methodology note (non-advisory)  
2. **Deal Context** — Operating model today → target; operator strategy status  
3. **Operator Profiles for Review** — 3–5 archetype cards (profile layer)  
4. **Specific operators** (if strategy status ≥ exploring) — Table: operator name, alignment score, label, link to Alignment Detail  
5. **Alignment Detail** — Expandable factor breakdown (reuse breakdown modal pattern from brand dashboard)  
6. **Review Considerations / Questions / Data Gaps**  
7. **Suggested Workflow Action** — e.g. “May be relevant if owner confirms third-party management scope”  
8. Print / Save PDF (browser print)

### G.4 Future layout

- **Operator Explorer** — alignment badges on cards, completeness %  
- **Operator shortlist** table (new Target List analogue)  
- **Deal room / PDF export** shared with advisors  
- Side-by-side compare **2–3 operators** (not “winner”)

### G.5 Placement recommendation

| Placement | MVP? | Later? |
|-----------|------|--------|
| **My Deals** (per-deal action + modal) | **Yes — best first** | — |
| Standalone `operator-alignment-snapshot.html` | Yes | — |
| Deal detail / deal-summary | Optional link | Yes |
| Operator Explorer | No | **Yes** — badges + detail panel |
| Operator shortlist tab | No | Yes — needs new table |
| Admin Operator Setup | No | Internal QA only |
| PDF / email share | Print only MVP | Server render later |

---

## H. Operator Explorer Updates

### H.1 Current state

- **List:** Live from Operator Setup Master (~12 records)  
- **Detail:** API falls back to `MOCK_OPERATORS` when Airtable match fails  
- **Gold mock:** `operator-explorer-gold-mock.html` for design parity  

### H.2 Gaps vs. OAS source-layer requirements

| Requirement | Status |
|-------------|--------|
| Operator profile cards | Partial — list tiles; detail incomplete |
| Capability summaries | Present as `cap_*` / longText — needs structured badges |
| Geography / market presence | Footprint totals — needs **Active Countries** display normalization |
| Brand experience | `brands` link works | 
| Conversion / repositioning | `brand_signal_*` — show as tags |
| Owner reporting capabilities | `infra_*` — bury in narrative |
| Commercial capabilities | `ov_*`, `bf_*` — needs summary strip |
| Case studies | Child table — OK |
| Fit / alignment signal badges | **Missing** |
| Data completeness indicators | **Missing** |

### H.3 Recommended additions (Airtable + UI)

1. **Normalized multis:** `active_countries`, `active_markets`, `experience_tags` (conversion, new-build, lifestyle, resort, all-inclusive)  
2. **Computed:** `profile_completeness_pct`, `data_confidence_level`, `explorer_visible` (formula from publish flags)  
3. **UI:** Completeness ring on card; “Alignment signals available” when deal context passed via query param  
4. **Remove dependency on mock detail** for production deals  

---

## I. Operator Setup Updates

### I.1 Required vs. optional (recommended)

| Priority | Fields | Audience |
|----------|--------|----------|
| **P0 (required for Explorer publish)** | company name, HQ, active countries/regions, chain scales, primary service model, bf_selected_* multis, submission_status, readyForInvestorPublication | Operator + admin |
| **P1 (alignment scoring)** | markets list, brand links, service granular selections, less-ideal situations, reporting cadence | Operator |
| **P2 (enhancement)** | fee structure summaries, case studies, leadership | Operator |
| **Admin-only** | internal notes, source type, data confidence overrides, audit fields | Admin |

### I.2 Field changes (recommend — do not execute in audit)

- Add structured **experience tags** (multi-select) mirroring deal project types  
- Add **minimum_keys** number  
- Add **owner_type_experience** (institutional, family office, single-asset)  
- Normalize **CALA / Mexico** as booleans derived from footprint OR explicit selects  
- Add **operator_profile_categories** multi-select aligned to product taxonomy  
- Add **setup_completion_score** formula (% P0 filled)  

### I.3 Owner-facing vs. internal

| Owner-facing on Explorer | Internal only |
|------------------------|---------------|
| Published profile, capabilities summary, markets, brands, case studies, diligence Q&A | Draft submission, internal notes, mapping QA fields, raw granular checkboxes (show aggregates only) |

---

## J. Phased Implementation Plan

| Phase | Scope | Deliverables |
|-------|--------|--------------|
| **0** | Audit only | This document + checklist (**current**) |
| **1** | Data model & mapping | Taxonomy doc, field matrix, dependency review with Airtable owner; no renames without grep audit |
| **2** | Profile-level OAS | Archetype config, My Deals modal, deal-only + profile cards, no specific-operator scores required |
| **3** | Specific-operator scoring | Batch endpoint, wire `scoreOperatorMatchForDeal`, ranked table, breakdown UI |
| **4** | Operator Explorer integration | Live detail, badges, completeness, remove mock fallback for published operators |
| **5** | PDF/export & sharing | Print CSS parity tests; optional server HTML export |

---

## K. Open Questions

### For product owner

1. Final **operator profile category** list — replace or extend `Preferred Third-Party Operator Profile` options?  
2. Should profile layer show **fixed archetypes** always, or only when deal has operator strategy ≠ “Not seeking operator input”?  
3. Maximum operators to show in specific-operator table (cap at 5? 10? only Active + published)?  
4. Relationship to **OCS** — separate buttons on My Deals or combined entry?  
5. Is **operator shortlist** (Target List analogue) in scope for Phase 3 or 4?  
6. Should alignment scores be **persisted** on deal or computed on demand only?

### For Airtable schema review

1. Confirm legacy **3rd Party Operator** tables empty vs. still synced — can writes be disabled?  
2. Approve new columns on Operator Setup vs. reusing `bf_*` / `cap_*` only?  
3. Any automations on `submission_status`, `readyForInvestorPublication`, or deal operator fields that would break on new selects?  
4. Create **Deal Operator Cache** / **Operator Target List** tables or defer?

### For UX / copy

1. Exact **Suggested Workflow Action** templates per `Operator Strategy Status`  
2. Modal vs. full-page default on desktop  
3. Spanish / bilingual deal fields — any copy impact?

---

## Appendix: File inventory (operator / alignment / match)

### Active — high priority

- `api/my-deals.js` — scoring engine  
- `api/brand-alignment-snapshot.js` — document template  
- `api/operator-capability-snapshot.js` — OCS  
- `api/lib/operator-setup-new-base-read.js` — operator load  
- `lib/operator-capability-inputs.js` — shared P0 names  
- `lib/third-party-operator-airtable-fields-used.js` — legacy deps  
- `public/my-deals.html` — workspace  
- `public/js/brand-alignment-snapshot.js`, `public/js/operator-capability-snapshot.js`  
- `docs/brand-alignment-snapshot-v1-audit.md`, `docs/operator-capability-inputs-v1.md`

### Active — supporting

- `public/operator-explorer.html`, `public/js/operator-explorer.js`, `api/operator-explorer.js`  
- `public/third-party-operator-setup-new-two.html`  
- `lib/operator-setup-write-plan.js`, `lib/operator-setup-new-base-writer.js`  
- `api/schemas/deal-setup-fields.js`  
- `lib/deal-setup-form-options.json`  
- `server.js`, `server.upload-ready.js`

### Prototype / legacy

- `public/operator-explorer-gold-mock.html`, `public/js/operator-explorer-gold-mock-data.js`  
- `api/brand-fit-analyzer.js`, `public/archive/deal-brand-fit-analyzer.html`  
- `public/third-party-operator-intake.html` (superseded)  
- `public/my-third-party-operators.html` (superseded)

---

## Recommended next Cursor prompt (after review)

```
Implement Operator Alignment Snapshot Phase 2 (profile-level) in deal-capture-proxy.

Constraints:
- Follow docs/operator-alignment-snapshot-audit.md and docs/operator-alignment-snapshot-implementation-checklist.md
- Mirror Brand Alignment Snapshot (api/brand-alignment-snapshot.js + public/js/brand-alignment-snapshot.js + my-deals modal)
- Product name: Operator Alignment Snapshot only; use alignment labels from audit §F/G; no advisory/recommendation language
- Phase 2 scope: deal context + Operator Profiles for Review (archetypes from approved taxonomy config) — no batch specific-operator scoring yet
- Reuse lib/operator-capability-inputs.js for deal P0 gating; keep OCS separate
- Add POST /api/ai/operator-alignment-snapshot and register in server.js AND server.upload-ready.js
- Add public/operator-alignment-snapshot.html + public/js/operator-alignment-snapshot.js + CSS (clone BAS shell)
- Add My Deals action data-action="operator-alignment"
- Do not rename Airtable fields; do not modify Airtable schema in this phase
- Add tests: scripts/test-operator-alignment-snapshot-v1.mjs (smoke + copy guard for banned phrases)
- Provide a field-mapping JSON for profile archetypes (fixtures/operator-profile-archetypes.json) for product to edit without code changes
```

---

*End of audit.*
