# Partner Intelligence — Extracted Facts (Airtable fields)

**Table name (exact):** `Partner Intelligence - Extracted Facts`  
**Base:** Primary (`AIRTABLE_BASE_ID`)  
**Status:** Proposed.

**Purpose:** Staging table for every AI- or human-extracted fact before it may appear in Brand Explorer or Operator Explorer. **Nothing in this table is live in Explorer until reviewed and published via the Published Explorer Fields workflow.**

---

## Links

| Link field | Links to | Required |
|------------|----------|----------|
| **Source Record** | `Partner Intelligence - Source Library` | Yes |
| **Parent Company** | Brand Basics (parent) | Context (mirror source) |
| **Brand** | Brand Basics | When Profile Type = Brand |
| **Operator / Management Company** | Operator Setup - Master | When Profile Type = Operator |

---

## Field specification

| Airtable field name | Type | Allowed values / notes |
|---------------------|------|------------------------|
| **Profile Type** | Single select | Brand, Operator, Parent Company, Service Provider, Other |
| **Explorer Type** | Single select | Brand Explorer, Operator Explorer, Internal Intelligence, Other |
| **Explorer Section** | Single line | e.g. `Franchise Economics`, `Operating Platform`, `Brand Identity` |
| **Field Name** | Single line | Canonical key from field registry (e.g. `feeStructure.initialFranchiseFee`) |
| **Extracted Value** | Long text | Raw extracted text |
| **Normalized Value** | Long text | Parsed/normalized for display |
| **Evidence Text** | Long text | **Required** — verbatim quote from source |
| **Page Number / Section / URL Anchor** | Single line | e.g. `Item 6, p. 42` or `#development-fees` |
| **Source Type** | Lookup or single select | Mirror from Source Record |
| **Source Quality** | Lookup or single select | Mirror from Source Record |
| **Confidence Score** | Number (0–100) | Algorithmic; not sole publish gate |
| **Confidence Level** | Single select | High, Medium, Low |
| **Extraction Type** | Single select | Directly Stated, Inferred, Needs Confirmation |
| **Public Visibility** | Single select | Public, Internal Only, Restricted |
| **Human Review Status** | Single select | Pending, Approved, Edited, Rejected, Needs More Source |
| **Approved Value** | Long text | Value approved for publish (may differ from extracted) |
| **Reviewer Notes** | Long text | |
| **Data Gap?** | Single select | Yes, No |
| **Follow-up Question** | Long text | For Helena outreach |
| **Last Updated** | Date | |
| **Extraction Run ID** | Single line | Correlates batch/server run |
| **Reviewed By** | Collaborator / Users link | |
| **Reviewed At** | Date | |

---

## Standard gap copy

When extraction finds no support for a requested field:

- **Extracted Value:** `Not confirmed in available sources.`
- **Data Gap?** = Yes
- **Human Review Status** = Pending (reviewer may approve gap as intentional)

---

## Explorer section catalog (Brand Explorer — PART 3 mapping)

Field registry keys should map to these sections (not all need MVP extraction):

| Explorer Section | Example field registry keys |
|------------------|----------------------------|
| Brand Identity | `basics.brandName`, `basics.parentCompany`, `basics.logo` |
| Brand Positioning | `presentation.overview.relative_positioning`, `basics.brandPositioning` |
| Parent Company | `basics.parentCompany` |
| Chain Scale | `basics.chainScale` |
| Brand Type | `basics.brandType` |
| Target Guest | `presentation.overview.typical_use_case`, `basics.targetGuest` |
| Target Owner Profile | `presentation.valueOwners.*` |
| Best-Fit Project Types | `projectFit.*` |
| New Build / Conversion / Resort / Urban / Airport / Mixed-Use Fit | `projectFit.newBuild`, `projectFit.conversion`, etc. |
| Franchise Economics | `feeStructure.*`, `dealTerms.*`, `presentation.economics.*` |
| Mandatory / Optional Fees | `feeStructure.*` |
| Training / Opening Support | `operationalSupport.*`, `presentation.operations.model.training` |
| Sales / Revenue / Distribution | `loyaltyCommercial.*`, `presentation.commercial.*` |
| Loyalty / Reservations / Technology | `loyaltyCommercial.*`, `presentation.loyalty.*` |
| Brand Standards Flexibility | `presentation.operations.flexibility.*`, `brandStandards.*` |
| Operating Model | `presentation.operations.model.*` |
| Staffing / Service Model | `operationalSupport.*` |
| F&B / Design / Room Requirements | `brandStandards.*`, `presentation.standards.*` |
| Regional Relevance | `footprint.specificMarkets`, `presentation.footprint.*` |
| Existing Footprint / Proof Points | `footprint.*`, `portfolioPerformance.*`, censusSummary |
| Brand Differentiators | `presentation.insight.*`, `basics.keyDifferentiators` |
| Deal Watchouts | `presentation.insight.watchouts` |
| Data Gaps | synthetic gap facts |
| Questions to Confirm | `followUpQuestion` on gap facts |
| Overall Source Confidence | rollup on publish (not single fact) |
| Last Reviewed Date | publish metadata |

---

## Explorer section catalog (Operator Explorer — PART 4 mapping)

**Canonical UI:** `operator-explorer-gold-mock.html` — **11 tabs** (9 core + Operator Materials + Dealality Insights). Tab → section mapping:

| Tab # | Tab name | Explorer sections for fact registry |
|-------|----------|--------------------------------------|
| 1 | Profile & Positioning | Company Snapshot, Ownership / Corporate Structure (partial) |
| 2 | Operating Platform | Pre-Opening, Transition, Conversion, Revenue, Sales, Digital, HR, Procurement, Technology |
| 3 | Brand & Relationships | Brand Relationships, Independent / Soft Brand Experience |
| 4 | Markets & Footprint | Regional Presence, Geographic Priorities, segment experience rows |
| 5 | Owner Engagement & Reporting | Owner Reporting, Budgeting, CAPEX, Performance Reviews, Engagement Cadence, Portals |
| 6 | Infrastructure & Data | Technology Stack (infra layer) |
| 7 | Leadership | Leadership |
| 8 | Project Fit & Deal Profile | Deal Fit Profile, Preferred Project Types, Min/Ideal Size (**preferences only — not OAS**) |
| 9 | Proof & Track Record | Portfolio proof, case studies, reference process |
| 10 | Operator Materials | Materials slots (`materials.*`) — link to Source Library |
| 11 | Dealality Insights | **Exclude** — Dealality-derived; not Partner Intelligence facts |

Optional tab when `?dealId=`: **Alignment Context** — **Exclude** (OAS).

| Explorer Section | Example registry keys |
|------------------|----------------------|
| Company Snapshot | `prefill.companyName`, `prefill.operatorType`, `explorerProfileJson` |
| Ownership / Corporate Structure | Governance fields |
| Leadership | `leadershipPlatform.*`, `leadershipTeamMembers.*` |
| Regional Presence | `prefill.activeCountries`, `prefill.activeMarkets` |
| Portfolio Size / Composition | `prefill.portfolioSize`, census footprint |
| Brand Relationships | `brandRelationships.*`, `brand_portfolio_mix_json` |
| Segment experience (Resort, Urban, Select, etc.) | `op_*_json`, `mkt_*_json`, `chainScalesSupported` |
| Pre-Opening / Transition / Conversion | `op_preopening_transition_json`, `op_conversion_repositioning_json` |
| Revenue / Sales / Digital | `op_commercial_engine_json` |
| Accounting / HR / Procurement | Operating platform child rows |
| Technology Stack | `infrastructurePlatform.*` |
| Owner Reporting / Budgeting / CAPEX | `op_owner_reporting_json`, engagement reporting |
| Owner Engagement / Portals / Reference | `engagementReporting.*` |
| Deal Fit Profile | `bf_*_json` (**factual preferences only — not OAS scores**) |
| Differentiators / Watchouts / Gaps / Questions | narrative JSON + gap facts |
| Overall Source Confidence | publish rollup |
| Last Reviewed Date | publish metadata |

**Critical:** Do not write OAS alignment scores or bands into Extracted Facts or Published fields.

---

## Publishing gate (reference)

See [partner-intelligence-repository-mvp-plan.md](./partner-intelligence-repository-mvp-plan.md) §9.

---

## API mapping

`MAP_PARTNER_FACT` in `api/lib/partner-intelligence-field-map.js`.
