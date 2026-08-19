# AI Visibility — Airtable schema (Phase 2D LIVE)

> **Status:** Applied 2026-08-13 on product base (`AIRTABLE_BASE_ID`).  
> Ensure: `npm run ensure:ai-visibility-schema` (dry-run) · apply requires `AI_VISIBILITY_SCHEMA_APPLY=true`  
> Seed: `npm run ai-visibility:seed-prompts` · apply requires `AI_VISIBILITY_PROMPT_SEED_APPLY=true`  
> Raw runs/responses remain outside Airtable (storage abstraction).

Canonical entity tables are **not** modified. Link fields point *to* Brand Basics / Operator Master.

## Durable regional cohort rule

**Headline regional AI Presence** (CALA / Europe / North America) uses prompts with:

- `Geography Scope = Region`
- matching `Commercial Region`

Country prompts (`Geography Scope = Country`) are **separate cohorts** and are **not** auto-rolled into headline regional metrics.

Global uses `Geography Scope = Global` only (never an average of regions).

## Table: `AI Visibility - Prompts` (`tblsQyfNuNPkSR2G1`)

| Field | Type | Options / notes |
|-------|------|-----------------|
| Prompt Name | singleLineText | Primary |
| Prompt ID | singleLineText | Stable id |
| Prompt Family | singleLineText | Cross-geo family key |
| Prompt Text | multilineText | |
| Version | singleLineText | Material wording → new Version row |
| Intent Territory | singleSelect | Brand Selection, Operator Selection, Conversion, New Build, HMA vs Franchise, Owner Economics, Owner Flexibility, Branded Residences, Mixed Use, Market / Geography, Chain Scale / Positioning, Development Strategy, Other |
| Stakeholder Relevance | multipleSelects | Brand, Operator, Owner, Admin |
| Entity Scope | singleSelect | Brand, Operator, Both |
| Geography Scope | singleSelect | Global, Region, Subregion, Country, Market |
| Commercial Region | singleSelect | CALA, Europe, North America, APAC, Middle East & Africa |
| Subregion | singleLineText | e.g. Caribbean |
| Country | singleLineText | Canonical display name |
| Country Code | singleLineText | Optional ISO alpha-2 |
| Market | singleLineText | |
| Geography Model Version | singleLineText | e.g. ai_visibility_geography_v1 |
| Peer Set ID | singleLineText | Config id (not Airtable peer table) |
| Chain Scale | singleLineText | |
| Asset Type | singleLineText | |
| Hotel Type | singleLineText | |
| Development Type | singleSelect | New Build, Conversion, Either, Unspecified |
| Branded Residences Relevance | checkbox | |
| Decision Stage | singleSelect | Early Evaluation, Shortlist, Contracting, Unspecified |
| Active | checkbox | |
| Monitoring Eligible | checkbox | |
| Cadence | singleSelect | Weekly, Monthly, Ad-hoc, Paused |
| Governance Status | singleSelect | Draft, Approved, Paused, Retired |
| Review Status | singleSelect | Not Reviewed, Needs Review, Reviewed, Deferred |
| Review Notes | multilineText | |
| Source / Rationale | multilineText | |
| Last Monitored At | dateTime | |
| Linked Brands | multipleRecordLinks → Brand Setup - Brand Basics | Optional |
| Linked Operators | multipleRecordLinks → Operator Setup - Master | Optional |

### Proposed provenance fields (not applied)

V1 activation **did not write** Airtable schema or prompt rows (`FIELDS_ADDED = 0`, `ROWS_CHANGED = 0`). File-store overlay is the live provenance source (`fixtures/ai-visibility/prompt-provenance-v1.json`). `ensure:ai-visibility-schema` skips these (`schemaApply: false`). Large/raw demand evidence stays out of Airtable.

**Operational (proposed, not applied)** — short SSOT for origin, parent linkage, and compact provenance. Scenario linkage remains Prompt Family + `fixtures/ai-visibility/scenario-registry-v1.json` (no new Scenario ID field this phase).

| Field | Type | Notes |
|-------|------|--------|
| Prompt Origin | singleSelect | OBSERVED, DERIVED, SCENARIO, LEGACY_UNCLASSIFIED |
| Origin Source Type | singleSelect | Observed source class — not AI citation source |
| Observed Theme | singleLineText | Normalized observed theme |
| Demand Tier | singleSelect | HIGH, MEDIUM, LOW, UNKNOWN — no invented volume |
| Demand Geography | singleLineText | Source country/language; not Brand AI filter geography |
| Date Observed | date | |
| Derived From Observed Prompt ID | singleLineText | Observed parent for DERIVED |
| Provenance Status | singleSelect | VALIDATED, CANDIDATE, NEEDS_EVIDENCE, LEGACY |

**Deferred (file store only)** — do not add blindly: Origin Source Name, Origin Source Reference, Observed Query, Demand Signal Type, Demand Evidence Count, Demand Methodology, Derived From Demand Signal ID, Owner Intent Subtheme, Provenance Notes, Created By Method, Last Provenance Review At, Sampling Priority.

See [ai-visibility/prompt-provenance-observed-demand.md](./ai-visibility/prompt-provenance-observed-demand.md).

## Table: `AI Visibility - Opportunities` (`tblGAoMaPqHwlYtyM`)

| Field | Type | Options / notes |
|-------|------|-----------------|
| Opportunity Name | singleLineText | Primary |
| Opportunity ID | singleLineText | |
| Entity Type | singleSelect | Brand, Operator |
| Linked Brand | multipleRecordLinks → Brand Basics | |
| Linked Operator | multipleRecordLinks → Operator Master | |
| Intent Territory | singleSelect | Same taxonomy as Prompts |
| Geography Scope | singleSelect | Global, Region, Subregion, Country, Market |
| Commercial Region | singleSelect | CALA, Europe, North America, APAC, Middle East & Africa |
| Subregion | singleLineText | |
| Country | singleLineText | |
| Market | singleLineText | |
| Peer Set ID | singleLineText | |
| Observation Window | singleLineText | |
| Observation | multilineText | Evidence summary |
| Competitor Leader | singleLineText | |
| Current Presence | singleLineText | |
| Competitor Presence | singleLineText | |
| Evidence Descriptor | singleSelect | Repeated across engines/runs, Emerging, Single-engine |
| Evidence Store Refs | multilineText | Non-Airtable IDs |
| Diagnostic Reason | singleSelect | Persistent Absence, Competitor Dominance, Source Gap, Representation Gap, Visibility Loss/Gain, Other |
| Recommended Action | multilineText | Human-owned |
| Interpretation Status | singleSelect | Evidence Only, Needs Review, Human Confirmed, AI-Assisted Draft |
| Status | singleSelect | New → Closed lifecycle |
| Human Review Status | singleSelect | |
| Rule Version | singleLineText | |
| Metric Version | singleLineText | |
| Evidence Count | number | |
| Engines Observed | singleLineText | |
| First Detected | dateTime | |
| Last Detected | dateTime | |
| Resolved At | dateTime | |

**Opportunity records seeded:** 0 (workflow structure only).

## Field map source

`lib/ai-visibility/airtable-schema-proposal.js`  
Live snapshot: `reports/ensure-ai-visibility-schema.json`
