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

File-store overlay is the live provenance source (`fixtures/ai-visibility/prompt-provenance-v1.json`). Do **not** run `ensure:ai-visibility-schema` for these until a separate apply task. Large demand evidence stays out of Airtable.

| Field | Type | Notes |
|-------|------|--------|
| Prompt Origin | singleSelect | OBSERVED, DERIVED, SCENARIO, LEGACY_UNCLASSIFIED |
| Origin Source Type | singleSelect | Observed source class — not AI citation source |
| Origin Source Name | singleLineText | |
| Origin Source Reference | singleLineText | Short ref only |
| Observed Query | singleLineText | |
| Observed Theme | singleLineText | |
| Demand Tier | singleSelect | HIGH, MEDIUM, LOW, UNKNOWN — no invented volume |
| Demand Signal Type | singleLineText | |
| Demand Geography | singleLineText | |
| Date Observed | date | |
| Demand Evidence Count | number | |
| Demand Methodology | multilineText | Required before HIGH/MEDIUM/LOW |
| Derived From Observed Prompt ID | singleLineText | |
| Derived From Demand Signal ID | singleLineText | |
| Owner Intent Subtheme | singleLineText | Intent Territory remains the family |
| Provenance Status | singleSelect | VALIDATED, CANDIDATE, NEEDS_EVIDENCE, LEGACY |
| Provenance Notes | multilineText | |
| Created By Method | singleSelect | |
| Last Provenance Review At | dateTime | |
| Sampling Priority | singleSelect | Future repeated-testing hook; scheduler off |

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
