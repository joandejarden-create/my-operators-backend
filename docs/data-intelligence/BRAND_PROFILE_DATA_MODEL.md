# Brand Profile Data Model

Standard conceptual structure for Dealality brand intelligence profiles.

This document defines the **target profile shape** for governance and extraction. Live Airtable field names live in schema docs such as `docs/*-airtable-fields.md` and Brand Explorer presentation mappings — do not invent fields from this list alone.

> **Important:** Do not call a brand profile Company Validated unless directly confirmed by the brand/company.

## Core Identity

| Field | Description |
|-------|-------------|
| Brand Name | Primary brand label |
| Parent Company | Corporate parent if applicable |
| Brand Family | Collection or family grouping |
| Chain Scale | Positioning tier (e.g. upscale, luxury) |

## Positioning and Asset Fit

| Field | Description |
|-------|-------------|
| Positioning | Brand positioning summary |
| Target Guest | Primary guest profile |
| Typical Asset Type | Resort, urban, select-service, etc. |
| Development Model | Franchise, managed, conversion-heavy, etc. |
| Conversion Relevance | Relevance to conversion opportunities |
| Conversion Requirements Summary | Key conversion themes |
| Brand Standards Themes | Standards that affect owner decisions |
| Owner Considerations | Owner-facing considerations |
| Agreement Structure Notes | High-level agreement themes (not legal advice) |

## Geography and Signals

| Field | Description |
|-------|-------------|
| Geographic Focus | Primary regions |
| CALA Presence | CALA-specific presence evidence |
| Fit Signals | Alignment-style signals for opportunities |
| Watchouts | Caution areas or gaps |

## Inputs and Sources

| Field | Description |
|-------|-------------|
| Required Owner Inputs | What an owner must supply for meaningful alignment |
| Relevant Source Materials | Linked sources used for this profile |

## Governance Metadata

| Field | Description |
|-------|-------------|
| Validation Status | Per [INTELLIGENCE_GOVERNANCE.md](./INTELLIGENCE_GOVERNANCE.md) |
| Usage Permission | Internal, display, scoring, external, etc. |
| Confidence Level | Overall or field-level where possible |
| Last Reviewed | Date of last human review |
| Refresh Due | Suggested refresh date |
| Company Validated | Yes only with direct confirmation |
| Company Validation Date | When confirmed |
| Evidence Notes | Source-backed notes |
| Missing Data Flags | Known gaps |

## Related Implementation (Existing)

- Brand Explorer UI: `public/js/brand-explorer*.js`, `api/brand-library.js`
- Presentation slots: [../brand-explorer-presentation-slots.md](../brand-explorer-presentation-slots.md)
- Factory runbook: [../brand-explorer-factory.md](../brand-explorer-factory.md)
- Partner extraction: [../partner-intelligence-repository-mvp-plan.md](../partner-intelligence-repository-mvp-plan.md)
