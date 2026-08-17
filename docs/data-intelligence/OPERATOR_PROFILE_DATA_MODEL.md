# Operator Profile Data Model

Standard conceptual structure for Dealality operator intelligence profiles.

This document defines the **target profile shape** for governance and extraction. Live Airtable field names live in schema docs such as `docs/operator-*-airtable-fields.md` — do not invent fields from this list alone.

> **Important:** Operator profile data may be Company Published or Source-Informed before being Company Validated. Do not call a profile Company Validated unless directly confirmed by the operator/company.

## Core Identity

| Field | Description |
|-------|-------------|
| Company Name | Operating company name |
| Operating Model | Third-party, owner-operator, hybrid, etc. |
| Third-Party Management Availability | Whether and how third-party management is offered |

## Portfolio and Coverage

| Field | Description |
|-------|-------------|
| Owned / Managed Portfolio | Scale and composition |
| Regional Coverage | Regions of operation |
| CALA Experience | CALA-specific experience evidence |
| Brand Relationships | Brand affiliations and partnerships |

## Capability Profile

| Field | Description |
|-------|-------------|
| Chain Scale Experience | Experience across chain scales |
| Resort Experience | Resort operating depth |
| Urban Experience | Urban operating depth |
| F&B Complexity | F&B capability level |
| Pre-Opening Capability | Opening support |
| Technical Services | Technical/engineering services |
| Revenue Management | RM capability |
| Sales & Marketing | Sales and marketing capability |

## Owner Interface

| Field | Description |
|-------|-------------|
| Owner Reporting Style | Reporting approach |
| Management Structure Preference | Preferred deal structures |
| Relevant Deal Types | Deal types typically pursued |

## Signals and Sources

| Field | Description |
|-------|-------------|
| Fit Signals | Alignment-style signals for opportunities |
| Watchouts | Caution areas or gaps |
| Source Materials | Linked sources used for this profile |

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

- Operator Explorer UI: `public/js/operator-explorer*.js`
- Operator Setup intake: `public/third-party-operator-setup-new-two.html`
- Field mapping audit: [../operator-setup-to-explorer-field-mapping-audit.md](../operator-setup-to-explorer-field-mapping-audit.md)
- Operator Alignment Snapshot: [../operator-alignment-snapshot-phase-1.md](../operator-alignment-snapshot-phase-1.md)
- Schema docs: `docs/operator-*-explorer-airtable-fields.md`
