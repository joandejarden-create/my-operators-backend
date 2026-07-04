# Operator Capability inputs (P0)

Canonical fields for **Operator Capability Snapshot** (deal-only Phase 1).

## Airtable

| Table | Field | Type |
|-------|-------|------|
| Deals | Current Operating Model | singleSelect |
| Deals | Opening / Transition Phase | singleSelect |
| Deals | Project Type | singleSelect (+3 options) |
| Location & Property | Primary Market Region | singleSelect |
| Strategic Intent | Preferred Future Operating Model | singleSelect |
| Strategic Intent | Operator Strategy Status | singleSelect |
| Strategic Intent | Operator Capability Priorities | multipleSelects |
| Strategic Intent | Owner Reporting Package | multipleSelects |
| Strategic Intent | Owner Reporting Frequency | singleSelect |

Create fields: `node scripts/ensure-operator-capability-input-fields.mjs` (requires `schema.bases:write`).

## Project Type (canonical)

- New Build
- Conversion / Reflag
- Renovation / Repositioning
- Existing Operating Hotel
- Adaptive Reuse
- Mixed-Use Hospitality Project
- Other / To Be Confirmed

Not used: `Land / greenfield only` (use **New Build**), `Acquisition of operating hotel` (future **Deal Situation** field).

Legacy values remain readable via `lib/project-type.js` normalization.

## Snapshot API (v1)

- `GET /api/deals/:dealId/operator-capability-snapshot` — advisor-facing payload (`snapshotStatus`: `allowed` | `limited` | `blocked`)
- `POST /api/ai/operator-capability-snapshot` — same payload (legacy clients)

UI: My Deals → **View Operator Capability Snapshot** action (modal) per deal row. Full page: `/operator-capability-snapshot.html?dealId=rec…`

Tests: `node scripts/test-operator-capability-snapshot-v1.mjs`

## Legacy (retained)

- Plan to Self-Manage or Hire Third Party?
- Services Required From Operator
- Preferred Reporting Frequency
- Decision Timeline for Brand/Operator
- Is the hotel currently managed by a third-party operator?

## Scripts

- `scripts/backfill-operator-capability-inputs.mjs` — infer P0 from legacy; `--dry-run`, `--force`
- `scripts/audit-operator-capability-field-fill.mjs` — fill rates and QA flags

## Code

- `lib/operator-capability-inputs.js` — options, scope helper
- `api/schemas/deal-setup-fields.js` — routing, required sections, conditional required
