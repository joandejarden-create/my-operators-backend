# Engagement & Reporting — Explorer Airtable fields (legacy doc)

**Canonical storage:** `Operator Setup - Engagement & Reporting` child table — see `docs/operator-engagement-reporting-explorer-airtable-fields.md`.

**Legacy (transition):** `ov_*_json` on `Operator Setup - Commercial Fit & Terms` — still mirrored on read; do not add new JSON fields.

## Subsections (JSON in long text)

| Prefill key | UI subsection | JSON shape |
|-------------|---------------|------------|
| `ov_strategic_owner_value_json` | Strategic Owner Value | `[{ title, description }]` |
| `ov_engagement_cadence_json` | Owner Engagement Cadence | `[{ cadence, engagementType, focus }]` |
| `ov_controls_governance_json` | Controls & Governance | `[{ title, description }]` |
| `ov_reports_received_json` | Reports Owners Receive | `[{ title, description }]` |
| `ov_owner_tools_json` | Owner Tools & Support Channels | `[{ title, description }]` |
| `ov_lifecycle_support_json` | Owner Support Across the Asset Lifecycle | `[{ stage, support }]` |

## Scripts

```bash
node scripts/ensure-operator-engagement-explorer-schema.mjs --apply
node scripts/seed-operator-engagement-explorer-data.mjs --apply
```
