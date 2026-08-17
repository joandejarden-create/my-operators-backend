# Leadership & Team — Explorer presentation fields (legacy JSON)

> **Preferred:** use child table **Operator Setup - Leadership Platform** — see `docs/operator-leadership-platform-child-table.md`.

Legacy long-text JSON on **Operator Setup - Governance, Delivery & Diligence** is still read as fallback when no child rows exist.

| Form key | Airtable column name | Type | UI section |
|----------|----------------------|------|------------|
| `lead_avg_hospitality_experience` | Leadership Avg Hospitality Experience | Single line | Leadership Snapshot KPI |
| `lead_org_structure_json` | Leadership Org Structure (JSON) | Long text | Organization Structure |
| `lead_team_depth_json` | Leadership Team Depth (JSON) | Long text | Team Depth by Function |
| `lead_language_capability_json` | Leadership Languages (JSON) | Long text | Language & Regional Capability |
| `lead_governance_cadence_json` | Leadership Governance Cadence (JSON) | Long text | Governance & Communication Cadence |
| `lead_team_markets_json` | Leadership Team Markets (JSON) | Long text | Team Experience Markets |
| `lead_owner_relationship_json` | Leadership Owner Relationship (JSON) | Long text | Owner Relationship Model |

Fixtures for draft content: `fixtures/operator-leadership-explorer-he-cala.json`
