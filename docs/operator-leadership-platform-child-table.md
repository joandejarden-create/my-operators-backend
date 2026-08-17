# Leadership Platform — child table (replaces JSON long-text)

Explorer sections **Organization Structure**, **Team Depth**, **Languages**, **Governance Cadence**, **Team Markets**, and **Owner Relationship** are stored as **linked child rows**, not JSON blobs.

## Airtable table to create

**Table name:** `Operator Setup - Leadership Platform`  
**Link field:** `Operator` → **Operator Setup - Master** (same pattern as Leadership Team Members)

| Column | Type | Notes |
|--------|------|--------|
| `Operator` | Link to Master | Required |
| `section` | Single select | Options below — spelling must match exactly |
| `display_order` | Number | Row order within section |
| `title` | Single line | Primary label (varies by section) |
| `subtitle` | Single line | Secondary label |
| `body` | Long text | Description / relevance / support |
| `extra` | Long text | Comma-separated tags or leader names |
| `depth` | Single select | **Team Depth rows only:** `Strong`, `Very Strong`, `Moderate / Strong`, `Emerging / Strong` |

### `section` single-select options (exact)

- `Organization Structure`
- `Team Depth`
- `Language`
- `Governance Cadence`
- `Team Market`
- `Owner Relationship`

Override table name: `AIRTABLE_OPERATOR_SETUP_LEADERSHIP_PLATFORM_TABLE`

## Operator Setup form

**Tab 8 — Leadership & Team → Explorer presentation**

- Labeled repeater rows per section (no raw JSON).
- **Load HE CALA draft rows** fills from `public/fixtures/operator-leadership-explorer-he-cala.json`.
- Save writes `body.leadershipPlatform` → child table via intake API.

## Column meaning by section

| section | title | subtitle | body | extra | depth |
|---------|-------|----------|------|-------|-------|
| Organization Structure | Layer title | — | Description | Tags (comma-separated) | — |
| Team Depth | Function | Lead / bench | Owner relevance | — | Team depth |
| Language | Language | Proficiency | Owner support | — | — |
| Governance Cadence | Cadence title | — | Description | — | — |
| Team Market | Market | — | Team experience | Relevant leaders | — |
| Owner Relationship | Touchpoint | Lead / function | Description | — | — |

## Legacy JSON fields (deprecated for editing)

These Governance long-text columns are **fallback read-only** if no child rows exist:

- Leadership Org Structure (JSON)
- Leadership Team Depth (JSON)
- Leadership Languages (JSON)
- Leadership Governance Cadence (JSON)
- Leadership Team Markets (JSON)
- Leadership Owner Relationship (JSON)

Read path: child table first → legacy JSON on prefill → Explorer built-in defaults.

## Code map

- `api/lib/operator-leadership-platform-map.js` — field map + read/write
- `public/js/operator-leadership-platform-setup.js` — Setup form repeaters
- `public/js/operator-leadership-team-sections.js` — Explorer render

## HE CALA seed (after table exists)

1. Setup → Leadership & Team → **Load HE CALA draft rows** → Save  
2. Or import rows manually from `fixtures/operator-leadership-explorer-he-cala.json`
