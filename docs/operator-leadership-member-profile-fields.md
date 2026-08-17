# Leadership Team Members — profile detail fields

Child table: **Operator Setup - Leadership Team Members**

Used on **Leadership Profiles** (Operator DNA / Explorer) as structured blocks under each executive card. **Summary** and **bio** remain the narrative layer; these fields power the chip/grid detail panel.

## Airtable columns

| Column (`snake_case`) | Type | Setup form (`exec_N_*`) |
|----------------------|------|-------------------------|
| `hospitality_experience_years` | Number (1 decimal) | `exec_N_hospitality_experience_years` |
| `company_tenure_years` | Number (1 decimal) | `exec_N_company_tenure_years` |
| `prior_background` | Single line text | `exec_N_prior_background` |
| `languages` | Multiple select | `exec_N_languages` |
| `market_experience` | Multiple select | `exec_N_market_experience` |
| `core_expertise` | Multiple select | `exec_N_core_expertise` |
| `relevant_asset_types` | Multiple select | `exec_N_relevant_asset_types` |

Allowed select options are defined in `api/lib/operator-leadership-member-map.js` (`LEADERSHIP_MEMBER_SELECT_OPTIONS`). Writes filter to allowed values only.

## Create columns in Airtable

```bash
node scripts/ensure-operator-leadership-member-profile-fields.mjs --apply
```

Requires Meta API access (`schema.bases:write`).

## Populate / backfill profile data

**All existing child rows** (fills only empty fields; infers from name/title/role):

```bash
node scripts/backfill-operator-leadership-team-members.mjs          # dry-run + report
node scripts/backfill-operator-leadership-team-members.mjs --apply    # write to Airtable
node scripts/backfill-operator-leadership-team-members.mjs --operator-id recXXXXXXXX --apply
```

**Single operator — known demo names** (legacy sample map):

```bash
node scripts/seed-operator-leadership-member-profile-data.mjs --operator-id recXXXXXXXX
```

## Code map

| Layer | Module |
|-------|--------|
| Field map + options | `api/lib/operator-leadership-member-map.js` |
| Browser options / Setup HTML inject | `public/js/operator-leadership-member-map.js` |
| Read (detail API) | `mapNewBaseLeadershipForDetail` in `api/lib/operator-setup-new-base-read.js` |
| Write (save) | `buildLeadershipRows` in `api/lib/operator-setup-new-base-writer.js` |
| Explorer card HTML | `public/js/operator-leadership-profile-detail.js` |
| Setup inject + prefill | `public/js/operator-setup-explorer-behavior.js` |

## UI behavior

- **DNA profile:** Profile detail always shown when any structured field is populated.
- **Setup:** Profile detail block is injected under each executive repeater row (before headshot). Cloned rows include the same fields.

## Tab-level vs per-leader

| Tab subsection | Per-leader field |
|----------------|------------------|
| Language & Regional Capability (team) | `languages` |
| Team Experience Markets (team) | `market_experience` |
| Leadership Snapshot KPIs | Rollups from `hospitality_experience_years` / team data |

Both layers are intentional: team bench vs named executive accountability.
