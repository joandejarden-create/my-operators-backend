# Engagement & Reporting — Explorer Airtable fields



**Table:** `Operator Setup - Engagement & Reporting` (child rows, linked to Master)  

**Prefill:** `engagementReporting` / `engagementPlatform` on detail API; legacy `ov_*_json` mirrored on read for transition.



## Child table columns



| Airtable field | Role |

|----------------|------|

| `Operator` | Link → Operator Setup - Master |

| `section` | Subsection (single select) |

| `row_key` | Stable key (cards, signals) |

| `display_order` | Sort order |

| `title` | Primary label |

| `subtitle` | Cadence (Weekly, Monthly, …) |

| `body` | Description / focus / card copy |

| `extra` | Signal value (reporting level, governance cadence) |



## Section values



- Strategic Owner Value

- Engagement Cadence

- Controls & Governance

- Reports Received

- Owner Tools

- Lifecycle Support

- Owner Value Card (from Setup value-card textareas)

- Optional Cluster

- Engagement Signal



## Scripts



```bash

node scripts/ensure-operator-engagement-reporting-table.mjs --apply

node scripts/migrate-operator-engagement-to-reporting-table.mjs

node scripts/migrate-operator-engagement-to-reporting-table.mjs --apply

node scripts/migrate-operator-engagement-to-reporting-table.mjs --apply
node scripts/restore-he-cala-engagement.mjs
```

**HE CALA:** `fixtures/operator-engagement-explorer-he-cala.json`

## Setup UI



- Repeaters: `public/js/operator-engagement-reporting-setup.js`

- Owner value cards (`ov_card_*`) remain on the main form; saved as child rows on submit.


