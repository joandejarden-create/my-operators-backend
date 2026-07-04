# Workspace KPI snapshots (brand + owner)

Weekly **My Deal Flow** / **Deal pipeline by stage** counts are computed from **Brand Deal Requests** (same Airtable table and status fields on both surfaces).

## Mirrored KPIs (same Brand Deal Request rows)

Counts only match when **both screens use the same BDR records** (same `id`s). Owner scope = all requests on the owner’s deals. Brand scope = requests for the signed-in brand (subset when one brand is logged in).

| Owner card | Brand card | Shared rule |
|------------|------------|-------------|
| **Awaiting brand** | **Brand action** | `isAwaitingBrand(row)` — ball with brand |
| **Owner action** | **Awaiting owner** | `isAwaitingOwner(row)` — ball with owner (`awaiting-info` or owner next step) |
| Outreach sent (7d) | Requests sent (7d) | `Request Sent At` in last 7 days |
| Stuck / at risk | Stuck / at risk | Overdue follow-up or 14+ days without activity (requires full BDR fields from API) |

Pipeline stages partition every row into exactly one bucket (`new`, `active-review`, `terms-proposal`, `nda-room`/`advanced`, closed, passed). Labels differ by persona (e.g. owner **Bid submitted** = brand **Terms & proposal** bucket).

### Multi-owner / multi-brand

- **Do not** compare owner portfolio totals to a single brand’s workspace unless you filter to that brand’s rows.
- KPI history scope keys include persona + filters: `v2|owner|…` vs `v2|brand|…`.
- Run `node scripts/test-workspace-kpi-mirror.mjs` after changing bucket or mirror rules.

### Audit in code

`auditWorkspaceKpiMirror(rows)` in `lib/deal-workspace-pipeline.js` returns violations if mirror or pipeline partition fails. The UI logs warnings in the browser console when a strip refresh detects a mismatch.

## History storage

1. **Preferred:** Airtable table `Workspace KPI Snapshots` (see `.env.example`).
2. **Fallback:** `data/brand-workspace-kpi-history.json` (local server file).

Scope keys: `v2|owner|…filters` and `v2|brand|…filters` so brand and owner histories do not collide.

API: `GET/POST /api/brand-workspace/kpi-history`

## Code

- `lib/deal-workspace-pipeline.js` — buckets, actions, snapshot math
- `public/js/deal-workspace-insights.js` — render + history sync
- `public/css/deal-workspace-insights.css` — shared strip styles
