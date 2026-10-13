# GDI Weekly Newness UI

## Surfaces

- Authenticated: `public/js/group-demand-intelligence/app.js` + `dealality-gdi-ui.js`
- Share: `public/js/group-demand-intelligence/share-app.js` (same chrome)
- Styles: `public/css/group-demand-intelligence.css`

## Pills

| Pill | When |
|---|---|
| NEW | `isNewThisWeek` / `weeklyDeltaState=NEW` |
| UPDATED | `weeklyDeltaState=UPDATED` |
| REACTIVATED | `weeklyDeltaState=REACTIVATED` |

Each pill includes a small status dot (not icon-only).

Secondary meta line: `Added Sep 21` / `Updated Sep 21` from `firstSeenAt` / `lastMaterialChangeAt`.

## Filters (“This Week” row)

ALL · New This Week · Updated · Reactivated · High Priority  

Default: ALL  

Filter key: `filters.weekly` via `data-gdi-weekly`.

## Header summary

Non-zero only:

`X new this week · Y updated · Z reactivated`

Rendered above browse presets.

## Data contract

Opportunities may carry:

- `weeklyDeltaState`
- `isNewThisWeek`
- `firstSeenRunId` / `lastSeenRunId`
- `lastMaterialChangeAt` / `lastMaterialChangeRunId`
- `weeklyChangedFields[]`

No Bethesda-specific UI branches.
