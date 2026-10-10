# FILTER TRACE — Customer GDI top-level nav

## Component

| Item | Location |
|------|----------|
| Shared UI module | `public/js/group-demand-intelligence/dealality-gdi-ui.js` |
| Filter preset renderer | `workflowPresetHtml()` |
| Filter application | `filterOpportunities()` → `filters.workflow` |
| Customer normalize helper | `normalizeCustomerWorkflowFilter()` |
| Wire-up / click handlers | `public/js/group-demand-intelligence/app.js` → `wireBrowseControls()` (`[data-gdi-workflow]`) |
| Render row | `renderBrowseBody()` → `.gdi-workflow-filter-row` |
| Styles | `public/css/group-demand-intelligence.css` (legend language + `.gdi-workflow-filter-row`) |
| Page shell | `public/group-demand-intelligence.html` (cache-bust `gdi-customer-filter-cleanup-v1`) |

## Filter enum / config (before)

```
All | Ready | Watching | Active Pursuits | Follow-Up Due | Hotel Selection | Closed
```

Values: `""`, `READY`, `WATCHING`, `ACTIVE_PURSUITS`, `FOLLOW_UP_DUE`, `HOTEL_SELECTION`, `CLOSED`

## Filter enum / config (after)

```
All | Ready | Watching
```

Values: `""`, `READY`, `WATCHING`

## Pursuit-specific additions (removed from nav)

- `ACTIVE_PURSUITS` — filtered by `pursuitId` / non-closed `pursuitStatus`
- `FOLLOW_UP_DUE` — filtered by `pursuitStatus === FOLLOW_UP_DUE`
- `HOTEL_SELECTION` — filtered by hotel-selection pursuit statuses
- `CLOSED` — filtered by closed / not-selected pursuit statuses

These branches were removed from customer filter application. Stale state values coerce to **All** via `normalizeCustomerWorkflowFilter`.

## Hotel-specific overrides

None. Change is shared for all GDI hotels (AC, Radisson, YOTEL, Bethesda, etc.).

## Route / query-state

Workflow filter is in-memory `state.filters.workflow` only (not a URL query param). Hotel selection remains `?hotelId=`.
