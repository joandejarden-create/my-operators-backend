# FOUNDER REPORT — Customer GDI filter cleanup

## Verdict

Customer-facing GDI top-level filters restored to **All / Ready / Watching**. Pursuit-management states are no longer top-level nav. Pursuit model, records, panel, and Start/View Pursuit actions remain.

## What changed

- Shared `workflowPresetHtml` now emits only All · Ready · Watching (chain-scale legend styling).
- Pursuit filter branches removed from customer `filterOpportunities` workflow path; stale pursuit filter state falls back to All.
- Cache-bust on GDI HTML assets so browsers load the cleanup.

## What did not change

- Ready / Watch qualification gates
- Pursuit data model / store / APIs / statuses
- Start Pursuit / View Pursuit / pursuit panel HTML
- Seeded 5 pursuits (AC ×2, Radisson ×3) — follow-ups, hotel-selection, drafts intact on disk

## Browser check

| Hotel | Top-level filters | Pursuit nav chips |
|-------|-------------------|-------------------|
| AC Hotel A Coruña | All / Ready / Watching | Absent |
| Radisson Santo Domingo | All / Ready / Watching | Absent |
| YOTEL Geneva Lake | All / Ready / Watching | Absent |
| Bethesda Marriott | All / Ready / Watching | Absent |

Bethesda customer list still shows **28** opportunities (All). YOTEL list still loads Ready + Watching cards under the shared surface.

## Note

Running localhost process returned `API route not found` for `/pursuits` — server binary may predate pursuit route registration. Repo routes and filesystem pursuit records are intact; restart the Node server to exercise live pursuit HTTP if needed. This cleanup did not modify backend APIs.
