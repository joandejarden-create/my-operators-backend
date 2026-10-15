# Verification checklist

## Automated

- [x] `test-admin-ai-demand-table-v1.mjs`
- [x] `test-gdi-local-meetings-catering-research-v1.mjs`

## Local :8080 (authenticated admin)

- [ ] AI Demand Reviews — sort asc/desc, search, coverage + PDF filters, Reset View, selection, sticky Actions
- [ ] ADP Action Plan — load from Reviews Action plan handoff
- [ ] GDI Reports — Location column, typed sort, date range on Last Generated, sticky Actions not clipped, bulk CSV (safe), bulk PDF only with confirm
- [ ] Report Archive — version counts, location, Reset View, compact actions
- [ ] Smaller viewport / 200% zoom — toolbar wraps; table scrolls horizontally inside shell
- [ ] Keyboard — sortable headers focusable; icon buttons have aria-label
- [ ] My Deals + customer GDI — no code paths changed in those UIs

## Production (after clean-main deploy)

- [ ] Authenticated AI Admin tabs load `admin-ai-demand-table.js?v=ada-table-parity-v1`
- [ ] GDI Actions column reachable without page overflow
- [ ] Counts match prior catalog semantics (no invented zeros)

## Screenshots

Place before/after captures in `screenshots/` when available from authenticated local/prod sessions:

- `reviews-before.png` / `reviews-after.png`
- `gdi-before.png` / `gdi-after.png`
- `archive-before.png` / `archive-after.png`
- `action-plan-after.png`
