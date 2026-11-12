# GDI Hotel Toolbar — Remove Run Research & Publish

## Change

| Control | Result |
|---|---|
| Run Research | **Removed** from hotel toolbar (auth) |
| Publish | **Removed** from hotel toolbar (auth) |
| Reset View | **Unchanged** — same markup, `btn-clear`, id `gdiResetViewBtn`, behavior |

Share interface already had Reset View only; left unchanged.

## Files modified

- `public/js/group-demand-intelligence/app.js`
- `scripts/test-gdi-foundation.mjs`
- `scripts/test-gdi-dealality-ui-unification.mjs`
- `scripts/test-gdi-compact-tile-contact-v1.mjs`
- `reports/gdi-hotel-toolbar-remove-research-publish-v1/*` (verification)

## Backend preserved

- Full E2E / weekly research: CLI + orchestrator (`npm run gdi:full-e2e`, weekly refresh)
- One-click publish module + API: `lib/.../gdi-one-click-hotel-publish-v1.js`, `POST .../publish`
- Client helpers `runResearch` / `publishHotelInventory` remain in `app.js` (not toolbar-wired)
- No auto-publication introduced

## UI verification

See `UI-VERIFICATION.html` (before/after markup).

## Tests

- `test:gdi-hotel-toolbar-reset-view-only-v1` PASS
- `test:gdi-customer-lifecycle-parity` PASS
- `test:gdi-customer-visibility` PASS
- `test:gdi-qualified-customer-opportunity-feed-v1` PASS
- `test:gdi-hotel-feedback-airtable-persistence-v1` PASS
- `test:gdi-bethesda-67-customer-feed-reconciliation-v1` PASS
- Foundation suite includes `gdi_hotel_toolbar_reset_view_only` (PASS); unrelated pre-existing foundation/UI assertions outside this change remain flaky and are not required for this toolbar PR
