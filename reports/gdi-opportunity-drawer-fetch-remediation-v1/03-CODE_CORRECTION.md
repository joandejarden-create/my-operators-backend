# Code correction

## Frontend

- `public/js/group-demand-intelligence/app.js`
  - `detailUrl` appends `&preview=staged` when founder staged preview is active (same as list).
  - Drawer loads **detail first**; decision/pursuit enrichments use a 2.5s race timeout.
  - Stale-open sequence prevents older responses from replacing a newer drawer.
  - Network `fetch` failures map to a friendly message (`GDI_NETWORK_FETCH_FAILED`), not raw "Failed to fetch".
- `public/js/group-demand-intelligence/gdi-universal-presentation-controller.js`
  - Friendly mapping for failed-to-fetch / abort / network errors.
  - Ignore `STALE_DETAIL_RESPONSE`.

## API

- `api/group-demand-intelligence.js` → `getGdiOpportunityDetail`
  - Passes `preferStaged` + `preview: "staged"` when `?preview=staged`.
  - Returns 404 for `OPPORTUNITY_NOT_IN_PUBLICATION_PACKAGE` without live-store hang.

## Cache markers

- Auth `app.js` + presentation controller: `gdi-drawer-fetch-remediation-v1`
- UI marker (`dealality-gdi-ui.js`) unchanged: `gdi-segment-class-preview-ui-v1`

## Tests

- `npm run test:gdi-opportunity-drawer-fetch-remediation-v1` (new permanent Full E2E gate)
- Updated staged-preview + recovery + no-mode-banner cache asserts
