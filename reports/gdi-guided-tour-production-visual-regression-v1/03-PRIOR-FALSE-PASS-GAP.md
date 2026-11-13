# Why prior guided-tour certification passed (false PASS)

## Gap summary

Prior suites certified **navigation**, not **rendered layout**.

A tour where Next/Back remain clickable can still present a blank dark viewport and a mid-page drawer. That is exactly what production step 6 exhibited.

## Checks that passed incorrectly

| Prior check | What it asserted | What it missed |
|-------------|------------------|----------------|
| `test:gdi-guided-report-tour-v1` | Source contract, step titles, version string | No browser pixels / geometry |
| `test:gdi-guided-tour-drawer-navigation-browser-v1` | Step index advances; Next hittable; drawer “open” | Drawer `y`, highlight height, blank scrim coverage |
| Complete-app release certificate (#250) | Share chrome / package / inventory | Did not fail on tour overlay geometry |
| Screenshots in drawer-nav reports | Captured for humans | Not evaluated by assertions (no screenshot-diff / overlay geometry gates) |

## Route / environment gaps

- Some runs used localhost founder preview or local share while production Railway still served **`gdi-howto-read-v1-1e`**.
- Backend merge SHA matching was treated as “assets match” — **false**. HTML/JS/CSS SHA-256 must be compared explicitly (see `02-ASSET-HASH-COMPARISON.json`).
- Viewport often 1440×900 (same as repro) — failure is not viewport-specific; assertions simply ignored overlay geometry.

## Required gate going forward

`npm run test:gdi-guided-tour-visual-acceptance-v1` must fail when:

- Drawer steps have `drawer.y > 40` (not pinned to right rail).
- Highlight cutout for step 6 is shorter than ~55% of viewport height.
- Coachmark overlaps the drawer rail.
- Center of viewport is blank tour-scrim with no understandable report/drawer.

**Do not reuse prior PASS without re-running this visual acceptance against the live Railway share URL after deploy.**
