# Highlight Changes toggle — Comparison Studio

**Status:** PASS (browser-verified Curio; unit tests green)

## Permanent URL
http://127.0.0.1:8082/brand-explorer-compare.html?beInternalPreview=1

## Behavior
- Toggle sits on the **RIGHT · PROPOSED** column head (with Added / Modified / Removed / Incomplete legend).
- **Default ON** (persisted in `localStorage` key `becs-highlight-changes`).
- **ON:** unit-level markers on the proposed column only (added / modified / incomplete) + non-destructive Removed indicators that jump to CURRENT.
- **OFF:** all highlight chrome hidden via CSS (`body.becs-hl-off`) — proposed profile renders as the finished customer presentation.
- Toggle does **not** rebuild the DOM → scroll, pairing, and geometry preserved (verified scrollTop unchanged).
- Independent of **Changes only** filter (filter = which subsections show; toggle = how changes are marked).
- Unchanged retained content is never annotated.
- Incomplete / `RESEARCH_REQUIRED` text is marked **Incomplete**, not Added/Modified.
- Gate 1 Renderer Parity hides the toggle (identical columns).

## Matching
Semantic units: proof/scenario cards, list bullets, table rows, images, clusters/metrics.
Stable identity: title/type, gallery ordinal for images, content fingerprint (includes image basenames).

## Evidence screenshots
- `screenshots/01-overview-text-ON.png` — DEVELOPMENT & POSITIONING modified outline + badge
- `screenshots/02-overview-text-OFF.png` — same section without highlight chrome
- `screenshots/03-materials-images-ON.png` — gallery slots MODIFIED badges
- `screenshots/04-materials-images-OFF.png` — clean customer gallery view

## Tests
```bash
node scripts/test-be-compare-highlight-changes-v1.mjs
```

## Governance
No Airtable writes · no publication · no content regeneration · Curio/Ascend not approved.
