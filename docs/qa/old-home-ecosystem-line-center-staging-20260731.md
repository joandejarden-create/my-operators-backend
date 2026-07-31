# Old Home ecosystem — dashed connectors + centered bottom rows (staging)

## Change
- CSS: `dealality-old-home-ecosystem.v20260731j.css`
- CDN: `https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6ce7a987c3a9f4525c030b_dealality-old-home-ecosystem.v20260731j.css`
- Wired only in Old Home page-head freeform (page `68108c2a063eeb5d1bd7ae90`)

## Fixes
1. **Line style:** connector lines use `1.5px dashed` to match `.oh-eco-core` (and other Old Home dashed structure lines), replacing solid gradient + chunky triangles.
2. **Bottom connector row centered on the box:** each connector child is full column width with centered label/line so “Responses” aligns with “Opportunity Context”.
3. **Closing panel centered on the box:** `#oh-eco-close` uses `justify-content:center; align-items:center` (group left/right inset equal).
4. Process strip border matched to soft white card/close treatment (`rgba(255,255,255,.08)`).

## Publish
- Staging-only: `publishToWebflowSubdomain: true`, `customDomains: []`
- Site `lastPublished`: `2026-07-31T18:23:05.309Z`
- Custom domains remain at `2026-07-31T18:09:46.822Z` (production unchanged)

## QA
Artifacts: `/opt/cursor/artifacts/ecosystem-line-center-qa-20260731/`
- `audit.json`, `close-measure.json` — dashed borders confirmed; close `centeredDelta: 0`
- Screenshots: stage, connectors, process, close
