# Pre-publish refinement return — Many Futures

**Status:** Ready for review. **Not published.**  
**Branch:** `cursor/many-futures-prepublish-refine-38e7`  
**Page:** Old Home `/old-home` · HtmlEmbed `faf5dede-519c-f93e-9c82-66cf832f1bf7`

## Delivery note (HtmlEmbed)

Webflow MCP could not accept the full ~34.5KB inline Code Embed in a single `set_settings` call from this agent environment. The HtmlEmbed stores a **967-character CDN loader** that fetches and injects the identical production HTML:

`https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6bc99ef37c6123f1f077b9_mf-v3b-embed.txt`

Loader correctly re-creates `<style>`, markup, and `<script>` nodes so interaction JS runs. Full source of truth remains `webflow-embeds/many-futures-section.html`.

## Screenshots

1. Desktop Preview: `/opt/cursor/artifacts/screenshots/mf-webflow-preview-desktop-1440.png`
2. Tablet Preview: `/opt/cursor/artifacts/screenshots/mf-webflow-preview-tablet-768.png`
3. Mobile Preview: `/opt/cursor/artifacts/screenshots/mf-webflow-preview-mobile-390.png`
4. Desktop New Operator close-up: `/opt/cursor/artifacts/screenshots/mf-webflow-preview-new-operator-desktop-closeup.png`
5. Mobile New Operator close-up: `/opt/cursor/artifacts/screenshots/mf-webflow-preview-new-operator-mobile-closeup.png`
6. Hotel candidates:
   - Recommended: `/opt/cursor/artifacts/assets/hotel-candidate-1-recommended.png`
   - Strong alt: `/opt/cursor/artifacts/assets/hotel-candidate-2-strong-alt.png`
   - Secondary: `/opt/cursor/artifacts/assets/hotel-candidate-3-secondary.png`

Designer live Preview MCP timed out; validation used a parity page with the **same CDN loader + production HTML** as the Webflow HtmlEmbed.

## Confirmations

7. **Displayed data fields exist in Dealality** — Operator: name, positioning, HQ, years, hotels, rooms, asset focus, brand mix/relationships from Operator Explorer. Other paths use Brand Explorer / Fee Estimator / Radar / Opportunity Review fields only.
8. **No unavailable functionality implied** — no invented scores, recommendations, or actions beyond existing Save / Request introduction chrome in the desktop Operator crop; mobile uses presentation cards labeled `INTERFACE SIMPLIFIED FOR PRESENTATION`.
9. **Development-only text absent** from production embed and Preview parity body (no Local prototype / Phase / PR / placeholder notes).
10. **`#platform-features` unchanged** — element `43d3da86-f5ea-ff1d-3222-8df4b218ee91` still present with `id=platform-features`; Features stand-in in parity page is local-only.
11. **Final Embed character count:** **34,478** (production HTML on CDN / `many-futures-section.html`). HtmlEmbed loader: **969**.
12. **Console / interaction tests** (parity Preview):
    - Console: favicon.ico 404 only (local static server); no embed errors
    - All 5 previews switch: pass
    - Hover: pass · Click pin: pass · Keyboard focus: pass · Enter: pass · Space: pass
    - Mobile selection placement: pass
    - Horizontal overflow @1440 / @390: none
    - Composition width @1440: **1232px** (~10% over prior 1120)

## Hotel image

Courtyard **TEMPORARY IMAGE** remains installed until a candidate is approved. Do not upload final hotel until approval.

### Ranked candidates
1. **Recommended** — evening urban façade, clear canopy/entrance, ~7–8 floors, warm interior light, Caribbean/urban plausible, no brand signage as primary subject.
2. **Strong alternative** — see `hotel-candidate-2-strong-alt.png`
3. **Secondary alternative** — see `hotel-candidate-3-secondary.png`

## Temporary labels
- **ILLUSTRATIVE OPPORTUNITY** — kept
- **TEMPORARY IMAGE** — kept while courtyard image remains

## Do not publish without explicit approval.
