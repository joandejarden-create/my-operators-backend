# Final Publish Gate — Many Futures Image Quality & Live Embed

**Status:** Ready for final approval · **Do not publish**  
**Date:** 2026-07-31  
**Branch:** `cursor/many-futures-phase-b-38e7`  
**CDN asset commit:** `41c298b469ca7d225b0291026a64cabd7c03035f`  
**Embed pin commit:** `0ce9c65` (points HtmlEmbed at hashed assets on `41c298b`)

---

## 1–2. Residual closures

### Tablet 2× primary visuals

No 1600px+ native product screenshots exist for Brand Explorer, Operator Explorer, Opportunity Review, or Deal Compare (sources were 1024px). **Upscaling was not used.**

| Primary | Old approach | Old dims | New approach | New dims / type |
|---|---|---|---|---|
| Brand Explorer (Q1, Q4 support) | PNG crop | 1024×576 desk / 780×520 mob | HTML/CSS reconstruction | Vector — crisp at all DPR |
| Operator Explorer (Q2) | PNG crop | 1024×576 / 780×520 | HTML/CSS reconstruction | Vector |
| Opportunity Review (Q4) | PNG crop | 1024×576 / 780×520 | HTML/CSS reconstruction | Vector |
| Deal Compare (Q5) | PNG crop | 1024×576 / 780×520 | HTML/CSS reconstruction | Vector |
| Dealality Radar (Q3 primary + supports) | PNG from 1024 radar | 1024×576 / 780×520 | Re-export from `market-map.png` **without upscale** | **1600×900** desk + **800×450** support + **900×600** mobile; lossless WebP companions |
| Deal Readiness (Q6) | Already HTML | — | Unchanged | Vector |

Radar source: `assets/sources/market-map.png` **1678×1808** → desk 1600×900 (downscale only).

### Brand Explorer mobile

Replaced soft PNG with HTML/CSS Brand Positioning panel:

- Readable Positioning / Audience callouts (not microscopic body copy)
- Value scenarios: Iconic Urban Flagship, Resort & Leisure, Adaptive Reuse
- Labeled “Interface simplified for presentation”
- No invented fields/scores/actions
- Crisp at 390px and 320px (HTML text)

---

## 3. Default eager image weight

**Desktop 1440 default transfer ≈ 367 KB** (target &lt; 750 KB) ✅

Eager / default-state assets:

| Asset | Transfer |
|---|---|
| `hotel-final-640.webp` | ~128 KB |
| `smart-matching-desktop.webp` (lossless) | ~110 KB |
| `radar-desktop-800.webp` (lossless) | ~130 KB |
| Brand Explorer | **0** (HTML) |

**After visiting all six states (desktop):** ≈ **825 KB** total image transfer.

**Tablet 768 @2× default:** ≈ **707 KB** (hotel-960 + smart-matching + radar-1600 WebP).

Mechanisms:

- Inactive panels defer `src`/`srcset` until activation
- `<picture media>` so only the matching breakpoint asset is requested
- Radar `srcset` 800w/1600w sized for support vs primary
- No preload of all question states

---

## 4. External CSS / JS safety

| Check | Confirmation |
|---|---|
| Versioned CSS URL | `https://cdn.jsdelivr.net/gh/joandejarden-create/my-operators-backend@41c298b469ca7d225b0291026a64cabd7c03035f/webflow-embeds/many-futures/dist/many-futures.c1ca015ca9d6.css` |
| Versioned JS URL | `https://cdn.jsdelivr.net/gh/joandejarden-create/my-operators-backend@41c298b469ca7d225b0291026a64cabd7c03035f/webflow-embeds/many-futures/dist/many-futures.d57035f5774b.js` |
| Immutable filenames | Content SHA-256 prefix in filename (`c1ca015ca9d6`, `d57035f5774b`); build refuses to overwrite a hash with different bytes |
| Commit pin | jsDelivr `@41c298b…` tree is immutable (`Cache-Control: immutable`) |
| Mismatch prevention | New CSS/JS content → new hash filename; Embed must be updated to new URLs; old hashes remain valid |
| `defer` on script | Yes |
| Failed script | Semantic HTML for Q1 remains visible; critical inline CSS keeps panels readable |
| No-JS | Default Q1 content in DOM; inactive panels use `hidden` |
| FOUC | Tiny critical inline style before stylesheet link |
| CDN markup loader | Backup only (`cdn-loader.html`) |

---

## 5. Webflow Preview / live validation

### Designer embed (Data API) — confirmed inserted

HtmlEmbed `faf5dede-519c-f93e-9c82-66cf832f1bf7` contains:

- `mf-ui--brand-explorer`
- `many-futures.c1ca015ca9d6.css`
- `radar-desktop.webp`
- `Illustrative Opportunity`
- No `TEMPORARY`
- No `brand-explorer-mobile.png`

### `#platform-features` — untouched

Section `43d3da86-f5ea-ff1d-3222-8df4b218ee91` still present with `id="platform-features"`.

### Designer Preview screenshots

**Designer MCP / `element_snapshot_tool` unavailable** (connection timeout). Staging `mvp-deal-capture.webflow.io/old-home` still serves the **last published** loader (`mf-embed-slot`) until a site publish — **not performed**.

### CDN parity smoke (exact Embed + live jsDelivr)

Screenshots of the identical HtmlEmbed payload loading production CDN CSS/JS/assets:

| Viewport | File |
|---|---|
| Desktop 1440 | `/opt/cursor/artifacts/many-futures/publish-gate/webflow-cdn-parity-desktop-1440.png` |
| Tablet 768 @2× (Q3 Radar primary) | `…/webflow-cdn-parity-tablet-768.png` |
| Mobile 390 | `…/webflow-cdn-parity-mobile-390.png` |
| Brand Explorer mobile closeup | `…/gate-mobile-390-brand-explorer-closeup.png` |
| Tablet 2× Radar primary closeup | `…/gate-tablet-2x-primary-radar-closeup.png` |
| Local parity suite | `gate-desktop-1440-q1.png`, `gate-mobile-390-q1.png`, etc. |

Also under `webflow-embeds/many-futures/visual-review/publish-gate/`.

### Interactive smoke (local + CDN parity)

| Test | Result |
|---|---|
| Console errors | None material (CDN assets 200; no embed runtime errors) |
| Keyboard Tab / Enter / Space | Pass — `aria-pressed` updates for operators (Enter) and clarify (Space) |
| Hover preview + click pin | Pass (existing JS) |
| Reduced motion | Pass — `prefers-reduced-motion` honored |
| Horizontal overflow | None |
| TEMPORARY label in embed | Absent |
| ILLUSTRATIVE OPPORTUNITY | Present |
| Final hotel image | Loads (`hotel-final-*.webp`) |
| Blank panels on switch | None observed |
| `#platform-features` | Untouched |

---

## 6. Final publish-gate checklist return

1. **Desktop screenshot:** `webflow-cdn-parity-desktop-1440.png` (CDN Embed parity; Designer Preview MCP unavailable)
2. **Tablet screenshot:** `webflow-cdn-parity-tablet-768.png`
3. **Mobile screenshot:** `webflow-cdn-parity-mobile-390.png`
4. **Tablet primary closeup:** `gate-tablet-2x-primary-radar-closeup.png` (1600px Radar)
5. **Brand Explorer mobile closeup:** `gate-mobile-390-brand-explorer-closeup.png` (HTML)
6. **Old → new dims:** see tables above
7. **Default eager transfer:** **≈ 367 KB** desktop
8. **After all six states:** **≈ 825 KB** desktop
9. **CSS URL:** see §4
10. **JS URL:** see §4
11. **Cache/versioning:** content-hashed filenames + immutable commit pin; no in-place overwrite
12. **Console:** clean for Embed assets
13. **Keyboard:** Pass
14. **Reduced motion:** Pass
15. **Two residuals closed:** Yes (HTML primaries + Radar 1600 / Brand Explorer HTML mobile)
16. **`#platform-features` untouched:** Yes

---

## Stop

**Do not publish.** Awaiting final approval.  
To see Designer Preview in-browser, open Old Home Preview once Designer MCP/session is connected; Embed code is already updated in Designer.
