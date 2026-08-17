/**
 * Phase 1B Render Fix — diagnosis + fix notes
 *
 * 1. Why raw CSS became visible
 * CSS was stored in Webflow CodeBlock elements (`type: CodeBlock`), not Embed and not
 * page custom code. CodeBlocks render their `code` setting as visible page content,
 * so `<style id="oh-p1b">…` appeared as literal text on the canvas.
 *
 * 2. Broken / incorrect elements (pre-fix)
 * - CodeBlock `data-oh-p1b="css"` containing `<style id="oh-p1b">…` (removed)
 * - CodeBlock `data-oh-p1b="css-part2"` containing `<style id="oh-p1b-2">…` (removed)
 * - Scenes 1–6 inserted via WHTML HTML-only: class names stripped (only `data-scene`
 *   remained), so layout was unstyled Div/Paragraph trees.
 *
 * 3. What changed to fix the render
 * - Removed CodeBlocks; never put CSS in CodeBlocks again.
 * - Flattened CSS to single-class selectors only (WHTML StyleParser constraint).
 * - Updated scene HTML so every styled node has its own class (no descendant CSS).
 * - Re-inserted scenes 1–6 via `data_whtml_builder` with `css` so Designer Style
 *   classes attach (`oh-p1b-scene`, `oh-p1b-opp-card`, artifacts, tracks, outcome).
 * - Moved full CSS into Old Home page freeform head as `#oh-p1b` (alongside `#oh-tt`).
 * - Scene kickers visually hidden via `.oh-p1b-kicker` clip (not public labels).
 *
 * 4. Chosen approach: B
 * Native Webflow structure (WHTML Div / Paragraph / Heading) + WHTML `css` for
 * Designer Style classes + page-head freeform CSS as Preview safety net.
 *
 * Post-fix verification (data layer)
 * - 6 `data-scene` roots with styleNames stuck
 * - 0 CodeBlocks; 0 visible `<style` text on canvas
 * - Root Home `lastUpdated` unchanged: 2026-07-30T13:46:47.675Z
 * - Old Home updated; not published
 */
