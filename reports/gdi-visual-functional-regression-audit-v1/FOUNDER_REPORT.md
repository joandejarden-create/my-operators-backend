# FOUNDER REPORT — GDI visual, commercial data & functional regression audit V1

## Verdict

Confirmed unintended regression: **revenue-stream SVG icons rendered muted gray on Research Preview** for inventories that still carried commercial class + Sales Opportunities keys but lacked thesis prose. Gold CSS was never removed. Shared restore now lights icons again (Guadalajara **43 / 25** active — historical baseline). No redesign. No publication/Airtable/paid changes.

## Founder answers

1. **Why did the SVG colors disappear?**  
   Icons use `currentColor`. Active state is gold; muted is gray. Research Preview resolved almost all icons to `muted` because thesis scrub + consistency demotion wiped class-mapped actives, and live sanitize did not share the PACKAGE restore path.

2. **Which PR / code change caused it?**  
   Not a CSS redesign PR. The gap is between PACKAGE SVG restore (Sheraton need-dates/SVG repair) and live Research Preview resolve/sanitize. Visibility increased after Research Preview inventory integrity recovery (PR #357).

3. **Other unintended changes confirmed?**  
   Client UI could re-demote trusted API COMBINED icon states when thesis text was empty (REG-SVG-002). No other confirmed unintended removals of filters, drawers, or commercial fields in this audit.

4. **Commercial fields lost from UI?**  
   Classification fields were still present in research records (`revenueStreamClass`, sales keys). Presentation mapping lost icon lighting — not the underlying commercial labels themselves. No invented commercial values were added.

5. **Controls / interactions removed?**  
   None confirmed. Sidebar inventory toggle (PR #358) preserved as intentional.

6. **Drawer sections affected?**  
   No evidenced wholesale drawer section removal. Icon state feeding the tile/drawer revenue indicators is restored.

7. **Filters affected?**  
   No confirmed filter regressions. Sales Opportunities keys remain the corroboration source for icon restore.

8. **Hotel-specific differences introduced?**  
   None. Shared resolver + shared CSS. Hotels without class/keys stay honestly muted.

9. **What was restored?**  
   Class+sales-key backed guestroom / catering gold icons on Research Preview (and UI trust of API/package icon states).

10. **What enhancements were preserved?**  
    Inventory integrity (#357), sidebar toggle (#358), COMBINED-without-event-thesis demotion, publication governance, dark-blue chrome, qualification/scoring.

11. **Fixes visible on actual local server?**  
    See `REGRESSION_TEST_RESULTS.md` — local server restarted on this revision; hard-refresh with cache-bust `gdi-svg-icon-color-restore-v1`.

12. **Permanent regression tests?**  
    `npm run test:gdi-visual-svg-icon-color-regression-v1` (gate cases + Guadalajara sanitize baseline + CSS gold token + PACKAGE/live parity).

## Artifacts

All files under `reports/gdi-visual-functional-regression-audit-v1/`.
