# Many Futures — Nine-Question Webflow Ship (Final Approval)

**Status:** Designer updated · **Do not publish**  
**Branch:** `cursor/many-futures-nine-questions-38e7`  
**CDN asset commit:** `61283145514e76e935a73d67c1acc5ddda48e292`  
**HtmlEmbed:** `faf5dede-519c-f93e-9c82-66cf832f1bf7` on Old Home  
**Artifacts:** `/opt/cursor/artifacts/many-futures/nine-question-ship/`  
**Repo mirror:** `webflow-embeds/many-futures/visual-review/nine-question-ship/`

---

## Asset URLs

| Asset | URL |
|-------|-----|
| CSS | `https://cdn.jsdelivr.net/gh/joandejarden-create/my-operators-backend@61283145514e76e935a73d67c1acc5ddda48e292/webflow-embeds/many-futures/dist/many-futures.45083cd808ed.css` |
| JS | `https://cdn.jsdelivr.net/gh/joandejarden-create/my-operators-backend@61283145514e76e935a73d67c1acc5ddda48e292/webflow-embeds/many-futures/dist/many-futures.a0d742bd3237.js` |
| Body (hashed) | `https://cdn.jsdelivr.net/gh/joandejarden-create/my-operators-backend@61283145514e76e935a73d67c1acc5ddda48e292/webflow-embeds/many-futures/dist/many-futures.21e51916e7d5.body.html` |

Webflow Code Embed uses the compact CDN body loader (`webflow-embed.html`, ~1.3KB) because full nine-state markup exceeds the ~50KB embed limit. Body markup uses `__MF_CDN_BASE__` token replacement so the content hash stays stable across CDN pin rebuilds.

---

## Height alignment (desktop 1440)

| Column | Height |
|--------|--------|
| Hotel / illustrative opportunity | **568px** |
| Nine-question selector | **568px** |
| Delta | **0px** |
| Per-button height | **56px** × 9 |

Desktop-only flex rail + `grid-template-rows: repeat(9, minmax(0, 1fr))`. Tablet/mobile keep natural-height buttons (`heightsForced: false` at 768/390/320).

---

## Transfer weights

| State | Transfer |
|-------|----------|
| Default (Q1) | **~303 KB** |
| After all nine | **~686 KB** |

---

## QA checklist

| # | Check | Result |
|---|-------|--------|
| 1 | Nine questions approved order | Pass |
| 2 | Desktop question list aligns with hotel card | Pass (Δ 0) |
| 3 | Q05 two-panel Outreach Setup + Opportunity Review | Pass (`mf-workspace--two-panel`, 2 feats) |
| 4 | Market Alerts curated language (no real-time/predictive) | Pass |
| 5 | Action Tracking + Activity Log & Next Action | Pass |
| 6 | Submit Proposal brand-only / Brand Response Workflow | Pass |
| 7 | No empty feature panels | Pass |
| 8 | No clipped question text | Pass |
| 9 | No visual overlap | Pass |
| 10 | No horizontal overflow (1440/1200/768/390/320) | Pass |
| 11–13 | Keyboard focus, Enter/Space pin, rapid switch | Pass |
| 14 | Reduced motion | Pass |
| 15–16 | Transfer weights near 300 / 682 KB | Pass (303 / 686) |
| 17 | Console errors | None |
| 18 | `#platform-features` unchanged | Confirmed present on Old Home; not modified |

---

## Screenshots (return package)

1. `01-desktop-1440-q1-rebrand.png`
2. `02-desktop-1440-q5-confidential.png`
3. `03-desktop-1440-q6-market.png`
4. `04-desktop-1440-q7-actions.png`
5. `05-mobile-390-q5-confidential.png`
6. `06-mobile-390-q6-market.png`
7. `07-mobile-390-q7-actions.png`
8. `08-contact-sheet-desktop-nine-states.png`
9. `09-contact-sheet-mobile-nine-states.png`
10–12. Heights in `height-comparison.json` + `12-desktop-height-alignment.png`
13. `13-q5-two-panel-closeup.png`
14. `14-market-alerts-closeup.png`
15. `15-activity-log-closeup.png`
16–17. CSS / JS URLs above
18. `transfer-weights.json`
19. `console-a11y-results.json`
20. `#platform-features` confirmed unchanged

**Do not publish. Stop for final approval.**
