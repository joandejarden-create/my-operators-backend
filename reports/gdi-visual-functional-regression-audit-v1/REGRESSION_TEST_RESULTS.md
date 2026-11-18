# Regression test results

## Automated

| Check | Command | Result |
|---|---|---|
| SVG icon color regression | `npm run test:gdi-visual-svg-icon-color-regression-v1` | PASS (5/5) |
| Opportunity card quality | `npm run test:gdi-opportunity-card-quality-v1` | PASS (10/10) |
| Icon gate (11 fixtures) | `runOpportunityRevenueStreamIconsGate()` | PASS |

## Guadalajara Research Preview icon counts (sanitize)

| Metric | Before | After |
|---|---|---|
| Guestroom active | 0 | 43 |
| Catering active | 0 | 25 |
| Both muted | 43 | 0 |

## Cache bust

- CSS: `group-demand-intelligence.css?v=gdi-svg-icon-color-restore-v1`
- JS: `dealality-gdi-ui.js?v=gdi-svg-icon-color-restore-v1`

## Local server verification

- Primary checkout branch: `fix/gdi-visual-svg-icon-color-regression-v1`
- Restarted Node `server.js` on `127.0.0.1:8080` from `C:\Dev\dealality\my-operators-backend` (listener PID recorded at delivery)
- HTML cache-bust `gdi-svg-icon-color-restore-v1` confirmed in served `group-demand-intelligence.html`
- Live API `.../hotels/recCAGqkERSXgiku4/opportunities?live=1` → **43 guestroom-active / 25 catering-active / 0 muted-both** (`listScope=FOUNDER_RESEARCH_INVENTORY`)
- Staged API `?preview=staged` → **36 / 21** active (package baseline preserved)
- Browser (Research Preview Guadalajara `?live=1`): DOM count `guestActive=43`, `eventActive=25`; sample active computed color `rgb(253, 181, 42)` (`#fdb52a`)
