# GDI Executive Summary — Phase 1 Browser Remediation

**Status:** `GDI_EXECUTIVE_SUMMARY_PHASE_1_BROWSER_VERIFIED — AWAITING_FOUNDER_ACCEPTANCE`  
**PR:** [#618](https://github.com/joandejarden-create/my-operators-backend/pull/618)  
**Auth:** `FOUNDER_GO_GDI_EXECUTIVE_SUMMARY_PHASE_1_20261127`

## Root cause — missing Executive Summary tab

| Fact | Evidence |
|---|---|
| `origin/main` still registers **3** GDI tabs | `GDI_CUSTOMER_TABS = [GDI_MAIN_TAB, GDI_PRIORITIES_TAB, GDI_PORTFOLIO_TAB]` |
| Executive Summary exists only on PR #618 | `GDI_EXEC_SUMMARY_TAB` + 4-tab array |
| Port **8080** is reserved for canonical `my-operators-backend` | `server.js` fail-closed unless `DEALALITY_CANONICAL_CHECKOUT=1` |
| Founder-visible 3-tab chrome matches **unmerged main / IC checkout**, not PR #618 | Not a pure browser-cache issue; served commit lacks the tab |
| Prior agent “browser validation” ran against **ws-gdi feature branch** with allow-env override | That is not the same as IC `main` on 8080 after merge |

**Conclusion:** The tab cannot appear on a founder browser that is serving `main` until PR #618 is merged (IC) **or** until :8080 is intentionally started from this PR branch.

Asset cache-bust bumped to `gdi-exec-summary-p1d` and a `<meta name="gdi-build">` stamp was added so the founder can verify served HTML/JS.

## Secondary root cause — STAGED founder preview hung / 401

`gdiPilotReadAuth` allows Bearer-less pilot reads and sets `gdiPilotUnauthRead=true`. The ES handler correctly refused STAGED preview in that mode, but the iframe JWT retry hung (`getAuthHeaders` timeout), leaving “Loading…”.

**Fix:** `middleware/gdiExecutiveSummaryAuth.js` — CURRENT stays on pilot-read; `?preview=staged|founder` uses `memberstackAuth` + `requireDealalityUser` (including local `DEV_AUTH_BYPASS`). Handler still applies founder/admin + hotel-scope gates. ES tab load now times out instead of hanging.

## Remediation in this update

1. Evenly spaced four-tab CSS + dedicated Executive Summary icon  
2. Cache-bust `p1d` + build meta stamp  
3. Founder/admin-only STAGED preview gate (ordinary hotel user → 403)  
4. Dedicated ES auth middleware for staged/founder preview  
5. Negative automated tests (unauth, pilot-unauth, ordinary user, cross-hotel, admin allow, route wiring)  
6. Hotel readiness audit (Bethesda 67 / YOTEL ID reconciliation / Wyndham)  
7. PowerShell: `validate` · `generate` · `readiness-audit`

## Staging investigation (this verification)

| Check | Result |
|---|---|
| Port 8080 PID | `22908` (after restart; prior `25772`) |
| Working directory | `C:\Dev\dealality\ws-gdi` |
| Branch | `feat/gdi-executive-summary-phase-1-20261127` |
| Served build stamp | `gdi-exec-summary-p1d` |
| Integrated route | `http://localhost:8080/app#/group-demand-intelligence` |
| Standalone alone | Insufficient — acceptance used integrated `/app` |

## Hotel reconciliation

### Bethesda `recLuxvwwxID7U2B8`

- CURRENT package customer-visible count: **67**
- Opportunities UI: **Showing 67 of 67**
- Executive Summary KPI: **67** Qualified opportunities  
- Same CURRENT package inventory; published inventory **not** modified

### YOTEL identity

| ID | Name | CURRENT |
|---|---|---|
| `recrPQcZg7SFARRb2` | YOTEL Geneva Lake (canonical) | **Yes** (42) — Phase 0 |
| `recaokE8psIbuVeuH` | Not YOTEL profile (mis-key) | No — Phase 1 “STAGED-only” was this wrong id |

Discrepancy resolved by identity audit; **no** publication changes.

### Wyndham `recnPB3Q02NO8Ma1o`

- Published / CURRENT: honest unavailable (dashes; no fabricated stats)  
- Authorized founder STAGED preview: **FOUNDER_PREVIEW**, **11** qualified (STAGED package; not CURRENT; `customerFacing=false`)  
- Locked 26 write-set **not** applied

## Browser acceptance evidence (integrated `/app` on :8080)

Screenshots under `reports/gdi-executive-summary-v1/screenshots/`:

1. `gdi-four-tabs-opportunities-default-integrated.png` — four tabs; Opportunities active  
2. `gdi-bethesda-exec-summary-67-integrated.png` — Bethesda ES populated + Research Receipt  
3. `gdi-bethesda-opportunities-67-integrated.png` — Bethesda 67 inventory  
4. `gdi-exec-summary-wyndham-unavailable-integrated.png` — Wyndham published unavailable  
5. `gdi-wyndham-founder-preview-staged-integrated.png` — authorized founder STAGED preview  
6. `gdi-hotel-switch-integrated.png` — hotel switch preserves GDI chrome  
7. `gdi-bethesda-priority-deeplink-drawer-integrated.png` — priority deep link → drawer  

### Exact URLs used

```
http://localhost:8080/app#/group-demand-intelligence?hotelId=recLuxvwwxID7U2B8
http://localhost:8080/app#/group-demand-intelligence?hotelId=recnPB3Q02NO8Ma1o
http://localhost:8080/app#/group-demand-intelligence?hotelId=recnPB3Q02NO8Ma1o&inventory=staged&preview=staged&tab=executive-summary
```

Served assets: `?v=gdi-exec-summary-p1d` · meta `gdi-build=gdi-exec-summary-p1d`.

## Exact validation commands

```powershell
cd C:\Dev\dealality\ws-gdi
git rev-parse HEAD
npm run test:gdi-executive-summary-v1

# Must serve THIS branch on :8080 for tab verification (IC merge still required for main)
node scripts/dealality-gdi-local-8080-stable-launch-v1.mjs --stop
$env:DEALALITY_CANONICAL_CHECKOUT='1'
node scripts/dealality-gdi-local-8080-stable-launch-v1.mjs

.\scripts\gdi\Invoke-GdiExecutiveSummary.ps1 -HotelId recLuxvwwxID7U2B8 -Mode validate
.\scripts\gdi\Invoke-GdiExecutiveSummary.ps1 -HotelId recrPQcZg7SFARRb2 -Mode validate
.\scripts\gdi\Invoke-GdiExecutiveSummary.ps1 -HotelId recnPB3Q02NO8Ma1o -Mode validate
.\scripts\gdi\Invoke-GdiExecutiveSummary.ps1 -Mode readiness-audit
# generate is dry-run by default (no paid research)
.\scripts\gdi\Invoke-GdiExecutiveSummary.ps1 -HotelId recLuxvwwxID7U2B8 -Mode generate
```

Regression: **17 PASS** (`npm run test:gdi-executive-summary-v1`).

## Governance

NO_PAID_RESEARCH · NO_PAID_ENRICHMENT · NO_AIRTABLE_WRITES · NO_STAGED_ACTIVATE · NO_CURRENT_PUBLISH · NO_PRODUCTION_DEPLOYMENT · no Wyndham write-set · no Phase 2 · no independent merge

## Stop token

`GDI_EXECUTIVE_SUMMARY_PHASE_1_BROWSER_VERIFIED — AWAITING_FOUNDER_ACCEPTANCE`
