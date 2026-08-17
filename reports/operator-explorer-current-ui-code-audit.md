# Operator Explorer — Current UI / Code Audit

**Date:** 2026-08-09  
**Mode:** Inventory only — do not rebuild UI

---

## Surfaces found

| Asset | Role | Verdict |
| ----- | ---- | ------- |
| `public/operator-explorer.html` + `public/js/operator-explorer.js` | Operator list / discovery | **Remain + extend** |
| `public/operator-explorer-detail.html` | Detail shell | **Remain + extend** |
| `public/operator-explorer-gold-mock.html` + `operator-explorer-gold-mock-data.js` | Gold baseline preview (Arbor/HE) | **Remain** as internal quality bar |
| `public/js/operator-explorer-new-base-profile.js` | New-base profile builder | **Remain + extend** |
| `public/css/operator-explorer*.css` | Styling | Remain |
| `public/operator-explorer-share` patterns (if present) | Share packs | Extend carefully (OE parallel to BE share) |
| Fixtures `fixtures/operator-*-*.json` | Tab content packs | Remain; goldens protected |
| `lib/partner-intelligence/operator-explorer-*` | OS, tab factory, provenance, baseline, overlays | **Remain** — production-protected process |
| `scripts/operator-explorer-*.mjs` | Factory/init/gates | Remain |
| `lib/operator-fit/*` | Fit engine v2/v2.1 | Remain; flags OFF for owners |
| `lib/operator-intelligence/*` | Claims policy, presence, conflicts, calibration overlay | Remain — research core seed |
| Operator Setup HTML / writer | Intake | Remain; writer flag legacy-primary |
| APIs: company profile, match-score, operator deal requests | Platform | Remain; do not wire My Deals owner pilot here |

---

## Filters / navigation

- List filters by Active / status (env-driven Active values)
- Optional `OPERATOR_EXPLORER_HIDE_TEST_RECORDS`
- Gold mock query by Master `rec…`
- Factory preview patterns exist for OE content — do not weaken baseline

---

## Remain / extend / replace / retire

| Area | Decision |
| ---- | -------- |
| Explorer list + detail | Extend toward content model sections when data ready |
| Gold mock | Remain as protected baseline viewer |
| Tab Factory fixtures | Remain; do not replace with Brand Explorer writers |
| Fit UI (internal) | Remain behind flags |
| Legacy OAS UI | Remain until Fit replaces by founder decision — out of scope |
| Antillano / dummy Explorer demos | Retire from production universe; keep fixtures in code |
| Brand Explorer pages | Not OE — do not merge UIs |

---

## Gaps vs desired content model

Missing or thin in UI data today: typed Market Presence depth, assignment inventory, verified brand relationships, recent momentum for operators, evidence footnotes consistently, Fit-ready vs Explorer-publishable distinction in chrome.
