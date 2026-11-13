# GDI Guided Tour Production Visual Regression V1 — Founder Report

**Verdict (pre-deploy):** **NOT CERTIFIED — CLIENT READY blocked** until production Railway share passes visual acceptance after V1_1g deploy.

**Package / fingerprint:** unchanged  
`gdi_pub_recLuxvwwxID7U2B8_20261113_reconciliation_v1`  
`441773211494cebaf50c03ef8921ccac6c57f31ac5cb2398585352b425e36f7f`

## Root cause (confirmed)

Demoting the Opportunity Details dialog from `showModal()` → `show()` for popover hit-testing left a non-modal `<dialog>` at **`y ≈ 416`** (`position: absolute`). Combined with a **~40vh** highlight cutout and a `9999px` box-shadow scrim, step 6 rendered as a blank dark overlay. See `01-ROOT-CAUSE.md` and `00-REPRO-prod.json`.

Production was still serving **`gdi-howto-read-v1-1e`** assets — backend SHA ≠ frontend identity (`02-ASSET-HASH-COMPARISON.json`).

## Fix shipped in this change set

- Shared CSS pin: `.gdi-drawer--tour-nonmodal`
- Shared tour demotion always applies the pin; clears on exit
- Engine: full-rail drawer highlight + left-column coachmark clearance
- Cache bust / version: **`GDI_GUIDED_REPORT_TOUR_V1_1g`** / `?v=gdi-howto-read-v1-1g`
- New gate: `npm run test:gdi-guided-tour-visual-acceptance-v1`

## Local verification (this machine)

| Check | Result |
|-------|--------|
| `test:gdi-guided-report-tour-v1` | PASS (V1_1g) |
| Visual acceptance @ `127.0.0.1:8080` desktop 1440×900 | PASS (`05-GUIDED-TOUR-STEP-RESULTS.json`) |
| Mobile 390×844 local share | PASS (`06-MOBILE-LOCAL-ACCEPTANCE.json`) |
| Production Railway visual acceptance | **PENDING deploy** |

Step 6 after fix (local): drawer `{x:880,y:0,w:560,h:900}`, highlight matches full rail, coachmark left of drawer, Next hittable.

## Prior false PASS

Documented in `03-PRIOR-FALSE-PASS-GAP.md`. Navigation-only suites + unevaluated screenshots + stale production cache.

## Authorization scope

- Technical UI/tour fixes only
- No package fingerprint change
- No other hotel publication

## Next mandatory gate

1. Merge PR → Railway SUCCESS for this tip SHA  
2. Confirm production HTML marker `gdi-howto-read-v1-1g` and matching asset hashes  
3. Re-run visual acceptance against the live Bethesda share URL (desktop + mobile)  
4. Only then flip verdict to CLIENT READY / CERTIFIED
