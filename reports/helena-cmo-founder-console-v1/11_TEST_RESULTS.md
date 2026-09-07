# 11 — Test Results

## Hotfix re-test (PR #34 runtime)

Commands:

```bash
node --check public/js/admin-helena-cmo.js
npm run test:helena-cmo-founder-console-v1
npm run playwright:helena-cmo-founder-console-smoke-v1
node scripts/test-batch1-route-auth.mjs
```

| Suite | Result |
|---|---|
| `node --check public/js/admin-helena-cmo.js` | PASS |
| `test:helena-cmo-founder-console-v1` | **10/10 PASS** (FC-01…FC-10) |
| `playwright:helena-cmo-founder-console-smoke-v1` | **12/12 PASS** |
| `test-batch1-route-auth.mjs` | PASS (existing auth regression) |

### FC gates (unit)

| ID | Test | Result |
|---|---|---|
| FC-01 | Brief loads Manual Week 1 | PASS |
| FC-02 | Top 3 + ≤3 decisions | PASS |
| FC-03 | UNKNOWN / DATA_GAP preserved; no fabricated accounts | PASS |
| FC-04 | Tier 1 present; EXECUTE/recurring/Cursor OFF | PASS |
| FC-05 | APPROVE → APPROVED_PREPARE_ONLY | PASS |
| FC-06 | Decision local persist without EXECUTE/OS sync | PASS |
| FC-07 | Attention counts action items | PASS |
| FC-08 | UI + API files exist | PASS |
| FC-09 | Browser-delivered JS parses (`node --check`) | PASS |
| FC-10 | Founder/admin auth policy (not bare `requireAdminAccess`) | PASS |

### Playwright smoke

Loads Founder Console in a clean Chromium context (no browser extensions):

- JS parses (no SyntaxError / ReferenceError)
- Founder Brief + Top 3 + Joan decisions render
- UNKNOWN / DATA_GAP visible
- EXECUTE OFF visible; no publish/EXECUTE controls
- Helena APIs return 200 for authorized founder/admin stub
- attention-count authorized fetch succeeds

Machine copies: `test-results.json`, `playwright-smoke-results.json`

---

## WHY ORIGINAL 8/8 MISSED THE BUG

The original suite only exercised **Node view-model + file existence**. It never:

1. Ran `node --check` on browser-delivered JS
2. Loaded the page in a browser
3. Asserted against console/page errors

So a fatal `SyntaxError` in `public/js/admin-helena-cmo.js` (unescaped `"` inside a double-quoted HTML string) shipped while unit tests stayed green.

The attention-count **403** was a frontend/backend policy mismatch (`canShowFounderNavOverrides` / `isDevMode` vs bare `requireAdminAccess`) also invisible to the original suite.

---

## WHAT TEST NOW PREVENTS REGRESSION

1. **FC-09** — `node --check` on `admin-helena-cmo.js` + `support-admin-gate.js` fails the suite if browser JS cannot parse.
2. **FC-10** — asserts Helena routes use `helenaCmoAdminAuth` / `requireHelenaCmoAdminAccess` (founder/admin constellation), and documents that bare `requireAdminAccess` rejects founder-only users.
3. **Playwright smoke** — clean browser context verifies Brief render, no uncaught SyntaxError/ReferenceError, authorized Helena API 200s (including attention-count), and no EXECUTE/publish affordances.

---

## Manual QA checklist (browser)

1. Admin → Helena CMO visible when founder/admin `/api/me` resolves
2. Founder Brief loads Week 1 Top 3 + ≤3 decisions
3. UNKNOWN/DATA_GAP visible on scoreboard
4. Attention badge does not spam 403s after identity resolves
5. APPROVE shows PREPARE-only semantics
6. Confirm no deploy / merge / EXECUTE / recurring enablement
