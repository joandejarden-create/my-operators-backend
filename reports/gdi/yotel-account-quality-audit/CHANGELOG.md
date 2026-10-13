# YOTEL account quality audit — changelog

## Code
- `account-quality-taxonomy-v1.js` — TRUE_* vs venue/generator/contact/parent shells; customer copy helpers
- `customer-readiness-gate-v1.js` — READY requires true account class
- `campaign-decomposition-orchestrator.js` — segment is role/type, not “child account”
- `dealality-gdi-ui.js` — scrub CHILD ACCOUNT; hide homepage URLs in footer
- `scripts/gdi-yotel-account-quality-audit.mjs` — freeze / classify / demote / report

## Data
- Prior YOTEL Ready 9 rematerialized → **3 READY**, **6 FUTURE_WATCH**
- Survivors: AidEx, CHI Geneva, SETAC Europe (`CONTACT_NOW`)
- Palexpo venue placeholders remain non-Ready
- Existing stronger children noted (AidEx, CHI) — no new accounts invented

## Non-changes
- Thresholds not lowered
- ADP / share tokens unchanged
