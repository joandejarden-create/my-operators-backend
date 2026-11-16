# CROSS_REPORT_TEST

Gate: `npm run test:adp-toronto-cross-report-reconciliation-v1`

Asserts equality across published live model, Performance Review v2, and AVA payload for:

- AI Consideration 73/220 = 33.2%
- Scenario Presence 24/63 = 38.1%
- #1 21/47 = 44.7%
- Top-3 40/47 = 85.1%
- Reality Coverage 4/21 = 19%
- Reality Gap (11 + 0.5×6)/21 = 66.7%
- Tracked attributes 21
- Territories total 63
- Displacement Bisha 24 … Shangri-La 16
- No stale 29% / 81.0% / “misses 17” in executiveRead
- Citation owned/external 9.5 / 90.5

Result: **PASS** (see test stdout).
