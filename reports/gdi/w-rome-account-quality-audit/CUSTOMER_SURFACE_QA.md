# W Rome customer surface QA

| Check | Result |
|---|---|
| Starting customerVisible cohort | 6 |
| Strict facing before | 0 |
| Facing after | 0 |
| Ready after | 0 |
| Venue placeholder on Ready | 0 |
| Homepage as buyer path on facing | 0 |
| CHILD ACCOUNT text on facing | CLEARED |
| QUALIFY+Ready contradictions | 0 |
| Bethesda shared card component | YES (dealality-gdi-ui) |
| Speculative accounts created | NO |

## Browser QA (2026-10-05)

| Check | Result |
|---|---|
| Property selected | `W Rome — Rome, Italy` |
| Showing | 0 of 0 Opportunities |
| Empty state | "No customer-ready opportunities are available for this property yet." |
| CHILD ACCOUNT text | NONE |
| Venue placeholder cards | NONE |
| QUALIFY filter count | 0 |
| Bethesda regression | 28 facing (shared card component) |
| YOTEL regression | 4 facing; label `YOTEL Geneva Lake — Founex, Switzerland` |

## Browser QA (2026-10-05)

| Check | Result |
|---|---|
| Property selected | `W Rome — Rome, Italy` |
| Showing | 0 of 0 Opportunities |
| Empty state | No customer-ready opportunities are available for this property yet. |
| CHILD ACCOUNT text | NONE |
| Venue placeholder cards | NONE |
| QUALIFY filter count | 0 |
| Bethesda regression | 28 facing (shared card component) |
| YOTEL regression | 4 facing; label `YOTEL Geneva Lake — Founex, Switzerland` |

Browser: open W Rome GDI after server restart — expect **0** customer cards (real sales targets only).
