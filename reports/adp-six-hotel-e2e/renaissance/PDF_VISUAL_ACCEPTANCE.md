# PDF visual acceptance — Renaissance `…2ff16a`

**Method:** PyMuPDF rasterize every page → PNG under `pdf-visual/`; text extract for metrics; human review of each page image.

## Current-report PDF (6 pages)

| Page | Check | Result |
|------|-------|--------|
| 1 | Hotel / 45% / 80 scenarios / Marquis | PASS |
| 2 | Metrics; no +45 pts; Baseline Oct 16 | PASS (post-regen) |
| 3 | Trends “Not comparable scope”; providers | PASS |
| 4–6 | Portfolio / competitive / actions / sources | PASS — readable, no internal errors |

Edition archive: `adp_current_2026-10-16_adp_renaissance_times_square_v4`  
Pre-regen copies: `pdf-history/`

## Monthly-review PDF (9 pages)

| Page | Check | Result |
|------|--------|--------|
| 1 | Cover OCT 16, 2026 (was SEP 2) | PASS |
| 2 | 45% / baseline disclosure / Marquis | PASS |
| 3–9 | Actions agenda / accountability | PASS — no blank/clipped critical content observed |

Review id: `adp_mr_renaissance_times_square_2026-10_v2`

## Fixed after first visual pass

1. False `+45.0 pts vs last run` (dry-run prior)  
2. Monthly cover monitoring date  
3. KPI baseline date tied to current period  
4. Non-comparable historical deltas suppressed  
