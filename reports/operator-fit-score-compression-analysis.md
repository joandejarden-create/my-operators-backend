# Operator Fit — Score Compression Analysis

**Audit only.** Aggregate from synthetic scenarios + pilot deals A–F eligible evaluations (`reports/operator-fit-differentiation-audit.json`).

## Layer structure verification

Implemented exactly as documented: **70%** Operator–Project · **15%** Structure · **15%** Brand (`PRIMARY_LAYER_WEIGHTS`).

On Deal C: brand N/A (skipped); structure unknown (0 in denom) → primary = `(opProject × 70) / 85`, which **compresses** a 59.0 op-project score to **48.6** before risk.

## Distribution summary

| Layer | n | Mean | Median | Min | Max | Stdev |
| ----- | -: | ---: | -----: | --: | --: | ----: |
| Operator–Project raw | 126 | 46.87 | 45.85 | 5.7 | 92.7 | 17.10 |
| Pre-risk (layered) | 126 | 47.23 | 45.60 | 4.0 | 90.4 | 17.50 |
| Displayed (after risk + ceiling) | 126 | 33.78 | 30.10 | 0 | 90.4 | 22.16 |
| Ranking Ready displayed (pilot RR rows) | 19 | 47.61 | 45.60 | 29.3 | 73.9 | 12.01 |

## Band counts (displayed, all eligible in sample)

Strong 10 · Good 9 · Potential 21 · **Limited 86**

Risk + ceilings push mass into Limited.

## Tie frequency

| Metric | Displayed sample | RR subset |
| ------ | ---------------: | --------: |
| Exact tie groups (same score) | 14 groups / 44 members | 2 / 4 |
| Near-tie pairs &lt;1 pt | 247 | 4 |
| &lt;2 pt | 480 | 10 |
| &lt;3 pt | 653 | 20 |
| &lt;5 pt | 1065 | 35 |

## Deal C

Exact displayed tie (38.6 / 38.6) among RR leaders — confirmed.

## Verdict

- Engine **can** spread scores (stdev ~17–22) — not a total flatline failure.  
- **70/15/15 with unknown structure** compresses Deal C specifically.  
- **Risk on unknowns + Limited ceiling behavior** increases Limited-band mass.  
- Exact/near ties are **common enough** that owner ordinal #1/#2 without materiality rules is risky.  
- Compression is **partially intended conservatism**, partially **unknown-layer + risk stacking**.
