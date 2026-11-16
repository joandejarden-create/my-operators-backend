# FOUNDER REPORT — Toronto ADP Cross-Report Metric Reconciliation V1

## Verdict

**YES** — Toronto customer-facing ADP outputs now use the same canonical Core measurement (`c8bd8b`) and shared published calculators for headline metrics. Stale 29% executive narrative and 81.0% Reality Gap presentation are removed. Performance Review rebuilt as **v2** from the 63-scenario / 252-observation Core period. PDFs registered in AI Admin artifact paths.

## Canonical headlines (where applicable)

33.2% · 38.1% · 44.7% · 85.1% · 19.0% · 66.7%  
Portfolio: 45.5% · #1 of 6 · 0.0% · 92.9% (308ba1)

## Citation reconciliation

| Field | Value |
|-------|-------|
| Cited responses | 63 |
| Owned responses | 6 |
| External-only | 57 |
| Owned share | **9.5%** |
| External share | **90.5%** |
| Previous incorrect | live UI 0%/100% when ownedShare missing; earlier confusion vs 9.5/90.5 |

## Stale values

| Value | Where | Fixed |
|-------|-------|-------|
| 29% | published executiveRead narrative | YES |
| 32.1% | MR v1 KPIs | YES (v2) |
| 31.7% | MR v1 KPIs | YES (v2) |
| 23.5% | MR v1 + live ER recompute risk | YES |
| 83.3% | MR v1 Top-3 | YES (v2 shows 85.1%) |
| 17 attributes | MR v1 / live recompute | YES (21) |
| 81.0% | web Reality Gap binary UI | YES (uses gapScore 66.7%) |

## Consistency

| Surface | Match | Period |
|---------|-------|--------|
| Live report | YES (browser QA) | c8bd8b |
| Performance Review | YES (v2 + PDF) | c8bd8b |
| Audit PDF | YES (kept) | c8bd8b |
| AI Admin | YES | c8bd8b + 308ba1 |

## Browser / PDF verification

- WEB BROWSER VERIFIED: **YES**
- PERFORMANCE REVIEW PDF VERIFIED: **YES**
- AUDIT PDF VALIDATED: **YES**
- STANDARD ADP PDF REGENERATED: **YES**

## What changed (derived only)

- UI Reality Gap: prefer `gapScore`; missed/partial wording
- Executive Read: prefer published Reality Coverage (no 17-attr recompute)
- MR builder: full territories; Top-3 + Tracked Attributes KPIs
- Published executiveRead repaired in place (backup retained)
- New MR version v2 + PDF; current-report PDF refreshed

**No provider rerun. No new Core/Loyalty period. No raw observation rewrite.**
