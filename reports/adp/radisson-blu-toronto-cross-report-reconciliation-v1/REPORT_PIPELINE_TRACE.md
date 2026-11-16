# REPORT PIPELINE TRACE

| Surface | Model / loader | Period resolver | Metric calculator | Reality Gap | Territory | Displacement | Citation |
|---------|----------------|-----------------|-------------------|-------------|-----------|--------------|----------|
| A. Live ADP web | `published-snapshot.loadPublishedReport` → `ai-demand-positioning.js` | published `period.periodId` / `corePeriodId` | published `executiveMetrics` | UI uses `rg.gapScore` (RG_WEIGHTED_PARTIAL_V2); wording uses missed/partial counts | `demandCapture.byIntent` | `lostDemand.displacement` | `evidence.ownedSourceShare` |
| B. AI Demand Performance Review | `buildMonthlyExecutiveReviewV1` ← published | archive meta `sourceCurrentPeriodId` = c8bd8b | same published EM | `resolveCanonicalRealityCoveragePct` + published RG | full `byIntent` (no slice) | published displacement leaders | N/A (not headline) |
| C. AI Visibility Audit & Action Plan | AVA payload `radisson-blu-toronto-downtown-v1.json` + archive PDF | meta `corePeriodId` c8bd8b | baked AVA headlines | AVA keeps certified 33.2/38.1/19.0; gap not forced to 81 | AVA narrative | peer #1 of 6 | not primary |
| D. AI Admin report summary | published manifest + MR archive index + current-report-pdf store | c8bd8b / 308ba1 | published | published gapScore 66.7 | published | published | published 9.5/90.5 |
| E. Action Plan | AVA / ADP actions from published | c8bd8b | published | supporting | — | — | — |
| F. Executive Summary | `buildExecutiveReadWithUx` + V3 composition on **published** payload | c8bd8b | EM rates; Reality prefers `realityGap.coveragePercent` | published | — | V3 keyInsight uses displacement | — |

## SURFACE → MODEL → CALCULATOR → PERIOD → OUTPUT

```
Live Web
  → published report JSON (c8bd8b)
  → executiveMetrics + realityGap.gapScore
  → owner-ai-demand / pdf-render UI
  → customer HTML/PDF

Performance Review
  → buildMonthlyExecutiveReviewV1(published)
  → archive adp_mr_…_2026-11_v2 + report.pdf
  → AI Admin Reviews tab / client PDF

AVA
  → payload + archive current.pdf (unchanged certified headlines)
  → AI Admin AVA surface

Executive Read
  → rebuild from published EM/RG (no live 17-attr recompute)
  → stamped on published report payload
```
