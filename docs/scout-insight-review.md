# Scout Insight Calibration & Evidence Review (Phase 5C)

## Purpose

Phase 5C makes Scout insights **explainable, commercially useful, and easy to review** without changing underlying data sources or adding AI enrichment.

Each insight can show:

- Why it was generated
- What evidence supports it
- What data is missing
- Whether confidence is justified
- Suggested commercial interpretation and review questions
- Recommended review action (Review, Save, Watch, Dismiss)

**Read-only** — no writes to Hotel Census, Travel Infrastructure, Demand Anchors, Opportunity Radar, or Brand Explorer. Users may still **save Scout signals** to the existing watchlist endpoint when a linked signal exists.

## APIs

### `GET /api/scout/insight-review`

Primary calibration + evidence review endpoint.

**Query:** `country`, `city`, `market`, `submarket`, `parentCompany`, `brand`, `chainScale`, `locationType`, `insightType`, `includeDemandOverlays=1`, `includeSavedSignals=1`, `includeSuppressed=1`, `limit`

**Response highlights:**

| Field | Description |
|-------|-------------|
| `summary.insightsReviewed` | Total insights in review set |
| `summary.strongInsights` | Strong quality count |
| `summary.directionalInsights` | Directional quality count |
| `summary.weakInsights` | Weak quality count |
| `summary.suppressedInsights` | Suppressed count |
| `summary.dataGaps` | Total data gap entries |
| `insightReviews` | Calibrated insight objects |
| `dataQualityNotes` | Scope-level data quality warnings |
| `source.readOnly` / `source.writes` | Always `true` / `false` |

### `GET /api/scout/market-insights` (extended)

Existing response shape is unchanged. Optional Phase 5C fields when `includeInsightReview=1`:

- `insightQualitySummary`
- `dataQualityNotes`
- `suppressedInsightCount`
- `insightReviews`
- `suppressedInsights` (when `includeSuppressed=1`)

Calibrated fields are also merged onto each item in `insights` when review mode is enabled.

## Quality levels

| Level | Meaning |
|-------|---------|
| **Strong** | Adequate census depth plus corroborating signals and/or demand overlays |
| **Directional** | Pattern visible but needs validation |
| **Weak** | Thin evidence; use for awareness only |
| **Suppressed** | Withheld from active review — insufficient data or absence-only logic |

## Evidence item structure

```json
{
  "evidenceType": "census_metric | signal | demand_overlay | saved_signal | hotel_example | gap_metric",
  "label": "",
  "value": "",
  "sourceTable": "",
  "recordId": "",
  "confidence": "High | Medium | Low"
}
```

## Calibration rules (summary)

Rules vary by `insightType` and consider:

- Open / branded / independent hotel counts
- Demand overlay availability
- Related Scout signals
- STR Market / Submarket filter completeness
- Whether the insight is based only on absence of data

**Example:** Parent underrepresentation is **Suppressed** when branded hotels &lt; 3; **Strong** when branded ≥ 10, parent open = 0, and overlays or signals corroborate.

## Review questions

Type-specific diligence prompts are returned in `suggestedReviewQuestions` (e.g. parent targeting, chain-scale saturation, independent conversion fit).

## Suppressed insights

Use `includeSuppressed=1` to include insights withheld for thin data. `suppressedReasons` explains why.

## UI (Scout Market Map — Insight View only)

- Quality summary KPIs (Strong / Directional / Weak / Data Gaps)
- Filters: Insight Quality, Priority, Confidence, Insight Type
- Cards show quality badge, evidence summary, expandable **Evidence** section
- Suggested review action buttons (Save when signal linked; Mark for Review / Watch are local-only status messages)

## Safety

- No Hotel Census writes
- No Travel Infrastructure writes
- No Demand Anchors writes
- No AI enrichment (Phase 5C)
- Opportunity Radar unchanged
- Brand Explorer unchanged

## Tests

```bash
node scripts/test-scout-insight-review.mjs
```

## Module map

| Module | Role |
|--------|------|
| `lib/scout/insight-calibration.js` | Quality classification, evidence, gaps, review questions |
| `lib/scout/market-insights.js` | Insight generation + `buildInsightReviewReport` |
| `api/scout-insight-review.js` | HTTP handler |
