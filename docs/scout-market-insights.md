# Scout Market Insight Engine (Phase 5B)

## Purpose

The **Scout Insight Engine** explains what may be underrepresented, what appears attractive, and what actions may be worth considering for a selected geography — using **explainable, read-only** Dealality data.

Language is intentionally cautious: insights use terms like *appears*, *may indicate*, *suggests*, *worth reviewing*, and *potential* — not factual recommendations.

**Opportunity Radar** (`/app#/opportunity-radar`) is unchanged. This engine powers the **Insight View** on Scout Market Map only.

## API

### `GET /api/scout/market-insights`

**Query:** `country`, `city`, `market`, `submarket`, `parentCompany`, `brand`, `chainScale`, `locationType`, `includeDemandOverlays=1`, `includeSavedSignals=1`, `includeInsightReview=1`, `includeSuppressed=1`, `insightType`, `limit=100`

**Phase 5C (optional):** `includeInsightReview=1` adds calibration fields — see [scout-insight-review.md](./scout-insight-review.md).

**Also via:** `GET /api/scout/market-map?includeInsights=1` (adds `insightSummary`, `insights`, `rankedOpportunities` without breaking existing fields)

### `GET /api/scout/insight-review` (Phase 5C)

Dedicated insight calibration and evidence review — see [scout-insight-review.md](./scout-insight-review.md).

## Data sources (read-only)

| Source | Use |
|--------|-----|
| Hotel Census | Supply metrics, chain scale, parent/brand presence |
| Scout opportunity signals | Related signal IDs, conversion/pipeline context |
| Scout Opportunity Signals (saved) | Human review boost in scoring |
| Travel Infrastructure + Demand Anchors | Demand-driver overlays |
| Brand Alias Mapping | Brand resolution + absent parent discovery |

**No writes** to Hotel Census, Travel Infrastructure, Demand Anchors, Radar, or Brand Explorer.

## Insight types

| Type | Purpose |
|------|---------|
| `parent_company_underrepresentation` | Parent absent or low share vs branded supply |
| `brand_underrepresentation` | Brand absent where comparable supply exists |
| `chain_scale_gap` | Thin or missing chain-scale segments |
| `independent_conversion_potential` | Independent clusters worth reviewing |
| `operator_white_space` | Operator / franchise fragmentation signals |
| `pipeline_momentum` | Pipeline hotels in scope |
| `all_inclusive_potential` | Resort/tourism context — review only |
| `branded_residential_potential` | Upscale + mixed-use/tourism — review only |
| `demand_driver_supported_opportunity` | Supply gaps aligned with demand overlays |

## Scoring (`SCOUT_INSIGHT_SCORING`)

Transparent additive score (cap 100) for **ranked opportunities**:

| Factor | Points |
|--------|--------|
| Existing branded supply | +10 |
| Independent supply cluster (≥15) | +15 |
| Large independent assets | +10 |
| Parent/brand gap | +20 |
| Chain scale gap | +15 |
| Pipeline activity | +10 |
| Demand drivers present | +15 |
| Airport/tourism/MICE/mixed-use anchor | +10 |
| Saved signal / human review | +5 |

## Ranked opportunity types

- Brand White Space
- Operator White Space
- Conversion Opportunity
- Pipeline / Development Opportunity
- All-Inclusive Potential
- Branded Residential Potential
- Demand-Driver Supported Market

## UI (Scout Market Map)

- **Insight View** tab
- Insight cards (type, **quality**, priority, confidence, text, evidence, next step)
- **Evidence** expandable section per insight (Phase 5C)
- Quality summary + filters (Strong / Directional / Weak / Data Gaps)
- **Ranked Opportunities** panel
- **Save Related Signal to Watchlist** when a linked Scout signal exists
- **Mark for Review** / **Watch** — local status only unless user saves via watchlist endpoint

## Current limitations

- No AI enrichment
- No automatic Airtable writes from insights
- All-inclusive and branded residential insights are low-confidence review prompts only
- Major parent discovery without filter uses Brand Alias Mapping (limited confidence)

## Tests

```bash
node scripts/test-scout-market-insights.mjs
node scripts/test-scout-insight-review.mjs
```
