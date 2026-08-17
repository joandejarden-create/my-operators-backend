# Scout Market Coverage (Phase 1)

**Endpoint:** `GET /api/scout/market-coverage`  
**Module:** `lib/scout/market-coverage.js`  
**Handler:** `api/scout-market-coverage.js`

Read-only market coverage and white-space intelligence from **Hotel Census** on Deal Capture Platform (`AIRTABLE_BASE_ID_ALT`). No Airtable writes.

---

## STR geography fields

Dealality treats **Hotel Census `Market` and `Submarket` as the official STR-defined geography fields.**

| Excel (STR export) | Hotel Census Airtable field | Notes |
|--------------------|----------------------------|--------|
| STR Market | `Market` | No separate `STR Market` column |
| STR Submarket | `Submarket` | No separate `STR Submarket` column |

STR Excel import (`lib/str-census-import/`) writes into these existing columns. Scout, Radar, and Brand Explorer read them as STR market/submarket taxonomy.

**Do not** add Airtable fields named `STR Market` or `STR Submarket`.

---

## Query parameters

| Param | Filters Hotel Census field |
|-------|---------------------------|
| `country` | `country` |
| `city` | `city` |
| `market` | `Market` |
| `strMarket` | `Market` (alias — same as `market`) |
| `submarket` | `Submarket` |
| `strSubmarket` | `Submarket` (alias — same as `submarket`) |
| `parentCompany` | `Parent Company` |
| `brand` | `Affiliation` (via Brand Alias Mapping) |
| `chainScale` | `Chain Scale` |
| `locationType` | `Location` |
| `status` | `status` |
| `includePipeline` | Include `Pipeline` rows when `1` |

---

## Response shape

- `filters` — applied filters + optional `brandResolution`
- `metrics` — open/pipeline counts, branded vs independent, geography cardinality
- `breakdowns` — `bySTRMarket` / `bySTRSubmarket` use `Market` / `Submarket` internally
- `whiteSpace` — parent/brand gap, independent cluster, pipeline activity signals
- `recordsSample` — sample matched rows
- `warnings` — alias / white-space context only (no “missing STR field” warnings)
- `source.fieldMapping` — documents logical → Airtable mapping

Example `source` metadata:

```json
{
  "fieldMapping": {
    "strMarket": "Market",
    "strSubmarket": "Submarket",
    "market": "Market",
    "submarket": "Submarket"
  },
  "strGeographyNote": "Excel STR Market → Hotel Census Market; Excel STR Submarket → Hotel Census Submarket. No separate STR Market/STR Submarket Airtable fields.",
  "readOnly": true,
  "writes": false
}
```

---

## White-space logic (Phase 1)

| `opportunityType` | When |
|-------------------|------|
| `parent_company_market_gap` | Selected parent has 0 open hotels in STR market; other branded hotels exist |
| `brand_market_gap` | Selected brand has 0 open hotels; parent or comparable chain scale present |
| `independent_conversion_cluster` | ≥15 open independents in market |
| `pipeline_activity` | Pipeline hotels in market |

Pass `parentCompany` and/or `brand` for gap signals; country-only queries still return independent/pipeline flags.

---

## Testing

```bash
node scripts/test-scout-market-coverage.mjs
node scripts/test-scout-opportunity-signals.mjs
```

Requires `AIRTABLE_API_KEY` and `AIRTABLE_BASE_ID_ALT`.

---

## Phase 2 — Opportunity signals

**Endpoint:** `GET /api/scout/opportunity-signals`  
**Module:** `lib/scout/opportunity-signals.js`

Read-only, explainable signals: parent/brand market gaps, independent clusters, large independents, pipeline activity, rebrand candidates, operator opportunity markets. See signal types and scoring in module `SIGNAL_TYPES` / `SCOUT_SIGNAL_SCORING`.

Report generator:

```bash
node scripts/generate-scout-opportunity-signals-report.mjs --country=Mexico
```

---

## Phase 3 — Signal watchlist

Saved signals live in **Scout Opportunity Signals** (Platform base). See `docs/scout-signal-watchlist.md`.

```bash
node scripts/ensure-scout-opportunity-signals-table.mjs --apply
node scripts/test-scout-signal-watchlist.mjs
```

---

## Related docs

- `docs/hotel-census-geography-population-rules.md` — Market/Submarket population from STR Excel
- `docs/brand-explorer-census-phase1-plan.md` — Brand Explorer census footprint (unchanged by Scout)
