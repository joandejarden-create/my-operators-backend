# Scout Market Opportunity Map (Phase 4)

## Overview

**Scout Market Map** is a **separate page** copied from the Opportunity Radar *structure* (filters, map, summary cards, view tabs) but wired to Scout APIs. It does **not** modify or share state with the existing Opportunity Radar.

| Item | Value |
|------|--------|
| Route | `/app#/scout-market-map` |
| Page label | Scout Market Map |
| Alternate title | Market Opportunity Map |
| API | `GET /api/scout/market-map` |

**Existing Opportunity Radar remains unchanged:**

- Route: `/app#/opportunity-radar`
- File: `public/deal-capture-radar-with-ranked-list.html`
- No shared localStorage keys with Scout Market Map

## Data sources (read-only aggregation)

| Layer | Source | Writes |
|-------|--------|--------|
| Hotel supply markers | Hotel Census | No |
| Generated opportunity signals | `lib/scout/opportunity-signals.js` | No |
| Saved / reviewed signals | Scout Opportunity Signals (Airtable) | No (this endpoint) |

Hotel Census is the source of truth for supply. **Market** and **Submarket** are the official STR geography fields.

Watchlist actions on the page use existing Scout Phase 3 endpoints:

- `POST /api/scout/opportunity-signals/save`
- `PATCH /api/scout/opportunity-signals/:signalId`

Those write **only** to Scout Opportunity Signals — never to Hotel Census, Radar, or Brand Explorer.

## Map layers (current)

1. **Branded supply** — open branded hotels (blue markers)
2. **Independent supply** — open independent hotels (purple markers)
3. **Pipeline** — pipeline hotels (amber markers)
4. **Generated Scout signals** — hotel-level signals with coordinates (gold markers)
5. **Saved watchlist signals** — from Scout Opportunity Signals (green markers)
6. **Market clusters** — market/submarket-level signals without exact coordinates, or supply clusters with ≥3 geocoded hotels
7. **Travel infrastructure** (Phase 5A) — cyan markers from Platform Travel Infrastructure table
8. **Demand anchors** (Phase 5A, if table exists) — pink markers from Demand Anchors table

Phase 5A also adds layer toggles, Overlay Category filter, Demand Drivers summary card, and a side panel for demand drivers without coordinates.

See `docs/scout-demand-overlays.md` for overlay API and field mapping details.

## View tabs

| Tab | Focus |
|-----|--------|
| Supply View | All geocoded hotels in scope |
| White Space View | Parent/brand/operator gap signals + clusters |
| Conversion View | Independent conversion, large independent, rebrand |
| Pipeline View | Pipeline hotels + pipeline activity signals |
| Watchlist View | Saved Scout Opportunity Signals |

## API: `GET /api/scout/market-map`

### Query parameters

- `country`, `city`, `market`, `submarket`
- `parentCompany`, `brand`, `chainScale`, `locationType`, `status`
- `signalType`, `reviewStatus`, `minPriorityScore`
- `includePipeline=1`, `includeSignals=1`, `includeSavedSignals=1`
- `includeDemandOverlays=1` (Phase 5A — travel infrastructure + demand anchors)
- `limit` (default 500, max 2000)

### Response shape

```json
{
  "success": true,
  "filters": {},
  "summary": {
    "hotelMarkers": 0,
    "signalMarkers": 0,
    "savedSignalMarkers": 0,
    "openHotels": 0,
    "pipelineHotels": 0,
    "brandedHotels": 0,
    "independentHotels": 0,
    "markets": 0,
    "submarkets": 0
  },
  "hotelMarkers": [],
  "signalMarkers": [],
  "savedSignalMarkers": [],
  "marketClusters": [],
  "generatedSignals": [],
  "demandOverlayMarkers": [],
  "demandOverlayMarkersWithoutCoordinates": [],
  "demandOverlaySummary": null,
  "warnings": [],
  "source": {
    "hotelSource": "Hotel Census",
    "signalSource": "Scout Opportunity Signals",
    "readOnly": true,
    "writes": false,
    "marketField": "Market",
    "submarketField": "Submarket"
  }
}
```

Hotel markers are included only when **Latitude** and **Longitude** are valid. Market/submarket-level generated signals without hotel coordinates are rolled into `marketClusters` instead of fake map pins.

## Files

| File | Role |
|------|------|
| `public/app/scout-market-map.html` | Page shell |
| `public/js/scout-market-map.js` | Filters, map, tabs, save/patch actions |
| `public/css/scout-market-map.css` | Scout-specific styles |
| `lib/scout/market-map.js` | Map report builder |
| `lib/scout/demand-overlays.js` | Demand overlay loader (Phase 5A) |
| `api/scout-market-map.js` | HTTP handler |
| `api/scout-demand-overlays.js` | Demand overlays HTTP handler (Phase 5A) |
| `scripts/test-scout-market-map.mjs` | Integration tests |
| `scripts/test-scout-demand-overlays.mjs` | Phase 5A overlay tests |

Route registration: `public/app.js` (new route + nav only), `server.js`, `server.upload-ready.js`.

## Future layers (not included yet)

- Tourism corridors
- Market development projects
- Mixed-use zones (as dedicated overlay layer)
- All-inclusive potential
- Branded residential potential
- AI enrichment

(Travel infrastructure and demand anchors are included in Phase 5A.)

## Market Insight Engine (Phase 5B)

- **Insight View** tab on Scout Market Map
- API: `GET /api/scout/market-insights`
- Optional: `GET /api/scout/market-map?includeInsights=1`
- See `docs/scout-market-insights.md` for insight types and scoring

## Insight Calibration & Evidence Review (Phase 5C)

- API: `GET /api/scout/insight-review`
- Extended: `GET /api/scout/market-insights?includeInsightReview=1`
- Insight View shows quality levels, evidence panels, review questions, and data gaps
- See `docs/scout-insight-review.md`

## Manual QA / regression

1. Open `/app#/opportunity-radar` — page loads as before (The Radar).
2. Open `/app#/scout-market-map` — Scout Market Map loads independently.
3. Apply filters; confirm summary cards and map markers update.
4. Save a generated signal to Watchlist; confirm it appears in Watchlist tab.
5. Patch saved signal to Researching / Ready for Outreach / Dismissed.
6. Confirm Hotel Census records are unchanged in Airtable.
7. Confirm Opportunity Radar JS/CSS files were not modified.

LocalStorage key for Scout filters: `scout_market_map_filters_v1` (does not touch Radar keys).

## Tests

```bash
node scripts/test-scout-market-map.mjs
node scripts/test-scout-insight-review.mjs
```
