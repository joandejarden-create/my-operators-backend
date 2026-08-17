# Scout Demand Overlays (Phase 5A)

## Purpose

Phase 5A adds the **first demand-driver overlay** to the Scout Market Map (`/app#/scout-market-map`). It reads Travel Infrastructure and optional Demand Anchors from the **Deal Capture Platform** base (`AIRTABLE_BASE_ID_ALT`) and displays them as optional map layers.

This is **read-only**. No Hotel Census writes. No Travel Infrastructure overwrites. Demand Anchors table is **not created** if missing.

## Data sources

| Overlay type | Airtable table | Scout `overlayType` |
|--------------|----------------|---------------------|
| Travel Infrastructure | `Travel Infrastructure Data` (or legacy name variant) | `travel_infrastructure` |
| Demand Anchors | `Demand Anchors` (optional) | `demand_anchor` |

Future inventory-only tables (not loaded in 5A):

- Market Development Projects
- Tourism Corridors
- Mixed-Use Zones

## API

### `GET /api/scout/demand-overlays`

Query: `country`, `city`, `market`, `submarket`, `overlayType`, `category`, `confidence`, `status`, `limit`

Response includes:

- `overlayMarkers` — records with valid coordinates only
- `overlayMarkersWithoutCoordinates` — listed in UI side panel, not placed on map
- `source.readOnly = true`, `source.writes = false`

### `GET /api/scout/market-map?includeDemandOverlays=1`

Adds non-breaking fields:

- `demandOverlayMarkers`
- `demandOverlayMarkersWithoutCoordinates`
- `demandOverlaySummary`

## Overlay marker format

```json
{
  "overlayId": "travel_infrastructure-recXXX",
  "overlayType": "travel_infrastructure",
  "category": "Airport",
  "name": "Cancún International Airport",
  "country": "Mexico",
  "latitude": 21.0365,
  "longitude": -86.8771,
  "source": {
    "table": "Travel Infrastructure Data",
    "recordId": "recXXX",
    "readOnly": true
  }
}
```

## Scout Market Map UI (Phase 5A)

Layer toggles, Overlay Category filter, Demand Drivers summary card, and a side panel for records without coordinates. See `docs/scout-market-opportunity-map.md` for full page context.

## Current limitations

- No AI enrichment
- No tourism corridor / mixed-use / market development project layers yet
- Opportunity Radar unchanged
- Brand Explorer unchanged
- Hotel Census unchanged

## Tests

```bash
node scripts/inventory-scout-demand-overlay-sources.mjs
node scripts/test-scout-demand-overlays.mjs
```
