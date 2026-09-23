# GDI Performance V1 — View Details

## Code path

| Layer | Location |
| --- | --- |
| Button | `dealality-gdi-ui.js` — `View Details` / `data-open` |
| Handler (share) | `share-app.js` → `openDetail(id)` |
| Handler (auth) | `app.js` → `openDetail(id)` (+ decision GET) |
| API share | `GET /api/group-demand-intelligence/share/hotels/:hotelId/opportunities/:id` |
| API auth | `GET /api/group-demand-intelligence/hotels/:hotelId/opportunities/:id` |

## Model (before)

**B + D + F:** one detail endpoint that re-fetched the entire hotel opportunity set from Airtable, then projected one row. No live research. Auth also fetched decision in parallel.

## Model (after)

1. Immediate drawer shell (title from list row).  
2. Detail GET prefers **in-memory hotel cache** (`X-GDI-Detail-Source: cache`).  
3. Cache miss → **single Airtable row** when configured, else full doc.  
4. Progression map TTL-cached / parallelized.

## Measured (Bethesda share)

| Metric | Before P50 | After P50 (warm) |
| --- | ---: | ---: |
| Detail API | 4881ms | **171ms** (cache) |
| Click ack | none (blocked) | **<100ms** shell |

## Pass bar

- Visual ack <100ms: **YES** (shell)  
- Warm click-to-detail ≤500ms: **YES** (~171ms API + render)  
- Server-backed P95 ≤1500ms warm: **YES** (178ms)
