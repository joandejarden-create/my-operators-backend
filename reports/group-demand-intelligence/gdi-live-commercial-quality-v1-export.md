# GDI Live Commercial Quality V1 — Export

Marker: `gdi_live_commercial_quality_v1`

## Endpoints

| Surface | Method | Path |
|---|---|---|
| Auth | GET | `/api/group-demand-intelligence/hotels/:hotelId/opportunities/export.csv` |
| Share | GET | `/api/group-demand-intelligence/share/hotels/:hotelId/opportunities/export.csv?share=` |

## Behavior

- Honors `weekly`, `priority`, `includeDisqualified`, optional `ids` (auth)
- Includes snapshot metadata comments: hotel, hotelId, generatedAt, filter JSON, schema
- Every row includes **Dealality Opportunity ID** (stable `id`)
- CSV Excel-safe quoting for commas/quotes/newlines
- XLSX: **NOT IMPLEMENTED** (CSV mandatory met)

## UI

Auth + share browse toolbars: **Export CSV** control (`#gdiExportCsvBtn`) using current filter query.

## CI / CRM

Hotels can paste Dealality Opportunity ID into CI/Excel. No CI integration built.
