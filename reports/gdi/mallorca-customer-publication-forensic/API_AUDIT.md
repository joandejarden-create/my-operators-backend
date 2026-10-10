# API Forensic — Sheraton / Castillo

Endpoint: `GET /api/group-demand-intelligence/hotels/:hotelId/opportunities`

| Hotel | hotelId | HTTP | total | Ready | Watching |
|-------|---------|------|-------|-------|----------|
| Sheraton | recjhQdAUiSyxfCqE | 200 | 0 | 0 | 0 |
| Castillo | rec82D9zpB8fede1I | 200 | 0 | 0 | 0 |
| YOTEL | recrPQcZg7SFARRb2 | 200 | 17 | present | present |
| Bethesda | recLuxvwwxID7U2B8 | 200 | 28 | present | present |

## Backend filter

Load bag → commercial quality → **filterCustomerFacingOpportunities** → DTO.
Sheraton/Castillo drop to 0 at facing filter (`customerVisible:false` + gate failures). Not a READY-only API bug.
