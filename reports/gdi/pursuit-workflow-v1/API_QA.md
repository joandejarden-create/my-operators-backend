# API QA

| Method | Path | Auth |
|--------|------|------|
| GET | `/api/group-demand-intelligence/pursuit/enums` | pilot read |
| GET | `/api/group-demand-intelligence/hotels/:hotelId/pursuits` | pilot read |
| GET | `/api/group-demand-intelligence/hotels/:hotelId/pursuits/:pursuitId` | pilot read |
| GET | `/api/group-demand-intelligence/hotels/:hotelId/opportunities/:opportunityId/pursuit` | pilot read |
| POST | `.../opportunities/:opportunityId/pursuit/start` | member |
| PATCH | `.../pursuits/:pursuitId` | member |
| POST | `.../pursuits/:pursuitId/response` | member |
| POST | `.../pursuits/:pursuitId/outcome` | member |
| POST | `.../pursuits/:pursuitId/follow-up` | member |

## Checks

- Start requires OUTREACH_NOW or OUTREACH_PREPARE
- Start does not change Ready gate fields
- List filter: `?filter=ACTIVE|FOLLOW_UP_DUE|HOTEL_SELECTION|CLOSED`
- Opportunity list overlay includes `pursuitId` / `pursuitStatus` / `canStartPursuit`
